//! M4 differential test, Postgres-backed form.
//!
//! Requires a real PostgreSQL with `wal_level=logical`. Ignored by
//! default; run with:
//!
//! ```sh
//! docker compose up -d postgres
//! cargo test --test differential_postgres -- --ignored
//! ```
//!
//! Flow: connect a reader to the test slot, run a random workload of
//! INSERT/UPDATE/DELETE on `public.users` through psql (NOT through
//! pipeline code), apply every decoded txn to an in-memory Z-set view,
//! and after every batch compare the view against a plain SELECT.
//! The oracle (both the workload and the comparison) shares no code
//! with the decoder or the engine.

use cdcx_pg::{Reader, ReaderConfig};
use cdcx_state::zset::{change_delta, ZSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const PG: &str = "postgres://postgres:postgres@localhost:5432/postgres";
const SLOT: &str = "cdcx_diff_slot";
const PUB: &str = "cdcx_diff_pub";

fn rng_next(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// One round-trip-free psql exec via the docker CLI (deliberately not
/// a Rust client: keeps the oracle independent of tokio-postgres
/// behavior and of any pipeline code).
fn psql(sql: &str) {
    let out = std::process::Command::new("docker")
        .args(["exec", "-i", "cdcx-postgres", "psql", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(sql.as_bytes())?;
            child.wait_with_output()
        })
        .expect("docker exec psql");
    assert!(
        out.status.success(),
        "psql failed:\nsql: {sql}\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn psql_rows(sql: &str) -> Vec<String> {
    let out = std::process::Command::new("docker")
        .args(["exec", "-i", "cdcx-postgres", "psql", "-U", "postgres", "-d", "postgres", "-At", "-v", "ON_ERROR_STOP=1"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(sql.as_bytes())?;
            child.wait_with_output()
        })
        .expect("docker exec psql");
    assert!(
        out.status.success(),
        "psql failed:\nsql: {sql}\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect()
}

#[tokio::test]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn streamed_changes_match_select() {
    // Clean slate: drop slot (if left over), reset table, recreate
    // publication. The slot is created fresh each run.
    psql(&format!(
        "SELECT pg_drop_replication_slot('{SLOT}') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{SLOT}');"
    ));
    psql("DROP PUBLICATION IF EXISTS cdcx_diff_pub;");
    psql("DROP TABLE IF EXISTS public.users CASCADE;");
    psql(
        "CREATE TABLE public.users (id BIGINT PRIMARY KEY, name TEXT, balance NUMERIC(12,2));
         ALTER TABLE public.users REPLICA IDENTITY FULL;
         CREATE PUBLICATION cdcx_diff_pub FOR TABLE public.users;",
    );

    // The reader (slot creation happens inside pgwire-replication's
    // connect, per its API).
    let config = ReaderConfig {
        host: "localhost".into(),
        port: 5432,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "postgres".into(),
        slot: SLOT.into(),
        publication: PUB.into(),
    };

    let view: Arc<Mutex<ZSet>> = Arc::new(Mutex::new(ZSet::new()));
    let processed = Arc::new(AtomicU64::new(0));

    let reader_view = view.clone();
    let reader_count = processed.clone();
    let reader = Reader::new(config);
    let stream = tokio::spawn(async move {
        reader
            .run(move |txn| {
                let mut v = reader_view.lock().unwrap();
                for change in &txn.changes {
                    change_delta(change).apply_delta(&mut v);
                }
                reader_count.fetch_add(1, Ordering::SeqCst);
            })
            .await
    });

    // Give the reader a moment to create the slot before writing.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Random workload: 20 batches, each a few statements.
    let mut seed = 0x1234_5678_9ABC_DEF0u64;
    for batch in 0..20 {
        let mut sql = String::from("BEGIN;");
        for _ in 0..(1 + rng_next(&mut seed) % 5) {
            let id = (rng_next(&mut seed) % 50) as i64;
            let pick: u64 = rng_next(&mut seed) % 3;
            if pick == 0 {
                sql.push_str(&format!(
                    " INSERT INTO public.users (id, name, balance) VALUES ({id}, 'n{id}', {id}.50) ON CONFLICT (id) DO UPDATE SET name = 'n{id}', balance = {id}.50;"
                ));
            } else if pick == 1 {
                sql.push_str(&format!(
                    " UPDATE public.users SET balance = balance + 1 WHERE id = {id};"
                ));
            } else {
                sql.push_str(&format!(" DELETE FROM public.users WHERE id = {id};"));
            }
        }
        sql.push_str(" COMMIT;");
        psql(&sql);

        // Wait until the reader has consumed the batch (a new txn
        // beyond the ones we've seen), then compare against SELECT.
        let before = processed.load(Ordering::SeqCst);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while processed.load(Ordering::SeqCst) <= before {
            assert!(
                std::time::Instant::now() < deadline,
                "reader stalled at batch {batch}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // Small settle window in case of interleaved autocommit.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Oracle: SELECT from the database. Compare sorted (id, name,
        // balance) tuples with the Z-set's positive-weight rows.
        let expected = psql_rows(
            "SELECT id, name, balance::text FROM public.users ORDER BY id;",
        );
        let mut got: Vec<String> = view
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, w)| *w > 0)
            .map(|(row, _)| {
                let id = row.iter().find(|(n, _)| n == "id").map(|(_, v)| v.to_string()).unwrap_or_default();
                let name = row.iter().find(|(n, _)| n == "name").map(|(_, v)| v.to_string()).unwrap_or_default();
                let bal = row.iter().find(|(n, _)| n == "balance").map(|(_, v)| v.to_string()).unwrap_or_default();
                format!("{id}|{name}|{bal}")
            })
            .collect();
        got.sort();
        let expected_sorted = {
            let mut e = expected;
            e.sort();
            e
        };
        assert_eq!(
            expected_sorted, got,
            "view diverged from SELECT at batch {batch}:\nSELECT: {expected_sorted:?}\nview:   {got:?}"
        );
    }

    stream.abort();
}
