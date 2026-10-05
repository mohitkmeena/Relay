//! M4 differential test harness (oracle-based, no Postgres required yet).
//!
//! Strategy: apply random changes to a Z-set view through the pipeline's
//! own change-delta machinery, and independently maintain an oracle table
//! (a plain HashMap keyed by primary key) through direct row semantics.
//! After every batch, the Z-set's positive-weight rows must exactly equal
//! the oracle's contents.
//!
//! The oracle deliberately reuses NO pipeline code: it interprets ops
//! directly. When the differential test moves to real Postgres (M4 final
//! form), the same oracle will compare against actual SELECT results.

use cdcx_model::{Change, Op, Value};
use cdcx_state::zset::{change_delta, ZSet};

/// Deterministic tiny PRNG (xorshift64) so failures reproduce from a seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Oracle: PK -> row. Updates overwrite; deletes remove.
#[derive(Default)]
struct Oracle {
    rows: std::collections::HashMap<i64, Vec<(String, Value)>>,
}

impl Oracle {
    fn apply(&mut self, change: &Change) {
        let pk_of = |row: &[(String, Value)]| {
            row.iter()
                .find(|(n, _)| n == "id")
                .and_then(|(_, v)| match v {
                    Value::Int(i) => Some(*i),
                    _ => None,
                })
                .unwrap_or(-1)
        };
        match change.op {
            Op::Insert | Op::Update => {
                if let Some(after) = &change.after {
                    self.rows.insert(pk_of(after), after.clone());
                }
            }
            Op::Delete => {
                if let Some(before) = &change.before {
                    self.rows.remove(&pk_of(before));
                }
            }
        }
    }

    fn contents(&self) -> Vec<Vec<(String, Value)>> {
        self.rows.values().cloned().collect()
    }
}

fn row_of(id: i64, v: i64) -> Vec<(String, Value)> {
    vec![
        ("id".into(), Value::Int(id)),
        ("v".into(), Value::Int(v)),
    ]
}

fn random_change(rng: &mut Rng, oracle: &Oracle) -> Change {
    let n_existing = oracle.rows.len() as u64;
    let id = if n_existing > 0 && rng.range(2) == 0 {
        // Touch an existing row (update or delete).
        let keys: Vec<i64> = oracle.rows.keys().copied().collect();
        keys[rng.range(n_existing as usize)]
    } else {
        rng.range(1000) as i64
    };
    let op = match rng.range(3) {
        0 => Op::Insert,
        1 => Op::Update,
        _ => Op::Delete,
    };
    let before = oracle.rows.get(&id).cloned();
    let after = if op == Op::Delete { None } else { Some(row_of(id, rng.range(100) as i64)) };
    // Skip no-op updates (same image both sides).
    if op == Op::Update && before == after {
        return random_change(rng, oracle);
    }
    // Skip re-insert of an existing PK (the decoder would never emit it).
    if op == Op::Insert && before.is_some() {
        return random_change(rng, oracle);
    }
    // Skip delete of a missing row.
    if op == Op::Delete && before.is_none() {
        return random_change(rng, oracle);
    }
    Change {
        namespace: "public".into(),
        table: "t".into(),
        op,
        after,
        before,
        lsn: rng.next(),
        index_in_txn: 0,
    }
}

fn zset_contents(z: &ZSet) -> Vec<Vec<(String, Value)>> {
    z.iter()
        .filter(|(_, w)| *w > 0)
        .map(|(row, _)| row.clone())
        .collect()
}

fn sorted(mut rows: Vec<Vec<(String, Value)>>) -> Vec<Vec<(String, Value)>> {
    rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    rows
}

#[test]
fn differential_10k_changes() {
    for seed in 0..25 {
        let mut rng = Rng::new(seed + 0x9E3779B97F4A7C15);
        let mut view = ZSet::new();
        let mut oracle = Oracle::default();

        for _ in 0..400 {
            let change = random_change(&mut rng, &oracle);
            oracle.apply(&change);
            change_delta(&change).apply_delta(&mut view);
        }

        let expected = sorted(oracle.contents());
        let got = sorted(zset_contents(&view));
        assert_eq!(
            expected, got,
            "differential mismatch at seed {seed}: oracle {expected:?} vs zset {got:?}"
        );
    }
}
