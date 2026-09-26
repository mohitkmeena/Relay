//! M7 differential test: aggregate views under random workloads and
//! crash-replay, validated against an independent oracle.

use cdcx_engine::{MaterializedView, ViewConfig};
use cdcx_model::{Change, Op, Value};
use cdcx_state::zset::change_delta;
use cdcx_state::zset::ZSet;

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

/// Oracle: recomputes SUM/COUNT by group from a plain row table, using
/// NO pipeline code (direct interpretation).
#[derive(Default)]
struct Oracle {
    rows: std::collections::HashMap<i64, (i64, i64)>, // id -> (group, value)
}

impl Oracle {
    fn apply(&mut self, change: &Change) {
        let pk = |row: &[(String, Value)]| {
            row.iter()
                .find(|(n, _)| n == "id")
                .and_then(|(_, v)| if let Value::Int(i) = v { Some(*i) } else { None })
                .unwrap_or(-1)
        };
        let group = |row: &[(String, Value)]| {
            row.iter()
                .find(|(n, _)| n == "g")
                .and_then(|(_, v)| if let Value::Int(i) = v { Some(*i) } else { None })
                .unwrap_or(-1)
        };
        let value = |row: &[(String, Value)]| {
            row.iter()
                .find(|(n, _)| n == "v")
                .and_then(|(_, v)| if let Value::Int(i) = v { Some(*i) } else { None })
                .unwrap_or(0)
        };
        match change.op {
            Op::Insert | Op::Update => {
                if let Some(after) = &change.after {
                    self.rows.insert(pk(after), (group(after), value(after)));
                }
            }
            Op::Delete => {
                if let Some(before) = &change.before {
                    self.rows.remove(&pk(before));
                }
            }
        }
    }

    /// SUM(v) and COUNT(*) per group.
    fn aggregates(&self) -> (std::collections::BTreeMap<i64, i64>, std::collections::BTreeMap<i64, i64>) {
        let mut sums = std::collections::BTreeMap::new();
        let mut counts = std::collections::BTreeMap::new();
        for (_, (g, v)) in &self.rows {
            *sums.entry(*g).or_insert(0) += v;
            *counts.entry(*g).or_insert(0) += 1;
        }
        (sums, counts)
    }
}

fn make_change(rng: &mut Rng, oracle: &Oracle) -> Change {
    let n = oracle.rows.len() as u64;
    let id = if n > 0 && rng.range(2) == 0 {
        let keys: Vec<i64> = oracle.rows.keys().copied().collect();
        keys[rng.range(n) as usize]
    } else {
        rng.range(1000) as i64
    };
    let op = match rng.range(3) {
        0 => Op::Insert,
        1 => Op::Update,
        _ => Op::Delete,
    };
    let before = oracle.rows.get(&id).map(|(g, v)| {
        vec![
            ("id".to_string(), Value::Int(id)),
            ("g".to_string(), Value::Int(*g)),
            ("v".to_string(), Value::Int(*v)),
        ]
    });
    if (op == Op::Insert && before.is_some())
        || (op == Op::Delete && before.is_none())
        || (op == Op::Update && before.is_none())
    {
        return make_change(rng, oracle);
    }
    let after = if op == Op::Delete {
        None
    } else {
        Some(vec![
            ("id".to_string(), Value::Int(id)),
            ("g".to_string(), Value::Int(rng.range(4) as i64)),
            ("v".to_string(), Value::Int(rng.range(50) as i64)),
        ])
    };
    if op == Op::Update && before == after {
        return make_change(rng, oracle);
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

fn view_aggregates(view: &MaterializedView) -> (std::collections::BTreeMap<i64, i64>, std::collections::BTreeMap<i64, i64>) {
    let aggs = view.aggregates();
    let mut sums = std::collections::BTreeMap::new();
    let mut counts = std::collections::BTreeMap::new();
    for (key, sum) in &aggs.sums {
        if let Some((_, Value::Int(g))) = key.iter().find(|(n, _)| n == "g") {
            sums.insert(*g, *sum);
        }
    }
    for (key, count) in &aggs.counts {
        if let Some((_, Value::Int(g))) = key.iter().find(|(n, _)| n == "g") {
            counts.insert(*g, *count);
        }
    }
    (sums, counts)
}

fn txn_of(changes: Vec<Change>) -> cdcx_model::Txn {
    let commit_lsn = changes.last().map(|c| c.lsn).unwrap_or(0);
    cdcx_model::Txn {
        commit_lsn,
        changes,
    }
}

#[test]
fn aggregate_views_match_oracle() {
    for seed in 1..=25 {
        let mut rng = Rng::new(seed * 0x517CC1B727220A95);
        let mut view = MaterializedView::new(ViewConfig {
            key_columns: vec!["g".into()],
            sum_column: Some("v".into()),
            count: true,
            ..ViewConfig::default()
        });
        let mut oracle = Oracle::default();

        // Batches of transactions; compare after every batch.
        for _ in 0..50 {
            let mut changes = Vec::new();
            for _ in 0..(1 + rng.range(5)) {
                let change = make_change(&mut rng, &oracle);
                oracle.apply(&change);
                changes.push(change);
            }
            view.process(&txn_of(changes));

            let (exp_sums, exp_counts) = oracle.aggregates();
            let (got_sums, got_counts) = view_aggregates(&view);
            assert_eq!(
                exp_sums, got_sums,
                "SUM mismatch at seed {seed}"
            );
            assert_eq!(
                exp_counts, got_counts,
                "COUNT mismatch at seed {seed}"
            );
        }
    }
}

#[test]
fn crash_and_replay_converges() {
    // Simulate: process N txns, snapshot state (as a checkpoint would),
    // then replay the txns after the snapshot point *again* from the
    // snapshot's state — modeling a crash after the checkpoint but
    // before the sink acked, where the slot restarts from the last
    // confirmed LSN.
    let mut rng = Rng::new(0xDEADBEEF);
    let mut oracle = Oracle::default();

    let mut txns = Vec::new();
    for _ in 0..40 {
        let mut changes = Vec::new();
        for _ in 0..(1 + rng.range(4)) {
            let change = make_change(&mut rng, &oracle);
            oracle.apply(&change);
            changes.push(change);
        }
        txns.push(txn_of(changes));
    }

    let config = ViewConfig {
        key_columns: vec!["g".into()],
        sum_column: Some("v".into()),
        count: true,
        ..ViewConfig::default()
    };

    // First run: everything.
    let mut view = MaterializedView::new(config.clone());
    for txn in &txns {
        view.process(txn);
    }
    let full = view_aggregates(&view);

    // Crashed run: apply txns up to the "checkpoint", save state via
    // JSON, replay the remainder from that state.
    let checkpoint_at = 25;
    let mut crashed = MaterializedView::new(config);
    for txn in &txns[..checkpoint_at] {
        crashed.process(txn);
    }
    let state = crashed.state();

    // "Restart": restore from state and replay everything after the
    // checkpoint.
    let mut restarted = MaterializedView::new(config);
    restarted.restore(&state);
    for txn in &txns[checkpoint_at..] {
        restarted.process(txn);
    }
    let replayed = view_aggregates(&restarted);

    // NOTE: correct only if the checkpoint state is at a txn boundary
    // and the replay starts exactly at checkpointed LSN — which is the
    // engine's contract.
    assert_eq!(full, replayed, "crash-replay diverged from full run");

    // And both agree with the oracle.
    let (exp_sums, exp_counts) = oracle.aggregates();
    assert_eq!(exp_sums, replayed.0);
    assert_eq!(exp_counts, replayed.1);
}

#[test]
fn duplicate_txn_delivery_is_detected_not_silent() {
    // At-least-once means duplicates reach the view. A duplicate txn
    // applied twice double-counts; the ENGINE must dedupe by
    // (commit_lsn) before calling process. This test pins the
    // contract: applying the same txn twice changes the aggregates,
    // i.e. there is no free idempotence — the engine's dedupe is load-
    // bearing. (When this test starts failing because the view became
    // idempotent, that's a feature — flip the assertion.)
    let txn = txn_of(vec![Change {
        namespace: "public".into(),
        table: "t".into(),
        op: Op::Insert,
        after: Some(vec![
            ("id".to_string(), Value::Int(1)),
            ("g".to_string(), Value::Int(0)),
            ("v".to_string(), Value::Int(10)),
        ]),
        before: None,
        lsn: 100,
        index_in_txn: 0,
    }]);
    let mut view = MaterializedView::new(ViewConfig {
        key_columns: vec!["g".into()],
        sum_column: Some("v".into()),
        count: true,
        ..ViewConfig::default()
    });
    view.process(&txn);
    let once = view_aggregates(&view);
    view.process(&txn);
    let twice = view_aggregates(&view);
    assert_ne!(once, twice, "views are not idempotent; engine dedupe is required");
    let _ = change_delta; // silence unused import if assertions change
}
