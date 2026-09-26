//! Signed-row sets (Z-sets): the algebraic core of incremental view
//! maintenance (M7).
//!
//! A Z-set maps rows to integer weights: +1 for insertion, -1 for
//! retraction. Applying a delta to a view is addition; a view is
//! "correct" when every surviving row's weight is exactly 1. This is the
//! DBSP formulation, and it makes the four filter transitions and the
//! aggregate operators composable without special cases.
//!
//! This module is pure and synchronous, like everything under cdcx-plan:
//! the engine calls these functions between durable checkpoints.

use std::collections::HashMap;

/// A row: a set of named values. Ordered for deterministic iteration.
pub type Row = Vec<(String, cdcx_model::Value)>;

/// A signed multiset of rows: row -> nonzero weight.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ZSet {
    rows: HashMap<Row, i64>,
}

impl ZSet {
    /// The empty Z-set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `weight` copies of `row`. Weights may be negative.
    pub fn add(&mut self, row: Row, weight: i64) {
        use std::collections::hash_map::Entry;
        match self.rows.entry(row) {
            Entry::Occupied(mut e) => {
                *e.get_mut() += weight;
                if *e.get() == 0 {
                    e.remove();
                }
            }
            Entry::Vacant(e) => {
                e.insert(weight);
            }
        }
    }

    /// Z-set addition: apply all of `other`'s deltas.
    pub fn merge(&mut self, other: &ZSet) {
        for (row, w) in &other.rows {
            self.add(row.clone(), *w);
        }
    }

    /// Apply this delta to a materialized view (also a Z-set, all
    /// weights >= 0 after compaction).
    pub fn apply_delta(&self, view: &mut ZSet) {
        view.merge(self);
        view.compact();
    }

    /// Remove zero-weight entries left behind by cancellation.
    pub fn compact(&mut self) {
        self.rows.retain(|_, w| *w != 0);
    }

    /// Iterate (row, weight) pairs. Only nonzero weights appear after
    /// [`ZSet::compact`].
    pub fn iter(&self) -> impl Iterator<Item = (&Row, i64)> {
        self.rows.iter().map(|(r, w)| (r, *w))
    }

    /// Number of distinct nonzero-weight rows.
    pub fn len(&self) -> usize {
        self.rows.values().filter(|w| **w != 0).count()
    }

    /// True when no rows have nonzero weight.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Convert a change into its Z-set delta: +1 for the new image, -1 for
/// the old image (update = both; delete = just -1).
pub fn change_delta(change: &cdcx_model::Change) -> ZSet {
    let mut z = ZSet::new();
    match (&change.before, &change.after) {
        (_, Some(after)) => {
            if change.op == cdcx_model::Op::Update {
                if let Some(before) = &change.before {
                    z.add(before.clone(), -1);
                }
            }
            z.add(after.clone(), 1);
        }
        (Some(before), None) => {
            z.add(before.clone(), -1);
        }
        (None, None) => {}
    }
    z
}

/// SUM aggregate over a Z-set, grouped by key columns.
///
/// Returns group-key -> sum. Rows with negative weight subtract, which
/// is what makes this incremental: applying a delta recomputes only the
/// touched groups.
pub fn sum_by(zset: &ZSet, key_columns: &[&str], value_column: &str) -> HashMap<Row, i64> {
    let mut sums = HashMap::new();
    for (row, weight) in zset.iter() {
        let key: Row = row
            .iter()
            .filter(|(name, _)| key_columns.contains(&name.as_str()))
            .cloned()
            .collect();
        let value = row
            .iter()
            .find(|(name, _)| name == value_column)
            .and_then(|(_, v)| match v {
                cdcx_model::Value::Int(i) => Some(*i),
                cdcx_model::Value::Float(f) => Some(f.0 as i64),
                _ => None,
            })
            .unwrap_or(0);
        *sums.entry(key).or_insert(0i64) += value * weight;
    }
    sums
}

/// COUNT aggregate over a Z-set, grouped by key columns.
pub fn count_by(zset: &ZSet, key_columns: &[&str]) -> HashMap<Row, i64> {
    let mut counts = HashMap::new();
    for (row, weight) in zset.iter() {
        let key: Row = row
            .iter()
            .filter(|(name, _)| key_columns.contains(&name.as_str()))
            .cloned()
            .collect();
        *counts.entry(key).or_insert(0i64) += weight;
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdcx_model::Value;

    fn row(pairs: &[(&str, i64)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Int(*v)))
            .collect()
    }

    #[test]
    fn insert_update_delete_invariants() {
        let mut view = ZSet::new();
        assert!(view.is_empty());

        // Insert: weight 1.
        let insert = cdcx_model::Change {
            namespace: "public".into(),
            table: "t".into(),
            op: cdcx_model::Op::Insert,
            after: Some(row(&[("id", 1), ("v", 10)])),
            before: None,
            lsn: 1,
            index_in_txn: 0,
        };
        change_delta(&insert).apply_delta(&mut view);
        assert_eq!(view.len(), 1);

        // Update: -1 old, +1 new.
        let update = cdcx_model::Change {
            namespace: "public".into(),
            table: "t".into(),
            op: cdcx_model::Op::Update,
            after: Some(row(&[("id", 1), ("v", 20)])),
            before: Some(row(&[("id", 1), ("v", 10)])),
            lsn: 2,
            index_in_txn: 0,
        };
        change_delta(&update).apply_delta(&mut view);
        assert_eq!(view.len(), 1);

        // Delete: -1.
        let delete = cdcx_model::Change {
            namespace: "public".into(),
            table: "t".into(),
            op: cdcx_model::Op::Delete,
            after: None,
            before: Some(row(&[("id", 1), ("v", 20)])),
            lsn: 3,
            index_in_txn: 0,
        };
        change_delta(&delete).apply_delta(&mut view);
        assert!(view.is_empty());
    }

    #[test]
    fn sum_and_count_are_incremental() {
        let mut view = ZSet::new();
        let changes = [
            (
                cdcx_model::Op::Insert,
                None,
                Some(row(&[("id", 1), ("g", 1), ("v", 10)])),
            ),
            (
                cdcx_model::Op::Insert,
                None,
                Some(row(&[("id", 2), ("g", 1), ("v", 5)])),
            ),
            (
                cdcx_model::Op::Insert,
                None,
                Some(row(&[("id", 3), ("g", 2), ("v", 100)])),
            ),
            (
                cdcx_model::Op::Update,
                Some(row(&[("id", 2), ("g", 1), ("v", 5)])),
                Some(row(&[("id", 2), ("g", 1), ("v", 50)])),
            ),
        ];
        for (op, before, after) in changes {
            let change = cdcx_model::Change {
                namespace: "public".into(),
                table: "t".into(),
                op,
                after,
                before,
                lsn: 1,
                index_in_txn: 0,
            };
            change_delta(&change).apply_delta(&mut view);
        }
        let sums = sum_by(&view, &["g"], "v");
        assert_eq!(sums.get(&row(&[("g", 1)])), Some(&60)); // 10 + 50
        assert_eq!(sums.get(&row(&[("g", 2)])), Some(&100));
        let counts = count_by(&view, &["g"]);
        assert_eq!(counts.get(&row(&[g_placeholder(1)])), Some(&2));
        assert_eq!(counts.get(&row(&[("g", 2)])), Some(&1));
    }

    fn g_placeholder(v: i64) -> (&'static str, i64) {
        ("g", v)
    }
}
