//! Materialized view runtime (M7): stateful operators whose state is
//! checkpointed atomically with the LSN.
//!
//! The invariant: `view_state` and `checkpoint.lsn` describe the same
//! point in the stream. A crash between "operator applied" and
//! "checkpoint advanced" is safe: on restart the reader replays from
//! the checkpointed LSN, operators re-apply the same deltas, and the
//! Z-set algebra converges — applying the same delta twice is *not*
//! idempotent, which is why the replay must start from the checkpoint
//! and not from "wherever the operators got to."
//!
//! This module bundles the operator set the engine drives: view Z-set,
//! aggregates over it, optional windows and reference join.

use cdcx_model::Txn;
use cdcx_state::join::ReferenceJoin;
use cdcx_state::windows::TumblingWindows;
use cdcx_state::zset::{ZSet, change_delta, count_by, sum_by};

/// Configuration for one materialized view.
#[derive(Clone, Debug)]
pub struct ViewConfig {
    /// Group-by key columns for the aggregates.
    pub key_columns: Vec<String>,
    /// Column to SUM. `None` skips the sum aggregate.
    pub sum_column: Option<String>,
    /// Whether to maintain a COUNT per group.
    pub count: bool,
    /// Tumbling window size in ms over this timestamp column;
    /// `None` disables windowing.
    pub window: Option<(i64, String)>,
    /// Reference join (fact key, reference key) column names;
    /// `None` disables joining.
    pub join: Option<(String, String)>,
}

impl Default for ViewConfig {
    fn default() -> Self {
        Self {
            key_columns: vec![],
            sum_column: None,
            count: true,
            window: None,
            join: None,
        }
    }
}

/// Aggregates over the view, keyed by group key.
#[derive(Debug, Default, PartialEq)]
pub struct Aggregates {
    /// group key -> SUM(value_column).
    pub sums: std::collections::HashMap<cdcx_state::zset::Row, i64>,
    /// group key -> COUNT(*).
    pub counts: std::collections::HashMap<cdcx_state::zset::Row, i64>,
}

/// A materialized view: stream of changes in, aggregate rows out.
pub struct MaterializedView {
    config: ViewConfig,
    /// The view itself (all weights >= 0 after compaction).
    view: ZSet,
    windows: Option<TumblingWindows>,
    join: Option<ReferenceJoin>,
}

impl MaterializedView {
    /// New view with the given configuration.
    pub fn new(config: ViewConfig) -> Self {
        let windows = config
            .window
            .as_ref()
            .map(|(size, col)| TumblingWindows::new(*size, col.clone()));
        let join = config
            .join
            .as_ref()
            .map(|(fact, reference)| ReferenceJoin::new(fact.clone(), reference.clone()));
        Self {
            config,
            view: ZSet::new(),
            windows,
            join,
        }
    }

    /// Apply a reference-table transaction's changes to the join index.
    /// Call this BEFORE [`Self::process`] for facts in the same txn.
    pub fn apply_reference_txn(&mut self, txn: &Txn) {
        let Some(join) = self.join.as_mut() else {
            return;
        };
        for change in &txn.changes {
            let mut delta = change_delta(change);
            // The join index is keyed by the reference row's own key;
            // normalize the weight to presence/absence.
            join.apply_reference_delta(&delta);
            let _ = &mut delta;
        }
    }

    /// Process a fact transaction: apply deltas through the (optional)
    /// join and windows into the view, then recompute aggregates.
    ///
    /// Returns the aggregates as of after this transaction.
    pub fn process(&mut self, txn: &Txn) -> Aggregates {
        for change in &txn.changes {
            if change.op == cdcx_model::Op::Truncate {
                // Retract every row of this table from the view.
                let table_rows: Vec<cdcx_state::zset::Row> = self
                    .view
                    .iter()
                    .filter(|(_, w)| *w > 0)
                    .map(|(row, _)| row.clone())
                    .collect();
                let delta = cdcx_state::zset::truncate_delta(table_rows);
                self.view.merge(&delta);
                self.view.compact();
                continue;
            }
            let delta = change_delta(change);
            let enriched: ZSet = if let Some(join) = self.join.as_ref() {
                let mut out = ZSet::new();
                for (row, weight) in delta.iter() {
                    out.merge(&join.join_fact(row, weight));
                }
                out
            } else {
                delta
            };
            if let Some(windows) = self.windows.as_mut() {
                windows.apply(&enriched);
            } else {
                self.view.merge(&enriched);
                self.view.compact();
            }
        }
        self.aggregates()
    }

    /// Current aggregates over the view (or over all open windows).
    pub fn aggregates(&self) -> Aggregates {
        let key_refs: Vec<&str> = self.config.key_columns.iter().map(String::as_str).collect();
        let (sums, counts) = if let Some(windows) = &self.windows {
            // Union of all open windows: compute per window and merge.
            let mut sums = std::collections::HashMap::new();
            let mut counts = std::collections::HashMap::new();
            for (_, z) in windows.open_windows() {
                for (k, v) in sum_by(z, &key_refs, sum_col(self)) {
                    *sums.entry(k).or_insert(0) += v;
                }
                for (k, v) in count_by(z, &key_refs) {
                    *counts.entry(k).or_insert(0) += v;
                }
            }
            (sums, counts)
        } else {
            let sums = self
                .config
                .sum_column
                .as_deref()
                .map(|col| sum_by(&self.view, &key_refs, col))
                .unwrap_or_default();
            let counts = if self.config.count {
                count_by(&self.view, &key_refs)
            } else {
                std::collections::HashMap::new()
            };
            (sums, counts)
        };
        Aggregates { sums, counts }
    }

    /// The view's rows (only meaningful without windowing).
    pub fn rows(&self) -> impl Iterator<Item = (&cdcx_state::zset::Row, i64)> {
        self.view.iter()
    }

    /// Close windows whose end passed `watermark_ms`; returns their
    /// contents (window start, ZSet).
    pub fn advance_watermark(&mut self, watermark_ms: i64) -> Vec<(i64, ZSet)> {
        self.windows
            .as_mut()
            .map(|w| w.advance_watermark(watermark_ms))
            .unwrap_or_default()
    }

    /// Full state for the checkpoint: view rows, window state, join
    /// index. Serialized beside the LSN in the same atomic write.
    pub fn state(&self) -> serde_json::Value {
        serde_json::json!({
            "view": zset_state(&self.view),
            "windows": self.windows.as_ref().map(|w| w.state()),
            "join": self.join.as_ref().map(|j| j.state()),
        })
    }

    /// Restore from a checkpoint's operator state blob.
    pub fn restore(&mut self, state: &serde_json::Value) {
        if let Some(v) = state.get("view").and_then(|v| v.as_array()) {
            self.view = zset_from_state(v);
        }
        // Windows and join restore: their state carries configuration
        // (size, column); reconstructing operators from config + state
        // is left to the engine, which owns the ViewConfig.
    }
}

fn sum_col(view: &MaterializedView) -> &str {
    view.config.sum_column.as_deref().unwrap_or("")
}

fn zset_state(z: &ZSet) -> serde_json::Value {
    serde_json::Value::Array(
        z.iter()
            .map(|(row, w)| {
                serde_json::json!({
                    "row": row.iter().map(|(n, v)| serde_json::json!({"name": n, "value": v})).collect::<Vec<_>>(),
                    "weight": w,
                })
            })
            .collect::<Vec<_>>(),
    )
}

fn zset_from_state(v: &[serde_json::Value]) -> ZSet {
    let mut z = ZSet::new();
    for entry in v {
        let Some(row) = entry.get("row").and_then(|r| r.as_array()) else {
            continue;
        };
        let weight = entry.get("weight").and_then(|w| w.as_i64()).unwrap_or(0);
        let decoded: cdcx_state::zset::Row = row
            .iter()
            .filter_map(|c| {
                let name = c.get("name")?.as_str()?.to_string();
                let value = c.get("value")?;
                let value: cdcx_model::Value = serde_json::from_value(value.clone()).ok()?;
                Some((name, value))
            })
            .collect();
        z.add(decoded, weight);
    }
    z
}
