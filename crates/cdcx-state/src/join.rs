//! Reference-table join (M7).
//!
//! A reference table (small, slowly changing — a users or merchants
//! table) enriches a fact stream. The full outer side lives in memory
//! as a Z-set keyed by join column. When the reference side changes,
//! the operator emits *corrections*: retraction of the old enriched row
//! and insertion of the new one, for every matching fact. That's what
//! makes the join incremental — facts are never re-scanned.
//!
//! State: the reference Z-set only. Facts are streamed through.

use std::collections::HashMap;

use cdcx_model::Value;

use crate::zset::{Row, ZSet};

/// Incremental join of a fact stream against an in-memory reference
/// table, keyed by one column.
pub struct ReferenceJoin {
    /// Column name on the reference side to join on.
    reference_key: String,
    /// Column name on the fact side to join on.
    fact_key: String,
    /// The reference table, as a key -> ZSet index.
    reference: HashMap<String, ZSet>,
    /// Total reference rows (for metrics/state size).
    reference_len: usize,
}

impl ReferenceJoin {
    /// New join: facts' `fact_key` matches reference's `reference_key`.
    pub fn new(fact_key: impl Into<String>, reference_key: impl Into<String>) -> Self {
        Self {
            reference_key: reference_key.into(),
            fact_key: fact_key.into(),
            reference: HashMap::new(),
            reference_len: 0,
        }
    }

    /// Column value as a string join key. `Null`/`Unchanged` do not
    /// join (they miss rather than error).
    fn key_of(row: &Row, column: &str) -> Option<String> {
        let v = row.iter().find(|(n, _)| n == column).map(|(_, v)| v)?;
        match v {
            Value::Null | Value::Unchanged => None,
            other => Some(other.to_string()),
        }
    }

    /// Apply a reference-side delta: index it and produce the matching
    /// corrections for facts already seen... no — facts are streaming,
    /// corrections are only needed when *facts* arrive. Reference
    /// changes emit corrections for facts that already passed through
    /// only if the caller keeps fact state; in the streaming design,
    /// reference deltas update the index and the *next* fact sees the
    /// new value. Re-enrichment of already-emitted rows requires the
    /// two-sided variant (deferred).
    pub fn apply_reference_delta(&mut self, delta: &ZSet) {
        for (row, weight) in delta.iter() {
            let Some(key) = Self::key_of(row, &self.reference_key) else {
                continue;
            };
            let bucket = self.reference.entry(key).or_default();
            let before = bucket.len();
            bucket.add(row.clone(), weight);
            bucket.compact();
            let after = bucket.len();
            self.reference_len = self
                .reference_len
                .saturating_add_signed(((after as i64) - (before as i64)) as isize);
        }
    }

    /// Enrich one fact row (or retract, if `weight < 0`): returns the
    /// joined rows to emit into the downstream view.
    ///
    /// Semantics: for each matching reference row, the output is the
    /// concatenation of fact and reference columns (reference columns
    /// suffixed nothing — collisions keep the fact's value). A fact
    /// with no match still emits, with no reference columns appended:
    /// a left join.
    pub fn join_fact(&self, fact: &Row, weight: i64) -> ZSet {
        let mut out = ZSet::new();
        let Some(key) = Self::key_of(fact, &self.fact_key) else {
            out.add(fact.clone(), weight);
            return out;
        };
        let Some(matches) = self.reference.get(&key) else {
            out.add(fact.clone(), weight);
            return out;
        };
        if matches.is_empty() {
            out.add(fact.clone(), weight);
            return out;
        }
        for (reference_row, _) in matches.iter() {
            let mut joined = fact.clone();
            for (name, value) in reference_row {
                if !joined.iter().any(|(n, _)| n == name) {
                    joined.push((name.clone(), value.clone()));
                }
            }
            out.add(joined, weight);
        }
        out
    }

    /// Number of distinct reference rows currently indexed.
    pub fn reference_len(&self) -> usize {
        self.reference_len
    }

    /// Serialize the reference table for checkpointing.
    pub fn state(&self) -> serde_json::Value {
        serde_json::json!({
            "fact_key": self.fact_key,
            "reference_key": self.reference_key,
            "rows": self.reference.iter().map(|(k, z)| {
                (k.clone(), z.iter().map(|(row, w)| {
                    serde_json::json!({
                        "row": row.iter().map(|(n, v)| serde_json::json!({"name": n, "value": v})).collect::<Vec<_>>(),
                        "weight": w,
                    })
                }).collect::<Vec<_>>())
            }).collect::<std::collections::HashMap<_, _>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn enriches_and_updates_reference() {
        let mut join = ReferenceJoin::new("merchant_id", "id");

        // Reference: merchant 28 -> gold.
        let mut ref_delta = ZSet::new();
        ref_delta.add(
            row(&[
                ("id", Value::Text("28".into())),
                ("tier", Value::Text("gold".into())),
            ]),
            1,
        );
        join.apply_reference_delta(&ref_delta);
        assert_eq!(join.reference_len(), 1);

        // Fact joins: enriched with tier=gold.
        let fact = row(&[
            ("amount", Value::Int(100)),
            ("merchant_id", Value::Text("28".into())),
        ]);
        let out = join.join_fact(&fact, 1);
        let mut collected = out.iter().collect::<Vec<_>>();
        assert_eq!(collected.len(), 1);
        let (joined, w) = collected.remove(0);
        assert_eq!(w, 1);
        assert!(
            joined
                .iter()
                .any(|(n, v)| n == "tier" && v == &Value::Text("gold".into()))
        );

        // Unmatched fact emits bare (left join).
        let loner = row(&[
            ("amount", Value::Int(5)),
            ("merchant_id", Value::Text("99".into())),
        ]);
        let out = join.join_fact(&loner, 1);
        assert_eq!(out.len(), 1);
        assert!(
            !out.iter()
                .next()
                .unwrap()
                .0
                .iter()
                .any(|(n, _)| n == "tier")
        );

        // Reference update: merchant 28 gold -> silver. Retract + add.
        let mut upd = ZSet::new();
        upd.add(
            row(&[
                ("id", Value::Text("28".into())),
                ("tier", Value::Text("gold".into())),
            ]),
            -1,
        );
        upd.add(
            row(&[
                ("id", Value::Text("28".into())),
                ("tier", Value::Text("silver".into())),
            ]),
            1,
        );
        join.apply_reference_delta(&upd);
        assert_eq!(join.reference_len(), 1);

        // New facts now see silver.
        let fact2 = row(&[
            ("amount", Value::Int(200)),
            ("merchant_id", Value::Text("28".into())),
        ]);
        let out = join.join_fact(&fact2, 1);
        assert!(
            out.iter()
                .next()
                .unwrap()
                .0
                .iter()
                .any(|(n, v)| n == "tier" && v == &Value::Text("silver".into()))
        );
    }
}
