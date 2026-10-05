//! YAML pipeline plans compiled into pure transformation operators.
//!
//! A plan is a per-table list of operators — select, rename, cast, map,
//! redact, filter — applied left to right to each [`Change`]. Operators
//! are pure, synchronous functions: no I/O, no async, which makes the
//! whole crate exhaustively unit-testable and keeps it usable both in
//! the engine and in tools (a `cdcx plan check` subcommand, say).
//!
//! The filter operator implements enter/leave/update semantics: a row
//! moving from match to no-match produces a tombstone (`after = None`)
//! rather than being silently dropped, so downstream materialized views
//! can retract it. That's the foundation M7 builds on.

pub mod filter;
pub mod ops;

pub use filter::FilterExpr;
pub use ops::{CastType, MapExpr, OpSpec, Plan, PlanError, PlanOp, SinkSpec, compile};

use cdcx_model::Change;

/// A compiled pipeline plan: apply per table, in order.
#[derive(Debug, Clone, Default)]
pub struct Compiled {
    /// Operator lists keyed by "namespace.table". A missing key means
    /// "pass through unchanged".
    pub tables: std::collections::HashMap<String, Vec<PlanOp>>,
    /// Per-table key columns, keyed by "namespace.table".
    pub keys: std::collections::HashMap<String, Vec<String>>,
}

impl Compiled {
    /// Compile from a [`Plan`] document.
    pub fn compile(plan: &Plan) -> Result<Self, PlanError> {
        Ok(Self {
            tables: compile(plan)?,
            keys: plan.keys.clone(),
        })
    }

    /// Apply the plan's operators for this change's table, if any.
    pub fn apply(&self, change: &mut Change) {
        let key = format!("{}.{}", change.namespace, change.table);
        if let Some(ops) = self.tables.get(&key) {
            for op in ops {
                op.apply(change);
            }
        }
    }

    /// The configured key columns for a table, or `None` to use the
    /// full row image (the REPLICA IDENTITY FULL default).
    pub fn key_columns(&self, namespace: &str, table: &str) -> Option<&[String]> {
        self.keys
            .get(&format!("{namespace}.{table}"))
            .map(Vec::as_slice)
    }
}
