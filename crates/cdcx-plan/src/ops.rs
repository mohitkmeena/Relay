//! Plan document model and operator compilation.
//!
//! The YAML schema (pipeline.yaml):
//!
//! ```yaml
//! tables:
//!   public.users:
//!     - select: [id, name, email]
//!     - rename: { email: user_email }
//!     - redact: [email]
//!     - cast: { balance: float }
//!     - filter: { op: "=", column: active, value: true }
//! ```

use std::collections::HashMap;

use cdcx_model::{Change, Op, Value};
use serde::{Deserialize, Serialize};

use crate::filter::FilterExpr;

pub use crate::filter::FilterExpr as Filter;

/// Error raised while compiling a plan document.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    /// The YAML document could not be parsed.
    #[error("plan YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// An operator referenced a column that doesn't exist in the change.
    #[error("column {column:?} not found in table {table:?}")]
    UnknownColumn {
        /// The missing column name.
        column: String,
        /// The table being planned.
        table: String,
    },
}

/// The plan document, as written in pipeline.yaml.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Plan {
    /// Per-table operator lists, keyed by "namespace.table".
    #[serde(default)]
    pub tables: HashMap<String, Vec<OpSpec>>,
}

/// One operator as written in YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OpSpec {
    /// Keep only the listed columns.
    Select {
        /// Columns to keep, in order.
        columns: Vec<String>,
    },
    /// Rename columns.
    Rename {
        /// old name -> new name.
        from: HashMap<String, String>,
    },
    /// Convert a column's values.
    Cast {
        /// Column to convert.
        column: String,
        /// Target type name: "int", "float", "bool", "text".
        to: String,
    },
    /// Redact a column: replace with a fixed token.
    Redact {
        /// Columns to redact.
        columns: Vec<String>,
    },
    /// Filter rows; see [`crate::filter`].
    Filter {
        /// Filter expression tree.
        #[serde(flatten)]
        expr: FilterExpr,
    },
}

/// A compiled, ready-to-apply operator.
#[derive(Debug, Clone)]
pub enum PlanOp {
    /// Keep only the listed columns.
    Select(Vec<String>),
    /// Rename columns.
    Rename(HashMap<String, String>),
    /// Convert a column's values.
    Cast {
        /// Column to convert.
        column: String,
        /// Target type.
        to: CastType,
    },
    /// Redact columns to a fixed token.
    Redact(Vec<String>),
    /// Filter with enter/leave semantics.
    Filter(FilterExpr),
}

/// Target types for the cast operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastType {
    /// `Value::Int`.
    Int,
    /// `Value::Float`.
    Float,
    /// `Value::Bool`.
    Bool,
    /// `Value::Text`.
    Text,
}

/// Compile a plan document into a table -> operators map.
pub fn compile(plan: &Plan) -> Result<HashMap<String, Vec<PlanOp>>, PlanError> {
    let mut tables = HashMap::new();
    for (table, specs) in &plan.tables {
        let mut ops = Vec::with_capacity(specs.len());
        for spec in specs {
            ops.push(match spec {
                OpSpec::Select { columns } => PlanOp::Select(columns.clone()),
                OpSpec::Rename { from } => PlanOp::Rename(from.clone()),
                OpSpec::Cast { column, to } => PlanOp::Cast {
                    column: column.clone(),
                    to: match to.as_str() {
                        "int" => CastType::Int,
                        "float" => CastType::Float,
                        "bool" => CastType::Bool,
                        "text" => CastType::Text,
                        other => {
                            return Err(PlanError::UnknownColumn {
                                column: other.to_string(),
                                table: table.clone(),
                            });
                        }
                    },
                },
                OpSpec::Redact { columns } => PlanOp::Redact(columns.clone()),
                OpSpec::Filter { expr } => PlanOp::Filter(expr.clone()),
            });
        }
        tables.insert(table.clone(), ops);
    }
    Ok(tables)
}

impl PlanOp {
    /// Apply this operator to a change, in place.
    ///
    /// Mutates `after` (and `before` where it makes sense) and may turn
    /// the change into a tombstone (see the filter operator).
    pub fn apply(&self, change: &mut Change) {
        match self {
            PlanOp::Select(columns) => {
                if let Some(after) = change.after.as_mut() {
                    after.retain(|(name, _)| columns.contains(name));
                }
                if let Some(before) = change.before.as_mut() {
                    before.retain(|(name, _)| columns.contains(name));
                }
            }
            PlanOp::Rename(from) => {
                if let Some(after) = change.after.as_mut() {
                    for (name, _) in after.iter_mut() {
                        if let Some(new) = from.get(name) {
                            *name = new.clone();
                        }
                    }
                }
                if let Some(before) = change.before.as_mut() {
                    for (name, _) in before.iter_mut() {
                        if let Some(new) = from.get(name) {
                            *name = new.clone();
                        }
                    }
                }
            }
            PlanOp::Cast { column, to } => {
                if let Some(after) = change.after.as_mut() {
                    for (name, value) in after.iter_mut() {
                        if name == column {
                            *value = cast_value(value, *to);
                        }
                    }
                }
            }
            PlanOp::Redact(columns) => {
                if let Some(after) = change.after.as_mut() {
                    for (name, value) in after.iter_mut() {
                        if columns.contains(name) {
                            *value = Value::Text("***".into());
                        }
                    }
                }
            }
            PlanOp::Filter(expr) => {
                let before_matched = change
                    .before
                    .as_ref()
                    .map(|b| expr.matches(b))
                    .unwrap_or(false);
                let after_matched = change
                    .after
                    .as_ref()
                    .map(|a| expr.matches(a))
                    .unwrap_or(false);
                // Enter/leave/update semantics:
                //   F->F: drop entirely (row never visible)
                //   F->T: keep as insert (row enters the view)
                //   T->T: keep as update
                //   T->F: convert to tombstone (row leaves the view)
                match (before_matched, after_matched) {
                    (false, false) => {
                        change.after = None;
                        change.before = None;
                        change.op = Op::Delete;
                    }
                    (true, false) => {
                        change.after = None;
                        change.op = Op::Delete;
                    }
                    (false, true) | (true, true) => {}
                }
            }
        }
    }
}

fn cast_value(value: &Value, to: CastType) -> Value {
    match (value, to) {
        (Value::Null, _) => Value::Null,
        (v, CastType::Text) => Value::Text(v.to_string()),
        (Value::Text(s), CastType::Int) => {
            s.parse().map(Value::Int).unwrap_or(Value::Text(s.clone()))
        }
        (Value::Numeric(s), CastType::Int) => s
            .parse()
            .map(Value::Int)
            .unwrap_or(Value::Numeric(s.clone())),
        (Value::Int(i), CastType::Int) => Value::Int(*i),
        (Value::Text(s), CastType::Float) => s
            .parse::<f64>()
            .map(|x| Value::Float(cdcx_model::F64(x)))
            .unwrap_or(Value::Text(s.clone())),
        (Value::Int(i), CastType::Float) => Value::Float(cdcx_model::F64(*i as f64)),
        (Value::Float(x), CastType::Float) => Value::Float(*x),
        (Value::Text(s), CastType::Bool) => match s.as_str() {
            "t" | "true" | "T" | "TRUE" => Value::Bool(true),
            "f" | "false" | "F" | "FALSE" => Value::Bool(false),
            _ => Value::Text(s.clone()),
        },
        (Value::Bool(b), CastType::Bool) => Value::Bool(*b),
        // Already the target type, or an incoherent cast: pass through.
        (v, _) => v.clone(),
    }
}
