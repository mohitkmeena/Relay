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
    /// Per-table primary key columns, keyed by "namespace.table".
    /// Tables without an entry use every column of the row image as
    /// the key (safe under REPLICA IDENTITY FULL).
    #[serde(default)]
    pub keys: HashMap<String, Vec<String>>,
    /// Sink configuration. `None` = console.
    #[serde(default)]
    pub sink: Option<SinkSpec>,
}

/// Where to deliver changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkSpec {
    /// Print to stdout (default).
    Console,
    /// POST to a webhook.
    Http {
        /// Webhook URL.
        url: String,
    },
    /// Insert into ClickHouse.
    Clickhouse {
        /// HTTP endpoint, e.g. "http://localhost:8123".
        url: String,
        /// Target database.
        database: String,
    },
    /// Deliver to every listed sink; all must confirm.
    Fanout {
        /// Inner sinks.
        sinks: Vec<SinkSpec>,
    },
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
    /// Compute a derived column. `expr` supports the same comparison
    /// literals as filters plus `"left"`/`"right"` references to
    /// columns and `"left+right"` arithmetic on ints.
    Map {
        /// New column name.
        column: String,
        /// Expression producing its value.
        expr: String,
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
    /// Compute a derived column from a small expression language:
    /// column references, integer literals, and `+`/`-`/`*` between
    /// int-typed operands. The result column overwrites on collision.
    Map {
        /// New column name.
        column: String,
        /// Parsed expression.
        expr: MapExpr,
    },
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
                OpSpec::Map { column, expr } => PlanOp::Map {
                    column: column.clone(),
                    expr: MapExpr::parse(expr)
                        .ok_or_else(|| PlanError::UnknownColumn {
                            column: expr.clone(),
                            table: table.clone(),
                        })?,
                },
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
            PlanOp::Map { column, expr } => {
                if let Some(after) = change.after.as_mut() {
                    if let Some(value) = expr.eval(after) {
                        if let Some(existing) =
                            after.iter_mut().find(|(name, _)| name == column)
                        {
                            existing.1 = value;
                        } else {
                            after.push((column.clone(), value));
                        }
                    }
                }
            }
        }
    }
}

/// A tiny arithmetic expression for the map operator.
///
/// Grammar: `term ( (+|-) term )*`, `term = factor ( * factor )*`,
/// `factor = <int literal> | <column name>`. Whitespace-insensitive.
/// Evaluation is over `Value::Int` only; anything else yields `None`
/// and the operator skips the column (no partial garbage).
#[derive(Debug, Clone, PartialEq)]
pub enum MapExpr {
    /// Integer literal.
    Lit(i64),
    /// Reference to a column.
    Column(String),
    /// `left + right`.
    Add(Box<MapExpr>, Box<MapExpr>),
    /// `left - right`.
    Sub(Box<MapExpr>, Box<MapExpr>),
    /// `left * right`.
    Mul(Box<MapExpr>, Box<MapExpr>),
}

impl MapExpr {
    /// Parse an expression string. `None` on syntax errors.
    pub fn parse(src: &str) -> Option<MapExpr> {
        let tokens: Vec<&str> = tokenize(src)?;
        let mut pos = 0;
        let expr = parse_add(&tokens, &mut pos)?;
        if pos != tokens.len() {
            return None; // trailing garbage
        }
        Some(expr)
    }

    /// Evaluate against a row; `None` if any operand is non-int.
    pub fn eval(&self, row: &[(String, Value)]) -> Option<Value> {
        Some(Value::Int(self.eval_int(row)?))
    }

    fn eval_int(&self, row: &[(String, Value)]) -> Option<i64> {
        match self {
            MapExpr::Lit(i) => Some(*i),
            MapExpr::Column(name) => match row
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v)?
            {
                Value::Int(i) => Some(*i),
                _ => None,
            },
            MapExpr::Add(a, b) => Some(a.eval_int(row)? + b.eval_int(row)?),
            MapExpr::Sub(a, b) => Some(a.eval_int(row)? - b.eval_int(row)?),
            MapExpr::Mul(a, b) => Some(a.eval_int(row)? * b.eval_int(row)?),
        }
    }
}

fn tokenize(src: &str) -> Option<Vec<&str>> {
    let mut tokens = Vec::new();
    let mut rest = src.trim();
    while !rest.is_empty() {
        let first = rest.as_bytes()[0];
        if first == b' ' || first == b'\t' {
            rest = &rest[1..];
        } else if first == b'+' || first == b'-' || first == b'*' {
            tokens.push(&rest[..1]);
            rest = &rest[1..];
        } else {
            // Identifier or int literal: longest run of [A-Za-z0-9_]
            let end = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            if end == 0 {
                return None; // unexpected character
            }
            tokens.push(&rest[..end]);
            rest = &rest[end..];
        }
    }
    Some(tokens)
}

fn parse_add(tokens: &[&str], pos: &mut usize) -> Option<MapExpr> {
    let mut left = parse_mul(tokens, pos)?;
    loop {
        // Running out of tokens ends the expression, not a failure.
        let Some(op) = tokens.get(*pos).copied() else {
            return Some(left);
        };
        if op == "+" {
            *pos += 1;
            let right = parse_mul(tokens, pos)?;
            left = MapExpr::Add(Box::new(left), Box::new(right));
        } else if op == "-" {
            *pos += 1;
            let right = parse_mul(tokens, pos)?;
            left = MapExpr::Sub(Box::new(left), Box::new(right));
        } else {
            return Some(left);
        }
    }
}

fn parse_mul(tokens: &[&str], pos: &mut usize) -> Option<MapExpr> {
    let mut left = parse_factor(tokens, pos)?;
    loop {
        // Running out of tokens ends the expression, not a failure.
        let Some(op) = tokens.get(*pos).copied() else {
            return Some(left);
        };
        if op == "*" {
            *pos += 1;
            let right = parse_factor(tokens, pos)?;
            left = MapExpr::Mul(Box::new(left), Box::new(right));
        } else {
            return Some(left);
        }
    }
}

fn parse_factor(tokens: &[&str], pos: &mut usize) -> Option<MapExpr> {
    let token = tokens.get(*pos).copied()?;
    *pos += 1;
    if let Ok(i) = token.parse::<i64>() {
        Some(MapExpr::Lit(i))
    } else if token
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        Some(MapExpr::Column(token.to_string()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_expr_parse_and_eval() {
        let expr = MapExpr::parse("balance * 2 + 1").unwrap();
        let row = vec![
            ("id".to_string(), Value::Int(1)),
            ("balance".to_string(), Value::Int(10)),
        ];
        assert_eq!(expr.eval(&row), Some(Value::Int(21)));

        // Non-int operand: skipped, not garbage.
        let row = vec![("balance".to_string(), Value::Text("x".into()))];
        assert_eq!(expr.eval(&row), None);

        // Trailing garbage rejected.
        assert!(MapExpr::parse("a +").is_none());
        assert!(MapExpr::parse("a $ b").is_none());
    }

    #[test]
    fn map_operator_writes_column() {
        let op = PlanOp::Map {
            column: "double_balance".into(),
            expr: MapExpr::parse("balance * 2").unwrap(),
        };
        let mut change = Change {
            namespace: "public".into(),
            table: "users".into(),
            op: Op::Insert,
            after: Some(vec![
                ("id".to_string(), Value::Int(1)),
                ("balance".to_string(), Value::Int(5)),
            ]),
            before: None,
            lsn: 1,
            index_in_txn: 0,
        };
        op.apply(&mut change);
        let after = change.after.unwrap();
        assert_eq!(
            after.iter().find(|(n, _)| n == "double_balance").map(|(_, v)| v.clone()),
            Some(Value::Int(10))
        );
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
