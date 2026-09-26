//! Filter expressions with enter/leave/update semantics.
//!
//! Expressions are written in YAML as nested maps and evaluated against
//! a row image (a slice of `(column, value)` pairs). Evaluation is total:
//! any comparison involving `Null` or mismatched types is false rather
//! than an error, so a filter never crashes the pipeline mid-transaction.

use cdcx_model::Value;
use serde::{Deserialize, Serialize};

/// A boolean expression tree over row columns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FilterExpr {
    /// `column = value`.
    Eq {
        /// Column to compare.
        column: String,
        /// Literal to compare against.
        value: serde_json::Value,
    },
    /// `column != value`.
    Ne {
        /// Column to compare.
        column: String,
        /// Literal to compare against.
        value: serde_json::Value,
    },
    /// `column < value`.
    Lt {
        /// Column to compare.
        column: String,
        /// Literal to compare against.
        value: serde_json::Value,
    },
    /// `column >= value` (exclusive lower bound is `lt`).
    Gt {
        /// Column to compare.
        column: String,
        /// Literal to compare against.
        value: serde_json::Value,
    },
    /// `column <= value`.
    Le {
        /// Column to compare.
        column: String,
        /// Literal to compare against.
        value: serde_json::Value,
    },
    /// `column >= value`.
    Ge {
        /// Column to compare.
        column: String,
        /// Literal to compare against.
        value: serde_json::Value,
    },
    /// All sub-expressions true.
    And {
        /// Sub-expressions.
        exprs: Vec<FilterExpr>,
    },
    /// Any sub-expression true.
    Or {
        /// Sub-expressions,
        exprs: Vec<FilterExpr>,
    },
    /// Sub-expression false.
    Not {
        /// Sub-expression.
        expr: Box<FilterExpr>,
    },
}

impl FilterExpr {
    /// Evaluate against a row image. Missing columns and type mismatches
    /// evaluate to false.
    pub fn matches(&self, row: &[(String, Value)]) -> bool {
        match self {
            FilterExpr::Eq { column, value } => {
                compare(row, column, value, |o| o == Ordering::Equal)
            }
            FilterExpr::Ne { column, value } => {
                compare(row, column, value, |o| o != Ordering::Equal)
            }
            FilterExpr::Lt { column, value } => {
                compare(row, column, value, |o| o == Ordering::Less)
            }
            FilterExpr::Gt { column, value } => {
                compare(row, column, value, |o| o == Ordering::Greater)
            }
            FilterExpr::Le { column, value } => {
                compare(row, column, value, |o| o != Ordering::Greater)
            }
            FilterExpr::Ge { column, value } => {
                compare(row, column, value, |o| o != Ordering::Less)
            }
            FilterExpr::And { exprs } => exprs.iter().all(|e| e.matches(row)),
            FilterExpr::Or { exprs } => exprs.iter().any(|e| e.matches(row)),
            FilterExpr::Not { expr } => !expr.matches(row),
        }
    }
}

/// Ordering used by comparisons: total over comparable pairs, with a
/// `Mixed` for incomparable (mismatched types / nulls).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ordering {
    /// Less than.
    Less,
    /// Equal.
    Equal,
    /// Greater than.
    Greater,
    /// Null involved or types not comparable.
    Mixed,
}

fn compare(
    row: &[(String, Value)],
    column: &str,
    literal: &serde_json::Value,
    ok: impl Fn(Ordering) -> bool,
) -> bool {
    let Some((_, value)) = row.iter().find(|(name, _)| name == column) else {
        return false;
    };
    ok(ordering(value, literal))
}

fn ordering(value: &Value, literal: &serde_json::Value) -> Ordering {
    use serde_json::Value as J;
    match (value, literal) {
        (Value::Null, _) | (_, J::Null) => Ordering::Mixed,
        (Value::Int(i), J::Number(n)) => n
            .as_i64()
            .map(|l| int_order(*i, l))
            .unwrap_or(Ordering::Mixed),
        (Value::Int(i), J::Bool(b)) => int_order(*i, *b as i64),
        (Value::Float(x), J::Number(n)) => n
            .as_f64()
            .map(|l| {
                if x.0 < l {
                    Ordering::Less
                } else if x.0 > l {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            })
            .unwrap_or(Ordering::Mixed),
        (Value::Bool(b), J::Bool(l)) => {
            if b == l {
                Ordering::Equal
            } else {
                Ordering::Mixed
            }
        }
        (Value::Text(s) | Value::Numeric(s), J::String(l)) => str_order(s, l),
        (Value::Text(s), J::Number(n)) => {
            // Text vs number: try numeric interpretation for convenience.
            if let (Ok(a), Some(b)) = (s.parse::<f64>(), n.as_f64()) {
                if a < b {
                    Ordering::Less
                } else if a > b {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            } else {
                Ordering::Mixed
            }
        }
        _ => Ordering::Mixed,
    }
}

fn int_order(a: i64, b: i64) -> Ordering {
    if a < b {
        Ordering::Less
    } else if a > b {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

fn str_order(a: &str, b: &str) -> Ordering {
    match a.cmp(b) {
        std::cmp::Ordering::Less => Ordering::Less,
        std::cmp::Ordering::Equal => Ordering::Equal,
        std::cmp::Ordering::Greater => Ordering::Greater,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> Vec<(String, Value)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn active_true() -> FilterExpr {
        FilterExpr::Eq {
            column: "active".into(),
            value: serde_json::json!(true),
        }
    }

    #[test]
    fn all_four_filter_transitions() {
        // F -> F: dropped
        let mut change = test_change(false, false);
        apply_filter(&mut change);
        assert!(change.after.is_none());
        assert!(change.before.is_none());

        // F -> T: kept as insert (enters view)
        let mut change = test_change(false, true);
        apply_filter(&mut change);
        assert!(change.after.is_some());

        // T -> T: kept as update
        let mut change = test_change(true, true);
        apply_filter(&mut change);
        assert!(change.after.is_some());

        // T -> F: tombstone (leaves view)
        let mut change = test_change(true, false);
        apply_filter(&mut change);
        assert!(change.after.is_none());
        assert!(change.before.is_some());
    }

    fn apply_filter(change: &mut cdcx_model::Change) {
        crate::ops::PlanOp::Filter(active_true()).apply(change);
    }

    fn test_change(before_active: bool, after_active: bool) -> cdcx_model::Change {
        cdcx_model::Change {
            namespace: "public".into(),
            table: "users".into(),
            op: cdcx_model::Op::Update,
            after: Some(row(&[
                ("id", Value::Int(1)),
                ("active", Value::Bool(after_active)),
            ])),
            before: Some(row(&[
                ("id", Value::Int(1)),
                ("active", Value::Bool(before_active)),
            ])),
            lsn: 1,
            index_in_txn: 0,
        }
    }
}
