//! Core data model for cdcx.
//!
//! This crate is intentionally free of I/O and async. It defines the
//! [`Change`], [`Txn`], and [`Value`] types that flow between the source,
//! plan, and sink stages of the pipeline, so every other crate can depend
//! on it without pulling in a database driver.
//!
//! Design note: the TOAST "unchanged" marker gets its own
//! [`Value::Unchanged`] variant. The compiler then forces every operator
//! that consumes a [`Value`] to decide what to do with it, instead of it
//! hiding inside `Option<Value>` where it can be silently dropped.

use std::fmt;

/// A hashable wrapper around `f64`, comparing by total bit pattern so
/// `Value` can key hash maps (Z-set rows, group keys). Note: `-0.0` and
/// `0.0` hash differently; that's acceptable for row identity since
/// PostgreSQL never produces `-0.0` from numeric/float text output.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct F64(pub f64);

impl PartialEq for F64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for F64 {}

impl std::hash::Hash for F64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl From<f64> for F64 {
    fn from(x: f64) -> Self {
        Self(x)
    }
}

impl std::fmt::Display for F64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A single decoded column value from a replication tuple.
///
/// `Unchanged` represents the pgoutput TOAST marker (`'u'`): the value was
/// not changed by this update and was not sent. Callers must handle it
/// explicitly — usually by substituting the previous row image's value.
#[derive(Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Value {
    /// PostgreSQL `NULL`.
    Null,
    /// The TOAST "unchanged" marker: value not sent for this change.
    Unchanged,
    /// A UTF-8 text value, decoded per the column's type OID.
    Text(String),
    /// An integer decoded from text format (int2/int4/int8).
    Int(i64),
    /// A boolean decoded from text format (`'t'`/`'f'`).
    Bool(bool),
    /// A floating-point number decoded from text format (float4/float8).
    Float(F64),
    /// A numeric value, kept as its text representation to avoid lossy
    /// conversion. A later milestone may swap this for `rust_decimal`.
    Numeric(String),
    /// A UTC timestamp in ISO-8601, decoded from text format.
    Timestamp(String),
    /// JSON stored as text; parsed lazily by operators that need structure.
    Json(String),
    /// A value of a type we do not decode specifically; kept as raw text.
    Raw(String),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Unchanged => write!(f, "UNCHANGED"),
            Value::Text(s)
            | Value::Numeric(s)
            | Value::Timestamp(s)
            | Value::Json(s)
            | Value::Raw(s) => write!(f, "{s:?}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Float(x) => write!(f, "{x}"),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Unchanged => f.write_str("UNCHANGED"),
            Value::Text(s)
            | Value::Numeric(s)
            | Value::Timestamp(s)
            | Value::Json(s)
            | Value::Raw(s) => f.write_str(s),
            Value::Int(i) => write!(f, "{i}"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Float(x) => write!(f, "{x}"),
        }
    }
}

/// The kind of row change, one per row-level pgoutput message.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum Op {
    /// Row inserted.
    Insert,
    /// Row updated. Both old and new images are present when the table has
    /// `REPLICA IDENTITY FULL`; otherwise the old image carries only the key
    /// columns.
    Update,
    /// Row deleted.
    Delete,
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Op::Insert => f.write_str("INSERT"),
            Op::Update => f.write_str("UPDATE"),
            Op::Delete => f.write_str("DELETE"),
        }
    }
}

/// A decoded row-level change, the atomic unit of the pipeline.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Change {
    /// Table namespace, e.g. `"public"`.
    pub namespace: String,
    /// Table name.
    pub table: String,
    /// Kind of change.
    pub op: Op,
    /// Column values of the new row image (after). `None` for deletes.
    pub after: Option<Vec<(String, Value)>>,
    /// Column values of the old row image (before). Present for updates and
    /// (with full identity) deletes.
    pub before: Option<Vec<(String, Value)>>,
    /// Per-change LSN from the WAL, used for ordering.
    pub lsn: u64,
    /// Position of this change within its transaction.
    pub index_in_txn: u32,
}

impl Change {
    /// Return the primary-key or identity columns of the change as name/value
    /// pairs, taken from the new image when present, else the old image.
    ///
    /// Until the plan stage provides per-table key configuration, every
    /// column of the image is returned; tables with `REPLICA IDENTITY FULL`
    /// therefore round-trip correctly, and narrower identities degrade to
    /// whatever columns the old image carries.
    pub fn key(&self) -> &[(String, Value)] {
        match (&self.after, &self.before) {
            (Some(a), _) => a.as_slice(),
            (None, Some(b)) => b.as_slice(),
            (None, None) => &[],
        }
    }
}

/// A committed transaction's changes, normalized for downstream stages.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Txn {
    /// LSN of the transaction's commit record; the watermark for acks.
    pub commit_lsn: u64,
    /// Changes in commit order.
    pub changes: Vec<Change>,
}
