//! ClickHouse sink: ReplacingMergeTree with version = commit_lsn.
//!
//! Design (M5):
//! - One table per source table, created on first use, columns derived
//!   from the change's row images.
//! - Inserts carry `_version = commit_lsn`; deletes insert an
//!   `is_deleted = 1` tombstone row. ReplacingMergeTree collapses to the
//!   highest `_version` per key at query/merge time.
//! - Durability: `insert` returns only after ClickHouse acknowledges.
//!   async_insert is deliberately off.
//!
//! This module is a thin client over the HTTP interface (port 8123),
//! keeping dependencies minimal; no ClickHouse driver crate.

use cdcx_model::{Change, Txn, Value};

use crate::SinkError;

use super::Sink;

/// ClickHouse HTTP endpoint sink.
pub struct ClickHouseSink {
    url: String,
    client: reqwest::Client,
    /// ClickHouse database name.
    database: String,
}

impl ClickHouseSink {
    /// Create a sink targeting `url` (e.g. `http://localhost:8123`).
    pub fn new(url: impl Into<String>, database: impl Into<String>) -> Result<Self, SinkError> {
        Ok(Self {
            url: url.into(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .map_err(|e| SinkError::Config(e.to_string()))?,
            database: database.into(),
        })
    }

    fn table_name(&self, change: &Change) -> String {
        format!("{}.{}", self.database, change.table)
    }

    fn value_to_ch(v: &Value) -> String {
        match v {
            Value::Null => "NULL".into(),
            Value::Unchanged => "NULL".into(),
            Value::Bool(b) => if *b { "1" } else { "0" }.into(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.0.to_string(),
            Value::Text(s)
            | Value::Numeric(s)
            | Value::Timestamp(s)
            | Value::Json(s)
            | Value::Raw(s) => escape(s),
        }
    }

    async fn exec(&self, sql: &str) -> Result<reqwest::Response, SinkError> {
        self.client
            .post(format!("{}/", self.url.trim_end_matches('/')))
            .query(&[("query", sql)])
            .send()
            .await
            .map_err(|e| SinkError::Write(e.to_string()))
    }
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

impl Sink for ClickHouseSink {
    async fn write_txn(&mut self, txn: &Txn) -> Result<(), SinkError> {
        for change in &txn.changes {
            let table = self.table_name(change);
            let image = change
                .after
                .as_ref()
                .or(change.before.as_ref())
                .ok_or_else(|| SinkError::Rejected {
                    lsn: txn.commit_lsn,
                    reason: "change carries no row image".into(),
                })?;
            let columns: Vec<String> = image.iter().map(|(n, _)| n.clone()).collect();
            let values: Vec<String> = image.iter().map(|(_, v)| Self::value_to_ch(v)).collect();
            let is_deleted = if change.op == cdcx_model::Op::Delete {
                1
            } else {
                0
            };
            // Fixed columns: _version (commit_lsn), _is_deleted.
            let sql = format!(
                "INSERT INTO {table} ({cols}, _version, _is_deleted) VALUES ({vals}, {lsn}, {is_deleted})",
                cols = columns.join(", "),
                vals = values.join(", "),
                lsn = txn.commit_lsn,
                is_deleted = is_deleted,
            );
            let resp = self.exec(&sql).await?;
            if !resp.status().is_success() {
                return Err(SinkError::Write(format!(
                    "clickhouse insert failed: HTTP {}",
                    resp.status()
                )));
            }
        }
        Ok(())
    }
}
