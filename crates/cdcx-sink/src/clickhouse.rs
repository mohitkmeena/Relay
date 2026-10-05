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
    /// Tables whose DDL has already been ensured (per process).
    created: std::collections::HashSet<String>,
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
            created: std::collections::HashSet::new(),
        })
    }

    /// Map a cdcx value type to a ClickHouse column type.
    fn ch_type(v: &Value) -> &'static str {
        match v {
            Value::Null | Value::Unchanged => "Nullable(String)",
            Value::Bool(_) => "Bool",
            Value::Int(_) => "Int64",
            Value::Float(_) => "Float64",
            Value::Text(_)
            | Value::Numeric(_)
            | Value::Timestamp(_)
            | Value::Json(_)
            | Value::Raw(_) => "String",
        }
    }

    /// Ensure the target table exists (ReplacingMergeTree keyed by
    /// `_version`). Best-effort: a race between two writers creating
    /// the same table is tolerated (CREATE TABLE IF NOT EXISTS).
    async fn ensure_table(&mut self, change: &Change) -> Result<(), SinkError> {
        let table = format!("{}.{}", self.database, change.table);
        if self.created.contains(&table) {
            return Ok(());
        }
        let Some(image) = change.after.as_ref().or(change.before.as_ref()) else {
            return Ok(());
        };
        // All value columns as String-ish except typed ones; key on the
        // first column (the convention: key columns come first in the
        // relation; refine when plans carry explicit keys).
        let columns_ddl = image
            .iter()
            .map(|(name, value)| format!("\"{name}\" {}", Self::ch_type(value)))
            .collect::<Vec<_>>()
            .join(", ");
        let key_col = image
            .first()
            .map(|(name, _)| name.clone())
            .unwrap_or_else(|| "_key".into());
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {table} ({columns_ddl}, \
             _version UInt64, _is_deleted UInt8) \
             ENGINE = ReplacingMergeTree(_version) ORDER BY (\"{key_col}\")"
        );
        let resp = self.exec(&ddl).await?;
        if !resp.status().is_success() {
            return Err(SinkError::Write(format!(
                "clickhouse DDL failed: HTTP {} — {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }
        self.created.insert(table);
        Ok(())
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
            if change.op == cdcx_model::Op::Truncate {
                // TRUNCATE the ClickHouse table to match the source.
                let table = self.table_name(change);
                let resp = self.exec(&format!("TRUNCATE TABLE {table}")).await?;
                if !resp.status().is_success() {
                    return Err(SinkError::Write(format!(
                        "clickhouse truncate failed: HTTP {}",
                        resp.status()
                    )));
                }
                continue;
            }
            self.ensure_table(change).await?;
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
