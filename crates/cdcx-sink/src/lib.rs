//! Delivery targets for cdcx.
//!
//! A sink receives whole [`Txn`]s and reports durability. The engine's
//! ack watermark (M2) only advances past a transaction's `commit_lsn`
//! after [`Sink::write_txn`] returns `Ok`, so sinks must not return
//! success before the write is durable: the console sink is trivially
//! durable, HTTP requires the remote's 2xx, ClickHouse requires the
//! insert response.

pub mod clickhouse;
pub mod console;
pub mod http;

pub use clickhouse::ClickHouseSink;
pub use console::ConsoleSink;
pub use http::HttpSink;

use cdcx_model::Txn;
use std::future::Future;

/// Error returned by a sink when a transaction could not be durably written.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    /// The sink is fundamentally misconfigured; retrying won't help.
    #[error("sink misconfigured: {0}")]
    Config(String),
    /// The write failed; the engine may retry the transaction.
    #[error("sink write failed: {0}")]
    Write(String),
    /// The sink rejected the transaction's shape (bad schema, etc.).
    #[error("sink rejected txn {lsn}: {reason}")]
    Rejected {
        /// The rejected transaction's commit LSN.
        lsn: u64,
        /// Why it was rejected.
        reason: String,
    },
}

/// A transaction delivery target.
pub trait Sink: Send + Sync {
    /// Durably write one transaction. Returning `Ok` means the write is
    /// durable and the engine may advance its watermark past `txn.commit_lsn`.
    fn write_txn(&mut self, txn: &Txn) -> impl Future<Output = Result<(), SinkError>> + Send;
}
