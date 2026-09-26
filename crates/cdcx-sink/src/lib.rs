//! Delivery targets for cdcx.
//!
//! A sink receives whole [`Txn`]s and reports durability. The engine's
//! ack watermark (M2) only advances past a transaction's `commit_lsn`
//! after [`Sink::write_txn`] returns `Ok`, so sinks must not return
//! success before the write is durable: the console sink is trivially
//! durable, HTTP requires the remote's 2xx, ClickHouse requires the
//! insert response.
//!
//! [`Sink`] returns an `impl Future` (not dyn-compatible), so runtime
//! selection (plan-driven sinks, fan-out) goes through [`DynSink`],
//! which boxes the future manually — no async-trait dependency.

pub mod clickhouse;
pub mod console;
pub mod fanout;
pub mod http;

pub use clickhouse::ClickHouseSink;
pub use console::ConsoleSink;
pub use fanout::FanOutSink;
pub use http::HttpSink;

use cdcx_model::Txn;
use std::future::Future;
use std::pin::Pin;

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

/// Type-erased sink for runtime selection (plans, fan-out).
pub struct DynSink {
    inner: Box<dyn SinkErased>,
}

impl DynSink {
    /// Erase a statically-known sink.
    pub fn new<S: Sink + 'static>(sink: S) -> Self {
        Self {
            inner: Box::new(sink),
        }
    }
}

impl Sink for DynSink {
    async fn write_txn(&mut self, txn: &Txn) -> Result<(), SinkError> {
        self.inner.write_txn(txn).await
    }
}

/// The vtable target: boxes the future returned by [`Sink::write_txn`].
trait SinkErased: Send + Sync {
    fn write_txn<'a>(
        &'a mut self,
        txn: &'a Txn,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;
}

impl<S: Sink + 'static> SinkErased for S {
    fn write_txn<'a>(
        &'a mut self,
        txn: &'a Txn,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(Sink::write_txn(self, txn))
    }
}
