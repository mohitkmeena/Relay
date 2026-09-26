//! Deliver one transaction to every configured sink. A txn is acked
//! (and the watermark advances) only when ALL sinks confirm durability.

use cdcx_model::Txn;

use crate::DynSink;
use crate::SinkError;

use super::Sink;

/// Fan-out: writes to each sink in order, in sequence.
///
/// Ordering guarantee: if sink N fails, sinks 1..N-1 may already have
/// the txn. On retry (at-least-once) they receive it again — sinks must
/// tolerate duplicates, e.g. ClickHouse via ReplacingMergeTree version
/// collapse, HTTP receivers via idempotency keys.
pub struct FanOutSink {
    sinks: Vec<DynSink>,
}

impl FanOutSink {
    /// Wrap one or more erased sinks.
    pub fn new(sinks: Vec<DynSink>) -> Self {
        Self { sinks }
    }
}

impl Sink for FanOutSink {
    async fn write_txn(&mut self, txn: &Txn) -> Result<(), SinkError> {
        for sink in &mut self.sinks {
            sink.write_txn(txn).await?;
        }
        Ok(())
    }
}
