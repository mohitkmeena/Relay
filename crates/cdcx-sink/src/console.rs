//! Console sink: prints transactions to stdout. Trivially durable.

use cdcx_model::Txn;

use crate::SinkError;

use super::Sink;

/// Prints each transaction's changes to stdout, one line per change.
#[derive(Debug, Default)]
pub struct ConsoleSink;

impl ConsoleSink {
    /// Create a console sink.
    pub fn new() -> Self {
        Self
    }
}

impl Sink for ConsoleSink {
    async fn write_txn(&mut self, txn: &Txn) -> Result<(), SinkError> {
        println!(
            "=== txn commit_lsn={} changes={} ===",
            txn.commit_lsn,
            txn.changes.len()
        );
        for change in &txn.changes {
            println!(
                "  {} {}.{} lsn={} #{}",
                change.op, change.namespace, change.table, change.lsn, change.index_in_txn
            );
            if change.op == cdcx_model::Op::Truncate {
                continue;
            }
            if let Some(before) = &change.before {
                println!("    before: {before:?}");
            }
            if let Some(after) = &change.after {
                println!("    after:  {after:?}");
            }
        }
        Ok(())
    }
}
