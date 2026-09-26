//! Pipeline runtime: source -> normalize -> plan -> sink, with the ack
//! watermark that makes delivery at-least-once.
//!
//! The ordering rule the whole design hangs on:
//!
//! 1. The reader decodes a transaction and hands it over.
//! 2. The plan's operators transform it (pure, in-memory).
//! 3. The sink writes it and returns `Ok` **only when durable**.
//! 4. Only then does the engine advance the checkpoint and
//!    `confirmed_flush_lsn` (via the reader's applied-LSN hook).
//!
//! A crash anywhere before step 4 means the transaction is replayed from
//! the slot on restart — duplicates are possible, loss is not.

use cdcx_model::Txn;

pub mod materialized;

use cdcx_plan::Compiled;
use cdcx_sink::Sink;
use cdcx_state::{Checkpoint, FileCheckpoint};
pub use materialized::{Aggregates, MaterializedView, ViewConfig};

use thiserror::Error;

/// Error surfaced by the engine.
#[derive(Debug, Error)]
pub enum EngineError {
    /// The source failed.
    #[error("source failed: {0}")]
    Source(#[from] cdcx_pg::ReaderError),
    /// The sink failed; the pipeline stops to avoid losing the txn.
    #[error("sink failed at lsn {lsn}: {source}")]
    Sink {
        /// The transaction's commit LSN that could not be written.
        lsn: u64,
        /// The underlying sink error.
        source: cdcx_sink::SinkError,
    },
    /// Checkpoint persistence failed.
    #[error("checkpoint failed: {0}")]
    Checkpoint(#[from] cdcx_state::CheckpointError),
}

/// Runtime wiring for one pipeline.
pub struct Engine {
    plan: Compiled,
    checkpoint: FileCheckpoint,
    checkpoint_path: std::path::PathBuf,
    /// Materialized-view operator state, checkpointed atomically with
    /// the LSN (M7). `None` for pure pass-through pipelines.
    view: Option<MaterializedView>,
}

impl Engine {
    /// Build an engine from a compiled plan and a checkpoint file path.
    pub fn new(
        plan: Compiled,
        checkpoint_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, EngineError> {
        let checkpoint_path = checkpoint_path.into();
        let checkpoint = FileCheckpoint::load(&checkpoint_path)?;
        Ok(Self {
            plan,
            checkpoint,
            checkpoint_path,
            view: None,
        })
    }

    /// Attach a materialized view whose state is checkpointed
    /// atomically with the LSN.
    pub fn with_view(mut self, view: MaterializedView) -> Self {
        self.view = Some(view);
        self
    }

    /// Process one transaction: dedupe, transform, view-update, sink,
    /// checkpoint.
    ///
    /// This is the unit the crash-safety argument is written against.
    /// Any error aborts the pipeline without advancing the watermark.
    /// The view's state is advanced BEFORE the sink write and committed
    /// in the same checkpoint as the LSN only after the sink confirms —
    /// so a crash mid-transaction replays the delta from the old
    /// checkpoint, keeping state and stream position consistent.
    ///
    /// Transactions with `commit_lsn <= checkpoint.lsn` are skipped
    /// entirely: at-least-once delivery means the slot may replay
    /// already-acked transactions after a restart, and (unlike Z-set
    /// deltas) sinks and views are not idempotent on their own. This
    /// check IS the dedupe; see the differential test that pins it.
    pub async fn process<S: Sink>(&mut self, txn: Txn, sink: &mut S) -> Result<(), EngineError> {
        let commit_lsn = txn.commit_lsn;
        if commit_lsn <= self.checkpoint.get().lsn {
            tracing::debug!(
                commit_lsn,
                checkpoint = self.checkpoint.get().lsn,
                "skipping already-acked transaction"
            );
            return Ok(());
        }
        let mut txn = txn;
        for change in &mut txn.changes {
            self.plan.apply(change);
        }
        if let Some(view) = self.view.as_mut() {
            view.process(&txn);
        }
        sink.write_txn(&txn)
            .await
            .map_err(|source| EngineError::Sink {
                lsn: commit_lsn,
                source,
            })?;
        let operator_state = self
            .view
            .as_ref()
            .map(|v| v.state())
            .unwrap_or_else(|| self.checkpoint.get().operator_state.clone());
        self.checkpoint.advance(Checkpoint {
            lsn: commit_lsn,
            operator_state,
            version: 1,
        })?;
        Ok(())
    }

    /// The highest durably-acked LSN.
    pub fn acked_lsn(&self) -> u64 {
        self.checkpoint.get().lsn
    }

    /// Path of the checkpoint file, for logging and tooling.
    pub fn checkpoint_path(&self) -> &std::path::Path {
        &self.checkpoint_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdcx_model::{Change, Op, Value};
    use cdcx_sink::{Sink, SinkError};

    struct FlakySink {
        fail_times: std::sync::atomic::AtomicU32,
    }

    impl Sink for FlakySink {
        async fn write_txn(&mut self, _txn: &Txn) -> Result<(), SinkError> {
            use std::sync::atomic::Ordering;
            if self.fail_times.fetch_sub(1, Ordering::SeqCst) > 0 {
                return Err(SinkError::Write("flaky".into()));
            }
            Ok(())
        }
    }

    fn txn(lsn: u64) -> Txn {
        Txn {
            commit_lsn: lsn,
            changes: vec![Change {
                namespace: "public".into(),
                table: "users".into(),
                op: Op::Insert,
                after: Some(vec![("id".into(), Value::Int(1))]),
                before: None,
                lsn,
                index_in_txn: 0,
            }],
        }
    }

    #[tokio::test]
    async fn watermark_does_not_advance_on_sink_failure() {
        let dir = std::env::temp_dir().join(format!("cdcx-eng-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cp = dir.join("cp.json");

        let plan = Compiled::default();
        let mut engine = Engine::new(plan, &cp).unwrap();
        let mut sink = FlakySink {
            fail_times: std::sync::atomic::AtomicU32::new(1),
        };

        // First write fails: watermark must not advance.
        let err = engine.process(txn(100), &mut sink).await.unwrap_err();
        assert!(matches!(err, EngineError::Sink { lsn: 100, .. }));
        assert_eq!(engine.acked_lsn(), 0);

        // Replay succeeds (at-least-once): watermark advances.
        engine.process(txn(100), &mut sink).await.unwrap();
        assert_eq!(engine.acked_lsn(), 100);

        // A restarted engine reloads the checkpoint.
        let engine2 = Engine::new(Compiled::default(), &cp).unwrap();
        assert_eq!(engine2.acked_lsn(), 100);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn replayed_txn_at_or_below_watermark_is_skipped() {
        let dir = std::env::temp_dir().join(format!("cdcx-eng-dedupe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cp = dir.join("cp.json");

        let mut engine = Engine::new(Compiled::default(), &cp).unwrap();
        let mut sink = RecordingSink::default();

        // Process txns 100 and 200.
        engine.process(txn(100), &mut sink).await.unwrap();
        engine.process(txn(200), &mut sink).await.unwrap();
        assert_eq!(engine.acked_lsn(), 200);
        assert_eq!(sink.written.lock().unwrap().len(), 2);

        // Slot replay after restart redelivers 100, 200, and a fresh 300.
        // The acked ones must be skipped; only 300 is new work.
        engine.process(txn(100), &mut sink).await.unwrap();
        engine.process(txn(200), &mut sink).await.unwrap();
        engine.process(txn(300), &mut sink).await.unwrap();
        assert_eq!(engine.acked_lsn(), 300);
        assert_eq!(
            sink.written.lock().unwrap().as_slice(),
            &[100, 200, 300],
            "replayed txns must not reach the sink twice"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Records the LSN of every txn that reaches it.
    #[derive(Default)]
    struct RecordingSink {
        written: std::sync::Mutex<Vec<u64>>,
    }

    impl Sink for RecordingSink {
        async fn write_txn(&mut self, txn: &Txn) -> Result<(), SinkError> {
            self.written.lock().unwrap().push(txn.commit_lsn);
            Ok(())
        }
    }
}
