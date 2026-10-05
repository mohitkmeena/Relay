//! Runtime metrics (M6): counts, watermarks, and slot lag, exposed as
//! a plain snapshot struct. Prometheus endpoint can come later; the
//! data model comes first.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Live pipeline counters. All fields are monotonically increasing
/// (except lag, a gauge) so a restart resets them — durability lives
/// in the checkpoint, not here.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Transactions received from the source (post-dedupe).
    pub txns: AtomicU64,
    /// Individual changes received.
    pub changes: AtomicU64,
    /// Transactions durably written and checkpointed.
    pub acked: AtomicU64,
    /// Transactions skipped as replays.
    pub deduped: AtomicU64,
    /// Sink write failures (retried or fatal).
    pub sink_errors: AtomicU64,
    /// Rows dropped as late by windowing.
    pub late_rows: AtomicU64,
    /// Schema drift events detected.
    pub drift_events: AtomicU64,
}

/// Shared handle; cheap to clone.
pub type MetricsHandle = Arc<Metrics>;

/// Create a shared metrics registry.
pub fn metrics() -> MetricsHandle {
    Arc::new(Metrics::default())
}

impl Metrics {
    /// Increment a counter.
    pub fn incr(&self, field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }

    /// Read a counter.
    pub fn get(&self, field: &AtomicU64) -> u64 {
        field.load(Ordering::Relaxed)
    }

    /// Human-readable one-line snapshot (for logging).
    pub fn snapshot(&self) -> String {
        format!(
            "txns={} changes={} acked={} deduped={} sink_errors={} late={} drift={}",
            self.get(&self.txns),
            self.get(&self.changes),
            self.get(&self.acked),
            self.get(&self.deduped),
            self.get(&self.sink_errors),
            self.get(&self.late_rows),
            self.get(&self.drift_events),
        )
    }
}
