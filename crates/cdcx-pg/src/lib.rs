//! PostgreSQL source for cdcx.
//!
//! Wraps [`pgwire_replication`] to provide a decoded stream of
//! [`cdcx_model::Txn`] values. The wire-level protocol (connection,
//! slot/stream lifecycle, standby status updates) belongs to the crate;
//! decoding the pgoutput payload inside `XLogData` frames — Relation,
//! Insert, Update, Delete, Truncate — is ours, in [`decoder`].
//!
//! The reader is protocol version 1 (no streaming of in-progress
//! transactions), which matches the project's delivery guarantees.

pub mod backfill;
pub mod decoder;
pub mod ops;
pub mod reader;
pub mod snapshot;

pub use backfill::{backfill_then_stream, BackfillError};
pub use decoder::{from_pg_text, DecodeError};
pub use ops::{DriftDetector, DriftKind, HEARTBEAT_INTERVAL, SchemaDrift, SlotLag, slot_lag};
pub use reader::{Reader, ReaderConfig, ReaderError};
pub use snapshot::{
    SlotOptions, SnapshotError, SnapshotHandoff, copy_table, create_slot_with_snapshot,
    resolve_slot_snapshot,
};
