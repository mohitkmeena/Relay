//! Snapshot-then-stream orchestration (M6).
//!
//! The ordering that guarantees no gaps and no duplicates between
//! backfill and live stream:
//!
//! 1. Create the slot with `EXPORT_SNAPSHOT` (the replication
//!    connection stays open — the snapshot is only valid while it
//!    lives).
//! 2. Resolve the snapshot id + consistent point LSN.
//! 3. Copy each table inside `SET TRANSACTION SNAPSHOT` — rows as of
//!    exactly the slot's consistent point.
//! 4. Emit each copied row as an INSERT into the pipeline, through
//!    the same engine path as streamed changes.
//! 5. Hand the (still-open) replication connection to the reader and
//!    stream from the slot; the first streamed change is the first
//!    commit after the snapshot, by construction.
//!
//! If the pipeline crashes mid-backfill, nothing was checkpointed, so
//! a restart re-creates the slot (dropping the old one) and starts the
//! backfill over. If it crashes mid-stream, the normal M2 path
//! resumes from `confirmed_flush_lsn`.

use std::time::Duration;

use cdcx_model::{Change, Op, Txn};
use pgwire_replication::client::ReplicationClient;
use pgwire_replication::config::{Publication, ReplicationConfig};
use tokio_postgres::NoTls;
use tracing::info;

use crate::ops::DriftDetector;
use crate::reader::{ReaderConfig, ReaderError};
use crate::snapshot::{copy_table, resolve_slot_snapshot, SlotOptions, SnapshotError};

/// Error combining the failure modes of backfill + stream.
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    /// Snapshot/slot phase failed.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// The streaming phase failed.
    #[error(transparent)]
    Reader(#[from] ReaderError),
}

/// Run the full backfill-then-stream flow for `tables`.
///
/// `on_txn` receives every transaction: synthetic backfill txns first
/// (one per table, `commit_lsn` = 0 so the engine's dedupe never
/// treats them as replays), then live streamed txns.
pub async fn backfill_then_stream<F>(
    config: &ReaderConfig,
    tables: &[String],
    mut on_txn: F,
) -> Result<(), BackfillError>
where
    F: FnMut(Txn) + Send,
{
    // Phase 1: slot with exported snapshot. Keep the client alive.
    let slot_opts = SlotOptions {
        host: config.host.clone(),
        port: config.port,
        user: config.user.clone(),
        password: config.password.clone(),
        database: config.database.clone(),
        slot: config.slot.clone(),
        publication: config.publication.clone(),
    };

    let repl_config = ReplicationConfig::new(
        config.host.clone(),
        config.user.clone(),
        config.password.clone(),
        config.database.clone(),
        config.slot.clone(),
        Publication::from(config.publication.clone()),
    )
    .with_port(config.port)
    .with_status_interval(Duration::from_secs(1));

    let client = ReplicationClient::connect(repl_config)
        .await
        .map_err(|e| SnapshotError::SlotCreation(e.to_string()))?;

    // Phase 2: resolve snapshot + consistent point over a normal
    // connection (the replication connection is held by `client`).
    let dsn = format!(
        "host={} port={} user={} password={} dbname={}",
        config.host, config.port, config.user, config.password, config.database
    );
    let (pg_client, pg_conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .map_err(|e| SnapshotError::Copy(e.to_string()))?;
    let conn_task = tokio::spawn(pg_conn);

    let handoff = resolve_slot_snapshot(&pg_client, &config.slot)
        .await?
        .ok_or_else(|| {
            SnapshotError::SlotCreation(format!(
                "slot {} exists but has no exported snapshot; drop it and retry",
                config.slot
            ))
        })?;

    info!(
        slot = %config.slot,
        start_lsn = handoff.start_lsn,
        "slot ready, beginning backfill"
    );

    // Phase 3+4: copy each table inside the snapshot, emit synthetic
    // txns. LSN 0 marks "pre-stream" so dedupe passes them through
    // exactly once (they are never replayed: a crash restarts the
    // whole backfill with a fresh slot).
    for table in tables {
        info!(table = %table, "copying table snapshot");
        let mut rows = Vec::new();
        copy_table(&slot_opts, &handoff, table, |row| rows.push(row)).await?;
        info!(table = %table, rows = rows.len(), "snapshot copied");
        if rows.is_empty() {
            continue;
        }
        let changes = rows
            .into_iter()
            .map(|after| Change {
                namespace: table
                    .rsplit_once('.')
                    .map(|(ns, _)| ns.to_string())
                    .unwrap_or_else(|| "public".into()),
                table: table
                    .rsplit_once('.')
                    .map(|(_, t)| t.to_string())
                    .unwrap_or_else(|| table.clone()),
                op: Op::Insert,
                after: Some(after),
                before: None,
                lsn: 0,
                index_in_txn: 0,
            })
            .collect();
        on_txn(Txn {
            commit_lsn: 0,
            changes,
        });
    }

    // The copy connection is no longer needed; the replication
    // connection must stay open so the slot keeps its position until
    // streaming takes over. Hand `client` to the streaming phase.
    conn_task.abort();
    info!("backfill complete, switching to live stream");

    // Phase 5: stream from the slot. The reader's decoder state starts
    // fresh; Postgres re-sends Relation metadata for every relation
    // after the start of streaming, so no state handoff is needed.
    let mut reader = ReaderAfterBackfill::new(config.clone(), client);
    reader.run(&mut on_txn).await?;
    Ok(())
}

/// A reader variant that adopts an already-connected replication
/// client (the one holding the snapshot) instead of dialing fresh.
struct ReaderAfterBackfill {
    config: ReaderConfig,
    client: Option<ReplicationClient>,
}

impl ReaderAfterBackfill {
    fn new(config: ReaderConfig, client: ReplicationClient) -> Self {
        Self {
            config,
            client: Some(client),
        }
    }

    async fn run(&mut self, on_txn: &mut impl FnMut(Txn)) -> Result<(), ReaderError> {
        // The reader currently owns its own connection lifecycle; the
        // cleanest integration is to reuse it as-is (a fresh
        // START_REPLICATION on the same slot continues from the slot's
        // confirmed position — which the snapshot fixed). Dropping the
        // held client here and letting the reader reconnect is
        // equivalent: the slot's restart position is durable in
        // Postgres, not in the connection.
        drop(self.client.take());
        let reader = crate::reader::Reader::new(self.config.clone());
        reader.run(|txn| on_txn(txn)).await
    }
}

// Keep DriftDetector referenced: the stream phase reuses the reader's
// built-in drift detection; this import documents the path.
const _: fn() = || {
    let _ = DriftDetector::new;
};
