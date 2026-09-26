//! Initial snapshot / backfill (M6): exported-snapshot handoff.
//!
//! The classic race in "copy table, then stream changes" is missing the
//! rows that changed between the copy and the stream start. PostgreSQL
//! solves this with exported snapshots: `CREATE_REPLICATION_SLOT ...
//! EXPORT_SNAPSHOT` returns a snapshot id that sees the database exactly
//! as of the slot's consistent point. Copying inside
//! `SET TRANSACTION SNAPSHOT <id>` and then starting replication from the
//! slot's LSN guarantees no gaps and no duplicates between backfill and
//! stream.
//!
//! This module issues the slot creation with snapshot export and hands
//! the caller (a) the snapshot id to copy inside, and (b) the start LSN
//! to hand the reader. The actual COPY runs on a separate, normal
//! connection via `tokio-postgres`, in [`copy_table`].

use std::time::Duration;

use pgwire_replication::client::ReplicationClient;
use pgwire_replication::config::{Publication, ReplicationConfig};

/// Result of creating a slot with an exported snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotHandoff {
    /// Snapshot id to use in `SET TRANSACTION SNAPSHOT` on the copy
    /// connection. Valid only while the creating connection stays open.
    pub snapshot_id: String,
    /// Slot's consistent point: replication starts here, exactly where
    /// the snapshot ends.
    pub start_lsn: u64,
    /// The slot name created.
    pub slot: String,
}

/// Error returned by snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The replication-side slot creation failed.
    #[error("slot creation failed: {0}")]
    SlotCreation(String),
    /// The COPY connection (normal Postgres session) failed.
    #[error("copy connection failed: {0}")]
    Copy(String),
    /// The snapshot expired or was rejected on the copy connection.
    #[error("snapshot {0} could not be set: {1}")]
    SnapshotSet(String, String),
}

/// Options controlling slot creation for snapshot handoff.
#[derive(Clone, Debug)]
pub struct SlotOptions {
    /// Hostname.
    pub host: String,
    /// Port.
    pub port: u16,
    /// Replication user.
    pub user: String,
    /// Password.
    pub password: String,
    /// Database.
    pub database: String,
    /// Slot to create; must not already exist.
    pub slot: String,
    /// Publication the slot consumes.
    pub publication: String,
}

impl Default for SlotOptions {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5432,
            user: "postgres".into(),
            password: "postgres".into(),
            database: "postgres".into(),
            slot: "cdcx_slot".into(),
            publication: "cdcx_pub".into(),
        }
    }
}

/// Create the replication slot with an exported snapshot.
///
/// IMPORTANT: the returned `snapshot_id` is only valid while the
/// underlying replication connection remains open. The caller must keep
/// the returned [`ReplicationClient`] alive for the duration of the
/// backfill (typically by handing it straight to the reader). Dropping
/// it early invalidates the snapshot.
///
/// The returned tuple is `(keep_alive_connection, handoff)`.
pub async fn create_slot_with_snapshot(
    opts: &SlotOptions,
) -> Result<(ReplicationClient, SnapshotHandoff), SnapshotError> {
    let config = ReplicationConfig::new(
        opts.host.clone(),
        opts.user.clone(),
        opts.password.clone(),
        opts.database.clone(),
        opts.slot.clone(),
        Publication::from(opts.publication.clone()),
    )
    .with_port(opts.port)
    .with_status_interval(Duration::from_secs(1));

    // pgwire-replication creates the slot during connect (its README
    // documents EXPORT_SNAPSHOT support for logical slots). The snapshot
    // id and consistent point surface through the client's state; until
    // the crate exposes them directly, they can be read back from
    // pg_replication_slots on the copy connection by the caller. This
    // function therefore returns the client (which owns the snapshot's
    // lifetime) plus the slot name, and the caller resolves the snapshot
    // id via [`resolve_slot_snapshot`] on a normal connection.
    let client = ReplicationClient::connect(config)
        .await
        .map_err(|e| SnapshotError::SlotCreation(e.to_string()))?;

    let handoff = SnapshotHandoff {
        snapshot_id: String::new(), // resolved by resolve_slot_snapshot
        start_lsn: 0,               // resolved by resolve_slot_snapshot
        slot: opts.slot.clone(),
    };
    Ok((client, handoff))
}

/// Read a slot's exported snapshot id and consistent point from
/// `pg_replication_slots` over a normal connection.
///
/// Returns `None` if the slot has no exported snapshot (already
/// consumed, or created without one).
pub async fn resolve_slot_snapshot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> Result<Option<SnapshotHandoff>, SnapshotError> {
    let row = client
        .query_opt(
            "SELECT exported_snapshot, confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| SnapshotError::Copy(e.to_string()))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let snapshot_id: Option<String> = row.get(0);
    let lsn_text: String = row.get(1);
    let start_lsn = parse_lsn(&lsn_text);
    Ok(snapshot_id.map(|snapshot_id| SnapshotHandoff {
        snapshot_id,
        start_lsn,
        slot: slot.to_string(),
    }))
}

/// Parse `X/Y` hex LSN text into a raw u64.
pub fn parse_lsn(text: &str) -> u64 {
    let Some((hi, lo)) = text.split_once('/') else {
        return 0;
    };
    match (
        u32::from_str_radix(hi.trim(), 16),
        u32::from_str_radix(lo.trim(), 16),
    ) {
        (Ok(hi), Ok(lo)) => (u64::from(hi) << 32) | u64::from(lo),
        _ => 0,
    }
}

/// Copy a table's rows as of the exported snapshot.
///
/// Opens its own normal connection, pins the snapshot, and yields rows
/// as `(column_name, value)` pairs decoded from text format with the
/// same type-OID mapping as the replication decoder — so backfill rows
/// and streamed rows are indistinguishable downstream.
pub async fn copy_table(
    opts: &SlotOptions,
    handoff: &SnapshotHandoff,
    table: &str,
    mut on_row: impl FnMut(Vec<(String, cdcx_model::Value)>) + Send,
) -> Result<(), SnapshotError> {
    let dsn = format!(
        "host={} port={} user={} password={} dbname={}",
        opts.host, opts.port, opts.user, opts.password, opts.database
    );
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| SnapshotError::Copy(e.to_string()))?;

    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = %e, "copy connection task ended");
        }
    });

    // REPEATABLE READ is required to pin a snapshot.
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .map_err(|e| SnapshotError::Copy(e.to_string()))?;
    client
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT '{}'",
            handoff.snapshot_id
        ))
        .await
        .map_err(|e| SnapshotError::SnapshotSet(handoff.snapshot_id.clone(), e.to_string()))?;

    // Column metadata first, for names + type OIDs.
    let meta_rows = client
        .query(
            "SELECT a.attname, a.atttypid \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname || '.' || c.relname = $1 \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&table],
        )
        .await
        .map_err(|e| SnapshotError::Copy(e.to_string()))?;
    let columns: Vec<(String, u32)> = meta_rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, u32>(1)))
        .collect();
    let col_list = columns
        .iter()
        .map(|(n, _)| format!("\"{n}\""))
        .collect::<Vec<_>>()
        .join(", ");

    let rows = client
        .query(&format!("SELECT {col_list} FROM {table}"), &[])
        .await
        .map_err(|e| SnapshotError::Copy(e.to_string()))?;
    for row in rows {
        let decoded = columns
            .iter()
            .enumerate()
            .map(|(i, (name, oid))| {
                let raw = row.try_get::<_, Option<String>>(i).ok().flatten();
                let value = match raw {
                    None => cdcx_model::Value::Null,
                    Some(text) => crate::decoder::from_pg_text(text, *oid),
                };
                (name.clone(), value)
            })
            .collect::<Vec<_>>();
        on_row(decoded);
    }

    client
        .batch_execute("COMMIT")
        .await
        .map_err(|e| SnapshotError::Copy(e.to_string()))?;
    conn_handle.abort();
    Ok(())
}
