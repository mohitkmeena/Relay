//! Replication stream reader: `pgwire-replication` events to [`Txn`]s.

use std::time::Duration;

use cdcx_model::{Change, Txn};
use pgwire_replication::Lsn;
use pgwire_replication::client::{ReplicationClient, ReplicationEvent};
use pgwire_replication::config::{Publication, ReplicationConfig};
use tracing::{debug, info, warn};

use crate::decoder::{DecodeError, Decoder};
use crate::ops::{DriftDetector, DriftKind};

/// Error surfaced by the reader.
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    /// The underlying replication connection failed.
    #[error("replication connection error: {0}")]
    Connection(String),
    /// A pgoutput payload failed to decode.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

/// Configuration for one replication source. Mirrors the subset of
/// `ReplicationConfig` a pipeline file needs, with environment-friendly
/// defaults for local Docker Postgres.
#[derive(Clone, Debug)]
pub struct ReaderConfig {
    /// Hostname of the PostgreSQL server.
    pub host: String,
    /// Port, usually 5432.
    pub port: u16,
    /// Replication user (needs `REPLICATION` privilege).
    pub user: String,
    /// Password for the replication user.
    pub password: String,
    /// Database whose publication to consume.
    pub database: String,
    /// Replication slot name. Persistent: survives restarts.
    pub slot: String,
    /// Publication name; must exist on the server.
    pub publication: String,
}

impl Default for ReaderConfig {
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

/// Reads decoded transactions from one PostgreSQL replication slot.
///
/// One `Reader` per slot. Create it, call [`Reader::run`] inside a tokio
/// task, and it yields whole [`Txn`]s — the reader buffers row changes
/// between Begin and Commit and only emits once the commit arrives.
pub struct Reader {
    config: ReaderConfig,
}

impl Reader {
    /// Create a reader for the given config.
    pub fn new(config: ReaderConfig) -> Self {
        Self { config }
    }

    /// Stream decoded transactions forever, passing each to `on_txn`.
    ///
    /// Standby status updates (the ack) are sent by the underlying client
    /// on its own schedule; advancing the applied LSN happens in the engine
    /// (M2), not here.
    pub async fn run<F>(&self, mut on_txn: F) -> Result<(), ReaderError>
    where
        F: FnMut(Txn),
    {
        let cfg = ReplicationConfig::new(
            self.config.host.clone(),
            self.config.user.clone(),
            self.config.password.clone(),
            self.config.database.clone(),
            self.config.slot.clone(),
            Publication::from(self.config.publication.clone()),
        )
        .with_port(self.config.port)
        .with_status_interval(Duration::from_secs(1));

        info!(
            slot = %self.config.slot,
            publication = %self.config.publication,
            "connecting to replication stream"
        );

        let mut client = ReplicationClient::connect(cfg)
            .await
            .map_err(|e| ReaderError::Connection(e.to_string()))?;

        let mut decoder = Decoder::new();
        let mut drift = DriftDetector::new();
        let mut current: Option<TxnBuffer> = None;

        loop {
            let Some(event) = client
                .recv()
                .await
                .map_err(|e| ReaderError::Connection(e.to_string()))?
            else {
                info!("replication stream ended");
                return Ok(());
            };
            match event {
                ReplicationEvent::Begin { .. } => {
                    current = Some(TxnBuffer::default());
                }
                ReplicationEvent::XLogData {
                    data, wal_start, ..
                } => {
                    let Some(buffer) = current.as_mut() else {
                        warn!(
                            lsn = wal_start.to_string(),
                            "change data outside a transaction; dropping"
                        );
                        keepalive_fallback(&client, wal_start);
                        continue;
                    };
                    let mut slice: &[u8] = &data;
                    while !slice.is_empty() {
                        match decoder.decode(&mut slice) {
                            Ok(Some(message)) => {
                                if let crate::decoder::Message::Relation {
                                    relation_id,
                                    namespace,
                                    name,
                                    columns,
                                } = &message
                                {
                                    let pairs: Vec<(String, u32)> = columns
                                        .iter()
                                        .map(|c| (c.name.clone(), c.type_oid))
                                        .collect();
                                    let table = format!("{namespace}.{name}");
                                    if let Some(drift) = drift.observe(*relation_id, table, &pairs)
                                    {
                                        match &drift.kind {
                                            DriftKind::ColumnAdded { column } => {
                                                warn!(
                                                    table = %drift.table,
                                                    column = %column,
                                                    "schema drift: column added"
                                                );
                                            }
                                            DriftKind::ColumnRemoved { column } => {
                                                warn!(
                                                    table = %drift.table,
                                                    column = %column,
                                                    "schema drift: column removed"
                                                );
                                            }
                                            DriftKind::TypeChanged {
                                                column,
                                                old_oid,
                                                new_oid,
                                            } => {
                                                warn!(
                                                    table = %drift.table,
                                                    column = %column,
                                                    old_oid,
                                                    new_oid,
                                                    "schema drift: type changed"
                                                );
                                            }
                                            DriftKind::NewRelation => {
                                                debug!(
                                                    table = %drift.table,
                                                    "relation appeared in stream"
                                                );
                                            }
                                        }
                                    }
                                }
                                if let Some(change) = decoder.to_change(
                                    &message,
                                    wal_start.as_u64(),
                                    buffer.changes.len() as u32,
                                ) {
                                    buffer.changes.push(change);
                                }
                                // A Truncate message may cover several
                                // relations; to_change emitted the
                                // first, append one for each remaining.
                                if let crate::decoder::Message::Truncate {
                                    relation_ids, ..
                                } = &message
                                {
                                    for relation_id in relation_ids.iter().skip(1) {
                                        if let Some(change) = decoder.truncate_change(
                                            *relation_id,
                                            wal_start.as_u64(),
                                            buffer.changes.len() as u32,
                                        ) {
                                            buffer.changes.push(change);
                                        }
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
                ReplicationEvent::Commit { end_lsn, .. } => {
                    if let Some(TxnBuffer { changes }) = current.take()
                        && !changes.is_empty()
                    {
                        on_txn(Txn {
                            commit_lsn: end_lsn.as_u64(),
                            changes,
                        });
                    }
                    // M2+: ack ordering. The engine checkpoints only
                    // after the sink confirms; the client's periodic
                    // standby updates keep the connection alive between
                    // engine acks. `update_applied_lsn` is monotonic,
                    // so an engine-driven ack at a lower LSN than the
                    // client already sent is a no-op — safe.
                    client.update_applied_lsn(end_lsn);
                }
                ReplicationEvent::KeepAlive { wal_end, .. } => {
                    debug!(lsn = %wal_end, "keepalive");
                }
                ReplicationEvent::Message { .. } => {
                    debug!("logical decoding message; not consumed yet");
                }
                ReplicationEvent::StoppedAt { .. } => {
                    info!("replication stopped at requested LSN");
                    return Ok(());
                }
            }
        }
    }
}

/// Buffer of changes accumulated between Begin and Commit.
#[derive(Default)]
struct TxnBuffer {
    changes: Vec<Change>,
}

fn keepalive_fallback(_client: &ReplicationClient, _lsn: Lsn) {
    // Placeholder until M2 moves ack control into the engine; the client
    // already sends periodic standby status updates on its own.
}
