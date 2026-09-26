//! Operational monitors (M6): slot lag, heartbeat, Relation-change
//! (DDL) detection.
//!
//! These are separate from the reader so they can run on their own
//! schedule over a normal (non-replication) connection.

use std::time::Duration;

/// Lag numbers for one slot, as of the query instant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SlotLag {
    /// Bytes of WAL written server-wide minus bytes confirmed by the slot.
    pub bytes: i64,
    /// The slot's confirmed flush LSN.
    pub confirmed_flush_lsn: u64,
    /// The server's current WAL write LSN.
    pub current_wal_lsn: u64,
}

/// Query slot lag over a normal Postgres connection.
///
/// Uses `pg_stat_replication_slots`-style math: current WAL LSN minus
/// the slot's `confirmed_flush_lsn`. Returns `None` if the slot does
/// not exist (dropped, or not yet created).
pub async fn slot_lag(
    client: &tokio_postgres::Client,
    slot: &str,
) -> Result<Option<SlotLag>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text, pg_current_wal_lsn()::text \
             FROM pg_replication_slots WHERE slot_name = $1 AND active",
            &[&slot],
        )
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let confirmed: String = row.get(0);
    let current: String = row.get(1);
    let confirmed_lsn = crate::snapshot::parse_lsn(&confirmed);
    let current_lsn = crate::snapshot::parse_lsn(&current);
    Ok(Some(SlotLag {
        bytes: current_lsn as i64 - confirmed_lsn as i64,
        confirmed_flush_lsn: confirmed_lsn,
        current_wal_lsn: current_lsn,
    }))
}

/// Emit a heartbeat into the WAL so idle databases still advance.
///
/// Without periodic writes, `confirmed_flush_lsn` never moves on an
/// idle source, lag metrics are meaningless, and `pg_replication_slot`
/// retention can grow unbounded. A transactional logical message is the
/// cheapest visible marker: it flows through the slot as a
/// `ReplicationEvent::Message` and commits an LSN the reader can ack.
///
/// Call this on a timer (e.g. every 10s) from a separate normal
/// connection.
pub async fn emit_heartbeat(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "SELECT pg_logical_emit_message(true, 'cdcx', 'heartbeat')",
            &[],
        )
        .await?;
    Ok(())
}

/// A detected Relation-message change (schema drift).
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaDrift {
    /// Relation OID whose definition changed.
    pub relation_id: u32,
    /// Namespace.table of the changed relation.
    pub table: String,
    /// What changed.
    pub kind: DriftKind,
}

/// What kind of Relation change was detected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftKind {
    /// A column was added; the decoder can continue (values arrive for
    /// the new column) but the plan may not expect it.
    ColumnAdded {
        /// Name of the new column.
        column: String,
    },
    /// A column was removed. Old images referencing it will now be
    /// short; operators must tolerate missing columns.
    ColumnRemoved {
        /// Name of the removed column.
        column: String,
    },
    /// A column's type OID changed. Text decoding continues, but typed
    /// operators may misinterpret; requires attention.
    TypeChanged {
        /// Column name.
        column: String,
        /// Previous type OID.
        old_oid: u32,
        /// New type OID.
        new_oid: u32,
    },
    /// The relation is new since the reader last saw it (first change
    /// after a DDL create + publication alter).
    NewRelation,
}

/// Watch for Relation-message changes between stream restarts.
///
/// The reader persists the last-seen Relation metadata per relation
/// (column names, type OIDs) into the checkpoint's operator state. On
/// restart, the first Relation message for a table is compared against
/// the persisted one; differences become [`SchemaDrift`]s. This catches
/// the dangerous case — a plan compiled for a schema that silently
/// changed — instead of decoding garbage.
#[derive(Debug, Default)]
pub struct DriftDetector {
    /// Last known columns per relation OID: (name, type_oid) pairs.
    known: std::collections::HashMap<u32, (String, Vec<(String, u32)>)>,
}

impl DriftDetector {
    /// New detector with no prior knowledge.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restore from checkpointed state (JSON produced by [`Self::state`]).
    pub fn restore(state: &serde_json::Value) -> Self {
        let mut known = std::collections::HashMap::new();
        if let serde_json::Value::Object(map) = state {
            if let Some(serde_json::Value::Object(rels)) = map.get("relations") {
                for (oid, entry) in rels {
                    let Ok(relation_id) = oid.parse::<u32>() else {
                        continue;
                    };
                    if let serde_json::Value::Object(rel) = entry {
                        let table = rel
                            .get("table")
                            .and_then(|t| t.as_str())
                            .unwrap_or_default()
                            .to_string();
                        let columns = rel
                            .get("columns")
                            .and_then(|c| c.as_array())
                            .map(|cols| {
                                cols.iter()
                                    .filter_map(|c| {
                                        Some((
                                            c.get("name")?.as_str()?.to_string(),
                                            c.get("oid")?.as_u64()? as u32,
                                        ))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        known.insert(relation_id, (table, columns));
                    }
                }
            }
        }
        Self { known }
    }

    /// Serialize current knowledge for the checkpoint.
    pub fn state(&self) -> serde_json::Value {
        let mut relations = serde_json::Map::new();
        for (oid, (table, columns)) in &self.known {
            relations.insert(
                oid.to_string(),
                serde_json::json!({
                    "table": table,
                    "columns": columns.iter().map(|(n, o)| {
                        serde_json::json!({ "name": n, "oid": o })
                    }).collect::<Vec<_>>(),
                }),
            );
        }
        serde_json::json!({ "relations": relations })
    }

    /// Feed the current Relation message; returns drift if it differs
    /// from what was last seen.
    pub fn observe(
        &mut self,
        relation_id: u32,
        table: String,
        columns: &[(String, u32)],
    ) -> Option<SchemaDrift> {
        let drift = match self.known.get(&relation_id) {
            None => SchemaDrift {
                relation_id,
                table: table.clone(),
                kind: DriftKind::NewRelation,
            },
            Some((_, known_cols)) => {
                if let Some((name, _)) = columns
                    .iter()
                    .find(|(n, _)| !known_cols.iter().any(|(kn, _)| kn == n))
                {
                    SchemaDrift {
                        relation_id,
                        table: table.clone(),
                        kind: DriftKind::ColumnAdded {
                            column: name.clone(),
                        },
                    }
                } else if let Some((name, _)) = known_cols
                    .iter()
                    .find(|(n, _)| !columns.iter().any(|(cn, _)| cn == n))
                {
                    SchemaDrift {
                        relation_id,
                        table: table.clone(),
                        kind: DriftKind::ColumnRemoved {
                            column: name.clone(),
                        },
                    }
                } else {
                    // Same names; look for a type change.
                    columns.iter().find_map(|(name, oid)| {
                        let old = known_cols
                            .iter()
                            .find(|(kn, _)| kn == name)
                            .map(|(_, ko)| *ko)
                            .unwrap_or(0);
                        (old != 0 && old != *oid).then(|| SchemaDrift {
                            relation_id,
                            table: table.clone(),
                            kind: DriftKind::TypeChanged {
                                column: name.clone(),
                                old_oid: old,
                                new_oid: *oid,
                            },
                        })
                    })?
                }
            }
        };
        self.known.insert(relation_id, (table, columns.to_vec()));
        Some(drift)
    }
}

/// Recommended heartbeat cadence: frequent enough to keep lag metrics
/// fresh, cheap enough to run forever.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
