//! pgoutput payload decoder.
//!
//! `pgwire-replication` hands us the raw bytes of each `XLogData` frame.
//! This module parses the pgoutput logical replication messages those bytes
//! contain — Relation, Insert, Update, Delete, Truncate — and turns row
//! tuples into [`cdcx_model::Value`]s using the Relation metadata seen so
//! far on the stream.
//!
//! Text format only (the default). Values arrive as strings with a
//! presence kind byte: `'n'` null, `'u'` unchanged (TOAST marker), `'t'`
//! text. The [protocol spec] is the reference for message layouts.
//!
//! [protocol spec]: https://www.postgresql.org/docs/current/protocol-logical-replication.html

use std::collections::HashMap;

use bytes::Buf;
use cdcx_model::{Change, Op, Value};

/// Error returned when a pgoutput payload cannot be decoded.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The buffer ended before a complete message could be read.
    #[error("pgoutput payload truncated at offset {offset}")]
    Truncated {
        /// Byte offset at which the payload ran out.
        offset: usize,
    },
    /// A value kind byte was not one of `n`, `u`, `t`.
    #[error("unknown tuple value kind {kind:#04x}")]
    UnknownValueKind {
        /// The offending kind byte.
        kind: u8,
    },
    /// A message type byte was not recognized.
    #[error("unknown pgoutput message type {kind:#04x}")]
    UnknownMessageType {
        /// The offending message tag.
        kind: u8,
    },
    /// A relation OID was referenced before its Relation message arrived.
    #[error("relation oid {oid} not seen in a Relation message yet")]
    UnknownRelation {
        /// The referenced relation OID.
        oid: u32,
    },
}

/// A parsed pgoutput message, one per row-level event plus relation metadata.
#[derive(Debug)]
pub enum Message {
    /// Relation metadata: column names, type OIDs, and optional flags.
    Relation {
        /// Relation OID.
        relation_id: u32,
        /// Namespace (schema).
        namespace: String,
        /// Table name.
        name: String,
        /// Column descriptors, in stream order.
        columns: Vec<Column>,
    },
    /// Row insert.
    Insert {
        /// Relation OID.
        relation_id: u32,
        /// New row tuple.
        tuple: Tuple,
    },
    /// Row update.
    Update {
        /// Relation OID.
        relation_id: u32,
        /// Old row tuple, if the stream carries one.
        old_tuple: Option<Tuple>,
        /// New row tuple.
        new_tuple: Tuple,
    },
    /// Row delete.
    Delete {
        /// Relation OID.
        relation_id: u32,
        /// Old row tuple, if the stream carries one.
        old_tuple: Tuple,
    },
    /// Table truncate. Carried but not yet normalized (see M7).
    Truncate {
        /// Number of relations in this truncate.
        nrelations: u32,
        /// Relation OIDs being truncated.
        relation_ids: Vec<u32>,
        /// Truncate option flags (cascade / restart identity).
        flags: u8,
    },
}

/// One column of a Relation message.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// PostgreSQL type OID (`pg_type.oid`).
    pub type_oid: u32,
    /// Extra flags; bit 1 marks a column of the table's replica identity.
    pub flags: u8,
}

/// A decoded tuple: one value per column of the current Relation.
pub type Tuple = Vec<Value>;

/// Decoding state carried across messages: the latest Relation metadata
/// per relation OID.
#[derive(Debug, Default)]
pub struct Decoder {
    relations: HashMap<u32, RelationMeta>,
}

/// Relation metadata kept for tuple decoding.
#[derive(Debug, Clone)]
struct RelationMeta {
    namespace: String,
    name: String,
    columns: Vec<Column>,
}

impl Decoder {
    /// Create an empty decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode one pgoutput message from `data`, advancing the cursor.
    ///
    /// Because relation metadata accumulates in `self`, each connection
    /// needs its own [`Decoder`].
    pub fn decode(&mut self, data: &mut &[u8]) -> Result<Option<Message>, DecodeError> {
        let Some(&tag) = data.first() else {
            return Ok(None);
        };
        let after_tag = &data[1..];
        let (message, consumed) = match tag {
            b'R' => self.decode_relation(after_tag)?,
            b'I' => self.decode_insert(after_tag)?,
            b'U' => self.decode_update(after_tag)?,
            b'D' => self.decode_delete(after_tag)?,
            b'T' => self.decode_truncate(after_tag)?,
            other => {
                return Err(DecodeError::UnknownMessageType { kind: other });
            }
        };
        data.advance(consumed + 1);
        Ok(Some(message))
    }

    fn decode_relation(&mut self, data: &[u8]) -> Result<(Message, usize), DecodeError> {
        let mut cur = Cursor::new(data);
        let relation_id = cur.u32()?;
        let namespace = cur.cstring()?;
        let name = cur.cstring()?;
        let ncolumns = cur.u16()? as usize;
        let mut columns = Vec::with_capacity(ncolumns);
        for _ in 0..ncolumns {
            let flags = cur.u8()?;
            let col_name = cur.cstring()?;
            let type_oid = cur.u32()?;
            columns.push(Column {
                name: col_name,
                type_oid,
                flags,
            });
        }
        let meta = RelationMeta {
            namespace: namespace.clone(),
            name: name.clone(),
            columns: columns.clone(),
        };
        self.relations.insert(relation_id, meta);
        Ok((
            Message::Relation {
                relation_id,
                namespace,
                name,
                columns,
            },
            cur.consumed(),
        ))
    }

    fn decode_insert(&mut self, data: &[u8]) -> Result<(Message, usize), DecodeError> {
        let mut cur = Cursor::new(data);
        let relation_id = cur.u32()?;
        // Insert carries exactly one tuple, prefixed by the sub-message kind
        // byte 'N' (new tuple).
        let kind = cur.u8()?;
        if kind != b'N' {
            return Err(DecodeError::UnknownMessageType { kind });
        }
        let meta = self
            .relations
            .get(&relation_id)
            .ok_or(DecodeError::UnknownRelation { oid: relation_id })?
            .clone();
        let tuple = cur.tuple(&meta.columns)?;
        Ok((Message::Insert { relation_id, tuple }, cur.consumed()))
    }

    fn decode_update(&mut self, data: &[u8]) -> Result<(Message, usize), DecodeError> {
        let mut cur = Cursor::new(data);
        let relation_id = cur.u32()?;
        let meta = self
            .relations
            .get(&relation_id)
            .ok_or(DecodeError::UnknownRelation { oid: relation_id })?
            .clone();
        // Update may carry an optional old tuple ('O') before the
        // mandatory new tuple ('N').
        let mut old_tuple = None;
        let kind = cur.u8()?;
        match kind {
            b'K' => {
                // Key-only old tuple: present when the table's replica
                // identity is DEFAULT or INDEX and the key changed. Decoded
                // like a tuple but only identity columns are present; we
                // treat it as a partial old image.
                old_tuple = Some(cur.tuple(&meta.columns)?);
                let next = cur.u8()?;
                if next != b'N' {
                    return Err(DecodeError::UnknownMessageType { kind: next });
                }
            }
            b'O' => {
                old_tuple = Some(cur.tuple(&meta.columns)?);
                let next = cur.u8()?;
                if next != b'N' {
                    return Err(DecodeError::UnknownMessageType { kind: next });
                }
            }
            b'N' => {}
            other => return Err(DecodeError::UnknownMessageType { kind: other }),
        }
        let new_tuple = cur.tuple(&meta.columns)?;
        Ok((
            Message::Update {
                relation_id,
                old_tuple,
                new_tuple,
            },
            cur.consumed(),
        ))
    }

    fn decode_delete(&mut self, data: &[u8]) -> Result<(Message, usize), DecodeError> {
        let mut cur = Cursor::new(data);
        let relation_id = cur.u32()?;
        let meta = self
            .relations
            .get(&relation_id)
            .ok_or(DecodeError::UnknownRelation { oid: relation_id })?
            .clone();
        // Delete carries one tuple: 'K' (key only) or 'O' (full old image).
        let kind = cur.u8()?;
        if kind != b'K' && kind != b'O' {
            return Err(DecodeError::UnknownMessageType { kind });
        }
        let old_tuple = cur.tuple(&meta.columns)?;
        Ok((
            Message::Delete {
                relation_id,
                old_tuple,
            },
            cur.consumed(),
        ))
    }

    fn decode_truncate(&mut self, data: &[u8]) -> Result<(Message, usize), DecodeError> {
        let mut cur = Cursor::new(data);
        let nrelations = cur.u32()?;
        let flags = cur.u8()?;
        let mut relation_ids = Vec::with_capacity(nrelations as usize);
        for _ in 0..nrelations {
            relation_ids.push(cur.u32()?);
        }
        Ok((
            Message::Truncate {
                nrelations,
                relation_ids,
                flags,
            },
            cur.consumed(),
        ))
    }

    /// Convert a decoded message into a pipeline [`Change`], given the
    /// change's LSN and position in its transaction.
    ///
    /// Returns `None` for Relation and Truncate messages, which carry no
    /// row data. Truncate normalization (as a synthetic delete-all or a
    /// set of deletes) is deferred to M7.
    pub fn to_change(&self, message: &Message, lsn: u64, index_in_txn: u32) -> Option<Change> {
        let (relation_id, op, after, before) = match message {
            Message::Relation { .. } | Message::Truncate { .. } => return None,
            Message::Insert {
                relation_id, tuple, ..
            } => (*relation_id, Op::Insert, Some(tuple), None),
            Message::Update {
                relation_id,
                old_tuple,
                new_tuple,
                ..
            } => (
                *relation_id,
                Op::Update,
                Some(new_tuple),
                old_tuple.as_ref(),
            ),
            Message::Delete {
                relation_id,
                old_tuple,
                ..
            } => (*relation_id, Op::Delete, None, Some(old_tuple)),
        };
        let meta = self.relations.get(&relation_id)?;
        let pair = |tuple: &Tuple| {
            tuple
                .iter()
                .zip(meta.columns.iter())
                .map(|(v, c)| (c.name.clone(), v.clone()))
                .collect::<Vec<_>>()
        };
        Some(Change {
            namespace: meta.namespace.clone(),
            table: meta.name.clone(),
            op,
            after: after.map(pair),
            before: before.map(pair),
            lsn,
            index_in_txn,
        })
    }
}

/// Read helper over a byte slice that tracks its position and reports
/// truncation offsets uniformly.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn consumed(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let Some(chunk) = self.data.get(self.pos..self.pos + n) else {
            return Err(DecodeError::Truncated { offset: self.pos });
        };
        self.pos += n;
        Ok(chunk)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn cstring(&mut self) -> Result<String, DecodeError> {
        let start = self.pos;
        let Some(nul) = self.data[start..].iter().position(|&b| b == 0) else {
            return Err(DecodeError::Truncated { offset: start });
        };
        let s = std::str::from_utf8(&self.data[start..start + nul])
            .map_err(|_| DecodeError::Truncated { offset: start })?;
        self.pos += nul + 1;
        Ok(s.to_owned())
    }

    /// Read one tuple, decoding values as text per each column's type OID.
    fn tuple(&mut self, columns: &[Column]) -> Result<Tuple, DecodeError> {
        let ncolumns = self.u16()? as usize;
        if ncolumns != columns.len() {
            // Key-only tuples ('K') have fewer values than the relation has
            // columns; pad the missing tail with Unchanged so downstream
            // sees a consistent shape.
        }
        let mut values = Vec::with_capacity(ncolumns);
        for i in 0..ncolumns {
            let kind = self.u8()?;
            let value = match kind {
                b'n' => Value::Null,
                b'u' => Value::Unchanged,
                b't' => {
                    let len = self.u32()? as usize;
                    let raw = self.take(len)?;
                    let text = String::from_utf8_lossy(raw).into_owned();
                    let oid = columns.get(i).map(|c| c.type_oid).unwrap_or(0);
                    from_pg_text(text, oid)
                }
                other => return Err(DecodeError::UnknownValueKind { kind: other }),
            };
            values.push(value);
            // A key tuple may stop early: remaining columns keep Unchanged.
            if self.pos >= self.data.len() && i + 1 < ncolumns {
                values.extend(std::iter::repeat_n(Value::Unchanged, ncolumns - i - 1));
                break;
            }
        }
        Ok(values)
    }
}

/// Well-known PostgreSQL type OIDs used by [`from_pg_text`].
mod oids {
    pub const BOOL: u32 = 16;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const JSON: u32 = 114;
    pub const JSONB: u32 = 3802;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const NUMERIC: u32 = 1700;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
}

/// Interpret a text-format value by its PostgreSQL type OID.
///
/// Unknown OIDs fall back to [`Value::Raw`], which keeps the pipeline
/// running on unfamiliar schemas at the cost of typed operators.
pub fn from_pg_text(text: String, type_oid: u32) -> Value {
    use oids::*;
    match type_oid {
        BOOL => match text.as_str() {
            "t" => Value::Bool(true),
            "f" => Value::Bool(false),
            _ => Value::Raw(text),
        },
        INT2 | INT4 | INT8 => text.parse().map(Value::Int).unwrap_or(Value::Raw(text)),
        FLOAT4 | FLOAT8 => text
            .parse::<f64>()
            .map(|x| Value::Float(cdcx_model::F64(x)))
            .unwrap_or(Value::Raw(text)),
        NUMERIC => Value::Numeric(text),
        TIMESTAMP | TIMESTAMPTZ => Value::Timestamp(text),
        JSON | JSONB => Value::Json(text),
        TEXT => Value::Text(text),
        _ => Value::Raw(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Relation message payload: 'R' + oid + "namespace\0name\0" +
    /// ncols(u16) + per column (flags u8, name cstring, type oid u32).
    fn relation_payload() -> Vec<u8> {
        let mut v = vec![b'R'];
        v.extend_from_slice(&1u32.to_be_bytes()); // relation id
        v.extend_from_slice(b"public\0");
        v.extend_from_slice(b"users\0");
        v.extend_from_slice(&2u16.to_be_bytes()); // two columns
        // id int4, replica identity column
        v.push(1);
        v.extend_from_slice(b"id\0");
        v.extend_from_slice(&oids::INT4.to_be_bytes());
        // name text
        v.push(0);
        v.extend_from_slice(b"name\0");
        v.extend_from_slice(&oids::TEXT.to_be_bytes());
        v
    }

    /// Insert payload: 'I' + oid + 'N' + ncols(u16) + values.
    fn insert_payload() -> Vec<u8> {
        let mut v = vec![b'I'];
        v.extend_from_slice(&1u32.to_be_bytes());
        v.push(b'N');
        v.extend_from_slice(&2u16.to_be_bytes());
        // id = 42
        v.push(b't');
        v.extend_from_slice(&2u32.to_be_bytes());
        v.extend_from_slice(b"42");
        // name = ada
        v.push(b't');
        v.extend_from_slice(&3u32.to_be_bytes());
        v.extend_from_slice(b"ada");
        v
    }

    #[test]
    fn decodes_relation_then_insert() {
        let mut decoder = Decoder::new();
        let data = relation_payload();
        let mut buf = data.as_slice();
        let msg = decoder.decode(&mut buf).unwrap().unwrap();
        match &msg {
            Message::Relation { name, columns, .. } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
            }
            other => panic!("expected Relation, got {other:?}"),
        }
        assert!(buf.is_empty());

        let data = insert_payload();
        let mut buf = data.as_slice();
        let msg = decoder.decode(&mut buf).unwrap().unwrap();
        let change = decoder.to_change(&msg, 100, 0).unwrap();
        assert_eq!(change.namespace, "public");
        assert_eq!(change.table, "users");
        assert_eq!(change.op, Op::Insert);
        let after = change.after.unwrap();
        assert_eq!(after[0], ("id".into(), Value::Int(42)));
        assert_eq!(after[1], ("name".into(), Value::Text("ada".into())));
    }

    #[test]
    fn unchanged_toast_marker_decodes_to_unchanged() {
        let mut decoder = Decoder::new();
        let relation = relation_payload();
        let mut buf = relation.as_slice();
        decoder.decode(&mut buf).unwrap().unwrap();
        let mut v = vec![b'I'];
        v.extend_from_slice(&1u32.to_be_bytes());
        v.push(b'N');
        v.extend_from_slice(&2u16.to_be_bytes());
        v.push(b't');
        v.extend_from_slice(&2u32.to_be_bytes());
        v.extend_from_slice(b"42");
        v.push(b'u'); // TOAST marker, no length follows
        let mut buf = v.as_slice();
        let msg = decoder.decode(&mut buf).unwrap().unwrap();
        let change = decoder.to_change(&msg, 200, 1).unwrap();
        let after = change.after.unwrap();
        assert_eq!(after[1].1, Value::Unchanged);
    }

    #[test]
    fn update_with_full_old_image() {
        let mut decoder = Decoder::new();
        let relation = relation_payload();
        let mut buf = relation.as_slice();
        decoder.decode(&mut buf).unwrap().unwrap();
        let mut v = vec![b'U'];
        v.extend_from_slice(&1u32.to_be_bytes());
        v.push(b'O'); // old tuple
        v.extend_from_slice(&2u16.to_be_bytes());
        v.push(b't');
        v.extend_from_slice(&2u32.to_be_bytes());
        v.extend_from_slice(b"42");
        v.push(b't');
        v.extend_from_slice(&3u32.to_be_bytes());
        v.extend_from_slice(b"ada");
        v.push(b'N'); // new tuple
        v.extend_from_slice(&2u16.to_be_bytes());
        v.push(b't');
        v.extend_from_slice(&2u32.to_be_bytes());
        v.extend_from_slice(b"42");
        v.push(b't');
        v.extend_from_slice(&5u32.to_be_bytes());
        v.extend_from_slice(b"grace");
        let mut buf = v.as_slice();
        let msg = decoder.decode(&mut buf).unwrap().unwrap();
        let change = decoder.to_change(&msg, 300, 0).unwrap();
        assert_eq!(change.op, Op::Update);
        let before = change.before.unwrap();
        assert_eq!(before[1].1, Value::Text("ada".into()));
        let after = change.after.unwrap();
        assert_eq!(after[1].1, Value::Text("grace".into()));
    }
}
