//! Durable checkpoint and operator state for cdcx.
//!
//! The central invariant (M2, hardened in M7): **state and LSN commit
//! atomically**. A checkpoint records the highest transaction LSN whose
//! effects are durable downstream, plus the state of stateful operators.
//! On restart, the pipeline resumes from `confirmed_flush_lsn` and the
//! saved operator state; anything after is replayed (at-least-once).
//!
//! M2 ships the in-memory + JSON-file implementation. The interface is
//! intentionally async-free so it can later back a RocksDB or similar
//! store without touching call sites.

pub mod join;
pub mod windows;
pub mod zset;

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Error returned by checkpoint operations.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// The checkpoint file could not be read or written.
    #[error("checkpoint I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The checkpoint file is not valid JSON or has an unreadable version.
    #[error("checkpoint corrupted: {0}")]
    Corrupted(String),
}

/// A persisted pipeline checkpoint.
///
/// `lsn` is only advanced once the sink has confirmed durable write of
/// every change up to and including that LSN. Anything beyond may be
/// replayed after a crash — duplicates are allowed, loss is not.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    /// Highest durably-acked transaction commit LSN (0 = nothing acked).
    pub lsn: u64,
    /// Opaque state blob for stateful operators (M7); empty until then.
    pub operator_state: serde_json::Value,
    /// Format version, for forward migration.
    pub version: u32,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            lsn: 0,
            operator_state: serde_json::Value::Null,
            version: 1,
        }
    }
}

/// Atomic (write-temp-then-rename) JSON-file checkpoint store.
///
/// Atomic rename guarantees a crash mid-write never leaves a torn
/// checkpoint: readers see either the old or the new file, never a mix.
#[derive(Debug)]
pub struct FileCheckpoint {
    path: PathBuf,
    current: Checkpoint,
}

impl FileCheckpoint {
    /// Load the checkpoint at `path`, or a fresh one if it doesn't exist.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Ok(Self {
                path,
                current: Checkpoint::default(),
            });
        }
        let raw = fs::read_to_string(&path)?;
        let current: Checkpoint =
            serde_json::from_str(&raw).map_err(|e| CheckpointError::Corrupted(e.to_string()))?;
        if current.version != 1 {
            return Err(CheckpointError::Corrupted(format!(
                "unsupported checkpoint version {}",
                current.version
            )));
        }
        Ok(Self { path, current })
    }

    /// The currently durable checkpoint.
    pub fn get(&self) -> &Checkpoint {
        &self.current
    }

    /// Persist a new checkpoint atomically.
    ///
    /// Callers must only advance `lsn` past the current value after the
    /// sink has confirmed durability — this method trusts the engine's
    /// ordering, it does not re-verify it.
    pub fn advance(&mut self, checkpoint: Checkpoint) -> Result<(), CheckpointError> {
        debug_assert!(checkpoint.lsn >= self.current.lsn, "lsn must not regress");
        let tmp = self.path.with_extension("tmp");
        let json = serde_json::to_vec_pretty(&checkpoint)
            .map_err(|e| CheckpointError::Corrupted(e.to_string()))?;
        fs::write(&tmp, json)?;
        fs::rename(&tmp, &self.path)?;
        self.current = checkpoint;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_default() {
        let dir = std::env::temp_dir().join(format!("cdcx-cp-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("checkpoint.json");

        let mut cp = FileCheckpoint::load(&path).unwrap();
        assert_eq!(cp.get().lsn, 0);
        assert!(path.exists() == false);

        cp.advance(Checkpoint {
            lsn: 42,
            operator_state: serde_json::json!({ "sums": { "m1": 5 } }),
            version: 1,
        })
        .unwrap();
        assert!(path.exists());

        let reloaded = FileCheckpoint::load(&path).unwrap();
        assert_eq!(reloaded.get().lsn, 42);
        assert_eq!(
            reloaded.get().operator_state,
            serde_json::json!({ "sums": { "m1": 5 } })
        );
        fs::remove_dir_all(&dir).ok();
    }
}
