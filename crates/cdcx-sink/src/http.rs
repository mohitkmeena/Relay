//! HTTP webhook sink: POSTs each transaction as JSON, with retries.

use std::time::Duration;

use cdcx_model::Txn;
use serde::Serialize;

use crate::SinkError;

use super::Sink;

/// JSON body posted per transaction.
#[derive(Serialize)]
struct TxnEnvelope<'a> {
    commit_lsn: u64,
    changes: &'a [cdcx_model::Change],
}

/// POSTs transactions to a webhook URL.
///
/// Durability contract: `write_txn` returns `Ok` only after the remote
/// responds 2xx. Non-2xx responses and transport errors are retried with
/// exponential backoff up to `max_attempts`, after which the write fails
/// and the engine stops advancing its watermark (blocking the source) —
/// deliberate backpressure rather than data loss.
pub struct HttpSink {
    url: String,
    client: reqwest::Client,
    max_attempts: u32,
    initial_backoff: Duration,
}

impl HttpSink {
    /// Create a sink posting to `url`.
    pub fn new(url: impl Into<String>) -> Result<Self, SinkError> {
        Ok(Self {
            url: url.into(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| SinkError::Config(e.to_string()))?,
            max_attempts: 8,
            initial_backoff: Duration::from_millis(100),
        })
    }

    /// Override retry behavior.
    pub fn with_retry(mut self, max_attempts: u32, initial_backoff: Duration) -> Self {
        self.max_attempts = max_attempts;
        self.initial_backoff = initial_backoff;
        self
    }
}

impl Sink for HttpSink {
    async fn write_txn(&mut self, txn: &Txn) -> Result<(), SinkError> {
        let body = TxnEnvelope {
            commit_lsn: txn.commit_lsn,
            changes: &txn.changes,
        };
        let mut backoff = self.initial_backoff;
        let mut last_err = None;
        for attempt in 1..=self.max_attempts {
            match self.client.post(&self.url).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => {
                    last_err = Some(SinkError::Write(format!(
                        "attempt {attempt}: HTTP {}",
                        resp.status()
                    )));
                }
                Err(e) => {
                    last_err = Some(SinkError::Write(format!("attempt {attempt}: {e}")));
                }
            }
            if attempt < self.max_attempts {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
        }
        Err(last_err.unwrap_or(SinkError::Write("no attempts made".into())))
    }
}
