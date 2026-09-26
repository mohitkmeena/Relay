//! cdcx: change data capture from PostgreSQL, with incremental views.

use anyhow::{Context, Result};
use cdcx_engine::{Engine, MaterializedView, ViewConfig};
use cdcx_pg::{Reader, ReaderConfig};
use cdcx_plan::{Compiled, Plan};
use cdcx_sink::ConsoleSink;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let reader_config = reader_config_from_env()?;
    let plan = load_plan()?;
    let compiled = Compiled::compile(&plan).context("compiling pipeline plan")?;

    info!(
        host = %reader_config.host,
        database = %reader_config.database,
        slot = %reader_config.slot,
        tables = compiled.tables.len(),
        "starting cdcx"
    );

    // Materialized view (M7), if CDCX_VIEW is set ("g:sum:v" style).
    let engine = Engine::new(compiled, checkpoint_path()).context("loading checkpoint")?;
    let mut engine = match view_config_from_env() {
        Some(config) => {
            let view = MaterializedView::new(config);
            engine.with_view(view)
        }
        None => engine,
    };

    let mut sink = ConsoleSink::new();

    // CDCX_SNAPSHOT=1: backfill from an exported snapshot, then stream.
    // Tables to backfill default to every table in the plan.
    if std::env::var("CDCX_SNAPSHOT").ok().as_deref() == Some("1") {
        let tables: Vec<String> = match std::env::var("CDCX_SNAPSHOT_TABLES") {
            Ok(list) => list.split(',').map(|t| t.trim().to_string()).collect(),
            Err(_) => plan.tables.keys().cloned().collect(),
        };
        info!(?tables, "running initial snapshot backfill");
        cdcx_pg::backfill_then_stream(&reader_config, &tables, |txn| {
            if let Err(e) = futures::executor::block_on(engine.process(txn, &mut sink)) {
                eprintln!("fatal: transaction processing failed: {e:#}");
                std::process::exit(1);
            }
        })
        .await
        .context("backfill + stream failed")?;
    } else {
        let reader = Reader::new(reader_config);
        reader
            .run(|txn| {
                // Drive the async engine from the sync reader callback. A
                // failed transaction aborts the process: the checkpoint has
                // not advanced, so a restart replays this txn (at-least-once).
                if let Err(e) = futures::executor::block_on(engine.process(txn, &mut sink)) {
                    // Sink failure blocks the watermark; stop rather than
                    // silently lose the transaction.
                    eprintln!("fatal: transaction processing failed: {e:#}");
                    std::process::exit(1);
                }
            })
            .await
            .context("replication reader failed")?;
    }

    info!(
        acked_lsn = engine.acked_lsn(),
        checkpoint = ?engine.checkpoint_path(),
        "cdcx stopped"
    );
    Ok(())
}

fn checkpoint_path() -> std::path::PathBuf {
    std::env::var("CDCX_CHECKPOINT")
        .map(Into::into)
        .unwrap_or_else(|_| "checkpoint.json".into())
}

fn load_plan() -> Result<Plan> {
    match std::env::var("CDCX_PLAN") {
        Ok(path) => {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading plan file {path}"))?;
            let plan: Plan = serde_yaml::from_str(&raw).context("parsing plan YAML")?;
            Ok(plan)
        }
        Err(_) => Ok(Plan::default()),
    }
}

/// Parse CDCX_VIEW as "key_col:sum:value_col" or "key_col:count".
fn view_config_from_env() -> Option<ViewConfig> {
    let spec = std::env::var("CDCX_VIEW").ok()?;
    let parts: Vec<&str> = spec.split(':').collect();
    match parts.as_slice() {
        [keys, "count"] => Some(ViewConfig {
            key_columns: keys.split(',').map(String::from).collect(),
            sum_column: None,
            count: true,
            ..ViewConfig::default()
        }),
        [keys, "sum", col] => Some(ViewConfig {
            key_columns: keys.split(',').map(String::from).collect(),
            sum_column: Some((*col).into()),
            count: true,
            ..ViewConfig::default()
        }),
        _ => {
            tracing::warn!(spec = %spec, "CDCX_VIEW not understood; expected 'k:count' or 'k:sum:v'");
            None
        }
    }
}

fn reader_config_from_env() -> Result<ReaderConfig> {
    let mut config = ReaderConfig::default();
    if let Ok(host) = std::env::var("CDCX_PG_HOST") {
        config.host = host;
    }
    if let Ok(port) = std::env::var("CDCX_PG_PORT") {
        config.port = port.parse().context("CDCX_PG_PORT is not a number")?;
    }
    if let Ok(user) = std::env::var("CDCX_PG_USER") {
        config.user = user;
    }
    if let Ok(password) = std::env::var("CDCX_PG_PASSWORD") {
        config.password = password;
    }
    if let Ok(database) = std::env::var("CDCX_PG_DATABASE") {
        config.database = database;
    }
    if let Ok(slot) = std::env::var("CDCX_PG_SLOT") {
        config.slot = slot;
    }
    if let Ok(publication) = std::env::var("CDCX_PG_PUBLICATION") {
        config.publication = publication;
    }
    Ok(config)
}
