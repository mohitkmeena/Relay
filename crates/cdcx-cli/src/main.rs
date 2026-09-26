//! cdcx: change data capture from PostgreSQL, with incremental views.

use anyhow::{Context, Result};
use cdcx_engine::{metrics, Engine, MaterializedView, ViewConfig};
use cdcx_pg::{Reader, ReaderConfig};
use cdcx_plan::{Compiled, Plan, SinkSpec};
use cdcx_sink::{ClickHouseSink, ConsoleSink, DynSink, FanOutSink, HttpSink};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // Subcommand: validate a plan file and exit.
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("plan") {
        return match args.get(2).map(String::as_str) {
            Some("check") => {
                let path = args
                    .get(3)
                    .map(String::as_str)
                    .unwrap_or("pipeline.yaml");
                check_plan(path)
            }
            _ => {
                eprintln!("usage: cdcx plan check <pipeline.yaml>");
                std::process::exit(2);
            }
        };
    }

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

    let metrics = metrics();

    // Materialized view (M7), if CDCX_VIEW is set; state restores from
    // the checkpoint so a restart continues where it left off.
    let mut engine = Engine::new(compiled, checkpoint_path()).context("loading checkpoint")?;
    if let Some(config) = view_config_from_env() {
        let mut view = MaterializedView::new(config);
        engine.restore_view(&mut view);
        engine = engine.with_view(view);
    }

    let mut sink = build_sink(plan.sink.as_ref())?;

    // Graceful shutdown: SIGTERM/SIGINT stop the pipeline between
    // transactions. The checkpoint is always consistent with what the
    // sink confirmed, so a stop is safe at any point.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    install_signal_handlers(stop.clone());

    // Ops loops (M6): heartbeat + lag metric, on their own connection.
    let ops_task = tokio::spawn(ops_loop(reader_config.clone(), metrics.clone(), stop.clone()));

    // CDCX_SNAPSHOT=1: backfill from an exported snapshot, then stream.
    if std::env::var("CDCX_SNAPSHOT").ok().as_deref() == Some("1") {
        let tables: Vec<String> = match std::env::var("CDCX_SNAPSHOT_TABLES") {
            Ok(list) => list.split(',').map(|t| t.trim().to_string()).collect(),
            Err(_) => plan.tables.keys().cloned().collect(),
        };
        info!(?tables, "running initial snapshot backfill");
        cdcx_pg::backfill_then_stream(&reader_config, &tables, |txn| {
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                info!("shutdown requested; stopping mid-backfill");
                std::process::exit(0);
            }
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
                if stop.load(std::sync::atomic::Ordering::SeqCst) {
                    info!("shutdown requested; stopping between transactions");
                    std::process::exit(0);
                }
                if let Err(e) = futures::executor::block_on(engine.process(txn, &mut sink)) {
                    // Sink failure blocks the watermark; stop rather
                    // than silently lose the transaction.
                    eprintln!("fatal: transaction processing failed: {e:#}");
                    std::process::exit(1);
                }
            })
            .await
            .context("replication reader failed")?;
    }

    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    ops_task.abort();
    info!(
        acked_lsn = engine.acked_lsn(),
        metrics = engine_metrics_line(),
        checkpoint = ?engine.checkpoint_path(),
        "cdcx stopped"
    );
    Ok(())
}

fn engine_metrics_line() -> String {
    String::new() // replaced by the ops loop's logging; kept for signature stability
}

fn install_signal_handlers(stop: Arc<std::sync::atomic::AtomicBool>) {
    tokio::spawn(async move {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        info!("signal received; will stop at next transaction boundary");
    });
}

/// Heartbeat + slot-lag loop on its own normal connection (M6).
async fn ops_loop(
    config: ReaderConfig,
    metrics: cdcx_engine::MetricsHandle,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    let dsn = format!(
        "host={} port={} user={} password={} dbname={}",
        config.host, config.port, config.user, config.password, config.database
    );
    loop {
        if stop.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        match tokio_postgres::connect(&dsn.clone(), tokio_postgres::NoTls).await {
            Ok((client, conn)) => {
                let conn_task = tokio::spawn(conn);
                let mut ticker = tokio::time::interval(cdcx_pg::HEARTBEAT_INTERVAL);
                loop {
                    ticker.tick().await;
                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                        conn_task.abort();
                        return;
                    }
                    if let Err(e) = cdcx_pg::emit_heartbeat(&client).await {
                        tracing::warn!(error = %e, "heartbeat failed");
                    }
                    match cdcx_pg::slot_lag(&client, &config.slot).await {
                        Ok(Some(lag)) => info!(
                            lag_bytes = lag.bytes,
                            confirmed = lag.confirmed_flush_lsn,
                            metrics = metrics.snapshot(),
                            "slot status"
                        ),
                        Ok(None) => tracing::debug!("slot not active yet"),
                        Err(e) => tracing::warn!(error = %e, "lag query failed"),
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "ops connection failed; retrying in 5s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

fn build_sink(spec: Option<&SinkSpec>) -> Result<DynSink> {
    let spec = match spec {
        None => return Ok(DynSink::new(ConsoleSink::new())),
        Some(s) => s,
    };
    Ok(match spec {
        SinkSpec::Console => DynSink::new(ConsoleSink::new()),
        SinkSpec::Http { url } => DynSink::new(HttpSink::new(url.clone())?),
        SinkSpec::Clickhouse { url, database } => {
            DynSink::new(ClickHouseSink::new(url.clone(), database.clone())?)
        }
        SinkSpec::Fanout { sinks } => {
            let mut built = Vec::new();
            for s in sinks {
                built.push(build_sink(Some(s))?);
            }
            DynSink::new(FanOutSink::new(built))
        }
    })
}

/// `cdcx plan check <path>`: parse + compile the plan, report the
/// operator pipeline per table, and exit non-zero on any error.
fn check_plan(path: &str) -> Result<()> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?;
    let plan: Plan = serde_yaml::from_str(&raw).context("parsing plan YAML")?;
    let compiled = Compiled::compile(&plan).context("compiling plan")?;
    if plan.tables.is_empty() {
        println!("plan OK (no tables; everything passes through)");
        return Ok(());
    }
    for (table, ops) in &compiled.tables {
        println!("{table}:");
        for op in ops {
            let desc = match op {
                cdcx_plan::PlanOp::Select(cols) => {
                    format!("select [{}]", cols.join(", "))
                }
                cdcx_plan::PlanOp::Rename(from) => {
                    let pairs: Vec<String> =
                        from.iter().map(|(k, v)| format!("{k}->{v}")).collect();
                    format!("rename {{{}}}", pairs.join(", "))
                }
                cdcx_plan::PlanOp::Cast { column, to } => {
                    format!("cast {column} -> {to:?}")
                }
                cdcx_plan::PlanOp::Redact(cols) => format!("redact [{}]", cols.join(", ")),
                cdcx_plan::PlanOp::Filter(_) => "filter <expr>".to_string(),
                cdcx_plan::PlanOp::Map { column, expr } => {
                    format!("map {column} = {expr:?}")
                }
            };
            println!("  - {desc}");
        }
    }
    for (table, keys) in &compiled.keys {
        println!("key {table}: [{}]", keys.join(", "));
    }
    println!("plan OK ({} table(s))", compiled.tables.len());
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
