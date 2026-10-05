# cdcx

Change data capture (CDC) from PostgreSQL, in Rust. cdcx decodes the
PostgreSQL logical replication stream (pgoutput) into typed row-level
change events and delivers them to sinks with at-least-once semantics.

## Status

Early development (M1: reader + decoder). See the [roadmap](#roadmap) below.

## Quickstart (Docker)

```sh
# 1. Start local Postgres with wal_level=logical (requires Docker)
docker compose up -d

# 2. Build and run cdcx
cargo run --release
```

You should see `cdcx` connect to slot `cdcx_slot` on publication
`cdcx_pub`. Then, in another terminal:

```sh
docker exec -it cdcx-postgres psql -U postgres -d postgres -c \
  "INSERT INTO users (name, email, balance) VALUES ('ada', 'ada@example.com', 100.50);"
```

and cdcx prints the decoded change:

```
=== txn commit_lsn=... changes=1 ===
  INSERT public.users lsn=... #0
    after:  [("id", 1), ("name", "ada"), ...]
```

Point it at a real server by setting the environment variables below;
nothing in cdcx assumes Docker.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `CDCX_PG_HOST` | `localhost` | PostgreSQL host |
| `CDCX_PG_PORT` | `5432` | PostgreSQL port |
| `CDCX_PG_USER` | `postgres` | Replication user (needs `REPLICATION` privilege) |
| `CDCX_PG_PASSWORD` | `postgres` | Password |
| `CDCX_PG_DATABASE` | `postgres` | Database |
| `CDCX_PG_SLOT` | `cdcx_slot` | Replication slot (persistent, survives restarts) |
| `CDCX_PG_PUBLICATION` | `cdcx_pub` | Publication to consume |

`RUST_LOG` controls log verbosity (`info`, `debug`, `cdcx_pg=trace`).

## Roadmap

- [x] **M0** — Spike: choose replication client (`pgwire-replication` 0.4)
- [x] **M1** — Reader + pgoutput decoder (Relation/Insert/Update/Delete/Commit, TOAST marker)
- [x] **M2** — Normalizer + ack watermark + atomic file checkpoint (crash-safe at-least-once)
- [x] **M3** — YAML plan: select / rename / cast / redact / filter (enter/leave semantics)
- [x] **M4** — Differential test harness: oracle vs Z-set views, incl. crash-replay (Postgres-backed harness pending)
- [x] **M5** — Sinks: console, HTTP webhook w/ retries, ClickHouse (ReplacingMergeTree)
- [x] **M6** — Ops: exported-snapshot backfill, slot lag metric, heartbeat, DDL/drift detection
- [x] **M7** — Incremental view maintenance: Z-set core, SUM/COUNT group-by, tumbling windows, reference join, atomic state+LSN

> All milestone code is written but **not yet compiled or tested** — see Testing.

## Development

```sh
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

> **Status note:** M0–M7 code is written, compiled, and unit/differential
> tests pass. The Postgres-backed differential test requires `docker
> compose up -d postgres` and runs with `cargo test --test
> differential_postgres -- --ignored`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow, style, and
milestone map. New to the codebase? Start with `crates/cdcx-model` — it
has no dependencies and defines everything that flows through the
pipeline.

## License

[MIT](LICENSE)
