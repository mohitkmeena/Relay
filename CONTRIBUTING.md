# Contributing to cdcx

Thanks for your interest in contributing! This document covers the
workflow, code style, and where work is tracked.

## Getting started

```sh
git clone https://github.com/mohit-meena/relay.git
cd relay
docker compose up -d     # local Postgres, wal_level=logical
cargo test --workspace    # everything should pass
```

Rust stable (1.88+). If `cargo test` needs Postgres (the differential
harness in M4 will), the compose file provides it on `localhost:5432`.

## Workflow

1. Open an issue before large changes; link it from your PR.
2. Fork / branch from `main`. Branch naming: `feat/…`, `fix/…`,
   `docs/…`, `chore/…`.
3. Keep PRs small and focused. One logical change per PR.
4. Every PR must pass `cargo fmt --check`, `cargo clippy`, and
   `cargo test`. CI enforces this.
5. Squash-merge; the PR title becomes the commit subject.

## Code style

- `cargo fmt` output is authoritative. No custom style debates.
- Library crates use `thiserror`; only the CLI uses `anyhow`.
- Public items carry doc comments; `missing_docs` is a warning and
  CI keeps it at zero.
- The `Value::Unchanged` variant (TOAST marker) must be handled
  explicitly everywhere it's matched — never silently discarded.
- Keep `cdcx-model` I/O-free and async-free. It's the shared vocabulary
  of the pipeline.
- Decoder code must not depend on the plan crate; the M4 oracle depends
  on that separation to be a meaningful cross-check.

## Commit messages

Conventional style: `area: imperative summary`, e.g.
`decoder: handle key-only tuples in DELETE messages`. Wrap at 72
characters; body explains why, not what.

## Testing expectations

- Decoder: table-driven unit tests with hand-built pgoutput byte
  payloads (see `crates/cdcx-pg/src/decoder.rs` tests).
- Pipeline: differential tests against real Postgres (M4+).
- Bug fixes come with a test that fails before the fix.

## Milestones

Work is organized in milestones M1–M7 (see README roadmap). M1–M4 are
the correctness core; M5–M7 turn it into a product. Pick an issue
labeled `good first issue` to start.

## Reporting security issues

Please don't open public issues for security problems. 
Email `mohitln.developer@gmail.com` the maintainer instead (see the GitHub profile).