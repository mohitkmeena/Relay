## Summary

What this PR changes and why. Link the issue it closes (`Closes #N`).

## How was this tested?

Unit tests, differential test run, manual verification against local
Postgres — whatever applies.

## Checklist

- [ ] `cargo fmt --all --check` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] `cargo test --workspace` passes
- [ ] New public items have doc comments
- [ ] `Value::Unchanged` handled explicitly in any new match
