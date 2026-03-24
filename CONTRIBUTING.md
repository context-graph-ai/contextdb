# Contributing to contextdb

## Build and Test

Run all four verification checks before submitting:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
```

## Crate Layout

All crates live under `crates/`:

| Crate | Purpose |
|---|---|
| `crates/contextdb-core` | Shared types, errors, and traits |
| `crates/contextdb-tx` | MVCC transaction manager |
| `crates/contextdb-relational` | Relational (row) storage |
| `crates/contextdb-graph` | Graph edge storage and BFS |
| `crates/contextdb-vector` | Vector storage and ANN search |
| `crates/contextdb-parser` | SQL/PGQ parser (pest grammar) |
| `crates/contextdb-planner` | Query planner |
| `crates/contextdb-engine` | Database engine (orchestrates all subsystems) |
| `crates/contextdb-server` | NATS sync server and client |
| `crates/contextdb-cli` | CLI REPL |
