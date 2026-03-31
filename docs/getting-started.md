# Getting Started

Build and run contextdb in under 2 minutes.

---

## Prerequisites

- [Rust stable](https://rustup.rs/) (1.75+)
- Git

## Build

```bash
git clone https://github.com/context-graph-ai/contextdb.git
cd contextdb
cargo build --release -p contextdb-cli
```

This builds only the CLI binary and its dependencies (~90 seconds on a modern machine).

## First REPL Session

```bash
./target/release/contextdb-cli :memory:
```

Try the state machine — the feature that makes contextdb different from plain SQL:

```sql
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  reasoning TEXT
) STATE MACHINE (status: draft -> [active, rejected], active -> [superseded]);

INSERT INTO decisions (id, status, reasoning)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 'draft', 'initial assessment');

-- Valid transition: draft -> active
UPDATE decisions SET status = 'active'
WHERE id = '550e8400-e29b-41d4-a716-446655440000';

-- Invalid transition: active -> draft (rejected by the database)
UPDATE decisions SET status = 'draft'
WHERE id = '550e8400-e29b-41d4-a716-446655440000';
-- Error: invalid state transition: active -> draft
```

The database enforces the state machine. No application code needed.

## Persist to Disk

Replace `:memory:` with a file path. Everything else works the same:

```bash
./target/release/contextdb-cli ./my.db
```

Single file. Crash-safe via redb. Reopen and your data is there.

## Use as a Library

contextdb is an embedded database — the CLI is for exploration. The primary interface is the Rust API:

```rust
use contextdb_engine::Database;
use contextdb_core::Value;
use std::collections::HashMap;

let db = Database::open(std::path::Path::new("./my.db"))?;

let params = HashMap::new();
db.execute(
    "CREATE TABLE observations (
       id UUID PRIMARY KEY,
       data JSON,
       embedding VECTOR(384)
     ) IMMUTABLE",
    &params,
)?;

// Insert with parameters
let mut params = HashMap::new();
params.insert("id".into(), Value::Uuid(uuid::Uuid::new_v4()));
params.insert("data".into(), Value::Json(serde_json::json!({"type": "sensor"})));
params.insert("embedding".into(), Value::Vector(vec![0.1; 384]));

db.execute(
    "INSERT INTO observations (id, data, embedding) VALUES ($id, $data, $embedding)",
    &params,
)?;
```

Add to your `Cargo.toml`:

```toml
[dependencies]
contextdb-engine = { git = "https://github.com/context-graph-ai/contextdb" }
contextdb-core = { git = "https://github.com/context-graph-ai/contextdb" }
```

> contextdb is not yet published to crates.io. Git dependencies are required until the first crates.io release.

## What's Next

- [Why contextdb?](why-contextdb.md) — the problems it solves and how it compares to alternatives
- [Usage Scenarios](usage-scenarios.md) — 16 problem-first walkthroughs with SQL
- [Query Language](query-language.md) — full SQL, graph, and vector reference
- [CLI Reference](cli.md) — REPL commands, sync, scripting
