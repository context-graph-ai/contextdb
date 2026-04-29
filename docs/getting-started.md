# Getting Started

Run contextdb in under a minute.

---

## Install

### Download a pre-built binary (recommended)

Download the latest release for your platform from [GitHub Releases](https://github.com/context-graph-ai/contextdb/releases/latest):

```bash
# Linux x86_64
curl -fsSL https://github.com/context-graph-ai/contextdb/releases/latest/download/x86_64-unknown-linux-gnu.tar.gz | tar xz
# Linux ARM64
curl -fsSL https://github.com/context-graph-ai/contextdb/releases/latest/download/aarch64-unknown-linux-gnu.tar.gz | tar xz
# macOS Intel
curl -fsSL https://github.com/context-graph-ai/contextdb/releases/latest/download/x86_64-apple-darwin.tar.gz | tar xz
# macOS Apple Silicon
curl -fsSL https://github.com/context-graph-ai/contextdb/releases/latest/download/aarch64-apple-darwin.tar.gz | tar xz
```

### Install via cargo

```bash
cargo install contextdb-cli
```

### Build from source

Requires [Rust stable](https://rustup.rs/) (1.75+) and Git.

```bash
git clone https://github.com/context-graph-ai/contextdb.git
cd contextdb
cargo build --release -p contextdb-cli
```

## First REPL Session

```bash
contextdb-cli :memory:
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

Audit-frozen columns — declare `IMMUTABLE` on any column whose value must never be silently rewritten after the initial INSERT:

```sql
CREATE TABLE audit_decisions (
  id UUID PRIMARY KEY,
  decision_type TEXT NOT NULL IMMUTABLE,
  description TEXT NOT NULL IMMUTABLE,
  status TEXT NOT NULL DEFAULT 'active'
);

INSERT INTO audit_decisions (id, decision_type, description)
VALUES ('550e8400-e29b-41d4-a716-446655440001', 'sql-migration', 'adopt contextdb');

-- Mutable column: succeeds
UPDATE audit_decisions SET status = 'superseded'
WHERE id = '550e8400-e29b-41d4-a716-446655440001';

-- Flagged column: rejected with Error::ImmutableColumn
UPDATE audit_decisions SET decision_type = 'other'
WHERE id = '550e8400-e29b-41d4-a716-446655440001';
-- Error: column `decision_type` on table `audit_decisions` is immutable
```

The row stays at its original `decision_type`; the session continues. To record a correction, INSERT a new row and mark the original `superseded`.

## Indexes and Scale

contextdb is designed for agents holding tens of thousands of entities with
sub-100ms filtered retrieval. Indexes accelerate filtered scans so a
10,000-row table answers `WHERE tag = 'x'` in microseconds instead of
milliseconds.

```sql
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  tag TEXT,
  value INTEGER
);

CREATE INDEX idx_tag ON observations (tag);

INSERT INTO observations (id, tag, value)
VALUES ('650e8400-e29b-41d4-a716-446655440010', 'pay', 1);

SELECT value FROM observations WHERE tag = 'pay';
```

`.explain` shows which plan ran — `IndexScan` for the filtered select above,
`Scan` for a query whose WHERE clause does not match a declared index.

## Rank Vector Search by Outcomes

Vector search can use a schema-declared rank policy when cosine similarity is
not the only signal. The joined column must be indexed:

```bash
contextdb-cli :memory: <<'SQL'
CREATE TABLE outcomes (
  id UUID PRIMARY KEY,
  decision_id UUID NOT NULL,
  success BOOLEAN NOT NULL
);
CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id);

CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  confidence REAL,
  embedding VECTOR(2) RANK_POLICY (
    JOIN outcomes ON decision_id,
    FORMULA 'coalesce({confidence}, 1.0) * coalesce({success}, 1.0)',
    SORT_KEY effective_confidence
  )
);

INSERT INTO decisions (id, description, confidence, embedding) VALUES
  ('11111111-1111-1111-1111-111111111111', 'closest but failed', 1.0, [1.0, 0.0]),
  ('22222222-2222-2222-2222-222222222222', 'less similar but worked', 1.0, [0.5, 0.0]),
  ('33333333-3333-3333-3333-333333333333', 'fallback with no outcome', 0.25, [0.75, 0.0]);

INSERT INTO outcomes (id, decision_id, success) VALUES
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '11111111-1111-1111-1111-111111111111', FALSE),
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '22222222-2222-2222-2222-222222222222', TRUE);

SELECT id, description, confidence
FROM decisions
ORDER BY embedding <=> [1.0, 0.0] USE RANK effective_confidence
LIMIT 5;
SQL
```

Expected ordering:

```text
22222222-2222-2222-2222-222222222222 | less similar but worked | 1.0
33333333-3333-3333-3333-333333333333 | fallback with no outcome | 0.25
11111111-1111-1111-1111-111111111111 | closest but failed | 1.0
```

The formula and join path are resolved at DDL time, stored with the schema, and
replicated through sync. `JOIN outcomes ON decision_id` looks up
`outcomes.decision_id` through `outcomes_decision_id_idx` and compares it with
`decisions.id`. Search results are ranked by the formula before the top-k
cutoff. On large HNSW-backed indexes, this ranking applies to the vector
candidates returned by ANN retrieval; use a single current summary row on the
joined side when outcome ranking must be deterministic.

Rank policies are schema. To change a policy today, recreate the affected table
with the new `RANK_POLICY` clause and reload the rows:

```sql
DROP TABLE decisions;

CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  confidence REAL,
  embedding VECTOR(2) RANK_POLICY (
    JOIN outcomes ON decision_id,
    FORMULA 'coalesce({vector_score}, 0.0) * coalesce({confidence}, 1.0) * coalesce({success}, 1.0)',
    SORT_KEY effective_confidence
  )
);
```

## Persist to Disk

Replace `:memory:` with a file path. Everything else works the same:

```bash
contextdb-cli ./my.db
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
contextdb-engine = "0.3.4"
contextdb-core = "0.3.4"
```

## What's Next

- [Why contextdb?](why-contextdb.md) — the problems it solves and how it compares to alternatives
- [Usage Scenarios](usage-scenarios.md) — 16 problem-first walkthroughs with SQL
- [Query Language](query-language.md) — full SQL, graph, and vector reference
- [CLI Reference](cli.md) — REPL commands, sync, scripting
