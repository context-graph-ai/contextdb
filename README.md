<p align="center">
  <img src="assets/banner.svg" alt="contextdb" width="800">
</p>

[![CI](https://github.com/context-graph-ai/contextdb/actions/workflows/ci.yml/badge.svg)](https://github.com/context-graph-ai/contextdb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/contextdb-engine)](https://crates.io/crates/contextdb-engine)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![docs.rs](https://docs.rs/contextdb-engine/badge.svg)](https://docs.rs/contextdb-engine)

# contextdb

An embedded database for agentic memory systems. Relational storage, graph traversal, and vector similarity search under unified MVCC transactions — in a single file, in a single process. Every agent, device, or service runs its own contextdb. They sync bidirectionally through a central server by dial-by-key — reaching it through its own cryptographic identity, no broker to install — knowledge learned by one becomes available to all, with per-table conflict resolution. No port forwarding, no VPN — a node behind NAT dials outbound, and machines on one LAN sync with zero external infrastructure.

If you're building agent memory today, you're probably stitching together SQLite for state, a vector database for embeddings, and application code for graph traversal. contextdb replaces all three — and adds **enforceable policy constraints** (state machines, DAG enforcement, cascading propagation) that the database guarantees, not application code.

contextdb ships no built-in schema. You define your own tables and attach policy to them. The `decisions` table below — and the agentic-memory tables (`observations`, `intentions`, `digests`) used as running examples throughout these docs — is one example schema, not something contextdb requires:

```
contextdb> UPDATE decisions SET status = 'draft' WHERE id = '550e8400...';
Error: invalid state transition: active -> draft
```

No PostgreSQL-style validation triggers. No duplicated application-side
constraint checks. The database enforces policy invariants, while host
callbacks are reserved for explicit observation/cascade workflows.

**Familiar conventions, nothing new to learn:** PostgreSQL-compatible SQL, [pgvector](https://github.com/pgvector/pgvector) syntax for vector search (`<=>`), and [SQL/PGQ](https://www.iso.org/standard/76120.html)-style `GRAPH_TABLE ... MATCH` for graph queries — the subset that matters for bounded traversal, not the full standard.

**Language support:** contextdb is a Rust library and CLI today. Python and TypeScript bindings are on the roadmap — contributions welcome.

**Website:** [contextdb.tech](https://contextdb.tech) · **Docs:** [contextdb.tech/docs](https://contextdb.tech/docs/)

See [Why contextdb?](docs/why-contextdb.md) for the full problem statement, or jump to [Getting Started](docs/getting-started.md) to try it in 2 minutes.

## Why Not SQLite + Extensions?

| Capability | SQLite + extensions | contextdb |
|---|---|---|
| Vector search | sqlite-vec (separate extension, no unified transactions with relational data) | Built-in, auto-HNSW at 1K vectors, pre-filtered search, same MVCC transaction as rows |
| Graph traversal | Recursive CTEs (unbounded, no cycle detection) | SQL/PGQ with bounded BFS, DAG enforcement, typed edges |
| State machines | CHECK constraints + validation triggers (bypassable) | `STATE MACHINE` in DDL, enforced by the database engine |
| Atomic cross-model updates | Application-level coordination | Single MVCC transaction across relational + graph + vector |
| Sync | Build your own | Bidirectional collaborative sync — each database syncs changesets with conflict resolution, not WAL pages |
| Immutable tables | Not enforceable with bypassable validation triggers | `IMMUTABLE` keyword, enforced by the database engine |
| Cascading invalidation | Application code | `PROPAGATE` in DDL — state changes cascade along edges and FKs |

## Use It As a Library

contextdb is an embedded database. The primary interface is the Rust API:

```rust
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

let db = Database::open(std::path::Path::new("./my.db"))?;
// or: Database::open_memory() for ephemeral

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

// Vector similarity search
let mut query_params = HashMap::new();
query_params.insert("query".into(), Value::Vector(vec![0.1; 384]));

let result = db.execute(
    "SELECT id, data FROM observations ORDER BY embedding <=> $query LIMIT 10",
    &query_params,
)?;

// Graph traversal
let mut graph_params = HashMap::new();
graph_params.insert("start".into(), Value::Uuid(uuid::Uuid::new_v4()));

let result = db.execute(
    "SELECT target_id FROM GRAPH_TABLE(
       edges MATCH (a)-[:DEPENDS_ON]->{1,3}(b)
       WHERE a.id = $start
       COLUMNS (b.id AS target_id)
     )",
    &graph_params,
)?;

// Subscribe to commits
let rx = db.subscribe();
// rx is a std::sync::mpsc::Receiver<CommitEvent>
```

**Ownership:** a database file has exactly one *writer* at a time — a second
writable open of the same path (this process or another) returns
`Error::DatabaseLocked`. Reading is a separate door that does not take the write
lock: while a process owns the store, a read session is served by that owner over
its authenticated local channel, and when nobody owns it several direct readers
read the committed snapshot side by side. Either way there is no copy of the data
outside the store. See
[Store Ownership & Concurrency](docs/architecture.md#store-ownership--concurrency).

### Triggers

contextdb rejects PG-style validation triggers as an invariant mechanism:
constraints such as `STATE MACHINE`, `IMMUTABLE`, `DAG`, and `PROPAGATE` are
engine-enforced. It does support host-callback Triggers for
transactional observation and cascade writes that belong with the firing
transaction.

```rust
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

let db = Database::open_memory();
db.execute("CREATE TABLE observation (id UUID PRIMARY KEY)", &HashMap::new())?;
db.execute(
    "CREATE TABLE derived (id UUID PRIMARY KEY, observation_id UUID)",
    &HashMap::new(),
)?;
db.execute(
    "CREATE TRIGGER observation_seen ON observation WHEN INSERT",
    &HashMap::new(),
)?;

db.register_trigger_callback("observation_seen", |db, ctx| {
    db.execute_in_tx(
        ctx.tx,
        "INSERT INTO derived (id, observation_id) VALUES ($id, $observation)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
            (
                "observation".to_string(),
                ctx.row_values.get("id").cloned().unwrap_or(Value::Null),
            ),
        ]),
    )?;
    Ok(())
})?;
db.complete_initialization()?;
```

The callback runs synchronously inside the firing transaction's commit window.
Same-DB cross-thread writers wait-and-proceed inside the engine, unrelated
databases proceed independently, same-thread callback reentry receives
`CallbackReentry`, callback tx-bound handles stay isolated to the runner
thread, cron same-DB contention remains immediate, and an unhealthy wait trips
the bounded deadlock guard with a structured `tracing::warn!`.

### One Query, Three Subsystems

Find semantically similar observations within a graph neighborhood, filtered by relational predicates — a query that would take ~40 lines of Python across SQLite, ChromaDB, and a hand-rolled BFS:

```sql
WITH neighborhood AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges MATCH (start)-[:RELATES_TO]->{1,3}(related)
    WHERE start.id = $entity_id
    COLUMNS (related.id AS b_id)
  )
),
candidates AS (
  SELECT o.id, o.data, o.embedding
  FROM observations o
  INNER JOIN neighborhood n ON o.entity_id = n.b_id
  WHERE o.observation_type = 'config_change'
)
SELECT id, data FROM candidates
ORDER BY embedding <=> $query_embedding
LIMIT 5
```

One query. One transaction. One process.

Add to your `Cargo.toml`:

```toml
[dependencies]
contextdb-engine = "1.0.0"
contextdb-core = "1.0.0"
uuid = { version = "1", features = ["v4"] }
serde_json = "1"
```

## Install

```bash
# Install the CLI
cargo install contextdb-cli

# Or run the sync server
cargo install contextdb-server

# Or via Docker (no clone needed)
curl -O https://raw.githubusercontent.com/context-graph-ai/contextdb/main/docker-compose.yml
docker compose up
```

On startup the server prints its enrollment ticket to stdout — its
cryptographic identity plus reachable addresses — as `enrollment ticket: <...>`.
Copy that ticket and pass it to an edge with `--sync-endpoint` to connect (see
[CLI Reference](docs/cli.md)). For scripting, `--show-ticket` prints the bare
ticket and exits, and `--ticket-file <path>` writes it to a file. With Docker,
the ticket appears in `docker compose logs`.

The server keeps its identity in `<db-path>.fabric-identity.key` — a secret you
should back up and never commit to git (losing it changes the node's identity
and invalidates its tickets). See the [CLI Reference](docs/cli.md) for the full
list of files it writes.

## Or Explore With the CLI

```bash
cargo build --release -p contextdb-cli
./target/release/contextdb :memory:
```

```
contextdb> CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT NOT NULL)
           STATE MACHINE (status: draft -> [active, rejected], active -> [superseded]);
ok (rows_affected=0)

contextdb> INSERT INTO decisions VALUES ('550e8400-e29b-41d4-a716-446655440000', 'draft');
ok (rows_affected=1)

contextdb> UPDATE decisions SET status = 'active' WHERE id = '550e8400-e29b-41d4-a716-446655440000';
ok (rows_affected=1)

contextdb> UPDATE decisions SET status = 'draft' WHERE id = '550e8400-e29b-41d4-a716-446655440000';
Error: invalid state transition: active -> draft

contextdb> .schema decisions
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL
) STATE MACHINE (status: active -> [superseded], draft -> [active, rejected]);
```

### Two-Vector Walkthrough

```sql
CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4), vector_vision VECTOR(8));
INSERT INTO evidence (id, vector_text, vector_vision) VALUES
  ('11111111-1111-1111-1111-111111111111', [1,0,0,0], [0,1,0,0,0,0,0,0]);
SHOW VECTOR_INDEXES;
SELECT id FROM evidence ORDER BY vector_text <=> '[1,0,0,0]' LIMIT 1;
SELECT id
FROM evidence
ORDER BY vector_text <=> ROW_VECTOR('evidence', 'vector_text', '11111111-1111-1111-1111-111111111111')
LIMIT 1;
```

Each `VECTOR(N)` column is its own index, keyed by `(table, column)`. Use
`VECTOR(N) WITH (quantization = 'F32'|'SQ8'|'SQ4')` to choose the per-column
storage footprint; omitted quantization defaults to `F32`.

### Upgrading From 0.3.x

Opening a legacy vector store without the named-index format marker returns
`LegacyVectorStoreDetected`. `contextdb migrate <path>` brings the store forward
in place, writing a `<path>.bak` backup first — rehearse it on a copy, and take a
`contextdb snapshot export` beforehand. If that store is unavailable to you,
recovery is still explicit: sync from a peer already on the named-index storage
format, or recreate the schema and reimport the data.

## What It Does

**Relational (PostgreSQL-compatible SQL)** — SELECT, INSERT, UPDATE, DELETE, JOINs (INNER/LEFT), CTEs, upsert (`ON CONFLICT DO UPDATE`), DISTINCT, LIMIT, IN with subqueries, LIKE, BETWEEN, parameter binding (`$name`).

**Graph (SQL/PGQ-style)** — `GRAPH_TABLE(... MATCH ...)` following SQL/PGQ conventions for bounded BFS, typed edges, variable-length paths (`{1,3}`), and direction control. DAG constraint enforcement prevents cycles. State propagation cascades changes along graph edges.

**Vector (pgvector conventions)** — Cosine similarity search via `<=>`. Query with a bound vector, vector literal, or `ROW_VECTOR('table', 'column', key)` to reuse a persisted row vector as the query vector. Every `VECTOR(N)` column is a named index; `SHOW VECTOR_INDEXES` reports table, column, dimension, quantization, vector count, and bytes. F32 auto-switches between brute-force (< 1000 vectors) and HNSW indexing; SQ8/SQ4 keep exact search through 5000 vectors to preserve self-recall. Pre-filtered search narrows candidates before scoring.

**Unified transactions** — One transaction atomically updates relational rows, graph adjacency structures, and vector indexes. One read snapshot sees consistent state across all three. MVCC with consistent snapshots — readers never block writers.

**Enforceable policy constraints** — `IMMUTABLE` tables, `STATE MACHINE` column transitions, `DAG` cycle prevention, single-column and composite foreign keys, `RETAIN` with TTL expiry, `PROPAGATE` for cascading state changes along edges and foreign keys. Enforced by the database — no application code can bypass them.

**Collaborative sync** — Every contextdb instance is a full read-write database. Each runs a SyncClient that syncs bidirectionally with a central SyncServer by dial-by-key: the server is reached through its own cryptographic identity (its enrollment ticket), not a broker address — nothing to install or expose. Machines on one LAN sync with zero external infrastructure and no internet, over direct connections; the default configuration contacts no third-party service. Crossing networks, the operator either self-hosts a small stateless relay or opts into the free public relays — connectivity is never a paid feature. Offline-first: each database works independently, syncing changesets when connected. Tables declare conflict handling as `SYNC CONFLICT KEEP FIRST` or `KEEP LATEST`, and declare travel as `SYNC PUSH ONLY`, `PULL ONLY`, `TWO WAY`, or `OFF`; the database persists, displays, transports, and honors those same words. The server runs the same contextdb engine — self-host it, or point your databases at a hosted server.

**Persistence** — Single-file storage via redb. Crash-safe. Compute/storage separated via the `WriteSetApplicator` trait (local redb for open source, object store for enterprise).

**Plugin system** — `DatabasePlugin` trait with lifecycle hooks (`pre_commit`, `post_commit`, `on_open`, `on_close`, `on_ddl`, `on_query`, `post_query`, `health`, `describe`, `on_sync_push`, `on_sync_pull`). Applications inject plugins via `Database::open_with_plugin()`.

**Subscriptions** — `db.subscribe()` returns a `std::sync::mpsc::Receiver<CommitEvent>`, one per subscriber, and every commit is fanned out to all of them. `db.subscribe_with_capacity(n)` sets the per-subscriber queue depth.

## Scale Envelope

contextdb is designed for agentic memory, not data warehousing:

- 10K-1M rows
- Sparse graphs with bounded traversal (depth <= 10)
- Append-heavy writes, small transactions
- Configurable memory budget via `SET MEMORY_LIMIT` (no hard-coded ceiling)
- Configurable file-growth budget via `SET DISK_LIMIT` / `SHOW DISK_LIMIT` or `--disk-limit` for file-backed databases
- See [Architecture](docs/architecture.md#memory-limit-on-edge-devices) for memory-limit behavior on edge devices.
- Laptops, ARM64 devices (browser and mobile via Rust's WASM target are future directions)

## Documentation

Full documentation is available at [contextdb.tech/docs](https://contextdb.tech/docs/), or browse the source files:

| Doc | What it covers |
|-----|---------------|
| **[Capability Index](docs/capability-index.md)** | One page: what contextdb is, what it is not, and the numbers it stops at |
| **[Getting Started](docs/getting-started.md)** | Build, first REPL session, library embedding — 2 minutes |
| **[Why contextdb?](docs/why-contextdb.md)** | Problem statement, design philosophy, comparison with alternatives |
| **[Usage Scenarios](docs/usage-scenarios.md)** | 16 problem-first walkthroughs: constraints, graph queries, vector search, sync, propagation |
| **[Query Language](docs/query-language.md)** | SQL, graph MATCH, vector search, constraints, built-in functions |
| **[Sync Across Two Machines](docs/sync-two-machines.md)** | Stand up a hub, enroll two edges, converge in both directions |
| **[CLI Reference](docs/cli.md)** | REPL commands, sync commands, non-interactive scripting |
| **[Architecture](docs/architecture.md)** | Crate map, storage engine, MVCC, sync protocol, work ledger and blob plane, upgrades and recovery, plugin system |
| **[Benchmarking](docs/benchmarking.md)** | How the benchmarks are built and run |
| **[Agent Readiness](docs/agent-readiness.md)** | How this repo measures whether AI assistants can use and contribute to it |

## Architecture

11-crate Rust workspace:

| Crate | Role |
|-------|------|
| `contextdb-core` | Types, executor traits, errors, table metadata |
| `contextdb-tx` | MVCC transaction manager with deferred-apply write sets |
| `contextdb-relational` | Relational executor (scan, insert, upsert, delete) |
| `contextdb-graph` | Graph executor (bounded BFS, adjacency index, DAG enforcement) |
| `contextdb-vector` | Vector executor (cosine similarity, HNSW, pre-filtered search) |
| `contextdb-hnsw` | HNSW graph index used by the vector executor |
| `contextdb-parser` | SQL parser (pest grammar with GRAPH_TABLE + vector extensions) |
| `contextdb-planner` | Rule-based query planner |
| `contextdb-engine` | Database engine — wires all subsystems, plugin API, subscriptions |
| `contextdb-server` | Sync server and client (dial-by-key transport, conflict resolution) |
| `contextdb-cli` | Interactive CLI REPL |

## Building

```bash
cargo build --workspace
cargo test --workspace
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
