# Architecture

contextdb is a 10-crate Rust workspace. This document covers the crate structure, subsystem design, key traits, and extension points.

---

## Crate Map

```
contextdb-core          Types, executor traits, errors, Value enum, TableMeta
    │
contextdb-tx            MVCC transaction manager, WriteSet, WriteSetApplicator trait
    │
    ├── contextdb-relational    Row storage, scan, insert, upsert, delete
    ├── contextdb-graph         Adjacency index, bounded BFS, DAG enforcement
    └── contextdb-vector        Cosine similarity, brute-force + HNSW auto-switch
            │
contextdb-parser        pest grammar → AST (SQL + GRAPH_TABLE + vector extensions)
    │
contextdb-planner       AST → PhysicalPlan (rule-based, no cost optimizer)
    │
contextdb-engine        Database struct — wires all subsystems, plugin API, subscriptions
    │
    ├── contextdb-server    SyncServer + SyncClient (NATS transport, conflict resolution)
    └── contextdb-cli       Interactive REPL binary
```

Dependencies flow downward. `contextdb-engine` owns the `Database` struct and is the crate applications depend on.

---

## Subsystem Design

### Relational (`contextdb-relational`)

The canonical source of truth. All rows live here. Graph and vector indexes are secondary structures derived from relational data.

- In-memory row store with column-typed `Value` enum
- Point lookups by primary key, range scans with filter predicates
- Upsert via `INSERT ... ON CONFLICT DO UPDATE`
- DDL metadata stored alongside rows (columns, types, constraints)

### Graph (`contextdb-graph`)

Dedicated adjacency index maintained incrementally as edges are inserted/deleted. Not recursive SQL over edge tables.

- Bounded BFS with configurable max depth (engine limit: 10)
- Edge-type filtering per hop
- Direction control (outgoing, incoming, bidirectional)
- DAG cycle detection on insert (BFS from target back to source)
- Deduplication: `(source_id, target_id, edge_type)` is a natural key

### Vector (`contextdb-vector`)

Secondary index over relational rows with `VECTOR(n)` columns.

- Cosine similarity via `<=>` operator
- Below ~1000 vectors: brute-force exact scan
- At/above ~1000 vectors: HNSW (via `hnsw_rs`) with 10x overfetch + exact reranking
- Pre-filtered search: WHERE clause narrows candidates before scoring
- HNSW rebuilt from persisted vectors on `Database::open()` — no separate serialization
- OOM during HNSW build falls back to brute-force via `catch_unwind`

---

## Unified Transactions (MVCC)

`contextdb-tx` provides MVCC with consistent read snapshots:

- Each read sees a consistent snapshot across relational, graph, and vector state
- Writers don't block readers; readers don't block writers
- Writes are serialized through a commit mutex (one writer at a time)
- `WriteSet` accumulates all mutations within a transaction
- On commit, the `WriteSet` is applied atomically to all subsystems
- Propagation (state machine transitions cascading along edges/FKs) happens within the same `WriteSet`

---

## Storage: `WriteSetApplicator`

The boundary between compute and storage:

```rust
pub trait WriteSetApplicator: Send + Sync {
    fn apply(&self, ws: WriteSet) -> Result<()>;
    fn new_row_id(&self) -> RowId;
}
```

Two implementations:

| Implementation | Used by | Behavior |
|---------------|---------|----------|
| `CompositeStore` (in-memory) | `Database::open_memory()` | Applies to in-memory stores directly |
| `PersistentCompositeStore` | `Database::open(path)` | Applies to in-memory stores + flushes to redb |

This trait is the extension point for additional backends if required. The engine owns compute state (in-memory stores, HNSW cache). The applicator owns durability.

### Persistence (`redb`)

Single-file storage via redb:

- Flush-on-commit: every committed `WriteSet` is written to redb
- On open: all data loaded from redb into memory, HNSW rebuilt
- Crash-safe: redb provides atomic transactions
- Tables: rows, DDL metadata, graph edges, vectors, counters

---

## Plugin System

```rust
pub trait DatabasePlugin: Send + Sync {
    fn pre_commit(&self, ws: &WriteSet, source: CommitSource) -> Result<()>;
    fn post_commit(&self, ws: &WriteSet, source: CommitSource);
    fn on_open(&self) -> Result<()>;
    fn on_close(&self) -> Result<()>;
    fn on_ddl(&self, change: &DdlChange) -> Result<()>;
    fn on_query(&self, sql: &str) -> Result<()>;
    fn post_query(&self, sql: &str, duration: Duration, outcome: &QueryOutcome);
    fn health(&self) -> PluginHealth;
    fn describe(&self) -> serde_json::Value;
    fn on_sync_push(&self, changeset: &mut ChangeSet) -> Result<()>;
    fn on_sync_pull(&self, changeset: &mut ChangeSet) -> Result<()>;
}
```

All methods have default no-op implementations. `CorePlugin` ships as the default and handles engine-internal concerns (subscriptions, retention pruning).

Inject a custom plugin:

```rust
let plugin = Arc::new(MyPlugin::new());
let db = Database::open_with_plugin(path, plugin)?;
// or: Database::open_memory_with_plugin(plugin)?
```

`pre_commit` can reject a transaction by returning `Err`. `post_commit` fires after the write is durable. Applications like cg and Vigil use contextdb as a library and accept `Database` via dependency injection — they are database **users**, not plugin authors.

---

## Subscriptions

Reactive commit notifications via bounded broadcast channels:

```rust
let rx: Receiver<CommitEvent> = db.subscribe();
// or with custom capacity:
let rx = db.subscribe_with_capacity(256);
```

```rust
pub struct CommitEvent {
    pub source: CommitSource,  // User or Autocommit
    pub lsn: u64,
    pub tables_changed: Vec<String>,
    pub row_count: usize,
}
```

Fan-out to multiple subscribers. Dead channels are cleaned up automatically. Graceful shutdown disconnects all subscribers.

---

## Sync

Edge-to-server replication over NATS. The sync layer uses the same `Database` engine on both sides.

### Components

- `SyncClient` — runs on edge devices. Pushes local changes to server, pulls remote changes.
- `SyncServer` — runs on the server. Receives pushes, serves pulls.

Both communicate via NATS subjects: `sync.{tenant_id}.push` / `sync.{tenant_id}.pull`.

### Change Tracking

- Every committed row is assigned an LSN (Log Sequence Number)
- `SyncClient` tracks push and pull watermarks (the LSN of the last synced change)
- On push: sends all changes since the push watermark
- On pull: requests all changes since the pull watermark
- After restart: `full_state_snapshot` fallback rebuilds from current state (the ephemeral change log is lost)

### Conflict Resolution

Per-table configurable policies:

- `LatestWins` — most recent write by logical timestamp (default)
- `ServerWins` — server version takes precedence
- `EdgeWins` — edge version takes precedence
- `InsertIfNotExists` — insert if absent, skip otherwise

### Transport

NATS with automatic chunking for payloads exceeding the 1MB NATS message limit. Vector byte sizes are accounted for in batch estimation. WebSocket transport for edge clients (port 9222), native protocol for server-to-server (port 4222).

### DDL Sync

Schema changes (CREATE TABLE, ALTER TABLE, DROP TABLE) are synced alongside data. Constraints (PRIMARY KEY, NOT NULL, UNIQUE, STATE MACHINE, DAG) are preserved across sync.

---

## Query Pipeline

```
SQL string
  → contextdb-parser (pest grammar → AST)
  → contextdb-planner (AST → PhysicalPlan)
  → contextdb-engine (dispatches to executors)
    → contextdb-relational (row operations)
    → contextdb-graph (BFS traversal)
    → contextdb-vector (ANN search)
  → QueryResult { columns, rows, rows_affected }
```

The planner is rule-based (no cost optimizer). Key planning decisions:

- `GRAPH_TABLE` in FROM → `PhysicalPlan::GraphBfs`
- `ORDER BY ... <=> ...` → `PhysicalPlan::VectorSearch` (with candidate restriction from WHERE)
- CTE containing `GRAPH_TABLE` → recursive plan composition
- `IN (SELECT ...)` → subquery evaluation

---

## Memory Accounting

`MemoryAccountant` tracks memory usage against a configurable budget. Set via `--memory-limit` in the CLI or `MemoryAccountant::with_budget(bytes)` in the API. All vector and row allocations are accounted. Budget exceeded → operations return `MemoryLimitExceeded`.
