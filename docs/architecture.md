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
    ├── contextdb-server    SyncServer + SyncClient (dial-by-key transport, conflict resolution)
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

Secondary index over relational rows with `VECTOR(n)` columns. Index identity is
the full `(table, column)` pair, so one table can carry separate text, image,
audio, or policy embeddings with different dimensions and quantization choices.

- Cosine similarity via `<=>` operator
- `VECTOR(N) WITH (quantization = 'F32'|'SQ8'|'SQ4')` per column
- SQ8/SQ4 columns keep quantized live payloads and quantized HNSW payloads;
  f32 is reconstructed only at API/materialization boundaries
- Below ~1000 vectors: brute-force exact scan
- F32 at/above ~1000 vectors: HNSW (via `hnsw_rs`) with overfetch + exact reranking
- SQ8/SQ4 through 5000 vectors: exact scan to preserve self-recall; larger
  quantized indexes use HNSW
- Pre-filtered search: WHERE clause narrows candidates before scoring
- HNSW is built lazily per index; a search against one vector column does not
  build sibling indexes
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

## Store Ownership & Concurrency

A database file is opened by exactly one owner at a time. A second open of the
same path — whether from another thread in the same process or from a separate
process — returns `Error::DatabaseLocked`. This is enforced at two layers: an
in-process open registry and an on-disk PID lock backing an OS file lock. There
is no read-only, shared, or replica open; single-writer ownership is a deliberate
guarantee of the substrate, not a missing feature.

This is the standard embedded-database model — the same shape as SQLite, LMDB, or
redb: the application that needs the data **owns the handle for its lifetime** and
routes every read and write through that owner. Two consequences for anyone
embedding contextdb:

- **A long-running service answers its own queries.** If a process holds the
  database open (for example a daemon that ingests and records events), a
  *second* command that wants to read that data must ask the running owner — it
  cannot independently re-open the file while the owner holds it. Expose the read
  through the owning process (an in-process call, or a small local request to the
  running service); do not start a competing opener.
- **Never keep a shadow copy.** Working around the lock by mirroring the data
  into a second file or an in-memory side-store outside the owner is an
  anti-pattern: it creates a second source of truth, drifts from the real one,
  and defeats the integrity that single-writer ownership exists to provide. The
  fix for "I can't open it from over here" is always "route through the owner,"
  never "keep my own copy."

If concurrent multi-process access is ever genuinely required, that is a substrate
feature request (a read-only or shared open surface), not something to emulate in
the consuming application.

---

## Trigger Concurrency

ObservationTriggers are host callbacks declared with `CREATE TRIGGER` and
registered through `Database::register_trigger_callback`. They are not
PG-style validation triggers; schema invariants remain engine-enforced through
DDL. A trigger callback runs synchronously inside the firing transaction's
commit window, and callback writes use the supplied tx-bound `Database` handle
so relational rows, graph edges, and vectors commit atomically.

The callback-active contract is deliberately split by concurrency domain:

- same-DB trigger Class B waits-and-proceeds inside the engine, including
  public tx-control, SQL write paths, direct write helpers, and internal
  handles that share the same trigger state
- unrelated cross-DB writers proceed independently; a parked callback on DB-X
  does not poison ordinary worker-thread writes on DB-Y
- Class A callback-thread reentry returns `CallbackReentry`; retrying inside
  the callback body is misuse
- callback tx-bound handles remain isolated to their runner thread
- cron same-DB Class B keeps the immediate typed cron callback-active error
- a same-DB trigger wait that exceeds `CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS`
  returns the typed trigger callback-active error and emits one warning

Waiters do not hold the public-operation read guard while parked. That lets
`close()` acquire the write barrier after the active callback exits; a parked
writer then wakes to the ordinary closed-handle error instead of proceeding.

### Operator Runbook

Healthy same-DB trigger contention does not emit tracing events. A warning means
the bounded deadlock guard fired:

```rust
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

tracing_subscriber::registry()
    .with(EnvFilter::from_default_env())
    .with(fmt::layer().json())
    .init();
```

The warning carries structured fields:

```text
trigger_name=<name> waited_ms=<milliseconds> surface=<begin|commit|rollback|apply_changes|close|execute|execute_in_tx|direct helper>
```

Interpretation: no warning means normal wait-and-proceed contention; a warning
means the callback did not finish within the guard budget. The default guard is
60 seconds. Override with `CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS`; values below
5 seconds can false-positive under legitimate deep cascades.

---

## Storage: `WriteSetApplicator`

The boundary between compute and storage:

```rust
pub trait WriteSetApplicator: Send + Sync {
    fn apply(&self, ws: &WriteSet) -> Result<()>;
    fn new_row_id(&self) -> RowId;
}
```

Two implementations:

| Implementation | Used by | Behavior |
|---------------|---------|----------|
| `CompositeStore` (in-memory) | `Database::open_memory()` | Applies to in-memory stores directly |
| `PersistentCompositeStore` | `Database::open(path)` | Applies to in-memory stores + flushes to redb |

This trait is the extension point for additional backends if required. The
applicator borrows the commit `WriteSet`; stores clone only the row, edge, or
vector data they retain. The engine owns compute state (in-memory stores, HNSW
cache). The applicator owns durability.

### Persistence (`redb`)

Single-file storage via redb:

- Flush-on-commit: every committed `WriteSet` is written to redb
- On open: relational rows load into memory with table-local index maintenance,
  graph adjacency and vector/HNSW indexes rebuild afterward
- Crash-safe: redb provides atomic transactions
- Tables: rows, DDL metadata, graph edges, vector entries, counters
- Vector entries use one composite-key table keyed by `(table, column, row_id)`.
- A `metadata` table stores `format_version = "1.0.0"`; missing markers are
  treated as legacy stores, while unreadable markers are reported as corrupt.

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

`pre_commit` can reject a transaction by returning `Err`. `post_commit` fires after the write is durable. Downstream applications use contextdb as a library and accept `Database` via dependency injection — they are database **users**, not plugin authors.

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

## Memory Limit On Edge Devices

`SET MEMORY_LIMIT`, `SHOW MEMORY_LIMIT`, and the `CONTEXTDB_MEMORY_LIMIT` /
`--memory-limit` startup option all feed the same global memory accountant.
Vector operations attribute allocations with tags such as
`vector_insert@evidence.vector_text` and `build_hnsw@evidence.vector_vision` so
operators can identify the offending index from errors.

On a 2GB Jetson-class device, prefer SQ8 for high-dimensional evidence:

```sql
SET MEMORY_LIMIT '1536M';
CREATE TABLE evidence (
  id UUID PRIMARY KEY,
  vector_text VECTOR(768) WITH (quantization = 'SQ8'),
  vector_vision VECTOR(512) WITH (quantization = 'SQ8')
);
```

`SHOW VECTOR_INDEXES` gives structured per-index counts and live vector payload
byte totals, including any materialized HNSW payload estimate; use it
instead of parsing memory operation tags.

---

## Sync

The wire protocol is currently `PROTOCOL_VERSION = 4` (ALPN `contextdb.sync.v4`).
The server reports the supported protocol version in `contextdb-server --version`
and in its INFO logs; mismatched envelopes are rejected instead of being
partially applied.

### Deployment Topology

contextdb uses a client-server sync model where every instance — client or server — runs the same database engine. There is no "replica" or "read-only copy." Each database is a full read-write contextdb that works independently offline.

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  contextdb   │  │  contextdb   │  │  contextdb   │
│  (laptop)    │  │  (service)   │  │  (device)    │
│  SyncClient  │  │  SyncClient  │  │  SyncClient  │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │ dial            │ dial            │ dial
       │                 │                 │
       └────────┬────────┴────────┬────────┘
                │ sync endpoint (dial-by-key) │
                └────────┬────────────────┘
                         │
                ┌────────┴───────┐
                │  contextdb     │
                │  (server)      │
                │  SyncServer    │
                └────────────────┘
```

Each client database accumulates knowledge independently — decisions, observations, corrections, embeddings. On sync, changesets flow bidirectionally: local changes push up, server changes pull down. This is collaborative sync, not WAL replication — logical changesets with per-table conflict resolution, so knowledge learned by any participant propagates to all others.

Dial-by-key means clients reach the server through its cryptographic identity, not a broker address. A node behind NAT dials outbound, so machines on one LAN sync over direct connections with no port forwarding, no VPN, and no network configuration. The default configuration contacts no third-party service. To introduce peers across networks, the operator either self-hosts a small stateless `iroh-relay` (which only forwards end-to-end-encrypted bytes) via `relay=<url>`, or opts into the free public relays with `relay=n0` — connectivity is never a paid feature. A self-hosted relay presenting a private or self-signed certificate is trusted by pointing `relay-ca=<cert-file>` at its PEM bundle or single DER certificate. Dynamic address resolution is equally opt-in: `publish=` announces a node's addresses to a chosen service (n0's free one or self-hosted) and `lookup=` (mdns / n0 / a self-hosted zone or relay) resolves peers by identity alone — with these, tickets survive IP changes.

The server is just a contextdb instance running SyncServer. Self-host it, or point your client databases at a hosted server — the client binary and database files don't change, only the enrollment ticket they dial. Managed hosting is coming soon — [join the waitlist](https://contextdb.tech).

### Components

- `SyncClient` — runs on each participant. Pushes local changes to server, pulls remote changes.
- `SyncServer` — runs on the central server. Receives pushes, serves pulls.

Both communicate over per-tenant sync channels: `sync.{tenant_id}.push` / `sync.{tenant_id}.pull`.

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

Dial-by-key: each machine is reached by its own cryptographic identity (an ed25519 public key), carried by the Iroh library (iroh 1.0, wire-stable). A framed stream carries each payload whole — there is no 1MB message ceiling and no broker-side chunking; payload size is bounded by batching above the transport, and vector byte sizes are accounted for in batch estimation. LAN peers connect directly; cross-network peers are introduced by a self-hosted or opt-in relay.

The original NATS broker transport is retained behind the `nats` cargo feature as a deprecated compatibility adapter; it is not on the default path.

### DDL Sync

Schema changes (CREATE TABLE, ALTER TABLE, DROP TABLE) are synced alongside data. Constraints (PRIMARY KEY, NOT NULL, UNIQUE, single-column and composite FOREIGN KEY, STATE MACHINE, DAG) are preserved across sync.

### Tenants and Contexts

Two identifiers look similar but sit on different axes; don't conflate them. A
`TenantId` (`contextdb_core::TenantId`) is a sync-surface identity — the
isolation boundary a `SyncClient`/`SyncServer` pair operates under, one tenant
per sync relationship (the sync channels above are literally namespaced
`sync.{tenant_id}.push` / `.pull`). A context id is a different axis entirely:
an in-database scoping handle that rows carry, controlling which rows are
visible within a single store, independent of who that store syncs with.
Downstream consumers map their own organizing handle —
an intention, a site, a tenant of their own — onto one or both of contextdb's
axes, but the axes themselves stay separate: `TenantId` answers *who syncs with
whom*, a context id answers *which rows are visible*.

---

## Work Ledger and Media Plane

Two library surfaces, no CLI. They're documented together because a job on the
ledger can reference a `blob_ref` input, and resolving that reference is the
media plane's job, not the ledger's.

### Work Ledger

There are two modules named `work_ledger`, owned by different crates, at
different layers. Importing the wrong one is an easy mistake — disambiguate by
what you need:

- **`contextdb_engine::work_ledger`** is the class-blind bookkeeping layer: seven
  append-only tables recording job lifecycle events. A job's state — Pending,
  Leased, Done, Failed, or Cancelled — is *computed* from those rows, never
  stored as a column. A job is submitted (`submit_job`), claimed with a lease
  (`insert_claim`), completed exactly once (`record_result`), failed
  (`record_failure` — the failure row is what legalizes the next attempt), or
  cancelled. Lease expiry is advisory wall-clock time supplied by the caller;
  the engine never trusts a clock on its own, and lease expiry alone does not
  mean a job has been abandoned — that judgment belongs to the caller deciding
  whether to re-claim.
- **`contextdb_server::work_ledger`** is the distributed execution layer built on
  top, over the transport-neutral `SyncClient`. `claim_job` claims by push: it
  inserts a local claim row, pushes it, and the hub's conflict reply on the
  claim key is the arbitration verdict. If the hub is unreachable, the claim is
  held locally as `Won { synced: false }` rather than blocking. `poll_and_execute_once`
  and `run_worker_loop` drive execution; the product supplies a `WorkExecutor`
  trait implementation as the seam where real work happens.

If you're looking at the server module and want to know where job state
actually lives, it's the tables and the pure claim/lease functions in
`contextdb_engine::work_ledger`. If you're looking at the engine module and
want the cross-machine claim path — the part that arbitrates between two
workers racing for the same job — that's `contextdb_server::work_ledger::claim_job`,
one layer up.

### Media / Blob Plane

`contextdb_server::BlobService` (re-exported from the server crate root) moves
opaque, content-addressed bytes between nodes:

- **Ingest** on the holder node: `ingest_bytes(&[u8]) -> BlobHash` or
  `ingest_file(&Path) -> BlobHash`. The returned `BlobHash` is the hash of the
  bytes, so ingesting the same content twice is idempotent.
- **Serve** on the holder: `serve_on(&IrohServer)` registers the serving
  handler. It checks the requesting peer's authorization against the ledger
  *before* any payload bytes move — the check is the gate, not an
  after-the-fact audit.
- **Fetch** on the consumer: `resolve_blob_ref(&BlobHash, holder_ticket, sink)`
  is async, fetches node-to-node, and is hash-verified — only bytes that hash
  to the requested `BlobHash` ever reach the sink. Errors are a matchable
  `ResolveError`: `Unentitled`, `PolicyForbidden`, `HashMismatch`,
  `HolderUnreachable`, `LocalStoreUnavailable`, `BlobNotFound`,
  `TransferAborted`, `SinkWrite`.
- **Reclaim**: `reclaim_unreferenced(now_ms, grace_ms)` frees a blob once every
  job referencing it is terminal past the grace window (or once no job
  references it at all). A later resolve attempt against a reclaimed blob
  returns `BlobNotFound`, even from an otherwise-entitled caller.
- **Direct vs. relay**: whether a transfer goes direct or via a relay is a
  serve-time choice on the holder's endpoint spec. By default it's direct
  only — contextdb contacts no relay unless asked. The operator opts into the
  public relays or a self-hosted relay URL, the same relay configuration
  described under Sync. The choice rides in the serve ticket, so a consumer
  behind NAT that can't reach the holder directly is bridged automatically.

**Resolving a blob requires an entitling claim.** A consumer node may fetch a
blob only while it holds a *live* claim — lease still ahead, that attempt not
failed — on a job whose inputs reference that blob's hash
(`node_holds_claim_for_blob`). This is the actual security boundary, and it's
checked holder-side at serve time, not trusted from the requester. There is
also a local, identity-blind pre-check the consumer can run before dialing —
that check exists purely to avoid a wasted network round trip; it is not the
entitlement boundary and should never be treated as one.

The ledger reserves the `blob_ref` input kind but doesn't resolve it:
`materialize_inputs` refuses a `blob_ref` input with `Error::InputRequiresBlobResolver`,
pointing the caller at the blob resolver above. The ledger tracks that a job
depends on a blob; the media plane is what actually moves the bytes.

```rust,no_run
// Node A (holder): ingest a blob, serve it, and submit a job that references it.
use contextdb_engine::work_ledger::{InputRef, JobSpec, submit_job};
use contextdb_server::{BlobService, work_ledger::claim_job};

let bytes = std::fs::read("frame.jpg")?;
let blob_hash = blob_service.ingest_bytes(&bytes)?;
blob_service.serve_on(&iroh_server);

let spec = JobSpec::builder("job-1", "describe-image", "once", "node-a")
    .input_refs(vec![InputRef::blob_ref(blob_hash.clone())])
    .build();
let no_direct_inputs: [&[u8]; 0] = [];
submit_job(&db, &spec, &no_direct_inputs)?;

// Node B (worker): claim the job under a lease, then resolve the referenced blob.
// Holding a live claim on this job is what entitles Node B to fetch the bytes.
let claim = claim_job(&sync_client, "job-1", 1, "node-b", lease_deadline_ms, now_ms).await?;
let mut sink = Vec::new();
let bytes_written = blob_service
    .resolve_blob_ref(&blob_hash, &holder_ticket, &mut sink)
    .await?;
```

---

## Upgrades and Recovery

### Upgrading the store format

The on-disk store carries a format-version marker (current: `1.0.0`). Opening a
data root written by an incompatible older format fails closed with
`LegacyVectorStoreDetected` rather than silently corrupting or misreading it.

There is no automated in-place migration today. The supported upgrade path is
one of:

- sync from a peer already running the 1.0+ format, so the new store is
  populated by a normal sync pull instead of by reading the old file directly, or
- recreate the schema on a fresh data root and reimport the data.

### Recovering a wedged or corrupt data root

A corrupt or truncated store is detected on open and surfaced as
`StoreCorrupted`, with the error message naming the next step rather than
leaving the caller to guess. There is no in-place repair. Recovery is one of:

- restore from a backup, or
- restore from a healthy sync peer, or
- remove the data-root file and let it recreate empty, then repopulate it (by
  sync or by reimport).

A second `open` of a data-root file already held open by another process — same
process or a different one — returns a database-locked error; that is the
ownership guarantee described under Store Ownership & Concurrency, not a
corruption signal, and doesn't call for any of the recovery steps above.

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

## Memory And Disk Budgets

`MemoryAccountant` tracks memory usage against a configurable budget. Set via `--memory-limit` in the CLI or `MemoryAccountant::with_budget(bytes)` in the API. All vector and row allocations are accounted. Budget exceeded → operations return `MemoryBudgetExceeded`.

File-backed databases also support a persisted disk budget:

- startup ceiling/default via `--disk-limit` or `CONTEXTDB_DISK_LIMIT`
- runtime control via `SET DISK_LIMIT` / `SHOW DISK_LIMIT`
- persisted live config in the redb file so reopen preserves the limit

Disk enforcement happens in the engine write paths before `INSERT`, `UPDATE`, and sync-apply work begins. Once the on-disk file is at or above the configured limit, further file-backed writes fail with `DiskBudgetExceeded`. In-memory databases accept the SQL but ignore disk budgeting because there is no backing file to measure.
