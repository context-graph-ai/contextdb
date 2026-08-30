# Architecture

contextdb is an eleven-crate Rust workspace. This document covers the crate structure, subsystem design, key traits, and extension points.

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
            └── contextdb-hnsw  Vendored third-party HNSW index (see below)
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

`contextdb-hnsw` is the eleventh crate and the one exception to "in-house surface": it is a
**vendored third-party crate** — the upstream `hnsw_rs` 0.3.4 HNSW implementation, carried here
under its own upstream authorship and its `MIT OR Apache-2.0` licence, with deterministic seeded
builds on top. Treat it as vendored dependency code: `contextdb-vector` is where a change to how
contextdb *uses* an HNSW index belongs, and a change inside `contextdb-hnsw` itself is a change to
vendored upstream code, to be made deliberately and kept minimal.

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

**Writing** a database file is owned by exactly one process at a time. A second
writable open of the same path — whether from another thread in the same process
or from a separate process — returns `Error::DatabaseLocked`. This is enforced at
two layers: an in-process open registry and an on-disk PID lock backing an OS
file lock. Single-writer ownership is a deliberate guarantee of the substrate,
not a missing feature.

**Reading** is a separate door that does not take the write lock. A read session
(`ReadSession` in Rust, `contextdb <path>` without `--write` on the CLI) resolves
one of two routes when it opens, and never changes route mid-session:

- **Owner route** — a live process owns the store, so the reader is served that
  owner's committed state over an authenticated local channel. The reader never
  attaches to the file, and the owner's declared ceilings and deadlines govern
  what it may ask for.
- **File route** — nobody owns the store, so the reader reads the committed
  snapshot from the file directly and leaves every byte of the store folder
  unchanged. Several direct readers coexist on one store.

The two directions see each other. A writable open of a store a writer already
owns is refused with `held_by_writer`, and a writable open while direct readers
are still hydrating is refused with `held_by_readers` — a reader takes a hold
beside the store for exactly as long as it is hydrating, precisely so a starting
writer waits rather than tearing the file out from under it, and publishes a
best-effort record of itself so that refusal can name who to go and look at. A direct read that finds committed state a writable open
would have to mend refuses with `direct_read_requires_writer` instead of serving
something the file cannot back.

This is still the standard embedded-database model — the same shape as SQLite,
LMDB, or redb: the application that mutates the data **owns the handle for its
lifetime**, and every write goes through that owner. Two consequences for anyone
embedding contextdb:

- **A long-running service answers its own writes and can be read beside.** A
  process that holds the database open serves its own reads in-process; a
  *second* command that wants that data opens a read session, which reaches the
  running owner over its channel. What a second process must never do is start a
  competing *writable* opener.
- **Never keep a shadow copy.** Working around the lock by mirroring the data
  into a second file or an in-memory side-store outside the owner is an
  anti-pattern: it creates a second source of truth, drifts from the real one,
  and defeats the integrity that single-writer ownership exists to provide. The
  fix for "I can't open it from over here" is "read it through a read session,"
  never "keep my own copy."

### The three read doors, and why the owner-only one never falls back

`ReadSession::open` (and `ReadSession::open_with_options`, which carries the caller's own limits
and deadlines) resolves either route: the live owner's channel when a process owns the store, the
committed file when none does.

Some questions only a running owner can answer — what the process is doing, whether it is serving
yet, anything asked of the process rather than of the data — and the committed file has no answer
to give. `ReadSession::open_owner_only` is the door for those: it takes the owner route when a
process owns the store, and when none does it says plainly that the owner is not running
(`owner_not_running`) instead of falling back to the file. A writer that has claimed the store
but has not yet published whether it is serving is never reported absent: the caller waits for
that writer's own answer inside the deadlines it declared, and if the answer does not come in
time it is told the store is owned and not serving (`owner_not_serving`) — never that nobody
owns it.

It never opens the store's file at all, and that is the point of it. Reading the committed image
publishes a reader that a writer starting beside it must wait for, so a readiness probe that fell
through to the file would stand in the way of the very process it is waiting for; and a file that
cannot be read directly would answer with the file's condition instead of the plain fact that
nobody owns the store. A caller that wants the committed rows when there is no owner opens an
ordinary session instead. The exported signatures and a runnable four-line example are in
[`docs/getting-started.md`](getting-started.md#reading-a-store-from-a-second-process).

A refused read arrives as `Error::ReadFailure`. Its `kind()` is the stable classification a
program branches on and its `detail()` carries the machine-readable specifics of that
classification; the sentence a person reads stays in the error's `Display` and its wording is free
to change. A store somebody holds for writing refuses with the `HeldByWriter` kind and a
`ReadFailureDetail::HeldByWriter` detail that names the store in `store_path` and the holding
process in `process_id` — filled in whenever that writer has published a record about itself, and
`None` when it has not, so a caller can tell "held by process 4213" from "held, by someone who did
not say who". Reading the process id out of the prose is never necessary.

---

## Trigger Concurrency

Triggers are host callbacks declared with `CREATE TRIGGER` and
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
- a same-DB trigger wait that exceeds the deadlock guard's timeout returns the
  typed trigger callback-active error and emits one warning

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
means the callback did not finish within the guard budget. The guard is a fixed
60-second default and is not configurable via environment variable — the
environment is not a behavior surface on this stack (see [CLI Reference](cli.md)).

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

All methods have default no-op implementations. `CorePlugin` ships as the default and handles engine-internal concerns (subscriptions). Retention pruning and version cleanup are NOT plugin-driven — they are the engine-owned maintenance loop described below.

Inject a custom plugin:

```rust
let plugin = Arc::new(MyPlugin::new());
let db = Database::open_with_plugin(path, plugin)?;
// or: Database::open_memory_with_plugin(plugin)?
```

---

## Maintenance (retention + version cleanup)

contextdb starts one background maintenance thread per database, and only when that database declares something to maintain: a `RETAIN` window on some table, `HISTORY CURRENT ONLY` on some table, or a durable trigger with an audit history. It ticks once a minute, does near-zero work when there is nothing to reclaim (a cheap gate reads only the in-memory table map, never the commit lock, before deciding whether a cycle is worth running), and self-starts and self-stops as declarations arrive or leave — no consumer call is required. A database that declares none of the above starts no thread at all, so an embedding consumer (a library caller that never declares `RETAIN` or `HISTORY CURRENT ONLY`) gets no background thread it did not ask for.

One cycle runs two passes, in order: retention (rows past their declared `RETAIN` window) then version cleanup (superseded versions of a table declaring `HISTORY CURRENT ONLY`) — a row that expires this cycle is never version-collapsed first.

**Version cleanup and a held read snapshot.** Every in-flight statement registers its own read snapshot for the call's duration, and a caller that needs to reuse a `SnapshotId` ACROSS separate calls (on a table declaring `HISTORY CURRENT ONLY`) registers it explicitly via `Database::pin_snapshot`, holding the returned guard for as long as the snapshot is still wanted. A version-cleanup pass samples every currently-registered snapshot (not merely the oldest) plus the committed watermark, atomically, once at the start of the pass, and defers any superseded version still visible to ANY of those registered snapshots to a later cycle — a version created between two registered snapshots and superseded after both is protected by the higher one even though the lower one alone would not see it. Protection begins when `pin_snapshot` RETURNS: a pin requested while a removal pass is already mid-flight, for a snapshot at or before that pass's sampled watermark, waits for the pass to finish first (bounded by one pass duration) before registering, so the pin can never return with a false promise of protection the SAME in-flight pass is still free to violate — the next cleanup cycle honors it instead. A pin for a snapshot strictly after the pass's watermark registers immediately: nothing that pass can prune was ever visible to it. Versions a pass already reclaimed before the pin is requested cannot come back; that boundary is unchanged.

**The cost model.** Both passes remove exactly what they reclaim — the row versions, the change-log entries that referenced them, and (version cleanup only) the vector copies attached to a released row version — each in its own bounded, point-removal redb write transaction: row/change-log removal first, then vector/edge removal (only when there is a released vector to reclaim), then commit-index removal, up to three independent transactions per pass. Nothing else in the file is read or rewritten in any of them: a table's surviving rows, an unrelated table's rows, and every vector or edge that is not part of what is being reclaimed stay untouched. Memory only follows once every persisted transaction has succeeded; a failure between transactions leaves persisted state strictly AHEAD of the in-memory snapshot — benign over-retention, never a lost version or a corrupt change-log entry — and the next maintenance cycle re-attempts and completes the same work. Cost is proportional to what is reclaimed, never to the size of the database or to how much unrelated data (vectors, edges, other tables) shares the file. Version cleanup never opens the graph tables at all — edge identity is self-owned (`source`, `target`, `edge_type` plus its own `created_tx`/`lsn`), versioned by no relational row, so cleanup neither needs nor claims edge boundedness; a table that accumulates superseded edge copies needs its own accounting, tracked separately.

**This pass-scoping is separate from — and does not replace — redb's own file compaction; the two are different-shaped costs reported through different receipts.** A scoped pass is O(what it reclaimed); a redb `compact()` rewrites the *whole file* to turn freed pages back into real file-size reduction, so it is never folded into a pass's own timing. Do not assume every prune shrinks the file: redb reuses freed pages in place, so a steady-state cycle usually reclaims bytes without the file getting smaller at all — only a compaction does that. Cross-check any file-shrink claim against a real measurement rather than assuming it from the reclaim numbers; a redb compaction has been observed to *grow* a file in at least one measured case (page reorganization overhead), so "compacted" is not a synonym for "smaller."

Retention keeps its original, self-contained decision: `run_pruning_cycle_checked` samples the dead-space fraction *before* it prunes and, if that pre-prune reading is already at or over the shared threshold, compacts once, synchronously, inside that same call — reported on `PruningReport` (`compacted`, `fragmentation_before`, `file_bytes_before`/`_after`). Currency version cleanup (`compact_currency_versions`) does **not** — it never calls `compact()` itself, at any threshold, because a small file where routine superseded-version debris is a large fraction of it can cross the threshold on *every* cycle, and coupling a scoped O(pruned) pass to an O(whole-file) rewrite on every tick reintroduces the exact per-cycle cost the pass-scoping exists to remove (measured directly by the version-cleanup scaling bench). Compaction for a currency table is instead:

- **An explicit operator action** — `Database::compact_now()` (`.maintenance compact` in the CLI): unconditional, on demand, no threshold, no interval gate. Returns a `CompactionReport` (`ran`, `duration_micros`, `bytes_before`/`_after`, `file_shrank`, `fragmentation_before`, `handle_recycled`, `handle_recycle_micros`).
- **A much rarer automatic path** — `Database::run_maintenance_cycle`'s engine-owned tick checks the threshold *and* a minimum interval (`AUTO_COMPACT_MIN_INTERVAL`, one hour by default) after every scheduled cycle's own passes have already run, outside any commit lock; it fires at most once per interval regardless of how many cycles cross the threshold in between. Its result rides `MaintenanceReport.compaction` — the same `CompactionReport` shape, separate from `currency.redb_compacted` (which now always reads `false`: it describes only what the scoped currency pass itself decided, which is nothing).

**Compaction restores file size AND steady-state write cost.** A file-level redb `compact()` shrinks the file and fully normalizes its on-disk btree, but redb also retains in-process allocator/region bookkeeping sized to the database's historical peak allocation, and a file-level compact does not reset that on its own. So `RedbPersistence::compact` — the one function both the explicit and automatic paths call — closes the store's redb handle and reopens the same file immediately after the file-level compaction finishes, under the same lock that already serializes every other access to the store; no caller can observe an intermediate closed state. This is what `handle_recycled`/`handle_recycle_micros` report, timed separately from the file-level compaction itself. A long-running embedded consumer has no other opportunity to clear that in-process state — it cannot process-restart — so compaction does it on the consumer's behalf every time. On a reopen failure the on-disk file is untouched (nothing more is written to it after the file-level compact finishes) and the store is left closed; a fresh `Database::open` on the same path recovers every row.

A deliberate working-headroom margin between the shrink and the recycle was tried and measured NOT to help: redb's own close-time bookkeeping (`Drop for redb::Database`'s `ensure_allocator_state_table_and_trim`) regrows a maximally-shrunk file to roughly the same final size whether or not a margin was left beforehand, so the margin only cost two extra write transactions for no measured benefit — removed.

**A store under its DECLARED-FROM-THE-START maintenance regime meets the steady-state ceiling immediately, on its very first post-compact cycle** — every measurement of a table that has always had `HISTORY CURRENT ONLY` declared, compacted regularly, shows no elevated window at all. **A RETROFIT root — a table carrying a large amount of history accumulated BEFORE it was ever compacted even once — pays one real, but strictly one-time and single-cycle, elevated cost right after its first compaction**, confirmed directly (the version-cleanup-scaling bench's A2 arm, and its own `run_a2_retrofit_recipe`): the cycle immediately following a retrofit root's first-ever compaction runs measurably slower than the declared-from-the-start regime, but the very NEXT cycle already drops back into the normal few-millisecond range, and a second `compact_now()` run after that real write activity — never a special mechanism, just calling the same explicit operator action again — fully restores the identical regime a declared-from-the-start table has from the start. The honest operational guidance for a retrofit root: run `.maintenance compact` once to reclaim space, expect one elevated cycle immediately after, and run it a second time once some normal write activity has occurred to lock in the restored steady-state cost — this is the ordinary explicit action used twice, not a distinct maintenance mode.

**Eligibility is declared, not named.** A table is version-cleanup-eligible because it declares `HISTORY CURRENT ONLY`, never because of its name. The three built-in fabric tables (`work_capabilities`, `peer_directory`, `work_node_contacts`) declare it in their own `CREATE TABLE` text like any other table would.

**Those three tables' declared policy, plus `work_inputs`' `RETAIN 7 DAYS`, is engine-owned, not operator policy.** All four are built-in work-fabric tables (see the [work-fabric skill](../skills/work-fabric/SKILL.md)) whose own bookkeeping depends on staying at the shape declared in their own `CREATE TABLE` text: `work_inputs`' retention window is what keeps ledger-carried input copies bounded, and the three currency tables' `HISTORY CURRENT ONLY` plus their `SYNC CONFLICT KEEP LATEST` (or `SYNC OFF`) is what makes version-cleanup safe to reclaim their superseded rows at all. A locally-typed `ALTER TABLE` refuses any `RETAIN` / `HISTORY` / `SYNC CONFLICT` / `SYNC ...` / `SYNC SAFE` value that mismatches one of these four tables' own canonical declaration, with a message naming the table as engine-owned infrastructure — including `SET SYNC ...` (`work_node_contacts`' own `SYNC OFF` declaration is guarded on this axis exactly like the others); silence, or an explicit restate of the table's already-declared value on that axis, still passes, the same mismatch-only contract the rest of this paragraph describes for the other doors. A locally-typed `CREATE TABLE` of one of these four names is guarded too: it refuses unless the declared shape structurally matches the owning installer's own `CREATE TABLE` text — every column (name, type, nullability, primary key, uniqueness, default, references, `EXPIRES`, `IMMUTABLE`) AND every table-level option (a table-level `IMMUTABLE`, `STATE_MACHINE`, `DAG`, or `PROPAGATE` clause, none of which any of the nine reserved names' own DDL declares) — and refuses an explicit non-canonical policy clause the same way the ALTER door does — a table outside the nine reserved names remains entirely unrestricted (silence on policy, i.e. the pre-declaration legacy shape, still passes). A locally-typed `ALTER TABLE ADD COLUMN` / `DROP COLUMN` / `RENAME COLUMN` naming one of the nine reserved names is refused outright too, before any other ALTER validation runs — column shape on these nine is exactly as fixed against a column-level ALTER as it is against `SET RETAIN` / `SET HISTORY` / `SET SYNC CONFLICT` / `SET SYNC ...`. Existence is checked first: on a store where the table was never installed, this ALTER reports "table not found," not the engine-owned refusal — the same existence-before-ownership ordering every other clause in this door already follows, so an absent table and a governed one are never confused. The arriving-sync-DDL mirror of both guards reconstructs the wire columns and table-level constraint text into real `CREATE TABLE` SQL and runs it through the same parser and the same shape judgment, so a peer cannot smuggle a table-level option (or a per-column attribute) past the wire path that the local path already refuses. The shape guard actually spans **nine** reserved names, not four: the same four plus the five hub-refereed work-ledger tables (`work_jobs`, `work_claims`, `work_results`, `work_failures`, `work_cancellations`), whose real arbitration is a hardcoded `keep_first` override applied at every sync chokepoint rather than a fully declared policy shape. Those five get a narrower, SYNC-CONFLICT-only counterpart of the policy guard: a `CREATE TABLE` or `ALTER TABLE`, locally or over an arriving sync DDL, that declares a SYNC CONFLICT value other than `keep_first` refuses; silence, or an explicit restate of `keep_first`, still passes. `SHOW SYNC_CONFLICT_POLICY` renders every one of these seven work-ledger tables, plus `peer_directory` (also engine-owned, via its own declared `SYNC CONFLICT KEEP LATEST`), actually present in the store as `{table}={word} (engine-owned)`, distinct from an operator-declared row. `work_node_contacts` declares no conflict policy at all, so it never renders in `SHOW SYNC_CONFLICT_POLICY`, in any form. An arriving sync DDL naming one of the original four is held to the identical shape by THREE guards because a peer's own DDL always carries the table's FULL current shape, whether or not a given axis actually changed: an EXPLICIT differing value — spelled as an `AlterTable`, as a `CreateTable` adopting an already-existing table, or as a fresh `CreateTable` of a reserved name (guarding against DROP + CREATE circumvention) — is refused atomically for the whole batch before any of it is written; an axis the arriving DDL is simply SILENT on PRESERVES the table's current declared value instead of being read as an implicit clear, which is what lets a half-healed peer's own in-progress multi-step reconcile interoperate. This is not a workaround for a missing knob: an installer (or a peer) that only ever heals a legacy root back to its own declared shape needs no exception, because a healing call always restates that same declared shape verbatim, and a verbatim restatement always applies — locally, and over sync.

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

`SET MEMORY_LIMIT`, `SHOW MEMORY_LIMIT`, and the `--memory-limit` startup option all
feed the same global memory accountant.
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

The wire protocol is currently `PROTOCOL_VERSION = 6`. The ALPN identifier
is `contextdb.sync.v6` — it names the transport framing, including the reply-receipt
byte that lets graceful shutdown prove the dialing peer received a terminal reply
and the per-publication nonce that prevents identical large replies from sharing
durable bytes or completion state.
Payload version skew is caught by the envelope check below, not the ALPN.
Once shutdown closes admission, the hub finishes sync work it already accepted,
including the chunk and authenticated completion exchange for an oversized reply.
New requests are refused. A peer that stops acknowledging replies cannot hold the
hub forever: each reply receipt and the complete graceful-drain phase have a
30-second failure ceiling, after which the endpoint closes the stalled work. These
deadlines are liveness bounds, not scheduling delays; normal progress advances on
protocol state and returns as soon as the accepted work is complete.
Before route admission, the default server policy permits at most 128 incoming
connection/handshake tasks and 64 MiB of aggregate declared sync-frame payload.
Request-frame reads must make application-byte progress every 30 seconds; QUIC
keepalives do not reset that application deadline. Operators declare different
server-local limits with `pre-admission-connections=`, `pre-admission-bytes=`, and
`request-read-idle-ms=` in the bind endpoint spec. Exhaustion refuses new work
immediately instead of allocating memory or creating a waiting task.
The edge's push request uses the general 60-second sync-operation ceiling, so it
does not reset a valid larger atomic push while the hub is still committing; on
shutdown the hub's 30-second drain ceiling therefore decides first and returns a
confirmed reply or a loud failure.
The hub reserves at most 1,024 unfinished oversized replies in its shutdown
registry and refuses the next reply before staging it. Each authenticated,
validated chunk refreshes a 30-second inactivity lease. A transfer that resumes
while the hub is still serving is re-registered after its durable manifest and
chunk validate; a transfer that was silent for the full lease before shutdown is
stalled work and is not allowed to reopen admission during drain.
The server reports the supported protocol version in `contextdb-server --version`
and in its INFO logs; mismatched envelopes are rejected instead of being
partially applied.

The completed, not-yet-released version 6 includes
`WireRowChange.arrival` (the row's ordering position on the node that
accepted it) and `PullResponse.source` (the serving store's per-tenant
incarnation), a distinct PURGE instruction lane, a schema-provenance positional
slot that remains present even when empty, and the structured
`PurgeRequiresAuthoritativeHub` push refusal. See "Arrival Ordering" and "Pull
Cursors Are Bound To Their Serving Store" below. Peers using the same v5
transport framing but different payload versions are refused loudly on push,
pull, and the dedicated status exchange: no rows move, no watermark advances
on either side, and the error names the remedy (upgrade both ends) rather than
just the two version numbers. An older `contextdb.sync.v4` peer cannot negotiate
the receipt-bearing v5 framing at all; that connection refusal likewise names
transport-version alignment and upgrading both ends as the remedy. Nothing is
lost because either refusal happens before a watermark can advance.

Future work bumps the protocol version whenever it changes sync bytes or sync semantics. SQL, storage, CLI, or maintenance work that leaves sync unchanged does not bump the protocol.

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

**A table needs a sync identity to sync.** A row is told apart from another
row of the same table by its identity: a declared `PRIMARY KEY`
(single-column or table-level `PRIMARY KEY (a, b, ...)`), or failing that, an
indexed `id` column as a fallback. A table declaring neither has no way to
tell one row from another across the wire — this is a **keyless table**.

A keyless table that would never leave the machine anyway (`SYNC OFF`) is
unaffected; it was never eligible to sync either way. But a keyless table
that WOULD sync (any other direction — the default is `SYNC TWO WAY`) makes
`push()` refuse loudly with `Error::SyncError`, naming the three ways to fix
it, rather than silently reporting success while that table's rows never
actually cross the wire:

```
table 'events' has no usable sync identity — no declared PRIMARY KEY and no
indexed `id`-column fallback — so its rows cannot be told apart across the
wire. Push refuses rather than silently omitting them while reporting
success. Fix one of: declare a PRIMARY KEY on 'events'; add an indexed `id`
column as the fallback identity; or declare 'events' SYNC OFF.
```

This is a covering-index requirement in spirit: a table an application means
to sync must declare an identity, the same way a table meant to be looked up
efficiently declares an index. There is no silent partial-sync mode — either
the table can be synced (it has an identity) or it explicitly opts out
(`SYNC OFF`); nothing in between quietly drops rows.

### Conflict Resolution

Each table declares its conflict behavior in DDL:

- `SYNC CONFLICT KEEP FIRST` — the hub's first accepted value remains.
- `SYNC CONFLICT KEEP LATEST` — the later accepted value replaces it.

Whichever policy applies, a conflict means two machines genuinely diverged. A row that arrives
carrying exactly what the receiving node already holds is a re-delivery — the everyday case being
an edge pulling back rows it just pushed — and is a pure no-op that appears in none of the three
counts an apply reports: `applied_rows` counts rows that changed local state, `conflicts` records
genuine divergence at a natural key, and `skipped_rows` counts rows a policy or the context scope
turned away. Re-delivered data changed nothing and refused nothing, so it is counted nowhere. The
decision is made per row against that row's own content, so one changeset mixing a re-delivery
with a genuine divergence still reports the divergence in full.

Both of those judgements are made strictly within what the receiver can see. A row outside the
receiving handle's context scope is refused before its content is ever compared — it counts as
`skipped_rows` and never as a conflict. A conflict says two peers both saw a row and disagreed
about it, which a receiver that cannot see the row has no basis to claim, and the record would
name the hidden row's natural key, disclosing the very existence the access boundary exists to
hide. So "identical content is a no-op" is a statement about rows within visible scope; a hidden
row is refused outright, whatever it contains.

#### Arrival Ordering

`KEEP LATEST` arbitrates on **one clock: the accepting node's own ordering of arrivals** — never
two machines' independent LSN counters. Each row's wire form carries `arrival`: the ordering
position some node already gave it, or absent when the sender itself authored the row fresh and
never yet synced it anywhere. The rule:

- byte-identical values → no-op (the ordinary re-delivery case above);
- the incoming row carries no arrival → this accepting node is the one ordering it, so it always
  wins over whatever is already held, regardless of the sender's own local clock;
- the incoming row carries an arrival at or below the position already stored for that row → a
  stale echo — a no-op, never a conflict and never counted as skipped;
- the incoming row carries a strictly higher arrival → it wins.

The winner is therefore always the mutation the accepting node took last — never the row from
whichever machine happened to have run more local writes first. `arrival` is minted from the
accepting node's own commit LSN (the same value already restored monotonically at open — no new
counter, no new durable table) and is re-stamped forward on every winning apply, so a later
relay of the same row carries the position the FLEET actually agrees on, not the sender's own
unrelated counter. A row accepted without an established position takes EXACTLY its own commit
position on the accepting node — never a value sampled before that commit was ordered, so two
rows accepted around the same instant can never be minted the same arrival only to land at two
different committed positions.

#### Pull Cursors Are Bound To Their Serving Store

A pull cursor is only ever compared against the history of the store that issued it. The puller
persists `(source incarnation, lsn)` as one record — never a bare `Lsn` — so a page served by a
store other than the one the cursor addresses is discarded unapplied, the cursor resets to
`Lsn(0)`, and the client fully re-pulls the new store's history. This covers two operator-facing
scenarios a bare watermark cannot: pointing an edge at a different endpoint for the same tenant,
and a hub wiped and rebuilt under the same transport identity. A mid-pull source change (the
serving store changes between two pages of one paged pull) discards only the mismatched page —
whatever already applied from earlier pages, and their cursor advance, stands. The existing
stale-restore guard (fires when the new store's clock is numerically BEHIND the old cursor) is
unaffected and still applies; source binding closes the complementary case where the new store is
numerically AHEAD but holds real history below the old cursor.

### Transport

Dial-by-key: each machine is reached by its own cryptographic identity (an ed25519 public key), carried by the Iroh library (iroh 1.0, wire-stable). A framed stream carries each payload whole; payload size is bounded by batching above the transport, and vector byte sizes are accounted for in batch estimation. LAN peers connect directly; cross-network peers are introduced by a self-hosted or opt-in relay.

Iroh is the single sync transport. Every exchange authenticates its peer by cryptographic fabric identity.

`SyncClient::new` gives any file-backed `Database` a durable identity without
exposing the database's persistence path: with a bare enrollment ticket it
loads or creates `<canonical-db-path>.fabric-identity.key`. Reopening that
database and recreating the database file while retaining this adjacent key
both keep the same node identity; a recreated database still has its new store
incarnation. An endpoint that explicitly supplies `identity=<key-file>` always
takes precedence over the adjacent key. An in-memory edge must supply that
explicit persisted identity itself: a bare ticket is refused before it dials or
signs a sync request. The lower-level raw transport may still use an ephemeral
identity only for protocol-level anonymous-refusal coverage where no database
participates.

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

The relationship between the axes is one-to-many: **one tenant owns many
contexts.** A person or organization is one tenant — their sync/routing
identity — and partitions their worlds into contexts under that single
tenancy (one user's product-development context and hobby-electronics context;
one customer's N per-site contexts). The stack's consumers default a fresh
capture context to the tenant's own name — "your tenant is your first
context" — and users narrow into per-world contexts deliberately. The two
identifiers are never two names for one thing and are not to be unified.

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

`contextdb_server::BlobStore` (re-exported from the server crate root) moves
opaque, content-addressed bytes between nodes:

The bytes, chunk manifests, partial-transfer checkpoints, and protection tags
live inside ContextDB's own redb file. They are not a second filesystem store.
That ownership is what makes authoritative `PURGE` honest: one transaction
removes the selected ledger lineage and every now-unreferenced engine-held blob
copy, or the whole statement fails with no partial erasure. After success no
ContextDB query, history read, serve, export, resume path, or engine-created
backup can reach those bytes. This is logical engine erasure, not a claim that
redb can overwrite filesystem journals, SSD wear-leveling ghosts, or external
OS snapshots; encryption-at-rest with key erasure is the route to that stronger
forensic guarantee.

This boundary covers media deliberately ingested for distributed work, such as
a detection clip sent from a submitting node to a GPU worker. An application's
independent recording archive is outside ContextDB: for example, deleting
continuous camera footage written by Vigil's own media pipeline remains a
Vigil retention/erasure responsibility and must be coordinated separately for
a product-level "erase this person" operation.

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
  `TransferAborted`, `SinkWrite`, `FetchTimedOut`.
- **Fetch deadline**: the whole `resolve_blob_ref` attempt — dial, the
  holder's tag bookkeeping, and the verified transfer loop — is bounded by a
  declared `BlobFetchPolicy { fetch_deadline_ms }` (documented default:
  `120_000`, i.e. 120s). Set it per instance with
  `blob_store.set_fetch_policy(BlobFetchPolicy { fetch_deadline_ms: 30_000 })`
  before calling `resolve_blob_ref`. The bound is enforced by spawning the
  fetch and timing out the JOIN, not the future being awaited directly, and
  the abandoned fetch is aborted at the next yield point — a fetch that
  never yields continues occupying its worker thread until it completes, it
  is not preempted. **This bound is runtime-shape-dependent, not
  unconditional:** on a MULTI-THREAD tokio runtime the caller genuinely
  returns within the declared timeout regardless of the fetch's internal
  behavior, because a non-yielding spawned task can only occupy the one
  worker thread it landed on, leaving the timer free to fire on another
  thread. On a CURRENT-THREAD runtime there is only one OS thread total, so
  a spawned fetch that never cooperatively yields starves that thread
  entirely — including the timer backing this very deadline — and the
  caller does NOT return within the bound until the fetch itself yields or
  completes. Dropping the `resolve_blob_ref` future early (e.g. because ITS
  OWN caller applied a shorter outer timeout) has the same multi-thread-only
  guarantee, so a current-thread runtime is never left waiting
  un-cancellably by this mechanism alone — choose a multi-thread runtime
  when this deadline must hold regardless of a peer's behavior.
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

The seven ledger tables are not created for you. Every call below — `submit_job`,
`job_state`, `claim_job` — needs them present, so install them at open on each node:
`install_work_ledger_schema(&db)?` is idempotent, safe to call every time, and safe when
the schema already arrived via sync.

```rust,no_run
// Node A (holder): ingest a blob, serve it, and submit a job that references it.
use contextdb_engine::work_ledger::{InputRef, JobSpec, install_work_ledger_schema, submit_job};
use contextdb_server::{BlobStore, work_ledger::claim_job};

install_work_ledger_schema(&db)?;

let bytes = std::fs::read("frame.jpg")?;
let blob_hash = blob_store.ingest_bytes(&bytes)?;
blob_store.serve_on(&iroh_server);

let spec = JobSpec::builder("job-1", "describe-image", "once", "node-a")
    .input_refs(vec![InputRef::blob_ref(blob_hash.clone())])
    .build();
let no_direct_inputs: [&[u8]; 0] = [];
submit_job(&db, &spec, &no_direct_inputs)?;

// Node B (worker): claim the job under a lease, then resolve the referenced blob.
// Holding a live claim on this job is what entitles Node B to fetch the bytes.
let claim = claim_job(&sync_client, "job-1", 1, "node-b", lease_deadline_ms, now_ms).await?;
let mut sink = Vec::new();
let bytes_written = blob_store
    .resolve_blob_ref(&blob_hash, &holder_ticket, &mut sink)
    .await?;
```

---

## Upgrades and Recovery

### Upgrading the store format

The on-disk store carries a format-version marker (current: `1.0.0`). Opening a
data root written by an incompatible older release — either the top-level
marker doesn't match, or the marker matches but the underlying
`TableMeta`/`ColumnDef` row-meta layout still predates this release (see
`v1.0.0`, below) — fails closed with `LegacyVectorStoreDetected` rather than
silently corrupting or misreading it. The error names the recovery command:

```bash
contextdb migrate ./my.db
```

`migrate` writes a `./my.db.bak` backup of the untouched original BEFORE
changing anything, reads every row/edge/vector/DDL statement out of the legacy
root through a dedicated legacy-format reader, writes it into a fresh
current-format root, and atomically swaps it in. A second `migrate` run on the
now-current-format path is a safe no-op; running it on a path that was never
legacy refuses without touching the file. Sync-from-a-1.0+-peer remains an
alternative when you would rather populate a fresh store by a normal sync pull
than migrate the file directly.

The `v1.0.0`-specific case `migrate` was built against: that release's
`TableMeta`/`ColumnDef` structs had fewer trailing fields than today's, and
because the on-disk struct-as-tuple encoding carries no field-count marker, a
decoder that optimistically reads past its OWN declared fields (the pattern
this crate uses to tolerate an OLDER, shorter *current*-shaped payload) does
not cleanly detect "no more fields for me" on a genuinely OLDER struct shape —
it keeps consuming bytes belonging to the next field, and only surfaces once a
borrowed byte lands somewhere it can't satisfy. `migrate`'s legacy reader
matches the exact old field layout instead of leaning on that same tolerance.

### Recovering a wedged or corrupt data root

A corrupt or truncated store is detected on open and surfaced as
`StoreCorrupted`, with the error message naming the next commands rather than
leaving the caller to guess:

```bash
contextdb diagnose ./my.db   # read-only: reports what is salvageable/diagnosable, never modifies the store
contextdb reset ./my.db --force   # destructive: recreates a fresh, empty current-format store at the same path
```

`diagnose` reads the store's format marker and top-level schema layout through a
read-only handle and reports its diagnosis (current-format and readable,
legacy-format, or corrupt/truncated with the underlying reason) — it never
opens the store read-write and never writes to the path, so running it is
always safe. `reset` refuses without `--force` (see [CLI Reference](cli.md) for
the exit code it uses); with `--force` it deletes the existing file and
recreates an empty store, so restore anything you need from a backup or a
healthy sync peer FIRST if the data still matters.

A second `open` of a data-root file already held open by another process — same
process or a different one — returns a database-locked error; that is the
ownership guarantee described under Store Ownership & Concurrency, not a
corruption signal, and doesn't call for either command above.

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

The database tracks all row, graph, and vector allocations against one memory
budget. Embedding callers use the durable
`Database::set_memory_limit(Some(bytes))`; its setting survives reopen and
remains runtime-configurable. A process that needs a non-raisable startup
ceiling uses `contextdb_engine::database::open_with_startup_limits` (or
`open_memory_with_startup_limit`). The CLI's `--memory-limit` uses that same
startup path. Budget exceeded →
operations return `MemoryBudgetExceeded`. The raw mutable accountant is an
engine implementation detail and is not attachable or reachable through the
normal public API.

File-backed databases also support a persisted disk budget:

- startup ceiling/default via `--disk-limit`
- runtime control via `SET DISK_LIMIT` / `SHOW DISK_LIMIT`
- persisted live config in the redb file so reopen preserves the limit

Disk enforcement happens in the engine write paths before `INSERT`, `UPDATE`, and sync-apply work begins. Once the on-disk file is at or above the configured limit, further file-backed writes fail with `DiskBudgetExceeded`. In-memory databases accept the SQL but ignore disk budgeting because there is no backing file to measure.
