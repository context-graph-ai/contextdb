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
    └── contextdb-vector        Partitioned maintained search, durable base/change/tail generations
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

The engine depends on the maintained **`contextdb-redb`** package (library name `redb`) at
`crates/contextdb-redb`, based on upstream redb 4.1.0
(crates.io checksum `8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`, upstream
revision `6ed1f981ba4deab0b2adbdd7bccb46ec409b2191`). It retains the upstream `MIT OR Apache-2.0`
licence. The crate remains outside the workspace so its complete upstream integration suite runs
from its own locked manifest in CI. Its provenance, fork delta, upgrade procedure, and upstream
contribution path are maintained in `crates/contextdb-redb/MAINTENANCE.md`. Distribution requires
publishing this package before the engine, formatting/linting/testing its standalone manifest, and
building the unpacked engine against the unpacked fork with `scripts/verify-packaged-engine.sh`.
The tag publication job must depend on those checks for the same commit. An upstream proposal
remains part of maintenance; no upstream acceptance is claimed.

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
- A two-level local registry: `(table, column)` identifies a vector index and a typed declared
  partition key identifies one local search layout. The unpartitioned column has one empty key.
  This is a search layout only, never an authorization boundary, tenant, or sync direction.
  <!-- enforced by: vector_partition_query_contract::equality_on_every_partition_component_selects_one_named_tuple, vector_partition_query_contract::partition_scope_does_not_weaken_context_scope_or_principal_filters, vector_partition_sync_inspection_contract::partitioned_sync_derives_receiver_membership_without_changing_owner_pairing -->
- A commit updates the durable base/change/tail lifecycle for the affected layout. Inserts enter a
  fresh searchable tail; deletes and replacements add tombstones. Searches merge the valid base,
  sealed committed changes, and tail under one MVCC snapshot.
  <!-- enforced by: vector_serving_merge_contract::updated_base_and_tail_publish_one_visible_row_per_identity, vector_search_mode_bounded_contract::old_snapshot_keeps_each_partition_graph_after_a_later_write, vector_tail_transaction_contract::local_tail_admission_refuses_the_complete_transaction_before_durability -->
- `AUTO_INDEX_AT` and HNSW (`M`, `EF_CONSTRUCTION`, `EF_SEARCH`) are declarable per-column
  workload policy; silence keeps the compatibility profile. `AUTO` applies its effective threshold
  to the aggregate allowed set, while `EXACT` and `INDEXED` remain explicit whole-query contracts.
  The canonical SQL reference defines syntax, defaults, and online effects.
  <!-- enforced by: vector_policy_resolver_contract::declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries, vector_search_mode_bounded_contract::auto_uses_the_aggregate_selected_scope_not_each_partition, vector_search_mode_bounded_contract::exact_override_is_exhaustive_across_partitions_and_matches_rust_and_bounded_reads, vector_search_mode_bounded_contract::indexed_small_nonempty_scope_refuses_until_maintenance_then_keeps_a_staged_delta_visible -->
- Filtered and broad searches stay bounded. Selected layouts are searched independently with a
  global candidate heap, then one final top-k; the engine does not load every selected graph at once.
  <!-- enforced by: vector_partition_query_contract::finite_in_partition_scope_merges_before_one_limit, vector_search_mode_bounded_contract::filtered_many_partition_search_reuses_partition_local_candidates, vector_lazy_raw_residency_contract::indexed_many_partition_reads_point_load_only_visible_versioned_candidates -->
- Indexed routes have an acceptance target of at least 95% recall-at-10 against `EXACT` on the
  same stored column.
  <!-- enforced by: vector_held_out_native_reference::held_out_unfiltered_native_reference_recovers_the_required_neighbors, vector_serving_merge_contract::held_out_filtered_graph_search_preserves_quality_at_sparse_and_broad_selectivities -->

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
- On open: relational rows load with table-local index maintenance. Vector lifecycle metadata opens
  first; valid base and change graphs and raw-vector pages are loaded or mapped only for the active
  working set, rather than rebuilding every vector index.
  <!-- enforced by: vector_lazy_restart_residency_contract::reopen_keeps_saved_partitions_dormant_then_reclaims_one_least_recently_used_route_under_pressure, vector_lazy_consumer_restart_contract::point_lookup_after_reopen_loads_only_the_row_partition_and_returns_its_vector -->
- Crash-safe: redb provides atomic transactions
- Tables: rows, DDL metadata, graph edges, vector entries, counters
- Vector entries use one composite-key table keyed by `(table, column, row_id)`.
- A `metadata` table stores `format_version = "1.0.0"`; missing markers are
  treated as legacy stores, while unreadable markers are reported as corrupt.

### Vector generations, restart, and repair

Vector base generations, committed-change graphs, tombstones, and their checksums are durable local
state. Publishing a replacement writes temporary state, verifies it, then atomically switches the
authoritative pointer. A crash therefore leaves either the previous complete generation or the new
complete generation, never a half-published one. Loaded base and sealed change graphs are immutable:
inserts go into a fresh mutable tail, and deletes/replacements use snapshot-visible tombstones.
A deletion does not remove a node from a sealed graph in place. Search excludes invisible versions
and resolves candidates against the snapshot's authoritative vector. Maintenance publishes a
replacement that omits obsolete entries; an already-open snapshot keeps its compatible generation
until it releases its pin.
<!-- enforced by: vector_generation_quarantine_compaction_lock_contract::later_mutations_publish_a_new_generation_truncate_only_covered_journal_and_replay_the_suffix, vector_resource_maintenance_contract::replacement_refusal_precedes_workspace_and_keeps_the_serving_generation, vector_generation_authoritative_purge_replacement_contract::partial_purge_atomically_publishes_a_survivor_generation_from_a_dormant_route, vector_generation_quarantine_compaction_lock_contract::pinned_old_snapshot_keeps_its_indexed_generation_until_release_then_reclaims_superseded_bytes, vector_partition_restart_contract::an_open_old_snapshot_and_a_later_historical_snapshot_keep_their_complete_vector_views -->

Restart validates lightweight lifecycle metadata, keeps the last valid generation available, and
replays only verified changes after that generation into a new mutable tail as needed. It does not
reconstruct every vector before serving the first query. A corrupt or incompatible newest generation
is quarantined; a maintained indexed query for that layout refuses until repair, while `AUTO` may use
exact work only within the active budget. Repair and first build run through maintenance outside the
query path, so an older valid route and ordinary relational/graph reads remain usable.
<!-- enforced by: vector_partition_restart_contract::partitioned_indexed_search_survives_two_clean_restarts_without_a_query_rebuild, vector_lazy_replay_budget_contract::first_bounded_restart_query_charges_lazy_decode_and_replay_to_the_request_ceiling, vector_generation_quarantine_compaction_lock_contract::corrupt_lazy_base_becomes_typed_unavailable_then_repairs_from_raw_vectors, vector_generation_quarantine_compaction_lock_contract::newer_catalog_format_quarantines_only_that_route, vector_generation_quarantine_compaction_lock_contract::corrupt_journal_quarantines_and_repairs_one_route_while_a_cold_neighbour_builds, vector_generation_quarantine_compaction_lock_contract::caller_driven_repair_replaces_the_quarantined_generation_and_survives_restart, vector_generation_quarantine_compaction_lock_contract::engine_owned_worker_repairs_the_quarantined_route_without_a_caller_cycle -->

Raw vectors, base graphs, change graphs, and compaction workspace are independently accounted for.
Their pages are mapped or loaded for the active working set and can be evicted when idle; an active
query pins what it needs. When a declared `MEMORY_LIMIT` cannot admit a selected graph, idle graph
chains are reclaimed in least-recently-used order only until the complete load fits. Without a
declared memory limit, loaded graphs remain resident. <!-- enforced by: vector_lazy_restart_residency_contract::reopen_keeps_saved_partitions_dormant_then_reclaims_one_least_recently_used_route_under_pressure, vector_resource_maintenance_contract::unlimited_memory_keeps_loaded_graphs_warm_across_search_and_idle_maintenance, vector_resource_maintenance_contract::working_set_releases_other_partitions_before_the_next_selected_load --> Charged bytes are ContextDB's memory-accountant
view, not a claim about the operating system's physical mapped pages. Disk budget covers durable
vector and index state before a write is accepted. The memory target is the product declaration
`SET MEMORY_LIMIT 2G`, exactly 2147483648 bytes. <!-- enforced by: statement_effect_contract::binary_memory_declaration_accepts_quoted_and_unquoted_sizes --> Whole-process RSS, which also covers opening,
writes, queries, maintenance and replay, is separate from the charged vector/index categories.

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

contextdb starts one background maintenance thread per database, under the default engine-owned
policy, when the database declares something to maintain: a `RETAIN` window, `HISTORY CURRENT ONLY`,
a durable trigger with audit history, or maintained vector work. It advances a finite batch and does
near-zero work when nothing is pending. An embedding host may explicitly choose caller-driven
maintenance instead: then ContextDB starts no hidden thread and each existing
`Database::run_maintenance_cycle` call (or CLI `.maintenance run`) advances one finite batch.
Declarations, writes, open, and a first query never wait for every vector to be indexed.
<!-- enforced by: tests/integration/maintenance_ownership_tests.rs::a_fresh_database_defaults_to_engine_owned_maintenance, tests/integration/maintenance_ownership_tests.rs::caller_driven_spawns_no_thread_however_much_is_declared, vector_maintained_lifecycle_contract::caller_driven_vector_indexes_are_built_and_maintained_only_by_maintenance, vector_maintained_lifecycle_contract::engine_owned_file_maintenance_publishes_a_durable_indexed_route_for_a_reopened_reader -->

Each cycle advances retention, trigger-audit retention, version cleanup, and maintained vector
work. The default engine-owned loop polls every 60 seconds; callers can persist a different database
interval through `SET MAINTENANCE_POLL_INTERVAL`, `Database::set_maintenance_poll_interval`, or the
CLI's millisecond-only `--maintenance-poll-ms`; omitting the CLI flag uses the database's persisted
interval. The timer is only the trigger for evaluating work: per-column thresholds decide whether a
healthy route joins that wake. Each wake samples the finite vector backlog and advances every needy
partition sequentially within declared memory and work limits, with never-built partitions first.
There is no separate per-wake partition-count limit; finiteness comes from the fixed sample. A
per-column `CONSOLIDATION` policy decides when healthy graphs need rebuilding (rules and defaults:
[Vector Similarity Search](query-language.md#vector-similarity-search)); `CONSOLIDATION NONE`
suppresses only those threshold rebuilds, never construction, topology replacement, or quarantine
repair. Graph construction uses an immutable sample outside the short partition publication section, so
a same-partition write can finish into the newer fresh tail while the sampled generation publishes.
That write stays searchable and remains pending for a later publication. Retention runs before
current-only version cleanup, so an expiring row is not first collapsed as history. Initial graph
construction and repair use this maintenance path exclusively. Caller-driven policy is an open option
and CLI mode, starts no background worker, and uses the same all-needy cycle when the host calls it.
<!-- enforced by: vector_partition_declaration_contract::consolidation_and_maintenance_poll_declarations_round_trip_and_reset, vector_resource_maintenance_contract::changing_the_poll_interval_returns_while_engine_maintenance_is_active, read_cli_journeys_invocation::maintenance_flags_require_write_and_configure_the_writer, vector_resource_maintenance_contract::in_memory_maintenance_honours_declared_consolidation_thresholds, vector_resource_maintenance_contract::in_memory_construction_leaves_same_partition_vector_writes_usable, vector_resource_maintenance_contract::failed_partition_does_not_starve_actual_work_and_reports_active_progress -->

A partition-local build refusal is attached to the vector maintenance report alongside successful
index and partition counts. It does not short-circuit the wake's closing graph reclamation, empty
partition retirement, automatic-compaction attempt, or cycle stamp; cancellation remains a typed
interruption and returns immediately.
<!-- enforced by: vector_resource_maintenance_contract::refused_partition_still_closes_the_cycle_and_retires_a_sibling_partition, vector_resource_maintenance_contract::dropping_a_sampled_vector_column_does_not_abort_cycle_closing -->

**Version cleanup and a held read snapshot.** Every in-flight statement registers its own read snapshot for the call's duration, and a caller that needs to reuse a `SnapshotId` ACROSS separate calls (on a table declaring `HISTORY CURRENT ONLY`) registers it explicitly via `Database::pin_snapshot`, holding the returned guard for as long as the snapshot is still wanted. A version-cleanup pass samples every currently-registered snapshot (not merely the oldest) plus the committed watermark, atomically, once at the start of the pass, and defers any superseded version still visible to ANY of those registered snapshots to a later cycle — a version created between two registered snapshots and superseded after both is protected by the higher one even though the lower one alone would not see it. Protection begins when `pin_snapshot` RETURNS: a pin requested while a removal pass is already mid-flight, for a snapshot at or before that pass's sampled watermark, waits for the pass to finish first (bounded by one pass duration) before registering, so the pin can never return with a false promise of protection the SAME in-flight pass is still free to violate — the next cleanup cycle honors it instead. A pin for a snapshot strictly after the pass's watermark registers immediately: nothing that pass can prune was ever visible to it. Versions a pass already reclaimed before the pin is requested cannot come back; that boundary is unchanged.

**The cost model.** Version cleanup examines eligible table histories and the affected vector
columns' partition-directory metadata to select exact version identities; selection does not load
vector bodies. One redb transaction then removes the selected row versions and change-log entries,
vector bodies, directory memberships, eligible deletion records, and eligible commit-index entries. Memory
changes only after that transaction commits. A crash before commit leaves the selected scope intact;
a crash after commit leaves it durably removed, so retry does not depend on a deleted discovery row.
Registered readers defer versions they still need. Immutable graphs may still name old versions;
query merge scores each distinct candidate from its snapshot-visible native vector before ordering
and applying the limit, even after cleanup leaves only one directory version.
<!-- enforced by: tests/integration/version_cleanup_scoping_tests.rs::version_cleanup_releases_a_pruned_rows_vector_copy, tests/integration/version_cleanup_scoping_tests.rs::version_cleanup_cost_is_invariant_to_unrelated_vector_ballast, tests/integration/version_cleanup_scoping_tests.rs::a_registered_snapshot_defers_cleanup_instead_of_losing_the_version, vector_serving_merge_contract::current_only_cleanup_keeps_native_candidate_order_across_readers_and_restart -->

Retention first commits expiry as a visibility boundary: newly opened reads exclude expired rows,
while already-registered snapshots retain their complete row/vector/graph view. Physical reclamation
defers versions those readers still need, including vector generations. After the pins are released,
a later cycle can reclaim them; expiry alone does not promise an immediate fall in resident bytes.
Retention commits its selected row versions, change-log and source-LSN records, vector
copies, reclaimable adjacency, reader-deferred adjacency identities, eligible commit-index entries,
and reclaimable vector generations in one redb transaction. In-memory removal follows that commit.
Cleanup cost includes selection through the
eligible histories/directories, membership-prefix checks for selected vectors, and reading and
rewriting affected change-log LSN groups. It is not proportional only to the number of removed keys.
Surviving rows and unrelated tables remain untouched. Version cleanup never opens graph tables:
edge identity is self-owned and is not versioned by a relational row.
<!-- enforced by: tests/integration/retention_tests.rs::retention_defers_reclaim_without_breaking_vector_search, tests/integration/retention_tests.rs::retention_keeps_rows_an_explicit_snapshot_pin_is_entitled_to, tests/integration/named_vector_indexes_tests.rs::nv_retain_prunes_every_vector_index_for_expired_row -->

**File compaction is separate from scoped cleanup.** Retention and version cleanup reclaim only the
versions they select; redb can reuse those pages, so reclaiming rows does not itself promise a
smaller file.

Automatic file compaction has two triggers. A retention pass that actually pruned rows starts a
sweep when its pre-prune dead-space sample is at least 50%. At the end of a maintenance cycle, a
new sweep starts when dead space is at least 50% and at least one hour has elapsed since the last
completed sweep. Both use ContextDB's vendored redb `compact_step` path to relocate at most 64
pages in one batch. Relocation uses copy-on-write pages and retains old pages behind redb's reader
fence, so existing and newly opened reads can overlap an active batch. Foreground writes share the
single writer lock with each finite relocation or free-page transaction; the database handle lock
is released during relocation. Later maintenance cycles resume the cursor until the sweep completes.
Completion recycles the handle only when no live storage read transaction owns it; otherwise the
existing handle stays open. A restart discards the in-process cursor and a later eligible cycle
starts a new sweep. `MaintenanceReport.compaction` reports the end-of-cycle batch; `handle_recycled`
and `file_shrank` report observed results, not unconditional recycling or shrink guarantees.
<!-- enforced by: storage_compaction_online::relocation_batches_preserve_values_with_interleaved_writes_and_restart, storage_compaction_online::automatic_storage_batches_allow_recording_and_readback_before_completion, storage_compaction_online::paused_storage_statistics_allow_a_foreground_commit, tests/integration/compaction_separation_tests.rs::the_automatic_compaction_path_is_interval_gated_not_per_cycle, tests/integration/compaction_separation_tests.rs::compact_now_recycles_the_handle_and_reports_it_honestly -->

`Database::compact_now()` (the CLI's `.maintenance compact`) is the explicit alternative. It drains
the file compaction immediately, without the automatic threshold or interval gate. It is distinct
from `.maintenance run` and from the scoped cleanup reports.
<!-- enforced by: tests/integration/compaction_separation_tests.rs::compact_now_is_unconditional_and_reports_a_real_receipt, tests/integration/compaction_separation_tests.rs::currency_cleanup_never_compacts_on_its_own -->

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

The 2 GB edge working-set target guides constrained deployments. For a constrained device, SQ8 can
reduce the stored vector footprint:

```sql
SET MEMORY_LIMIT '1536M';
CREATE TABLE evidence (
  id UUID PRIMARY KEY,
  vector_text VECTOR(768) WITH (quantization = 'SQ8'),
  vector_vision VECTOR(512) WITH (quantization = 'SQ8')
);
```

`SHOW VECTOR_INDEXES` gives one summary row per vector column,
including aggregate counts and live vector payload byte totals. `SHOW
VECTOR_PARTITIONS FOR table.column` supplies the typed-key, per-partition
lifecycle detail, including each durable generation's state; use those
surfaces instead of parsing memory operation tags.
<!-- enforced by: vector_partition_sync_inspection_contract::existing_two_row_sync_keeps_each_vector_with_its_row_and_shows_summary, vector_partition_sync_inspection_contract::show_vector_partitions_supports_all_sql_forms, vector_inspection_explain_truth_contract::vector_inspection_is_passive_and_reports_real_pre_and_post_maintenance_facts -->

---

## Sync

This release emits and accepts sync protocol 7. Vector declarations use that protocol's schema
vocabulary without changing the vector payload shape. The ALPN identifier remains `contextdb.sync.v6`;
it names the transport framing, including the reply-receipt byte that lets graceful shutdown prove
the dialing peer received a terminal reply and the per-publication nonce that prevents identical
large replies from sharing durable bytes or completion state. Payload version skew is checked by
the envelope, not the ALPN.
<!-- enforced by: protocol_version_bump_tests::first_release_accepts_only_protocol_seven, protocol_version_bump_tests::every_noncurrent_envelope_is_rejected_by_encoding_and_decoding -->
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
When a peer cannot represent a table's declared schema vocabulary, ContextDB holds that table with
a typed diagnostic naming the table, missing capability, and node to upgrade. Unaffected tables
continue syncing and the held table resumes after the capability is available.
<!-- enforced by: vector_schema_mixed_version_compatibility_contract::older_receiver_keeps_ordinary_tables_flowing_then_resumes_the_held_table_after_upgrade, vector_schema_mixed_version_push_compatibility_contract::newer_edge_keeps_ordinary_tables_flowing_then_resumes_the_held_table_after_hub_upgrade -->

The supported skew window is the current protocol version plus the two immediately previous
released versions. The current wire retains row arrival ordering, the serving-store incarnation in
pull responses, the purge instruction lane, schema provenance, and the structured
authoritative-hub purge refusal.
<!-- enforced by: protocol_version_bump_tests::current_wire_arrival_and_source_fields_round_trip, protocol_version_bump_tests::current_wire_purge_instruction_and_typed_authority_error_round_trip, schema_provenance_wire_contract::nonempty_schema_provenance_round_trips_and_validates -->

Future work bumps the protocol version whenever it changes sync bytes or sync semantics. SQL, storage, CLI, or maintenance work that leaves sync unchanged does not bump the protocol.

Partitioned vector search leaves the sync message shape unchanged. Partition-key declarations travel
with ordinary schema DDL; on receipt, ContextDB derives the local layout membership from the row it
accepted in the same transaction. A key-only update moves each non-NULL vector locally even when no
new vector payload arrives. This local layout neither authorizes a row nor changes a table's declared
sync direction.
<!-- enforced by: vector_partition_sync_inspection_contract::partitioned_sync_derives_receiver_membership_without_changing_owner_pairing, vector_partition_sync_inspection_contract::received_key_only_update_moves_the_unchanged_vector, vector_partition_sync_inspection_contract::every_consolidation_form_keeps_schema_identity_across_sync -->

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

### Manifests and outcomes

The engine's private `custody` modules own canonical policy, manifest, signed outcome, and
incarnation records in the existing config-value journal. `SyncClient` and `SyncServer` in
`contextdb-engine` perform authenticated bind, ordinary manifested push, status, and outcome fetch;
`contextdb-server` re-exports them. Row/outcome acceptance shares the production Redb transaction.
Local `DISCARD` stages a physical erasure projection in the ordinary write transaction and publishes
it with the ordinary writes only after durable success. It reuses the authoritative purge's physical
erasure writer and failure boundary but creates no fleet frontier.

Custody does not maintain a per-commit history chain or inspect its journal on unrelated writes.
The owned immutable metadata cache is invalidated at custody commits and erasure. Admin `SHOW`
uses that metadata; scoped delivery reads use the existing root visibility and read-route budgets.

Every application column contributes to the canonical BLAKE3 row and unit digests; local bookkeeping
belongs outside those rows when identical imports must compare equivalent. One durable terminal
outcome shares the unit's commit. Outcome fetch returns every outcome after the supplied acceptance position,
including every outcome sharing one position. Watermarks never substitute for receipts.
The journal grows with retained units and is erased with them, not reclaimed by ordinary delivery.
On mutable tables, updating or deleting a manifested row retires its current manifest ownership in
the row commit while retaining signed history and terminal outcomes for inspection. A replacement
root without a newly registered manifest receives `manifest_required`; unrelated units keep syncing.
Deletes follow the table's ordinary conflict policy and are never reoffered as live manifested rows.
Delivery counts include current manifest owners only; re-registration creates a new pending unit.

An ordinary status request carries one hub-signed prefix checkpoint. The hub checks that prefix
against its committed image; unrelated writes and purges cannot conceal a lost accepted prefix.
A current image retains its incarnation. An older image rotates it, including a declared snapshot
from before the first binding, and the edge automatically rebinds and returns old credit to pending.
Missing policy declarations still require the operator to declare them. Verification failure is
reported to the caller.
A listed node-local purge predicate and bound values travel as permitted journal erasure instructions;
each edge selects and erases its own keys within the same applied boundary.

### Components

- `SyncClient` — runs on each participant. Pushes local changes to server, pulls remote changes.
- `SyncServer` — runs on the central server. Receives pushes, serves pulls.

Both communicate over per-tenant sync channels: `sync.{tenant_id}.push` / `sync.{tenant_id}.pull`.
Custody adds `BindApplicationTablePolicyRequest` and `FetchDeliveryOutcomesRequest`. Manifests and
outcomes travel on ordinary push/reply; ordinary status carries the hub incarnation and verifies
one signed prefix checkpoint. There is no separate purge or authority-exchange request kind.

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

Hub declarations persist before an application table exists. The first authenticated binding
freezes each declared policy. Bound local and arriving DDL must match the binding; arriving DDL
cannot overwrite an explicitly declared local policy. A per-table refusal leaves unrelated tables
available for sync.

### Tenants and Contexts

A tenant's application-table policy is declared at its authoritative hub and bound independently
of enrollment. The binding names the tenant, hub identity and database incarnation.

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
