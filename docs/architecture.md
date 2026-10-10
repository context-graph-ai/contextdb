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

Modules that carry the bounded writer working set (details in each crate's `AGENTS.md`):

| Crate | Module | What it does |
|---|---|---|
| `contextdb-redb` | `tree_store/page_store/allocator_pager.rs` | Pages allocator bitmap words through 32 fixed windows with same-directory scratch spill |
| `contextdb-hnsw` | `paged.rs` | Builds and searches an HNSW graph over caller-supplied point storage, keeping one insertion's workspace |
| `contextdb-vector` | `hnsw/paged_builder.rs`, `store/membership.rs` | Transactional paged graph builder and tail; durable raw-membership sources with no retained directory |
| `contextdb-relational` | `source.rs` | Root-pinned row and index reads with staged-row merge |
| `contextdb-graph` | `source.rs` | Root-pinned adjacency reads with an edge overlay |
| `contextdb-tx` | `commit_directory.rs`, `ordered_staged.rs` | Durable commit-history seam; ordered staged rows |
| `contextdb-engine` | `persistence/writer_*.rs`, `database/writer_read.rs` | Pinned-root writer reads, durable history cursors, open bounds, statement read scope |

A writable file store keeps a bounded working set however long its history is: it opens without
reading row versions, change log or commit index, serves reads from roots pinned at publication,
and releases pages, indexes and history after use; readers keep the view they opened with.
<!-- enforced by: writable_open_never_touches_history_families::writable_open_never_touches_history_families, writer_reopen_and_doubling_keep_a_fixed_resident_envelope::writer_reopen_and_doubling_keep_a_fixed_resident_envelope, writer_reader_root_survives_commit_eviction_and_cleanup::writer_reader_root_survives_commit_eviction_and_cleanup -->

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
An upstream proposal remains part of maintenance; no upstream acceptance is claimed.

The fork's `contextdb-internal` byte-table seam provides bounded lookup, prefix inspection and a
root-pinned opaque cursor. Reject-only windows can exclude an unrelated oversized singleton without
materializing its full extent. Possible matches use ordinary complete-page admission and decoding;
retained roots keep complete checksum verification. Complete nodes reached by lookup, prefix
iteration or cursor navigation validate their layouts before offset access, so malformed leaves and
branches return typed corruption without panic. Branch cycles on all three paths return typed
corruption without hanging. The engine owns inventory policy above this seam.
<!-- enforced by: crates/contextdb-redb/src/tree_store/bounded_bytes.rs::absent_lookup_and_prefix_neighbor_reject_without_materializing_the_extent, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::retained_root_inspection_keeps_full_checksum_admission, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::bounded_lookup_rejects_malformed_leaf_and_branch_without_panicking, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::bounded_prefix_iteration_rejects_malformed_leaf_and_branch_without_panicking, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::cursor_layout_validation_rejects_malformed_navigation, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::bounded_lookup_rejects_branch_cycle_without_hanging, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::bounded_prefix_iteration_rejects_branch_cycle_without_hanging, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::cursor_navigation_rejects_branch_cycle_without_hanging -->

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
  <!-- enforced by: vector_policy_resolver_contract::declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries, vector_search_mode_bounded_contract::auto_admits_indexed_planning_by_aggregate_then_exact_compares_each_small_partition, vector_search_mode_bounded_contract::exact_override_is_exhaustive_across_partitions_and_matches_rust_and_bounded_reads, vector_search_mode_bounded_contract::indexed_small_nonempty_scope_refuses_until_maintenance_then_keeps_a_staged_delta_visible -->
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
one of two routes when it opens. Ordinary reads keep that route for the session:

- **Owner route** — a live process owns the store, so the reader is served that
  owner's committed state over an authenticated local channel. The owner's
  declared ceilings and deadlines govern what it may ask for.
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

An in-memory database has no backing file, companion, or owner channel.
<!-- enforced by: crates/contextdb-cli/tests/read_cli_journeys_invocation.rs::memory_store_emits_no_route_notice_and_reports_owner_state_not_applicable -->

The supplied runtime directory names the owner channel; a channel startup failure leaves the
database open with a `not_serving` status. Reader coordination uses the platform per-user
directory when available, otherwise the supplied directory. Inventories inspect every known
candidate directory, and an empty inventory does not prove that no reader exists. A writer
publishes purge fences to each candidate. If one cannot be authenticated, the affected purge or
reclamation refuses with a typed error naming that directory while database open remains
available. On Linux each reader also holds byte-free database-file advisory presence locks.
An unmatched lock retains potentially old pages during commits and refuses explicit purge or
reclamation with `ReaderCoordinationUnavailable`, covering readers with different supplied roots
when no platform root exists. The filesystem must support Linux OFD locks. Other Unix systems
without a common platform root retain a conservative floor and refuse these operations when
the inventory cannot be proved complete.

**Holder presence.** A process that owns background work beside a store, and keeps working while
its `Database` handle is closed, proves it is alive with `persistence::holder_presence::hold`: an
exclusive Linux OFD lock on one byte of the store's empty `<store>.holders` companion, chosen from
the work's 16-byte token. The lock lasts as long as the returned guard, independent of any
`Database` handle, and the kernel releases it when the holder exits for any reason, a kill or a
crash included; a stopped or suspended process keeps it. `probe` asks whether a token's holder is
alive without changing a byte or creating the companion, and `wait_released` blocks in the kernel
until the holder releases or exits, with no polling and no timeout. A filesystem that cannot take
the lock refuses with `HolderPresenceUnavailable` before anything is written, and so does any
platform without OFD locks. Holder locks never touch the data file or the `.lock` companion, so
they neither hide nor impersonate a reader or the writer. Presence reports liveness only: a
consumer that publishes work under a claim still fences that publication on its own token inside
the publishing transaction, because a holder can lose its lock without exiting.

Once a file reader has captured its committed image, it publishes an authenticated registration
for that physical store incarnation and exact storage root for the lifetime of the shared session
target. A later writer validates the pre-writer registrations before it can allocate or reclaim
pages. Redb authenticates each saved root page and pins its transaction as a floor for durable
deferred frees. Cursor admission keeps only fixed-size root metadata and does not enumerate the
database under the commit lock. While a root is held, this conservative floor can also defer
unrelated freed pages. They remain in the store's existing disk allocation and disk limit; the
on-demand root-page diagnostic counts only the pages reachable from registered roots. The last
close or authenticated process death releases the snapshot floor; subsequent durable writer
commits reclaim eligible extents incrementally. Each such commit examines at most 64 deferred-free
list entries and reclaims at most 64 allocated extents in total across redb's data and system free
trees. An extent is one contiguous allocation and can span multiple physical pages, so this is
not a 64-physical-page or byte bound. The separate append-time and memory acceptance gates still
apply.
The remaining list position is durable, so an ordinary abort before commit-only reclamation leaves
pending work intact and later commits continue after restart. Reclamation runs only inside commit,
after the ordinary abort path is disabled; aborting after that private reclaim stage is not a
supported transaction operation. A released cursor does not make the next small append drain an
arbitrarily large backlog.
Explicit close drains eligible deferred frees through bounded commits before publishing the
read-only handoff. A live reader or savepoint leaves protected work for a later writer. After
handoff publication, no storage write is permitted. Cleanup or handoff failure is reported from
explicit close after owner resources are released; an owner-read timeout can defer that failure
until a later close finishes the drain. Destructor cleanup does not promise a drain.
This is allocator cleanup behind the reader fence, distinct from retention's selection and removal
of logical row versions below. Handles and cursors sharing a direct-file session target share one
root hold. The registration does not hold writer admission for the session.
<!-- enforced by: cursor_survives_writer_handover::closing_a_cursor_keeps_the_next_small_append_cleanup_bounded, crates/contextdb-redb/src/transactions.rs::released_snapshot_cleanup_is_bounded_durable_and_eventually_drains, crates/contextdb-redb/src/transactions.rs::empty_deferred_lists_consume_the_entry_budget, crates/contextdb-redb/src/transactions.rs::one_unit_budget_progresses_in_both_trees -->

Before asking an owner to open a cursor, the client registers a provisional hold on the exact
physical file. After receiving the first page it publishes the exact saved root before releasing
that provisional hold. This overlap protects the original snapshot even when the owner closes and
a replacement writer commits between response receipt and exact registration; registration does
not require the old writer to remain present. The exact hold remains with the cursor when the owner
exits. If the owner channel is withdrawn, the cursor continues through the file reader at
that registered snapshot: it reexecutes its statement, skips the rows already delivered, and
returns the remaining pages under the original cursor limits. A new writer may commit in between
without changing the cursor's answer or forcing the caller to retry. Authoritative erasure returns
its typed refusal and immediately releases the client's cursor registration and retained resources,
even while the failed cursor object remains alive. A complete first page or final fetch releases
the cursor's root even when the caller keeps its cursor object. A deadline watcher uses the cursor's
declared clock to release an idle or lifetime-expired root without another fetch; a later fetch
keeps the typed expiry. Direct-file cursors release their own execution resources at the same
terminal pages, while a shared direct-file session root remains held for that session's lifetime.
This continuation is internal to an already-open cursor; the session's selected route and ordinary
request behavior remain fixed.
Replay spends the caller's per-fetch work and active-time budget. If catching up uses that budget,
the cursor keeps its position and may return an empty page with `has_more` before continuing on
the next fetch. Admission reads one root page regardless of unrelated store size; explicit
diagnostics may inspect all root pages. Replay work grows with rows already delivered.
<!-- enforced by: cursor_survives_writer_handover::cursor_opened_before_writer_handover_finishes_its_snapshot, cursor_survives_writer_handover::owner_exit_after_open_response_does_not_break_client_registration, cursor_survives_writer_handover::small_cursor_admission_work_and_charged_memory_ignore_unrelated_store_size, cursor_survives_writer_handover::final_fetch_releases_pages_before_the_cursor_object_is_dropped, cursor_survives_writer_handover::idle_expiry_reclaims_pages_without_another_client_request, cursor_survives_writer_handover::replay_yields_under_one_public_fetch_budget_and_keeps_its_progress, authoritative_purge_reader_publication::owner_cursor_paused_before_carrier_publication_cannot_emit_after_purge -->

A file reader opens from compact indexes the writer keeps beside the data, not from a copy of the
whole store. At open it reads one small membership record per stored vector, checked against a
commitment the writer publishes in the same transaction, and it compares that complete
membership and visibility copy with a set commitment carried in the committed vector family
roots — the same identity that authenticates vector bodies — before the copy controls search
selection. It also compares the vector table's physical root fingerprint with the committed one
without reading any vector body. It copies only the handful of settings a reader needs to start
and compares them with an ordered digest carried in the committed Configuration root. The bytes
an open reads for vectors and settings therefore do not grow with vector dimension or with
unrelated configuration values; the vector part grows only with the number of vectors, one small
record each. Rows, vector bodies and every other setting are read and authenticated from the
reader's pinned root only when a query selects them. A tampered or incomplete index or startup
copy is refused at open as corruption. A membership copy whose deletion markers hide a still-
committed vector is refused at open even when its local checksums agree: the committed vector
roots carry an order-independent lattice-hash commitment to the complete membership set, which the
writer keeps current by folding in only the memberships each commit changes, so appending to a
large store does not rehash its memberships. The copied custody index used for read-only policy
and delivery inspection authenticates each copied record and compares the complete copy with the
same kind of commitment in the committed Configuration root, also maintained per changed record,
so a shortened copy is refused as damage even when its local checksums agree. That check runs when
inspection reads the index, not at open, and it does not read unrelated configuration values.
A damaged vector key is refused at open; damage confined to a vector's stored value is refused
when a query selects that vector.
<!-- enforced by: open_reads_compact_vector_and_config_state::opening_equal_vector_memberships_reads_bytes_independent_of_dimension, open_reads_compact_vector_and_config_state::opening_reads_only_config_values_needed_for_the_reader_image, open_reads_compact_vector_and_config_state::a_tampered_compact_vector_directory_is_refused_at_open, open_reads_compact_vector_and_config_state::a_self_consistent_visibility_copy_cannot_hide_a_committed_vector, open_reads_compact_vector_and_config_state::a_committed_vector_delete_stays_absent_after_reopen, open_reads_compact_vector_and_config_state::a_replayed_startup_setting_with_recomputed_copy_checksums_is_refused_at_open, open_reads_compact_vector_and_config_state::a_missing_copied_startup_setting_with_recomputed_copy_checksums_is_refused_at_open, open_reads_compact_vector_and_config_state::a_custody_copy_missing_one_policy_cannot_return_a_shortened_show_list, read_only_open_loads_only_the_working_set::open_configuration_proofs_follow_needed_settings_not_unrelated_config_entries, relational_on_demand_source_contract::public_relational_reads_fault_only_the_selected_rows_and_survive_writer_recycle, raw_vector_on_demand_source_contract::mixed_file_reads_point_load_exact_vectors_and_keep_their_snapshot_after_recycle, direct_file_reader_contract_tests::selected_quantized_vectors_are_authenticated_before_they_are_returned -->

When the owner closes gracefully, readers are handed over rather than cut off. The owner first
stops accepting writes, makes the file's committed state clean for direct readers, and publishes
that the store is quiescent while it still holds the store; only then does it withdraw its read
channel. Owner reads it had already accepted run to completion, or to their own declared limits,
before it releases the store. A reader that opens during the handoff takes the file route and sees
the committed rows. When several callers close the same database at once, one of them performs the
shutdown and the others return only after it has finished, so a writable reopen after `close()`
returns never races a half-finished shutdown.
<!-- enforced by: graceful_owner_drop_completes_accepted_read::graceful_close_publishes_quiescence_before_withdrawing_the_channel, graceful_owner_drop_completes_accepted_read::dropped_owner_finishes_an_accepted_request_before_releasing_the_store, graceful_owner_drop_completes_accepted_read::two_ordinary_close_callers_wait_for_one_complete_shutdown, graceful_owner_drop_completes_accepted_read::ordinary_close_waits_until_callback_dispatcher_releases_writer_resources -->

`ReadSession::charged_bytes` reports the memory the session is currently charged for. On the file
route that is the session's own charge; on the owner route it is the owner's shared usage, which
can include other work. Closing a cursor releases its result memory, but a storage or proof page
still held by the session or another reader stays charged until its last holder releases it.
Evicting the cache entry alone does not release that charge.
<!-- enforced by: session_charged_bytes::public_charged_bytes_reports_and_releases_cursor_hydration_on_both_routes, direct_file_image_memory_lifetime::a_read_within_limit_succeeds_and_keeps_its_page_charged_until_the_session_drops, crates/contextdb-engine/src/persistence/search_page_reuse_tests.rs::a_proof_page_a_statement_still_holds_stays_charged_after_the_cache_evicts_it, crates/contextdb-engine/src/persistence/search_page_reuse_tests.rs::statement_cleanup_after_success_refusal_and_cancellation_leaves_no_uncharged_page -->

A stored value too large for a read's memory budget is refused before its source buffer is
allocated. Opening a session need not read that value: the refusal belongs to the statement that
selects it, on both the file and owner routes, and leaves no new charge behind.
<!-- enforced by: direct_file_image_memory_lifetime::one_oversized_durable_value_is_refused_before_its_source_buffer_is_allocated -->

Fallible engine reads that return history-sized results use a per-operation reader child and
admit retained output as it is built. `trigger_audit_history` and `changes_since_with_arrivals`
therefore refuse with `MemoryBudgetExceeded` when their selected history exceeds that child;
`get_edge_properties` uses the same pattern for its returned properties. Configuration-scale
metadata reads (`table_names`, `table_meta`, `retention_sync_peer`, `list_triggers`, and
`registered_trigger_callbacks`) remain infallible and are exempt from per-operation admission.
`plugin_describe` returns host-supplied plugin output, and `RowChange`/`ChangeSet` DTO cloning and
transformations operate on values already owned by the caller; those allocations belong to the
host. `trigger_audit_log` is a bounded recent ring rather than persisted unbounded history.

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
registered through `Database::register_transactional_trigger_callback` (or the
older `register_trigger_callback` name). They are not
PG-style validation triggers; schema invariants remain engine-enforced through
DDL. A trigger callback runs synchronously inside the firing transaction's
commit window, and callback writes use the supplied tx-bound `Database` handle
so relational rows, graph edges, and vectors commit atomically. The callback
runs before final validation, so its writes are validated with the rest of the
transaction, and a refused commit publishes none of them. External effects
belong in a sink registered with `Database::register_sink`, which the durable
queue invokes only after commit; see
[`README.md`](../README.md#triggers) for the delivery and retry contract.

The callback-active contract is deliberately split by concurrency domain:

- same-DB trigger contention waits-and-proceeds inside the engine, including
  public tx-control, SQL write paths, direct write helpers, and internal
  handles that share the same trigger state
- unrelated cross-DB writers proceed independently; a parked callback on DB-X
  does not poison ordinary worker-thread writes on DB-Y
- callback-thread reentry returns `CallbackReentry`; retrying inside
  the callback body is misuse
- callback tx-bound handles remain isolated to their runner thread
- cron same-DB contention keeps the immediate typed cron callback-active error
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
- Committed row bodies: a file-backed store keeps key, index, constraint and policy columns and
  values up to 1 KiB resident; larger text/JSON values and vectors stay only in redb. Anything that
  hands a row on — a pushed or pulled sync changeset, a full-state snapshot, a custody manifest
  digest — reads the paged-out columns back from that row's exact durable version when the change is
  built, one row at a time, so the wire unit carries every committed column. A unit that then
  exceeds one sync frame travels on the oversized staging transport. Under a bounded history
  answer the version's stored size is charged to the declared memory limit before it is read.
  <!-- enforced by: delivery_manifest_contract::a_manifested_unit_travels_on_the_ordinary_push_and_the_oversized_staging_path_and_is_never_served_to_a_pulling_peer -->
- Vector membership commitments are updated from the records changed by the commit. Appending
  or deleting one vector does not rehash the complete membership directory; the same incremental
  rule applies to a custody-policy write's set commitment.
  <!-- enforced by: one_vector_append_does_not_rescan_the_directory::appending_one_vector_hashes_only_what_that_commit_changed, one_vector_append_does_not_rescan_the_directory::deleting_one_vector_hashes_only_what_that_commit_changed, one_vector_append_does_not_rescan_the_directory::one_custody_policy_write_hashes_only_what_that_commit_changed -->
- Tables: rows, DDL metadata, graph edges, vector entries, counters
- Vector entries use one composite-key table keyed by `(table, column, row_id)`.
- A `metadata` table stores `format_version = "1.0.0"`; missing markers are
  treated as incompatible stores, while unreadable markers are reported as corrupt.

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
Generation admission counts raw versions separately from selected live graph points at one committed
frontier. Ordinary resident generations price and copy only their consumed frontier-live entries,
with an optional LSN boundary for a change graph. Selection preserves every retained membership,
MVCC interval and canonical digest, including when writes advance after the captured frontier;
refusal precedes body copying. Tombstone metadata does not add ordinary graph points. Full cold
snapshots and delete-only carriers retain their raw-history path; change graphs use the actual live
delta and the existing delete-only carrier rule.
<!-- enforced by: crates/contextdb-engine/src/database/maintenance_resource_tests.rs::durable_generation_workspace_separates_build_and_persistence_owners, crates/contextdb-engine/src/database/maintenance_resource_tests.rs::a_retained_raw_body_has_decode_scratch_even_without_live_graph_points, vector_generation_admission_contract::retained_versions_and_tombstones_admit_base_replacement_and_serve_old_graph, vector_generation_delta_admission_contract::small_delta_refuses_growing_prior_base_candidate_before_allocation_then_retries -->
<!-- enforced by: crates/contextdb-engine/src/database/maintenance_resource_tests.rs::resident_generation_selection_preserves_retained_canonical_digests, crates/contextdb-engine/src/database/maintenance_resource_tests.rs::resident_generation_selection_keeps_the_pinned_frontier_during_concurrent_writes, crates/contextdb-engine/src/database/maintenance_resource_tests.rs::resident_generation_selection_admits_history_below_the_full_copy_bound_and_refuses_before_bodies, crates/contextdb-vector/src/store/generation_selection_tests.rs::selected_generation_materialization_preserves_all_representations_and_snapshot_intervals, crates/contextdb-vector/src/store/generation_selection_tests.rs::selected_generation_refuses_inconsistent_metadata_before_body_materialization -->

A resident partition snapshot funds its copies without a generation-owned decoder. Dormant or
evicted raw partitions admit loader decode scratch separately; direct cold reads and delete-only
carriers retain the generation decoder's 1 MiB serde scratch floor as well as declared-body scratch,
even when retained history has no live graph points.
<!-- enforced by: crates/contextdb-engine/src/database/vector_generation_decode_owner_tests.rs::resident_f32_generation_funds_snapshot_without_a_decoder_owner, crates/contextdb-engine/src/database/vector_generation_decode_owner_tests.rs::resident_sq8_generation_funds_snapshot_without_a_decoder_owner, crates/contextdb-engine/src/database/vector_generation_decode_owner_tests.rs::resident_sq4_generation_funds_snapshot_without_a_decoder_owner, crates/contextdb-engine/src/database/vector_generation_decode_owner_tests.rs::direct_cold_and_delete_carrier_generations_keep_the_decode_floor, crates/contextdb-engine/src/database/vector_generation_decode_owner_tests.rs::dormant_partition_loader_admits_its_decode_floor_before_reads_and_releases -->

Construction owns the raw snapshot, selected input and
builder workspace; after input clones drop, encoding owns the builder and one checked codec
envelope bound and encoder scratch. After the builder drops, the raw snapshot coexists with the
candidate. The state digest equals the BLAKE3 hash of the canonical encoding of sorted snapshot-live entries
(the proof checks byte identity, not streaming or peak allocation). After the raw snapshot drops, persistence
owns the candidate and its encoding, validation and durable-write copies. Only change publication
loads the prior base. The shared accountant atomically replaces each phase's held charge without
exposing its capacity or charging both phases; refusal leaves the old charge intact.
<!-- enforced by: crates/contextdb-engine/src/database/maintenance_resource_tests.rs::durable_generation_workspace_separates_build_and_persistence_owners, crates/contextdb-vector/src/hnsw.rs::durable_codec_capacity_bounds_all_stored_representations_and_checks_overflow, crates/contextdb-engine/src/database/maintenance_resource_tests.rs::streamed_state_digest_preserves_the_canonical_live_entry_encoding, vector_generation_admission_contract::encoding_shrink_transfers_held_workspace_without_additive_headroom, vector_generation_admission_contract::candidate_shrink_transfers_held_workspace_without_additive_headroom, vector_generation_admission_contract::growing_persistence_transitions_refuse_atomically_for_base_and_change_publication, vector_generation_delta_admission_contract::small_delta_refuses_growing_prior_base_candidate_before_allocation_then_retries -->
Page preparation
retains one graph body and frames each physical key and page only during insertion. Reader-held
old generations remain charged until their final holder releases them.
<!-- enforced by: crates/contextdb-engine/src/persistence/generation_admission_tests.rs::near_extent_sized_page_roots_are_streamed_and_every_disk_key_is_counted, vector_generation_admission_contract::multiple_pinned_generations_remain_charged_until_their_final_snapshot_release -->
An immutable candidate refines only its graph-page frame bound: sequential base/change page
insertion needs the larger frame, rather than their sum, and no frame for empty graph bytes.
The conservative constructor and payload bounds still apply.
<!-- enforced by: crates/contextdb-engine/src/persistence/vector_streamed_frame_tests.rs::concrete_streamed_frame_covers_exact_capacities_and_root_edges, crates/contextdb-engine/src/persistence/vector_streamed_frame_tests.rs::candidate_streamed_frame_is_sequential_max_and_empty_zero, vector_generation_admission_contract::fresh_ten_thousand_dense_vectors_publish_and_restart_under_writer_limit, vector_generation_admission_contract::tiny_limit_refuses_with_typed_details_without_partial_generation -->
Actual Redb source extents
are admitted before allocation independently of decode/build workspace; uncached pages retain
that charge until their final access guard drops.
Their pages are mapped or loaded for the active working set and can be evicted when idle; an active
query pins what it needs. When a declared `MEMORY_LIMIT` cannot admit a selected graph, idle graph
chains are reclaimed in least-recently-used order only until the complete load fits. Without a
declared memory limit, loaded graphs remain resident. <!-- enforced by: vector_lazy_restart_residency_contract::reopen_keeps_saved_partitions_dormant_then_reclaims_one_least_recently_used_route_under_pressure, vector_resource_maintenance_contract::unlimited_memory_keeps_loaded_graphs_warm_across_search_and_idle_maintenance, vector_resource_maintenance_contract::working_set_releases_other_partitions_before_the_next_selected_load --> A read session searching saved graphs keeps the graph pages it filled warm across repeated
queries when its declared memory covers every selected graph's pages: after the first wide query,
repeating it reads no further pages. Under a tighter declaration the selected graphs share the
remaining memory, and a declaration too small to hold the graphs a query needs is refused with
`MemoryBudgetExceeded` while its charge stays within the declaration. <!-- enforced by: a_wide_find_keeps_its_entry_pages_warm::repeated_wide_finds_stop_filling_pages_within_the_declared_memory, a_wide_find_keeps_its_entry_pages_warm::a_short_declaration_keeps_its_charge_and_refuses_missing_topologies --> Charged bytes are ContextDB's memory-accountant
view, not a claim about the operating system's physical mapped pages. Disk budget covers durable
vector and index state before a write is accepted. The memory target is the product declaration
`SET MEMORY_LIMIT 2G`, exactly 2147483648 bytes. <!-- enforced by: statement_effect_contract::binary_memory_declaration_accepts_quoted_and_unquoted_sizes --> Whole-process RSS, which also covers opening,
writes, queries, maintenance and replay, is separate from the charged vector/index categories.

Scalar publications keep all vector memberships and intervals while unchanged stored raw bodies
share one immutable allocation. Sharing requires bit-identical F32/SQ8/SQ4 representations,
including signed zero and quantized bounds. Body and shared-header charges follow each allocation
through moves, eviction and store removal until its final holder drops; directories remain charged
per version. `HISTORY ALL` therefore grows version metadata and writer retention with retained
version count, even when unchanged vector bodies stay shared and bounded. A finite memory ceiling
still limits admission of additional history.
<!-- enforced by: crates/contextdb-vector/src/store/shared_payload_tests.rs::retained_scalar_memberships_share_one_body_and_keep_all_snapshot_intervals, crates/contextdb-vector/src/store/shared_payload_tests.rs::hydration_shares_only_bit_identical_history_in_every_stored_representation, crates/contextdb-vector/src/store/shared_payload_tests.rs::shared_cross_partition_body_survives_source_removal_and_final_external_holder, crates/contextdb-vector/src/store/shared_payload_tests.rs::shared_allocation_header_stays_charged_until_final_holder_after_store_drop, vector_memory_ownership_reconciliation_contract::retained_journey_scalar_publications_bound_unique_payloads_and_admit_a_small_write -->

Received columns may own accounting without a store-wide accountant. Batch preparation reserves
each touched column's directory, shared-header and required copied-move body growth with its actual
funder before publication. Inserted bodies are caller-admitted beforehand; abort or refusal releases
only preparation reservations and retains that caller-admitted body charge. Publication adopts payload
charges until the final holder drops; settlement returns unused reserved bytes to their funder. Mixed
legacy columns keep their existing insertion protocol. Detached-copy hydration moves unique uncharged
loaded bodies out of shared headers without cloning; shared histories and charged or resident
sources retain their owners. Refused copy hydration releases workspace and remains retryable.
<!-- enforced by: crates/contextdb-vector/src/store/received_publication_accounting_tests.rs::received_publication_prepares_next_insert_and_releases_its_last_holder, crates/contextdb-vector/src/store/received_publication_accounting_tests.rs::received_columns_keep_distinct_funders_and_legacy_columns_in_a_mixed_batch, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_unique_unwrap_moves_the_body_and_preserves_shared_or_charged_owners, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_hydration_retains_only_new_singleton_bodies_and_releases_headers, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_header_refusal_releases_loader_body_workspace_and_retries, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_mixed_history_preserves_bits_and_shared_final_holder_charges, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_already_resident_sources_keep_representation_charge_and_funder -->

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
interruption, is checked around inventory, before and after each covered-journal cleanup retry,
and during normal post-publication cleanup. Both `ReadCancelled` and `PublicationInterrupted`
return before later partitions advance. Validation, key collection and deletion check cancellation
every 64 records and before commit. Uncommitted cleanup aborts, temporary reservations
release, earlier committed cleanup remains valid across restart, and the interrupted cycle receives
no closing stamp.
<!-- enforced by: vector_resource_maintenance_contract::refused_partition_still_closes_the_cycle_and_retires_a_sibling_partition, vector_resource_maintenance_contract::dropping_a_sampled_vector_column_does_not_abort_cycle_closing, crates/contextdb-engine/src/database/vector_journal_cleanup_cancellation_tests.rs::cancellation_after_inventory_capture_skips_all_cleanup_and_releases_reservations, crates/contextdb-engine/src/database/vector_journal_cleanup_cancellation_tests.rs::cancellation_after_committed_journal_retry_leaves_later_partitions_untouched_on_restart, crates/contextdb-engine/src/database/vector_journal_cleanup_cancellation_tests.rs::publication_interruption_at_normal_t2_is_terminal_and_preserves_the_restart_frontier, crates/contextdb-engine/src/persistence/bounded_catalog_tests.rs::journal_cleanup_scan_cancellation_aborts_validation_collection_and_deletion -->

Public and background cycles use an admitted committed catalog inventory, separate from startup
inspection. A caller-refused singleton body permits the root-pinned cursor to continue to siblings;
navigation or container refusal makes the inventory incomplete and authorizes no catalog-dependent
reclaim or retirement. Already-clean installed catalogs skip journal cleanup. Pending cleanup
admits its existing three-key reservation before materialization and retains that owner throughout.
Source pins and borrowed views end inside the commit boundary before scalar marks are applied;
sampling and marking do not copy giant partition keys. Candidate sorting admits its heap scratch
before allocation, releases partial work on refusal and retries from a fresh inventory.
The cursor continues when a refused giant body follows a healthy catalog and can still discover a
later durable-only catalog. Closing route-slot refusal preserves publication and skips removal;
wide typed route metadata is admitted before a cold catalog lookup.
<!-- enforced by: crates/contextdb-redb/src/tree_store/bounded_bytes.rs::cursor_continues_after_refused_singleton_in_every_sibling_position, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::cursor_navigation_refusal_is_terminal_and_has_no_backend_read, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_incomplete_container_sample_preserves_serving_and_closes, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_sampled_route_slot_refusal_drops_partial_work_and_closes, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_candidate_slot_refusal_drops_partial_work_and_closes, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_closing_route_slot_refusal_preserves_publication_and_skips_removal, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_candidate_sort_heap_refusal_releases_and_retries_stably, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_cold_covered_cleanup_refusal_keeps_serving_and_closes, crates/contextdb-vector/tests/borrowed_route_metadata_adapter.rs::giant_source_metadata_and_marking_allocate_no_giant_key_copy_or_encoding -->

Frontier metadata can reject unrelated extents through charged bounded windows; selected or
possibly matching metadata admits the complete physical extent before backend read or decoding,
using `prepare_vector_generation`. Generation-body admission starts after construction reservation
succeeds. Cleanup-only journal pages use `truncate_vector_partition_journal`; catalog reclamation
uses the preparation operation. Unknown invalid catalogs do not quarantine unrelated routes;
miskeyed authenticated catalogs quarantine their canonical route, and raised limits retry fresh.
<!-- enforced by: crates/contextdb-engine/src/database/vector_frontier_metadata_admission_tests.rs::cold_oversized_catalog_extent_refuses_during_preparation_before_decode, crates/contextdb-engine/src/database/vector_frontier_metadata_admission_tests.rs::cold_oversized_journal_extent_refuses_during_frontier_count_before_decode, crates/contextdb-engine/src/database/vector_frontier_metadata_admission_tests.rs::cold_oversized_membership_extent_refuses_before_fallback_count_decode, crates/contextdb-engine/src/database/vector_quarantine_inventory_admission_tests.rs::missing_catalog_inventory_admits_cold_oversized_source_after_construction, crates/contextdb-engine/src/database/vector_quarantine_inventory_admission_tests.rs::corrupt_catalog_inventory_admits_cold_oversized_source_after_construction, crates/contextdb-engine/src/database/vector_source_page_admission.rs::cleanup_page_admissions_keep_typed_attribution_and_release_their_source_owner, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_small_unknown_invalid_catalog_does_not_quarantine, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_miskey_before_valid_catalog_quarantines_canonical_route, crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs::public_cycle_possible_match_blocks_until_raised_limit_retries_fresh -->

A covered-journal refusal skips only that route's build and keeps its serving/catalog state while a
sibling publishes. An incomplete `INDEXED` frontier refuses; `AUTO` uses that partition's complete
exact fallback and preserves the sibling's indexed route. Historical raw-only routes with uncovered
deletes remain candidates and physically clean their journals before retirement.
<!-- enforced by: crates/contextdb-engine/src/database/vector_retry_frontier_integrity_tests.rs::cleanup_refusal_skips_its_build_while_sibling_and_complete_query_frontiers_progress, vector_delete_only_journal_progress_contract::delete_only_unpinned_final_route_physically_cleans_its_journal_before_retirement, vector_delete_only_journal_progress_contract::delete_only_final_route_truncates_covered_history_before_snapshot_release_can_retire_it -->

**Version cleanup and a held read snapshot.** Every in-flight statement registers its own read snapshot for the call's duration, and a caller that needs to reuse a `SnapshotId` ACROSS separate calls (on a table declaring `HISTORY CURRENT ONLY`) registers it explicitly via `Database::pin_snapshot`, holding the returned guard for as long as the snapshot is still wanted. Each open store funds and charges a fixed pool of `ReadLimits::SHIPPED_SNAPSHOT_PIN_SLOTS` (64) pin slots; `pin_snapshot` returns a typed memory-budget refusal when all slots are in use, and dropping a pin returns its slot. A version-cleanup pass samples every currently-registered snapshot (not merely the oldest) plus the committed watermark, atomically, once at the start of the pass, and defers any superseded version still visible to ANY of those registered snapshots to a later cycle — a version created between two registered snapshots and superseded after both is protected by the higher one even though the lower one alone would not see it. Protection begins when `pin_snapshot` RETURNS: a pin requested while a removal pass is already mid-flight, for a snapshot at or before that pass's sampled watermark, waits for the pass to finish first (bounded by one pass duration) before registering, so the pin can never return with a false promise of protection the SAME in-flight pass is still free to violate — the next cleanup cycle honors it instead. A pin for a snapshot strictly after the pass's watermark registers immediately: nothing that pass can prune was ever visible to it. Versions a pass already reclaimed before the pin is requested cannot come back; that boundary is unchanged.

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

Retention selects due versions from RETAIN tables at one commit frontier, borrowing source
payloads and FK references instead of cloning table histories. The shared memory accountant
admits candidate identities and expiry mutations before allocation; reclamation uses the same
borrowed selection and preserves complete-lifetime checks without copying unrelated payloads.
An empty or not-due retained table reserves no expiry workspace as unrelated history grows.
Reclamation admits selected vector directory identities and edge deletion keys, without loading
vector bodies or copying edge properties merely to remove them. Reader-deferred edge identities
retain their own memory charge between cycles; carry-only retries admit copied identities and
durable encoding before allocation, including after restart. Position-map and surviving index-key
rebuilds for selected tables, generation catalogs/reader chains and affected-LSN sibling buffers
are admitted before durable reclamation commits. Resident vector removal visits selected
indexes and updates whole-row reverse maps in place.
Selected-row identity repair admits the borrowed durable encoding before decoding bodies omitted
from the resident directory, and holds that charge through identity publication.

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

**Those three tables' declared policy, plus `work_inputs`' `RETAIN 7 DAYS`, is engine-owned, not operator policy.** All four are built-in work-fabric tables (see the [work-fabric skill](../skills/work-fabric/SKILL.md)) whose own bookkeeping depends on staying at the shape declared in their own `CREATE TABLE` text: `work_inputs`' retention window is what keeps ledger-carried input copies bounded, and the three currency tables' `HISTORY CURRENT ONLY` plus their `SYNC CONFLICT KEEP LATEST` (or `SYNC OFF`) is what makes version-cleanup safe to reclaim their superseded rows at all. A locally-typed `ALTER TABLE` refuses any `RETAIN` / `HISTORY` / `SYNC CONFLICT` / `SYNC ...` / `SYNC SAFE` value that mismatches one of these four tables' own canonical declaration, with a message naming the table as engine-owned infrastructure — including `SET SYNC ...` (`work_node_contacts`' own `SYNC OFF` declaration is guarded on this axis exactly like the others); silence, or an explicit restate of the table's already-declared value on that axis, still passes, the same mismatch-only contract the rest of this paragraph describes for the other doors. A locally-typed `CREATE TABLE` of one of these four names is guarded too: it refuses unless the declared shape structurally matches the owning installer's own `CREATE TABLE` text — every column (name, type, nullability, primary key, uniqueness, default, references, `EXPIRES`, `IMMUTABLE`) AND every table-level option (a table-level `IMMUTABLE`, `STATE_MACHINE`, `DAG`, or `PROPAGATE` clause, none of which any of the nine reserved names' own DDL declares) — and refuses an explicit non-canonical policy clause the same way the ALTER door does — a table outside the nine reserved names remains entirely unrestricted (silence on policy still passes). A locally-typed `ALTER TABLE ADD COLUMN` / `DROP COLUMN` / `RENAME COLUMN` naming one of the nine reserved names is refused outright too, before any other ALTER validation runs — column shape on these nine is exactly as fixed against a column-level ALTER as it is against `SET RETAIN` / `SET HISTORY` / `SET SYNC CONFLICT` / `SET SYNC ...`. Existence is checked first: on a store where the table was never installed, this ALTER reports "table not found," not the engine-owned refusal — the same existence-before-ownership ordering every other clause in this door already follows, so an absent table and a governed one are never confused. The arriving-sync-DDL mirror of both guards reconstructs the wire columns and table-level constraint text into real `CREATE TABLE` SQL and runs it through the same parser and the same shape judgment, so a peer cannot smuggle a table-level option (or a per-column attribute) past the wire path that the local path already refuses. The shape guard actually spans **nine** reserved names, not four: the same four plus the five hub-refereed work-ledger tables (`work_jobs`, `work_claims`, `work_results`, `work_failures`, `work_cancellations`), whose real arbitration is a hardcoded `keep_first` override applied at every sync chokepoint rather than a fully declared policy shape. Those five get a narrower, SYNC-CONFLICT-only counterpart of the policy guard: a `CREATE TABLE` or `ALTER TABLE`, locally or over an arriving sync DDL, that declares a SYNC CONFLICT value other than `keep_first` refuses; silence, or an explicit restate of `keep_first`, still passes. `SHOW SYNC_CONFLICT_POLICY` renders every one of these seven work-ledger tables, plus `peer_directory` (also engine-owned, via its own declared `SYNC CONFLICT KEEP LATEST`), actually present in the store as `{table}={word} (engine-owned)`, distinct from an operator-declared row. `work_node_contacts` declares no conflict policy at all, so it never renders in `SHOW SYNC_CONFLICT_POLICY`, in any form. An arriving sync DDL naming one of the original four is held to the identical shape by THREE guards because a peer's own DDL always carries the table's FULL current shape, whether or not a given axis actually changed: an EXPLICIT differing value — spelled as an `AlterTable`, as a `CreateTable` adopting an already-existing table, or as a fresh `CreateTable` of a reserved name (guarding against DROP + CREATE circumvention) — is refused atomically for the whole batch before any of it is written; an axis the arriving DDL is simply SILENT on PRESERVES the table's current declared value instead of being read as an implicit clear, which is what lets a half-healed peer's own in-progress multi-step reconcile interoperate. This is not a workaround for a missing knob: an installer (or a peer) that restates the canonical declaration needs no exception, because a healing call always restates that same declared shape verbatim, and a verbatim restatement always applies — locally, and over sync.

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

This release emits and accepts only protocol 7; envelopes using any other protocol version are
refused. The current wire retains row arrival ordering, the serving-store incarnation in
pull responses, the purge instruction lane, schema provenance, and the structured
authoritative-hub purge refusal.
<!-- enforced by: protocol_version_bump_tests::current_wire_arrival_and_source_fields_round_trip, protocol_version_bump_tests::current_wire_purge_instruction_and_typed_authority_error_round_trip, schema_provenance_wire_contract::nonempty_schema_provenance_round_trips_and_validates -->

The wire version bumps when a change alters sync bytes or sync semantics. SQL, storage, CLI, or maintenance work that leaves sync unchanged does not bump the protocol. A downstream consumer that must adapt to a new wire does that in its own release; the bump is not a contextdb defect.
<!-- enforced by: protocol_future_bump_rule_tests::architecture_requires_a_protocol_version_bump_for_every_envelope_shape_change -->

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

The keyless fallback — a column named `id` with no declared key — is usable
only when `id` has a covering index. Sync apply otherwise refuses with
`exact sync key probe on keyless(id) has no covering index` (the table name
fills the first slot: a table `events` would read `events(id)`).
<!-- enforced by: tests/integration/composite_primary_key_tests.rs::c5b_guard_a_keyless_id_table_replicates_unchanged, keyless_sync_eligibility_tests::keyless_table_with_id_column_but_no_covering_index_still_refuses_push -->

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

A synced `ALTER TABLE` carries the operation its author ran, not just the resulting column list.
`DROP COLUMN` and `RENAME COLUMN` travel as explicit, signed markers inside the authenticated DDL,
and a receiving machine removes or renames a column only when such a marker names it: a dropped
column leaves with its stored values, vectors and index, and a renamed one keeps its values,
vectors and index under the new name, on the author, on every receiver, and after a restart. A
column that an arriving `ALTER TABLE` simply does not mention is kept, with its data, whether or
not the statement restates the primary key. A receiver never infers a drop from an omitted column
or a rename from two columns that happen to share a type. A node that relays a received `ALTER`
logs the same markers, so a machine further down the line applies the same operation.
<!-- enforced by: sql_surface_other::sync_partial_vector_shape_alter_does_not_drop_omitted_side_columns, sql_surface_other::sync_partial_alter_restating_primary_key_still_preserves_omitted_side_column, sql_surface_other::sync_partial_alter_omitting_a_matching_type_column_is_not_treated_as_a_rename, sql_surface_other::sync_explicit_drop_column_removes_data_and_names_the_drop_as_the_authors_operation, sql_surface_other::sync_explicit_vector_rename_moves_vectors_and_names_the_rename_as_the_authors_operation, declared_memory_limit_bounds_a_writer::an_authors_signature_travels_two_hops_a_restart_and_a_third_peer -->

An authored `SET MAINTAIN COUNT` declaration travels through authenticated push to the hub.
The hub installs the declaration and answers a matching exact `COUNT(*)`. A peer
without that schema capability holds back the affected table while other tables keep syncing;
once the peer supports it, the held declaration can be pushed.
<!-- enforced by: authenticated_sync_contract_tests::a_maintained_count_declaration_crosses_sync_or_holds_back_its_table -->

Received schema history replays one source transaction at a time, in author order. A table or
trigger created and later dropped still participates in the intervening transactions; the
receiver does not reduce the history to its final schema before applying the rows.
<!-- enforced by: tests/acceptance/trigger.rs::t3_sync_trigger_table_create_drop_history_replays_chronologically, tests/acceptance/trigger.rs::t3_sync_trigger_vector_create_data_drop_history_replays_without_callbacks, tests/acceptance/trigger.rs::t3_sync_trigger_ddl_replay_respects_sender_lsn_order -->

An arriving `CREATE TABLE` declaration whose columns differ from the existing table is reported
as a schema mismatch instead of silently replacing that table.
<!-- enforced by: tests/acceptance/schema_lifecycle.rs::f23_schema_mismatch_between_edge_and_server_is_detected, tests/integration/ddl_sync_tests.rs::ds10_schema_divergence_detected -->

A repeated received schema unit with the same recorded migration identity returns its original
apply result without changing the installed schema or rows, including after the receiver reopens.
The engine's `received_schema_atomic_retry_tests::identical_received_marker_vector_retry_returns_its_original_observation_without_mutation`
asserts the returned result and stored state are equal before and after the retry. An authenticated
push retry after a lost reply also leaves the receiver's DDL history unchanged.
<!-- enforced by: schema_migration_chunked_transport::oversized_authenticated_schema_stays_invisible_until_one_commit_and_retry -->

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
- **Serve** on the holder: `serve_on(&IrohServer) -> Result<()>` registers the serving
  handler. It checks the requesting peer's authorization against the ledger
  *before* any payload bytes move — the check is the gate, not an
  after-the-fact audit. Each request obtains its own child of the store's declared reader memory.
  Hosts supplying their existing provider use `serve_on_with_read_budget_provider`. Directory registration failures propagate.
- **Fetch** on the consumer: `resolve_blob_ref(&BlobHash, holder_ticket, sink)`
  is async, fetches node-to-node, and is hash-verified — only bytes that hash
  to the requested `BlobHash` ever reach the sink. Errors are a matchable
  `ResolveError`: `Unentitled`, `PolicyForbidden`, `HashMismatch`,
  `HolderUnreachable`, `LocalStoreUnavailable`, `BlobNotFound`,
  `TransferAborted`, `SinkWrite`, `FetchTimedOut`, `LedgerRead(Error)`.
  The default creates an operation child internally; hosts supplying their actual
  READ owner use `resolve_blob_ref_with_read_memory`. Ledger source refusals retain
  their typed core error, including when returned by the holder on the same
  blob stream before Bao payload data. A malformed or oversized control frame
  returns a typed protocol refusal.
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
  returns `BlobNotFound`, even from an otherwise-entitled caller. The captured ledger root stays funded by the operation child throughout
  the sweep. `spawn_reclaim_driver(grace_ms)` obtains a child of the declared
  reader parent for each cycle. Explicit host variants are
  `reclaim_unreferenced_with_read_memory` and
  `spawn_reclaim_driver_with_read_budget_provider`.
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
blob_store.serve_on_with_read_budget_provider(&iroh_server, read_budgets.clone())?;

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
    .resolve_blob_ref_with_read_memory(read_memory.clone(), &blob_hash, &holder_ticket, &mut sink)
    .await?;
```

---

## Store Format and Recovery

The on-disk store carries a format-version marker (current: `1.0.0`). Opening a
store with an incompatible marker, schema layout, or versioned-key shape fails
with `LegacyVectorStoreDetected`. A store written by an earlier build is not
read. Move it aside and create the store again.

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
written by an earlier build, or corrupt/truncated with the underlying reason) — it never
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

Disk enforcement happens in the engine write paths before `INSERT`, `UPDATE`, and sync-apply work begins. The storage allocator also reserves each projected physical file growth before allocating it, so pages retained for a live file reader and newly allocated writer pages cannot carry the file past the configured limit. Once the on-disk file is at or above the configured limit, or an allocator growth would cross it, further file-backed writes fail with `DiskBudgetExceeded` without poisoning the writer. In-memory databases accept the SQL but ignore disk budgeting because there is no backing file to measure.
