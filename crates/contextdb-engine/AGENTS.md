# contextdb-engine — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). The CLI read contract this crate
implements is [`docs/cli.md`](../../docs/cli.md); sync and storage design is in
[`docs/architecture.md`](../../docs/architecture.md).

**Purpose:** `Database` — the embeddable engine that runs SQL over one MVCC store composed of
relational, graph and vector state, persists it, serves bounded reads, maintains it in the
background, and (feature `sync-orchestration`) syncs it.

## Owns / must not own

| Area | Owning files |
|---|---|
| Transactions, commit, `execute`, DDL, triggers, events | `database.rs`, `database/` (`gate.rs` schema-publication gate, `event_bus.rs`, `cron.rs`, `trigger.rs`) — `TxId`/`SnapshotId` come from core, `TransactionManager` from `contextdb-tx` |
| Store composition | `composite_store.rs` (one `WriteSet` across `RelationalStore`, `GraphStore`, `VectorStore`), `persistent_store.rs`, `plugin.rs` (`DatabasePlugin` lifecycle hooks — not a storage seam) |
| Persistence | `persistence.rs` (redb file, companion record and lock, claim window, reader holds and breadcrumbs, scoped durable cleanup), `persistence/`, `metadata_page.rs` |
| Execution | `executor.rs` (every `PhysicalPlan` arm, `eval_function`, `is_known_scalar_function`, `validate_sort_key`), `executor/bounded.rs` (the one bounded kernel and its budgets) |
| Bounded reads | `read_session.rs` (one `ReadSession`, two routes), `direct_file_reader.rs` (file route: sealed hydrated snapshot), `owner_read/` (`admission`, `client`, `service`), `local_transport/` (addressing, auth, framing, deadlines) |
| Maintenance | `database.rs`: `spawn_maintenance`, `run_pruning_cycle(_checked)`, `run_maintenance_cycle`, `MaintenancePolicy`; `vector_observations.rs` |
| Vector lifecycle | Declaration, validation, scheduling and generation/partition catalog in `database.rs` and `persistence.rs`; partition state and consolidation run in `contextdb-vector`, the graph algorithm in `contextdb-hnsw` |
| Sync | `sync_client.rs`, `sync_server.rs`, `sync_types.rs`, `protocol.rs`, `subjects.rs`, `transport/`, `work_ledger.rs`, `transfer_receipts.rs`, `peer_directory.rs` |
| Custody and erasure | `custody/` (policy, preparation, canonical BLAKE3 row encoding, delivery, records, inspection, incarnation), `database/discard.rs`, `database/purge_predicate.rs`, purge kernel in `database.rs` |

Must not own: grammar (parser), plan rules (planner), row/adjacency/vector data structures
(relational, graph, vector), the KV engine (`contextdb-redb`), refusal vocabulary
(`contextdb-core::read_contract`). `contextdb-server` re-exports sync; its transport and staging
files are audit mirrors that stay byte-identical to the engine's, never independent code.
**Forbidden without exception:** a shadow store beside the real one; a second transport beside
`local_transport`; a third read route or second way to open a store for reading; a duplicated
refusal type; consumer-specific execution semantics outside `executor/bounded.rs`. `:memory:` has
no file, companion, owner channel or cross-process route. `open_owner_only` asks the channel or
fails — it never falls back to the file.

## Seams

Below: core, tx, relational, graph, vector, parser, planner, redb. Entry points: `Database::open`,
`open_with_options`, `open_memory`, `open_with_plugin`, `execute`, `begin`/`commit`/`rollback`,
`ReadSession::open`/`open_owner_only`. Above: `contextdb-server` (re-exports `sync_client`,
`sync_server`, `protocol`, `subjects`, `transport`, `transfer_receipts`), `contextdb-cli`,
embedders.

## Clock seam

Every persisted timestamp goes through `contextdb_core::Wallclock::now()` — never
`SystemTime::now()` or inline epoch math. Mock with
`let _g = Wallclock::test_clock_guard(|| 1_700_000_000_000);` (the guard restores on drop, even on
panic; a trailing `reset_test_clock()` leaks on panic). The override is thread-local:
engine-spawned threads see the real clock, so drive background work synchronously
(`run_pruning_cycle_checked`, `run_maintenance_cycle`). Guards:
`crates/contextdb-core/tests/test_estate_audit.rs` (per-file sleep and raw-clock ratchet — lower a
count, never raise it) and `timestamp_audit.rs` beside it.

## Invariants and their guards

| Invariant | Guard |
|---|---|
| A commit is atomic across relational, graph and vector; rollback undoes all three | `tests/engine_tests.rs::test_cross_subsystem_atomic_commit`, `::test_rollback_across_all_subsystems` |
| A snapshot never sees later commits, for rows and vectors | `tests/engine_tests.rs::test_mvcc_snapshot_isolation`, `::test_vector_snapshot_isolation` |
| A local `CREATE TABLE` never replaces an existing table | `tests/a_local_create_table_never_replaces_an_existing_table.rs::a_refused_recreate_leaves_the_existing_columns_and_rows_untouched` |
| A corrupt store is refused at open, never half-served | `tests/corrupt_store_refused_at_open.rs::a_truncated_store_is_refused_when_a_session_opens_it` |
| Only a legacy root is a migration source | `tests/migration_source_determines_publication.rs::a_current_format_root_is_not_a_migration_source_at_all` |
| Both routes return equivalent results, and a failed owner never falls back to the file mid-session | `tests/read_session_route_contract.rs::file_and_owner_routes_select_once_and_return_canonical_equivalent_results`, `::owner_disconnect_after_partial_reply_never_falls_back_within_the_session` |
| The owner-only door never touches the file | `tests/owner_only_session_never_touches_the_file.rs::an_owner_only_open_with_no_owner_leaves_the_committed_file_alone` |
| Owner admission holds N and refuses N+1 without queueing | `tests/owner_read_admission_contract.rs::default_and_raised_capacity_hold_exactly_n_and_refuse_n_plus_one_without_a_queue` |
| An owner answer is private until terminal success | `tests/owner_read_client_contract.rs::multiple_chunks_remain_private_until_terminal_success` |
| The reported plan trace is what the executor did | `tests/bounded_read_plan_trace_matches_the_executor.rs::a_bounded_read_describes_its_plan_the_way_the_executor_does` |
| Access gates refuse the same rows on both routes | `tests/bounded_read_refuses_what_the_gate_denies.rs::a_row_the_principals_grants_hide_is_refused_by_both_doors` |
| No maintenance thread without eligible work; close joins it | `src/database.rs::currency_version_compaction_tests::non_eligible_database_never_spawns_a_maintenance_thread`, `::close_joins_the_maintenance_thread` |
| A checked prune that fails to persist leaves memory unchanged | `src/database.rs::retention_prune_persistence_tests::checked_prune_reports_persistence_failure_without_mutating_memory` |
| Caller-driven vector indexes change only through `run_maintenance_cycle` | `tests/vector_maintained_lifecycle_contract.rs::caller_driven_vector_indexes_are_built_and_maintained_only_by_maintenance` |
| Partition declarations survive reopen with canonical defaults | `tests/vector_partition_declaration_contract.rs::partitioned_vector_schema_survives_reopen_with_canonical_defaults` |
| Partition scope never weakens context or principal filters | `tests/vector_partition_query_contract.rs::partition_scope_does_not_weaken_context_scope_or_principal_filters` |
| Server sync and transport sources are byte-exact engine mirrors | `tests/sync_source_mirror_tests.rs::server_sync_sources_are_exact_engine_audit_mirrors` |
| A failed synced commit publishes neither row nor receipt | `tests/sync_receipt_atomicity_tests.rs::failed_synced_commit_publishes_neither_row_nor_authenticated_receipt` |
| Engine-owned ledger tables refuse local conflict-policy declarations | `tests/show_sync_conflict_policy_tests.rs::engine_owned_work_ledger_arbitration_refuses_a_local_declaration_attempt` |
| `PURGE` runs only standalone; `DISCARD` is refused on a hub-bound node | `src/database/authoritative_purge_public_contract_tests.rs::public_purge_requires_standalone_execution_without_invalidating_transactions`, `tests/custody_purge_and_discard_contract.rs::edge_discard_follows_declared_modes_and_one_local_transaction_boundary` |
| An incoming sync row cannot revive a row its tombstone deleted | unguarded — no test drives the tombstone check on sync apply directly |
| Every function `eval_function` accepts is also in the ORDER BY allowlist | unguarded — the two lists are hand-synced |

## Where a change lives

| Change | Where it lands |
|---|---|
| New scalar function | One arm in `eval_function` plus `is_known_scalar_function`, plus tests. |
| New statement execution | Its `PhysicalPlan` arm in `executor.rs`; bounded reads also through `executor/bounded.rs`. |
| Read route, admission, owner channel | `read_session.rs`, `owner_read/`, `local_transport/` — extend, never add a parallel path. |
| Durable format, cleanup atomicity | `persistence.rs`; keep row, change-log, vector and adjacency cleanup in one write transaction. |
| Vector declaration semantics | `database.rs` validation; syntax in the parser, execution in `contextdb-vector`. |
| Wire vocabulary | `protocol.rs`, `subjects.rs`; rerun the server transport-boundary tests and `sync_source_mirror_tests.rs`. |
| Custody, purge, discard | `custody/`, `database/discard.rs`, `database/purge_predicate.rs`, and the standalone purge kernel itself (selection, validation, outcome/`PurgeReportShape` reporting) in `database.rs` — a distinct-outcome change (e.g. zero rows selected) lands there. Only bind and outcome-fetch add wire request kinds; custody inspection runs through the bounded kernel and its budgets; no unrelated persistence opening carries a custody fault seam. |

## Fast test

```bash
cargo test -p contextdb-engine --lib                  # src/ unit tests
cargo test -p contextdb-engine --test <file_stem>     # the contract file you touched
```
