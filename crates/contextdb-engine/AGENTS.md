# contextdb-engine — Agent Rules

Purpose: compose the stores into atomic `Database` operations, persist them, serve bounded reads,
run declared maintenance, and sync. Read the [root rules](../../AGENTS.md) first.

## Owns / must not own

Owns SQL execution, transaction orchestration, durable publication, reader registration, erasure,
callbacks, sync semantics and work-ledger state. Grammar belongs to parser, planning to planner,
store data structures to their crates, and refusal vocabulary to core. Never add a shadow store,
a second transport, third read route, second read opener or consumer-specific execution kernel.

## Seams

Above: CLI, server and Rust embedders. Below: core, tx, relational, graph, vector, parser, planner
and the redb fork. Public entries are `Database::open`, `open_with_options`, `open_with_plugin`,
`execute`, `begin`/`commit`/`rollback`, and `ReadSession::open`/`open_owner_only`/`charged_bytes`.
`open_memory` is test-only file-backed scratch, not an embedding API or in-memory product mode.
`DatabasePlugin` is a lifecycle seam; callbacks prepare transactional writes before validation,
while external effects use post-commit sinks. Both read routes use `executor/bounded.rs`.
Writer sources pin published roots and merge staged overlays; they must not hydrate history at open.
Detailed mechanisms and claim bindings: [architecture](../../docs/architecture.md).

Persist time through `contextdb_core::Wallclock::now()`. Tests use the thread-local
`Wallclock::test_clock_guard`; drive background work synchronously. Never raise clock/sleep ratchets.
Admit before allocation and keep charges until final physical release, including cursor-held pages.
Test observations extend existing seams; they must not create a production execution path.

## Invariants and their guards

Paths below are relative to this crate unless they begin with `crates/` or `../../`.

| Invariant | Guard (file → test) |
|---|---|
| Writable open does not load history | `tests/engine_seams/writable_open_never_touches_history_families.rs::writable_open_never_touches_history_families` |
| Published reader roots survive writes and cleanup | `tests/engine_seams/writer_reader_root_survives_commit_eviction_and_cleanup.rs::writer_reader_root_survives_commit_eviction_and_cleanup` |
| Doubling durable data keeps a fixed resident envelope | `tests/engine_seams/writer_reopen_and_doubling_keep_a_fixed_resident_envelope.rs::writer_reopen_and_doubling_keep_a_fixed_resident_envelope` |
| Crash recovery publishes one complete commit | `tests/engine_seams/crash_cutpoints_recover_one_complete_row_index_vector_log_commit.rs::crash_cutpoints_recover_one_complete_row_index_vector_log_commit` |
| Both read routes apply access gates | `tests/bounded/bounded_read_refuses_what_the_gate_denies.rs::a_row_the_principals_grants_hide_is_refused_by_both_doors` |
| Held pages remain charged after cache eviction | `src/persistence/search_page_reuse_tests.rs::a_proof_page_a_statement_still_holds_stays_charged_after_the_cache_evicts_it` |
| Session cache share is local and resets at reopen | `tests/read/read_cache_share_statements.rs::a_read_cache_share_is_scoped_to_its_session_and_not_persisted` |
| Compaction declarations persist and invalid changes preserve state | `tests/engine_seams/storage_compaction_is_declarable_and_unmeasured_fragmentation_reads_unknown.rs::storage_compaction_declarations_show_their_source_and_survive_reopen` |
| AUTO is exact and INDEXED is refused | `tests/vector/a_search_is_exact_with_no_index_built.rs::an_auto_find_answers_exactly_after_commit_with_no_maintenance`, `::indexed_is_refused_at_every_door` |
| Partial synced ALTER preserves omitted columns | `tests/sql_surface/sql_surface_other.rs::sync_partial_alter_restating_primary_key_still_preserves_omitted_side_column` |
| Received schema failure publishes no prefix or receipt | `tests/sync/a_received_schema_transaction_never_publishes_a_prefix.rs::a_received_schema_transaction_never_publishes_a_prefix` |
| Parent purge erases retained declared children | `tests/custody/custody_purge_and_discard_contract.rs::a_purge_erases_every_retained_life_of_a_declared_child` |
| Retention preparation is admitted before allocation | `src/database/bounded_retention_preparation_tests.rs::impossible_expiry_limit_preserves_old_state_with_typed_refusal` |
| Scalar history shares vector bodies without losing memberships | `tests/vector/vector_memory_ownership_reconciliation_contract.rs::retained_journey_scalar_publications_bound_unique_payloads_and_admit_a_small_write` |
| Clock/sleep and persisted-column ratchets remain closed | `crates/contextdb-core/tests/test_estate_audit.rs::test_estate_audit_no_new_sleeps_or_raw_clock_reads`, `crates/contextdb-core/tests/timestamp_audit.rs::timestamp_audit_no_new_txid_shaped_columns` |

## Where a change lives

| Change | Where it lands |
|---|---|
| Scalar function | `executor.rs`: update both `eval_function` and `is_known_scalar_function`; test ORDER BY too. |
| Read opening, routing, channel | `read_session.rs`, `direct_file_reader.rs`, `owner_read/`, `local_transport/`. |
| Bounded execution, cursor pages, cache share | `executor/bounded.rs`, `read_session/`, `read_image_memory.rs`; physical page charges in persistence. |
| DDL, synced author markers, callbacks | `database.rs`, `database/`; durable installation in `persistence.rs`. Never infer drop/rename from omission. |
| Root witnesses, reader holds, compaction, erasure | `persistence/`: `history_root_witness.rs`, `reader_registration.rs`, `reader_presence.rs`, `purge_publication.rs`, `erasure_frontiers.rs`, `writer_file.rs`. |
| Writer reads/history/overlay | `persistence/writer_*.rs`, `database/writer_read.rs`; store traits stay in relational, graph and tx. |
| Counts and content totals | `maintained_counts.rs`, `persistence/content_size.rs`, `content_identity.rs`, executor; do not enumerate history during inspection. |
| Vector declarations and lifecycle | `database.rs`, `database/vector_maintenance_inventory.rs`, `database/vector_source_page_admission.rs`, persistence; search belongs to vector. |
| Custody, purge, discard | `custody/`, `database/custody_store.rs`, `database/discard.rs`, `database/purge_predicate.rs`. |
| Sync, network, ledger | `sync_client.rs`, `sync_server.rs`, `sync_types.rs`, `protocol.rs`, `transport/iroh.rs`, `work_ledger.rs`; server re-exports rather than mirrors. |
| Memory accounting / CLI documents | `memory_accounting.rs`; `cli_render.rs`, `metadata_page.rs`; CLI owns its rendering. |

## Fast tests

`cargo test -p contextdb-engine --lib`

`cargo test -p contextdb-engine --features test-seams --test sql_surface`

For sync: `cargo test -p contextdb-engine --features test-seams,iroh --test sync --test sync_schema_transactions`.
