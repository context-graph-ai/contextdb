# contextdb-vector — Agent Rules

Purpose: serve partitioned exact nearest-neighbour search over each `(table, column)` under one
MVCC snapshot. Read the [root rules](../../AGENTS.md) first.

## Owns / must not own

Owns `VectorStore`, partition membership, policy resolution, exact top-k merge, quantized payloads,
and their memory ownership. `ivf/` retains the dormant clustered-index implementation; this release
neither constructs nor serves it. Engine owns durable storage, generation admission, maintenance
scheduling, row authorization and sync; parser owns SQL syntax. Never retain an all-row directory
or add a second index implementation. Public `VectorEntry` and durable codecs keep owned Vecs;
private shared bodies retain their original funder's charge through final physical release.

## Seams

Above: engine supplies root-pinned `DormantRawVectorLoader` and raw-membership sources and owns
maintenance scheduling. Below: core `VectorExecutor`/errors and tx snapshots/`WriteSetApplicator`.
`mem.rs::MemVectorExecutor` implements the executor. `store/membership.rs` owns durable cursor
traits. Test observations use existing `test_seam.rs` hooks, never a production search route.
Detailed memory and declaration contracts: [architecture](../../docs/architecture.md).

## Invariants and their guards

| Invariant | Guard (file → test) |
|---|---|
| Search on one `(table, column)` never returns another column's vectors | `tests/vector_tests.rs` → `search_on_ref_a_returns_only_ref_a_vectors_under_10000_iteration_apply_storm` |
| Equal scores order by ascending row id on the exact path (the indexed route is switched off in this release) | `tests/vector_tests.rs` → `public_search_ties_order_by_row_id_over_a_large_population` |
| A snapshot sees exactly the vectors committed at it | `tests/vector_tests.rs` → `mvcc_snapshot_isolation`; `src/store/transaction_visibility_tests.rs` |
| Concurrent updates and deletes leave one live vector and no deleted rows in results | `tests/vector_tests.rs` → `concurrent_same_ref_updates_leave_single_live_vector_after_commit`, `same_ref_concurrent_deletes_leave_no_deleted_rows_in_results` |
| A wrong dimension is a typed error for the named index | `tests/vector_tests.rs` → `dimension_mismatch_typed_error_with_positive_control` |
| Retention pruning in selected columns leaves an unrelated column's rows and answers intact | `tests/vector_tests.rs` → `selected_index_row_pruning_preserves_unrelated_index_rows_and_answers` |
| Every search, index and consolidation threshold comes from the declared column policy resolved in one place (`VectorIndexLayout::resolve_policy` / `effective_auto_index_at` in `store.rs`); an undeclared value falls back to a documented default there, but a declared value is never silently overridden by a constant elsewhere | `crates/contextdb-engine/tests/vector/vector_policy_resolver_contract.rs` → `declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries` |
| Scalar publications preserve every MVCC membership and interval while unchanged raw bodies share one allocation, charged through its final holder | `src/store/shared_payload_tests.rs::retained_scalar_memberships_share_one_body_and_keep_all_snapshot_intervals`, `src/store/shared_payload_tests.rs::shared_cross_partition_body_survives_source_removal_and_final_external_holder`, `src/store/shared_payload_tests.rs::shared_allocation_header_stays_charged_until_final_holder_after_store_drop` |
| Received columns prepare directory, shared-header and required copied-move body growth with their own funder even without a store-wide accountant; inserted bodies are caller-admitted beforehand, abort or refusal releases only preparation reservations, and published payload charges last until the final holder drops | `src/store/received_publication_accounting_tests.rs::received_publication_prepares_next_insert_and_releases_its_last_holder`, `src/store/received_publication_accounting_tests.rs::received_columns_keep_distinct_funders_and_legacy_columns_in_a_mixed_batch` |
| Detached-copy hydration moves unique uncharged bodies out of shared headers; shared histories and charged or resident sources keep their representation and final-holder charge | `src/store/detached_copy_payload_tests.rs::detached_copy_unique_unwrap_moves_the_body_and_preserves_shared_or_charged_owners`, `src/store/detached_copy_payload_tests.rs::detached_copy_hydration_retains_only_new_singleton_bodies_and_releases_headers`, `src/store/detached_copy_payload_tests.rs::detached_copy_header_refusal_releases_loader_body_workspace_and_retries`, `src/store/detached_copy_payload_tests.rs::detached_copy_mixed_history_preserves_bits_and_shared_final_holder_charges`, `src/store/detached_copy_payload_tests.rs::detached_copy_already_resident_sources_keep_representation_charge_and_funder` |
| Resident generation selection copies only frontier-live entries after the optional LSN boundary, preserving representations and history; inconsistent directory metadata refuses before body materialization | `src/store/generation_selection_tests.rs::selected_generation_materialization_preserves_all_representations_and_snapshot_intervals`, `src/store/generation_selection_tests.rs::selected_generation_refuses_inconsistent_metadata_before_body_materialization` |
| Borrowed source metadata and scalar marking copy no giant partition key; stale source identities cannot mark a replacement column or partition | `tests/borrowed_route_metadata_adapter.rs::giant_source_metadata_and_marking_allocate_no_giant_key_copy_or_encoding`, `tests/borrowed_route_metadata_adapter.rs::replacement_column_rejects_the_old_state_without_marking_the_new_source`, `tests/borrowed_route_metadata_adapter.rs::removed_and_recreated_partition_rejects_the_old_state_in_the_same_column` |
| Unpartitioned membership pages reload at constant residency | `crates/contextdb-engine/tests/engine_seams/unpartitioned_vector_membership_pages_reload_at_constant_residency.rs` → `unpartitioned_vector_membership_pages_reload_at_constant_residency` |
| Engine routing keeps AUTO exact and refuses INDEXED | `crates/contextdb-engine/tests/vector/a_search_is_exact_with_no_index_built.rs::an_auto_find_answers_exactly_after_commit_with_no_maintenance`, `::indexed_is_refused_at_every_door` |

## Where a change lives

| Change | Where it lands |
|---|---|
| Exact search, filtering, merge, refusal | `store.rs`, `mem.rs::BoundedBruteForceCursor`; engine supplies dormant loaders. |
| Raw membership cursor | `store/membership.rs`; `store.rs` registers durable sources without retaining a directory. |
| Quantization or final-holder accounting | `quantized.rs`, `memory_budget.rs`; `store.rs` owns preparation/adoption and receipts. |
| Declared policy and dormant index code | `store.rs::VectorIndexLayout` / `ResolvedVectorPolicy`, `ivf/`; do not enable index serving implicitly. |
| Retention / transaction visibility | `store.rs`, `mem.rs`; borrow identities and admit selected changes before copying. |
| Persistent generation, startup inventory, scheduling | Engine `database.rs`, `persistence.rs`; never here. |

## Fast tests

`cargo test -p contextdb-vector --features test-seams`

`cargo test -p contextdb-engine --features test-seams --test vector`

Most tests require `test-seams`; an empty default-feature target is not a passing feature check.
