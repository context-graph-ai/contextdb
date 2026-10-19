# contextdb-vector — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). This crate answers nearest-neighbour
queries over every `VECTOR(n)` column: one index per `(table, column)`, split into partition
layouts, each served from a sealed base graph, sealed change graphs and a mutable tail under one
MVCC snapshot.

## Owns, and must not own

Owns: the index registry and partition layouts (`store.rs`: `VectorStore`, `IndexState`,
`VectorIndexLayout`, `ResolvedVectorPolicy`); exact, filtered and indexed search and the global
top-k merge; the in-memory generation lifecycle (register, preload, evict, quarantine, publish a
replacement, reclaim once no snapshot needs it); one partition's maintenance step
(`VectorStore::run_hnsw_maintenance_cycle`); the graph wrapper and its owned generation bytes
(`hnsw.rs`: `HnswIndex`); the one unfiltered F32 traversal that serves both a resident graph and
a saved graph read page by page (`hnsw/unfiltered.rs`) and the multi-partition union search
(`hnsw/union.rs`); `SQ8`/`SQ4` payloads (`quantized.rs`); memory charge ownership
(`memory_budget.rs`); the core `VectorExecutor` implementation (`mem.rs`: `MemVectorExecutor`).

Large partitions are served and built without holding the population in memory. A partition can
register a paged builder or reclaimer (`register_partition_paged_builder`,
`register_partition_paged_reclaimer`, progress and publish/abandon calls in `store.rs`) that
inserts a durable batch at a time through `hnsw/paged_builder.rs`: the engine supplies authenticated
point pages and lookup directories (`PagedVectorReader`, `PagedVectorTransaction`,
`PagedVectorGraphSource`, `PagedVectorPersistence`), and this crate retains no all-node graph or
row-to-vector map. The mutable tail can be paged the same way (`PagedVectorTail`, installed and
appended through `install_partition_paged_tail` / `append_partition_paged_tail`), so a search sees
saved base, change graphs and tail with the same visibility, tie order and work charges.
Raw memberships are read through durable sources pinned to one root (`store/membership.rs`:
`RawVectorMembershipSource`, cursors, `LazyRawVectorMembershipSource`); no decoded directory is
retained, and partition membership is verified on first use.
<!-- enforced by: crates/contextdb-engine/tests/engine_seams/unpartitioned_vector_membership_pages_reload_at_constant_residency.rs::unpartitioned_vector_membership_pages_reload_at_constant_residency, crates/contextdb-engine/tests/engine_seams/paged_initial_replacement_and_reclamation_progress_under_writes.rs::paged_initial_replacement_and_reclamation_progress_under_writes, crates/contextdb-engine/tests/engine_seams/first_search_after_writer_reopen_uses_saved_pages_without_build.rs::first_search_after_writer_reopen_uses_saved_pages_without_build -->

Must not own: where generation bytes, raw vectors, the catalog or the change journal are stored,
or when a maintenance cycle runs — the engine does both. SQL grammar, planning, row
authorization and sync belong to the parser, planner and engine. A second graph implementation
never goes here or anywhere: graph algorithms come from `contextdb-hnsw`, and anything new that
vector search needs from a graph is written here, in `hnsw.rs`, on top of that fork.

## Seams

- **Above:** `contextdb-engine` owns a `VectorStore`, drives maintenance from
  `Database::run_maintenance_cycle`, and implements `DormantVectorGraphLoader` and
  `DormantRawVectorLoader` (`database.rs`) so a partition loads only when a query or a
  maintenance step needs it.
- **Below:** `contextdb-hnsw` (library name `hnsw_rs`) for graphs; `contextdb-core` for
  `VectorExecutor`, `VectorIndexRef`, `VectorEntry`, `Error`; `contextdb-tx` for snapshots and
  `WriteSetApplicator`.
- **Test seams:** pause, fault and observation hooks are `#[doc(hidden)]`, behind the `test-seams`
  feature (`test_seam.rs`, the `*ForTest` types re-exported from `lib.rs`). Production code never
  reads them.

## Invariants and their guards

| Invariant | Guard (file → test) |
|---|---|
| Search on one `(table, column)` never returns another column's vectors | `tests/vector_tests.rs` → `search_on_ref_a_returns_only_ref_a_vectors_under_10000_iteration_apply_storm` |
| Equal scores order by ascending row id on the exact and the graph path | `tests/vector_tests.rs` → `public_search_ties_order_by_row_id_in_bruteforce_and_hnsw_paths` |
| A snapshot sees exactly the vectors committed at it | `tests/vector_tests.rs` → `mvcc_snapshot_isolation`; `src/store/transaction_visibility_tests.rs` |
| Concurrent updates and deletes leave one live vector and no graph ghosts | `tests/vector_tests.rs` → `concurrent_same_ref_updates_leave_single_live_vector_after_commit`, `same_ref_concurrent_deletes_leave_no_zombie_hnsw_entries` |
| A wrong dimension is a typed error for the named index | `tests/vector_tests.rs` → `dimension_mismatch_typed_error_with_positive_control` |
| Building one index never blocks search or ingest on another | `tests/vector_tests.rs` → `index_b_search_completes_while_index_a_hnsw_build_paused`, `reindex_on_index_a_does_not_block_ingest_on_index_b` |
| Retention pruning in selected columns leaves an unrelated column's resident generation, graph and indexed answers intact | `tests/vector_tests.rs` → `selected_index_row_pruning_preserves_unrelated_resident_generation_and_indexed_answers` |
| Charged bytes balance: a refusal on one index leaves every other index's charge exact | `tests/vector_tests.rs` → `memory_accountant_rejection_on_a_leaves_b_accountant_byte_balance_correct`, `clear_hnsw_keeps_accounting_charged_while_search_holds_graph` |
| A generation with no live vectors loads as an empty graph | `tests/vector_tests.rs` → `a_durable_generation_with_no_live_vectors_loads_as_an_empty_graph` |
| Corrupt or incompatible generation bytes are refused, never searched | `src/hnsw.rs` → `tests::durable_generation_refuses_corruption_and_incompatible_column_definition` |
| A saved graph read page by page returns the same neighbours, order and ties, and charges the same distance work and refusals, as the same graph resident in memory | `src/hnsw.rs` → `tests::saved_graph_pages_preserve_owned_neighbor_ids_and_order_across_search_breadths`, `tests::saved_graph_pages_charge_the_same_distance_expansions_as_owned_graphs`, `tests::saved_graph_pages_refuse_the_same_distance_budget_as_owned_graphs`, `tests::saved_graph_pages_return_the_exact_lowest_row_ids_for_equal_vectors` |
| A saved base plus resident tail preserves row-id ties even at search breadth one | `src/hnsw.rs` → `tests::mixed_saved_base_and_resident_tail_keep_exact_row_id_ties_at_breadth_one` |
| Exhaustive AUTO can stream a dormant partition one body at a time when bulk loading is refused; unrestricted explicit EXACT on a writer still requires that load | `crates/contextdb-engine/tests/vector/vector_lazy_raw_residency_contract.rs` → `auto_exhaustive_find_answers_inside_memory_that_cannot_hold_a_partition` |
| Unfiltered search descends each upper level until no closer neighbour remains before searching the bottom level | `src/hnsw.rs` → `tests::unfiltered_upper_descent_finishes_each_level_before_searching_bottom` |
| A multi-partition search stays within its one statement work budget (`max(EF_SEARCH, k)` × stored bottom-level degree, `2 × M` plus the 4 links that keep every point reachable) and completes its answer; a distance already computed while admitting partitions is reused rather than recomputed, without changing any answer | `crates/contextdb-engine/tests/vector/multi_partition_union_recovers_nearest_rows.rs` → `uniform_three_and_eight_partition_shared_walk_regression`, `clustered_one_three_and_eight_partition_shared_walk_regression` |
| Filtered search charges no work for excluded history | `src/hnsw.rs` → `tests::ordinary_allowed_search_excluded_history_adds_no_visit_or_distance_work` |
| Recall-at-10 of the indexed route is at least 95% of exact | `crates/contextdb-engine/tests/vector/vector_held_out_native_reference.rs` → `held_out_unfiltered_native_reference_recovers_the_required_neighbors` |
| Every search, index and consolidation threshold comes from the declared column policy resolved in one place (`VectorIndexLayout::resolve_policy` / `effective_auto_index_at` in `store.rs`); an undeclared value falls back to a documented default there, but a declared value is never silently overridden by a constant elsewhere | `crates/contextdb-engine/tests/vector/vector_policy_resolver_contract.rs` → `declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries` |
| Default HNSW breadth and topology follow the live partition size and quantization; partial declarations remain valid across profile boundaries | `src/store.rs` → `tests::compatibility_profile_edges_and_partial_policy_validation_agree` |
| Scalar publications preserve every MVCC membership and interval while unchanged raw bodies share one allocation, charged through its final holder | `src/store/shared_payload_tests.rs::retained_scalar_memberships_share_one_body_and_keep_all_snapshot_intervals`, `src/store/shared_payload_tests.rs::shared_cross_partition_body_survives_source_removal_and_final_external_holder`, `src/store/shared_payload_tests.rs::shared_allocation_header_stays_charged_until_final_holder_after_store_drop` |
| Received columns prepare directory, shared-header and required copied-move body growth with their own funder even without a store-wide accountant; inserted bodies are caller-admitted beforehand, abort or refusal releases only preparation reservations, and published payload charges last until the final holder drops | `src/store/received_publication_accounting_tests.rs::received_publication_prepares_next_insert_and_releases_its_last_holder`, `src/store/received_publication_accounting_tests.rs::received_columns_keep_distinct_funders_and_legacy_columns_in_a_mixed_batch` |
| Detached-copy hydration moves unique uncharged bodies out of shared headers; shared histories and charged or resident sources keep their representation and final-holder charge | `src/store/detached_copy_payload_tests.rs::detached_copy_unique_unwrap_moves_the_body_and_preserves_shared_or_charged_owners`, `src/store/detached_copy_payload_tests.rs::detached_copy_hydration_retains_only_new_singleton_bodies_and_releases_headers`, `src/store/detached_copy_payload_tests.rs::detached_copy_header_refusal_releases_loader_body_workspace_and_retries`, `src/store/detached_copy_payload_tests.rs::detached_copy_mixed_history_preserves_bits_and_shared_final_holder_charges`, `src/store/detached_copy_payload_tests.rs::detached_copy_already_resident_sources_keep_representation_charge_and_funder` |
| Resident generation selection copies only frontier-live entries after the optional LSN boundary, preserving representations and history; inconsistent directory metadata refuses before body materialization | `src/store/generation_selection_tests.rs::selected_generation_materialization_preserves_all_representations_and_snapshot_intervals`, `src/store/generation_selection_tests.rs::selected_generation_refuses_inconsistent_metadata_before_body_materialization` |
| Borrowed source metadata and scalar marking copy no giant partition key; stale source identities cannot mark a replacement column or partition | `tests/borrowed_route_metadata_adapter.rs::giant_source_metadata_and_marking_allocate_no_giant_key_copy_or_encoding`, `tests/borrowed_route_metadata_adapter.rs::replacement_column_rejects_the_old_state_without_marking_the_new_source`, `tests/borrowed_route_metadata_adapter.rs::removed_and_recreated_partition_rejects_the_old_state_in_the_same_column` |
| A writable reopen searches saved pages without rebuilding the graph | `crates/contextdb-engine/tests/engine_seams/first_search_after_writer_reopen_uses_saved_pages_without_build.rs` → `first_search_after_writer_reopen_uses_saved_pages_without_build` |
| Unpartitioned membership pages reload at constant residency | `crates/contextdb-engine/tests/engine_seams/unpartitioned_vector_membership_pages_reload_at_constant_residency.rs` → `unpartitioned_vector_membership_pages_reload_at_constant_residency` |
| Initial build, replacement and reclamation progress in finite batches beside live writes | `crates/contextdb-engine/tests/engine_seams/paged_initial_replacement_and_reclamation_progress_under_writes.rs` → `paged_initial_replacement_and_reclamation_progress_under_writes` |
| The AUTO index threshold is judged on the aggregate across partitions, before partition cost | `crates/contextdb-engine/tests/engine_seams/aggregate_auto_threshold_precedes_partition_cost_and_keeps_scope.rs` → `aggregate_auto_threshold_precedes_partition_cost_and_keeps_scope` |

Publication, restart, repair and partition behaviour end to end are guarded by the engine's
`crates/contextdb-engine/tests/vector_*_contract.rs` files; the claims they bind are tagged in
`docs/architecture.md` and `docs/query-language.md`.

## Where a change lives

| Change | Where it lands |
|---|---|
| Streaming exhaustive bodies after refused bulk admission | `mem.rs::BoundedBruteForceCursor`; `store.rs::DormantRawVectorLoader` tells it whether a committed-image exact read can stream. The engine supplies the loader. |
| A search route, merge order or refusal | `store.rs` (per-layout search, global merge), `mem.rs` for the `VectorExecutor` entry |
| Graph build or search behaviour, the owned generation byte format | `hnsw.rs`; unfiltered F32 search in `hnsw/unfiltered.rs` (one walk for resident and saved graphs — never a second copy), multi-partition union in `hnsw/union.rs`; the fork only for a primitive `hnsw.rs` cannot build on |
| Paged (transactional) graph build, reclaim and tail; durable point/row/exact lookup records | `hnsw/paged_builder.rs`; the graph algorithm is `contextdb-hnsw` `src/paged.rs`; the engine supplies the page and directory storage. |
| Reading raw memberships from durable storage | `store/membership.rs` traits; `store.rs` registers sources (`register_lazy_durable_raw_partition`, `register_durable_raw_reverse_source`). Never retain a decoded directory here. |
| A quantization format | `quantized.rs`, plus the resident-byte estimate beside it |
| Per-column policy resolution (`AUTO_INDEX_AT`, `HNSW`, `CONSOLIDATION`, limits) | `store.rs` (`VectorIndexLayout`, `ResolvedVectorPolicy`); the grammar is the parser's |
| Store-size-dependent default search breadth profile | `src/store.rs::compatibility_hnsw_profile`, used by `VectorIndexLayout::resolve_policy`; test all size-band boundaries and partial policy validation in `src/store.rs::compatibility_profile_edges_and_partial_policy_validation_agree`. |
| What one maintenance step does to a partition | `store.rs` (`run_hnsw_maintenance_cycle`, `maintenance_need`) |
| When maintenance runs, what is persisted, how a partition is loaded from disk | Engine crate (`database.rs`, `persistence.rs`) |
| Read-only test observations | `test_seam.rs`, exported from `lib.rs` under the test feature; search behavior remains in `store.rs` and `hnsw.rs`. |
| Transaction-visible vector state | `store/transaction_visibility_tests.rs` guards overlay behavior; implementation stays in `store.rs` and `mem.rs`. |
| Shared raw-body ownership, received-column funders or detached-copy hydration | `store.rs` owns preparation, adoption and receipts; `quantized.rs` owns private payload representations and final-drop charges. Public `VectorEntry` and durable encoding stay owned-Vec based. |
| Resident generation input selection or borrowed maintenance metadata | `store.rs::entries_for_partition_generation_at_snapshot` and `VectorPartitionSourceMaintenanceMetadata`; engine owns frontier admission, catalog inventory and durable cleanup. |

Retention's directory visitor borrows partition keys and raw directory identities, and lets the
engine admit each selected mutation before copying it; it never loads vector bodies for deletion.
<!-- enforced by: crates/contextdb-engine/src/database/bounded_retention_preparation_tests.rs::reclamation_admits_vector_and_edge_identities_without_payload_copies -->

## Fast tests

```bash
cargo test -p contextdb-vector --features test-seams
cargo test -p contextdb-engine --features test-seams --test vector
```

Most vector test files compile only with `test-seams` (`#![cfg(feature = "test-seams")]` or
per-test gates); without the flag they build and run nothing. `cargo test --workspace` enables it
through feature unification.

Retention uses `prune_row_ids_in_indexes` for selected columns, borrows partition states and removes
whole-row reverse-map entries in place; unrelated indexes retain their resident graph state.
<!-- enforced by: crates/contextdb-vector/tests/vector_tests.rs::selected_index_row_pruning_preserves_unrelated_resident_generation_and_indexed_answers, crates/contextdb-engine/src/database/bounded_retention_preparation_tests.rs::reclamation_admits_vector_and_edge_identities_without_payload_copies -->

Raw bodies have two private owners. Legacy void insertion, loading and moves keep their Vec inline,
adopting only the caller-admitted body bytes and allocating no shared header; their existing
metadata-growth protocol is unchanged. Fallible loaded-state accounting prices bodies,
directories and promotion headers before moving each owned Vec into shared ownership without
cloning it. Existing body charges retain their actual funder; a later open's newly admitted header
retains its own funder through final drop. Refusal preserves representation and caller reservations.
A prepared move from
an owned source admits a distinct shared body and header while retaining the historical source;
later moves from shared sources keep distinct MVCC memberships on one immutable allocation.
Hydration shares only bit-identical stored representations, preserving signed zero and quantized
bounds. Owned bodies count individually; shared bodies deduplicate by Arc allocation identity.
Each allocation keeps its charge through final holder drop, including partition moves, eviction
and registry removal. Receipts include live shared headers in `raw_reverse_directory`;
`raw_vector_bodies` retains its payload-byte meaning. Directory memberships remain charged per
version. Public `VectorEntry` values and durable codecs keep their existing owned-Vec encoding.
<!-- enforced by: crates/contextdb-vector/src/store/shared_payload_tests.rs::retained_scalar_memberships_share_one_body_and_keep_all_snapshot_intervals, crates/contextdb-vector/src/store/shared_payload_tests.rs::shared_cross_partition_body_survives_source_removal_and_final_external_holder, crates/contextdb-vector/src/store/shared_payload_tests.rs::shared_payload_preparation_rollback_and_refusal_preserve_membership_and_charge, crates/contextdb-vector/src/store/shared_payload_tests.rs::eviction_and_reload_share_unchanged_history_and_keep_prior_holder_charged, crates/contextdb-vector/src/store/shared_payload_tests.rs::hydration_shares_only_bit_identical_history_in_every_stored_representation, crates/contextdb-vector/src/store/shared_payload_tests.rs::representation_sharing_checks_quantized_bounds_length_payload_and_kind, crates/contextdb-vector/src/store/shared_payload_tests.rs::shared_allocation_header_stays_charged_until_final_holder_after_store_drop, crates/contextdb-vector/src/store/shared_payload_tests.rs::refused_shared_header_hydration_releases_body_workspace_and_retries_without_membership_loss, crates/contextdb-engine/tests/vector/vector_memory_ownership_reconciliation_contract.rs::retained_journey_scalar_publications_bound_unique_payloads_and_admit_a_small_write -->

Legacy compatibility and promotion ownership are additionally guarded by deterministic tests in
`src/store/shared_payload_tests.rs`: `legacy_void_inserts_adopt_only_funded_bodies_without_shared_header_headroom`,
`loaded_owned_promotion_refusal_preserves_reservations_then_moves_each_body_without_cloning`,
`mixed_already_accounted_open_admits_only_new_headers_and_transfers_live_counters`,
`owned_empty_payloads_promote_to_distinct_shared_allocations`,
`retained_owned_clone_requires_its_own_body_charge_and_never_copies_a_charge_token`,
`prepared_owned_source_copy_is_admitted_and_mixed_memberships_prune_exactly`,
`mixed_owned_shared_eviction_and_reload_preserve_final_holder_charges`, and
`late_legacy_index_inherits_only_the_callers_funded_body_owner`,
`promotion_keeps_legacy_body_and_new_header_with_their_actual_funders`, and
`already_accounted_empty_state_hydration_attaches_every_new_shared_charge`, and
`legacy_shared_source_move_retains_its_funded_owned_copy_without_new_header`.
<!-- enforced by: crates/contextdb-vector/src/store/shared_payload_tests.rs::legacy_void_inserts_adopt_only_funded_bodies_without_shared_header_headroom, crates/contextdb-vector/src/store/shared_payload_tests.rs::loaded_owned_promotion_refusal_preserves_reservations_then_moves_each_body_without_cloning, crates/contextdb-vector/src/store/shared_payload_tests.rs::mixed_already_accounted_open_admits_only_new_headers_and_transfers_live_counters, crates/contextdb-vector/src/store/shared_payload_tests.rs::owned_empty_payloads_promote_to_distinct_shared_allocations, crates/contextdb-vector/src/store/shared_payload_tests.rs::retained_owned_clone_requires_its_own_body_charge_and_never_copies_a_charge_token, crates/contextdb-vector/src/store/shared_payload_tests.rs::prepared_owned_source_copy_is_admitted_and_mixed_memberships_prune_exactly, crates/contextdb-vector/src/store/shared_payload_tests.rs::mixed_owned_shared_eviction_and_reload_preserve_final_holder_charges, crates/contextdb-vector/src/store/shared_payload_tests.rs::late_legacy_index_inherits_only_the_callers_funded_body_owner, crates/contextdb-vector/src/store/shared_payload_tests.rs::promotion_keeps_legacy_body_and_new_header_with_their_actual_funders, crates/contextdb-vector/src/store/shared_payload_tests.rs::already_accounted_empty_state_hydration_attaches_every_new_shared_charge, crates/contextdb-vector/src/store/shared_payload_tests.rs::legacy_shared_source_move_retains_its_funded_owned_copy_without_new_header -->

Received publications can attach accounting to individual columns without a store-wide accountant.
Prepared batches reserve directory, shared-header and required copied-move body growth with each
touched column's actual funder before publication. Inserted bodies are caller-admitted beforehand;
abort or refusal releases only preparation reservations and retains that caller-admitted body charge.
Publication adopts payload charges until the final holder drops; settlement returns unused reserved
bytes to their funder. Mixed legacy columns keep their existing insertion protocol.
<!-- enforced by: crates/contextdb-vector/src/store/received_publication_accounting_tests.rs::received_publication_prepares_next_insert_and_releases_its_last_holder, crates/contextdb-vector/src/store/received_publication_accounting_tests.rs::received_columns_keep_distinct_funders_and_legacy_columns_in_a_mixed_batch -->

Detached-copy hydration unwraps only unique, uncharged shared allocations, moves their Vec bodies
without cloning and releases redundant headers after they are freed. Shared history and charged
or already-resident sources retain their owners; refused hydration releases loader workspace and
can retry without losing memberships.
<!-- enforced by: crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_unique_unwrap_moves_the_body_and_preserves_shared_or_charged_owners, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_hydration_retains_only_new_singleton_bodies_and_releases_headers, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_header_refusal_releases_loader_body_workspace_and_retries, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_mixed_history_preserves_bits_and_shared_final_holder_charges, crates/contextdb-vector/src/store/detached_copy_payload_tests.rs::detached_copy_already_resident_sources_keep_representation_charge_and_funder -->

Resident generation materialization selects the pinned snapshot's live entries and optional
post-LSN delta before copying bodies. It preserves F32/SQ8/SQ4 representations, intervals and all
retained memberships; metadata disagreement refuses before body materialization.
<!-- enforced by: crates/contextdb-vector/src/store/generation_selection_tests.rs::selected_generation_materialization_preserves_all_representations_and_snapshot_intervals, crates/contextdb-vector/src/store/generation_selection_tests.rs::selected_generation_refuses_inconsistent_metadata_before_body_materialization -->
