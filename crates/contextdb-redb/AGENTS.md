# contextdb-redb — Agent Rules

Purpose: provide the maintained redb 4.1.0 B-tree storage dependency beneath file-backed stores.
Read the [root rules](../../AGENTS.md) and binding [MAINTENANCE.md](MAINTENANCE.md).

## Owns / must not own

Owns bounded compaction, retained-root primitives, page admission and allocator paging. Engine
owns all ContextDB thresholds, schedules, layouts and memory policy; additive orchestration
belongs there. Keep the fork delta minimal, the five upstream integration targets byte-identical,
and update `upstream-4.1.0.patch` plus MAINTENANCE's source table for every source change.

## Seams

Library name `redb`, package `contextdb-redb`, excluded from the workspace: use its own locked
manifest. Engine is the consumer; core supplies errors/accounting. The `contextdb-internal`
feature exposes `compact_step`/`compact_step_with_hook`, root fingerprints, external retained
snapshots and bounded byte-table cursors. Native views keep the paired committed id/root and
admitted retention guard. Never add test-observation changes to the fork for an engine defect.
Allocator bitmaps use bounded windows and same-directory scratch; engine funds their envelope.
Deferred-free cleanup has separate extent/entry budgets; engine sets them and drives close drain.

## Invariants and their guards

| Invariant | Guard (file → test) |
|---|---|
| Relocation batches preserve every value across interleaved writes and a restart | `crates/contextdb-engine/tests/engine_seams/storage_compaction_online.rs` → `relocation_batches_preserve_values_with_interleaved_writes_and_restart` |
| A refused page allocation aborts the batch without advancing the cursor | `crates/contextdb-engine/tests/engine_seams/storage_compaction_online.rs` → `allocation_refusal_aborts_relocation_without_advancing_its_position` |
| A loaded page remains charged until its final holder drops, including after result drop or cache eviction | `crates/contextdb-engine/tests/direct_file_image_memory_lifetime.rs` → `a_read_within_limit_succeeds_and_keeps_its_page_charged_until_the_session_drops`; `crates/contextdb-engine/src/persistence/search_page_reuse_tests.rs` → `a_proof_page_a_statement_still_holds_stays_charged_after_the_cache_evicts_it` |
| A live read snapshot survives relocated and reused pages | `src/transactions.rs` → `online_relocation_tests::live_snapshot_survives_relocated_and_reused_pages` |
| Relocation progresses while reads are live and yields to a waiting writer | `crates/contextdb-engine/src/database/storage_compaction_overlap_tests.rs` → `automatic_relocation_progresses_with_live_storage_reads_and_waiting_recordings` |
| A paused statistics snapshot does not block a foreground commit | `crates/contextdb-engine/tests/engine_seams/storage_compaction_online.rs` → `paused_storage_statistics_allow_a_foreground_commit` |
| Backend-read observation counts the bytes actually read and ends with the operation | `crates/contextdb-engine/tests/engine_seams/storage_compaction_online.rs` → `backend_read_observation_counts_actual_bytes_and_ends_with_the_operation` |
| Complete nodes reached by bounded lookup, prefix iteration and cursor navigation validate their layouts before offset access; malformed leaves and branches return corruption without panic | `src/tree_store/bounded_bytes.rs::tests::bounded_lookup_rejects_malformed_leaf_and_branch_without_panicking`, `src/tree_store/bounded_bytes.rs::tests::bounded_prefix_iteration_rejects_malformed_leaf_and_branch_without_panicking`, `src/tree_store/bounded_bytes.rs::tests::cursor_layout_validation_rejects_malformed_navigation` |
| Bounded lookup, prefix iteration and cursor navigation reject branch cycles before child page fetches, returning typed corruption without hanging | `src/tree_store/bounded_bytes.rs::tests::bounded_lookup_rejects_branch_cycle_without_hanging`, `src/tree_store/bounded_bytes.rs::tests::bounded_prefix_iteration_rejects_branch_cycle_without_hanging`, `src/tree_store/bounded_bytes.rs::tests::cursor_navigation_rejects_branch_cycle_without_hanging` |
| Reject-only windows admit before backend reads and release their charges; possible matches retain complete-page admission | `src/tree_store/bounded_bytes.rs::tests::window_refusal_precedes_backend_read_and_window_charge_releases`, `src/tree_store/bounded_bytes.rs::tests::absent_lookup_and_prefix_neighbor_reject_without_materializing_the_extent` |
| An opaque cursor continues past a caller-refused singleton, but navigation refusal is terminal; streamed key checks release their windows | `src/tree_store/bounded_bytes.rs::tests::cursor_continues_after_refused_singleton_in_every_sibling_position`, `src/tree_store/bounded_bytes.rs::tests::cursor_navigation_refusal_is_terminal_and_has_no_backend_read`, `src/tree_store/bounded_bytes.rs::tests::cursor_streamed_key_checks_release_windows_without_materializing_giant_keys` |
| Bounded cursor handles keep their captured root across abort, commit and reopen; retained roots use complete checksum admission | `src/tree_store/bounded_bytes.rs::tests::cursor_handles_pin_the_old_root_across_abort_commit_reopen_and_integrity`, `src/tree_store/bounded_bytes.rs::tests::cursor_retained_root_keeps_full_checksum_admission` |
| Singleton insert-before, insert-after with a small separator, and neighboring deletion preserve the old giant value without faulting it; a giant separator keeps ordinary extent admission | `src/tree_store/bounded_bytes.rs::tests::value_prefix_rejection_and_insert_before_preserve_abort_reopen_and_pinned_reader`, `src/tree_store/bounded_bytes.rs::tests::small_separator_splits_both_orders_without_faulting_the_giant_value`, `src/tree_store/bounded_bytes.rs::tests::insert_after_separator_window_refuses_before_read_and_releases_its_owner`, `src/tree_store/bounded_bytes.rs::tests::insert_after_giant_separator_uses_ordinary_extent_admission`, `src/tree_store/bounded_bytes.rs::tests::deleting_a_partial_leaf_does_not_fault_its_large_singleton_neighbor` |
| An externally registered root remains readable when a writer grows the file after the reader captures its header | `src/db.rs` → `test::external_reader_keeps_its_root_when_writer_grows_after_header_read` (requires `contextdb-internal`); `crates/contextdb-engine/tests/read/cursor_survives_writer_handover.rs` → `cursor_opened_before_writer_handover_finishes_its_snapshot` |
| Allocator open work is a fixed number of region decodes however many regions the file has | `tests/allocator_residency_contract.rs` → `allocator_open_decodes_a_fixed_number_of_regions` |
| Dirty allocator windows stay bounded inside one transaction, and spilled state survives non-durable then durable commits, aborts, retained readers and registered external snapshots | `tests/allocator_residency_contract.rs` → `allocator_windows_bound_dirty_regions_in_one_transaction`, `allocator_nondurable_then_durable_commit_preserves_spilled_rows`, `allocator_abort_repair_and_retained_reader_preserve_exact_rows`, `allocator_spill_preserves_registered_external_snapshot` |
| A scratch failure at any point refuses publication and the next open repairs from the durable root; a backend without scratch keeps the in-memory allocator | `tests/allocator_residency_contract.rs` → `allocator_scratch_failure_prevents_publication_and_repairs_on_reopen`, `allocator_nth_scratch_faults_refuse_publication_and_repair`, `allocator_backend_without_scratch_keeps_its_in_memory_fallback` |
| Compaction in one-page batches keeps every root, and a refused first batch does fixed work before admission | `tests/allocator_residency_contract.rs` → `allocator_one_page_compaction_batches_finish_and_preserve_roots`, `allocator_compaction_rejection_has_fixed_pre_admission_work` |
| Files written by older redb releases still open | `tests/backward_compatibility.rs` (upstream suite) |
| The fork's `Cargo.lock` is unchanged by packaging | no Rust test — checked by `scripts/verify-packaged-engine.sh` |
| Recorded `upstream-4.1.0.patch` hunks produce the maintained source lines they name | `crates/contextdb-core/tests/redb_fork_patch_matches_its_sources.rs::the_redb_fork_patch_produces_the_sources_it_names` |
| The complete source delta is recorded and the five upstream test targets are unmodified | unguarded — regenerate the patch with the MAINTENANCE.md command and compare |

## Where a change lives

| Change | Where it lands |
|---|---|
| Compaction policy, admission limits, batch size | Engine persistence; never here. |
| Bounded byte lookup/cursor and layout validation | `tree_store/bounded_bytes.rs`, exported through `table.rs`, `transactions.rs`, `lib.rs`. |
| Page lifetime and charge | `read_admission.rs`, `tree_store/page_store/cached_file.rs`; refund after physical free. |
| Allocator windows / scratch | `tree_store/page_store/{allocator_pager,bitmap,buddy_allocator,region,page_manager}.rs`, `file_backend/`, `db.rs`. |
| Retained-root readers / commit hooks | `db.rs`, `transactions.rs`, `transaction_tracker.rs`, `commit_work.rs`, `table_write_observer.rs`. |
| Upstream upgrade or any source delta | Follow MAINTENANCE.md; regenerate the patch and retain upstream tests/lockfile. |

## Fast tests

`cargo test --manifest-path crates/contextdb-redb/Cargo.toml --locked --features contextdb-internal`

`cargo test -p contextdb-engine --features test-seams --test engine_seams`
