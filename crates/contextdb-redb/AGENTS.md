# contextdb-redb — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). This crate is ContextDB's
maintained fork of the upstream redb 4.1.0 package: the single-file, crash-safe B-tree store under
every file-backed database.

## Owns, and must not own

Owns: redb's storage engine, plus the retained delta upstream lacks — bounded resumable
compaction (`compact_step` with `CompactionCursor`/`CompactionProgress`, relocating a finite page
batch that yields to a waiting writer), live-reader observations, a retained statistics snapshot
(`storage_statistics_snapshot`), thread-scoped read admission and backend-read observation
(`read_admission.rs`: `with_read_memory_admission`, `with_read_page_charge`, `ReadPageCharge`,
`observe_backend_reads`, and one-pass `scan_reads`/`with_scan_reads` under which a scan grows the
read cache by at most a 16 KiB allowance and otherwise replaces evictable bytes), a graceful
read-only handoff that publishes a clean committed header while the writer still owns the file
(`Database::prepare_read_only_handoff`), and per-table root fingerprints read without faulting
payload pages (`table_root_fingerprint`) with a same-transaction callback after a relocation batch
(`compact_step_with_hook`) so the engine can bind a moved root before commit.
The `contextdb-internal` byte-table seam adds lazy bounded lookup, prefix/value-prefix iteration
and a root-pinned `BoundedByteCursor` / `BoundedByteHandle` (`tree_store/bounded_bytes.rs`). It can
reject an unrelated oversized singleton through charged windows; a possible match uses ordinary
complete-page admission and decoding. Retained roots keep complete checksum verification.
<!-- enforced by: crates/contextdb-redb/src/tree_store/bounded_bytes.rs::absent_lookup_and_prefix_neighbor_reject_without_materializing_the_extent, crates/contextdb-redb/src/tree_store/bounded_bytes.rs::retained_root_inspection_keeps_full_checksum_admission -->

Allocator metadata is paged, not resident. Both the region tracker and the buddy-allocator
bitmaps read through a set of 4096-byte windows (`tree_store/page_store/allocator_pager.rs`) sized
from the writer's page-cache share the engine passes in: one byte in 32 of that share, never fewer
than 32 windows, carved out of the page cache so the engine's one cache charge covers it. Windows
are found through a fixed open-addressed identity index sized once per admitted count, so
evict/insert churn never allocates. A dirty window spills to a disposable same-directory scratch
file the backend supplies (`ScratchBackend`) as one 4104-byte record (identity plus words), a miss
reloads that record with one read through a 4104-byte stack buffer, and bitmap words load lazily
from the saved or virtual base. Free-order lookups descend the bitmap summary rather than sweeping
leaf words. Resident allocator bytes therefore follow the declared budget and do not grow with the
file's region count, a scratch I/O fault refuses publication
and is repaired from the durable root on reopen, and a backend with no scratch keeps the old
in-memory allocator. Compaction advances past a protected allocator source without reading its
payload. The engine reads the retained envelope through the fork's resident-byte and region-count
observations; it decides every budget.
<!-- enforced by: crates/contextdb-redb/tests/allocator_residency_contract.rs::allocator_open_decodes_a_fixed_number_of_regions, crates/contextdb-redb/tests/allocator_residency_contract.rs::allocator_windows_bound_dirty_regions_in_one_transaction, crates/contextdb-redb/tests/allocator_residency_contract.rs::allocator_scratch_failure_prevents_publication_and_repairs_on_reopen, crates/contextdb-redb/src/tree_store/page_store/allocator_pager.rs::window_churn_allocates_nothing, crates/contextdb-redb/src/tree_store/page_store/page_manager.rs::allocation_word_reads_do_not_grow_with_store_size, crates/contextdb-redb/src/tree_store/page_store/page_manager.rs::allocation_window_loads_do_not_grow_with_store_size -->

Must not own: any ContextDB policy — thresholds, batch sizes, intervals, memory limits, table
layouts. The engine decides those (`persistence.rs` calls `compact_step` with its own 64-page
batch). **The delta against upstream stays minimal**, and the five upstream integration targets
(`backward_compatibility`, `basic_tests`, `integration_tests`, `multimap_tests`,
`multithreading_tests`) stay byte-identical to upstream. What ContextDB builds on top of redb is
additive code and lives in the engine, not here.

Deferred-free cleanup uses separate extent and entry budgets: one unit per allocated extent
reclaimed and one per free-list entry visited, shared across both free trees. Each tree receives
an initial fair share; unused capacity can pass to the other tree. The engine sets
each budget to 64 per durable commit. On idle owner close, eligible backlog drains through
repeated commits that retain those per-commit caps. An extent can span multiple physical pages,
so the engine's append-time and memory acceptance gates still apply to this cleanup.
The `test-seams` fault at an eligible cleanup commit simulates a storage I/O
failure after the pending free lists have been checked, so engine close tests
can prove the error reaches the caller without publishing a partial commit.

[`MAINTENANCE.md`](MAINTENANCE.md) is binding: the exact upstream package, checksum and revision,
every changed source file and why, the complete delta ([`upstream-4.1.0.patch`](upstream-4.1.0.patch))
with the command that regenerates it, the upgrade procedure, and the upstream proposals. A change
to `src/` updates the patch and that table in the same commit.

## Seams

- **Library name** stays `redb`; the engine depends on package `contextdb-redb`, published first.
- **Outside the workspace** (`exclude` in the root `Cargo.toml`): it builds and tests from its own
  manifest and its own upstream `Cargo.lock`, so `cargo test --workspace` never covers it.
- **Above:** only `contextdb-engine` (`persistence.rs`, `read_image_memory.rs`,
  `vector_observations.rs`). The `test-seams` feature exposes `write_wait_observer` to engine tests.

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
| When to compact, batch size, thresholds, handle recycling | Engine crate, `persistence.rs` — never here |
| Page ownership and admission lifetime | `read_admission.rs`, `tree_store/page_store/cached_file.rs` (`PageMemory`, `CachedPage`, `ChargedPage`); limits and budget policy stay in the engine. |
| Reject-only byte-table inspection, opaque cursors and complete-node layout validation | `tree_store/bounded_bytes.rs`, exported through `table.rs`, `transactions.rs` and `lib.rs` behind `contextdb-internal`; windows in `page_store/cached_file.rs` / `page_manager.rs`. Inventory policy stays in the engine. |
| Commit work and write observation | `commit_work.rs`, `table_write_observer.rs`, `transactions.rs`, `transaction_tracker.rs`; engine policy stays in `persistence.rs`. |
| Allocator bitmap windows, scratch spill, lazy bitmap loading | `tree_store/page_store/allocator_pager.rs`, `bitmap.rs`, `buddy_allocator.rs`, `region.rs`, `page_manager.rs`; scratch factory in `file_backend/`, backend hook in `src/db.rs`; record in `MAINTENANCE.md` and the patch. Test-only counters: `src/db/allocator_metadata_test_seams.rs`. |
| Table mutation or storage key layout | `table.rs`, `multimap_table.rs`, `tree_store/`; update `MAINTENANCE.md` and the fork patch for any fork delta. |
| Engine-facing retained-root reader entry points | `src/db.rs`: `Builder::open_read_only_external_with_backend`, `ReadOnlyDatabase::begin_external_read`, `Database::begin_external_read_nonblocking`, and `Builder::create_with_backend_and_external_snapshots` behind non-default `contextdb-internal` (`Cargo.toml`); the engine owns authenticated registration and root lifetime. See `MAINTENANCE.md` for the retained fork delta. |
| A storage primitive the engine cannot build from outside | Here, as the smallest upstream-shaped change, plus the patch and MAINTENANCE.md row |
| An upstream upgrade | MAINTENANCE.md: start from the new package and its unmodified tests and lockfile, rebase the delta, bump this package and the engine dependency together |
| Exporting fork primitives and testing backend region opening | `src/lib.rs`, `src/open_with_backend_region_override_tests.rs`; backend behavior remains in `src/db.rs`. |

## Fast tests

```bash
cargo test --manifest-path crates/contextdb-redb/Cargo.toml --locked
cargo test --manifest-path crates/contextdb-redb/Cargo.toml --locked --features contextdb-internal
cargo test -p contextdb-engine --features test-seams --test engine_seams
```

The hidden internal `NativeReadView` owns a paired committed id/root and an exactly
admitted shared retention guard. Native reader registration uses the single
hashbrown reader registry with original invoices in combined allocation headers;
replacement admission precedes allocation, physical free precedes refund, and the
last registration releases its backing. Legacy backing is replaced before native
qualification. Byte/u8 lookups and fingerprints use that same captured root.
The paired capture, rollback and ownership contracts and all nine changed source
paths are recorded in MAINTENANCE.md; upstream proposals remain unsubmitted.
