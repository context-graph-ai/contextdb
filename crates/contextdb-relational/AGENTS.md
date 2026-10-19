# contextdb-relational — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md).

**Purpose:** hold versioned resident row images and index postings and answer snapshot reads;
graph and vector state are derived from the canonical rows.

## Owns / must not own

Owns (`src/store.rs`) `RelationalStore`: per-table `VersionedRow` lists keyed by `RowId`, row-id
allocation, index storage (`IndexStorage`, `IndexEntry` with `created_tx`/`deleted_tx`
visibility), prepared-then-published index membership (`prepare_memberships` →
`publish_memberships`), retention-expiry preparation and publication, sync-source sidecars
(`sync_source_lsn`, `SyncSourceKind`), whole-table replacement for received schemas
(`PreparedRelationalPublication`), and index-only candidate derivation
(`unauthorised_index_candidates`). `committed_directory_row_for_meta` keeps required columns
resident; `committed_row_may_omit_columns` tells the engine when to rehydrate the exact durable
version before sync or custody extraction. `src/membership.rs` owns the immutable, path-copied
index membership image readers hold, charged to a `ReadMemoryBudget`. `src/mem.rs` owns
`MemRelationalExecutor`: the `RelationalExecutor` implementation, the bounded cursors
(`BoundedPhysicalCursor`, `BoundedIndexCursor`, `BoundedOrderedRowCursor`,
`BoundedExactIndexCursor`), and the write-time checks it enforces — immutable tables refuse
upsert and delete (`ImmutableTable`), and a declared state machine refuses an undeclared
transition (`InvalidStateTransition`).

`src/source.rs` owns the root-scoped read seam a durable writer plugs into (`RelationalReadSource`,
`RelationalWindow`, `RelationalCursor`, `RelationalIndexRange`, `visible_row`, `exact_probe`,
`staged_row`): a read pinned to one durable root returns one admitted record at a time and keeps no
decoded cache, and staged (uncommitted) rows are merged over the durable answer through
`StagedExactProbe`/`StagedRelationalPosting` so a constraint probe never scans unrelated pending
writes. `src/mem.rs` takes either source, so the resident image is no longer required to hold all
history.

Must not own: durability (the engine's `persistence.rs` writes redb; this crate owns the resident image),
the commit gate (the engine sequences prepare → durable commit → publish), authorization
(`unauthorised_index_candidates` is explicitly not an authorization predicate — callers intersect
it with their own scope and principal decision), SQL, or graph/vector structures.

## Seams

- Below: `contextdb-core` (`TableMeta`, `VersionedRow`, `RowId`, `TxId`, `SnapshotId`,
  `RelationalExecutor` trait, `read_memory`) and `contextdb-tx` (`TransactionManager`,
  `WriteSetApplicator`).
- Above: `contextdb-engine` only — `composite_store.rs`, `persistence.rs`, `executor.rs`,
  `executor/bounded.rs`, `database.rs`, `database/gate.rs`.
- `membership` is `#[doc(hidden)]` public so the engine can hold images; it is not API.

## Invariants and their guards

| Invariant | Guard |
|---|---|
| A file-backed sender includes shed large columns in a manifested sync unit | `crates/contextdb-engine/tests/custody/delivery_manifest_contract.rs::a_manifested_unit_travels_on_the_ordinary_push_and_the_oversized_staging_path_and_is_never_served_to_a_pulling_peer` |
| A snapshot sees only rows committed at or before it | `tests/relational_tests.rs::scan_respects_snapshot_visibility`, `::mvcc_snapshot_isolation` |
| A rolled-back insert is never visible | `tests/relational_tests.rs::rollback_hides_inserted_rows` |
| Upsert reports Insert / Update / NoOp exactly | `tests/relational_tests.rs::upsert_insert_update_noop` |
| An immutable table refuses delete and upsert | `tests/relational_tests.rs::observations_are_immutable_for_delete_and_upsert` |
| A declared state machine refuses an undeclared transition | `tests/relational_tests.rs::invalidation_state_machine_enforced` |
| Pruning row versions keeps older snapshots' view of surviving versions | `src/store.rs::tests::remove_row_versions_preserves_sorted_same_row_positions` |
| Membership images stay balanced and release their memory charge when the last reader drops | `src/membership.rs::tests::persistent_membership_paths_balance_and_release_when_the_last_reader_drops` |
| Bounded seeks keep duplicate keys, direction, and exclusive bounds | `src/membership.rs::tests::bounded_seeks_preserve_duplicate_keys_direction_and_exclusive_edges` |
| A refused (over-budget) membership change leaves identity, visibility and charge unchanged | `src/membership.rs::tests::refused_path_copy_leaves_identity_visibility_and_charge_unchanged` |
| A captured cursor keeps its membership image across later same-id changes | `crates/contextdb-engine/tests/engine_seams/snapshot_visible_posting_membership_contract.rs::captured_current_cursor_keeps_its_image_across_same_id_changes_and_cancellation` |
| Current membership does not walk retired postings after replacements and reopen | `crates/contextdb-engine/tests/engine_seams/snapshot_visible_posting_membership_contract.rs::current_index_membership_does_not_walk_retained_true_postings_after_replacements_and_reopen` |
| A constraint probe reads only its key's durable postings plus the pending writes that match, and keeps competing-commit conflicts | `crates/contextdb-engine/tests/engine_seams/paged_constraints_preserve_staging_and_competing_commit_conflicts.rs` → `paged_constraints_preserve_staging_and_competing_commit_conflicts` |
| Writable open never reads the history families (row versions, change log, commit index) | `crates/contextdb-engine/tests/engine_seams/writable_open_never_touches_history_families.rs` → `writable_open_never_touches_history_families` |
| A deleted row's posting is tombstoned (its key, including any partition key, is kept and `deleted_tx` stamped — never dropped), so an older snapshot still finds it | `crates/contextdb-engine/tests/engine_seams/snapshot_visible_posting_membership_contract.rs::retired_partitions_keep_old_exact_membership_after_moves_delete_and_reopen` |
| Declared maintained counts survive restart and sync and answer from stored counts | `crates/contextdb-engine/tests/engine_seams/declaring_a_count_on_a_table_with_rows_is_atomic_and_charged.rs::a_maintained_count_survives_restart_and_checkpoint_restore`; `crates/contextdb-server/tests/authenticated_sync_contract_tests.rs::a_maintained_count_declaration_crosses_sync_or_holds_back_its_table` |

## Where a change lives

| Change | Where it lands |
|---|---|
| A new write-time row rule that needs only the table's `TableMeta` | `src/mem.rs` insert/upsert/delete path, with a typed `contextdb_core::Error`. |
| A new index kind or posting layout | `IndexStorage` in `src/store.rs`, plus `membership.rs` if readers hold it. |
| A new bounded cursor shape | `src/mem.rs`; its memory charge goes through `ReadMemoryBudget`, never an uncharged buffer. |
| Reading rows or index postings from durable storage at a pinned root | `src/source.rs` traits; the engine implements them (`persistence/writer_adapters.rs`); merge of staged rows in `exact_probe` / `staged_row`. |
| Which row columns remain resident | `store.rs::committed_directory_row_for_meta` and `committed_row_may_omit_columns`; outbound rehydration belongs to engine `database.rs` and `database/custody_store.rs`. |
| Maintained counts and their declaration, installation, and committed-image reads | Engine `executor.rs`, `database.rs`, `persistence.rs`, and `executor/bounded.rs`; see the engine guide. This crate must not own the count store or read policy. Guarded by `crates/contextdb-engine/tests/engine_seams/declaring_a_count_on_a_table_with_rows_is_atomic_and_charged.rs::declaring_a_count_on_a_table_with_rows_is_correct_immediately` and `crates/contextdb-server/tests/authenticated_sync_contract_tests.rs::a_maintained_count_declaration_crosses_sync_or_holds_back_its_table`. |
| Anything that must survive restart | Engine `persistence.rs` — this crate only mirrors durable state. |

## Fast test

```bash
cargo test -p contextdb-relational
```
