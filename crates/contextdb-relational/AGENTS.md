# contextdb-relational — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md).

**Purpose:** hold every table's versioned rows and index postings in memory and answer snapshot
reads over them; this is the canonical row store — graph and vector state are derived from it.

## Owns / must not own

Owns (`src/store.rs`) `RelationalStore`: per-table `VersionedRow` lists keyed by `RowId`, row-id
allocation, index storage (`IndexStorage`, `IndexEntry` with `created_tx`/`deleted_tx`
visibility), prepared-then-published index membership (`prepare_memberships` →
`publish_memberships`), retention-expiry preparation and publication, sync-source sidecars
(`sync_source_lsn`, `SyncSourceKind`), whole-table replacement for received schemas
(`PreparedRelationalPublication`), and index-only candidate derivation
(`unauthorised_index_candidates`). `src/membership.rs` owns the immutable, path-copied index
membership image readers hold, charged to a `ReadMemoryBudget`. `src/mem.rs` owns
`MemRelationalExecutor`: the `RelationalExecutor` implementation, the bounded cursors
(`BoundedPhysicalCursor`, `BoundedIndexCursor`, `BoundedOrderedRowCursor`,
`BoundedExactIndexCursor`), and the write-time checks it enforces — immutable tables refuse
upsert and delete (`ImmutableTable`), and a declared state machine refuses an undeclared
transition (`InvalidStateTransition`).

Must not own: durability (the engine's `persistence.rs` writes redb; this crate is memory only),
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
| A snapshot sees only rows committed at or before it | `tests/relational_tests.rs::scan_respects_snapshot_visibility`, `::mvcc_snapshot_isolation` |
| A rolled-back insert is never visible | `tests/relational_tests.rs::rollback_hides_inserted_rows` |
| Upsert reports Insert / Update / NoOp exactly | `tests/relational_tests.rs::upsert_insert_update_noop` |
| An immutable table refuses delete and upsert | `tests/relational_tests.rs::observations_are_immutable_for_delete_and_upsert` |
| A declared state machine refuses an undeclared transition | `tests/relational_tests.rs::invalidation_state_machine_enforced` |
| Pruning row versions keeps older snapshots' view of surviving versions | `src/store.rs::tests::remove_row_versions_preserves_sorted_same_row_positions` |
| Membership images stay balanced and release their memory charge when the last reader drops | `src/membership.rs::tests::persistent_membership_paths_balance_and_release_when_the_last_reader_drops` |
| Bounded seeks keep duplicate keys, direction, and exclusive bounds | `src/membership.rs::tests::bounded_seeks_preserve_duplicate_keys_direction_and_exclusive_edges` |
| A refused (over-budget) membership change leaves identity, visibility and charge unchanged | `src/membership.rs::tests::refused_path_copy_leaves_identity_visibility_and_charge_unchanged` |
| A captured cursor keeps its membership image across later same-id changes | `crates/contextdb-engine/tests/snapshot_visible_posting_membership_contract.rs::captured_current_cursor_keeps_its_image_across_same_id_changes_and_cancellation` |
| Current membership does not walk retired postings after replacements and reopen | `crates/contextdb-engine/tests/snapshot_visible_posting_membership_contract.rs::current_index_membership_does_not_walk_retained_true_postings_after_replacements_and_reopen` |
| A deleted row's posting is tombstoned (its key, including any partition key, is kept and `deleted_tx` stamped — never dropped), so an older snapshot still finds it | `crates/contextdb-engine/tests/snapshot_visible_posting_membership_contract.rs::retired_partitions_keep_old_exact_membership_after_moves_delete_and_reopen` |

## Where a change lives

| Change | Where it lands |
|---|---|
| A new write-time row rule that needs only the table's `TableMeta` | `src/mem.rs` insert/upsert/delete path, with a typed `contextdb_core::Error`. |
| A new index kind or posting layout | `IndexStorage` in `src/store.rs`, plus `membership.rs` if readers hold it. |
| A new bounded cursor shape | `src/mem.rs`; its memory charge goes through `ReadMemoryBudget`, never an uncharged buffer. |
| Anything that must survive restart | Engine `persistence.rs` — this crate only mirrors durable state. |

## Fast test

```bash
cargo test -p contextdb-relational
```
