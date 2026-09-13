# contextdb-tx — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). MVCC design is
in [`docs/architecture.md`](../../docs/architecture.md).

## Purpose

Assign transaction and snapshot identities, hold a deferred-apply `WriteSet`
per active transaction, and publish that write set through
`WriteSetApplicator` on commit so relational, graph, and vector state change
together.

## Owns / must not own

Owns (`src/manager.rs`) `TransactionManager`: `begin`, `snapshot`,
`with_write_set` / `with_write_set_detached`, `commit` / `commit_with_lsn` /
`commit_with_reserved_lsn_callback`, `rollback`, LSN allocation, the commit
index, and `CommitFailure`. Owns (`src/write_set.rs`) `WriteSet`,
`WriteSetApplicator`, `RelationalDeletePredicate`, and
`row_matches_delete_predicates`.

Must not own: what a write set means in storage (the applicator in the engine
and store crates applies it), SQL, durability, or conflict policy. Never add a
second transaction manager beside this one.

## Seams

- Below: `contextdb-core` (`TxId`, `SnapshotId`, `Lsn`, `RowId`, `Error`,
  typed atomics).
- Above: `contextdb-relational`, `contextdb-graph`, `contextdb-vector`, and
  `contextdb-engine` (`composite_store.rs`, `database.rs`).
- Entry point: `TransactionManager::new` / `new_with_counters`; stores
  implement `WriteSetApplicator`.

## Invariants and their guards

| Invariant | Guard |
|---|---|
| `begin` assigns strictly increasing `TxId`s | `tests/tx_tests.rs::begin_is_monotonic` |
| A commit advances the snapshot; a rollback does not | `tests/tx_tests.rs::snapshot_advances_on_commit`, `::rollback_does_not_advance_snapshot` |
| A non-empty commit updates the watermark; an empty commit does not | `tests/tx_tests.rs::commit_updates_watermark`, `::empty_commit_does_not_advance_snapshot_or_lsn` |
| A second commit or rollback of the same `TxId` is `TxNotFound` | `tests/tx_tests.rs::double_commit_returns_tx_not_found`, `::double_rollback_returns_tx_not_found` |
| A late lower `TxId` is reassigned above the watermark | `tests/tx_tests.rs::late_lower_tx_is_reassigned_above_watermark` |
| A failed apply removes the transaction under the commit lock and does not advance the watermark or LSN allocator | `tests/tx_safety_tests.rs::tx_01_failed_apply_removes_transaction_under_commit_lock`, `::tx_02_commit_failure_does_not_advance_watermark`, `::tx_04_failed_apply_rewinds_lsn_allocator` |
| A sync `TxId` floor is not visible until commit | `tests/tx_safety_tests.rs::tx_03_sync_txid_floor_is_not_visible_until_commit` |
| Commit removes the active transaction before apply | `tests/tx_safety_tests.rs::tx_05_commit_removes_active_transaction_before_apply` |
| A failed DDL LSN allocation does not advance `current_lsn` | `tests/tx_safety_tests.rs::tx_06_failed_ddl_lsn_allocation_does_not_advance_current_lsn` |
| Prepare runs after the transaction is frozen; prepare, apply, and return see canonical final rows | `tests/tx_safety_tests.rs::tx_07_prepare_runs_after_transaction_is_frozen`, `::tx_08_prepare_apply_and_return_see_canonical_final_rows` |
| After-apply publication precedes commit visibility | `tests/tx_safety_tests.rs::tx_09_after_apply_publication_precedes_commit_visibility` |

## Where a change lives

| Change | Where it lands |
|---|---|
| Commit, rollback, snapshot, LSN, or `TxId` assignment | `src/manager.rs` |
| Write-set fields, emptiness, canonicalization | `src/write_set.rs` |
| How a write set is applied to rows, edges, or vectors | The store crate that implements `WriteSetApplicator`, sequenced by the engine |
| A new identifier newtype | Core crate `types.rs` — not here |

## Fast test

```bash
cargo test -p contextdb-tx
```
