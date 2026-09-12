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
(`hnsw.rs`: `HnswIndex`); `SQ8`/`SQ4` payloads (`quantized.rs`); memory charge ownership
(`memory_budget.rs`); the core `VectorExecutor` implementation (`mem.rs`: `MemVectorExecutor`).

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
| Charged bytes balance: a refusal on one index leaves every other index's charge exact | `tests/vector_tests.rs` → `memory_accountant_rejection_on_a_leaves_b_accountant_byte_balance_correct`, `clear_hnsw_keeps_accounting_charged_while_search_holds_graph` |
| A generation with no live vectors loads as an empty graph | `tests/vector_tests.rs` → `a_durable_generation_with_no_live_vectors_loads_as_an_empty_graph` |
| Corrupt or incompatible generation bytes are refused, never searched | `src/hnsw.rs` → `durable_generation_refuses_corruption_and_incompatible_column_definition` |
| Filtered search charges no work for excluded history | `src/hnsw.rs` → `ordinary_allowed_search_excluded_history_adds_no_visit_or_distance_work` |
| Recall-at-10 of the indexed route is at least 95% of exact | `crates/contextdb-engine/tests/vector/vector_held_out_native_reference.rs` → `held_out_unfiltered_native_reference_recovers_the_required_neighbors` |
| Every search, index and consolidation threshold comes from the declared column policy resolved in one place (`VectorIndexLayout::resolve_policy` / `effective_auto_index_at` in `store.rs`); an undeclared value falls back to a documented default there, but a declared value is never silently overridden by a constant elsewhere | `crates/contextdb-engine/tests/vector/vector_policy_resolver_contract.rs` → `declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries` |

Publication, restart, repair and partition behaviour end to end are guarded by the engine's
`crates/contextdb-engine/tests/vector_*_contract.rs` files; the claims they bind are tagged in
`docs/architecture.md` and `docs/query-language.md`.

## Where a change lives

| Change | Where it lands |
|---|---|
| A search route, merge order or refusal | `store.rs` (per-layout search, global merge), `mem.rs` for the `VectorExecutor` entry |
| Graph build or search behaviour, the owned generation byte format | `hnsw.rs`; the fork only for a primitive `hnsw.rs` cannot build on |
| A quantization format | `quantized.rs`, plus the resident-byte estimate beside it |
| Per-column policy resolution (`AUTO_INDEX_AT`, `HNSW`, `CONSOLIDATION`, limits) | `store.rs` (`VectorIndexLayout`, `ResolvedVectorPolicy`); the grammar is the parser's |
| What one maintenance step does to a partition | `store.rs` (`run_hnsw_maintenance_cycle`, `maintenance_need`) |
| When maintenance runs, what is persisted, how a partition is loaded from disk | Engine crate (`database.rs`, `persistence.rs`) |

## Fast tests

```bash
cargo test -p contextdb-vector --features test-seams
cargo test -p contextdb-engine --features test-seams --test vector
```

Most vector test files compile only with `test-seams` (`#![cfg(feature = "test-seams")]` or
per-test gates); without the flag they build and run nothing. `cargo test --workspace` enables it
through feature unification.
