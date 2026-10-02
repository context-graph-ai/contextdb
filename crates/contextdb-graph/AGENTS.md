# contextdb-graph — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). Graph SQL
(`GRAPH_TABLE`, `DAG`) is in [`docs/query-language.md`](../../docs/query-language.md).

## Purpose

Hold the adjacency index and answer bounded BFS over it; this is the canonical
graph store — rows live in `contextdb-relational`, and this crate indexes
`(source, target, edge_type)` edges derived from them.

## Owns / must not own

Owns (`src/store.rs`) `GraphStore`: forward and reverse adjacency maps,
received-schema publication (`PreparedGraphPublication`,
`prepare_received_schema_publication` → `publish_prepared_received_schema`,
`publish_prepared_received_edge_types` for an affected edge set),
and apply of inserts/deletes. `src/mem.rs` owns `MemGraphExecutor`: the
`GraphExecutor` implementation, bounded BFS (`MAX_VISITED` 100_000), DAG cycle
refusal on insert (`register_dag_edge_types` then `insert_edge`), direction
and edge-type filtering, prepared DAG declaration publication
(`PreparedDagEdgeTypes`), and the parked-traversal continuation that survives
adjacency reclaim.

`src/source.rs` owns storage-neutral adjacency reads (`GraphReadSource`, `GraphSnapshot`,
`GraphCursor`, `GraphEdgeWindow`, `GraphQuery`, `GraphOverlay`): the engine supplies a pinned
durable root, each edge window keeps its admission owner until dropped, and a historical logical
snapshot filters that one physical root by retained MVCC intervals instead of loading newer edges.
`MemGraphExecutor` reads through this seam, with uncommitted edges layered on as an overlay.

Must not own: durability (the engine's `persistence.rs` writes redb; this crate
is memory only), the commit gate (the engine sequences prepare → durable commit
→ publish), SQL grammar or planning, or row storage.

## Seams

- Below: `contextdb-core` (`GraphExecutor`, `AdjEntry`, `NodeId`, `Direction`,
  `Error`) and `contextdb-tx` (`TransactionManager`, `WriteSetApplicator`).
- Above: `contextdb-engine` only — `composite_store.rs`, `executor.rs`,
  `executor/bounded.rs`, `database.rs`.
- Entry points: `GraphStore::new`, `MemGraphExecutor::new`, `insert_edge`,
  `bfs`, `neighbors`.

## Invariants and their guards

| Invariant | Guard |
|---|---|
| BFS on a chain stops at the requested hop bound | `tests/graph_tests.rs::bfs_depth_bound_on_chain` |
| A cycle does not emit duplicate nodes | `tests/graph_tests.rs::bfs_cycle_no_duplicates` |
| A snapshot never sees later committed edges | `tests/graph_tests.rs::bfs_respects_mvcc_snapshot` |
| `max_depth` 1 returns only the first hop | `tests/graph_tests.rs::accepts_max_depth_one` |
| Crossing the visited ceiling is a typed error | `tests/graph_tests.rs::bfs_visited_limit_error` |
| Incoming and Both directions return the matching neighbours | `tests/graph_tests.rs::direction_incoming_and_both` |
| An edge-type filter ignores other types; an empty graph returns nothing | `tests/graph_tests.rs::edge_type_filtering_and_empty_graph` |
| A parked traversal still emits every visible neighbour after reclaim | `tests/bounded_traversal_survives_adjacency_compaction.rs::a_parked_traversal_emits_every_visible_neighbour_when_all_the_entries_it_consumed_are_reclaimed`, `::a_parked_traversal_emits_every_visible_neighbour_when_a_consumed_entry_survives_the_reclaim` |
| A million-entry traversal queue is released on a small stack | `tests/long_traversal_queue_drops_iteratively.rs::a_traversal_queue_holding_a_million_entries_is_released_on_a_small_stack` |
| A `DAG`-declared edge type refuses a cycle insert, including a self-loop | `crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs::dag_01_cycle_rejected_via_insert_sql`, `::dag_02_self_loop_rejected_via_insert_sql` |
| A received ordinary edge-table drop removes its links, including after restart | `crates/contextdb-engine/tests/sync/a_received_drop_of_an_edge_table_takes_its_links.rs::a_received_drop_of_an_ordinary_edge_table_removes_its_links_now_and_after_a_restart`, `::an_authenticated_received_drop_of_an_ordinary_edge_table_removes_its_links` |
| A graph delete seeks only its own edge identity and decodes no unrelated edges | `crates/contextdb-engine/tests/engine_seams/graph_delete_seeks_only_its_edge_identity.rs` → `graph_delete_seeks_only_its_edge_identity` |

## Where a change lives

| Change | Where it lands |
|---|---|
| Adjacency layout, whole-schema or affected-edge publication | `src/store.rs` |
| Reading adjacency from a pinned durable root, edge windows and the staged overlay | `src/source.rs`, consumed by `src/mem.rs` and `src/store.rs`; the engine implements the source. |
| BFS, visited ceiling, DAG cycle check and prepared declaration publication, direction/type filters | `src/mem.rs`; export public types from `src/lib.rs` |
| `GRAPH_TABLE` grammar | Parser crate |
| Plan shape for `GraphBfs` | Planner crate |
| Anything that must survive restart | Engine `persistence.rs` — this crate only mirrors durable state |

## Fast test

```bash
cargo test -p contextdb-graph
```
