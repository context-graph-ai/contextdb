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
`prepare_received_schema_publication` → `publish_prepared_received_schema`),
and apply of inserts/deletes. `src/mem.rs` owns `MemGraphExecutor`: the
`GraphExecutor` implementation, bounded BFS (`MAX_VISITED` 100_000), DAG cycle
refusal on insert (`register_dag_edge_types` then `insert_edge`), direction
and edge-type filtering, and the parked-traversal continuation that survives
adjacency reclaim.

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

## Where a change lives

| Change | Where it lands |
|---|---|
| Adjacency layout, received-schema publication | `src/store.rs` |
| BFS, visited ceiling, DAG cycle check, direction/type filters | `src/mem.rs` |
| `GRAPH_TABLE` grammar | Parser crate |
| Plan shape for `GraphBfs` | Planner crate |
| Anything that must survive restart | Engine `persistence.rs` — this crate only mirrors durable state |

## Fast test

```bash
cargo test -p contextdb-graph
```
