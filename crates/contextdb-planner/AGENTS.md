# contextdb-planner — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md).

**Purpose:** turn one parsed `Statement` into one `PhysicalPlan` by fixed rules — no statistics, no
cost model, no schema lookup.

## Owns / must not own

Owns the `PhysicalPlan` tree and its per-statement plan structs (`src/plan.rs`), the routing rules
from AST shape to operator (`src/planner.rs`), the static `PhysicalPlan::explain()` text, and the
plan-time refusals that need only the statement: vector ordering without `LIMIT`
(`UnboundedVectorSearch`, `UseRankRequiresLimit`), `USE VECTOR` / `USE RANK` without a vector
ordering, a `ROW_VECTOR` ordering mixed with other `ORDER BY` items, graph depth over the engine
cap (`BfsDepthExceeded`, cap 10, default 5), and a `MATCH` with no edge.

Must not own: grammar or AST shape (parser); any decision that needs a table's declared schema —
immutability, state machines, which index serves a predicate, vector search mode resolution,
partition scope, `validate_sort_key` — those run in the engine executor against `TableMeta`.
Index selection happens in the engine.
Never add a catalog handle here to make a plan "smarter". The planner maps
`MAINTAIN COUNT` declarations to the engine's sort direction and passes
`SET`/`SHOW VECTOR_SERVING_CACHE` as plan variants; the engine owns storage and execution.
`SHOW CONTENT_SIZE` maps directly to `ShowContentSize`; the engine owns byte accounting and
the file-backed scope. A grouped `COUNT(*)` (the only `GROUP BY` the parser admits) plans as
`GroupedCount { input: Scan, group_by, columns }` with no schema lookup; whether a declared
maintained count serves it, counts it from the rows, or refuses it is the engine's decision.
A `ROW_NUMBER` window lowers to `WindowRank { input, window, binding }` between the source
filter and the projection; the engine binds its columns at runtime and chooses per-group index
seeks or the Sort-and-rank fallback.

## Seams

- Below: `contextdb-parser` (`ast::Statement`, `Expr`, `SelectBody`) and `contextdb-core`
  (`Error`, `Direction`, `PropagationRule`).
- Entry point: `contextdb_planner::plan(&Statement) -> Result<PhysicalPlan>`.
- Above: `contextdb-engine` executes the plan (`executor.rs`, `executor/bounded.rs`) and is the
  only consumer. `HnswSearch` exists in the enum but the planner emits `VectorSearch`; the engine
  decides whether a built index answers it.

## Invariants and their guards

| Invariant | Guard |
|---|---|
| A `GRAPH_TABLE ... MATCH` CTE plans as `GraphBfs` | `tests/planner_tests.rs::match_cte_routes_to_graph_bfs` |
| `ORDER BY col <=> $q LIMIT k` plans as `VectorSearch`; a plain predicate plans as `Scan` | `tests/planner_tests.rs::cosine_order_routes_to_vector_search`, `::standard_select_routes_to_scan` |
| A CTE the outer query never reads is not planned | `tests/planner_tests.rs::unused_graph_cte_is_not_included_in_explain` |
| Immutability is a runtime check, never a plan-time one | `tests/planner_tests.rs::immutability_checked_at_runtime_not_plan_time` |
| A hop count over the engine cap is refused | `tests/planner_tests.rs::depth_over_cap_rejected` |
| `PURGE` keeps its own plan, never lowered to `Delete` | `tests/purge_planner_contract_tests.rs::purge_plan_remains_distinct_from_ordinary_delete` |
| Static explain never prints the key that selects a `ROW_VECTOR` source | `tests/vector_explain_redaction_contract.rs::static_vector_explain_redacts_every_supported_row_vector_source_key_shape` |
| `USE VECTOR` on a query without a vector ordering is refused | `crates/contextdb-engine/tests/vector/vector_partition_query_contract.rs::every_use_vector_mode_refuses_a_non_vector_ordering_query` |
| `USE RANK` without a vector ordering, or without `LIMIT`, is refused | `tests/planner_tests.rs::use_rank_requires_vector_order_and_limit` |
| A `ROW_VECTOR` ordering must be the only `ORDER BY` item | `crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs::prv_24_row_vector_order_by_rejects_unsupported_sort_shapes` |
| A declared count remains correct immediately on a populated table | `crates/contextdb-engine/tests/engine_seams/declaring_a_count_on_a_table_with_rows_is_atomic_and_charged.rs::declaring_a_count_on_a_table_with_rows_is_correct_immediately` |
| The serving-cache setting survives reopen and clears on `NONE` | `crates/contextdb-engine/tests/vector/a_declared_serving_cache_is_honoured.rs::a_declared_serving_cache_survives_a_reopen_and_clears_on_none` |

## Where a change lives

| Change | Where it lands |
|---|---|
| A new statement kind | A `PhysicalPlan` variant or plan struct in `plan.rs`, its arm in `plan()`, its `explain()` text; then the engine executor arm. The parser adds the `Statement` first. |
| A new query-shape refusal that needs only the AST | `planner.rs`, returning a typed `contextdb_core::Error` variant (add the variant in core). |
| A refusal that needs the table's declaration | Engine executor, not here. |
| New vector query options | `VectorSearch` fields in `plan.rs`, filled from the parser's `SelectBody`. |
| `MAINTAIN COUNT` projection or `SET`/`SHOW VECTOR_SERVING_CACHE` routing | `planner.rs` maps the parsed declaration or statement; `plan.rs` carries the resulting plan. |
| Columns or vector sources nested in row expressions | `planner.rs` recursive expression walkers. |
| Explain wording | `PhysicalPlan::explain()`; redact anything a bound value could leak. |

## Fast test

```bash
cargo test -p contextdb-planner
```
