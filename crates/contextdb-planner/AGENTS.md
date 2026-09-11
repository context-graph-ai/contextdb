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
`try_plan_index_scan` is a deliberate stub returning `None`; index selection happens in the engine.
Never add a catalog handle here to make a plan "smarter".

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
| `USE VECTOR` on a query without a vector ordering is refused | `crates/contextdb-engine/tests/vector_partition_query_contract.rs::every_use_vector_mode_refuses_a_non_vector_ordering_query` |
| `USE RANK` without a vector ordering, or without `LIMIT`, is refused | unguarded here (the variants appear only in engine wire-value documents) |
| A `ROW_VECTOR` ordering must be the only `ORDER BY` item | unguarded |

## Where a change lives

| Change | Where it lands |
|---|---|
| A new statement kind | A `PhysicalPlan` variant or plan struct in `plan.rs`, its arm in `plan()`, its `explain()` text; then the engine executor arm. The parser adds the `Statement` first. |
| A new query-shape refusal that needs only the AST | `planner.rs`, returning a typed `contextdb_core::Error` variant (add the variant in core). |
| A refusal that needs the table's declaration | Engine executor, not here. |
| New vector query options | `VectorSearch` fields in `plan.rs`, filled from the parser's `SelectBody`. |
| Explain wording | `PhysicalPlan::explain()`; redact anything a bound value could leak. |

## Fast test

```bash
cargo test -p contextdb-planner
```
