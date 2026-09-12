---
name: vector-search
description: Similarity search in contextdb — maintained and partitioned embedding columns, the <=> operator, bounded filters, query modes, inspection, schema-declared USE RANK policies, and hybrid graph + vector queries.
---

# Vector search in contextdb

**Prerequisite:** needs the `contextdb` binary. In a checkout of this repo:
`cargo build --release -p contextdb-cli`. Other install options: [`docs/getting-started.md`](../../docs/getting-started.md).

Vectors are not a bolt-on store here. A `VECTOR(n)` column lives in the same table, the same MVCC
snapshot and the same transaction as your rows and edges — so one query can filter relationally,
traverse a graph, and rank by similarity without leaving the database.

The operator is pgvector's `<=>` (cosine distance) in `ORDER BY`. There is **no `vector_search()`
function** — `<=>` is the whole surface.

## Recipe checklist

1. Declare one `VECTOR(n)` column per embedding space you need (below) — a row can carry more than
   one.
2. Search with `ORDER BY <col> <=> <vector> LIMIT n` (mandatory `LIMIT`). Add a `WHERE` clause to
   pre-filter, not post-filter.
3. **If you need "close AND actually worked", not just "close"**, go to `USE RANK` below — plain
   `<=>` order alone cannot express that.
4. **If you need "close, but only within this graph neighborhood"**, go to the hybrid query at the
   bottom — that's the graph skill's traversal composed with steps 1–2 here.
5. **If a query errors `VectorIndexDimensionMismatch`**, the literal you passed doesn't match the
   column's declared `VECTOR(n)` — go back to step 1 and check `.schema <table>`.

## Declare an embedding column

```bash
contextdb ./vec.db --write <<'SQL'
CREATE TABLE evidence (
  id UUID PRIMARY KEY,
  category TEXT NOT NULL,
  vector_text VECTOR(4) PARTITION_KEY (category) MAX_PARTITIONS 256 SEARCH_MODE AUTO,
  vector_vision VECTOR(8) WITH (quantization = 'SQ8')
);
SHOW VECTOR_INDEXES;
SQL
```

Each `VECTOR(n)` column is its own named index, keyed by `(table, column)` — a row can carry a text
embedding and a vision embedding side by side, searched independently. `PARTITION_KEY` makes local
search layouts from one or more non-null identity columns. It is not a tenant, authorization rule,
or sync direction: the normal row-access rule still applies, and a syncing receiver derives the
local layout from the accepted row without a new vector-sync field. Omit `PARTITION_KEY` for one
logical layout. With it, omitted `MAX_PARTITIONS` means 256 live-plus-snapshot-retained layouts;
`.schema` prints that effective value. `SHOW VECTOR_INDEXES` reports the declared key and limit,
live/retained layout counts, lifecycle, bytes, and broad-route state.
<!-- enforced by: vector_partition_declaration_contract::partitioned_vector_schema_survives_reopen_with_canonical_defaults, vector_partition_query_contract::partition_scope_does_not_weaken_context_scope_or_principal_filters, vector_partition_sync_inspection_contract::partitioned_sync_derives_receiver_membership_without_changing_owner_pairing, vector_partition_sync_inspection_contract::existing_two_row_sync_keeps_each_vector_with_its_row_and_shows_summary -->

Quantization is per column: `F32` (default), `SQ8`, `SQ4` — the knob for storage footprint.

## Search

```bash
contextdb ./vec.db --write <<'SQL'
INSERT INTO evidence (id, category, vector_text) VALUES ('11111111-1111-1111-1111-111111111111', 'A', [1.0, 0.0, 0.0, 0.0]);
INSERT INTO evidence (id, category, vector_text) VALUES ('22222222-2222-2222-2222-222222222222', 'A', [0.9, 0.1, 0.0, 0.0]);
INSERT INTO evidence (id, category, vector_text) VALUES ('33333333-3333-3333-3333-333333333333', 'B', [0.0, 1.0, 0.0, 0.0]);

SELECT id, category FROM evidence
ORDER BY vector_text <=> [1.0, 0.0, 0.0, 0.0]
LIMIT 2;
SQL
```

Nearest first (lower cosine distance = more similar): the `A` rows, then `B`.

**`LIMIT` is required.** An unbounded `ORDER BY ... <=> ...` is rejected — there is no "return
everything, ranked".

**The CLI has no parameter binding**, so CLI recipes use vector literals. Both `[1.0, 0.0, 0.0,
0.0]` and `'[1,0,0,0]'` parse. From the library, bind `Value::Vector(...)` as `$query` instead —
which is what you want for a 384- or 768-dimension embedding nobody wants to paste.

### Pre-filtered search

Put the filter in `WHERE`. The engine **filters first, then scores only matching rows** — it does
not rank the whole table and post-filter:

```bash
printf "SELECT id FROM evidence WHERE category = 'A' ORDER BY vector_text <=> [1.0, 0.0, 0.0, 0.0] LIMIT 5;\n" \
  | contextdb ./vec.db --json
```
```json
{"result":{"columns":["id"],"rows":[{"id":"11111111-1111-1111-1111-111111111111"},{"id":"22222222-2222-2222-2222-222222222222"}]}}
```

### Search by an existing row's vector

`ROW_VECTOR('table', 'column', key)` reuses a persisted row vector as the query vector — "more like
this one", without reading the embedding out and sending it back in. It is valid **only** as the
right side of `<=>` in `ORDER BY`:

```bash
printf "SELECT id FROM evidence WHERE id != '11111111-1111-1111-1111-111111111111' ORDER BY vector_text <=> ROW_VECTOR('evidence', 'vector_text', '11111111-1111-1111-1111-111111111111') LIMIT 2;\n" \
  | contextdb ./vec.db
```

The source vector is read from the same MVCC snapshot as candidate filtering and scoring. Scoped
handles honor source-row visibility: a hidden source row returns the same typed read-scope error as
an explicit anchor read. Missing table → `TableNotFound`; non-vector column → `UnknownVectorIndex`;
dimension mismatch → `VectorIndexDimensionMismatch`; missing row → `PersistedRowVectorRowMissing`;
NULL cell → `PersistedRowVectorCellNull`.
<!-- enforced by: sql_surface_other::prv_06_row_vector_query_uses_one_snapshot_after_reopen_and_fresh_process, sql_surface_other::prv_07_row_vector_query_rejects_missing_or_wrong_index_source_with_distinct_variants, sql_surface_other::prv_13_row_vector_query_honors_scoped_handle_context_isolation -->

## Maintained indexing and search modes

There is no separate vector index to create. ContextDB maintains the local layouts after commits;
first build and repair run outside the query path. A restart validates the saved generations and
replays only later committed changes, rather than rebuilding every vector before the first search.
If an indexed route is not ready, an ordinary query can use exact comparison only when its active
budget permits it; an `INDEXED` query refuses clearly instead of hiding the problem with a scan.
<!-- enforced by: vector_maintained_lifecycle_contract::caller_driven_vector_indexes_are_built_and_maintained_only_by_maintenance, vector_partition_restart_contract::partitioned_indexed_search_survives_two_clean_restarts_without_a_query_rebuild, vector_search_mode_bounded_contract::indexed_small_nonempty_scope_refuses_until_maintenance_then_keeps_a_staged_delta_visible -->

`SEARCH_MODE AUTO` is the declaration default. `AUTO` decides by count: below the column's
effective threshold over the aggregate allowed set it answers exact when the active limits can
pay and a typed refusal when they cannot; at or above that threshold it uses the maintained route.
`EXACT` examines every
allowed stored vector or refuses when its active budget cannot pay for it — use it for an audit.
`INDEXED` requires the maintained bounded route even for a small non-empty layout — use it for a
live loop that must not trade index loss for a costly scan. <!-- enforced by: vector_search_mode_bounded_contract::auto_uses_the_aggregate_selected_scope_not_each_partition, vector_search_mode_bounded_contract::exact_override_is_exhaustive_across_partitions_and_matches_rust_and_bounded_reads, vector_search_mode_bounded_contract::indexed_small_nonempty_scope_refuses_until_maintenance_then_keeps_a_staged_delta_visible, vector_lazy_raw_residency_contract::auto_refuses_typed_when_the_exact_score_array_cannot_fit --> Override once per vector query:

```sql
SELECT id FROM evidence
WHERE category IN ('A', 'B')
ORDER BY vector_text <=> [1.0, 0.0, 0.0, 0.0]
USE VECTOR INDEXED
LIMIT 5;
```

One key equality searches one layout; finite `IN`/`OR` searches a few; a prefix or no key predicate
searches all authorized layouts. ContextDB merges them by score before `LIMIT`, so this always means
one best-answer list, not one list per layout. If one required layout cannot satisfy `INDEXED`, the
whole query refuses — it never silently omits a layout.
<!-- enforced by: vector_partition_query_contract::equality_on_every_partition_component_selects_one_named_tuple, vector_partition_query_contract::finite_in_partition_scope_merges_before_one_limit, vector_partition_query_contract::composite_partition_prefix_selects_every_tuple_below_the_prefix, vector_partition_query_contract::no_partition_key_predicate_returns_one_global_limited_answer, vector_search_mode_bounded_contract::auto_keeps_healthy_partition_results_for_preflight_and_mid_merge_fallbacks -->

Check which one is live:

```bash
printf ".explain SELECT id FROM evidence ORDER BY vector_text <=> [1.0, 0.0, 0.0, 0.0] LIMIT 5\n" \
  | contextdb ./vec.db --json | jq -r '.explain.physical_plan'
```

`.explain` shows the requested/resolved mode, one/few/all authorized layouts, routes, global merge,
and safe refusal or recovery reason. It does not build or repair an index.
<!-- enforced by: vector_inspection_explain_truth_contract::vector_explain_names_mode_scope_route_merge_and_safe_filter_recovery_without_secrets, vector_inspection_explain_truth_contract::vector_inspection_is_passive_and_reports_real_pre_and_post_maintenance_facts -->

`AUTO_INDEX_AT` and `HNSW (M, EF_CONSTRUCTION, EF_SEARCH)` are per-column declarations for that
threshold and graph workload; they are not process-wide switches. `AUTO_INDEX_AT` and `EF_SEARCH`
take effect for newly opened queries without rebuilding, while `M` or `EF_CONSTRUCTION` schedule a
replacement graph and the previous complete graph continues serving. Leave them silent to keep the
compatibility profile, or alter them online with `ALTER TABLE ... ALTER COLUMN ... SET ...`. Use
[the query-language reference](../../docs/query-language.md#vector-similarity-search) for the
canonical syntax, `DEFAULT` behavior, rendering, and defaults. Inspection separates desired topology
from the serving graph and its build revision. `EF_SEARCH` takes effect immediately; the serving
breadth follows current query policy even if that build revision is older. Actual queries raise an
explicit breadth only to `k`, or an omitted breadth to at least the profile and `10 * k`.
<!-- enforced by: vector_policy_revision_publication_contract::online_vector_policy_revisions_keep_the_complete_graph_serving_and_reject_stale_publication, vector_policy_resolver_contract::declared_ef_search_reaches_graph_work_without_the_silent_compatibility_floor, vector_serving_merge_contract::query_policy_discloses_serving_snapshot_and_full_partition_search_breadth -->

### Broad filters and recovery

Use `WHERE` to narrow candidates before vector ranking. A narrow allowed set can be exact under
`AUTO`; a broad filter needs a bounded filtered-index route. If ContextDB cannot identify allowed
rows through a bounded candidate route, `INDEXED` refuses rather than silently scanning the table;
`AUTO` does exact work only within the active budget. `.explain` says when an ordinary relational
index would make the filter supportable.
<!-- enforced by: vector_search_mode_bounded_contract::indexed_broad_filter_refuses_before_scan_and_becomes_eligible_with_a_relational_index -->

Initial graph construction and repair advance only through maintenance, never inside a query or
as a full build that declarations, writes, or open must wait for. Loaded generations stay sealed;
tombstones exclude obsolete versions until replacement publication, while pinned old snapshots
keep their compatible view. Retention can defer physical reclamation until those readers finish.
<!-- enforced by: vector_maintained_lifecycle_contract::caller_driven_vector_indexes_are_built_and_maintained_only_by_maintenance, vector_generation_quarantine_compaction_lock_contract::pinned_old_snapshot_keeps_its_indexed_generation_until_release_then_reclaims_superseded_bytes, tests/integration/retention_tests.rs::retention_defers_reclaim_without_breaking_vector_search -->

Inspect lifecycle without triggering work (these commands require an unrestricted admin handle;
a constrained caller uses the redacted `.explain`, which omits per-partition adaptive settings):

```sql
SHOW VECTOR_INDEXES;
SHOW VECTOR_PARTITIONS FOR evidence.vector_text;
```

A populated caller-driven row might read
`partition_key={"category":"camera"} live_rows=120 base_generation=4 base_tx=891
pending_inserts=3 tombstones=2 query_state=ready maintenance_state=idle
maintenance_reason=tombstones recovery_action=run_maintenance_cycle`. The matching summary reports
`broad_route=fanout`, its aggregate readiness and exact unavailable/stalled counts, and keeps
`bytes = charged_vector_bytes + charged_index_bytes`; durable byte columns describe file storage
separately and remain zero in memory-only databases.
<!-- enforced by: vector_partition_sync_inspection_contract::constrained_handle_refuses_whole_vector_inspection, vector_inspection_explain_truth_contract::restricted_explain_admits_only_visible_identities_at_equal_headroom, vector_inspection_explain_truth_contract::file_backed_inspection_separates_positive_durable_bytes_from_charged_residency_once -->

The detail view names each local key, live/retained rows, base, pending work, bytes, state, reason,
and recovery action. `ready` means `INDEXED` can serve that layout. Under default engine-owned
maintenance, wait for automatic work; under explicit caller-driven maintenance, call
`Database::run_maintenance_cycle` or `.maintenance run`. If admission reaches the limit, raise it
online with `ALTER TABLE evidence ALTER COLUMN vector_text SET MAX_PARTITIONS 512`; lowering is
refused while live-plus-retained use is higher.
<!-- enforced by: vector_partition_cleanup_inspection_contract::inspection_names_the_lifecycle_of_a_populated_unavailable_partition, vector_inspection_explain_truth_contract::the_same_pending_partition_reports_the_owner_of_maintenance_on_both_show_surfaces, vector_partition_declaration_contract::online_partition_limit_and_search_mode_changes_update_the_declaration, vector_partition_sync_inspection_contract::a_lowered_partition_limit_arriving_over_sync_is_refused_and_changes_nothing -->

For a 2 GiB memory limit, use `SET MEMORY_LIMIT 2G` (CLI `--memory-limit 2G`):
2147483648 bytes of charged memory <!-- enforced by: statement_effect_contract::binary_memory_declaration_accepts_quoted_and_unquoted_sizes -->; whole-process RSS is separate. On a synced or
restored table, inspect `.schema` for the authored `CONTEXT_ID` and Simple/Split `SCOPE_LABEL`
clauses; partition selection never replaces those access rules. <!-- enforced by: vector_partition_query_contract::partition_scope_does_not_weaken_context_scope_or_principal_filters, vector_partition_sync_inspection_contract::a_synced_restricted_reads_disclosure_lists_only_its_own_authorized_ids -->

## Rank by outcomes, not just similarity — `USE RANK`

When cosine similarity is not the only signal you care about, declare a **rank policy** on the
vector column. The formula and join path are resolved at DDL time, stored with the schema, and
replicated through sync — so every caller asking for the same `SORT_KEY` gets the same ranking, and
no application copies formula text into its queries.

The joined column must be indexed.

```bash
contextdb ./rank.db --write <<'SQL'
CREATE TABLE outcomes (
  id UUID PRIMARY KEY,
  decision_id UUID NOT NULL,
  success BOOLEAN NOT NULL
);
CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id);

CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  confidence REAL,
  embedding VECTOR(2) RANK_POLICY (
    JOIN outcomes ON decision_id,
    FORMULA 'coalesce({confidence}, 1.0) * coalesce({success}, 1.0)',
    SORT_KEY effective_confidence
  )
);

INSERT INTO decisions (id, description, confidence, embedding) VALUES
  ('11111111-1111-1111-1111-111111111111', 'closest but failed', 1.0, [1.0, 0.0]),
  ('22222222-2222-2222-2222-222222222222', 'less similar but worked', 1.0, [0.5, 0.0]),
  ('33333333-3333-3333-3333-333333333333', 'fallback with no outcome', 0.25, [0.75, 0.0]);

INSERT INTO outcomes (id, decision_id, success) VALUES
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '11111111-1111-1111-1111-111111111111', FALSE),
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '22222222-2222-2222-2222-222222222222', TRUE);

SELECT id, description, confidence
FROM decisions
ORDER BY embedding <=> [1.0, 0.0] USE RANK effective_confidence
LIMIT 5;
SQL
```

Expected ordering — the vector-nearest row comes **last**, because it failed:

```text
+--------------------------------------+--------------------------+------------+
| id                                   | description              | confidence |
+--------------------------------------+--------------------------+------------+
| 22222222-2222-2222-2222-222222222222 | less similar but worked  | 1          |
| 33333333-3333-3333-3333-333333333333 | fallback with no outcome | 0.25       |
| 11111111-1111-1111-1111-111111111111 | closest but failed       | 1          |
+--------------------------------------+--------------------------+------------+
(3 rows)
```

`{confidence}` binds a column on the anchor row, `{success}` a column on the joined row, and
`{vector_score}` the raw cosine score if you want similarity in the formula. Ranking is applied to
the candidates **before** the top-k cutoff. On a large HNSW-backed index it ranks the candidates
ANN retrieval returned — when outcome ranking must be deterministic, keep a single current summary
row on the joined side.

**Rank policies are schema.** To change one today, recreate the table with the new `RANK_POLICY`
clause and reload the rows.

### Worked example 2 — the same query, without `USE RANK`, to see what the policy actually changes

Run this against the `rank.db` example 1 just built, dropping `USE RANK effective_confidence` —
no `--write`, because it only reads:

```bash
echo "SELECT id, description FROM decisions ORDER BY embedding <=> [1.0, 0.0] LIMIT 5;" \
  | contextdb ./rank.db --json
```

```json
{"result":{"columns":["id","description"],"rows":[{"description":"closest but failed","id":"11111111-1111-1111-1111-111111111111"},{"description":"less similar but worked","id":"22222222-2222-2222-2222-222222222222"},{"description":"fallback with no outcome","id":"33333333-3333-3333-3333-333333333333"}]}}
```

Plain cosine order puts the row that FAILED first, because it's the closest vector. That is exactly
the ordering `USE RANK` exists to override. **Validate you actually declared the rank policy** by
comparing these two queries — if adding `USE RANK <sort_key>` doesn't change the order at all, the
policy isn't attached to the column you're querying (check `.schema decisions` for a `rank_policy`
key) or `USE RANK` named the wrong `SORT_KEY`.

**If `ORDER BY ... USE RANK <name>` is refused as unknown**, the `SORT_KEY` in the `RANK_POLICY`
clause doesn't match — re-check `.schema decisions` for the exact declared name; it is
case-sensitive and not inferred from the formula.

## The hybrid query — graph narrows, vector ranks

This is the query contextdb exists for: find decisions that are semantically similar *and* still
active, then trace each back to the entities it was based on. In a stitched SQLite + vector-store +
hand-rolled-BFS stack this is ~40 lines of application code across three systems. Here it is one
statement, one transaction, one process.

```bash
contextdb :memory: <<'SQL'
CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT NOT NULL, status TEXT NOT NULL, confidence REAL, embedding VECTOR(4));
CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL, entity_type TEXT NOT NULL, properties JSON);
CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('DEPENDS_ON', 'BASED_ON');

INSERT INTO decisions (id, description, status, confidence, embedding) VALUES ('11111111-1111-1111-1111-111111111111', 'use managed RDS for the primary datastore', 'active', 0.9, [1.0, 0.0, 0.0, 0.0]);
INSERT INTO entities (id, name, entity_type, properties) VALUES ('22222222-2222-2222-2222-222222222222', 'RDS', 'SERVICE', '{"region": "us-east-1"}');
INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', 'BASED_ON');

WITH similar_decisions AS (
  SELECT id, description, confidence
  FROM decisions
  WHERE status = 'active'
  ORDER BY embedding <=> [1.0, 0.0, 0.0, 0.0]
  LIMIT 10
),
basis_entities AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (d)-[:BASED_ON]->(b)
    WHERE d.id IN (SELECT id FROM similar_decisions)
    COLUMNS (b.id AS b_id)
  )
)
SELECT sd.id, sd.description, sd.confidence, e.name, e.properties
FROM similar_decisions sd
LEFT JOIN basis_entities be ON TRUE
LEFT JOIN entities e ON e.id = be.b_id;
SQL
```

Three paradigms in one statement: **vector** finds semantically similar decisions, **relational**
filters to `active` and joins entity metadata, **graph** traverses `BASED_ON` to the basis.

The inverse ordering — graph first to narrow the neighbourhood, then vector to rank inside it —
works equally well and is usually what you want when the graph is the cheaper filter:

```bash
contextdb :memory: <<'SQL'
CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL);
CREATE TABLE observations (id UUID PRIMARY KEY, entity_id UUID, data TEXT, embedding VECTOR(4));

INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', 'RELATES_TO');
INSERT INTO observations (id, entity_id, data, embedding) VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '22222222-2222-2222-2222-222222222222', 'connection pool exhausted', [1.0, 0.0, 0.0, 0.0]);
INSERT INTO observations (id, entity_id, data, embedding) VALUES ('cccccccc-cccc-cccc-cccc-cccccccccccc', '99999999-9999-9999-9999-999999999999', 'unrelated, outside the neighborhood', [1.0, 0.0, 0.0, 0.0]);

WITH neighborhood AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (task)-[:RELATES_TO]->{1,2}(related)
    WHERE task.id = '11111111-1111-1111-1111-111111111111'
    COLUMNS (related.id AS b_id)
  )
),
candidates AS (
  SELECT o.id, o.data, o.embedding
  FROM observations o
  INNER JOIN neighborhood n ON o.entity_id = n.b_id
)
SELECT id, data FROM candidates
ORDER BY embedding <=> [1.0, 0.0, 0.0, 0.0]
LIMIT 5;
SQL
```

The unrelated observation is equally similar and still does not come back — it is outside the
neighbourhood the graph selected.

Variable-length paths always need an explicit upper bound (`{1,2}`); the engine's maximum traversal
depth is 10.
<!-- enforced by: tests/acceptance/query_surface.rs::f98_graph_neighborhood_scoped_vector_search, tests/integration/hybrid_query_on_true_join_tests.rs::scenario4_hybrid_query_returns_rows_once_traversal_has_data -->

## Gotchas

- **`LIMIT` is mandatory** on any `<=>` ordering.
- **Dimensions must match** the column's declared `VECTOR(n)`, or you get
  `VectorIndexDimensionMismatch`.
- **Search routes to the column named in `ORDER BY`**, not to "the table's vector" — a two-vector
  table has two independent indexes.
- **Vector lifecycle recovery uses inspection and maintenance**, as described in this recipe.
  `contextdb migrate <path>` is only for a store diagnosed as legacy-format.
- **`PROPAGATE ON STATE <s> EXCLUDE VECTOR`** drops a row out of vector results when it reaches a
  state — the declarative way to stop invalidated rows from being retrieved.

## Depth

- Operator, `ROW_VECTOR`, pre-filtering, indexing thresholds, rank-policy grammar: [`docs/query-language.md`](../../docs/query-language.md#vector-similarity-search)
- Rank policy grammar — `RANK_POLICY`, `JOIN`, `FORMULA` placeholders, `SORT_KEY`, `USE RANK`: [`docs/query-language.md`](../../docs/query-language.md#rank-policies)
- More hybrid patterns: [`docs/usage-scenarios.md`](../../docs/usage-scenarios.md) scenarios 4, 7, 8, 13

## Next

- `GRAPH_TABLE`, `DAG`, state machines, `PROPAGATE` — the graph half of the hybrid query → [`skills/querying-the-graph/SKILL.md`](../querying-the-graph/SKILL.md)
- Open a database, run SQL, read `--json` → [`skills/using-contextdb/SKILL.md`](../using-contextdb/SKILL.md)
- Replicate embeddings across machines → [`skills/sync/SKILL.md`](../sync/SKILL.md)
