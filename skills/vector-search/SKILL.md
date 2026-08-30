---
name: vector-search
description: Similarity search in contextdb — embedding columns, the <=> operator, pre-filtered search, schema-declared USE RANK policies, and the hybrid graph + vector query.
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
  category TEXT,
  vector_text VECTOR(4),
  vector_vision VECTOR(8) WITH (quantization = 'SQ8')
);
SHOW VECTOR_INDEXES;
SQL
```

Each `VECTOR(n)` column is its own named index, keyed by `(table, column)` — a row can carry a text
embedding and a vision embedding side by side, searched independently. `SHOW VECTOR_INDEXES`
reports `table`, `column`, `dimension`, `quantization`, `vector_count` and `bytes`.

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

## Indexing is automatic

No index to create, no rebuild to schedule. The engine picks the strategy from the vector count:

- below ~1000 vectors — brute-force linear scan (exact)
- `F32` at/above ~1000 — HNSW approximate nearest neighbours (recall target ≥ 95%)
- `SQ8`/`SQ4` through 5000 — exact scan, to preserve self-recall; larger quantized indexes use HNSW

Check which one is live:

```bash
printf ".explain SELECT id FROM evidence ORDER BY vector_text <=> [1.0, 0.0, 0.0, 0.0] LIMIT 5\n" \
  | contextdb ./vec.db --json | jq -r '.explain.physical_plan'
```

Brute force reads `Scan -> VectorSearch`; once the index switches to HNSW the same line reads
`Scan -> HNSWSearch`.

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

## Gotchas

- **`LIMIT` is mandatory** on any `<=>` ordering.
- **Dimensions must match** the column's declared `VECTOR(n)`, or you get
  `VectorIndexDimensionMismatch`.
- **Search routes to the column named in `ORDER BY`**, not to "the table's vector" — a two-vector
  table has two independent indexes.
- **Opening a pre-0.3.4 store** without the named-index format marker (or any older store whose
  row/column schema layout predates the current release) returns `LegacyVectorStoreDetected`,
  naming the recovery command: `contextdb migrate <path>` migrates it in place (backs up first,
  never destroys the original). Syncing from a peer already on the current format, or recreating
  the schema and reimporting, remain alternatives if you'd rather not migrate the file directly.
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
