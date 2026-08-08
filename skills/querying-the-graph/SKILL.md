---
name: querying-the-graph
description: Declare a DAG edge table, traverse it with GRAPH_TABLE (paths, reachability, incoming/outgoing/variable-length), get cycle inserts refused, enforce STATE MACHINE transitions with PROPAGATE cascades, and combine graph + vector in one query.
---

# Querying the graph

contextdb's graph is not a bolt-on: edges live in an ordinary table, traversal is a native
`GRAPH_TABLE` operator over dedicated adjacency indexes (bounded BFS, not recursive SQL), and one
statement can filter relationally, walk the graph, and rank by vector similarity in the same
transaction. This skill is the one-stop recipe set for all of that — start here before grepping
`docs/query-language.md` yourself.

**Prerequisite:** needs the `contextdb` binary. In a checkout of this repo:
`cargo build --release -p contextdb-cli`. Other install options:
[`docs/getting-started.md`](../../docs/getting-started.md).

## Recipe 1 — declare a DAG edge table, insert edges, get a cycle refused

This is the block to reach for first: it is the entire "add a dependency graph" job in one paste,
including the refusal you need to know about before you rely on this table staying acyclic.

1. Design the edge table with `source_id`, `target_id`, `edge_type` columns (the shape
   `GRAPH_TABLE` requires) and a `DAG(...)` clause naming every edge type that must stay acyclic.
   Edge types you don't list in `DAG(...)` are not cycle-checked.
2. Run the `CREATE TABLE` + seed `INSERT`s below.
3. Validate: insert an edge that would close a cycle and confirm it is refused with
   `CycleDetected`, not silently accepted.
4. If the refusal names an edge type you did NOT expect to be checked, you added it to `DAG(...)`
   by mistake — go to step 1 and remove it, or embrace the constraint if it is actually correct
   for that edge type.

### Worked example 1 — dependency chain, cycle refused

```bash
contextdb ./graph.db <<'SQL'
CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE edges (
  id UUID PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
) DAG('DEPENDS_ON', 'BASED_ON');

INSERT INTO nodes (id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'root');
INSERT INTO nodes (id, name) VALUES ('22222222-2222-2222-2222-222222222222', 'mid');
INSERT INTO nodes (id, name) VALUES ('33333333-3333-3333-3333-333333333333', 'leaf');

INSERT INTO edges (id, source_id, target_id, edge_type)
  VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', 'DEPENDS_ON');
INSERT INTO edges (id, source_id, target_id, edge_type)
  VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333', 'DEPENDS_ON');
SQL
```

```text
ok (rows_affected=0)
ok (rows_affected=0)
ok (rows_affected=1)
ok (rows_affected=1)
ok (rows_affected=1)
ok (rows_affected=1)
ok (rows_affected=1)
```

Now close the loop (`leaf -> root`, which combined with the existing chain would be a cycle):

```bash
echo "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ('cccccccc-cccc-cccc-cccc-cccccccccccc', '33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111', 'DEPENDS_ON');" \
  | contextdb ./graph.db
```

```text
Error: cycle detected: inserting DEPENDS_ON edge from 33333333-3333-3333-3333-333333333333 to 11111111-1111-1111-1111-111111111111 would create a cycle
```

Exit code `1`. The row was never inserted — validate with `SELECT COUNT(*) FROM edges;` (still `2`,
not `3`).

### Worked example 2 — a duplicate edge is deduplicated, not refused

Don't confuse this with the cycle case: re-inserting the *same* `(source_id, target_id,
edge_type)` triple is accepted and silently deduplicated (`rows_affected=0`), because it can never
introduce a cycle that wasn't already possible.

```bash
echo "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ('dddddddd-dddd-dddd-dddd-dddddddddddd', '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', 'DEPENDS_ON');" \
  | contextdb ./graph.db --json
```

```json
{"rows_affected":0}
```

Exit code `0` — this is success, not a refusal. If you need to tell "deduplicated" apart from
"genuinely new edge", compare `SELECT COUNT(*) FROM edges;` before and after.

**If your insert fails with `CycleDetected` and you did NOT expect a cycle**, the graph is telling
you something true about the data you tried to write — do not retry with a different edge ID, the
refusal is about the `(source, target, edge_type)` relationship, not the row's identity.

## Recipe 2 — traverse with `GRAPH_TABLE`: reachability and paths

1. Wrap the `GRAPH_TABLE(...)` call in a `WITH` CTE — this is the blessed shape. `GRAPH_TABLE`
   results cannot be aliased and joined inline in the same `FROM` clause the way a normal subquery
   can; wrapping in `WITH` and joining the CTE always works.
2. Pattern shape: `MATCH (a)-[:EDGE_TYPE]->(b)` outgoing, `MATCH (a)<-[:EDGE_TYPE]-(b)` incoming,
   `MATCH (a)-[:EDGE_TYPE]-(b)` either direction, `MATCH (a)-[]->(b)` any edge type.
3. For more than one hop, add a bound: `-[:EDGE_TYPE]->{1,N}`. **The upper bound is mandatory** —
   an unbounded traversal is a parse error. The engine's own traversal ceiling is 10 hops.
4. Run the query, then validate the row set is exactly what you expect — an empty result from a
   graph query is easy to mistake for "the pattern is wrong" when the real cause is an edge type
   typo or a `DAG(...)` list that doesn't include the type you're querying (that only affects
   cycle-checking, not traversal, but it's a sign the schema and the query disagree).

### Worked example 1 — outgoing reachability (2 hops out)

Using the `graph.db` from Recipe 1 (root → mid → leaf via `DEPENDS_ON`):

```bash
cat <<'SQL' | contextdb ./graph.db --json
WITH reachable AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (a)-[:DEPENDS_ON]->{1,3}(b)
    WHERE a.id = '11111111-1111-1111-1111-111111111111'
    COLUMNS (b.id AS b_id)
  )
)
SELECT n.name FROM nodes n INNER JOIN reachable r ON r.b_id = n.id;
SQL
```

```json
[{"name":"mid"},{"name":"leaf"}]
```

### Worked example 2 — incoming edges: "what depends on this basis?"

```bash
contextdb ./basis.db <<'SQL'
CREATE TABLE decisions (id UUID PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('BASED_ON');
INSERT INTO decisions (id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'use-rds');
INSERT INTO decisions (id, name) VALUES ('22222222-2222-2222-2222-222222222222', 'managed-db-tier');
INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', 'BASED_ON');
SQL
```

`use-rds` is `BASED_ON -> managed-db-tier`. Ask the reverse question — "which decisions are based
on `managed-db-tier`?" — with an incoming-edge pattern, no need to know the direction the edge was
originally inserted in:

```bash
cat <<'SQL' | contextdb ./basis.db --json
WITH dependents AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (a)<-[:BASED_ON]-(b)
    WHERE a.id = '22222222-2222-2222-2222-222222222222'
    COLUMNS (b.id AS b_id)
  )
)
SELECT d.name FROM decisions d INNER JOIN dependents dep ON dep.b_id = d.id;
SQL
```

```json
[{"name":"use-rds"}]
```

**Validate a traversal that returns nothing** by checking the edge exists at all first:
`SELECT * FROM edges WHERE edge_type = 'BASED_ON';` — an empty `GRAPH_TABLE` result with rows
present in the edge table means the pattern's direction or hop bound is wrong, not that the graph
is empty. **If `GRAPH_TABLE(...)  AS alias` followed by `JOIN` fails to parse, go back to step 1**
— wrap it in `WITH` instead; that parse error is the tell.

## Recipe 3 — state machines: legal transitions accepted, illegal ones refused

1. Declare `STATE MACHINE (column: state -> [allowed, states], ...)` on the table. The first
   value written on `INSERT` is the row's initial state — it is not itself checked against the
   transition graph (there is nothing to transition from yet).
2. Run an `UPDATE` that matches a declared edge — confirm `rows_affected=1` and no error.
3. Run an `UPDATE` that does NOT match a declared edge — confirm the engine refuses it with
   `invalid state transition: <from> -> <to>` and the row is unchanged.
4. Validate by re-reading the row: the illegal attempt must show the pre-attempt state, not the
   attempted one.

### Worked example 1 — legal transition, then reading the result

```bash
contextdb ./decisions.db <<'SQL'
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL
) STATE MACHINE (status: active -> [invalidated, superseded]);
INSERT INTO tasks (id, status) VALUES ('11111111-1111-1111-1111-111111111111', 'active');
UPDATE tasks SET status='invalidated' WHERE id='11111111-1111-1111-1111-111111111111';
SELECT status FROM tasks WHERE id='11111111-1111-1111-1111-111111111111';
SQL
```

```text
ok (rows_affected=0)
ok (rows_affected=1)
ok (rows_affected=1)
+-------------+
| status      |
+-------------+
| invalidated |
+-------------+
```

### Worked example 2 — illegal transition refused, row unchanged

The state machine above declares no edge OUT of `invalidated` — so going back to `active` is
refused:

```bash
echo "UPDATE tasks SET status='active' WHERE id='11111111-1111-1111-1111-111111111111';" \
  | contextdb ./decisions.db --json
```

```json
{"error":{"class":"sql","line":1,"message":"invalid state transition: invalidated -> active"}}
```

Exit code `1`. Confirm the row didn't move: `SELECT status FROM tasks WHERE id = '...';` still
reads `invalidated`. **If an `UPDATE` you expected to be legal is refused, re-read the
`STATE MACHINE (...)` clause with `.schema tasks`** — `state_machine.transitions` in the `--json`
output is the literal transition graph the engine is enforcing, so compare it directly against
what you typed rather than guessing.

## Recipe 4 — `PROPAGATE ON EDGE`: cascade a transition along the graph

1. Declare `STATE MACHINE` and `PROPAGATE ON EDGE <edge_type> <direction> STATE <trigger> SET
   <target>` together on the table that owns the state. `direction` is `INCOMING`, `OUTGOING`, or
   `BOTH` — relative to the row that transitioned. Add `MAX DEPTH n` to bound the cascade and
   `ABORT ON FAILURE` if a failed propagation hop should roll back the whole transition.
2. Trigger the cascade by making the qualifying transition on the source row.
3. Validate by reading every row you expect to have cascaded, not just the one you updated.

### Worked example — invalidating a basis cascades to what cites it

```bash
contextdb ./cascade.db <<'SQL'
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL
) STATE MACHINE (status: active -> [invalidated, superseded])
  PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated;

CREATE TABLE cites_edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL);

INSERT INTO tasks (id, name, status) VALUES ('11111111-1111-1111-1111-111111111111', 'basis-decision', 'active');
INSERT INTO tasks (id, name, status) VALUES ('22222222-2222-2222-2222-222222222222', 'dependent-decision', 'active');
INSERT INTO cites_edges (id, source_id, target_id, edge_type)
  VALUES ('33333333-3333-3333-3333-333333333333', '22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111', 'CITES');

UPDATE tasks SET status='invalidated' WHERE id='11111111-1111-1111-1111-111111111111';
SELECT id, name, status FROM tasks ORDER BY name;
SQL
```

`dependent-decision`'s `CITES` edge points AT `basis-decision` — that's an *incoming* edge from
`basis-decision`'s point of view, matching `INCOMING` in the clause above. Both rows read
`invalidated` afterward:

```json
[{"id":"11111111-1111-1111-1111-111111111111","name":"basis-decision","status":"invalidated"},
 {"id":"22222222-2222-2222-2222-222222222222","name":"dependent-decision","status":"invalidated"}]
```

**If the cascade didn't reach a row you expected**, check the edge direction against the clause's
`INCOMING`/`OUTGOING`/`BOTH` first (the single most common miss), then check `MAX DEPTH` if one is
declared — a cascade past the declared depth stops silently by design, not as an error.

## Recipe 5 — hybrid graph + vector: narrow with the graph, rank with vectors

The full recipe (rank-inside-a-neighborhood, and the inverse similar-then-traverse ordering) with
worked examples and expected output lives in
[`skills/vector-search/SKILL.md`](../vector-search/SKILL.md#the-hybrid-query--graph-narrows-vector-ranks)
— it needs `VECTOR(n)` columns and the `<=>` operator, which belong to that skill. Come back here
for the graph half; go there for the combined query.

## Gotchas

- **`GRAPH_TABLE(...)` cannot be aliased and joined inline** (`FROM GRAPH_TABLE(...) AS r JOIN
  ...` is a parse error). Always wrap it in a `WITH` CTE first — every worked example above does
  this.
- **The upper bound on a variable-length path is mandatory** (`-[:EDGE]->{1,N}`), and the engine
  refuses to traverse past depth 10 even if you ask for more.
- **Only edge types listed in `DAG(...)` are cycle-checked.** An edge table with no `DAG(...)`
  clause accepts cycles freely — that's sometimes what you want (a citation graph, say), so it is
  not a defect, but it means "I declared `DAG` but a cycle still landed" is always a sign the edge
  type isn't in the list, not an engine bug.
- **A duplicate `(source_id, target_id, edge_type)` insert is a silent no-op success
  (`rows_affected=0`), not a refusal** — don't mistake `0` rows affected for a failed insert.
- **`PROPAGATE ON EDGE` cascades are evaluated against the state machine's own transition rules at
  each hop** — a cascade that would set a downstream row to a state its own `STATE MACHINE` clause
  doesn't allow is refused like any other illegal transition, which can abort the whole statement
  under `ABORT ON FAILURE`.

## Depth

- Full grammar for `GRAPH_TABLE`, patterns, and variable-length paths: [`docs/query-language.md`](../../docs/query-language.md#graph-traversal)
- `DAG`, `STATE MACHINE`, `PROPAGATE ON EDGE`, `PROPAGATE ON STATE ... EXCLUDE VECTOR` table options: [`docs/query-language.md`](../../docs/query-language.md#table-options)
- Graph adjacency probe / index-selection trace: [`docs/query-language.md`](../../docs/query-language.md#graph-adjacency-probe)

## Next

- Open a database, run SQL, read `--json`, branch on exit codes → [`skills/using-contextdb/SKILL.md`](../using-contextdb/SKILL.md)
- Similarity search and the hybrid query in full → [`skills/vector-search/SKILL.md`](../vector-search/SKILL.md)
- Replicate this graph across machines → [`skills/sync/SKILL.md`](../sync/SKILL.md)
