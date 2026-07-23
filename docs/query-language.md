# Query Language

contextdb's query language is built on three standards:

- **PostgreSQL-compatible SQL** — DDL, DML, expressions, operators, JOINs, CTEs, `ON CONFLICT DO UPDATE`, `$param` binding
- **pgvector conventions** — `<=>` operator for cosine similarity in `ORDER BY`
- **SQL/PGQ-style graph queries** — `GRAPH_TABLE(... MATCH ...)` following SQL/PGQ conventions for bounded graph traversal (not a full standard implementation)

On top of these, contextdb adds **declarative policy primitives**: `IMMUTABLE`, `STATE MACHINE`, `DAG`, `RETAIN`, `SYNC`, and `PROPAGATE`. These are contextdb-specific extensions — everything else should feel familiar if you've used PostgreSQL.

contextdb ships with no built-in tables or schema. You define your own tables and attach these primitives to whichever columns need them. The example tables in this reference (`documents`, `tasks`, `items`, `media`, and so on) are illustrative only — stand-ins for whatever schema you design, not something contextdb provides.

All examples work in the Rust API via `db.execute(sql, &params)` where parameters are passed as `HashMap<String, Value>`. The CLI REPL does not support parameter binding (`$param`) — use literal values directly. Vector search works in the CLI using vector literals: `ORDER BY embedding <=> [0.1, 0.2, 0.3] LIMIT 5`.

---

## Statements

### CREATE TABLE

`documents` here is an example table you might define — there is no built-in schema; name your own tables and columns:

```sql
CREATE TABLE documents (
  id UUID PRIMARY KEY,
  data JSON,
  embedding VECTOR(384),
  bucket TEXT,
  recorded_at TIMESTAMP DEFAULT NOW()
) IMMUTABLE
```

See [Table Options](#table-options) for IMMUTABLE, STATE MACHINE, DAG, RETAIN, SYNC, and PROPAGATE.

Later examples in this reference also use a second illustrative table, `items`:

```sql
CREATE TABLE items (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  item_type TEXT,
  context_id UUID,
  is_deprecated BOOLEAN DEFAULT FALSE,
  embedding VECTOR(384),
  created_at TIMESTAMP DEFAULT NOW()
)
```

### ALTER TABLE

```sql
ALTER TABLE t ADD [COLUMN] col TYPE
ALTER TABLE t DROP [COLUMN] col
ALTER TABLE t RENAME COLUMN old TO new
ALTER TABLE t SET RETAIN 7 DAYS [SYNC SAFE]
ALTER TABLE t DROP RETAIN
ALTER TABLE t SET SYNC OFF | SYNC PUSH ONLY | SYNC PULL ONLY | SYNC TWO WAY
```

A table's conflict policy is declared on the table itself with the
`SYNC CONFLICT KEEP FIRST | KEEP LATEST` clause (see CREATE TABLE), not set
through a separate statement.

### DROP TABLE

```sql
DROP TABLE t
```

### CREATE TRIGGER

```sql
CREATE TRIGGER document_seen ON documents WHEN INSERT;
CREATE TRIGGER item_changed ON items WHEN UPDATE;
DROP TRIGGER document_seen;
```

`CREATE TRIGGER` declares a host-callback Trigger. The callback is
registered through the Rust API with `Database::register_trigger_callback` and
activated by `Database::complete_initialization`. Triggers are for
transactional observation and cascade writes; they are not validation triggers.
Use `STATE MACHINE`, `IMMUTABLE`, `DAG`, and `PROPAGATE` for engine-enforced
invariants.

Concurrency: the callback runs inside the firing transaction. Same-DB
cross-thread writers wait-and-proceed, unrelated databases proceed
independently, callback-thread reentry receives `CallbackReentry`, callback
tx-bound handles stay isolated to the runner thread, cron same-DB contention
remains immediate, and the bounded deadlock guard emits a structured
`tracing::warn!` with `trigger_name`, `waited_ms`, and `surface`.
See [Architecture](architecture.md#trigger-concurrency) for the operator
runbook.

### CREATE INDEX

```sql
CREATE INDEX idx_name ON t (col)
```

### INSERT

```sql
INSERT INTO documents (id, data, embedding)
VALUES ($id, $data, $embedding)

-- Multiple rows
INSERT INTO items (id, name) VALUES ($id1, $name1), ($id2, $name2)

-- Upsert
INSERT INTO items (id, name) VALUES ($id, $name)
ON CONFLICT (id) DO UPDATE SET name = $name
```

### UPDATE / DELETE

```sql
UPDATE tasks SET status = 'superseded' WHERE id = $id
DELETE FROM scratch WHERE created_at < $cutoff
```

### SELECT

```sql
SELECT [DISTINCT] columns FROM table
  [INNER JOIN | LEFT JOIN other ON condition]
  [WHERE condition]
  [ORDER BY col [ASC|DESC]]
  [USE RANK sort_key]
  [LIMIT n]
```

### CTEs

```sql
WITH active AS (
  SELECT id, name FROM items WHERE status = 'active'
)
SELECT * FROM active WHERE name LIKE 'sensor%'
```

Multiple CTEs via comma separation. Non-recursive only.

### Anchor-Shape vs List-Shape Reads

Scoped handles filter ordinary reads to the rows visible to that handle. List
shapes keep this silent-filter behavior: full scans, non-unique predicates,
ranges, `IN (...)` predicates even on primary keys, ordered `LIMIT 1`, and
sort-elided index walks return `Ok` with only visible rows.

Explicit-anchor `SELECT` shapes are different. If a constrained handle names a
specific row identity through equality on a primary key, a single-column
`UNIQUE`, or every column of a composite `UNIQUE`, and the row exists but is
hidden by the handle, the query returns the typed visibility error from the
gate that hid it: `ContextScopeViolation`, `ScopeLabelViolation`, or
`AclDenied`. Missing rows still return an empty result.

Predicate writes keep their existing contract. `UPDATE ... WHERE pk = $id` and
`DELETE ... WHERE pk = $id` against a hidden row return
`Ok(rows_affected = 0)` rather than a typed read-refusal error.

### Transactions

```sql
BEGIN;
-- statements
COMMIT;

-- or
ROLLBACK;
```

### Configuration

```sql
SHOW SYNC_CONFLICT_POLICY;
SET MEMORY_LIMIT '512M';
SHOW MEMORY_LIMIT;
SET DISK_LIMIT '1G';
SET DISK_LIMIT 'none';
SHOW DISK_LIMIT;
```

`SHOW MEMORY_LIMIT` returns `limit`, `used`, `available`, and `startup_ceiling`.

`SHOW DISK_LIMIT` returns the same columns for file-backed storage. On `:memory:` databases, disk limit commands are accepted but ignored.

---

## Column Types

| Type | Description | Example |
|------|-------------|---------|
| `INTEGER` / `INT` | 64-bit signed integer | `42` |
| `REAL` / `FLOAT` | 64-bit floating point | `3.14` |
| `TEXT` | UTF-8 string | `'hello'` |
| `BOOLEAN` / `BOOL` | Boolean | `TRUE`, `FALSE` |
| `UUID` | 128-bit UUID | `'550e8400-e29b-41d4-a716-446655440000'` |
| `TIMESTAMP` | Stored as a Unix timestamp (`Value::Timestamp(i64)`); ISO 8601 text literals are also accepted on input | `NOW()` |
| `JSON` | JSON value | `'{"key": "value"}'` |
| `VECTOR(n)` | Fixed-dimension float vector | `[0.1, 0.2, 0.3]` |
| TXID | Engine-issued transaction id (`Value::TxId`). Populate only via the library API with a bound parameter; SQL literals are rejected. Sync-apply advances the local TxId allocator past incoming peer values. | `Value::TxId(tx.id())` |

NULL values display as `NULL`. Vectors display as `[0.1, 0.2, ...]`.

Vector columns can choose per-index scalar quantization:

```sql
CREATE TABLE media (
  id UUID PRIMARY KEY,
  source TEXT,
  vector_text VECTOR(8) WITH (quantization = 'SQ8'),
  vector_vision VECTOR(4) WITH (quantization = 'SQ4')
);
```

Valid quantization values are `F32`, `SQ8`, and `SQ4`; the default is `F32`.
Each vector column is a separate `(table, column)` index. Search routes to the
column named in `ORDER BY`:

```sql
SELECT id FROM media
WHERE source = 'camera-1'
ORDER BY vector_vision <=> '[0,0,1,0]' LIMIT 5;
```

Inspect registered vector indexes with:

```sql
SHOW VECTOR_INDEXES;
```

It returns `table`, `column`, `dimension`, `quantization`, `vector_count`, and
`bytes`.

### Rank Policies

A `VECTOR(n)` column can declare a rank policy that combines raw cosine
similarity with typed columns from the anchor row and one indexed joined row.
The policy is attached to the specific `(table, vector_column)` index. Because
the policy lives in schema, every caller that asks for the same `SORT_KEY` gets
the same ranking behavior; applications do not need to copy formula text into
each query.

Grammar shape:

```sql
<column> VECTOR(n)
  [WITH (quantization = 'F32' | 'SQ8' | 'SQ4')]
  RANK_POLICY (
    JOIN <joined_table> ON <joined_indexed_column>,
    FORMULA '<rank expression>',
    SORT_KEY <identifier>
  )
```

```sql
CREATE TABLE task_results (
  id UUID PRIMARY KEY,
  task_id UUID NOT NULL,
  success BOOLEAN NOT NULL
);

CREATE INDEX task_results_task_id_idx ON task_results(task_id);

CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  confidence REAL,
  embedding VECTOR(384) RANK_POLICY (
    JOIN task_results ON task_id,
    FORMULA 'coalesce({confidence}, 1.0) * coalesce({success}, 1.0)',
    SORT_KEY effective_confidence
  )
);
```

Use the policy during vector search with `USE RANK <sort_key>`:

```sql
SELECT id, description
FROM tasks
ORDER BY embedding <=> $query USE RANK effective_confidence
LIMIT 10;
```

Without `USE RANK`, the same query returns cosine ordering:

```sql
SELECT id, description
FROM tasks
ORDER BY embedding <=> $query
LIMIT 3;

-- id   description        vector_score
-- d1   closest match      1.0
-- d2   near match         0.75
-- d3   weaker match       0.5
```

With the policy, ordering uses the formula before the final `LIMIT`:

```sql
SELECT id, description
FROM tasks
ORDER BY embedding <=> $query USE RANK effective_confidence
LIMIT 3;

-- id   description        vector_score   rank
-- d2   near match         0.75           1.0
-- d3   weaker match       0.5            0.5
-- d1   closest match      1.0            0.0
```

`USE RANK` requires the vector `ORDER BY ... <=> ...` and `LIMIT` in the same
query. Unknown sort keys return `RankPolicyNotFound`; there is no silent
fallback to cosine ordering.

Formula references use `{column}`. Supported operands are `REAL`, `INTEGER`,
`BOOLEAN`, numeric literals, `{vector_score}`, `+`, `*`, parentheses, and
`coalesce(expr, literal)`. `BOOLEAN` values coerce only inside the rank
formula (`TRUE` = `1.0`, `FALSE` = `0.0`). `TEXT`, `JSON`, `VECTOR`, dotted
refs, subqueries, `CASE`, subtraction, division, and arbitrary function calls
are rejected at DDL time. `*` binds tighter than `+`; parentheses override
precedence.

`JOIN table ON column` is a single left-outer lookup through an existing index
on the joined table. The joined column is resolved to an anchor-side join column
at DDL time and the protected joined-table index is used at search time.
Candidates with no joined row remain eligible; joined columns evaluate as
`NULL`, so `coalesce` can provide a fallback. Dropping the joined table, joined
column, resolved anchor join column, formula-referenced columns, or the
protected join index is refused while the rank policy depends on it.

Anchor-side join-column resolution is deterministic. If the joined column is
the joined table's primary key, ContextDB first looks for
`<singular_joined_table>_id` or `<joined_table>_id` on the anchor table, then
falls back to a same-named anchor column, then the anchor primary key. For other
joined columns, ContextDB uses a same-named anchor column when present,
otherwise the anchor primary key. Ambiguous inferred anchor columns are rejected
at DDL time.

`min_similarity` applies to raw cosine before the rank formula runs. A candidate
below the similarity floor is excluded even if its joined-row metric would have
made the formula large.

Current limits to account for in production designs:

- On large HNSW-backed vector indexes, rank policies rank the ANN candidate set
  returned by vector retrieval before applying the final top-k. They do not
  force an exhaustive scan of every row in the corpus. If a formula does not
  reference `{vector_score}`, use a larger search limit or an exact workflow
  when cosine is a weak candidate generator for the metric being optimized.
- If more than one joined row matches a candidate, the current policy uses one
  matched row, chosen by highest internal `RowId`. Model joined data as a
  single current summary row when ranking semantics need to be stable.
- Formula `{column}` references resolve anchor-row columns before joined-row
  columns. The reserved `{vector_score}` name cannot be shadowed, and an `id`
  present on both sides is rejected as ambiguous. Avoid duplicate formula
  column names until table-qualified references exist.
- `JOIN table ON column` is rank-policy lookup syntax, not arbitrary SQL join
  syntax. It does not support predicates, composite joins, aggregation, or
  ordering.
- The formula language is intentionally closed for safety. Functions such as
  `min`, `max`, `clamp`, time decay, division, and subtraction are not part of
  the current surface.
- Sync currently round-trips rank policies through rendered DDL text. Structured
  policy replication is a future hardening item.

---

## Column Constraints

`projects` is another illustrative table, referenced below by a foreign key:

```sql
CREATE TABLE projects (id UUID PRIMARY KEY, name TEXT NOT NULL)
```

```sql
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL DEFAULT 0.0,
  email TEXT UNIQUE,
  project_id UUID REFERENCES projects(id)
)
```

Table-level composite foreign keys are supported when the referenced parent
tuple is covered by an ordered `PRIMARY KEY` or `UNIQUE` constraint:

```sql
CREATE TABLE parent (tenant INTEGER, number INTEGER, UNIQUE(tenant, number));
CREATE TABLE child (
  id INTEGER PRIMARY KEY,
  tenant INTEGER,
  number INTEGER,
  FOREIGN KEY (tenant, number) REFERENCES parent(tenant, number)
);
```

| Constraint | Description |
|------------|-------------|
| `PRIMARY KEY` | Unique row identifier |
| `NOT NULL` | Value required |
| `UNIQUE` | No duplicate values (single column). A duplicate INSERT on a `UNIQUE` column is a silent no-op (returns `Ok(rows_affected=0)`), matching the composite-uniqueness contract. |
| `DEFAULT expr` | Default value for inserts |
| `REFERENCES table(col)` / `FOREIGN KEY (...) REFERENCES ...` | Foreign key — writes are rejected if the referenced row or tuple does not exist; in explicit transactions the error may surface at `COMMIT` |
| `IMMUTABLE` | Column is audit-frozen — INSERT sets the value once; `UPDATE`, `ON CONFLICT DO UPDATE`, sync-apply mutations, and schema-altering DDL against the column are rejected with `Error::ImmutableColumn` |

### Audit-Frozen Columns

An audit-frozen column carries data that must not be silently rewritten by anyone, through any path. Declare it with `IMMUTABLE`:

```sql
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  task_type TEXT NOT NULL IMMUTABLE,
  description TEXT NOT NULL IMMUTABLE,
  reasoning JSON,
  confidence REAL,
  status TEXT NOT NULL DEFAULT 'active'
) STATE MACHINE (status: active -> [superseded, archived])
```

`task_type` and `description` are provenance — set once at INSERT and never rewritten. `status` and `confidence` remain mutable. An `UPDATE tasks SET task_type = '…'` returns `Error::ImmutableColumn`; the row is unchanged. Sync-apply across a synced edge enforces the same rule on the peer: incoming row-changes that mutate a flagged column are rejected and surface in `ApplyResult.conflicts`. `ALTER TABLE ... DROP COLUMN`, `RENAME COLUMN`, and column-type-altering ALTER against a flagged column are refused.

Correction without rewrite — the supersede pattern. When a recorded row turns out to be wrong, insert a new row with the corrected values and mark the original `superseded`:

```sql
-- Original (frozen)
INSERT INTO tasks (id, task_type, description, status)
VALUES ('11111111-1111-4111-8111-111111111111', 'sql-migration', 'migrate to contextdb', 'active');

-- Correction: a new row, not an update. Both rows remain queryable.
INSERT INTO tasks (id, task_type, description, status)
VALUES ('22222222-2222-4222-8222-222222222222', 'sql-migration', 'migrate to contextdb (rev 2)', 'active');
UPDATE tasks SET status = 'superseded' WHERE id = '11111111-1111-4111-8111-111111111111';
```

Nothing disappears. The audit trail shows both the original commitment and its correction.

### Composite Uniqueness

Enforce uniqueness across a combination of columns using a table-level constraint:

```sql
CREATE TABLE edges (
  id UUID PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL,
  UNIQUE(source_id, target_id, edge_type)
)
```

A duplicate `(source_id, target_id, edge_type)` tuple is a silent no-op — the second INSERT returns `Ok(rows_affected=0)` and the row count is unchanged, making agent operations idempotent. Rows that share individual column values but differ in at least one constrained column are allowed. Rows with `NULL` in any constrained column do not participate in the composite uniqueness check.

### Composite Primary Key

When a row's identity is a combination of columns — not a single surrogate key — declare it with a table-level `PRIMARY KEY (col, col, ...)`:

```sql
CREATE TABLE metric_windows (
  machine_id TEXT NOT NULL,
  sensor_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  value INTEGER NOT NULL,
  PRIMARY KEY (machine_id, sensor_id, metric, window_start)
)
```

The whole declared tuple is the row's identity everywhere it matters, including across synchronization: two machines writing the same `(machine_id, sensor_id, metric, window_start)` are the same row, and two rows differing in only one key column — even a later one such as `window_start` — are distinct rows and both survive a sync. Re-declaring the identical tuple is a duplicate and is refused. The columns are matched in the order declared, and the declaration round-trips through `.schema` and travels with synced DDL to a receiving machine.

Use a table-level `PRIMARY KEY (...)` for a multi-column key only; a single-column key stays the column-level `id UUID PRIMARY KEY` form. A table-level `PRIMARY KEY (...)` cannot be combined with a column-level `PRIMARY KEY`, and its columns must exist and be exact-matchable (not `REAL`, `JSON`, or `VECTOR`). A table that promises cross-machine delivery (`RETAIN ... SYNC SAFE`) satisfies its key requirement with a multi-column primary key just as it would with a single-column one. A composite `UNIQUE` constraint or a multi-column index is a uniqueness or lookup rule, never the sync identity — only `PRIMARY KEY` is.

### Foreign Key State Propagation

Trigger a state change on this row when the referenced row transitions:

```sql
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  project_id UUID REFERENCES projects(id)
    ON STATE archived PROPAGATE SET invalidated
) STATE MACHINE (status: active -> [invalidated, superseded])
```

When a `projects` row transitions to `archived`, any `tasks` row referencing it transitions to `invalidated`.

---

## Table Options

Table options appear after the closing `)` of the column list. Multiple options can be combined. They attach to whatever table and columns you define — contextdb has no built-in tables of its own.

### IMMUTABLE

Rows cannot be updated or deleted after insertion. Useful for append-only data like event logs and audit trails:

```sql
CREATE TABLE documents (
  id UUID PRIMARY KEY,
  data JSON,
  embedding VECTOR(384)
) IMMUTABLE
```

### STATE MACHINE

Restrict a column's value transitions to declared edges:

```sql
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL
) STATE MACHINE (status: draft -> [active, rejected], active -> [superseded])
```

Inserting a row sets the initial state. Updates that violate the transition graph are rejected.

### DAG

Enforce directed acyclic graph constraint on specified edge types, preventing cycles:

```sql
CREATE TABLE edges (
  id UUID PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
) DAG('DEPENDS_ON', 'BASED_ON')
```

Inserting an edge that would create a cycle returns `CycleDetected`. Duplicate `(source_id, target_id, edge_type)` inserts are silently deduplicated.

### RETAIN

Automatic row expiry. Units: `SECONDS`, `MINUTES`, `HOURS`, `DAYS`. Optional `SYNC SAFE` means a row is not expired here until the destination confirms it received it:

```sql
CREATE TABLE scratch (
  id UUID PRIMARY KEY,
  data TEXT
) RETAIN 24 HOURS SYNC SAFE
```

Can also be set via ALTER TABLE:

```sql
ALTER TABLE scratch SET RETAIN 7 DAYS;
ALTER TABLE scratch DROP RETAIN;
```

`RETAIN` says only WHEN rows expire and `SYNC SAFE` says only that expiry waits on delivery. Neither decides where rows travel — that is the separate `SYNC` clause below. Writing a direction inside the retention clause (`RETAIN 24 HOURS SYNC SAFE PUSH ONLY`) is a parse error naming the clause to use instead.

### SYNC

Where a table's rows travel under synchronization. It applies to retained and non-retained tables alike, persists with the table definition, renders in `.schema`, and travels with the definition to other machines:

```sql
CREATE TABLE windows (
  id UUID PRIMARY KEY,
  body TEXT
) RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY
```

| Clause | Meaning |
|---|---|
| `SYNC OFF` | rows stay on the machine that wrote them |
| `SYNC PUSH ONLY` | rows go out and never come back |
| `SYNC PULL ONLY` | rows arrive from other machines; nothing local goes out |
| `SYNC TWO WAY` | rows travel both ways |

A table that declares no direction is `SYNC TWO WAY` — the default, and the recovery contract: delete a machine's database, recreate it against the same tenant, and every still-live row comes back.

Changeable on an existing table — but `windows` is `SYNC SAFE`, and `SYNC SAFE` combined with `SYNC PULL ONLY` can never deliver the table outward for a destination to confirm, so this is refused (see below):

```sql
ALTER TABLE windows SET SYNC PULL ONLY;
```

On a table without `SYNC SAFE`, the same direction change is accepted:

```sql
ALTER TABLE items SET SYNC PULL ONLY;
```

`SYNC SAFE` with a direction that never delivers the table (`SYNC OFF` or `SYNC PULL ONLY`) is refused when it is written — at `CREATE`, at `ALTER`, and when the definition arrives from another machine. The promise could never be kept, so the rows would simply never expire. Plain `RETAIN` with no delivery promise may declare any direction, including `SYNC OFF` for a colocated installation that keeps one copy.

### SYNC CONFLICT

Which value survives when the same row is written on more than one machine, or the same key is re-sent. Like the direction clause, it is declared on the table itself, persists with the definition, renders in `.schema`, and travels with the synced definition to other machines, so the durable hub honors the table's declared policy:

```sql
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  body TEXT
) SYNC CONFLICT KEEP FIRST
```

| Clause | Meaning |
|---|---|
| `SYNC CONFLICT KEEP FIRST` | write-once — the first value written for a key stays; a re-send of that key does not overwrite it |
| `SYNC CONFLICT KEEP LATEST` | last-writer-wins — a re-send of a key replaces the value already there |

A table that declares no policy is `SYNC CONFLICT KEEP FIRST` — the non-overwriting default, so a re-send of an existing key never silently rewrites it.

On a hub the policy is resolved in one order: a system table's baked policy wins first, then the table's own declaration, then the default. So an application table always gets exactly the policy it declared, while the engine's own distributed tables keep the policy their contract requires.

The policy composes with the retention and direction clauses on one table:

```sql
CREATE TABLE windows (
  id UUID PRIMARY KEY,
  body TEXT
) RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY SYNC CONFLICT KEEP FIRST
```

### PROPAGATE ON EDGE

Cascade state changes along graph edges when a row transitions:

```sql
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL
) STATE MACHINE (status: active -> [invalidated, superseded])
  PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated
```

When a `tasks` row transitions to `invalidated`, rows connected via incoming `CITES` edges also transition to `invalidated`. Options: `INCOMING`, `OUTGOING`, `BOTH` for edge direction. `MAX DEPTH n` limits traversal. `ABORT ON FAILURE` rolls back if any propagation fails.

### PROPAGATE ON STATE ... EXCLUDE VECTOR

Remove a row's vector from similarity search results when it enters a given state, without deleting the row:

```sql
CREATE TABLE tasks (...)
  PROPAGATE ON STATE invalidated EXCLUDE VECTOR
  PROPAGATE ON STATE superseded EXCLUDE VECTOR
```

### Combining Options

Options compose — a real-world table might use several:

```sql
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL,
  created_at TIMESTAMP DEFAULT NOW(),
  project_id UUID REFERENCES projects(id)
    ON STATE archived PROPAGATE SET invalidated,
  embedding VECTOR(384)
) STATE MACHINE (status: active -> [invalidated, superseded])
  PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated
  PROPAGATE ON STATE invalidated EXCLUDE VECTOR
  PROPAGATE ON STATE superseded EXCLUDE VECTOR
```

---

## Expressions and Operators

### Comparison

`=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`

### Logical

`AND`, `OR`, `NOT`

### Arithmetic

`+`, `-`, `*`, `/`

### Pattern Matching

```sql
WHERE name LIKE 'sensor%'       -- % matches any substring
WHERE name LIKE 'item_3'        -- _ matches single character
WHERE name NOT LIKE '%draft%'
```

### Range

```sql
WHERE confidence BETWEEN 0.5 AND 1.0
WHERE confidence NOT BETWEEN 0 AND 0.1
```

### Set Membership

```sql
WHERE status IN ('active', 'draft')
WHERE id IN (SELECT id FROM other_table WHERE ...)
WHERE status NOT IN ('deleted', 'archived')
```

Subqueries in `IN` must select exactly one column.

### NULL Checks

```sql
WHERE superseded_at IS NULL
WHERE embedding IS NOT NULL
```

---

## Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `COUNT(*)` | INTEGER | Count all rows |
| `COUNT(col)` | INTEGER | Count non-NULL values in column |
| `COALESCE(a, b, ...)` | varies | First non-NULL argument |
| `NOW()` | TIMESTAMP | Current Unix timestamp |

COUNT operates over the entire result set. No GROUP BY or HAVING — use CTEs or application-level grouping for aggregation.

---

## Parameter Binding

In the Rust API, parameters are passed as `HashMap<String, Value>`:

```rust
let mut params = HashMap::new();
params.insert("item_id".into(), Value::Uuid(id));
params.insert("type".into(), Value::Text("sensor".into()));

let result = db.execute(
    "SELECT * FROM items WHERE id = $item_id AND type = $type",
    &params,
)?;
```

The CLI does not support parameter binding — use literal values directly.

---

## Graph Traversal

Graph queries use `GRAPH_TABLE` in the FROM clause with openCypher-subset `MATCH` patterns. The graph executor uses dedicated adjacency indexes and bounded BFS — graph traversal is a native operator, not recursive SQL.

### Syntax

```sql
SELECT columns FROM GRAPH_TABLE(
  edge_table
  MATCH pattern
  [WHERE condition]
  COLUMNS (expr AS alias, ...)
)
```

The `edge_table` is a table with `source_id`, `target_id`, and `edge_type` columns.

### Patterns

```sql
-- Outgoing edges
MATCH (a)-[:DEPENDS_ON]->(b)

-- Incoming edges
MATCH (a)<-[:BASED_ON]-(b)

-- Bidirectional
MATCH (a)-[:RELATES_TO]-(b)

-- Any edge type
MATCH (a)-[]->(b)
```

### Variable-Length Paths

```sql
-- Between 1 and 3 hops
MATCH (a)-[:DEPENDS_ON]->{1,3}(b)

-- 1 to 10 hops (explicit upper bound required)
MATCH (a)-[:EDGE]->{1,10}(b)
```

An explicit upper bound is always required. Maximum traversal depth enforced by the engine is 10.

### Filtering and Projection

Use WHERE to filter after traversal, COLUMNS to project results:

```sql
SELECT target_id FROM GRAPH_TABLE(
  edges
  MATCH (a)-[:DEPENDS_ON]->{1,3}(b)
  WHERE a.id = '550e8400-e29b-41d4-a716-446655440000'
  COLUMNS (b.id AS target_id)
)
```

### Composing with SQL via CTEs

Graph results become a relational CTE for joins, filters, or vector search:

```sql
WITH deps AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (a)-[:DEPENDS_ON]->{1,3}(b)
    WHERE a.id = $start
    COLUMNS (b.id AS b_id)
  )
)
SELECT t.id, t.status FROM tasks t
INNER JOIN deps ON t.id = deps.b_id
WHERE t.status = 'active'
```

### Graph + Vector: Neighborhood Similarity Search

Find semantically similar items within a graph neighborhood:

```sql
WITH neighborhood AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (a)-[:RELATES_TO]->{1,2}(b)
    COLUMNS (b.id AS b_id)
  )
),
candidates AS (
  SELECT id, name, embedding
  FROM items i
  INNER JOIN neighborhood n ON i.id = n.b_id
  WHERE i.is_deprecated = FALSE
)
SELECT id, name FROM candidates
ORDER BY embedding <=> $query
LIMIT 5
```

---

## Vector Similarity Search

### The `<=>` Operator

Cosine distance between two vectors. Used in ORDER BY for nearest-neighbor search:

```sql
-- Rust API with parameter binding
SELECT id, data FROM documents
ORDER BY embedding <=> $query_vector
LIMIT 10

-- CLI with vector literal (against a table with a short vector dimension,
-- so the literal can be written out in full)
CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4));

SELECT id FROM evidence
ORDER BY vector_text <=> [0.1, 0.2, 0.3, 0.4]
LIMIT 10
```

Lower distance = more similar. A `LIMIT` clause is required — unbounded vector searches are rejected.

The query vector can also come from an existing row:

```sql
SELECT id, data FROM documents
WHERE id != $query_id
ORDER BY embedding <=> ROW_VECTOR('documents', 'embedding', $query_id)
LIMIT 10
```

`ROW_VECTOR(table, column, key)` is only valid as the right side of `<=>` in
`ORDER BY`. The table and column arguments are string literals naming a
`VECTOR(n)` column, and `key` is a literal or parameter matched against the
source table's natural key, usually its primary key. The source vector is read
from the same MVCC snapshot as candidate filtering and vector scoring. Scoped
handles honor source-row visibility: a hidden source row returns the same typed
read-scope error as an explicit anchor read. Missing source tables return
`TableNotFound`, non-vector source columns return `UnknownVectorIndex`,
dimension mismatches return `VectorIndexDimensionMismatch`, missing source rows
return `PersistedRowVectorRowMissing`, and rows with NULL vector cells return
`PersistedRowVectorCellNull`.

### Pre-Filtered Search

Combine WHERE filters with vector ranking. The engine filters first, then scores only matching rows:

```sql
SELECT id, description FROM tasks
WHERE status = 'active'
ORDER BY embedding <=> $query
LIMIT 5
```

### Indexing

The engine automatically selects the search strategy based on vector count:

- Below ~1000 vectors: brute-force linear scan (exact)
- F32 at/above ~1000 vectors: HNSW approximate nearest neighbors (recall target >= 95%)
- SQ8/SQ4 through 5000 vectors: exact scan to preserve self-recall; larger quantized indexes use HNSW

No manual index creation needed. Use `.explain` in the CLI to see which strategy is active:

```
contextdb> .explain SELECT id FROM documents ORDER BY embedding <=> $q LIMIT 5
Scan -> VectorSearch
```

Below the HNSW threshold the vector search is brute-force (`Scan -> VectorSearch`); once the index switches to HNSW the same line reads `Scan -> HNSWSearch`.

---

## SQL Comments

Both styles are stripped before parsing:

```sql
-- Line comment
SELECT * FROM items; /* Block comment */
```

---

## Unsupported Features

These are explicitly rejected with descriptive error messages:

| Feature | Error |
|---------|-------|
| `WITH RECURSIVE` | `RecursiveCteNotSupported` |
| Window functions (`OVER`) | `WindowFunctionNotSupported` |
| `CREATE PROCEDURE` / `CREATE FUNCTION` | `StoredProcNotSupported` |
| Full-text search (`WHERE col MATCH pattern`) | `FullTextSearchNotSupported` |
| `GROUP BY` / `HAVING` | Not supported |
| `UNION` / `INTERSECT` / `EXCEPT` | Not supported |
| `INSERT ... SELECT` | Not supported |
| Subqueries outside `IN` | `SubqueryNotSupported` |
| SUM, AVG, MIN, MAX | Not supported (COUNT only) |

## Indexes

Indexes accelerate filtered scans. Declared indexes maintain a sorted B-tree
from the index key to the underlying row ids, with MVCC postings so every
read sees the set of rows live at its snapshot.

### CREATE INDEX

```sql
CREATE INDEX idx_bucket ON documents (bucket);

-- Per-column direction
CREATE INDEX idx_recent ON tasks (created_at DESC, id DESC);

-- Composite index (leading-prefix equality pushdown)
CREATE INDEX idx_items ON items (context_id, item_type, created_at DESC, id DESC);
```

Indexable column types: `INTEGER`, `TEXT`, `UUID`, `TIMESTAMP`, `TXID`,
`BOOLEAN`, `REAL`. `JSON` and `VECTOR` columns are rejected at DDL time
with `ColumnNotIndexable`; extract JSON fields into typed columns or use
HNSW for vectors.

Index names are scoped to the table. A duplicate name on the same table
returns `DuplicateIndex`; the same name on two different tables is allowed.

### DROP INDEX

```sql
DROP INDEX idx_bucket ON documents;
DROP INDEX IF EXISTS idx_bucket ON documents;
```

`DROP INDEX` without `IF EXISTS` on a nonexistent index returns
`IndexNotFound`. `DROP INDEX IF EXISTS` is idempotent (returns
`rows_affected == 0`).

### ALTER TABLE DROP COLUMN

```sql
ALTER TABLE t DROP COLUMN a;              -- defaults to RESTRICT
ALTER TABLE t DROP COLUMN a RESTRICT;     -- explicit
ALTER TABLE t DROP COLUMN a CASCADE;      -- drops dependent indexes
```

Under `RESTRICT` (the default), dropping a column referenced by any index
returns `ColumnInIndex { table, column, index }` naming the first dependent
index in declaration order. Under `CASCADE`, every index whose column list
mentions the target column is removed, and the returned `QueryResult.cascade`
carries a `dropped_indexes` list.

### Ordering

Indexes sort by declared direction per column. `NULL` sorts LAST under `ASC`
and FIRST under `DESC`, matching the engine's ORDER BY convention. Float64
values use `f64::total_cmp` — `NaN` sorts greater than any finite value,
matching the ordering test suite.

### Auto-Indexes

`PRIMARY KEY` and `UNIQUE` columns automatically acquire a backing index
named `__pk_<col>` for `PRIMARY KEY`, `__unique_<col>` for a single-column
`UNIQUE`, and `__unique_<col1>_<col2>...` for a composite
`UNIQUE (col1, col2, ...)` constraint. These indexes exist so PK / UNIQUE
constraint probes run in O(log n) and so `SELECT ... WHERE pk_col = $v`
queries pick an `IndexScan` without requiring a user `CREATE INDEX`.
Composite foreign keys also create child-side `__fk_...` auto-indexes so
parent deletes and tuple validation do not rely on table scans. A table whose
columns include `source_id`, `target_id`, and `edge_type` — the edge-row shape
graph traversal uses — additionally gets an internal
`__graph_edge_source_target_type` route index over that column triple, so
adjacency lookups do not scan. Because it covers the same three columns, it ties
a user-declared `(source_id, target_id, edge_type)` index and wins routing only
by creation order; both are fully index-driven.

Auto-indexes are elided from `.schema` output to keep schema printouts
focused on user-authored DDL. They remain visible in `EXPLAIN <query>`
output as index candidates so agents can programmatically confirm that
a query routed through the auto-index rather than a table scan.

When several user-declared or auto-indexes can route a filtered scan, contextdb
chooses the index with the longest matched leading prefix. Equality, `IN`, and
`IS NULL` on the first indexed column can push adjacent suffix equalities into
the same B-tree probe, so an index on `(source_id, target_id, edge_type)` is
preferred over `(source_id)` for `WHERE source_id IN (...) AND target_id = ...
AND edge_type = ...`. If two indexes match the same number of leading columns,
the more selective leading predicate wins (`=` / `IN` before range predicates
and `!=`, before `IS NULL` / `IS NOT NULL`). A remaining tie is resolved by index
creation order. This choice is intentionally based on filter selectivity, not
`ORDER BY`; a later sort can still be elided when the chosen index order proves
compatible with the requested ordering.

User-declared index names must not begin with `__pk_`, `__unique_`, or `__fk_`.
`CREATE INDEX __pk_id ON t (id)` returns
`ReservedIndexName { table, name, prefix }`.

### Index-Selection Trace Reasons

`.explain <sql>` and `QueryResult.trace` expose the chosen route and the
rejected index candidates. A routed composite prefix renders every pushed
column in index order:

```text
IndexScan { index: idx_edges_route }
  predicates_pushed: [source_id, target_id, edge_type]
  indexes_considered: [idx_edges_source: fewer predicate columns matched than chosen index]
```

Rejected candidates use stable reason strings so commercial products and
agents can distinguish a missed index from a deliberate planner decision:

| Reason | Meaning |
|--------|---------|
| `first column not in WHERE` | The index's leading column is absent from the filter. |
| `function call in predicate` | The leading column is wrapped in a function such as `UPPER(col)`. |
| `arithmetic in predicate` | The leading column appears inside arithmetic such as `col + 1`. |
| `non-literal rhs` | The leading predicate compares to another column or subquery rather than a literal or bound parameter. |
| `LIKE is residual-only` | `LIKE` remains a residual filter and does not drive an index scan. |
| `fewer predicate columns matched than chosen index` | Another index matched a longer leading prefix. |
| `lower selectivity than chosen index` | Another index matched the same prefix length with a more selective leading predicate. |
| `tied with chosen index; lost by creation order` | The candidates were otherwise equivalent and the earlier index won. |
| `no pinned vertex` | A graph adjacency candidate was rejected because the single-hop graph query had no pinned start or target vertex. |

### Graph Adjacency Probe

Single-hop `GRAPH_TABLE ... MATCH` queries with a pinned start or target vertex
route through the per-vertex graph adjacency index automatically. No
`CREATE INDEX` statement or schema change is required. In the CLI, run
`.trace on` before the query to see the physical routing:

```text
trace: AdjacencyProbe index=forward_adj pushed=[a.id] rows_examined=5
```

`forward_adj` and `reverse_adj` are public trace identifiers for the chosen
direction. For a single-hop query pinned directly by `a.id` or `b.id`,
`rows_examined` is the pinned vertex's visible degree. When the pin is resolved
from node metadata, the count also includes the start/target resolution work
before the degree walk. An unpinned single-hop query reports `EdgesScan` and
records the rejected adjacency candidate with reason `no pinned vertex` in the
programmatic trace. Multi-hop or variable-length traversals report `GraphBfs`.

### Error variants

| Error | When it fires |
|-------|---------------|
| `IndexNotFound { table, index }` | `DROP INDEX` without `IF EXISTS` on a missing index |
| `DuplicateIndex { table, index }` | `CREATE INDEX` with a name already in use on the same table |
| `ColumnNotIndexable { table, column, column_type }` | `CREATE INDEX` on a `JSON` or `VECTOR` column |
| `ColumnInIndex { table, column, index }` | `ALTER TABLE ... DROP COLUMN c RESTRICT` on a column referenced by an index |
| `ColumnNotFound { table, column }` | `CREATE INDEX` naming a column that does not exist on the table |
| `ReservedIndexName { table, name, prefix }` | `CREATE INDEX` using a name that begins with `__pk_`, `__unique_`, or `__fk_` (reserved for auto-indexes) |
