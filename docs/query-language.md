# Query Language

contextdb's query language is built on three standards:

- **PostgreSQL-compatible SQL** — DDL, DML, expressions, operators, JOINs, CTEs, `ON CONFLICT DO UPDATE`, `$param` binding
- **pgvector conventions** — `<=>` operator for cosine similarity in `ORDER BY`
- **SQL/PGQ-style graph queries** — `GRAPH_TABLE(... MATCH ...)` following SQL/PGQ conventions for bounded graph traversal (not a full standard implementation)

On top of these, contextdb adds **declarative constraints for agentic memory workloads**: `IMMUTABLE`, `STATE MACHINE`, `DAG`, `RETAIN`, and `PROPAGATE`. These are contextdb-specific extensions — everything else should feel familiar if you've used PostgreSQL.

All examples work in the Rust API via `db.execute(sql, &params)` where parameters are passed as `HashMap<String, Value>`. The CLI REPL does not support parameter binding (`$param`) — use literal values directly. Vector search works in the CLI using vector literals: `ORDER BY embedding <=> [0.1, 0.2, 0.3] LIMIT 5`.

---

## Statements

### CREATE TABLE

```sql
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  data JSON,
  embedding VECTOR(384),
  recorded_at TIMESTAMP DEFAULT NOW()
) IMMUTABLE
```

See [Table Options](#table-options) for IMMUTABLE, STATE MACHINE, DAG, RETAIN, and PROPAGATE.

### ALTER TABLE

```sql
ALTER TABLE t ADD [COLUMN] col TYPE
ALTER TABLE t DROP [COLUMN] col
ALTER TABLE t RENAME COLUMN old TO new
ALTER TABLE t SET RETAIN 7 DAYS [SYNC SAFE]
ALTER TABLE t DROP RETAIN
ALTER TABLE t SET SYNC_CONFLICT_POLICY 'latest_wins'
ALTER TABLE t DROP SYNC_CONFLICT_POLICY
```

### DROP TABLE

```sql
DROP TABLE t
```

### CREATE INDEX

```sql
CREATE INDEX idx_name ON t (col)
```

### INSERT

```sql
INSERT INTO observations (id, data, embedding)
VALUES ($id, $data, $embedding)

-- Multiple rows
INSERT INTO entities (id, name) VALUES ($id1, $name1), ($id2, $name2)

-- Upsert
INSERT INTO entities (id, name) VALUES ($id, $name)
ON CONFLICT (id) DO UPDATE SET name = $name
```

### UPDATE / DELETE

```sql
UPDATE decisions SET status = 'superseded' WHERE id = $id
DELETE FROM scratch WHERE created_at < $cutoff
```

### SELECT

```sql
SELECT [DISTINCT] columns FROM table
  [INNER JOIN | LEFT JOIN other ON condition]
  [WHERE condition]
  [ORDER BY col [ASC|DESC]]
  [LIMIT n]
```

### CTEs

```sql
WITH active AS (
  SELECT id, name FROM entities WHERE status = 'active'
)
SELECT * FROM active WHERE name LIKE 'sensor%'
```

Multiple CTEs via comma separation. Non-recursive only.

### Transactions

```sql
BEGIN
-- statements
COMMIT

-- or
ROLLBACK
```

### Configuration

```sql
SET SYNC_CONFLICT_POLICY 'latest_wins'
SHOW SYNC_CONFLICT_POLICY
SET MEMORY_LIMIT '512M'
SHOW MEMORY_LIMIT
SET DISK_LIMIT '1G'
SET DISK_LIMIT 'none'
SHOW DISK_LIMIT
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

---

## Column Constraints

```sql
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL DEFAULT 0.0,
  email TEXT UNIQUE,
  intention_id UUID REFERENCES intentions(id)
)
```

| Constraint | Description |
|------------|-------------|
| `PRIMARY KEY` | Unique row identifier |
| `NOT NULL` | Value required |
| `UNIQUE` | No duplicate values (single column). A duplicate INSERT on a `UNIQUE` column is a silent no-op (returns `Ok(rows_affected=0)`), matching the composite-uniqueness contract. |
| `DEFAULT expr` | Default value for inserts |
| `REFERENCES table(col)` | Foreign key — writes are rejected if the referenced row does not exist; in explicit transactions the error may surface at `COMMIT` |
| `IMMUTABLE` | Column is audit-frozen — INSERT sets the value once; `UPDATE`, `ON CONFLICT DO UPDATE`, sync-apply mutations, and schema-altering DDL against the column are rejected with `Error::ImmutableColumn` |

### Audit-Frozen Columns

An audit-frozen column carries data that must not be silently rewritten by anyone, through any path. Declare it with `IMMUTABLE`:

```sql
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  decision_type TEXT NOT NULL IMMUTABLE,
  description TEXT NOT NULL IMMUTABLE,
  reasoning JSON,
  confidence REAL,
  status TEXT NOT NULL DEFAULT 'active'
) STATE MACHINE (status: active -> [superseded, archived])
```

`decision_type` and `description` are provenance — set once at INSERT and never rewritten. `status` and `confidence` remain mutable. An `UPDATE decisions SET decision_type = '…'` returns `Error::ImmutableColumn`; the row is unchanged. Sync-apply across a NATS edge enforces the same rule on the peer: incoming row-changes that mutate a flagged column are rejected and surface in `ApplyResult.conflicts`. `ALTER TABLE ... DROP COLUMN`, `RENAME COLUMN`, and column-type-altering ALTER against a flagged column are refused.

Correction without rewrite — the supersede pattern. When a recorded decision turns out to be wrong, insert a new row with the corrected values and mark the original `superseded`:

```sql
-- Original (frozen)
INSERT INTO decisions (id, decision_type, description, status)
VALUES ('…A', 'sql-migration', 'adopt contextdb', 'active');

-- Correction: a new row, not an update. Both rows remain queryable.
INSERT INTO decisions (id, decision_type, description, status)
VALUES ('…B', 'sql-migration', 'adopt contextdb (rev 2)', 'active');
UPDATE decisions SET status = 'superseded' WHERE id = '…A';
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

### Foreign Key State Propagation

Trigger a state change on this row when the referenced row transitions:

```sql
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  intention_id UUID REFERENCES intentions(id)
    ON STATE archived PROPAGATE SET invalidated
) STATE MACHINE (status: active -> [invalidated, superseded])
```

When an `intentions` row transitions to `archived`, any `decisions` row referencing it transitions to `invalidated`.

---

## Table Options

Table options appear after the closing `)` of the column list. Multiple options can be combined.

### IMMUTABLE

Rows cannot be updated or deleted after insertion. Useful for append-only data like observations and audit logs:

```sql
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  data JSON,
  embedding VECTOR(384)
) IMMUTABLE
```

### STATE MACHINE

Restrict a column's value transitions to declared edges:

```sql
CREATE TABLE decisions (
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

Automatic row expiry. Units: `SECONDS`, `MINUTES`, `HOURS`, `DAYS`. Optional `SYNC SAFE` delays purging until synced:

```sql
CREATE TABLE scratch (
  id UUID PRIMARY KEY,
  data TEXT
) RETAIN 24 HOURS SYNC SAFE
```

Can also be set via ALTER TABLE:

```sql
ALTER TABLE scratch SET RETAIN 7 DAYS
ALTER TABLE scratch DROP RETAIN
```

### PROPAGATE ON EDGE

Cascade state changes along graph edges when a row transitions:

```sql
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL
) STATE MACHINE (status: active -> [invalidated, superseded])
  PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated
```

When a `decisions` row transitions to `invalidated`, rows connected via incoming `CITES` edges also transition to `invalidated`. Options: `INCOMING`, `OUTGOING`, `BOTH` for edge direction. `MAX DEPTH n` limits traversal. `ABORT ON FAILURE` rolls back if any propagation fails.

### PROPAGATE ON STATE ... EXCLUDE VECTOR

Remove a row's vector from similarity search results when it enters a given state, without deleting the row:

```sql
CREATE TABLE decisions (...)
  PROPAGATE ON STATE invalidated EXCLUDE VECTOR
  PROPAGATE ON STATE superseded EXCLUDE VECTOR
```

### Combining Options

Options compose — a real-world table might use several:

```sql
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL,
  intention_id UUID REFERENCES intentions(id)
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
params.insert("entity_id".into(), Value::Uuid(id));
params.insert("type".into(), Value::Text("sensor".into()));

let result = db.execute(
    "SELECT * FROM entities WHERE id = $entity_id AND type = $type",
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
  WHERE a.id = '550e8400-...'
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
SELECT d.id, d.status FROM decisions d
INNER JOIN deps ON d.id = deps.b_id
WHERE d.status = 'active'
```

### Graph + Vector: Neighborhood Similarity Search

Find semantically similar entities within a graph neighborhood:

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
  FROM entities e
  INNER JOIN neighborhood n ON e.id = n.b_id
  WHERE e.is_deprecated = FALSE
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
SELECT id, data FROM observations
ORDER BY embedding <=> $query_vector
LIMIT 10

-- CLI with vector literal
SELECT id, data FROM observations
ORDER BY embedding <=> [0.1, 0.2, 0.3]
LIMIT 10
```

Lower distance = more similar. A `LIMIT` clause is required — unbounded vector searches are rejected.

### Pre-Filtered Search

Combine WHERE filters with vector ranking. The engine filters first, then scores only matching rows:

```sql
SELECT id, description FROM decisions
WHERE status = 'active'
ORDER BY embedding <=> $query
LIMIT 5
```

### Indexing

The engine automatically selects the search strategy based on vector count:

- Below ~1000 vectors: brute-force linear scan (exact)
- At/above ~1000 vectors: HNSW approximate nearest neighbors (recall target >= 95%)

No manual index creation needed. Use `.explain` in the CLI to see which strategy is active:

```
contextdb> .explain SELECT id FROM observations ORDER BY embedding <=> $q LIMIT 5
HNSWSearch { table: "observations", limit: 5 }
```

---

## SQL Comments

Both styles are stripped before parsing:

```sql
-- Line comment
SELECT * FROM entities; /* Block comment */
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
CREATE INDEX idx_bucket ON observations (bucket);

-- Per-column direction
CREATE INDEX idx_recent ON decisions (created_at DESC, id DESC);

-- Composite index (leading-column equality + residual filter)
CREATE INDEX idx_entities ON entities (context_id, entity_type, created_at DESC, id DESC);
```

Indexable column types: `INTEGER`, `TEXT`, `UUID`, `TIMESTAMP`, `TXID`,
`BOOLEAN`, `REAL`. `JSON` and `VECTOR` columns are rejected at DDL time
with `ColumnNotIndexable`; extract JSON fields into typed columns or use
HNSW for vectors.

Index names are scoped to the table. A duplicate name on the same table
returns `DuplicateIndex`; the same name on two different tables is allowed.

### DROP INDEX

```sql
DROP INDEX idx_bucket ON observations;
DROP INDEX IF EXISTS idx_bucket ON observations;
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

Auto-indexes are elided from `.schema` output to keep schema printouts
focused on user-authored DDL. They remain visible in `EXPLAIN <query>`
output as index candidates so agents can programmatically confirm that
a query routed through the auto-index rather than a table scan.

User-declared index names must not begin with `__pk_` or `__unique_`.
`CREATE INDEX __pk_id ON t (id)` returns
`ReservedIndexName { table, name, prefix }`.

### Error variants

| Error | When it fires |
|-------|---------------|
| `IndexNotFound { table, index }` | `DROP INDEX` without `IF EXISTS` on a missing index |
| `DuplicateIndex { table, index }` | `CREATE INDEX` with a name already in use on the same table |
| `ColumnNotIndexable { table, column, column_type }` | `CREATE INDEX` on a `JSON` or `VECTOR` column |
| `ColumnInIndex { table, column, index }` | `ALTER TABLE ... DROP COLUMN c RESTRICT` on a column referenced by an index |
| `ColumnNotFound { table, column }` | `CREATE INDEX` naming a column that does not exist on the table |
| `ReservedIndexName { table, name, prefix }` | `CREATE INDEX` using a name that begins with `__pk_` or `__unique_` (reserved for auto-indexes) |

