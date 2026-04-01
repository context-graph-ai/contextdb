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
| `TIMESTAMP` | Unix timestamp | `NOW()` |
| `JSON` | JSON value | `'{"key": "value"}'` |
| `VECTOR(n)` | Fixed-dimension float vector | `[0.1, 0.2, 0.3]` |

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
| `UNIQUE` | No duplicate values |
| `DEFAULT expr` | Default value for inserts |
| `REFERENCES table(col)` | Foreign key |

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
