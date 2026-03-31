# Why contextdb?

Agents need memory that does more than store embeddings. They need to know what they decided, what those decisions were based on, and whether the basis has changed. They need to trace provenance through a graph of relationships, search by semantic similarity within a specific neighborhood, and have invalid state transitions rejected — not by application code, but by the database.

---

## The Problem

Most agents today stitch together three systems:

1. **SQLite** for relational state (what was decided, when, what status it's in)
2. **A vector database** (Chroma, Qdrant, Pinecone) for semantic search over embeddings
3. **Application code** for graph traversal (recursive CTEs, Python-side BFS, or a separate graph DB)

This works until it doesn't:

- **No unified transactions.** Crash between updating the row and the vector index? Inconsistent state. Your agent confidently searches over stale embeddings.
- **Unbounded graph walks.** Recursive CTEs or Python-side BFS have no built-in depth limits or cycle detection. One circular reference and your agent spins forever.
- **Three sync problems.** When the local instance goes offline (a laptop, a browser plugin, a mobile app), you need three sync strategies — one per system — or you accept data loss.
- **Constraints live in application code.** Every consumer duplicates the same validation: "don't transition from draft to superseded", "don't insert into this immutable table", "cascade this state change to dependents." Miss one consumer and the invariant breaks silently.

---

## How contextdb Is Different

contextdb replaces all three with one embedded database. One transaction atomically updates relational rows, graph adjacency structures, and vector indexes. One read snapshot sees consistent state across all three.

| Capability | SQLite + extensions | contextdb |
|---|---|---|
| Vector search | sqlite-vec (separate extension, no unified transactions with relational data) | Built-in, auto-HNSW at 1K vectors, pre-filtered search, same MVCC transaction as rows |
| Graph traversal | Recursive CTEs (unbounded, no cycle detection) | SQL/PGQ-style MATCH with bounded BFS, typed edges |
| State machines | CHECK constraints + triggers (bypassable) | `STATE MACHINE` in DDL, enforced by the database engine |
| Atomic cross-model updates | Application-level coordination | Single MVCC transaction across relational + graph + vector |
| Sync | Build your own | Built-in local-to-server replication with conflict resolution |
| Immutable tables | Not enforceable (triggers are bypassable) | `IMMUTABLE` keyword, enforced by the database engine |
| Cascading invalidation | Application code | `PROPAGATE` in DDL — state changes cascade along edges and FKs |

---

## Enforceable Policy Constraints

The most distinctive feature: constraints that the database guarantees, not application code.

**STATE MACHINE** — Define valid state transitions in DDL. The database rejects invalid transitions at the engine level, regardless of how the write arrives:

```sql
CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: draft -> [active, rejected], active -> [superseded]);

UPDATE decisions SET status = 'superseded' WHERE id = $id;
-- Error if current status is 'draft': invalid state transition: draft -> superseded
```

**PROPAGATE** — When one thing changes state, related things react automatically. Along graph edges, along foreign keys, with bounded depth:

```sql
PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated MAX DEPTH 3
```

Archive an intention, and in one transaction: FK-linked decisions transition to `invalidated`, edge-linked decisions also cascade, and invalidated decisions are excluded from vector search.

**DAG** — Prevent cycles in directed relationships. Enforced on insert via BFS from target back to source:

```sql
CREATE TABLE edges (...) DAG ('DEPENDS_ON', 'BLOCKS')
```

**IMMUTABLE** — Once inserted, rows cannot be updated or deleted:

```sql
CREATE TABLE observations (...) IMMUTABLE
```

**RETAIN** — Automatic TTL expiry with sync-safe option (rows aren't purged until synced):

```sql
CREATE TABLE scratch (...) RETAIN 24 HOURS
CREATE TABLE logs (...) RETAIN 90 DAYS SYNC SAFE
```

---

## Familiar Conventions

contextdb follows existing standards so there's nothing new to learn:

- **PostgreSQL-compatible SQL** — SELECT, INSERT, UPDATE, DELETE, JOINs, CTEs, upsert, DISTINCT, LIMIT, LIKE, BETWEEN, parameter binding (`$name`)
- **pgvector syntax** — Cosine similarity via `<=>` operator
- **SQL/PGQ-style graph queries** — `GRAPH_TABLE(... MATCH ...)` with bounded BFS, typed edges, variable-length paths (`{1,3}`), direction control

Not the full SQL/PGQ standard — just the subset that matters for bounded traversal in agentic workloads. contextdb is a focused tool for agent memory, not a general-purpose database.

---

## One Query, Three Subsystems

Find semantically similar observations within a graph neighborhood, filtered by relational predicates — a query that would take ~40 lines of Python across SQLite, ChromaDB, and a hand-rolled BFS:

```sql
WITH neighborhood AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges MATCH (start)-[:RELATES_TO]->{1,3}(related)
    WHERE start.id = $entity_id
    COLUMNS (related.id AS b_id)
  )
),
candidates AS (
  SELECT o.id, o.data, o.embedding
  FROM observations o
  INNER JOIN neighborhood n ON o.entity_id = n.b_id
  WHERE o.observation_type = 'config_change'
)
SELECT id, data FROM candidates
ORDER BY embedding <=> $query_embedding
LIMIT 5
```

One query. One transaction. One process.

---

## Design Envelope

contextdb is designed for agentic memory, not data warehousing:

- 10K-1M rows per database
- Sparse graphs with bounded traversal (depth <= 10)
- Append-heavy writes, small transactions
- Configurable memory budget via `SET MEMORY_LIMIT` (no hard-coded ceiling)
- Laptops, ARM64 devices (browser and mobile via Rust's WASM target are future directions)

---

## Next Steps

- [Getting Started](getting-started.md) — build and run in 2 minutes
- [Usage Scenarios](usage-scenarios.md) — 16 problem-first walkthroughs
- [Architecture](architecture.md) — how it works under the hood
