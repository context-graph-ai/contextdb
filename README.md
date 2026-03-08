# ContextDB

An embedded database engine for agentic memory systems. Combines relational storage, graph traversal, and vector similarity search under unified MVCC transactions.

## Status

**v0.1 — In-Memory Engine.** Fully functional with all query capabilities. No disk persistence yet (data lives in memory only).

## Features

- **Unified transactions** across relational rows, graph edges, and vector embeddings
- **SQL interface** with graph and vector extensions:
  - `MATCH` syntax (openCypher subset) in CTEs for bounded graph traversal
  - `<=>` operator for cosine similarity vector search
  - Standard SQL: SELECT, INSERT, UPDATE, DELETE, JOIN, WITH, ORDER BY, LIMIT
- **MVCC snapshot isolation** — readers never block writers
- **Schema enforcement** — observation immutability, invalidation state machine, vector dimension validation
- **CLI REPL** for interactive queries

## Quick Start

```bash
cargo build --release
./target/release/contextdb-cli :memory:
```

```sql
contextdb> INSERT INTO contexts (id, name, created_at)
           VALUES ('550e8400-e29b-41d4-a716-446655440000', 'test', 1709827200000);

contextdb> SELECT * FROM contexts;

contextdb> -- Graph traversal
contextdb> WITH affected AS (
             MATCH (e:Entity {id: $id})<-[:BASED_ON*1..3]-(d)
             RETURN d.id
           )
           SELECT * FROM decisions WHERE id IN (SELECT id FROM affected);

contextdb> -- Vector similarity search
contextdb> SELECT id, data FROM observations
           ORDER BY embedding <=> $query_vector
           LIMIT 10;
```

## Architecture

9-crate Rust workspace:

| Crate | Purpose |
|-------|---------|
| `contextdb-core` | Types, executor traits, error types, schema |
| `contextdb-tx` | Transaction manager with deferred-apply MVCC |
| `contextdb-relational` | Relational executor (scan, insert, upsert, delete) |
| `contextdb-graph` | Graph executor (bounded BFS, adjacency index) |
| `contextdb-vector` | Vector executor (cosine similarity, pre-filtered search) |
| `contextdb-parser` | SQL parser (pest grammar with MATCH + vector extensions) |
| `contextdb-planner` | Rule-based query planner |
| `contextdb-engine` | Database engine wiring all subsystems together |
| `contextdb-cli` | Interactive CLI REPL |

## Building

```bash
cargo build --workspace
```

## Testing

```bash
cargo test --workspace
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
