<p align="center">
  <img src="assets/banner.svg" alt="contextdb" width="800">
</p>

[![CI](https://github.com/context-graph-ai/contextdb/actions/workflows/ci.yml/badge.svg)](https://github.com/context-graph-ai/contextdb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/contextdb-engine)](https://crates.io/crates/contextdb-engine)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![docs.rs](https://docs.rs/contextdb-engine/badge.svg)](https://docs.rs/contextdb-engine)

# contextdb

An embedded database for agentic memory systems. Relational storage, graph traversal, and vector similarity search under unified MVCC transactions — in a single file, in a single process. Every agent, device, or service runs its own contextdb. They sync bidirectionally through a central server by dial-by-key — reaching it through its own cryptographic identity, no broker to install — knowledge learned by one becomes available to all, with per-table conflict resolution. No port forwarding, no VPN — a node behind NAT dials outbound, and machines on one LAN sync with zero external infrastructure.

If you're building agent memory today, you're probably stitching together SQLite for state, a vector database for embeddings, and application code for graph traversal. contextdb replaces all three — and adds **enforceable policy constraints** (state machines, DAG enforcement, cascading propagation) that the database guarantees, not application code.

contextdb ships no built-in schema. You define your own tables and attach policy to them. The `decisions` table below — and the agentic-memory tables (`observations`, `intentions`, `digests`) used as running examples throughout these docs — is one example schema, not something contextdb requires:

```
contextdb> UPDATE decisions SET status = 'draft' WHERE id = '550e8400...';
Error: invalid state transition: active -> draft
```

No PostgreSQL-style validation triggers. No duplicated application-side
constraint checks. The database enforces policy invariants, while host
callbacks are reserved for explicit observation/cascade workflows.

SQL, `<=>`, and `GRAPH_TABLE ... MATCH` follow existing conventions — the comparison with SQLite, the hybrid query, and the design envelope are in [Why contextdb?](docs/why-contextdb.md).

**Language support:** contextdb is a Rust library and CLI today. Python and TypeScript bindings are on the roadmap — contributions welcome.

**Website:** [contextdb.tech](https://contextdb.tech) · **Docs:** [contextdb.tech/docs](https://contextdb.tech/docs/)

See [Why contextdb?](docs/why-contextdb.md) for the full problem statement, or jump to [Getting Started](docs/getting-started.md) to try it in 2 minutes.

## Use It As a Library

contextdb is an embedded database. The primary interface is the Rust API:

```rust
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

let db = Database::open(std::path::Path::new("./my.db"))?;
// or: Database::open_memory() for ephemeral

let params = HashMap::new();

db.execute(
    "CREATE TABLE observations (
       id UUID PRIMARY KEY,
       data JSON,
       embedding VECTOR(384)
     ) IMMUTABLE",
    &params,
)?;

// Insert with parameters
let mut params = HashMap::new();
params.insert("id".into(), Value::Uuid(uuid::Uuid::new_v4()));
params.insert("data".into(), Value::Json(serde_json::json!({"type": "sensor"})));
params.insert("embedding".into(), Value::Vector(vec![0.1; 384]));

db.execute(
    "INSERT INTO observations (id, data, embedding) VALUES ($id, $data, $embedding)",
    &params,
)?;

// Vector similarity search
let mut query_params = HashMap::new();
query_params.insert("query".into(), Value::Vector(vec![0.1; 384]));

let result = db.execute(
    "SELECT id, data FROM observations ORDER BY embedding <=> $query LIMIT 10",
    &query_params,
)?;

// Graph traversal
let mut graph_params = HashMap::new();
graph_params.insert("start".into(), Value::Uuid(uuid::Uuid::new_v4()));

let result = db.execute(
    "SELECT target_id FROM GRAPH_TABLE(
       edges MATCH (a)-[:DEPENDS_ON]->{1,3}(b)
       WHERE a.id = $start
       COLUMNS (b.id AS target_id)
     )",
    &graph_params,
)?;

// Subscribe to commits
let rx = db.subscribe();
// rx is a std::sync::mpsc::Receiver<CommitEvent>
```

**Ownership:** a database file has exactly one writer at a time; reading is a
separate door that does not take the write lock. See
[Store Ownership & Concurrency](docs/architecture.md#store-ownership--concurrency)
and the CLI contract in [`docs/cli.md`](docs/cli.md#cli-client-contextdb).

### Triggers

contextdb rejects PG-style validation triggers as an invariant mechanism:
constraints such as `STATE MACHINE`, `IMMUTABLE`, `DAG`, and `PROPAGATE` are
engine-enforced. It does support host-callback Triggers for
transactional observation and cascade writes that belong with the firing
transaction.

```rust
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

let db = Database::open_memory();
db.execute("CREATE TABLE observation (id UUID PRIMARY KEY)", &HashMap::new())?;
db.execute(
    "CREATE TABLE derived (id UUID PRIMARY KEY, observation_id UUID)",
    &HashMap::new(),
)?;
db.execute(
    "CREATE TRIGGER observation_seen ON observation WHEN INSERT",
    &HashMap::new(),
)?;

db.register_trigger_callback("observation_seen", |db, ctx| {
    db.execute_in_tx(
        ctx.tx,
        "INSERT INTO derived (id, observation_id) VALUES ($id, $observation)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
            (
                "observation".to_string(),
                ctx.row_values.get("id").cloned().unwrap_or(Value::Null),
            ),
        ]),
    )?;
    Ok(())
})?;
db.complete_initialization()?;
```

The callback runs synchronously inside the firing transaction's commit window.
Same-DB cross-thread writers wait-and-proceed inside the engine, unrelated
databases proceed independently, same-thread callback reentry receives
`CallbackReentry`, callback tx-bound handles stay isolated to the runner
thread, cron same-DB contention remains immediate, and an unhealthy wait trips
the bounded deadlock guard with a structured `tracing::warn!`.

The hybrid `GRAPH_TABLE` + `<=>` query is in [Why contextdb?](docs/why-contextdb.md#one-query-three-subsystems).

Add to your `Cargo.toml`:

```toml
[dependencies]
contextdb-engine = "1.0.0"
contextdb-core = "1.0.0"
uuid = { version = "1", features = ["v4"] }
serde_json = "1"
```

## Install

```bash
# Install the CLI
cargo install contextdb-cli

# Or run the sync server
cargo install contextdb-server

# Or via Docker (no clone needed)
curl -O https://raw.githubusercontent.com/context-graph-ai/contextdb/main/docker-compose.yml
docker compose up
```

On startup the server prints its enrollment ticket to stdout — its
cryptographic identity plus reachable addresses — as `enrollment ticket: <...>`.
Copy that ticket and pass it to an edge with `--sync-endpoint` to connect (see
[CLI Reference](docs/cli.md)). For scripting, `--show-ticket` prints the bare
ticket and exits, and `--ticket-file <path>` writes it to a file. With Docker,
the ticket appears in `docker compose logs`.

The server keeps its identity in `<db-path>.fabric-identity.key` — a secret you
should back up and never commit to git (losing it changes the node's identity
and invalidates its tickets). See the [CLI Reference](docs/cli.md) for the full
list of files it writes.

## Or Explore With the CLI

```bash
cargo build --release -p contextdb-cli
./target/release/contextdb :memory:
```

```
contextdb> CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT NOT NULL)
           STATE MACHINE (status: draft -> [active, rejected], active -> [superseded]);
ok (rows_affected=0)

contextdb> INSERT INTO decisions VALUES ('550e8400-e29b-41d4-a716-446655440000', 'draft');
ok (rows_affected=1)

contextdb> UPDATE decisions SET status = 'active' WHERE id = '550e8400-e29b-41d4-a716-446655440000';
ok (rows_affected=1)

contextdb> UPDATE decisions SET status = 'draft' WHERE id = '550e8400-e29b-41d4-a716-446655440000';
Error: invalid state transition: active -> draft

contextdb> .schema decisions
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL
) STATE MACHINE (status: active -> [superseded], draft -> [active, rejected]);
```

### Two-Vector Walkthrough

```sql
CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4), vector_vision VECTOR(8));
INSERT INTO evidence (id, vector_text, vector_vision) VALUES
  ('11111111-1111-1111-1111-111111111111', [1,0,0,0], [0,1,0,0,0,0,0,0]);
SHOW VECTOR_INDEXES;
SELECT id FROM evidence ORDER BY vector_text <=> '[1,0,0,0]' LIMIT 1;
SELECT id
FROM evidence
ORDER BY vector_text <=> ROW_VECTOR('evidence', 'vector_text', '11111111-1111-1111-1111-111111111111')
LIMIT 1;
```

Every `VECTOR(N)` column is searchable with no separate index to create.
Index construction and repair run only through maintenance — see
[Vector Similarity Search](docs/query-language.md#vector-similarity-search).
Declare `PARTITION_KEY (...)` on a vector column to keep one smaller index per key
value, so a search whose `WHERE` names a key looks only inside that partition.
`SHOW VECTOR_INDEXES` returns one summary row per vector column, and `SHOW
VECTOR_PARTITIONS FOR table.column` shows each partition's state. Use `VECTOR(N) WITH (quantization = 'F32'|'SQ8'|'SQ4')` to choose the
per-column storage footprint; omitted quantization defaults to `F32`.
<!-- enforced by: vector_serving_merge_contract::updated_base_and_tail_publish_one_visible_row_per_identity, vector_partition_query_contract::equality_on_every_partition_component_selects_one_named_tuple, vector_lazy_raw_residency_contract::reopen_and_selected_queries_keep_raw_vector_residency_partition_local, vector_partition_sync_inspection_contract::existing_two_row_sync_keeps_each_vector_with_its_row_and_shows_summary, vector_partition_sync_inspection_contract::show_vector_partitions_supports_all_sql_forms -->

### Upgrading From 0.3.x

Opening a legacy vector store without the named-index format marker returns
`LegacyVectorStoreDetected`. `contextdb migrate <path>` brings the store forward
in place, writing a `<path>.bak` backup first — rehearse it on a copy, and take a
`contextdb snapshot export` beforehand. If that store is unavailable to you,
recovery is still explicit: sync from a peer already on the named-index storage
format, or recreate the schema and reimport the data.

## What It Does

**Relational (PostgreSQL-compatible SQL)** — SELECT, INSERT, UPDATE, DELETE, JOINs (INNER/LEFT), CTEs, upsert (`ON CONFLICT DO UPDATE`), DISTINCT, LIMIT, IN with subqueries, LIKE, BETWEEN, parameter binding (`$name`).

**Graph (SQL/PGQ-style)** — `GRAPH_TABLE(... MATCH ...)` following SQL/PGQ conventions for bounded BFS, typed edges, variable-length paths (`{1,3}`), and direction control. DAG constraint enforcement prevents cycles. State propagation cascades changes along graph edges.

**Vector (pgvector conventions)** — Cosine similarity search via `<=>`. Query with a bound vector, vector literal, or `ROW_VECTOR('table', 'column', key)` to reuse a persisted row vector as the query vector. Construction and repair of each column's index run only through maintenance ([query-language reference](docs/query-language.md#vector-similarity-search)), and a column can be partitioned by key so a search looks only inside the partitions it names: `SHOW VECTOR_INDEXES` returns one summary row per vector column, and `SHOW VECTOR_PARTITIONS FOR table.column` shows each partition's state. `AUTO_INDEX_AT` and HNSW settings are declared per column; leaving them unset keeps the defaults. Pre-filtered search narrows candidates before scoring. <!-- enforced by: sql_surface_other::prv_03_row_vector_query_matches_literal_vector_parity_for_trace_and_results, vector_maintained_lifecycle_contract::engine_owned_file_maintenance_publishes_a_durable_indexed_route_for_a_reopened_reader, vector_partition_query_contract::equality_on_every_partition_component_selects_one_named_tuple, vector_partition_sync_inspection_contract::show_vector_partitions_supports_all_sql_forms, vector_policy_resolver_contract::declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries, tests/integration/hnsw_tests.rs::h08_prefiltered_search_respects_candidate_bitmap -->

**Unified transactions** — One transaction atomically updates relational rows, graph adjacency structures, and vector indexes. One read snapshot sees consistent state across all three. MVCC with consistent snapshots — readers never block writers.

**Enforceable policy constraints** — `IMMUTABLE` tables, `STATE MACHINE` column transitions, `DAG` cycle prevention, single-column and composite foreign keys, `RETAIN` with TTL expiry, `PROPAGATE` for cascading state changes along edges and foreign keys. Enforced by the database — no application code can bypass them.

**Collaborative sync** — Every contextdb instance is a full read-write database. Each runs a SyncClient that syncs bidirectionally with a central SyncServer by dial-by-key: the server is reached through its own cryptographic identity (its enrollment ticket), not a broker address — nothing to install or expose. Machines on one LAN sync with zero external infrastructure and no internet, over direct connections; the default configuration contacts no third-party service. Crossing networks, the operator either self-hosts a small stateless relay or opts into the free public relays — connectivity is never a paid feature. Offline-first: each database works independently, syncing changesets when connected. Conflict policy and travel direction are declared on the table; see [`docs/query-language.md`](docs/query-language.md#table-options) and the recipe in [`skills/sync`](skills/sync/SKILL.md#7-a-delete-that-stays-deleted--across-sync-and-restart). The server runs the same contextdb engine — self-host it, or point your databases at a hosted server.

**Persistence** — Single-file storage via redb. Crash-safe. Compute/storage separated via the `WriteSetApplicator` trait (local redb for open source, object store for enterprise).

**Plugin system** — `DatabasePlugin` trait with lifecycle hooks (`pre_commit`, `post_commit`, `on_open`, `on_close`, `on_ddl`, `on_query`, `post_query`, `health`, `describe`, `on_sync_push`, `on_sync_pull`). Applications inject plugins via `Database::open_with_plugin()`.

**Subscriptions** — `db.subscribe()` returns a `std::sync::mpsc::Receiver<CommitEvent>`, one per subscriber, and every commit is fanned out to all of them. `db.subscribe_with_capacity(n)` sets the per-subscriber queue depth.

The 10K-1M / depth-10 design envelope is in [Why contextdb?](docs/why-contextdb.md#design-envelope).

## Documentation

Full documentation is available at [contextdb.tech/docs](https://contextdb.tech/docs/), or browse the source files:

| Doc | What it covers |
|-----|---------------|
| **[Capability Index](docs/capability-index.md)** | One page: what contextdb is, what it is not, and the numbers it stops at |
| **[Getting Started](docs/getting-started.md)** | Build, first REPL session, library embedding — 2 minutes |
| **[Why contextdb?](docs/why-contextdb.md)** | Problem statement, design philosophy, comparison with alternatives |
| **[Usage Scenarios](docs/usage-scenarios.md)** | Walkthroughs: constraints, graph queries, vector search, sync, propagation |
| **[Query Language](docs/query-language.md)** | SQL, graph MATCH, vector search, constraints, built-in functions |
| **[Sync Across Two Machines](docs/sync-two-machines.md)** | Stand up a hub, enroll two edges, converge in both directions |
| **[CLI Reference](docs/cli.md)** | REPL commands, sync commands, non-interactive scripting |
| **[Architecture](docs/architecture.md)** | Crate map, storage engine, MVCC, sync protocol, work ledger and blob plane, upgrades and recovery, plugin system |
| **[Benchmarking](docs/benchmarking.md)** | How the benchmarks are built and run |
| **[Agent Readiness](docs/agent-readiness.md)** | How this repo measures whether AI assistants can use and contribute to it |

## Architecture

11-crate Rust workspace:

| Crate | Role |
|-------|------|
| `contextdb-core` | Types, executor traits, errors, table metadata |
| `contextdb-tx` | MVCC transaction manager with deferred-apply write sets |
| `contextdb-relational` | Relational executor (scan, insert, upsert, delete) |
| `contextdb-graph` | Graph executor (bounded BFS, adjacency index, DAG enforcement) |
| `contextdb-vector` | Vector executor (cosine similarity, HNSW, pre-filtered search) |
| `contextdb-hnsw` | HNSW graph index used by the vector executor |
| `contextdb-parser` | SQL parser (pest grammar with GRAPH_TABLE + vector extensions) |
| `contextdb-planner` | Rule-based query planner |
| `contextdb-engine` | Database engine — wires all subsystems, plugin API, subscriptions |
| `contextdb-server` | Sync server and client (dial-by-key transport, conflict resolution) |
| `contextdb-cli` | Interactive CLI REPL |

A hub can declare which tables an enrolled edge may push. Every delivered unit
reports accepted or refused. Authoritative erasure is all-or-nothing across the
fleet. See [the custody SQL and API guide](docs/query-language.md#tenant-policy-and-event-custody).
<!-- enforced by: crates/contextdb-engine/tests/custody/tenant_table_policy_contract.rs::a_matching_expectation_binds_a_differing_clause_and_an_undeclared_table_are_typed_refusals_and_none_writes_on_either_side, crates/contextdb-engine/tests/custody/delivery_outcome_contract.rs::a_refused_unit_is_terminal_the_resend_obligation_ends_status_is_clean_and_the_rows_stay_locatable, crates/contextdb-engine/tests/custody/custody_purge_and_discard_contract.rs::a_multi_table_purge_is_one_erasure_boundary_with_per_table_results_and_is_refused_before_selection_when_illegal -->

## Building

```bash
cargo build --workspace
cargo test --workspace
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
