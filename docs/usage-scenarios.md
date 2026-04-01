# Usage Scenarios

16 problem-first walkthroughs showing how contextdb solves real agent memory problems with SQL. Each scenario starts with a problem, then shows the solution.

For background on why these problems exist and how contextdb compares to alternatives, see [Why contextdb?](why-contextdb.md). To build and try these examples yourself, see [Getting Started](getting-started.md).

> **Note:** Examples use `$param` parameter binding syntax, which works in the Rust API via `db.execute(sql, &params)`. The CLI REPL does not support parameter binding — use literal values directly when trying these interactively.

---

## Example Use Cases

Before diving into SQL, here are concrete scenarios where these problems surface:

**An AI coding assistant** stores decisions about file locations, architecture patterns, and tool preferences. When a file is renamed (observation), any decision that referenced the old path should be flagged as stale (invalidation). When the assistant searches its memory, it needs to find semantically similar past decisions (vector search) that are still valid (relational filter on status) and trace what they were based on (graph traversal).

**A security monitoring system** runs object detection on camera feeds. Each detection is an immutable observation with an embedding. The system learns that a particular person is a regular visitor (decision). When an unknown person appears, graph traversal finds all active decisions about expected visitors at that location. Dozens of sites sync corrections to a central server; new sites pull learned patterns immediately.

**An infrastructure monitoring agent** tracks services, their configurations, and the decisions made about alert thresholds. When a config change observation arrives (region moved, instance type changed), the agent needs to find which threshold decisions relied on the old configuration — automatically, not by manual lookup.

**A team knowledge system** stores architectural decisions with structured reasoning, links them to the entities they affect, and tracks outcomes. When a new team member's agent asks "why is this service configured this way?", the answer includes the decision, its reasoning, the entities it was based on, and whether any of those entities have changed since.

---

## Scenario 1: Store Observations With Provenance

**Problem:** You have observations arriving from multiple sources. Each observation is about an entity. You need to store them immutably and trace which entities and decisions they connect to.

```sql
-- Entities that observations are about
CREATE TABLE entities (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  properties JSON
)

-- Immutable observation log
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  observation_type TEXT NOT NULL,
  data JSON NOT NULL,
  entity_id UUID NOT NULL,
  source TEXT NOT NULL,
  embedding VECTOR(384),
  recorded_at TIMESTAMP DEFAULT NOW()
) IMMUTABLE

-- Graph edges connecting entities, decisions, observations
CREATE TABLE edges (
  id UUID PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
) DAG('DEPENDS_ON', 'BASED_ON')
```

Insert an observation and link it to its entity:

```sql
INSERT INTO observations (id, observation_type, data, entity_id, source, embedding)
VALUES ($id, 'config_change', $data, $entity_id, 'terraform-hook', $embedding);

INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES ($edge_id, $observation_id, $entity_id, 'OBSERVED_ON');
```

The observation is immutable — it can never be modified or deleted. The `OBSERVED_ON` edge is deduplicated if inserted twice — inserting the same `(source_id, target_id, edge_type)` is a silent no-op, making agent operations idempotent ("ensure this link exists" without checking first).

The `DAG('DEPENDS_ON', 'BASED_ON')` declaration means these specific edge types are acyclic — the engine rejects any insert that would create a cycle, checking both committed edges and pending writes in the current transaction. Other edge types in the same table (like `OBSERVED_ON`) can have cycles if needed.

---

## Scenario 2: Decisions With State Machines

**Problem:** Decisions have a lifecycle — draft, active, superseded, invalidated. Invalid transitions (draft directly to superseded) should be impossible.

```sql
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL,
  embedding VECTOR(384)
) STATE MACHINE (status: draft -> [active, rejected], active -> [superseded, invalidated])
```

```sql
-- Valid: draft -> active
UPDATE decisions SET status = 'active' WHERE id = $id;

-- Rejected by the database: draft -> superseded
UPDATE decisions SET status = 'superseded' WHERE id = $id;
-- Error: InvalidStateTransition { from: "draft", to: "superseded" }
```

---

## Scenario 3: Cascading Invalidation

**Problem:** A decision was based on an entity's state. The entity changed. Every decision that relied on the old state — and every decision that cited those decisions — should be flagged.

```sql
CREATE TABLE intentions (
  id UUID PRIMARY KEY,
  goal TEXT NOT NULL,
  status TEXT NOT NULL
) STATE MACHINE (status: active -> [archived, completed])

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
```

When you archive an intention:

```sql
UPDATE intentions SET status = 'archived' WHERE id = $intention_id;
```

In one atomic transaction:
1. The intention transitions to `archived`
2. All decisions referencing it via FK transition to `invalidated`
3. Decisions citing those decisions (via CITES edges) also transition to `invalidated`
4. Invalidated decisions are excluded from vector similarity search

No application code. The database handles the entire cascade.

---

## Scenario 4: "Has Anyone Solved This Before?" — Hybrid Query

**Problem:** An agent has a new task. It needs to find relevant past decisions — semantically similar, still active, and traceable back to their basis entities.

```sql
-- Find decisions similar to the current task
WITH similar_decisions AS (
  SELECT id, description, confidence
  FROM decisions
  WHERE status = 'active'
  ORDER BY embedding <=> $task_embedding
  LIMIT 10
),
-- For each decision, find what entities it was based on
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
LEFT JOIN entities e ON e.id = be.b_id
```

This combines all three paradigms:
- **Vector:** find semantically similar decisions
- **Relational:** filter to active status, join with entity metadata
- **Graph:** traverse BASED_ON edges to find the basis

---

## Scenario 5: "What Just Became Stale?" — Decision Staleness

**Problem:** An observation arrived that changed an entity's state. Which active decisions relied on the entity's *old* state?

```sql
-- The entity that changed
-- (application records the observation and updates the entity)

-- Find all decisions that were BASED_ON this entity
WITH affected AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (entity)<-[:BASED_ON]-(decision)
    WHERE entity.id = $changed_entity_id
    COLUMNS (decision.id AS b_id)
  )
)
SELECT d.id, d.description, d.status, d.confidence
FROM decisions d
INNER JOIN affected a ON d.id = a.b_id
WHERE d.status = 'active'
```

The PROPAGATE constraints (Scenario 3) handle automatic invalidation. This query is for when the application wants to inspect what *would* be affected before triggering a state change.

---

## Scenario 6: Emergent Unknown Detection — "Things You Didn't Know You Didn't Know"

**Problem:** An agent makes a decision based on an entity — say, a service's configuration. The decision's reasoning mentions the region and instance type, but doesn't enumerate every property it depends on. Later, an observation adds a *completely new* property to the entity — one that didn't exist at decision time. Maybe operations added a `rate_limit` field, or a `compliance_tier` appeared after a policy change. The decision was made without considering this property, and no one knew to watch for it.

This is the hardest problem in agentic memory: you can't search for something you don't know exists.

contextdb solves it structurally. A decision is `BASED_ON` an entity *as a whole*, not on specific fields. When any property changes — including properties that are entirely new — the database detects it:

```sql
-- Entity at decision time had: {region: "us-east-1", instance_type: "m5.large"}
-- Observation adds a new property: {region: "us-east-1", instance_type: "m5.large", rate_limit: 500}

-- The basis_diff shows: rate_limit: null → 500
-- (null = didn't exist when the decision was made)

-- Find decisions that were BASED_ON this entity — they're all potentially stale
WITH affected AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (entity)<-[:BASED_ON]-(decision)
    WHERE entity.id = $changed_entity_id
    COLUMNS (decision.id AS b_id)
  )
)
SELECT d.id, d.description, d.confidence
FROM decisions d
INNER JOIN affected a ON d.id = a.b_id
WHERE d.status = 'active'
```

The agent didn't know to watch for `rate_limit`. No one configured a trigger for it. But because the decision was linked to the entity via a `BASED_ON` edge, the graph traversal finds it, and the application can compute the basis diff: the entity's state at decision time vs. now.

Combined with cascading invalidation (Scenario 3), this propagates further: if decision D1 is invalidated because an unknown property appeared, and decision D2 cites D1 via a `CITES` edge, D2 is also flagged — automatically, via bounded BFS, within the same transaction.

This is not a filter you configure. It's a structural property of the graph: link decisions to what they're based on, and the database will tell you when the basis changes — even in ways nobody anticipated.

---

## Scenario 7: Graph Neighborhood + Vector Search

**Problem:** An agent is working on a task. It needs context — but not from the entire database. It needs relevant observations *within the neighborhood of entities related to the current task*.

```sql
-- Start from the task entity, traverse RELATES_TO edges to build a neighborhood
WITH neighborhood AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (task)-[:RELATES_TO]->{1,2}(related)
    WHERE task.id = $task_id
    COLUMNS (related.id AS b_id)
  )
),
-- Find observations linked to entities in the neighborhood
candidates AS (
  SELECT o.id, o.data, o.embedding
  FROM observations o
  INNER JOIN neighborhood n ON o.entity_id = n.b_id
)
-- Rank by semantic similarity to the current query
SELECT id, data FROM candidates
ORDER BY embedding <=> $query_embedding
LIMIT 5
```

Graph-first (narrow the scope), then vector (rank within scope). The inverse — vector-first, then graph — works equally well for different access patterns.

---

## Scenario 8: Digest Provenance — Tracing Conversations

**Problem:** Decisions and observations were extracted from conversations. When reviewing a decision, you need to trace it back to the conversation that produced it.

```sql
-- Conversation digests are immutable with embeddings for search
CREATE TABLE digests (
  id UUID PRIMARY KEY,
  source TEXT NOT NULL,
  summary TEXT NOT NULL,
  embedding VECTOR(384)
) IMMUTABLE

-- Find relevant conversations, then trace what was extracted from them
WITH relevant AS (
  SELECT id FROM digests
  ORDER BY embedding <=> $query
  LIMIT 10
),
extracted AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges
    MATCH (digest)<-[:EXTRACTED_FROM]-(derived)
    WHERE digest.id IN (SELECT id FROM relevant)
    COLUMNS (derived.id AS b_id)
  )
)
SELECT d.id, d.description, d.status
FROM decisions d
INNER JOIN extracted e ON d.id = e.b_id
```

---

## Scenario 9: Local-to-Server Sync

**Problem:** Multiple local instances accumulate data independently — a developer's laptop, a browser plugin, a mobile app — sometimes offline for hours or days. When they reconnect, data should sync without duplication, with clear conflict resolution.

```bash
# Central server
contextdb-server --tenant-id production --db-path ./server.db

# Instance 1 (laptop app) — works offline, pushes when connected
contextdb-cli ./local1.db --tenant-id production
contextdb> CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, reading REAL);
contextdb> INSERT INTO sensors VALUES ('...', 'temp-north', 23.5);
contextdb> .sync push
Pushed: 2 applied, 0 skipped, 0 conflicts

# Instance 2 (another machine) — pulls and gets everything, including schema
contextdb-cli ./local2.db --tenant-id production
contextdb> .sync pull
Pulled: 2 applied, 0 skipped, 0 conflicts
contextdb> SELECT * FROM sensors;
```

Sync is bidirectional, per-table configurable:

```sql
-- This table only pushes (observations flow up, never down)
ALTER TABLE observations SET SYNC_CONFLICT_POLICY 'insert_if_not_exists'
```

```
contextdb> .sync direction observations Push
contextdb> .sync direction decisions Both
contextdb> .sync direction scratch None
```

Each instance stores its data in a single file. No WAL directories, no journal files, no auxiliary indexes. Back up the file, copy it to another machine, or embed it in a container image.

Local instances use WebSocket transport (`ws://`) to connect to the NATS server. This is deliberate — WebSocket connections traverse NAT and firewalls without hole-punching, so a laptop behind a home router, a browser plugin, or a mobile app can all sync without network configuration.

Sync survives restarts. Change logs are ephemeral — they exist in memory for incremental sync while the process runs. After a restart, the database reconstructs a full-state snapshot from persisted data when a peer requests changes. Large payloads (tables with high-dimensional vectors or large JSON blobs) are automatically chunked below NATS's message limit and reassembled on the receiver.

In Rust:

```rust
use contextdb_server::SyncClient;

let client = SyncClient::new(db.clone(), "ws://nats:9222", "production");
client.push().await?;
client.pull_default().await?;
```

---

## Scenario 10: Retention and Cleanup

**Problem:** Scratch data, temporary observations, and working state should expire automatically. But synced data shouldn't be purged until it's been replicated.

```sql
-- Scratch notes expire after 24 hours
CREATE TABLE scratch (
  id UUID PRIMARY KEY,
  content TEXT,
  created_at TIMESTAMP DEFAULT NOW()
) RETAIN 24 HOURS

-- Observations are kept for 30 days, but not purged until synced
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  data JSON,
  embedding VECTOR(384)
) RETAIN 30 DAYS SYNC SAFE
```

The background pruning loop handles cleanup. `SYNC SAFE` ensures no data is lost before it reaches the server.

---

## Scenario 11: Reactive and Time-Based Processing

**Problem:** When data changes, downstream systems need to react — re-embed vectors, push to external APIs, trigger analysis pipelines. And some actions need to happen on a schedule — periodic staleness sweeps, retention cleanup, health checks. Building this with external cron jobs and polling is fragile: they run outside the transaction boundary, miss events during downtime, and duplicate logic.

contextdb provides two primitives that compose into cron-like behavior without external infrastructure:

**Event-driven:** `db.subscribe()` delivers commit events in real-time. The application reacts to specific table changes:

```rust
let rx = db.subscribe();  // std::sync::mpsc::Receiver<CommitEvent>

std::thread::spawn(move || {
    while let Ok(event) = rx.recv() {
        // event.tables_changed: which tables were modified
        // event.lsn: position in the change log
        // event.row_count: total operations in this commit
        // event.source: User | AutoCommit | SyncPull

        match event.source {
            CommitSource::SyncPull => {
                // Data arrived from another instance — check for conflicts
            }
            _ => {
                if event.tables_changed.contains(&"observations".to_string()) {
                    // New local observation — run impact analysis
                }
                if event.tables_changed.contains(&"invalidations".to_string()) {
                    // Invalidation detected — alert the agent
                }
            }
        }
    }
});
```

The receiver is `std::sync::mpsc::Receiver` (blocking). Use `std::thread::spawn` or `tokio::task::spawn_blocking` if integrating with async code.

Subscriptions are best-effort. If a subscriber's channel is full, the event is dropped — the commit never blocks. Applications that need guaranteed delivery should treat subscriptions as triggers to re-query, not as a durable event log.

**Time-driven:** `RETAIN` handles TTL-based expiry at the database level. The application can schedule periodic queries using standard Rust — since contextdb is an embedded library, a periodic task is just a function call:

```rust
// Periodic staleness sweep — every hour, find decisions
// whose basis entities changed since they were last reviewed
let db_clone = db.clone();
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(3600));
    loop {
        interval.tick().await;
        let result = db_clone.execute(
            "SELECT d.id, d.description FROM decisions d
             WHERE d.status = 'active'
             AND d.context_id = $ctx",
            &params,
        );
        // Application runs impact analysis on each active decision
    }
});
```

No external scheduler. No cron daemon. No polling service. The database is in-process — time-driven actions are just loops with `db.execute()` calls, and event-driven actions are subscription handlers. Both run inside the same process with full transactional guarantees.

---

## Scenario 12: Intentions, Blueprints, and Decision Lineage

**Problem:** Decisions don't appear from nowhere. An agent has an *intention* — a goal it's pursuing. When multiple agents across different instances express the same intent in different words ("monitor auth-service health", "watch the login system for failures", "track auth uptime"), those should normalize to the same pattern. And when a decision is superseded, you need to trace the full chain: which intention it served, what it was based on, and what replaced it.

Blueprints are parameterized intent templates. They normalize diverse expressions into a canonical form with typed slots:

```sql
-- Reusable intent templates
CREATE TABLE blueprints (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  template TEXT NOT NULL,
  slots JSON NOT NULL,
  embedding VECTOR(384)
) IMMUTABLE

-- What we're trying to achieve — an instance of a blueprint
CREATE TABLE intentions (
  id UUID PRIMARY KEY,
  goal TEXT NOT NULL,
  status TEXT NOT NULL,
  blueprint_id UUID,
  bindings JSON,
  context_id UUID NOT NULL,
  embedding VECTOR(384)
) STATE MACHINE (status: active -> [archived, completed, paused], paused -> [active])

-- What we chose to do — always in service of an intention
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL,
  intention_id UUID REFERENCES intentions(id)
    ON STATE archived PROPAGATE SET invalidated,
  context_id UUID NOT NULL,
  embedding VECTOR(384)
) STATE MACHINE (status: active -> [invalidated, superseded])
  PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated
  PROPAGATE ON STATE invalidated EXCLUDE VECTOR
```

The flow:

```sql
-- 1. Blueprint exists: "Monitor {service_name} for {failure_type}"
INSERT INTO blueprints (id, name, template, slots, embedding)
VALUES ($bp_id, 'service-health-monitor',
        'Monitor {service_name} for {failure_type}',
        '{"service_name": "TEXT", "failure_type": "TEXT"}',
        $bp_embedding);

-- 2. Agent creates an intention, bound to the blueprint
INSERT INTO intentions (id, goal, status, blueprint_id, bindings, context_id, embedding)
VALUES ($int_id, 'Monitor auth-service for login failures', 'active',
        $bp_id, '{"service_name": "auth-service", "failure_type": "login failures"}',
        $ctx_id, $int_embedding);

-- 3. Link intention to blueprint
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES ($edge_id, $int_id, $bp_id, 'INSTANTIATES');

-- 4. Decision serves the intention, based on entity state
INSERT INTO decisions (id, description, status, confidence, intention_id, context_id, embedding)
VALUES ($dec_id, 'Alert at 200ms p99 latency', 'active', 0.85,
        $int_id, $ctx_id, $dec_embedding);

INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES ($e1, $dec_id, $int_id, 'SERVES'),
       ($e2, $dec_id, $entity_id, 'BASED_ON');
```

When the decision is invalidated and replaced:

```sql
-- New decision supersedes the old one, serves the same intention
INSERT INTO decisions (id, description, status, confidence, intention_id, context_id, embedding)
VALUES ($new_dec_id, 'Alert at 350ms p99 — adjusted for eu-west-1', 'active', 0.9,
        $int_id, $ctx_id, $new_embedding);

INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES ($e3, $new_dec_id, $int_id, 'SERVES'),
       ($e4, $new_dec_id, $dec_id, 'SUPERSEDES'),
       ($e5, $new_dec_id, $entity_id, 'BASED_ON');

UPDATE decisions SET status = 'superseded' WHERE id = $dec_id;
```

Now the lineage is queryable: intention → decisions that served it (current and historical) → what each was based on → what superseded what. Blueprints let the server aggregate across instances: "how many active intentions match the `service-health-monitor` blueprint across all contexts?"

---

## Scenario 13: Outcomes and Learning From Results

**Problem:** An agent keeps recommending approaches that failed last time. It finds semantically similar past decisions, but has no signal for whether they *worked*. Without a feedback loop, the agent's memory is a filing cabinet — organized but not intelligent.

```sql
-- Track what actually happened after a decision
CREATE TABLE outcomes (
  id UUID PRIMARY KEY,
  decision_id UUID NOT NULL,
  success BOOLEAN NOT NULL,
  impact TEXT,
  measured_at TIMESTAMP DEFAULT NOW()
) IMMUTABLE

-- Link decision to its outcome
-- edges table already exists from earlier scenarios
```

Record an outcome:

```sql
INSERT INTO outcomes (id, decision_id, success, impact)
VALUES ($outcome_id, $decision_id, TRUE, 'p99 latency dropped to 180ms');

INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES ($edge_id, $decision_id, $outcome_id, 'HAS_OUTCOME');
```

Now precedent search factors in outcomes — decisions that worked rank higher:

```sql
WITH similar AS (
  SELECT d.id, d.description, d.confidence
  FROM decisions d
  WHERE d.status IN ('active', 'superseded')
  ORDER BY d.embedding <=> $task_embedding
  LIMIT 20
)
SELECT s.id, s.description, s.confidence,
       o.success, o.impact
FROM similar s
LEFT JOIN outcomes o ON o.decision_id = s.id
```

The application computes effective confidence: `decision.confidence * outcome.success_rate`. Decisions with poor outcomes sink in rankings. The agent learns from experience, not just similarity.

---

## Scenario 14: Invalidation as a First-Class Workflow

**Problem:** Scenario 3 showed automatic cascading invalidation — the database transitioning decisions to `invalidated`. But an agent developer needs more than a status flag. When a decision is invalidated, the application needs to know: *what exactly changed* (basis diff), *how critical is this* (severity), *has anyone acknowledged it* (lifecycle), and *what replaced it* (resolution).

Invalidations are first-class objects with their own state machine:

```sql
CREATE TABLE invalidations (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  affected_decision_id UUID NOT NULL,
  trigger_observation_id UUID NOT NULL,
  basis_diff JSON NOT NULL,
  severity TEXT NOT NULL,
  detected_at TIMESTAMP DEFAULT NOW(),
  resolved_at TIMESTAMP,
  resolution_decision_id UUID
) STATE MACHINE (status: pending -> [acknowledged], acknowledged -> [resolved, dismissed])
```

When impact analysis detects staleness, the application creates an invalidation record:

```sql
-- Record the invalidation with its basis diff
INSERT INTO invalidations (id, status, affected_decision_id, trigger_observation_id,
                           basis_diff, severity)
VALUES ($inv_id, 'pending', $decision_id, $observation_id,
        '{"field": "region", "old": "us-east-1", "new": "eu-west-1"}',
        'warning');

-- Link it via graph edges
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES ($e1, $inv_id, $decision_id, 'INVALIDATES'),
       ($e2, $inv_id, $observation_id, 'TRIGGERED_BY');
```

The invalidation lifecycle is the agent's inbox for stale knowledge:

```sql
-- What needs attention? (pending invalidations, critical first)
SELECT i.id, i.severity, i.basis_diff,
       d.description, d.confidence
FROM invalidations i
INNER JOIN decisions d ON d.id = i.affected_decision_id
WHERE i.status = 'pending'

-- Acknowledge it
UPDATE invalidations SET status = 'acknowledged' WHERE id = $inv_id;

-- Resolve it by creating a replacement decision
UPDATE invalidations SET status = 'resolved',
       resolved_at = NOW(),
       resolution_decision_id = $new_decision_id
WHERE id = $inv_id;
```

Severity is confidence-weighted: a high-confidence decision (0.9) that is the sole active decision serving its intention → `critical`. A low-confidence decision (0.3) with alternatives → `info`. The application controls the thresholds.

---

## Scenario 15: Enforceable Policy — Your Schema, Your Rules

**Problem:** The preceding scenarios use an agentic memory ontology (intentions, decisions, observations). But contextdb doesn't mandate any particular schema. It provides **enforceable policy primitives** — `STATE MACHINE`, `DAG`, `PROPAGATE`, `IMMUTABLE`, `RETAIN` — that you declare on *your* tables. The database guarantees them. No application code can bypass them, no consumer can forget to check them, no race condition can violate them.

Here's the same constraint system applied to a content workflow:

```sql
CREATE TABLE articles (
  id UUID PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT,
  status TEXT NOT NULL,
  embedding VECTOR(384)
) STATE MACHINE (status: draft -> [review], review -> [published, rejected],
                 rejected -> [draft], published -> [archived])
  PROPAGATE ON STATE archived EXCLUDE VECTOR

CREATE TABLE categories (
  id UUID PRIMARY KEY,
  parent_id UUID,
  name TEXT NOT NULL
)

CREATE TABLE category_edges (
  id UUID PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
) DAG('CHILD_OF')

CREATE TABLE edit_log (
  id UUID PRIMARY KEY,
  article_id UUID NOT NULL,
  editor TEXT NOT NULL,
  diff JSON,
  edited_at TIMESTAMP DEFAULT NOW()
) RETAIN 365 DAYS
```

Same enforceable policies, completely different domain:
- `STATE MACHINE` enforces the editorial workflow — no application code can jump from draft to published
- `DAG` prevents circular category hierarchies — no insert can create a cycle
- `RETAIN` expires old edit logs after a year — no external cron job needed
- `PROPAGATE ON STATE archived EXCLUDE VECTOR` removes archived articles from similarity search — no manual cleanup

These are database-level guarantees. The application declares the policy once in the schema. Every consumer — every agent, every API, every sync peer — is bound by it.

Schemas evolve at runtime too. As an agent learns about a new domain, it adds columns, renames fields, or drops what's no longer useful:

```sql
ALTER TABLE observations ADD COLUMN confidence REAL;
ALTER TABLE entities RENAME COLUMN entity_type TO kind;
ALTER TABLE scratch DROP COLUMN debug_notes;
```

Existing rows get `NULL` for newly added columns. Primary key columns cannot be dropped or renamed. DDL changes propagate through sync — when one instance adds a column, other instances pick it up on the next pull.

The agentic memory ontology is one powerful schema built with these tools. It's not the only one.

---

## Scenario 16: The Full Picture

A realistic agentic memory schema combines all of the above:

```sql
-- Isolation boundary
CREATE TABLE contexts (id UUID PRIMARY KEY, name TEXT NOT NULL)

-- Things in the world
CREATE TABLE entities (
  id UUID PRIMARY KEY,
  entity_type TEXT NOT NULL,
  name TEXT NOT NULL,
  context_id UUID NOT NULL,
  properties JSON
)

-- What happened (append-only facts, auto-expire after 90 days)
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  observation_type TEXT NOT NULL,
  data JSON NOT NULL,
  entity_id UUID NOT NULL,
  source TEXT NOT NULL,
  embedding VECTOR(384),
  recorded_at TIMESTAMP DEFAULT NOW()
) RETAIN 90 DAYS SYNC SAFE

-- Reusable intent templates
CREATE TABLE blueprints (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  template TEXT NOT NULL,
  slots JSON NOT NULL,
  embedding VECTOR(384)
) IMMUTABLE

-- What we're trying to achieve — may instantiate a blueprint
CREATE TABLE intentions (
  id UUID PRIMARY KEY,
  goal TEXT NOT NULL,
  status TEXT NOT NULL,
  blueprint_id UUID,
  bindings JSON,
  context_id UUID NOT NULL,
  embedding VECTOR(384)
) STATE MACHINE (status: active -> [archived, completed, paused], paused -> [active])

-- What we chose to do — always in service of an intention
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL,
  intention_id UUID REFERENCES intentions(id)
    ON STATE archived PROPAGATE SET invalidated,
  context_id UUID NOT NULL,
  embedding VECTOR(384)
) STATE MACHINE (status: active -> [invalidated, superseded])
  PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated
  PROPAGATE ON STATE invalidated EXCLUDE VECTOR
  PROPAGATE ON STATE superseded EXCLUDE VECTOR

-- What actually happened after a decision
CREATE TABLE outcomes (
  id UUID PRIMARY KEY,
  decision_id UUID NOT NULL,
  success BOOLEAN NOT NULL,
  impact TEXT,
  measured_at TIMESTAMP DEFAULT NOW()
) IMMUTABLE

-- Staleness notices with their own lifecycle
CREATE TABLE invalidations (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  affected_decision_id UUID NOT NULL,
  trigger_observation_id UUID NOT NULL,
  basis_diff JSON NOT NULL,
  severity TEXT NOT NULL,
  detected_at TIMESTAMP DEFAULT NOW(),
  resolved_at TIMESTAMP,
  resolution_decision_id UUID
) STATE MACHINE (status: pending -> [acknowledged], acknowledged -> [resolved, dismissed])

-- Conversation summaries (retrieval anchor)
CREATE TABLE digests (
  id UUID PRIMARY KEY,
  source TEXT NOT NULL,
  summary TEXT NOT NULL,
  embedding VECTOR(384),
  context_id UUID NOT NULL
) IMMUTABLE

-- Relationships between everything
CREATE TABLE edges (
  id UUID PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
) DAG('DEPENDS_ON', 'BASED_ON', 'CITES')
```

This schema gives an agent:
- **Structured state** — entities with typed properties, decisions with enforced lifecycles
- **Semantic recall** — vector search across observations, decisions, and digests
- **Provenance** — graph traversal traces what was based on what, who cited whom
- **Automatic invalidation** — state changes cascade through the graph, with first-class invalidation records
- **Feedback loop** — outcomes record whether decisions worked, weighting future precedent search
- **Intent normalization** — blueprints collapse diverse expressions into canonical patterns
- **Sync** — local instances work offline, push/pull when connected
- **Retention** — old observations expire, but not before syncing
