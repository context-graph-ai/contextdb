# Capability Index — what contextdb is, is not, and where it stops

One page to orient a new reader or an agent before it reads anything else: what contextdb
actually does, what it deliberately does not do, and the numbers that bound it. Every claim
below is backed by a doc section or a test in this repo — this page is a map, not a new source
of truth.

## What contextdb is

- **An embedded, schema-free, multi-model database.** Relational tables, graph traversal, and
  vector similarity search, all under one MVCC transaction, in a single file, in a single
  process. See [Why contextdb?](why-contextdb.md) and [Architecture](architecture.md).
- **A policy engine, not just storage.** `STATE MACHINE`, `DAG`, `IMMUTABLE`, `PROPAGATE`,
  `RETAIN`, and `HISTORY CURRENT ONLY` are declared in table DDL and enforced by the database
  itself, not by application code or bypassable triggers. See
  [Query Language](query-language.md).
- **PostgreSQL-compatible SQL** for relational work, [pgvector](https://github.com/pgvector/pgvector)
  syntax (`<=>`) for vector search, and a bounded subset of
  [SQL/PGQ](https://www.iso.org/standard/76120.html) `GRAPH_TABLE ... MATCH` for graph queries
  — familiar surface, not a new query language to learn.
- **Bidirectional dial-by-key sync.** Every database syncs changesets with a declared, per-table
  conflict policy (`SYNC CONFLICT KEEP FIRST | KEEP LATEST`) over the Iroh transport — nodes
  reach each other by cryptographic identity, no broker installed by default. See
  [Architecture — Sync](architecture.md).
- **A durable work ledger and a media/blob plane**, both pure library surfaces today (no CLI
  flag): jobs/claims/leases for distributing work across machines, and content-addressed blob
  ingest/fetch/reclaim for large media. See [Architecture](architecture.md).
- **A CLI, a REPL, and a Rust library** — `contextdb`/`contextdb-server` binaries plus the
  `contextdb-engine` crate as an embeddable dependency. See [CLI Reference](cli.md) and
  [Getting Started](getting-started.md).

## What contextdb is not

- **Not a schema.** The agentic-memory tables used as running examples throughout these docs
  (`decisions`, `observations`, `intentions`, `digests`) are one example schema you can delete
  or replace. contextdb ships no built-in schema and requires none — see the README and
  [Why contextdb?](why-contextdb.md).
- **Not a general-purpose database.** The SQL/PGQ graph subset covers bounded traversal for
  agentic workloads, not the full standard; contextdb is a focused tool for agent memory, not a
  data warehouse. See the [Design Envelope](why-contextdb.md#design-envelope).
- **Not a message broker or a third-party relay by default.** The default configuration
  contacts no external service; a self-hosted or opt-in `iroh-relay` is only for introducing
  peers across networks that can't reach each other directly. See
  [Architecture — Sync](architecture.md).
- **Not multi-tenant orchestration.** `--tenant-id` is a sync namespace — every client and
  server sharing one tenant ID replicate with each other. Running many isolated tenants behind
  one deployment, cross-tenant intelligence, and managed hosting are commercial-layer concerns
  outside this repo, not something contextdb itself provides.
- **Not a majority-vote distributed system.** Conflict arbitration is declared per table
  (`KEEP FIRST` / `KEEP LATEST`) and, for the five hub-refereed work-ledger tables, hardcoded to
  `keep_first` — there is no quorum or leader election; a hub is a specific, addressable machine
  an edge dials.
- **Not yet bound to Python or TypeScript.** Rust library and CLI today; other language bindings
  are on the roadmap (README).
- **Not schema-stable across the wire pre-release.** Until the first tagged release, the sync
  wire protocol may change in place with no version bump; every machine in a fleet tracks the
  same `dev` build. Post-release, a peer speaking a different protocol version is refused loudly
  on push, pull, and status — nothing is lost, but nothing syncs either, until every participant
  upgrades.

## Where it stops — the numbers

- **Design envelope** (from [Why contextdb? — Design Envelope](why-contextdb.md)): 10K–1M rows
  per database, sparse graphs with bounded traversal (depth ≤ 10), append-heavy writes with
  small transactions, laptops and ARM64 devices as the target hardware (browser/mobile via WASM
  is a future direction, not shipped).
- **Nine reserved table names** (`work_jobs`, `work_claims`, `work_results`, `work_failures`,
  `work_cancellations`, `work_inputs`, `work_capabilities`, `peer_directory`,
  `work_node_contacts`) carry a fixed, engine-owned shape and policy — an operator table using
  any other name is entirely unrestricted. See [Architecture](architecture.md).
- **A table that syncs needs a declared identity** (a `PRIMARY KEY` or an indexed `id` column) —
  a keyless table with any sync direction other than `SYNC OFF` refuses loudly rather than
  silently failing to replicate. See [Query Language](query-language.md).
- **Memory and disk are configurable, not unbounded**: `SET MEMORY_LIMIT` and `SET DISK_LIMIT`
  (or the process-start bootstrap functions) are how a caller sets a ceiling; there is no
  hard-coded default cap.

---

Linked from [`AGENTS.md`](../AGENTS.md). This page is generated by hand today; if it drifts from
the sections it cites, the cited doc is the source of truth.
