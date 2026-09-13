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
  This release emits and accepts sync **protocol 7**. The supported skew window is the current
  protocol plus the two previous **released** protocols.
  <!-- enforced by: protocol_version_bump_tests::first_release_accepts_only_protocol_seven, protocol_version_bump_tests::every_noncurrent_envelope_is_rejected_by_encoding_and_decoding -->
- **A durable work ledger and a media/blob plane**, both pure library surfaces today (no CLI
  flag): jobs/claims/leases for distributing work across machines, and content-addressed blob
  ingest/fetch/reclaim for large media. See [Architecture](architecture.md).
- **A CLI, a REPL, and a Rust library** — `contextdb`/`contextdb-server` binaries plus the
  `contextdb-engine` crate as an embeddable dependency. See [CLI Reference](cli.md) and
  [Getting Started](getting-started.md).
- **Maintained vector search.** A vector column may declare local `PARTITION_KEY` layouts and an
  effective default limit of 256 live-plus-snapshot-retained layouts. One, several, or all
  authorized layouts produce one global top-k; a partition is not a tenant, authorization rule, or
  sync direction. <!-- enforced by: vector_partition_declaration_contract::partitioned_vector_schema_survives_reopen_with_canonical_defaults, vector_partition_query_contract::finite_in_partition_scope_merges_before_one_limit, vector_partition_query_contract::no_partition_key_predicate_returns_one_global_limited_answer, vector_partition_query_contract::partition_scope_does_not_weaken_context_scope_or_principal_filters, vector_partition_sync_inspection_contract::partitioned_sync_derives_receiver_membership_without_changing_owner_pairing --> Maintained indexed routes have an at-least-95% recall-at-10 acceptance target
  against exact search on the same stored column. <!-- enforced by: vector_held_out_native_reference::held_out_unfiltered_native_reference_recovers_the_required_neighbors, vector_serving_merge_contract::held_out_filtered_graph_search_preserves_quality_at_sparse_and_broad_selectivities -->
  `AUTO_INDEX_AT` and HNSW (`M`, `EF_CONSTRUCTION`, `EF_SEARCH`) are
  declarable per-column workload policy; default silence keeps the compatibility profile. See
  [Query Language](query-language.md) and [Architecture](architecture.md).
  <!-- enforced by: vector_policy_declaration_contract::create_and_add_column_declarations_render_inspect_and_survive_reopen_and_export, vector_policy_resolver_contract::declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries -->

## What contextdb is not

- **Not a schema.** The agentic-memory tables used as running examples throughout these docs
  (`decisions`, `observations`, `intentions`, `digests`) are one example schema you can delete
  or replace. See the [README](../README.md) and
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
- **Schema vocabulary a peer cannot read holds back only that table.** The holdback names the
  affected table, missing capability, and node to upgrade while unrelated tables continue; the
  held table resumes when the capability is available.
  <!-- enforced by: vector_schema_mixed_version_compatibility_contract::older_receiver_keeps_ordinary_tables_flowing_then_resumes_the_held_table_after_upgrade, vector_schema_mixed_version_push_compatibility_contract::newer_edge_keeps_ordinary_tables_flowing_then_resumes_the_held_table_after_hub_upgrade -->

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
  the `CREATE TABLE` is accepted, and a keyless table declared with any sync direction other than
  `SYNC OFF` then refuses loudly at its first push rather than silently failing to replicate. See
  [Query Language](query-language.md).
- **Memory and disk are configurable, not unbounded**: `SET MEMORY_LIMIT` and `SET DISK_LIMIT`
  (or the process-start bootstrap functions) are how a caller sets a ceiling; there is no
  hard-coded default cap.
- **A CLI read is bounded, and a bounded read is complete or refused**: one `SELECT` succeeds only
  within the declared ceilings — 500 rows and 4 MiB by default — and crossing either publishes no
  rows and refuses with `owner_limit_exceeded`. Nothing is ever silently truncated, there is no
  flag that disables the ceiling, and `.cursor open` / `.cursor fetch` is how a larger result is
  paged. The embedded `Database::execute` library call keeps its uncapped contract.
  See [CLI Reference](cli.md).
- **All-scope vector search is bounded, not a promise to load every layout at once.** The engine
  fans out under the reader's work, memory, and cancellation limits and either returns one complete
  global answer or refuses honestly. <!-- enforced by: vector_search_mode_bounded_contract::exact_memory_admission_uses_selected_authorized_rows_as_unrelated_vectors_grow, vector_search_mode_bounded_contract::filtered_many_partition_search_reuses_partition_local_candidates, vector_lazy_restart_residency_contract::reopen_keeps_saved_partitions_dormant_then_reclaims_one_least_recently_used_route_under_pressure --> The edge-device memory target is `SET MEMORY_LIMIT 2G`
  (CLI `--memory-limit 2G`), exactly **2147483648 bytes**. <!-- enforced by: statement_effect_contract::binary_memory_declaration_accepts_quoted_and_unquoted_sizes --> Charged vector/index bytes and
  whole-process RSS, including build/repair/restart lifecycle work, are reported separately. See
  [Architecture](architecture.md) and [CLI Reference](cli.md).

---

Linked from [`AGENTS.md`](../AGENTS.md). This page is generated by hand today; if it drifts from
the sections it cites, the cited doc is the source of truth.


### Tenant-governed event custody (Unreleased)

Hub policy declaration/binding, transactional manifests, durable outcomes, lost-ack readback, and
policy-governed local discard are implemented in the engine. See
[SQL and API guidance](query-language.md#tenant-policy-and-event-custody). A unit's complete application rows are compared with BLAKE3, and only a durable authenticated
outcome ends pending delivery; a watermark is not a receipt. An old hub image that lost custody
revokes old credit automatically when the edge next checks the hub, without erasing edge rows.
Multi-table fleet purge and local discard each use one erasure boundary. Listed `SYNC OFF` predicates
select each edge's independently keyed rows; repeat delivery preserves fresh later creations.
The outcome journal consumes storage until its owning unit is purged or discarded locally.
Logical erasure does not promise forensic secure deletion or removal from operator-held backups;
those remain the separate erasure and backup-recovery boundaries.
