# Benchmarking

A catalogue of contextdb's benchmark targets: what each measures, which
measurement profile runs it, and the rules for writing new ones. Benchmarks are
descriptive. The product's acceptance bars are listed in the
[performance guide](performance/README.md#acceptance-bars); this page does not
define, relax or replace them.

## How to run benchmarks

All performance run commands — profiles, preparing clean checkouts, baseline
and candidate runs, comparison and publication — are in
[`docs/performance/README.md`](performance/README.md). They are not repeated
here.

## Targets

Every target is declared in its crate's `Cargo.toml` and uses Criterion.
`scripts/ci-performance.py` assigns targets to profiles and is authoritative if
this table and the script disagree. Targets marked `all only` run only in the
`all` profile.

| Crate | Target | Profile | What it measures |
|---|---|---|---|
| `contextdb-parser` | `parser_throughput` | `all` only | Parser-only microbenchmark. |
| `contextdb-engine` | `engine_throughput` | `vector` (filter `nv_`) | Local engine behaviour; its `nv_` cases cover named-vector indexes. |
| `contextdb-engine` | `engine_full_throughput` | `storage` | Larger write/reopen and mixed workflow paths. |
| `contextdb-engine` | `storage_scale_hardening` | `storage` | Table-local index maintenance, reopen locality, wide-row commit batch size and file-backed single-row autocommit. Its fixtures assert locality counters before timing, so a failure is a correctness/locality failure first. |
| `contextdb-engine` | `rank_policy_recall` | `vector` | Rank-policy recall. |
| `contextdb-engine` | `mixed_workflows_pr` | `workflows` | Bounded mixed graph, relational and vector workflows. |
| `contextdb-engine` | `subscriptions_pr` | `workflows` | Bounded subscription fan-out and backpressure workloads. |
| `contextdb-server` | `sync_pr` | `workflows` | Bounded sync workloads through direct sync APIs, without CLI/process overhead. |
| `contextdb-server` | `server_throughput` | `system` | CLI throughput. |
| `contextdb-server` | `server_sync_system` | `system` | System sync with authenticated Iroh and file-backed CLI/server processes; needs the release binaries the `system` profile builds first. |
| `contextdb-server` | `blob_transfer` | `all` only | Blob transfer. |
| `contextdb-engine` | `indexed_scan_filter_pk_inserts`, `indexed_scan_filter_file_backed_open`, `indexed_scan_filter_in_memory_pk_inserts`, `indexed_scan_filter_entity_list`, `indexed_scan_filter_non_id_pk_inserts` | `all` only | Indexed scan and filter paths. |
| `contextdb-engine` | `acceptance_three_paradigm_query`, `sql_surface_distinct`, `graph_adjacency_probe`, `composite_index_routing` | `all` only | Query-surface and routing paths; see each source. |
| `contextdb-engine` | `tier1_atomic_visibility_under_contention`, `tier1_event_bus_throughput`, `tier1_context_guard_overhead`, `tier1_scope_label_overhead`, `tier1_acl_overhead` | `all` only | Visibility, event bus, context, scope-label and ACL overheads. |
| `contextdb-engine` | `tier2_commit_validate_overhead`, `tier2_fk_revalidation_overhead`, `tier2_state_machine_overhead` | `all` only | Commit validation, foreign-key revalidation and state-machine overheads. |
| `contextdb-engine` | `commit_write_volume_scaling`, `sigterm_drain_in_flight_persistence`, `trigger_throughput`, `version_cleanup_scaling` (feature `test-seams`) | `all` only | Commit volume scaling, shutdown drain persistence, trigger throughput and version cleanup scaling. |

Some sources assert internal budgets or counters (for example a millisecond
budget inside a bench). Those assertions are real checks, but they are not
product acceptance bars.

## Design rules

- Keep setup outside timed loops unless startup cost is the thing being measured.
- Keep benchmarks bounded enough to avoid Criterion stretching runs into multi-minute samples.
- Derive system benchmarks from acceptance and integration scenarios rather than synthetic isolated loops.
- If a benchmark spends its whole run on setup or build noise and never reaches useful Criterion output, treat that as a benchmark-shape problem to fix, not a valid result.
