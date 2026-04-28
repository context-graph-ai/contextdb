# Benchmarking

ContextDB now has explicit benchmark tiers instead of one undifferentiated suite.

## Targets

- `cargo bench -p contextdb-parser --bench parser_throughput`
  - parser-only microbench
- `cargo bench -p contextdb-engine --bench engine_throughput`
  - developer smoke bench for local engine behavior, including named-vector
    index smoke gates under the `nv_` filter
- `cargo bench -p contextdb-engine --bench engine_full_throughput`
  - heavier engine benchmarks for larger write/reopen and mixed workflow paths
- `cargo bench -p contextdb-engine --bench mixed_workflows_pr`
  - bounded PR-tier mixed graph + relational + vector workflows
- `cargo bench -p contextdb-engine --bench subscriptions_pr`
  - bounded PR-tier subscription fan-out and backpressure workloads
- `cargo bench -p contextdb-server --bench server_throughput`
  - CLI smoke bench
- `cargo bench -p contextdb-server --bench server_sync_system`
  - system sync bench with real NATS via testcontainers
- `cargo bench -p contextdb-server --bench sync_pr`
  - bounded PR-tier sync workloads using direct sync APIs

## Guidance

- Use the parser and engine smoke benches for quick local regression checks.
- Use `engine_full_throughput` when you want larger local workloads.
- Use `mixed_workflows_pr` and `subscriptions_pr` for realistic engine-level PR regression coverage.
- Use `sync_pr` when you want realistic sync regression coverage without CLI/process overhead in the timed path.
- Use `server_sync_system` only when you are intentionally testing sync/system behavior.
- The server sync bench is heavier because it uses release binaries, a real NATS container, and file-backed CLI/server processes.

## Suggested Commands

- smoke:
  - `timeout 180s cargo bench -p contextdb-engine --bench engine_throughput -- --sample-size 10 --measurement-time 0.05 --warm-up-time 0.05`
  - `timeout 900s cargo bench -p contextdb-engine --bench engine_throughput -- nv_ --save-baseline named-vector-indexes`
  - `timeout 180s cargo bench -p contextdb-server --bench server_throughput -- --sample-size 10 --measurement-time 0.05 --warm-up-time 0.05`
- PR:
  - `timeout 180s cargo bench -p contextdb-engine --bench engine_full_throughput -- --sample-size 10 --measurement-time 0.05 --warm-up-time 0.05`
  - `timeout 180s cargo bench -p contextdb-engine --bench mixed_workflows_pr -- --sample-size 10 --measurement-time 0.05 --warm-up-time 0.05`
  - `timeout 180s cargo bench -p contextdb-engine --bench subscriptions_pr -- --sample-size 10 --measurement-time 0.05 --warm-up-time 0.05`
  - `timeout 180s cargo bench -p contextdb-server --bench sync_pr -- chunked_large_pull_600_rows --sample-size 10 --measurement-time 0.05 --warm-up-time 0.05`
- system:
  - `timeout 180s cargo bench -p contextdb-server --bench server_sync_system -- --sample-size 10 --measurement-time 0.05 --warm-up-time 0.05`

Always use `timeout` on heavier runs. If a benchmark spends the whole run on setup/build noise and never reaches useful Criterion output, treat that as a benchmark-shape problem to fix rather than a valid perf result.

The named-vector `nv_` cases use bounded local defaults so they are practical as
a smoke gate. Set `CONTEXTDB_NV_BENCH_FULL=1` or the per-case
`CONTEXTDB_NV_*` sizing variables in `benches/engine_throughput.rs` when running
the full ship-scale footprint, replay, and latency checks.

## Design Rules

- Keep setup outside timed loops unless startup cost is the thing being measured.
- Keep smoke benchmarks bounded enough to avoid Criterion stretching runs into multi-minute samples.
- Derive system benchmarks from acceptance and integration scenarios rather than synthetic isolated loops.
