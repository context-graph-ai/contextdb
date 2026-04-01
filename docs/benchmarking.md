# Benchmarking

ContextDB now has explicit benchmark tiers instead of one undifferentiated suite.

## Targets

- `cargo bench -p contextdb-parser --bench parser_throughput`
  - parser-only microbench
- `cargo bench -p contextdb-engine --bench engine_throughput`
  - developer smoke bench for local engine behavior
- `cargo bench -p contextdb-engine --bench engine_full_throughput`
  - heavier engine benchmarks for larger write/reopen and mixed workflow paths
- `cargo bench -p contextdb-server --bench server_throughput`
  - CLI smoke bench
- `cargo bench -p contextdb-server --bench server_sync_system`
  - system sync bench with real NATS via testcontainers
- `cargo bench -p contextdb-server --bench sync_pr`
  - bounded PR-tier sync workloads using direct sync APIs

## Guidance

- Use the parser and engine smoke benches for quick local regression checks.
- Use `engine_full_throughput` when you want larger local workloads.
- Use `sync_pr` when you want realistic sync regression coverage without CLI/process overhead in the timed path.
- Use `server_sync_system` only when you are intentionally testing sync/system behavior.
- The server sync bench is heavier because it uses release binaries and a real NATS container.

## Design Rules

- Keep setup outside timed loops unless startup cost is the thing being measured.
- Keep smoke benchmarks bounded enough to avoid Criterion stretching runs into multi-minute samples.
- Derive system benchmarks from acceptance and integration scenarios rather than synthetic isolated loops.
