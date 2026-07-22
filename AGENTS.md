# contextdb — Agent Rules

## Verification Gate

All five must pass before any commit, release, or "done" claim:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
cargo check --workspace --tests --features contextdb-engine/nats-tests,contextdb-server/nats
```

The fifth step is **compile-only** (no Docker, no broker, seconds): the deprecated
broker suites are feature-gated — `tests/integration.rs` hides several modules behind
`#[cfg(feature = "nats-tests")]`, and `contextdb-server`'s `sync_integration` needs
`nats` — so `cargo test --workspace` never builds them and an API change can break
them with every other step still green. That is not hypothetical: adding a field to
`RowChange` and a return type to `SyncClient::set_table_direction` broke both suites,
and several full green gates ran before anyone compiled them.

## Cargo Commands

Never run multiple cargo commands in parallel. They share an exclusive lock on `target/` and will deadlock. Run sequentially or chain with `&&`.

## Testing

- Tests use **testcontainers** for NATS. Never start Docker containers or NATS manually.
- Test suites: `cargo test -p contextdb-engine --test acceptance`, `--test integration`, `--test sql_surface_tests`
- Run the full workspace: `cargo test --workspace`

### Testing discipline

- **No nondeterministic or wall-clock-dependent tests.** Assert on state, counters, or events —
  never on elapsed time. No sleep-as-synchronization, no elapsed-ratio bounds. A negative claim
  ("nothing fired") is a counter/state read after a deterministic drive, not a sleep-and-check.
  One documented exception: the commit-index quadratic-regression guard
  (`commit_index_reconstruction_is_not_quadratic`) keeps its 10s elapsed bound — there the
  time bound IS the promise, with ~1000x margin.
- **Time-dependent behavior uses the clock seam.** Route every persisted timestamp through
  `contextdb_core::Wallclock::now()` and mock it in tests with `Wallclock::test_clock_guard`
  (RAII, restores the previous clock even on panic — prefer it over a trailing
  `reset_test_clock()`, which leaks the override on assertion failure under `--test-threads=1`).
  Limitation: the override is **thread-local** — engine-internal spawned threads see the real
  clock. Drive background work synchronously on the test thread instead (e.g.
  `run_pruning_cycle()`); never assume a spawned thread sees the mock.
- **Clock/timing audits.** `crates/contextdb-core/tests/timestamp_audit.rs` audits `TIMESTAMP`
  column declarations against a whitelist. Its companion gate-failing audit —
  `crates/contextdb-core/tests/test_estate_audit.rs`, flagging raw `SystemTime::now()`/inline
  epoch math and sleep-based timing assertions in tests against per-file frozen counts — HAS
  LANDED and is machine-enforced on every gate run. Both whitelists may be lowered, never
  raised: adding a raw clock read or a sleep to a test fails the gate rather than review.
- **Testability touches in `src/` are production-dead** — `#[doc(hidden)] ..._for_test`
  accessors or unused-in-production seams. Anything else is a behavior change and needs its own
  proof (benchmark + full gate), stated in the commit.
- **Never record test timing with a redirected `TMPDIR`.** Temp DBs on a real disk pay ~40×
  fsync cost vs tmpfs and silently corrupt every figure (mutation-testing runs may redirect
  `TMPDIR` to a fast disk — unset it before timing anything). A recorded timing
  states its TMPDIR/filesystem.
- **Deleting or merging tests requires mutation-testing evidence that coverage is preserved**
  (compare per-mutant results before and after, not summary counts).

## Workspace

10 crates: contextdb-core, contextdb-tx, contextdb-relational, contextdb-graph, contextdb-vector, contextdb-parser, contextdb-planner, contextdb-engine, contextdb-server, contextdb-cli.

## Query language

The query/DDL grammar reference (including the `PROPAGATE` extensions) is `docs/query-language.md`.

## Releases

Requires `cargo-release` installed locally (`cargo install cargo-release`). No `cargo login` needed — `--no-publish` skips local publishing; CI publishes via `CARGO_REGISTRY_TOKEN` org secret.

Use `cargo release {level} --execute --workspace --no-publish` where level is:
- `patch` (0.3.0 → 0.3.1) — bug fixes, no API changes
- `minor` (0.3.0 → 0.4.0) — new features, backward compatible
- `major` (0.3.0 → 1.0.0) — breaking API changes

This bumps versions across all 10 crates, commits, tags, and pushes. CI then handles crate publishing (rate limited to 5 per batch), Docker images, GitHub releases, and docs deployment to contextdb.tech.

## GitHub

Org: `context-graph-ai`, repo: `contextdb`. CI runs on push to main and PRs.
