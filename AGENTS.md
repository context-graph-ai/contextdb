# contextdb — Agent Rules

Two different jobs bring an agent here. Pick one; you do not need the other half.

- **Use contextdb** — drive the database from the CLI or embed it as a Rust library in
  something you are building. Start at [Skills](#skills-use-contextdb). No verification gate needed.
- **Contribute to contextdb** — change the code in this repo. Start at
  [Contributing](#contributing-change-this-repo). The verification gate below is binding.

---

## Skills (use contextdb)

Need the binaries first? `cargo build --release -p contextdb-cli -p contextdb-server` (first
build takes minutes) or use an existing `target/release`; full install options in
[`docs/getting-started.md`](docs/getting-started.md).

Task-shaped recipes, copy-paste runnable. Read the one that matches what you are doing.

| Skill | What it's for | Path |
|---|---|---|
| `using-contextdb` | Open a database, define tables, run SQL (including multi-line paste), read `--json`, branch on exit codes. | [`skills/using-contextdb/SKILL.md`](skills/using-contextdb/SKILL.md) |
| `sync` | Stand up a hub, enroll an edge with its ticket, push/pull, and read the applied / skipped / conflicts counts correctly. | [`skills/sync/SKILL.md`](skills/sync/SKILL.md) |
| `vector-search` | Embedding columns, `<=>` nearest-neighbour search, schema-declared `USE RANK` policies, and the hybrid graph + vector query. | [`skills/vector-search/SKILL.md`](skills/vector-search/SKILL.md) |
| `work-fabric` | Hand a job to another machine over the work ledger and move the bytes it needs over the blob plane. Library API — no CLI. | [`skills/work-fabric/SKILL.md`](skills/work-fabric/SKILL.md) |

Reference docs, when a skill is not enough:

| Doc | What it covers |
|---|---|
| [`docs/getting-started.md`](docs/getting-started.md) | Install, first REPL session, embedding as a library, two-machine sync |
| [`docs/cli.md`](docs/cli.md) | Every flag and meta-command, the `--json` document shapes, the exit-code table |
| [`docs/query-language.md`](docs/query-language.md) | SQL surface, `GRAPH_TABLE` traversal, vector search, constraints, what is unsupported |
| [`docs/architecture.md`](docs/architecture.md) | Crate map, MVCC, sync protocol, work ledger and blob plane, upgrades and recovery |
| [`docs/usage-scenarios.md`](docs/usage-scenarios.md) | 16 problem-first walkthroughs with SQL |
| [`docs/why-contextdb.md`](docs/why-contextdb.md) | Problem statement and comparison with alternatives |

contextdb ships **no built-in schema**. `decisions`, `observations`, `entities`, `edges` and
friends are example tables the docs define; you define your own and attach policy to them.

---

## Contributing (change this repo)

### Verification Gate

All five must pass before any commit, release, or "done" claim. The fifth
installs isolated release binaries and drives the production ticketed-Iroh
durability smoke:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
install_root="$(mktemp -d)"
cargo install --locked --path crates/contextdb-cli --root "$install_root"
cargo install --locked --path crates/contextdb-server --root "$install_root" \
  --features production-smoke-driver --bins
CONTEXTDB_CLI="$install_root/bin/contextdb" \
CONTEXTDB_SMOKE_DRIVER="$install_root/bin/contextdb-smoke-driver" \
CONTEXTDB_SERVER="$install_root/bin/contextdb-server" \
  scripts/installed-release-durable-sync-smoke.sh
```

`CONTRIBUTING.md` lists the same five steps for outside contributors; the two documents
agree.

### Cargo Commands

Never run multiple cargo commands in parallel. They share an exclusive lock on `target/` and will deadlock. Run sequentially or chain with `&&`.

### Testing

- Test suites: `cargo test -p contextdb-engine --test acceptance`, `--test integration`, `--test sql_surface_tests`
- Run the full workspace: `cargo test --workspace`

### Testing discipline

- **No nondeterministic or wall-clock-dependent tests.** Assert on state, counters, or events —
  never on elapsed time. No sleep-as-synchronization, no elapsed-ratio bounds. A negative claim
  ("nothing fired") is a counter/state read after a deterministic drive, not a sleep-and-check.
  One documented exception: the commit-index quadratic-regression guard
  (`commit_index_reconstruction_is_not_quadratic`) keeps its 10s elapsed bound — there the
  time bound IS the promise, with ~1000x margin.
- **Time-dependent behavior uses the clock seam**, and the two audits that enforce it are
  machine-run on every gate. The mechanics — `Wallclock::test_clock_guard`, the thread-local
  limitation, the ratchet you may lower but never raise — are in
  [`crates/contextdb-engine/AGENTS.md`](crates/contextdb-engine/AGENTS.md). Read it before you
  write a test that depends on time or a code path that persists a timestamp.
- **Testability touches in `src/` are production-dead** — `#[doc(hidden)] ..._for_test`
  accessors or unused-in-production seams. Anything else is a behavior change and needs its own
  proof (benchmark + full gate), stated in the commit.
- **Never record test timing with a redirected `TMPDIR`.** Temp DBs on a real disk pay ~40×
  fsync cost vs tmpfs and silently corrupt every figure (mutation-testing runs may redirect
  `TMPDIR` to a fast disk — unset it before timing anything). A recorded timing
  states its TMPDIR/filesystem.
- **Deleting or merging tests requires mutation-testing evidence that coverage is preserved**
  (compare per-mutant results before and after, not summary counts).

### Crate-local rules

Only two crates carry rules beyond the above. If you are editing them, read their file first:

| Crate | Rule |
|---|---|
| [`crates/contextdb-parser/AGENTS.md`](crates/contextdb-parser/AGENTS.md) | Char-boundary discipline: every fixed-width lookahead over input must be boundary-safe, or multi-byte UTF-8 panics the parser. |
| [`crates/contextdb-engine/AGENTS.md`](crates/contextdb-engine/AGENTS.md) | Clock-seam discipline: every persisted timestamp goes through `Wallclock::now()`; the test-estate ratchet audit enforces it. |

### Workspace

11 crates: contextdb-core, contextdb-tx, contextdb-relational, contextdb-graph, contextdb-vector, contextdb-hnsw, contextdb-parser, contextdb-planner, contextdb-engine, contextdb-server, contextdb-cli. Full map and dependency direction: [`docs/architecture.md`](docs/architecture.md#crate-map).

### Query language

The query/DDL grammar reference (including the `PROPAGATE` extensions) is `docs/query-language.md`.

### Releases

Requires `cargo-release` installed locally (`cargo install cargo-release`). No `cargo login` needed — `--no-publish` skips local publishing; CI publishes via `CARGO_REGISTRY_TOKEN` org secret.

Use `cargo release {level} --execute --workspace --no-publish` where level is:
- `patch` (0.3.0 → 0.3.1) — bug fixes, no API changes
- `minor` (0.3.0 → 0.4.0) — new features, backward compatible
- `major` (0.3.0 → 1.0.0) — breaking API changes

This bumps versions across all crates, commits, tags, and pushes. CI then handles crate publishing (rate limited to 5 per batch), Docker images, GitHub releases, and docs deployment to contextdb.tech.

### GitHub

Org: `context-graph-ai`, repo: `contextdb`. CI runs on push to main and PRs.
