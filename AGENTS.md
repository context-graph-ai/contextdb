# contextdb — Agent Rules

Two different jobs bring an agent here. Pick one; you do not need the other half.

- **Use contextdb** — drive the database from the CLI or embed it as a Rust library. Go to
  [Use contextdb](#use-contextdb). No verification gate applies to you.
- **Contribute to contextdb** — change the code in this repo. Go to
  [Contribute](#contribute-change-this-repo). The gate there is binding.

## Safety boundaries (both jobs, read before you run anything)

### There is no read-only way to open a store

Plain `contextdb <path>` is **always** a read-write session. Even a no-op meta-command like
`.help` rewrites the file's bytes. Never open a store you must not alter just to look at it —
copy it first and open the copy, or use `repair`, which is genuinely read-only:

```bash
contextdb repair ./their.db                       # read-only diagnosis, never modifies
peek="$(mktemp -d)/peek.db"; cp ./their.db "$peek" # read the DATA without touching the original
echo ".tables" | contextdb "$peek" --json
```

### Destructive commands need explicit confirmation

Every row below changes or destroys durable data. Run none of them against a store you did not
create for this task until the operator confirms that exact path. **Sole exception:** an
ephemeral, task-scoped store you created yourself (`:memory:`, or a file under `$(mktemp -d)`) —
there, run them freely and without asking.

| Goal | Sanctioned command | Guardrail |
|---|---|---|
| Find out why a store will not open | `contextdb repair <path>` | Read-only. Always safe. Start here. |
| Bring a legacy-format root forward | `contextdb migrate <path>` | Rehearse on a copy first (procedure below). Writes `<path>.bak`. |
| Recreate a wedged or corrupt store | `contextdb reset <path> --force` | Destroys all data. Recover what you need from a backup or a healthy sync peer first. `--force` is mandatory; without it the CLI refuses and exits `2`. |
| Take a backup before any of the above | `contextdb snapshot export <path> <dest>` | Purge-fenced; the safe prerequisite for `migrate` and `reset`. |
| Apply declared `RETAIN` windows now instead of waiting for the background cycle | `.maintenance run` (REPL meta-command) | Synchronous; expires rows the table's own DDL already declared expirable. |
| Erase rows permanently | TODO-D34 — guarded CLI purge verb | Not yet reachable from the CLI; purge is library-only until that verb lands. |
| Make a route, sink or schedule actually fire | TODO-D34 — CLI door for triggers/events/schedules | `CREATE EVENT TYPE`/`SINK`/`ROUTE`/`SCHEDULE` parse and the engine executes them, but no CLI path delivers yet. Drive them from the Rust library, or wait for that verb. |

### Migration rehearsal — run these four steps in order, do not add flags

1. `scratch="$(mktemp -d)"; cp ./prod.db "$scratch/rehearsal.db"` — never rehearse in place.
2. `contextdb migrate "$scratch/rehearsal.db"` — expect `migrated ... in place (N rows ...)`.
3. `echo ".tables" | contextdb "$scratch/rehearsal.db" --json`, plus the reads your application
   performs; confirm the rows are present.
4. Only if step 3 is clean: `contextdb snapshot export ./prod.db <dest>`, then `contextdb migrate ./prod.db`.

If step 2 says the store is already current-format, there is nothing to migrate — stop; do not
reach for `reset`.

### One cargo build at a time, and check the disk first

`cargo test --workspace` grows `target/` to **55–90 GB**, and incremental rebuilds keep growing it.
Never run two cargo commands at once (they deadlock on the `target/` lock) and never start a
workspace build while another is live on the box — chain with `&&` and wait. First:

```bash
df -h .                             # want ~100G free for a full workspace test run
export CARGO_PROFILE_DEV_DEBUG=0    # roughly halves target/, keeps backtraces usable
```

## Use contextdb

Need binaries? `cargo build --release -p contextdb-cli -p contextdb-server` (minutes on a cold
cache), or use an existing `target/release`; install options in
[`docs/getting-started.md`](docs/getting-started.md).

Throwaway store to try anything against, machine-readable output:

```bash
db="$(mktemp -d)/scratch.db"
printf "%s\n" \
  "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT NOT NULL);" \
  "INSERT INTO decisions VALUES ('550e8400-e29b-41d4-a716-446655440000', 'draft');" \
  "SELECT * FROM decisions;" | contextdb "$db" --json
# {"rows_affected":0}
# {"rows_affected":1}
# [{"id":"550e8400-e29b-41d4-a716-446655440000","status":"draft"}]
```

Prefer `--json` and read named fields; never scrape the human table output, which is capped at
100 rows and reflows freely. Under `--json`, stdout is JSON Lines (results only) and everything
else — errors `{"error":{"class":...}}`, notices, traces — goes to stderr. Branch on the exit code:
`0` success, `1` the run failed, `2` the command line was wrong so nothing ran, `3` a `.sync push`
was interrupted and is unconfirmed, so re-push (never treat `3` as failure).

Task-shaped recipes, copy-paste runnable. Read the one matching what you are doing.

| Skill | What it's for | Path |
|---|---|---|
| `using-contextdb` | Open a database, define tables, run SQL (including multi-line paste), read `--json`, branch on exit codes. | [`skills/using-contextdb/SKILL.md`](skills/using-contextdb/SKILL.md) |
| `querying-the-graph` | Declare a DAG edge table, traverse it with `GRAPH_TABLE`, get cycle inserts refused, enforce `STATE MACHINE` transitions with `PROPAGATE` cascades. | [`skills/querying-the-graph/SKILL.md`](skills/querying-the-graph/SKILL.md) |
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

Two policy declarations decide by themselves whether your data survives — pick deliberately:

| Situation | Declare | Consequence you are accepting |
|---|---|---|
| A row's first value is the fleet's value (facts, immutable observations) | `SYNC CONFLICT KEEP FIRST` (the default) | Later writes to that key lose — **including deletes**, which are arbitrated exactly like writes and will not propagate off the originating edge. |
| A row is a replaceable current-state cell (status, config, cursors) | `SYNC CONFLICT KEEP LATEST` | The last hub-accepted write wins; deletes propagate. |

## Contribute (change this repo)

### Verification gate

All five must pass before any commit, release, or "done" claim. The fifth installs isolated
release binaries and drives the production ticketed-Iroh durability smoke.

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

`CONTRIBUTING.md` lists the same five steps for outside contributors; the two documents agree.
Read the disk and single-build rules in [Safety boundaries](#one-cargo-build-at-a-time-and-check-the-disk-first)
before you start the third command. Narrower suites while iterating:
`cargo test -p contextdb-engine --test acceptance`, `--test integration`, `--test sql_surface_tests`.

### Where a change lives

Describe the capability, then go to the crate that owns it — do not go looking by filename.

| Change | Where it lands |
|---|---|
| A new scalar SQL function | ONE match arm in the engine crate's executor (`eval_function`), plus the ORDER BY allowlist (`is_known_scalar_function`), plus tests. The parser takes function calls generically and the planner needs nothing. |
| A function that works in `SELECT` but is refused in `ORDER BY` | You added it to `eval_function` only. Those two lists are hand-synced with no cross-reference — add every new function to both. |
| Grammar, DDL clauses, `PROPAGATE` | Parser crate; grammar reference is [`docs/query-language.md`](docs/query-language.md). |
| Sync protocol, conflict arbitration, work ledger | Engine crate owns the implementations; the server crate re-exports them. Never add a mirror module in the server — extend the engine and re-export. |
| Anything else | [`docs/architecture.md`](docs/architecture.md#crate-map) maps all 11 crates and the dependency direction. |

### Testing discipline

- **No nondeterministic or wall-clock-dependent tests.** Assert on state, counters, or events —
  never on elapsed time, and never sleep as synchronization. A negative claim ("nothing fired")
  is a counter read after a deterministic drive. One documented exception:
  `commit_index_reconstruction_is_not_quadratic`, where the time bound IS the promise.
- **Time-dependent behavior uses the clock seam.** Two audits enforce it on every gate; the
  mechanics (`Wallclock::test_clock_guard`, the thread-local limitation, the ratchet you may
  lower but never raise) are in [`crates/contextdb-engine/AGENTS.md`](crates/contextdb-engine/AGENTS.md).
- **When `timestamp_audit` fails on a change that touched no timestamps:** its whitelist is
  pinned by line number, so any line-count shift in a whitelisted file trips it. Do not disable
  or re-scope the audit — update the pinned line numbers to their new positions in the same
  commit and say so in the message.
- **Testability touches in `src/` are production-dead** — `#[doc(hidden)] ..._for_test`
  accessors or unused-in-production seams. Anything else is a behavior change and needs its own
  proof (benchmark + full gate), stated in the commit.
- **Never record test timing with a redirected `TMPDIR`.** Temp DBs on a real disk pay ~40×
  fsync cost vs tmpfs and silently corrupt every figure. Unset it before timing, and state the
  TMPDIR/filesystem alongside any recorded timing.
- **Deleting or merging tests requires mutation-testing evidence that coverage is preserved**
  (compare per-mutant results before and after, not summary counts).

### Crate-local rules

Only two crates carry rules beyond the above, and they add to this file rather than override it.
Read the matching one before you edit that crate.

| Crate | Rule |
|---|---|
| [`crates/contextdb-parser/AGENTS.md`](crates/contextdb-parser/AGENTS.md) | Char-boundary discipline: every fixed-width lookahead over input must be boundary-safe, or multi-byte UTF-8 panics the parser. |
| [`crates/contextdb-engine/AGENTS.md`](crates/contextdb-engine/AGENTS.md) | Clock-seam discipline: every persisted timestamp goes through `Wallclock::now()`; the test-estate ratchet audit enforces it. |

### Releases

Needs `cargo install cargo-release`. Run
`cargo release {patch|minor|major} --execute --workspace --no-publish` — patch for bug fixes,
minor for backward-compatible features, major for breaking API changes. It bumps every crate,
commits, tags, and pushes; CI (org `context-graph-ai`, repo `contextdb`, runs on `main` and PRs)
then publishes crates, Docker images, GitHub releases, and the contextdb.tech docs deploy. No
`cargo login` needed — CI holds the token.
