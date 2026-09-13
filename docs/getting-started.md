# Getting Started

Run contextdb in 2 minutes.

---

## Install

### Download a pre-built binary (recommended)

Download the latest release for your platform from [GitHub Releases](https://github.com/context-graph-ai/contextdb/releases/latest):

```bash
# Linux x86_64
curl -fsSL https://github.com/context-graph-ai/contextdb/releases/latest/download/x86_64-unknown-linux-gnu.tar.gz | tar xz
# Linux ARM64
curl -fsSL https://github.com/context-graph-ai/contextdb/releases/latest/download/aarch64-unknown-linux-gnu.tar.gz | tar xz
# macOS Intel
curl -fsSL https://github.com/context-graph-ai/contextdb/releases/latest/download/x86_64-apple-darwin.tar.gz | tar xz
# macOS Apple Silicon
curl -fsSL https://github.com/context-graph-ai/contextdb/releases/latest/download/aarch64-apple-darwin.tar.gz | tar xz
```

### Install via cargo

```bash
cargo install contextdb-cli
```

### Build from source

Requires [Rust stable](https://rustup.rs/) (1.75+) and Git.

```bash
git clone https://github.com/context-graph-ai/contextdb.git
cd contextdb
cargo build --release -p contextdb-cli
```

## First REPL Session

```bash
contextdb ./my.db --write
```

`--write` creates the store if it is missing and authorizes mutation. `:memory:` is always
writable with no flag; a file-backed store is different — see
[`docs/cli.md`](cli.md#cli-client-contextdb). Example tables such as `decisions` are yours to
define; see the [README](../README.md).

Try the state machine — the feature that makes contextdb different from plain SQL:

```sql
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  reasoning TEXT
) STATE MACHINE (status: draft -> [active, rejected], active -> [superseded]);

INSERT INTO decisions (id, status, reasoning)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 'draft', 'initial assessment');

-- Valid transition: draft -> active
UPDATE decisions SET status = 'active'
WHERE id = '550e8400-e29b-41d4-a716-446655440000';

-- Invalid transition: active -> draft (rejected by the database)
UPDATE decisions SET status = 'draft'
WHERE id = '550e8400-e29b-41d4-a716-446655440000';
-- Error: invalid state transition: active -> draft
```

The database enforces the state machine. No application code needed.

Audit-frozen columns — declare `IMMUTABLE` on any column whose value must never be silently rewritten after the initial INSERT:

```sql
CREATE TABLE audit_decisions (
  id UUID PRIMARY KEY,
  decision_type TEXT NOT NULL IMMUTABLE,
  description TEXT NOT NULL IMMUTABLE,
  status TEXT NOT NULL DEFAULT 'active'
);

INSERT INTO audit_decisions (id, decision_type, description)
VALUES ('550e8400-e29b-41d4-a716-446655440001', 'sql-migration', 'adopt contextdb');

-- Mutable column: succeeds
UPDATE audit_decisions SET status = 'superseded'
WHERE id = '550e8400-e29b-41d4-a716-446655440001';

-- Flagged column: rejected with Error::ImmutableColumn
UPDATE audit_decisions SET decision_type = 'other'
WHERE id = '550e8400-e29b-41d4-a716-446655440001';
-- Error: column `decision_type` on table `audit_decisions` is immutable
```

The row stays at its original `decision_type`; the session continues. To record a correction, INSERT a new row and mark the original `superseded`.

## Persist to Disk

The first session already wrote `./my.db`. One database file plus its `.lock` companion
(`my.db` and `my.db.lock`), and a third file — `my.db.fabric-identity.key` — once the store has
synced. Crash-safe via redb. Reopen and your data is there. Without `--write` the same path opens
a read-only session instead, which is the subject of the next chapter. See
[`docs/cli.md`](cli.md#cli-client-contextdb).

## Inspecting a store safely

You have a contextdb store on disk — maybe your own, maybe one a running service owns — and you
want to look inside without any risk of changing it. That is the default command:

```bash
contextdb ./my.db
```

This opens a **read-only session**. Mutation needs `--write`; the refusals and routing are in
[`docs/cli.md`](cli.md#cli-client-contextdb).
<!-- enforced by: read_cli_journeys_invocation::reading_an_idle_store_leaves_every_byte_and_the_folder_listing_unchanged, read_cli_journeys_invocation::a_reading_session_refuses_every_mutating_statement_before_it_executes, read_cli_journeys_invocation::bare_path_on_a_missing_store_refuses_and_creates_nothing -->

### 1. Look around

```text
contextdb(ro)> .tables
decisions
has_more: false

contextdb(ro)> .schema decisions
```

`.schema` reprints the declaration from the first session, with from-states sorted rather than
in the order you declared them.

```text
contextdb(ro)> SELECT id, status FROM decisions;
+--------------------------------------+--------+
| id                                   | status |
+--------------------------------------+--------+
| 550e8400-e29b-41d4-a716-446655440000 | active |
+--------------------------------------+--------+
(1 rows)
```

Three shapes to expect. `.tables` is a bounded page, so it always closes with a `has_more:` line.
`.schema` prints the from-states sorted. And an ordinary `SELECT` renders as a bordered table with
exactly one `(N rows)` footer.

The first reading command prints one route notice on stderr telling you **how** you are reading.
In human mode that is one sentence — `reading the committed snapshot taken at
2026-08-29T06:41:47Z` when nobody owns the store (you are reading a point-in-time snapshot), or
`reading through the live owner's local channel` when a live process owns it and is serving you
its committed state over an authenticated local channel (same commands, same shapes). Under
`--json` the same notice carries the machine form in `detail.route` (`"file"` or `"owner"`) and
`detail.snapshot_at` (the committed moment on the file route, `null` on the owner route) — the
shapes are in [`docs/cli.md`](cli.md#reading-routes).
<!-- enforced by: read_cli_journeys_session_shape::the_route_notice_is_emitted_once_on_stderr_at_the_first_store_reading_command -->

### 2. Everything else a reading session can do

The read surface has exactly one reference — [`docs/cli.md`](cli.md). Rather than restate it here
and let the two copies drift, three sentences and where each one is spelled out in full:

- **A result that crosses a ceiling is refused whole — never truncated, no partial rows** — and
  the refusal names both escapes and carries a copy-ready `.cursor open <statement>` you can page
  it with: [Ordinary results: complete or refused](cli.md#ordinary-results-complete-or-refused)
  and [Large results: the session cursor](cli.md#large-results-the-session-cursor).
  <!-- enforced by: read_cli_journeys_ordinary_results::one_row_past_the_ceiling_publishes_nothing_and_names_the_ceiling, read_cli_journeys_ordinary_results::the_refusal_carries_the_statement_and_a_copy_ready_cursor_command -->
- **Which escape is honest depends on your route**, so check the route notice or `.owner status`
  first: raising `--read-result-rows` / `--read-result-bytes` for a one-shot export works on the
  **file** route only — on the **owner** route the ceilings are the owner's, a reader cannot raise
  them, and the cursor is the in-session escape. If a live process owns the store, `contextdb
  <path>` reads through that owner automatically and `--write` is refused with `held_by_writer`:
  [Reading routes](cli.md#reading-routes) and [Declared limits](cli.md#declared-limits).
  <!-- enforced by: read_cli_journeys_live_owner::the_owner_route_refusal_names_the_writer_side_change_and_the_cursor, read_cli_journeys_live_owner::a_reading_session_routes_through_the_live_owner_and_says_so_once, read_cli_journeys_live_owner::a_second_writer_is_refused_and_told_how_to_read_instead, read_cli_journeys_live_owner::owner_status_reports_the_serving_owner_as_control_data -->
- **Scripts pass `--json`**: stdout is one JSON document per statement, every notice and error is
  a JSON document on stderr, and the process exit code is one of the four in
  [Exit Codes](cli.md#exit-codes).
  <!-- enforced by: read_cli_journeys_session_shape::a_piped_session_is_a_full_session_including_the_cursor -->

### 3. Scratch space

`contextdb :memory:` is a disposable, always-writable playground — no file, no `--write`
needed, gone on exit. <!-- enforced by: read_cli_journeys_invocation::memory_store_is_writable_without_the_flag_and_accepts_the_flag_as_a_no_op -->

## Indexes and Scale

contextdb is designed for agents holding tens of thousands of entities with
sub-100ms filtered retrieval. Indexes accelerate filtered scans so a
10,000-row table answers `WHERE tag = 'x'` in microseconds instead of
milliseconds.

```sql
-- `observations` is an example table you define - see the README.
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  tag TEXT,
  value INTEGER
);

CREATE INDEX idx_tag ON observations (tag);

INSERT INTO observations (id, tag, value)
VALUES ('650e8400-e29b-41d4-a716-446655440010', 'pay', 1);

SELECT value FROM observations WHERE tag = 'pay';
```

Composite indexes push a matched leading prefix, not just the first column:

```sql
CREATE INDEX idx_observation_route ON observations (tag, value, id);

.explain SELECT id FROM observations WHERE tag = 'pay' AND value = 1;
```

The explanation reports `IndexScan { index: idx_observation_route }` with
`predicates_pushed: [tag, value]`. A query whose `WHERE` clause does not match
the first column of any declared or auto-index reports `Scan`.

## Rank Vector Search by Outcomes

Vector search does not have to rank by cosine similarity alone. Declare a `RANK_POLICY` on the
vector column and every caller asking for the same `SORT_KEY` gets the same outcome-weighted
ordering — resolved at DDL time, stored with the schema, and replicated through sync — so the row
that is closest but *failed* sorts below the row that was less similar and *worked*. No
application copies formula text into its queries.

- Grammar — `RANK_POLICY`, `JOIN`, the `FORMULA` placeholders, `SORT_KEY`, `USE RANK`:
  [`docs/query-language.md`](query-language.md#rank-policies).
- Runnable walkthrough with the expected ordering, the same query without `USE RANK` for
  contrast, the join-index requirement, the HNSW/ANN caveat, and how to change a policy:
  [`skills/vector-search/SKILL.md`](../skills/vector-search/SKILL.md).

## Use as a Library

contextdb is an embedded database — the CLI is for exploration. The primary interface is the Rust API:

```rust
use contextdb_engine::Database;
use contextdb_core::Value;
use std::collections::HashMap;

let db = Database::open(std::path::Path::new("./my.db"))?;

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
```

Add to your `Cargo.toml`:

```toml
[dependencies]
contextdb-engine = "1.0.0"
contextdb-core = "1.0.0"
```

### Reading a store from a second process

`Database::open` takes ownership of a store file, so one process holds it at a time. Reading is a
separate door. `ReadSession::open` (or `ReadSession::open_with_options`, which carries the
caller's own limits and deadlines) reads a store whether or not somebody owns it: it routes
through the live owner's local channel when a process owns the store, and reads the committed
file directly when none does.

`ReadSession::open_owner_only` is the third door — the one for questions only a *running owner*
can answer (what the process is doing, whether it is serving yet). It takes the owner route when a
process owns the store, and when none does it says `owner_not_running` rather than falling through
to the file. Four exported names, and that is the whole surface:

```rust
use contextdb_engine::{ReadSession, ReadSessionOptions};

// The live owner if one is running, the committed file if not.
let session = ReadSession::open("./my.db")?;

// The same, with the caller's own ceilings and deadlines.
let bounded = ReadSession::open_with_options("./my.db", ReadSessionOptions::default())?;

// Administrative asking and readiness probing: an owner, or nothing.
let owner = ReadSession::open_owner_only("./my.db", ReadSessionOptions::default())?;
```

Why `open_owner_only` never falls through to the file, what a readiness probe would otherwise do
to the writer it is waiting for, and how a refused read splits into a stable `kind()` and a
machine-readable `detail()`:
[`docs/architecture.md`](architecture.md#store-ownership--concurrency).

## What's Next

- [Why contextdb?](why-contextdb.md) — the problems it solves and how it compares to alternatives
- [Usage Scenarios](usage-scenarios.md)
- [Query Language](query-language.md) — full SQL, graph, and vector reference
- [Sync Across Two Machines](sync-two-machines.md) — stand up a hub, enroll two edges, converge both ways
- [CLI Reference](cli.md) — REPL commands, sync, scripting
