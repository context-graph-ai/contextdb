---
name: using-contextdb
description: Open a contextdb database, define tables with enforced policy, run SQL including multi-line paste, read machine output with --json, and branch on exit codes.
---

# Using contextdb

**Prerequisite:** needs the `contextdb` binary. In a checkout of this repo:
`cargo build --release -p contextdb-cli`. Other install options (release download, `cargo
install`): [`docs/getting-started.md`](../../docs/getting-started.md).

contextdb is an embedded database — one file, one process, relational + graph + vector under one
transaction. `contextdb` is how you drive it from a shell. There is no server to start and no
schema to migrate to: **contextdb ships no built-in schema**, you create your own tables.

## Open a database

```bash
contextdb ./demo.db      # persisted; single file, crash-safe
contextdb :memory:       # ephemeral, discarded on exit
```

A file has exactly one owner at a time. A second open of the same path — from this process or
another — fails with `DatabaseLocked`. Keep one session; don't fan out concurrent CLI calls at the
same file.

**There is no read-only way to open a store with plain `contextdb <path>` — not even a no-op
meta-command like `.help`.** Every session, including one that only reads, rewrites the file's
bytes. Before you open a store you must not alter — someone else's, a backup, anything you didn't
create for this task — read
[`AGENTS.md`'s "There is no read-only way to open a store"](../../AGENTS.md#there-is-no-read-only-way-to-open-a-store)
for the sanctioned copy-first / `repair` pattern. `scripts/store-health-check.sh` (below) already
follows it — read it as a working example of the pattern, not just the prose.

When stdin is not a terminal the CLI runs in **pipe mode**: no banner, no prompt, results on
stdout, every error and diagnostic on stderr, and the process exit code tells you what happened.
That is the mode to script in.

## Create, insert, query — one paste

Statements are semicolon-terminated and may span as many lines as they need. A statement ends at
the first `;` that is **not** inside a quoted string or a comment, so a real schema file runs as
written. Anything still open when the input ends is run at that point.

```bash
contextdb ./demo.db <<'SQL'
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL
) STATE MACHINE (status: draft -> [active, rejected], active -> [superseded]);

INSERT INTO decisions (id, description, status) VALUES ('550e8400-e29b-41d4-a716-446655440000', 'adopt contextdb', 'draft');

UPDATE decisions SET status = 'active' WHERE id = '550e8400-e29b-41d4-a716-446655440000';

SELECT id, description, status FROM decisions;
SQL
```

Output shape — note that pipe mode echoes each INSERT before executing it, so a seeding log is
readable:

```text
ok (rows_affected=0)
INSERT INTO decisions (id, description, status) VALUES ('550e8400-e29b-41d4-a716-446655440000', 'adopt contextdb', 'draft');
ok (rows_affected=1)
ok (rows_affected=1)
+--------------------------------------+-----------------+--------+
| id                                   | description     | status |
+--------------------------------------+-----------------+--------+
| 550e8400-e29b-41d4-a716-446655440000 | adopt contextdb | active |
+--------------------------------------+-----------------+--------+
```

The `STATE MACHINE` clause is enforced by the engine, not by your code. Push the row backwards and
it is refused:

```bash
echo "UPDATE decisions SET status = 'draft' WHERE id = '550e8400-e29b-41d4-a716-446655440000';" \
  | contextdb ./demo.db
# stderr: Error: invalid state transition: active -> draft
# exit:   1
```

Other policy you can declare and the database will hold: `IMMUTABLE` columns and tables, `DAG`
cycle prevention on an edge table, `RETAIN <n> DAYS` TTL, `HISTORY CURRENT ONLY` to let the
maintenance loop reclaim a row's superseded versions once it declares `SYNC CONFLICT KEEP LATEST`
(or never syncs), and `PROPAGATE` cascades along edges and foreign keys. `RETAIN` bounds how long a
row lives; `HISTORY` bounds how many past versions of a live row are kept — declare both on a
status/heartbeat table that should both collapse and eventually age out. `HISTORY CURRENT ONLY` is
refused on a table that delivers to another machine under the default (keep-first) conflict policy —
the error names the fix. `.schema <table>` shows the declaration; reclaiming happens on the next
maintenance cycle, not synchronously within the ALTER that declared it.
See [`docs/query-language.md`](../../docs/query-language.md#table-options).

## Interactive REPL

Same engine, human output. Press Enter before the `;` and the statement continues under a `...>`
prompt, so a multi-line `CREATE TABLE` pastes whole:

```text
contextdb> CREATE TABLE notes (
      ...>   id UUID PRIMARY KEY,
      ...>   body TEXT
      ...> );
ok (rows_affected=0)
```

Human table output is capped at 100 rows and prints a footer saying so; pass `--all` to print every
row. Under `--json` there is no cap and `--all` is a no-op.

## Machine output: `--json`

`--json` makes stdout **pure JSON Lines** — one complete JSON document per line, one line per
statement or meta-command that produced a result, and nothing else. A run that produced no results
writes nothing to stdout.

A query is one line, a JSON array of row objects, uncapped:

```bash
echo "SELECT id, status FROM decisions;" | contextdb ./demo.db --json
```
```json
[{"id":"550e8400-e29b-41d4-a716-446655440000","status":"active"}]
```

```bash
echo "SELECT id, status FROM decisions;" | contextdb ./demo.db --json | jq -r '.[].status'
# active
```

A non-query statement is a small status object:

```bash
echo "CREATE TABLE t (id UUID PRIMARY KEY);" | contextdb :memory: --json
```
```json
{"rows_affected":0}
```

The INSERT echo is suppressed under `--json`, so a seeding script stays parseable end to end.

### Meta-commands are machine-readable too

Meta-commands start with `.`, stay **single-line**, and take **no `;`**. Each emits its own
document, keyed by its payload:

```bash
printf '.tables\n' | contextdb ./demo.db --json
```
```json
{"tables":["decisions"]}
```

```bash
printf '.schema decisions\n' | contextdb ./demo.db --json | jq '.state_machine'
```
```json
{"column":"status","transitions":{"active":["superseded"],"draft":["active","rejected"]}}
```

`.schema` returns the table's declared contract as data — `columns` (type, nullability, key /
unique / immutable flags, `references` with its propagate clause, vector quantization, rank
policy), `primary_key`, `indexes`, `state_machine`, `retain`, `sync_direction`, `conflict_policy`,
`dag_edge_types`, `propagate` — plus `ddl`, the exact text the human `.schema` prints, so a
snapshot/replay flow keeps working. **A policy the table never declared is absent, not defaulted.**
Test with `jq 'has("retain")'`, never by comparing against a value nobody wrote.

Which plan the engine chose:

```bash
printf ".explain SELECT id FROM decisions WHERE status = 'active'\n" \
  | contextdb ./demo.db --json | jq '.explain.physical_plan'
```

Full document table for `.tables` / `.schema` / `.explain` / `.trace` / the `.sync` family:
[`docs/cli.md`](../../docs/cli.md#--json).

### Errors under `--json`

Everything that is not a result goes to **stderr** — errors, traces, `.help` — so stdout stays
parseable. An error is one document:

```bash
echo "SELECT * FROM nope;" | contextdb ./demo.db --json
```
```json
{"error":{"class":"sql","message":"table not found: nope","line":1}}
```

`class` is the branch you act on: `sql` (fix the query), `sync` (the hub is unreachable or
refused), `io` (disk, lock, budget, corrupt store), `usage` (fix the command line). `line` is the
input line the failing statement started on, when the CLI knows it — the message never repeats it.
The engine's ~100 error variant names are deliberately not published; `message` carries the text.

A scripted run continues to the next statement after an error, so one run reports all its errors —
but the process never reports a run that hit an error as success.

## Exit codes — branch on these

Every contextdb binary reports one of four codes:

| Code | Meaning |
|---|---|
| `0` | Success. Everything the run attempted worked. |
| `1` | Error. The invocation was valid; something in the run failed — SQL or engine error, failed meta-command, definitive sync failure, database that could not be opened. |
| `2` | Usage error. The invocation itself is wrong and **nothing was attempted** — unknown flag, missing argument, unparseable value like `--memory-limit 12Q`, or `--tenant-id` with no `--sync-endpoint`. |
| `3` | A `.sync push` was interrupted after sending and its outcome is unconfirmed. Not a failure — **re-pushing is safe**. See the `sync` skill. |

Precedence: a definitive error (`1`) dominates an unconfirmed push (`3`), which dominates success
(`0`). A usage error (`2`) is terminal at startup, so it never competes.

```bash
echo "SELECT * FROM decisions;" | contextdb ./demo.db --json > rows.json 2> errors.jsonl
case $? in
  0) jq -r '.[].id' rows.json ;;
  2) echo "bad invocation — nothing ran"; cat errors.jsonl ;;
  3) echo "push unconfirmed — re-push on next start" ;;
  *) echo "run failed"; jq -r '.error.class + ": " + .error.message' errors.jsonl ;;
esac
```

One exception: an **interactive** terminal session exits `0` even after errors, because it showed
you each one as it happened (same as `psql` and `sqlite3`). An unconfirmed push still reports `3`
from an interactive session, because nobody can act on it once the process is gone.

## Gotchas worth knowing before you script

- **A `;` inside a string literal or a comment is not a terminator.** `INSERT ... VALUES ('a;b');`
  is one statement. An unclosed `/*` keeps the statement open — a `.tables` typed after it is
  comment text, not a command, and end of input fails loudly rather than silently dropping it.
- **A meta-command is only a meta-command when no statement is open.** A line starting with `.`
  inside an unfinished statement is that statement's text.
- **Budgets are SQL, not just flags.** `SET MEMORY_LIMIT '512M'` / `SET DISK_LIMIT '1G'` and their
  `SHOW` forms. For a file-backed database `SET DISK_LIMIT` persists and survives reopen;
  `:memory:` accepts and ignores it.
- **`LIMIT` is required on vector search.** Unbounded `ORDER BY <=>` is rejected. See the
  `vector-search` skill.
- **Not supported, by design:** `GROUP BY`/`HAVING`, `UNION`/`INTERSECT`/`EXCEPT`,
  `INSERT ... SELECT`, window functions, `WITH RECURSIVE`, subqueries outside `IN`, and aggregates
  other than `COUNT`. Full list: [`docs/query-language.md`](../../docs/query-language.md#unsupported-features).
- **Logs go to stderr** at level `ERROR` by default; raise with `RUST_LOG=debug`.

## Verify a store deterministically — run the script, don't hand-check

Whenever you need to confirm a store is healthy and readable — opens cleanly, `.tables` lists what
you expect, every table's row count is actually readable — run
[`scripts/store-health-check.sh`](../../scripts/store-health-check.sh) instead of re-deriving the
same three checks by hand:

1. `CONTEXTDB_CLI=<path-to-contextdb> scripts/store-health-check.sh ./demo.db`
2. Expected output on a healthy store: an `OK   repair: ...` line, an `OK   .tables: N table(s)`
   line, then one `OK   <table>  <n> row(s)` line per table.
3. Exit code `0` means every check passed. **If it exits `1`, the printed line tells you exactly
   which check failed** (repair reported a problem, or one table's row count was unreadable) — go
   fix that specific thing rather than re-running the whole store from scratch.

```bash
CONTEXTDB_CLI=./bin/contextdb scripts/store-health-check.sh ./demo.db
```
```text
OK   repair: repair: './demo.db' is current-format and its schema layout reads cleanly; nothing to repair.
OK   .tables: 1 table(s)
OK   decisions                        1 row(s)
```

The script itself follows the copy-first pattern above — it never opens your original file for the
inspection half, only a `mktemp -d` peek copy — so it is also the reference implementation to copy
if you're writing your own read-only-feeling check.

## Embedding it instead

The CLI is for exploration and scripting. The primary interface is the Rust API — same engine,
parameter binding, and a commit subscription channel:

```rust
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

let db = Database::open(std::path::Path::new("./my.db"))?;

db.execute(
    "CREATE TABLE observations (id UUID PRIMARY KEY, data JSON, embedding VECTOR(384)) IMMUTABLE",
    &HashMap::new(),
)?;

let mut params = HashMap::new();
params.insert("id".into(), Value::Uuid(uuid::Uuid::new_v4()));
params.insert("data".into(), Value::Json(serde_json::json!({"type": "sensor"})));
params.insert("embedding".into(), Value::Vector(vec![0.1; 384]));
db.execute(
    "INSERT INTO observations (id, data, embedding) VALUES ($id, $data, $embedding)",
    &params,
)?;
```

`$name` parameter binding is the library path — the CLI has no parameter binding, so CLI recipes
use literals. Details and the plugin/subscription surface:
[`docs/getting-started.md`](../../docs/getting-started.md#use-as-a-library).

## Next

- DAG edges, graph traversal, state machines, cascades → [`skills/querying-the-graph/SKILL.md`](../querying-the-graph/SKILL.md)
- Replicate across machines → [`skills/sync/SKILL.md`](../sync/SKILL.md)
- Similarity search and ranking → [`skills/vector-search/SKILL.md`](../vector-search/SKILL.md)
- Distribute jobs and blobs → [`skills/work-fabric/SKILL.md`](../work-fabric/SKILL.md)
- Retention, purge, backups, sync liveness → [`skills/operating-a-store/SKILL.md`](../operating-a-store/SKILL.md)
- Routes, sinks, schedules → [`skills/running-triggers-and-schedules/SKILL.md`](../running-triggers-and-schedules/SKILL.md)
