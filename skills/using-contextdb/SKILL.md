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
contextdb ./demo.db              # read an existing store; never creates, never mutates
contextdb ./demo.db --write      # create it if missing, and permit mutation
contextdb :memory:               # ephemeral writable scratch, discarded on exit
```

**Plain `contextdb <path>` (no `--write`) is a bounded read session by default** — it never
creates the store, never opens a writable handle, and leaves every byte in the store folder
unchanged; even `.help` doesn't rewrite anything. There is nothing to copy first. Two refusals
follow from that, and both are what you will hit first if you copy a recipe without the flag:

```bash
# A store that does not exist. A read session never creates one.
contextdb ./missing.db </dev/null
# stderr: Error: there is no store at this path: read-image store is not there: No such file or
#         directory (os error 2); --write creates it
# exit:   1

# A store that DOES exist — create ./demo.db first with the paste below, or this second
# example answers `store_not_found` too, not the write refusal it is here to show.
echo "INSERT INTO decisions VALUES ('550e8400-e29b-41d4-a716-446655440000', 'draft');" \
  | contextdb ./demo.db
# stderr: Error: this statement writes, so it needs a write session; rerun with --write
# exit:   1
```

`:memory:` is always writable and needs no flag; an explicit `--write` on it is an accepted no-op.

Reading is not exclusive. Several direct readers coexist on one store, and while a live process
owns the store a read session is served by that owner over its authenticated local channel — same
commands, same output shapes. `DatabaseLocked` is the *writable*-open refusal only: `--write`
against a store a live writer already owns is refused (`held_by_writer`), and the refusal tells
you that dropping `--write` reaches that owner's channel. See
[`AGENTS.md`'s "Reading is safe by default"](../../AGENTS.md#reading-is-safe-by-default) for the
owner-route/file-route contract. `scripts/store-health-check.sh` (below) reads a store directly,
with no peek copy, as a working example.

When stdin is not a terminal the CLI runs in **pipe mode**: no banner, no prompt, results on
stdout, every error and diagnostic on stderr, and the process exit code tells you what happened.
That is the mode to script in.

## Create, insert, query — one paste

Statements are semicolon-terminated and may span as many lines as they need. A statement ends at
the first `;` that is **not** inside a quoted string or a comment, so a real schema file runs as
written. Anything still open when the input ends is run at that point.

```bash
contextdb ./demo.db --write <<'SQL'
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
(1 rows)
```

Every successful ordinary `SELECT` ends with exactly one `(N rows)` footer and nothing else.

The `STATE MACHINE` clause is enforced by the engine, not by your code. Push the row backwards and
it is refused:

```bash
echo "UPDATE decisions SET status = 'draft' WHERE id = '550e8400-e29b-41d4-a716-446655440000';" \
  | contextdb ./demo.db --write
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

Human output prints one row per line and ends with a single `(N rows)` footer. No renderer
truncates and there is no row cap to disable — a result publishes complete or refuses. There is no
pager either: pipe to `less` if you want one.

## Machine output: `--json`

`--json` makes stdout **pure JSON Lines** — one complete JSON document per line, one line per
statement or meta-command that produced a result, and nothing else. A run that produced no results
writes nothing to stdout.

A query is one line: one namespaced document carrying the result's columns and its rows.

```bash
echo "SELECT id, status FROM decisions;" | contextdb ./demo.db --json
```
```json
{"result":{"columns":["id","status"],"rows":[{"id":"550e8400-e29b-41d4-a716-446655440000","status":"active"}]}}
```

```bash
echo "SELECT id, status FROM decisions;" | contextdb ./demo.db --json | jq -r '.result.rows[].status'
# active
```

`columns` carries the declared order, so a consumer can render a table without guessing it back
out of the first row.

### A result is complete or refused — it is never truncated

A `SELECT` succeeds only when the whole result fits the declared ceilings (`--read-result-rows`,
500 by default, and `--read-result-bytes`, 4 MiB). Crossing either one publishes **no rows** and
refuses with `owner_limit_exceeded`, exit `1`; the refusal carries the refused statement and the
copy-ready command that pages it instead, all on one line:

```text
Error: the answer went past the rows this read is allowed: 500 rows; .cursor open SELECT * FROM decisions; raise --read-result-rows / --read-result-bytes for a deliberate one-shot export
```

Under `--json` the same refusal is
`{"error":{"class":"io","detail":{"kind":"owner_limit_exceeded","limit":"result_rows","remedy_command":".cursor open SELECT * FROM decisions","statement":"SELECT * FROM decisions","value":500}, ...}}`
— branch on `detail.kind` and run `detail.remedy_command` verbatim.

So the two escapes for a big read are: raise `--read-result-rows` / `--read-result-bytes` for a
deliberate one-shot export on a store nobody owns, or page it with the session cursor —
`.cursor open <SELECT>`, then `.cursor fetch` until `has_more` is `false`. Reading through a live
owner you can only lower the owner's ceiling, never raise it, so the cursor is the in-session
escape there. Details: [`docs/cli.md`](../../docs/cli.md#large-results-the-session-cursor).

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
{"tables":{"continuation":null,"has_more":false,"items":["decisions"]}}
```

`.tables` and `.events status` answer as bounded pages: as many complete items as fit
`result_bytes`, plus `has_more` and a `continuation` string that is non-null exactly when
`has_more` is true. Resume with `.tables --continue <continuation>`; a continuation is accepted
only by the command that issued it.

```bash
printf '.schema decisions\n' | contextdb ./demo.db --json | jq '.schema.state_machine'
```
```json
{"column":"status","transitions":{"active":["superseded"],"draft":["active","rejected"]}}
```

`.schema` returns the table's declared contract as data — `columns` (type, nullability, key /
unique / immutable flags, `references` with its propagate clause, vector quantization, rank
policy), `primary_key`, `indexes`, `state_machine`, `retain`, `sync_direction`, `conflict_policy`,
`dag_edge_types`, `propagate` — plus `ddl`, the exact text the human `.schema` prints, so a
snapshot/replay flow keeps working. **A policy the table never declared is absent, not defaulted.**
Test with `jq '.schema | has("retain")'`, never by comparing against a value nobody wrote.

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
  0) jq -r '.result.rows[].id' rows.json ;;
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
  other than `COUNT` and `SUM` (`AVG`, `MIN` and `MAX` each refuse with
  `plan error: unknown function`). Full list: [`docs/query-language.md`](../../docs/query-language.md#unsupported-features).
- **Logs go to stderr** at level `ERROR` by default; raise with `RUST_LOG=debug`.

## Verify a store deterministically — run the script, don't hand-check

Whenever you need to confirm a store is healthy and readable — opens cleanly, `.tables` lists what
you expect, every table's row count is actually readable — run
[`scripts/store-health-check.sh`](../../scripts/store-health-check.sh) instead of re-deriving the
same three checks by hand:

1. `CONTEXTDB_CLI=<path-to-contextdb> scripts/store-health-check.sh ./demo.db`
2. Expected output on a healthy store: an `OK   diagnose: ...` line, an `OK   .tables: N table(s)`
   line, then one `OK   <table>  <n> row(s)` line per table.
3. Exit code `0` means every check passed. **If it exits `1`, the printed line tells you exactly
   which check failed** (diagnose reported a problem, or one table's row count was unreadable) —
   go fix that specific thing rather than re-running the whole store from scratch.

```bash
CONTEXTDB_CLI=./bin/contextdb scripts/store-health-check.sh ./demo.db
```
```text
OK   diagnose: diagnose: './demo.db' is current-format and its schema layout reads cleanly; nothing to correct.
OK   .tables: 1 table(s)
OK   decisions                        1 row(s)
```

Each store-reading step also emits one `read_route` notice on stderr, naming the route and the
committed snapshot it read; stdout stays the `OK`/`FAIL` lines the script's exit code summarizes.

The script itself reads the store directly — a plain `contextdb <path>` open (no `--write`) is
already a bounded read session, so there is no peek copy to make — making it also the reference
implementation to copy if you're writing your own read-only check.

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

## Restrict which rows a reader sees

Row-level authorization is opt-in and declared by the application. A data table names the column
carrying each row's access-control id; a grant table says which principal holds which id:

```sql
CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID);
CREATE TABLE notes (id INTEGER PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT);
INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id)
VALUES ('33333333-3333-4333-8333-333333333333', 'Agent', 'report-reader',
        '44444444-4444-4444-8444-444444444444');
```

Read through a handle that names the principal. An ordinary query and a bounded read session
answer the same rows, and rows the principal holds no grant for are absent from both:

```rust
let reader = db.scoped_with_constraints(None, None,
    Some(Principal::Agent("report-reader".to_string())));
let rows = reader.execute("SELECT payload FROM notes ORDER BY id", &HashMap::new())?;
```

<!-- enforced by: crates/contextdb-engine/tests/read_visibility_route_parity.rs::a_granting_principal_sees_the_same_rows_on_both_live_routes -->

A handle that narrowed itself by context or scope but named no principal that can hold grants is
refused the table with `Error::PrincipalRequired` — narrowing by another axis never turns the
grant filter off.
<!-- enforced by: crates/contextdb-engine/tests/read_visibility_route_parity.rs::a_context_only_handle_is_refused_an_access_controlled_table_on_both_live_routes -->

An administrative handle — no context, no scope, no principal — reads every row, and a table that
declares no `ACL REFERENCES` column is unaffected.
<!-- enforced by: crates/contextdb-engine/tests/read_visibility_route_parity.rs::a_table_without_an_acl_declaration_narrows_by_context_alone_on_every_route -->

A direct read of a CLOSED store declares nothing: `ReadSession::open` on a path with no live owner, or the CLI pointed at a path, reads it as its owner does and sees every row. Row-level authorization governs handles that name a principal; file permissions govern who may read a closed store at all.
<!-- enforced by: crates/contextdb-engine/tests/read_visibility_route_parity.rs::the_direct_route_reads_a_closed_store_as_its_owner_with_no_declared_narrowing -->

ACL is authorization, not relevance ranking: a withheld row is withheld because the reader is not
entitled to it, never scored or surfaced lower. Full recipe:
[`docs/query-language.md`](../../docs/query-language.md#access-controlled-rows-acl).

## Next

- DAG edges, graph traversal, state machines, cascades → [`skills/querying-the-graph/SKILL.md`](../querying-the-graph/SKILL.md)
- Replicate across machines → [`skills/sync/SKILL.md`](../sync/SKILL.md)
- Similarity search and ranking → [`skills/vector-search/SKILL.md`](../vector-search/SKILL.md)
- Distribute jobs and blobs → [`skills/work-fabric/SKILL.md`](../work-fabric/SKILL.md)
- Retention, purge, backups, sync liveness → [`skills/operating-a-store/SKILL.md`](../operating-a-store/SKILL.md)
- Routes, sinks, schedules → [`skills/running-triggers-and-schedules/SKILL.md`](../running-triggers-and-schedules/SKILL.md)
