---
name: using-contextdb
description: Open a contextdb store, run SQL, tune the reading session cache, count groups, rank rows within groups, and handle JSON results and refusals.
---

# Using contextdb

**Prerequisite:** needs the `contextdb` binary. In a checkout of this repo:
`cargo build --release -p contextdb-cli`. Other install options (release download, `cargo
install`): [`docs/getting-started.md`](../../docs/getting-started.md).

contextdb is an embedded database — one file, one process, relational + graph + vector under one
transaction. `contextdb` is how you drive it from a shell. There is no server to start and no
schema to migrate to: you create your own tables (see the [README](../../README.md)).

## Open a database

```bash
contextdb ./demo.db              # read an existing store; never creates, never mutates
contextdb ./demo.db --write      # create it if missing, and permit mutation
contextdb "$(mktemp -d)/scratch.db" --write  # task-scoped durable scratch
```

A file-backed open without `--write` is a bounded read session; the refusals, owner-vs-file
routing, and JSON/exit-code contract are in [`docs/cli.md`](../../docs/cli.md#cli-client-contextdb).
`scripts/store-health-check.sh` (below) reads a store directly as a working example.

```bash
# A store that does not exist. A read session never creates one.
contextdb ./missing.db </dev/null
# stderr: Error: there is no store at this path: read-image store is not there: No such file or
#         directory (os error 2); --write creates it
# exit:   1

# A store that DOES exist — create ./demo.db first with the paste below, or this second
# example answers the missing-store refusal too, not the write refusal it is here to show.
echo "INSERT INTO decisions VALUES ('550e8400-e29b-41d4-a716-446655440000', 'draft');" \
  | contextdb ./demo.db
# stderr: Error: this statement writes, so it needs a write session; rerun with --write
# exit:   1
```

There is no in-memory mode: `:memory:` is refused. A temporary file is still a durable store;
use `--write` to create or mutate it, and retain its path to reopen it.
<!-- enforced by: read_cli_journeys_invocation::memory_path_is_refused_because_contextdb_has_no_in_memory_mode, read_cli_journeys_invocation::write_flag_creates_the_store_a_read_refused_to_create -->

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
echo "CREATE TABLE t (id UUID PRIMARY KEY);" | contextdb "$(mktemp -d)/scratch.db" --write --json
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

A scripted run continues after a refused statement, including a memory or disk refusal; later
successful statements do not turn its exit status back into success.
<!-- enforced by: read_cli_journeys_script_continuation::a_memory_refusal_is_followed_by_independent_work_and_recovery, read_cli_journeys_script_continuation::a_disk_refusal_is_followed_by_independent_work_and_recovery -->

### Recover within the same session

1. Read the error on stderr; an empty successful result is not a refusal.
2. After a resource refusal, inspect `SHOW MEMORY_LIMIT` or `SHOW DISK_LIMIT`, then raise that
   limit with `SET MEMORY_LIMIT '10M'` or `SET DISK_LIMIT '1G'` if appropriate for your workload.
3. Retry the refused statement. Independent statements can run before recovery too. The script
   still exits `1` because an earlier statement failed.
<!-- enforced by: read_cli_journeys_script_continuation::a_refusal_is_distinguishable_from_a_successful_empty_result, read_cli_journeys_script_continuation::a_memory_refusal_is_followed_by_independent_work_and_recovery, read_cli_journeys_script_continuation::a_disk_refusal_is_followed_by_independent_work_and_recovery -->

This small example isolates continuation without needing a machine-specific memory ceiling:

```bash
contextdb "$(mktemp -d)/scratch.db" --write --json <<'SQL'
SELECT * FROM missing;
SELECT 1 + 1;
SQL
printf 'exit=%s\n' "$?"
```

Stdout:

```text
{"result":{"columns":["expr"],"rows":[{"expr":2}]}}
exit=1
```

Stderr:

```json
{"error":{"class":"sql","line":1,"message":"table not found: missing"}}
```
<!-- enforced by: exit_code_contract::run_continues_after_an_error_and_still_exits_one -->

For bounded reads, selecting an oversized stored value is refused before loading its body;
opening the session does not itself require loading that value. A refused statement leaves no
new charge behind. Reduce the requested data or use a sufficient declared read budget.
<!-- enforced by: direct_file_image_memory_lifetime::one_oversized_durable_value_is_refused_before_its_source_buffer_is_allocated -->

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
  `SHOW` forms. `SET DISK_LIMIT` persists and survives reopen,
  including on temporary-file stores.
- **`LIMIT` is required on vector search.** Unbounded `ORDER BY <=>` is rejected. See the
  `vector-search` skill.
- **Not supported, by design:** `HAVING`, `UNION`/`INTERSECT`/`EXCEPT`, `INSERT ... SELECT`,
  `WITH RECURSIVE`, subqueries outside `IN`, and aggregates other than `COUNT` and `SUM` (`AVG`,
  `MIN` and `MAX` each refuse with `plan error: unknown function`). `GROUP BY` works in one form
  only — `COUNT(*)` grouped by a column list the table declares under `MAINTAIN COUNT` — and every
  other grouped form is refused. The one window function is `ROW_NUMBER() OVER ([PARTITION BY
  cols] ORDER BY cols)`; other window forms are refused by name. Full list:
  [`docs/query-language.md`](../../docs/query-language.md#unsupported-features).
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

<!-- enforced by: crates/contextdb-engine/tests/read/read_visibility_route_parity.rs::a_granting_principal_sees_the_same_rows_on_both_live_routes -->

A handle that narrowed itself by context or scope but named no principal that can hold grants is
refused the table with `Error::PrincipalRequired` — narrowing by another axis never turns the
grant filter off.
<!-- enforced by: crates/contextdb-engine/tests/read/read_visibility_route_parity.rs::a_context_only_handle_is_refused_an_access_controlled_table_on_both_live_routes -->

An administrative handle — no context, no scope, no principal — reads every row, and a table that
declares no `ACL REFERENCES` column is unaffected.
<!-- enforced by: crates/contextdb-engine/tests/read/read_visibility_route_parity.rs::a_table_without_an_acl_declaration_narrows_by_context_alone_on_every_route -->

A direct read of a CLOSED store declares nothing: `ReadSession::open` on a path with no live owner, or the CLI pointed at a path, reads it as its owner does and sees every row. Row-level authorization governs handles that name a principal; file permissions govern who may read a closed store at all.
<!-- enforced by: crates/contextdb-engine/tests/read/read_visibility_route_parity.rs::the_direct_route_reads_a_closed_store_as_its_owner_with_no_declared_narrowing -->

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

## Tune a reading session's page cache

Prerequisites: the `contextdb` binary installed above and an existing file store. Python 3
is used only to select stable inspection fields in this example.

1. Open without `--write`; declare `SET READ_CACHE_SHARE = 0` to disable file page caching,
   or choose an integer percentage from 0 through 100 of the budget after fixed reservations.
2. Inspect with `SHOW READ_CACHE_SHARE` in that same process. The setting belongs to the
   session and is not persisted; `SET READ_CACHE_SHARE = DEFAULT` restores the derived default.
3. Read `last_refusal` after a refusal. An out-of-range or non-integer declaration returns
   `InvalidSetting`; a valid percentage can still fail memory admission. Both keep the previous
   declaration.
<!-- enforced by: read_cache_share_statements::a_declared_read_cache_share_is_inspected_as_declared_and_effective, read_cache_share_statements::a_read_cache_share_is_scoped_to_its_session_and_not_persisted, read_cache_share_statements::read_cache_shares_up_to_the_budget_are_accepted_and_100_and_above_refused, read_cache_share_inspection::cache_inspection_keeps_the_declaration_and_the_admitted_session_budget -->

Runnable example (stdout below; route notices use stderr):

```bash
db="$(mktemp -d)/cache.db"
printf 'CREATE TABLE t (id INTEGER PRIMARY KEY);\n' | contextdb "$db" --write --json > /dev/null
printf 'SET READ_CACHE_SHARE = 0;\nSHOW READ_CACHE_SHARE;\n' | contextdb "$db" --json |
  python3 -c 'import sys,json; docs=[json.loads(s) for s in sys.stdin]; row=docs[-1]["result"]["rows"][0]; print(json.dumps({k:row[k] for k in ("declared","effective_bytes")}))'
```

```text
{"declared": 0, "effective_bytes": 0}
```

## Count groups and number rows within a group

Prerequisite: the binary installed above; the example creates its own file store.

1. For grouped `COUNT(*)`, create an index led by the count columns, then declare that list
   with `ALTER TABLE ... SET MAINTAIN COUNT`. Pin a leading prefix with `=` or `IS NULL` and
   group by the remaining columns. A missing declaration refuses with the required ALTER text.
2. For row positions, use `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` and include
   tie-breaking columns when deterministic selection matters. Filter the position in an outer query
   to take a limited number per group. Other window forms, including a missing `ORDER BY`, return
   `WindowFunctionNotSupported`.
<!-- enforced by: consumer_declared_count_and_pending_seek_contract::grouped_event_counts_use_the_declared_context_stream_count_and_survive_reopen, grouped_session_top_n_contract::grouped_session_top_n_has_exact_context_ranks_and_fixed_declared_index_work -->

```bash
db="$(mktemp -d)/groups.db"
contextdb "$db" --write --json > /dev/null <<'SQL'
CREATE TABLE jobs (id INTEGER PRIMARY KEY, queue TEXT, state TEXT);
CREATE INDEX jobs_queue_state ON jobs(queue, state);
ALTER TABLE jobs SET MAINTAIN COUNT (queue, state);
INSERT INTO jobs VALUES (1, 'local', 'ready');
INSERT INTO jobs VALUES (2, 'local', 'ready');
INSERT INTO jobs VALUES (3, 'local', 'done');
SQL
contextdb "$db" --write --json <<'SQL'
SELECT state, COUNT(*) AS n FROM jobs WHERE queue = 'local' GROUP BY state;
SELECT id, ROW_NUMBER() OVER (PARTITION BY queue ORDER BY id DESC) AS position FROM jobs ORDER BY id;
SQL
```

```json
{"result":{"columns":["state","n"],"rows":[{"n":1,"state":"done"},{"n":2,"state":"ready"}]}}
{"result":{"columns":["id","position"],"rows":[{"id":1,"position":3},{"id":2,"position":2},{"id":3,"position":1}]}}
```

## Spell an inner join with JOIN

Prerequisite: the `jobs` store from the previous example.

1. Use `JOIN ... ON ...` wherever you would use `INNER JOIN ... ON ...`; the parser produces
   the same join. `LEFT JOIN` retains its existing meaning.
2. If parsing fails, check the table aliases and `ON` predicate against the runnable form below.
<!-- enforced by: bare_join_keyword_contract::bare_join_parses_to_the_same_ast_as_inner_join, bare_join_keyword_contract::bare_join_without_any_alias_parses, bare_join_keyword_contract::left_join_still_parses_as_left -->

```bash
printf 'SELECT a.id AS left_id, b.id AS right_id FROM jobs a JOIN jobs b ON a.id = b.id WHERE a.id = 1;\n' |
  contextdb "$db" --write --json
```

```json
{"result":{"columns":["left_id","right_id"],"rows":[{"left_id":1,"right_id":1}]}}
```

## Keep a reading session open across writer restarts

Prerequisites: the binary installed above and Python 3 for this two-process example.

1. Open a reading session without `--write`. It reads the committed file when no writer is
   active and follows a returning writer of that same store incarnation on later commands.
2. Keep the reader open while the writer exits and restarts. A cursor already opened keeps its
   captured snapshot; ordinary later commands select the current route. If an in-flight command
   loses its owner and fails, handle that refusal before issuing the next command.
<!-- enforced by: read_cli_journeys_route_changes::a_reading_shell_reports_its_route_again_each_time_its_writer_leaves_or_returns, reader_continuity_contract::a_file_reader_keeps_its_snapshot_after_a_writer_takes_the_store -->

This example drives ordinary statements through the same reader process throughout:

```bash
python3 - <<'PY'
import json, pathlib, subprocess, tempfile
path = str(pathlib.Path(tempfile.mkdtemp()) / "continuity.db")
def shell(write=False):
    return subprocess.Popen(["contextdb", path, "--json"] + (["--write"] if write else []),
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)
def statement(process, sql):
    process.stdin.write(sql + "\n")
    process.stdin.flush()
    return json.loads(process.stdout.readline())
def close(process):
    process.stdin.close()
    assert process.wait() == 0
writer = shell(True)
statement(writer, "CREATE TABLE items (id INTEGER PRIMARY KEY);")
statement(writer, "INSERT INTO items VALUES (1);")
reader = shell()
query = "SELECT id FROM items ORDER BY id;"
print(statement(reader, query)["result"]["rows"])
close(writer)
print(statement(reader, query)["result"]["rows"])
writer = shell(True)
statement(writer, "INSERT INTO items VALUES (2);")
print(statement(reader, query)["result"]["rows"])
close(reader)
close(writer)
PY
```

```text
[{'id': 1}]
[{'id': 1}]
[{'id': 1}, {'id': 2}]
```

The example hides stderr to keep its stdout comparison fixed; retain stderr in a real script
for the typed refusal and route notices.
