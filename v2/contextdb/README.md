# contextdb

An embedded SQL database: tables in one folder, inside your own process.

An application that keeps its records in one store and its embeddings in a separate vector
library has to keep the two consistent by hand; here a vector is a column, a row and its vector
commit together, and one `SELECT` filters rows and orders them by distance.

This is the first version of contextdb 2. What it has:

- tables with typed columns, a primary key of one column, of several or of none, `NOT NULL`,
  `UNIQUE`, `DEFAULT`, references between tables, and indexes;
- rules a table declares and every write obeys: rows that are never changed, the states a column
  may move through, edges that may not close a cycle, rows that follow a change of another row;
- who may see and write a row: Contexts, scope labels and principals;
- `ALTER TABLE`, `DROP TABLE` and `DROP INDEX`;
- `INSERT` (also `ON CONFLICT ... DO UPDATE`), `UPDATE`, `DELETE` and `SELECT`, which joins
  tables, groups, takes `WITH`, `UNION`, subqueries, `ROW_NUMBER()` and a pattern over a table
  of edges (`GRAPH_TABLE`);
- vector search: the rows nearest a vector by cosine distance, each with its distance, over
  vectors that a column may keep in partitions; exact on request, and through an index that
  `run_maintenance_cycle` builds and brings up to date once a column holds many vectors; a
  search can be ranked by a formula the column declares (`USE RANK`);
- `EXPLAIN` of any `SELECT`, `INSERT`, `UPDATE` or `DELETE`, which says how it would be answered;
- transactions, whose conflicts are decided when they commit;
- one process writing a store while any number of other processes read it;
- a memory limit that holds.

What it does not have yet: sync between machines, recursive queries, and the SQL named under
Statements as refused; each of those is refused and changes nothing. Some clauses are accepted,
kept and printed back by `.schema`, and nothing acts on them yet: `HISTORY`, `SYNC`,
`SYNC CONFLICT`, `DELIVERY MANIFEST` and `EDGE DISCARD`. Declared `RETAIN` removes due rows
during maintenance; see [Retention and checking a store](#retention-and-checking-a-store).
<!-- enforced by: each_retained_row_leaves_at_the_second_its_judgement_gives -->
`table_meta` reports `RETAIN`, `HISTORY`, `SYNC` and `SYNC CONFLICT`.
`Database::declared_table(sql)` returns the normalized table name and the same
`TableMeta` projection as a tuple, without opening a store.

It is not on crates.io yet. With Rust 1.89 or newer on Linux or macOS, clone
<https://github.com/context-graph-ai/contextdb>. Create a Rust binary project with
`cargo new notes-demo`, add this dependency to its `Cargo.toml`, replace `src/main.rs` with the
program below, and run `cargo run` from that project. Substitute your checkout path below.
Each standalone program uses a fresh store: its named folder must not exist before running it.

```toml
[dependencies]
contextdb = { path = "<your checkout>/v2/contextdb" }
```

```rust,no_run
use contextdb::{params, Database, Uuid, Value};

fn main() -> contextdb::Result<()> {
    let db = Database::open("./notes")?; // a folder, created if it is missing
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT NOT NULL, embedding VECTOR(3),
                             made TIMESTAMP DEFAULT NOW())",
        &params! {},
    )?;
    // A real embedding has hundreds of numbers and comes from a model; three show the shape.
    let notes = [("buy milk", vec![0.9, 0.1, 0.0]), ("call the plumber", vec![0.0, 0.2, 0.9])];
    for (body, embedding) in notes {
        db.execute(
            "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, $embedding)",
            &params! { "id" => Uuid::new_v4(), "body" => body, "embedding" => embedding },
        )?;
    }

    let tx = db.transaction()?; // rolls back if dropped
    tx.execute("UPDATE notes SET body = 'buy oat milk' WHERE body = 'buy milk'", &params! {})?;
    tx.commit()?;

    // The notes nearest a vector, nearest first, each with its distance.
    let found = db.execute(
        "SELECT body, embedding <=> $q AS distance FROM notes ORDER BY embedding <=> $q LIMIT 10",
        &params! { "q" => vec![1.0, 0.0, 0.0] },
    )?;
    for row in found.rows() {
        if let (Value::Text(body), Value::Float64(distance)) = (&row[0], &row[1]) {
            println!("{body}: {distance:.3}");
        }
    }
    Ok(())
}
```

It prints `buy oat milk: 0.006` and then `call the plumber: 1.000`. Call `db.close()?` when you
are done, as the later programs do: it lets the store go at once, and every later call on the
handle that can fail returns `DatabaseClosed` (see Refusals for the infallible
trigger-inspection calls that panic instead).
<!-- enforced by: every_writer_read_call_refuses_a_closed_handle -->

Maintenance runs automatically by default; a host choosing `Maintenance::Caller` calls
`db.run_maintenance_cycle()?` as its report directs (see Maintenance under Vector search);
a search before that still finds every vector, and reads more to do it. The worker exists
only while a writing handle is open; read-only sessions start no maintenance. The default
poll interval is 60,000 milliseconds, persisted with `SET MAINTENANCE_POLL_INTERVAL` and
inspected with `SHOW MAINTENANCE_POLL_INTERVAL`. A due vector build also wakes the worker.
Timer and caller cycles seal partial vector tails; a vector wake advances builds and expiry
without sealing those tails. Expiry is asynchronous: a due row stays readable until removed.
Closing stops and joins the worker; it does not request a final cycle. The CLI has no
maintenance command, so a short piped write is not a way to drive expiry. Keep a writing
host open for automatic maintenance, or call `run_maintenance_cycle` in an embedded host.

This page is also the crate's documentation, and the documentation tests compile this program as it is written here.
Rust 1.89 or newer; Linux and macOS. Licence: Apache-2.0. It is developed in the open at
<https://github.com/context-graph-ai/contextdb> by the context-graph-ai organisation.

## Statements

Syntax reference: brackets mark optional clauses and names stand for your own identifiers.
Vector options belong to the type and occur in the order shown, before column constraints:
`embedding VECTOR(3) PARTITION_KEY (context_id) NOT NULL`. Put `RANK_POLICY` on the vector
column searched with `USE RANK`. Either compaction percentage may stand alone, or both may
appear in either order; an omitted percentage uses its default.
<!-- enforced by: every_clause_a_table_may_declare_is_printed_back_and_declares_the_same_table_again, every_script_gives_its_expected_output -->

```text
CREATE TABLE [IF NOT EXISTS] name (
  column type [PRIMARY KEY] [NOT NULL] [UNIQUE] [DEFAULT value] [IMMUTABLE]
         [REFERENCES table (column)
            [ON STATE state PROPAGATE SET state [MAX DEPTH n] [ABORT ON FAILURE]]]
         [EXPIRES]  -- one TIMESTAMP column; acts only with RETAIN
         [CONTEXT_ID]
         [SCOPE_LABEL ('label', ...) | SCOPE_LABEL_READ ('label', ...) WRITE ('label', ...)]
         [ACL REFERENCES table (column)],
  column VECTOR(n) [WITH (quantization = 'F32' | 'SQ8')]             -- F32 when left out
                   [PARTITION_KEY (column, ...) [MAX_PARTITIONS m]]  -- m is 256 when left out
                   [SEARCH_MODE AUTO | EXACT | INDEXED]
                   [INDEXING_THRESHOLD a]                                 -- a is 100000 when left out
                   [COMPACTION NONE | COMPACTION (CHANGE_PERCENT = c, TOMBSTONE_PERCENT = t)]
                   [NOT NULL] [DEFAULT value] [IMMUTABLE]
                   [RANK_POLICY (JOIN table ON column, FORMULA 'formula', SORT_KEY name)],
  ...,
  [PRIMARY KEY (column, ...)],
  [UNIQUE (column, ...)],
  [FOREIGN KEY (column, ...) REFERENCES table (column, ...)]
)
  [IMMUTABLE]
  [STATE MACHINE (column: state -> [state, ...], state -> state, ...)]
  [DAG ('edge_type', ...)]
  [PROPAGATE ON EDGE edge_type INCOMING | OUTGOING | BOTH STATE state SET state
     [MAX DEPTH n] [ABORT ON FAILURE]]
  [PROPAGATE ON STATE state EXCLUDE VECTOR]
  [RETAIN n SECONDS | MINUTES | HOURS | DAYS [SYNC SAFE]]
  [HISTORY ALL | CURRENT ONLY]
  [SYNC OFF | PUSH ONLY | PULL ONLY | TWO WAY]
  [SYNC CONFLICT KEEP FIRST | KEEP LATEST]
  [MAINTAIN COUNT (column, ...) [, (column, ...)] | ALL PREFIXES | NONE]
  [DELIVERY MANIFEST OVER table, ...]
  [EDGE DISCARD NEVER | AFTER OUTCOME | ALWAYS];
CREATE INDEX name ON table (column [ASC | DESC], ...);
CREATE INDEX name ON table ((column -> 'key'));
ALTER TABLE table ADD [COLUMN] column type ...;
ALTER TABLE table DROP [COLUMN] column [RESTRICT | CASCADE];
ALTER TABLE table RENAME COLUMN column TO column;
ALTER TABLE table SET RETAIN ... | SET HISTORY ... | SET SYNC ... | SET SYNC CONFLICT ...
                      | SET MAINTAIN COUNT ...;
ALTER TABLE table DROP RETAIN;
ALTER TABLE table ALTER COLUMN column SET MAX_PARTITIONS m | SET SEARCH_MODE mode
  | SET INDEXING_THRESHOLD a | SET INDEXING_THRESHOLD DEFAULT
  | SET COMPACTION NONE | SET COMPACTION (CHANGE_PERCENT = c, TOMBSTONE_PERCENT = t)
  | SET COMPACTION DEFAULT;
DROP TABLE [IF EXISTS] table;
DROP INDEX [IF EXISTS] name ON table;
INSERT INTO table (column, ...) VALUES (value, ...), (value, ...)
  [ON CONFLICT (column, ...) DO UPDATE SET column = value, ...];
UPDATE table SET column = value, ... [WHERE condition];
DELETE FROM table [WHERE condition];
[WITH name AS (select), ...]
SELECT [DISTINCT] expression [AS name], ... | * [FROM table [alias]
    [[INNER | LEFT] JOIN table [alias] ON condition | CROSS JOIN table [alias]] ...]
  [WHERE condition] [GROUP BY expression, ...] [HAVING condition]
  [UNION [ALL] SELECT ...]
  [ORDER BY expression [ASC | DESC], ...] [LIMIT n] [OFFSET m];
SELECT ... FROM GRAPH_TABLE(edge_table MATCH (a)-[:TYPE]->{1,n}(b) [WHERE condition]
  COLUMNS (a.id AS name, b.id AS name, depth AS name));
SELECT column, ... | * FROM table [WHERE condition]
  ORDER BY column <=> vector [USE VECTOR AUTO | EXACT | INDEXED] [USE RANK name] [LIMIT k] [OFFSET m];
                                                           -- USE RANK needs a LIMIT
EXPLAIN select | insert | update | delete;
SET MEMORY_LIMIT 512M;   SET MEMORY_LIMIT NONE;   SHOW MEMORY_LIMIT;
SHOW VECTOR_INDEXES;
SHOW VECTOR_PARTITIONS [FOR table.column] [LIMIT count [OFFSET start]];
SHOW STATEMENT_STATISTICS [LIMIT count [OFFSET start]];   RESET STATEMENT_STATISTICS;
SET STATEMENT_STATISTICS_LIMIT count;
CREATE TRIGGER name ON table WHEN INSERT | UPDATE [INCLUDING SYNC];
CREATE EVENT TYPE name WHEN INSERT | UPDATE | DELETE ON table;
CREATE SINK name TYPE callback | TYPE webhook URL 'url';
CREATE ROUTE name EVENT event_type TO sink [WHERE column IN (value, ...)];
CREATE SCHEDULE name EVERY 'n seconds' TX (callback)
  [MISSED_TICK_POLICY 'skip-and-audit' | 'catch-up' | 'fail-loud' [WITHIN n SECONDS]];
DROP TRIGGER [IF EXISTS] name [ON table];
DROP ROUTE [IF EXISTS] name;
DROP SCHEDULE [IF EXISTS] name;
```

- A default is a literal or `NOW()`. A table, column or index name written without quotes is
  folded to lower case, as in Postgres; a state or an edge type is a value and is kept as written.
- A condition or a value is built from column names (`table.column` or `alias.column` when
  tables are joined), literals, `$name` values, `=  !=  <>  <  <=  >  >=`, `AND`, `OR`, `NOT`,
  `IS NULL`, `IS NOT NULL`, `+  -  *  /`, parentheses, `NOW()` and `COALESCE(a, b, ...)`; no
  other function is read. A comparison with NULL is not true, as in standard SQL.
  `x LIKE 'a%'` and `NOT LIKE` match text (`%` any run, `_` one character, backslash escapes
  either), and `x BETWEEN a AND b` and `NOT BETWEEN` include both ends. A JSON column is read
  with `->` (the field as JSON) and `->>` (the field as text). JSON values compare by value,
  so `5` equals `5.0`, and sort as Postgres `jsonb` sorts them: null, string, number, boolean,
  array, object; an array or an object by its size first; an object's keys shorter first, then
  by their bytes. `<` and `>` between JSON values are refused. A statement nests at most 64
  levels of parentheses, `NOT`, unary minus and subqueries; one deeper is a `ParseError`.
  An index over a field, `CREATE INDEX name ON table ((column -> 'key'))`, answers a condition on
  that field and follows the rows it indexes.
- `x IN (a, b, ...)` and `x NOT IN (a, b, ...)` test a value against a list, and
  `(x, y) IN ((a, b), (c, d), ...)` tests several columns at once, both as standard SQL has
  them. `x IN (SELECT ...)` and `(SELECT ...)` as one value take a subquery that does not read
  the outer row; one that does, and `EXISTS`, are refused (`SubqueryNotSupported`).
- `SELECT` takes `COUNT(*)`, `COUNT(column)`, `SUM`, `MIN`, `MAX` and `AVG`, with `GROUP BY` and
  `HAVING`, `DISTINCT`, and `AS` to name a column of the result. `ORDER BY` takes any
  expression of the select list or the table; `LIMIT` and `OFFSET` take a number or a `$name`.
- `INNER JOIN`, `LEFT JOIN` and `CROSS JOIN` join tables, each table with an optional alias.
  `RIGHT JOIN`, `FULL JOIN` and a second table after a comma are refused. `FROM` takes a table
  name, a `GRAPH_TABLE`, or a non-correlated derived table (`FROM (SELECT ...) AS x`),
  also with output-column aliases (`AS x(a, b)`). `LATERAL` is refused by name.
  <!-- enforced by: every_script_gives_its_expected_output -->
- `UNION` and `UNION ALL` join the answers of several `SELECT`s, and `ORDER BY`, `LIMIT` and
  `OFFSET` after the last apply to the whole; `UNION` drops repeated rows and `UNION ALL` keeps them. `INTERSECT` and `EXCEPT` are refused.
- `WITH name AS (select)` names an answer for the statement that follows; `WITH RECURSIVE` is
  refused (`RecursiveCteNotSupported`).
- `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` is the one window function; any other is
  refused (`WindowFunctionNotSupported`).
- **Names in a result.** A column is named by its `AS`; without one, a column by its own name,
  a function by its name (`ROW_NUMBER()` is `row_number`, `COUNT(*)` is `count`), and a
  subquery that gives one item by that item (`SELECT *` gives `?column?`). A bare `ORDER BY` name that the select list gives is that output column; one that
  two different items give, as in `SELECT t.id, id FROM t ORDER BY id`, is refused as ambiguous
  (`PlanError`), where Postgres answers. The same `SELECT` item written twice in `GROUP BY`, `DISTINCT` or `ORDER BY` is one expression.
- **A whole number and a REAL** are compared by exact value, not as two doubles: a stored
  9007199254740992.0 is not equal to 9007199254740993, which Postgres calls equal. The two differ only
  past 2^53, and equality stays transitive across `UNION`, `GROUP BY` and `DISTINCT`. Text
  quoted into a `UUID` or `TIMESTAMP` column is read as one, and any other comparison of unlike
  types is refused when the statement is read, whatever the table holds.
- **`GRAPH_TABLE(edge_table MATCH (a)-[:TYPE]->{1,n}(b) ... COLUMNS (...))`** is a pattern over a
  table of edges (`source_id`, `target_id`, `edge_type`). With one step and no range it gives a
  row for each matching edge; with a range `{m,n}` it walks along matching edges and reports
  each node once, at the shallowest `depth` it is reached in, whether the filter names no start
  (every node is then a start), one or several (a start a cycle leads back to is reported too); `<-` and `-` (either way)
  follow an edge against the arrow or from either end. A walk is at most 10 steps
  (`BfsDepthExceeded`); a step with no upper bound is refused (`UnboundedTraversal`), and so is a
  label on a node and a pattern with no `COLUMNS`. A pattern stands alone in `FROM`; to join it
  to a table, name it in a `WITH` first.
- `ORDER BY ... USE RANK name` orders a search by the formula of the `RANK_POLICY` of the column whose `SORT_KEY` is `name`,
  highest first, before the `LIMIT` and `OFFSET`. It needs a vector order and a `LIMIT`
  (`UseRankRequiresVectorOrder`, `UseRankRequiresLimit`), and a rank the column does not
  declare is refused (`RankPolicyNotFound`).
- `EXPLAIN` before a `SELECT`, `INSERT`, `UPDATE` or `DELETE` prints how it would be answered
  and does nothing else: an explained write changes no row. It refuses a statement exactly as
  the statement would be refused for what is wrong in the statement itself (an unknown column,
  a number of values that does not match the columns, an `ON CONFLICT` rule the table cannot
  keep), and not what only its rows would be refused for. A statement with two faults is
  refused for either one. A `$name` with no value is refused by name (`no value is bound to
  $name`, as a run refuses it), except a search's vector, which needs no value, and a parameter beside a key
  column, which gives `partitions: every partition ($name is not bound)`. A bound value of the
  wrong type or width, and a table with an access list and no principal (`PrincipalRequired`),
  are refused as a run refuses them.
  What a similarity search prints is described under [Vector search](#vector-search).
- `ORDER BY` puts NULL last ascending and first descending.
- The vector search form, the clauses of a `VECTOR` column and the two `SHOW` statements are
  described under [Vector search](#vector-search).
- A JSON field read with `->` can be compared with a quoted JSON literal, as in
  `doc -> 'kind' = '"note"'`; `->>` compares its text instead.
  <!-- enforced by: every_script_gives_its_expected_output -->
- `--` and `/* */` comments are read past.
- The library has one way to run a transaction, `Database::transaction`; its `execute` refuses
  `BEGIN`, `COMMIT` and `ROLLBACK` by name, which are words of the `contextdb` program.

Not in this version, and refused: `INSERT ... SELECT`, `INTERSECT`, `EXCEPT`, `RIGHT` and `FULL`
`JOIN`, recursive `WITH`, `EXISTS`, a subquery that reads the outer row, a window function other
than `ROW_NUMBER`, `CREATE PROCEDURE` and `CREATE FUNCTION`, `CREATE UNIQUE INDEX`
(write `UNIQUE` on the column or in the table); and the vector clauses `IVF (...)`, `SEARCH (...)` and
`WITH (quantization = 'SQ4')`, which no version reads; the refusal of each says what to write instead.
Most are refused by name; this is the sentence the refusal prints:

```text
plan error: INSERT ... SELECT is not supported in this version of the library
```

## Column types

| Type | Holds | In Rust |
|---|---|---|
| `INTEGER`, `INT` | a 64-bit signed whole number | `Value::Int64` |
| `REAL`, `FLOAT` | a 64-bit floating-point number | `Value::Float64` |
| `TEXT` | text | `Value::Text` |
| `BOOLEAN`, `BOOL` | `TRUE` or `FALSE` | `Value::Bool` |
| `UUID` | a UUID, written as quoted text | `Value::Uuid` |
| `TIMESTAMP` | milliseconds since the first moment of 1970, UTC; written as ISO 8601 text (`'2026-01-01T00:00:01Z'`) or taken from `NOW()` | `Value::Timestamp` |
| `TXID` | the number of a transaction (see Transactions); written only as a bound `Value::TxId` | `Value::TxId` |
| `JSON` | a JSON value, written as quoted text; the value is kept, not the text's spacing | `Value::Json` |
| `VECTOR(n)` | a vector of exactly `n` finite numbers, each kept as an `f32` (a column declared `SQ8` keeps a rounded vector, see the paragraph on quantization under Vector search), written `[0.1, 0.2, 0.3]`; searched with `<=>` | `Value::Vector` |

A value must be of its column's type: text into an `INTEGER` column is refused, and so is NULL
into a `NOT NULL` column, on `INSERT` and on `UPDATE` alike. A whole number is accepted into a
`REAL` column.

## Declaring a table and its rules

A table declares what its rows must obey, and every write is checked against it, whoever makes
the write. The shell prints each statement's answer; these statements declare two tables, one
following the other:

```sql
CREATE TABLE docs (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'live'
) STATE MACHINE (status: live -> [stale, gone], stale -> [live, gone]);

CREATE TABLE chunks (
  id INTEGER PRIMARY KEY,
  doc_id INTEGER REFERENCES docs(id) ON STATE stale PROPAGATE SET stale,
  status TEXT NOT NULL DEFAULT 'live'
) STATE MACHINE (status: live -> [stale], stale -> [live]);
```

- **Keys.** `PRIMARY KEY` is written on one column or as a table element over several, and
  `UNIQUE` the same. A table may declare no primary key: it keeps its rows under a hidden key,
  made of the number of the transaction that wrote the row and a count within it, which is not a
  column and which no statement can name. A key, a `UNIQUE` rule or an index has a limit on the
  columns it may name, and a row key a limit on its length; a declaration or a write past
  either is refused (`SchemaInvalid`, `KeyTooLong`). An index entry is never refused for a long
  value: it keeps the first 256 bytes of a long text and a digest of the whole, and a read checks the
  row, so no answer changes.
- **References.** `REFERENCES parent(column)` on a column, or `FOREIGN KEY (a, b) REFERENCES
  parent(x, y)` over several. The parent's columns must be its primary key or a `UNIQUE` rule, in
  that order, and of the child's types. A reference with a NULL in it is not checked.
  There is no `ON DELETE`, `ON UPDATE` or `CHECK`; what a change in a parent does to its children
  is `PROPAGATE`.
- **`IMMUTABLE`** on a table: rows are inserted once and never updated or deleted. On a column:
  it is set when the row is inserted and never changed.
- **`STATE MACHINE (column: from -> [to, ...])`**: an `UPDATE` may move the column only along a
  listed arrow. A state with no list is a state a row cannot leave. A new row may start in any
  state. A state machine over a column that is `IMMUTABLE`, or on a table that is, is refused
  when the table is declared, since no update could ever move the state.
- **`DAG ('edge_type', ...)`** is for a table of edges, one with `source_id UUID`,
  `target_id UUID` and `edge_type TEXT`: an edge of a listed type that would close a cycle is
  refused, by the statement that writes it and again at commit.
- **`PROPAGATE`**: when a row enters a state, the rows that reference it (`ON STATE s PROPAGATE
  SET t` on the reference), or the rows at the other end of its edges (`PROPAGATE ON EDGE`), are
  moved to another state in the same statement, along their own state machines. A table that
  declares `PROPAGATE ON EDGE` also needs `id UUID`. `MAX DEPTH n` bounds how many steps a change is
  followed from the row the statement changed (edges for `PROPAGATE ON EDGE`, references for `ON
  STATE`), 10 when not written; `ABORT ON FAILURE` refuses the whole statement when a row cannot move, where otherwise that row
  stays.
- **`PROPAGATE ON STATE s EXCLUDE VECTOR`** keeps the vector of a row in state `s` out of every
  search and every count of `SHOW VECTOR_*`. It is refused on a table with no state machine, and
  naming a state the machine does not have.

Run in the shell, the rules answer as they are broken (`chunks` row 12 names a document that is
not there; `docs` row 1 is moved to `stale`, and its chunks follow):

```text
contextdb> INSERT INTO docs (id, title) VALUES (1, 'plan');
ok (rows_affected=1)
contextdb> INSERT INTO chunks (id, doc_id) VALUES (10, 1), (11, 1);
ok (rows_affected=2)
contextdb> INSERT INTO chunks (id, doc_id) VALUES (12, 99);
Error: foreign key violation: chunks (doc_id) = (99) has no row in docs
contextdb> UPDATE docs SET status = 'stale' WHERE id = 1;
ok (rows_affected=1)
contextdb> SELECT id, status FROM chunks ORDER BY id;
+----+--------+
| id | status |
+----+--------+
| 10 | stale  |
| 11 | stale  |
+----+--------+
(2 rows)
contextdb> UPDATE docs SET status = 'gone' WHERE id = 1;
ok (rows_affected=1)
contextdb> UPDATE docs SET status = 'live' WHERE id = 1;
Error: invalid state transition of docs.status: gone -> live
contextdb> DELETE FROM docs WHERE id = 1;
Error: foreign key violation: docs is still referenced by chunks (doc_id) = (1)
```

**If you know Postgres.** A reference is checked when the transaction commits, not when the
statement ends, as if every one were `DEFERRABLE INITIALLY DEFERRED`; in the shell, where a
statement is its own transaction, the two come to the same. A primary key is judged when the
statement ends, as the SQL standard says: an `UPDATE` that swaps two keys or shifts a run of
them succeeds, where Postgres decides by the order it visits the rows unless the key is
declared `DEFERRABLE`; two rows left under one key are still refused, by that key. An index
name that begins `__pk_`,
`__unique_`, `__fk_`, `__graph_` or `__retain_` is the engine's, and is refused for an index of yours
(`ReservedIndexName`).

## Writing rows

`INSERT`, `UPDATE` and `DELETE` check, before they change anything: the type of each value, `NOT
NULL`, the key, every `UNIQUE` rule, and the rules of the table above. A statement that breaks one
is refused, names the table, the column or rule and the value, and changes nothing. In a transaction it
leaves the transaction usable.

For the following refusals, first declare and seed this example table:

```sql
CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, born INTEGER);
INSERT INTO people VALUES (1, 'ada', 1980);
```

<!-- enforced by: every_script_gives_its_expected_output -->

```text
contextdb> INSERT INTO people (id, name) VALUES (2, 'ada');
Error: unique constraint violation: people (name) = (ada)
contextdb> INSERT INTO people (id, name, born) VALUES (3, 'bob', 'x');
Error: column type mismatch: people.born expected INTEGER, got Text
contextdb> INSERT INTO people (id) VALUES (4);
Error: column people.name is NOT NULL
contextdb> INSERT INTO people (id, name, name) VALUES (5, 'c', 'd');
Error: plan error: name is named twice as a column people is written in
```

`INSERT ... ON CONFLICT (columns) DO UPDATE SET ...` takes the columns of the key or of a
`UNIQUE` rule, and updates the row that is in the way; it is subject to every rule an `UPDATE` is
(`IMMUTABLE` and a state machine included). `DO NOTHING`, `EXCLUDED` and `RETURNING` are not
there.

A refusal that depends only on the declaration (the table is not there, the table is
`IMMUTABLE`) comes before one that depends on the rows. Each refusal is its own kind of
`contextdb::Error`, listed on the type.

**If you know Postgres.** A table with no primary key keeps its rows under the hidden key, so
the same row written twice is two rows, and a refusal about a row cannot name a key.

## Transactions

`db.execute` is a transaction of one statement: when it returns, the change is on disk.
`db.transaction()` begins a longer one. Its statements see what it has written and nobody else
does until `commit` returns; dropping it, or `rollback`, discards it. A statement of it is
refused at once for a type, `NOT NULL`, a key too long, a taken key or `UNIQUE` value,
`IMMUTABLE`, the state machine, a cycle and a partition limit, and the transaction goes on; a
reference, a guarded row and a clash with another commit are refused by `commit`, which
writes nothing and ends the transaction.

A refused growth writes nothing and leaves the store usable; a failed write or sync makes the
handle refuse every call, and after closing and reopening, the store shows the state before
or after that commit.

Two open transactions that take the same key, the same `UNIQUE` value, or change the same row do
not wait for each other: the first to commit wins, and the other's `commit` is refused. The
refusal is the kind the clash is: `UniqueViolation` for a key or a `UNIQUE` value,
`ConditionalUpdateConflict` for a row changed meanwhile. This program runs the clashes:

```rust,no_run
use contextdb::{params, Database, Value};

fn main() -> contextdb::Result<()> {
    let db = Database::open("./accounts")?;
    db.execute(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT NOT NULL UNIQUE)",
        &params! {},
    )?;

    // Two transactions begin together and both take key 1.
    let first = db.transaction()?;
    let second = db.transaction()?;
    first.execute("INSERT INTO accounts VALUES (1, 'ana')", &params! {})?;
    second.execute("INSERT INTO accounts VALUES (1, 'bo')", &params! {})?;
    first.commit()?;
    match second.commit() {
        Ok(_) => println!("second committed"),
        Err(error) => println!("second: {error}"),
    }

    // One that takes a key and gives it up again, while another handle takes it.
    let tx = db.transaction()?;
    tx.execute("INSERT INTO accounts VALUES (2, 'cy')", &params! {})?;
    tx.execute("DELETE FROM accounts WHERE id = 2", &params! {})?;
    db.execute("INSERT INTO accounts VALUES (2, 'cy')", &params! {})?;
    match tx.commit() {
        Ok(_) => println!("third committed"),
        Err(error) => println!("third: {error}"),
    }

    // A transaction that declares a table writes no rows.
    let tx = db.transaction()?;
    tx.execute("CREATE TABLE extra (id INTEGER PRIMARY KEY)", &params! {})?;
    match tx.execute("INSERT INTO accounts VALUES (3, 'di')", &params! {}) {
        Ok(_) => println!("wrote"),
        Err(error) => println!("declaring: {error}"),
    }
    tx.rollback();

    // A guard: this commit stands only while account 1 is still owned by ana.
    db.execute("CREATE TABLE stamped (id INTEGER PRIMARY KEY, at TXID)", &params! {})?;
    let tx = db.transaction()?;
    let held = tx.guard_row_conditions(
        "accounts",
        "id",
        &Value::Int64(1),
        &[("owner".to_owned(), Value::Text("ana".to_owned()))],
    )?;
    // A TXID column takes the transaction's own number.
    tx.execute(
        "INSERT INTO stamped VALUES (1, $at)",
        &params! { "at" => Value::TxId(tx.id()) },
    )?;
    println!("guard holds now: {held}");
    db.execute("UPDATE accounts SET owner = 'ana2' WHERE id = 1", &params! {})?;
    match tx.commit() {
        Ok(_) => println!("fourth committed"),
        Err(error) => println!("fourth: {error}"),
    }

    let found = db.execute("SELECT id, owner FROM accounts ORDER BY id", &params! {})?;
    println!("{} rows", found.rows().len());
    db.close()
}
```

It prints (`./accounts` must not exist):

```text
second: unique constraint violation: accounts (id) = (1)
third: conditional update conflict: 1 staged update(s) no longer matched at commit
declaring: a transaction either declares tables or writes rows: commit this one and run the statement in the next
guard holds now: true
fourth: conditional update conflict: 1 staged update(s) no longer matched at commit
2 rows
```

- A transaction either declares (`CREATE`, `ALTER`, `DROP`) or writes rows, not both: the
  statement that would mix them is refused (`MixedSchemaAndRowWrite`), in either order, and the
  transaction goes on. Its declarations commit together and take hold when it commits.
- `Transaction::id()` is the transaction's number, taken when it begins and above every commit
  there is. A `TXID` column holds such a number, bound as `Value::TxId`; SQL has no `TXID`
  literal, and a number above the transaction's own is refused (`TxIdOutOfRange`). The commit
  carries the transaction's number, or the next after the newest commit when another commit
  overtook it.
- `Transaction::guard_row_conditions(table, key_column, &key, &[(column, value)])` answers
  whether the row `key_column = key` is visible to the handle and holds every value now, and if
  so makes the commit stand only while it still does. A guard writes nothing.
- `SET MEMORY_LIMIT` is not part of a transaction: run inside one, it takes effect and the store
  remembers it at once, and a rollback does not undo it.

**If you know Postgres.** The second of two transactions that clash does not wait for a lock; it
is refused when it commits, and then it has lost everything it wrote, also when it inserted a key
and deleted it again while another commit took the key. A transaction that declares a table
cannot also write rows.

## Event declarations and transactional triggers

Event types, sinks, routes and schedules are persisted declarations.
`Database::declared_events()` lists triggers, event types, sinks and routes after reopen;
it does not return schedules in this version. Routes queue durable actions with the row
transaction; maintenance delivers them after commit through the handler registered with
`register_sink(name, principal, deliver)`. An action without a handler stays pending. Callback
and webhook sinks both use the registered host handler; the host handles HTTP for a webhook URL.
The maintenance report counts confirmed deliveries. The shell registers no host callbacks.
`DROP TRIGGER`, `DROP ROUTE` and `DROP SCHEDULE` accept `IF EXISTS`;
`DROP TRIGGER ... ON table` checks that the trigger belongs to that table. A schedule declared
again under the same name replaces its declaration and resets its first due time.
<!-- enforced by: declared_events_and_a_create_tables_table_are_read_from_the_store, every_script_gives_its_expected_output -->

Declare `CREATE TRIGGER name ON table WHEN INSERT` (or `UPDATE`) and register its host
handler with `register_transactional_trigger_callback(name, callback)`. The callback receives
`(&Database, &TriggerContext)` and writes through that handle inside the firing transaction,
with its reader's permissions. Callbacks run before final constraint validation, may supply
referenced parents, and cascade through `trigger_cascade_depth_cap()` (16); an error, panic,
missing handler or deeper cascade refuses the complete commit. Persisted triggers need their
callbacks registered again after reopen; `TriggerCallbackMissing` otherwise aborts a firing
commit. Callback failure kinds are `TriggerCascadeDepthExceeded`, `TriggerCallbackFailed`
for a panic, or the error the callback returned. The handle permits SQL only,
on the callback thread during dispatch; `SET MEMORY_LIMIT` and control calls are refused.
`complete_initialization()` checks every declared trigger has a callback; `declared_events()?.triggers`
and `registered_trigger_callbacks()` inspect declarations and registrations. These callbacks
prepare database rows; external notifications must wait for durable commit.
<!-- enforced by: trigger_writes_and_the_firing_rows_commit_or_roll_back_together, callback_writes_fire_triggers_through_the_cap_and_refuse_the_whole_commit_beyond_it -->

For `CREATE SCHEDULE timer EVERY '10 seconds' TX (advance)`, register
`register_cron_callback("advance", callback)`, where the callback is
`Fn(&Database) -> Result<()> + Send + Sync + 'static`. It writes through the supplied
transaction-bound handle. Declaring schedules and registering callbacks require an unnarrowed
handle; any handle may drive their maintenance cycle. Each successful callback's rows, pending
route actions and next due time commit together. Failed or missing callbacks discard their
writes, consume the tick and return an error through the maintenance cycle. Due-time progress
survives reopen and does not invalidate an unrelated open row transaction.

The default `skip-and-audit` policy runs once for the ticks due. `catch-up` runs each tick in its
`WITHIN n SECONDS` window; an omitted window is zero seconds. `fail-loud` consumes multiple due
ticks and returns `MissedTicksExceeded`. Register callbacks again after reopening the store.
<!-- enforced by: a_schedule_runs_its_callback_on_any_handle -->

A host installs `Arc<dyn DatabasePlugin>` in `DatabaseOpenOptions::plugin` (`Some(plugin)`);
`None` uses `CorePlugin`, whose hooks are no-ops. `pre_commit(&ChangeSet, CommitSource)` sees the
same change set the trigger callbacks prepared and may refuse it before final validation.
`ChangeSet::is_empty()` inspects that set; its storage mutators stay private. `commit_failed`
receives the original refusal. `post_commit` runs after the disk confirms and the commit lock
is released; a notification panic cannot turn durable success into a refusal. Hooks cannot
reenter the same database: control calls return `CallbackActive`. `on_open` and `on_close`
follow the shared open's lifetime, so closing one scoped handle does not close its host plug-in;
implicit final close ignores the close hook's result. `on_query`, `post_query` and `on_ddl`
observe submitted statements; the declaration hook takes their SQL text. `plugin_health()`
and `plugin_describe()` return the host's health and description and panic on a closed handle.

`db.subscribe()?` returns `std::sync::mpsc::Receiver<CommitEvent>` with capacity 1,024;
`db.subscribe_with_capacity(n)?` chooses its capacity (zero means one). Each event has `source`
(`CommitSource::User` for a transaction or `AutoCommit` for an implicit commit), `lsn` (the
confirmed lean `TxId`), sorted `tables_changed` and `row_count`. Delivery is best-effort: a full
channel or exhausted notification budget drops the notification and never blocks the commit.
Receivers should use it to wake and re-query, rather than as a durable event log. A queued
event retains its memory charge until dropped; disconnected channels are removed on publication.

`event_bus_status()?` lists declared event types, routes and sinks, whether each sink has a
registered handler, and its counters. `sink_metrics(name)?` returns that sink's confirmed
outcome counters for this open and its committed pending depth; pending actions remain visible
after reopen. The two reports and the two subscription calls require an unnarrowed handle:
otherwise they return `AdminRequired` naming the call, before reading store-wide activity.
<!-- enforced by: a_plug_in_refusal_leaves_nothing_and_subscribers_hear_only_after_disk_confirms, a_sink_is_delivered_to_once_for_each_event, a_narrowed_reader_is_told_the_same_whatever_is_done_to_rows_it_may_not_see -->

## Who may see and write a row

A handle made with `Database::scoped_with_constraints(contexts, scope_labels, principal)` (or
`scoped_with_contexts(contexts)`) shares the open store and sees, and may write, only the rows
those three allow; `None` declares nothing for that rule; labels are built with
`ScopeLabel::new("a")`. A handle with no narrowing (`Database::open`, or
`scoped_with_constraints(None, None, None)`) sees every row and is the only one that may run
`CREATE`, `ALTER`, `DROP`, `SET MEMORY_LIMIT` and the two `SHOW VECTOR_*` statements;
`set_memory_limit` is refused as `SET MEMORY_LIMIT` is. A narrowed handle may drive
`run_maintenance_cycle`: it maintains the whole store and withholds every report count as `None`. A handle made from a
narrowed handle keeps the Contexts and labels both name and the principal the first had, and a
transaction begun on a narrowed handle is narrowed the same way.

A table opts in by declaring a column:

- `CONTEXT_ID` on a `UUID` column: a row belongs to the Context it holds, and a handle narrowed
  to Contexts reads and writes only rows of those.
- `SCOPE_LABEL ('a', 'b')` on a `TEXT` column: a row written must carry one of the listed labels
  and a label the handle holds; it hides no row. `SCOPE_LABEL_READ ('a') WRITE ('a', 'b')` hides
  rows as well: a handle that holds labels reads the rows whose label is on the read list and
  one it holds, and a row with no label is hidden from it; it writes under the write list.
- `ACL REFERENCES grants(acl_id)` on a `UUID` column: a row is shown to a principal that holds a
  grant for its `acl_id`. A grant is a row of the table `grants`, with `principal_kind` (`Agent`
  or `Human`), `principal_id` and `acl_id`; the grant row has to be visible to the handle too. A
  narrowed handle that names no principal, or `Principal::System`, is refused such a table
  (`PrincipalRequired`).

What a narrowed handle gets:

- A list read (a scan, a range, a count) leaves hidden rows out, and says nothing of them.
- A `SELECT` that names one hidden row whole, by its primary key or a `UNIQUE` rule (`=` a
  value, a `$name` or a `(SELECT ...)`; not `IN` or `OR`), is refused
  by the rule that hid it: `ContextScopeViolation`, `ScopeLabelViolation`, `AclDenied`, or
  `PrincipalRequired` for a table that wants a principal and got none. That tells "not yours"
  from "not there", which is an empty answer; the sentence never carries a value or a number of
  the hidden row. A `GRAPH_TABLE` whose filter names one start node by `id` with `=` (a value,
  a `$name` or a `(SELECT ...)`) is refused likewise when the node is hidden; a list of starts
  (`IN`, `OR`) leaves the hidden node out and answers the rest, and NULL names no start.
- An `UPDATE` or `DELETE` changes no row the handle cannot see, and says it changed none; an
  `INSERT` or `UPDATE` that would put a row outside the handle's Contexts, labels or grants is
  refused by the same kinds, and so is a statement whose `PROPAGATE` would move a row the handle
  may not write.
- The statements named above as the unnarrowed handle's reach every Context at once and are
  refused with `AdminRequired`, naming the statement.
  The answer, the refusals and `explain` of a narrowed handle do not depend on the counts,
  vectors or index state of data it may not see, so the state of the index is neither printed
  nor used to choose an answer on a shared partition. `INDEXED` is served only where the
  declaration and narrowing prove that the selected partitions are wholly visible to the
  handle: its Context column must lead the partition key, and no label or access rule may hide
  rows in them. Otherwise `VectorIndexedNarrowed` refuses the request; use `AUTO` or `EXACT`.
  Its `explain` of a search prints the declaration lines (`requested_mode`
  as declared), `filter_path` and `recovery`, and no `resolved_mode`, `key`, `route`, partition,
  `small_set` or `refusal` line. The work counters a result carries (`rows_examined`,
  `distance_evaluations`) count only rows the handle may see that the statement read, including reads for writes.
  A narrowed search counts only comparisons of visible stored rows. It receives no
  store-wide work totals or maintenance counts, and `QueryResult::peak_bytes()` is `None`,
  including for `SHOW MEMORY_LIMIT`; that statement still returns its own handle limit.
  <!-- enforced by: a_narrowed_reader_is_told_the_same_whatever_is_done_to_rows_it_may_not_see -->

```rust,no_run
use std::collections::BTreeSet;

use contextdb::{params, ContextId, Database, Principal, Uuid};

fn main() -> contextdb::Result<()> {
    let db = Database::open("./reports")?;
    db.execute(
        "CREATE TABLE grants (id INTEGER PRIMARY KEY, principal_kind TEXT, principal_id TEXT,
                              acl_id UUID)",
        &params! {},
    )?;
    db.execute(
        "CREATE TABLE reports (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID,
                               acl_id UUID ACL REFERENCES grants(acl_id), body TEXT)",
        &params! {},
    )?;
    let (mine, theirs, key) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    db.execute(
        "INSERT INTO grants VALUES (1, 'Agent', 'reader', $key)",
        &params! { "key" => key },
    )?;
    for (id, context) in [(1_i64, mine), (2, theirs)] {
        db.execute(
            "INSERT INTO reports VALUES ($id, $context, $key, 'text')",
            &params! { "id" => id, "context" => context, "key" => key },
        )?;
    }

    let contexts = BTreeSet::from([ContextId::new(mine)]);
    let agent = db.scoped_with_constraints(
        Some(contexts),
        None,
        Some(Principal::Agent("reader".to_owned())),
    );
    let list = agent.execute("SELECT id FROM reports", &params! {})?;
    println!("a list read: {} row", list.rows().len());
    for sql in ["SELECT body FROM reports WHERE id = 2", "DELETE FROM reports WHERE id = 2"] {
        match agent.execute(sql, &params! {}) {
            Ok(result) => println!("{} rows changed", result.rows_affected()),
            Err(error) => println!("{error}"),
        }
    }
    if let Err(error) = agent.execute(
        "INSERT INTO reports VALUES (3, $other, $key, 'text')",
        &params! { "other" => theirs, "key" => key },
    ) {
        println!("{error}");
    }
    if let Err(error) = agent.execute("DROP TABLE reports", &params! {}) {
        println!("{error}");
    }
    let nobody = db.scoped_with_contexts(BTreeSet::from([ContextId::new(mine)]));
    if let Err(error) = nobody.execute("SELECT id FROM reports", &params! {}) {
        println!("{error}");
    }
    db.close()
}
```

It prints (`./reports` must not exist):

```text
a list read: 1 row
context scope violation: this row of reports is outside the Contexts this handle was given
0 rows changed
context scope violation: this row of reports is outside the Contexts this handle was given
DROP TABLE reaches every Context, label and principal: run it through the handle that declared no narrowing
table reports is access-controlled: read it through a handle that names a principal
```

**If you know Postgres.** This is not row-level security, which leaves a hidden row out of every
read, a `SELECT` by key included.

## Changing a table

```text
contextdb> ALTER TABLE docs ADD COLUMN owner TEXT DEFAULT 'nobody';
ok (rows_affected=0)
contextdb> SELECT id, owner FROM docs;
+----+--------+
| id | owner  |
+----+--------+
| 1  | nobody |
+----+--------+
(1 row)
contextdb> ALTER TABLE docs RENAME COLUMN title TO heading;
ok (rows_affected=0)
contextdb> ALTER TABLE docs RENAME COLUMN id TO ident;
Error: schema invalid: column 'docs.id' is in the PRIMARY KEY, which stays as the table declared it, so it is neither dropped nor renamed
contextdb> CREATE INDEX docs_by_heading ON docs (heading);
ok (rows_affected=0)
contextdb> ALTER TABLE docs DROP COLUMN heading;
Error: column docs.heading is used by the index docs_by_heading; drop the index first, or write CASCADE
contextdb> ALTER TABLE docs DROP COLUMN heading CASCADE;
+-----------------+
| dropped_indexes |
+-----------------+
| docs_by_heading |
+-----------------+
(1 row)
contextdb> DROP TABLE docs;
Error: schema invalid: table 'docs' is referenced by table 'chunks', so it was not dropped; drop that table first
```

- `ADD COLUMN` writes no row again: a row written before it reads the column's `DEFAULT` (NULL
  without one). A `NOT NULL` column added to a table that has rows needs a `DEFAULT`.
- `DROP COLUMN` is `RESTRICT` unless `CASCADE` is written: a column an index names is refused,
  and `CASCADE` drops those indexes and reports them. The column's bytes leave the store at the
  commit.
- A column is neither dropped nor renamed while the table is declared by it: a column of the
  primary key, an `IMMUTABLE` column, a vector column's `PARTITION_KEY`, the three columns of a
  table of edges that has a `DAG`, or a column a rank policy reads. `CASCADE` does not lift that.
- `RENAME COLUMN` carries the column's indexes and its vectors with it.
- `SET RETAIN`, `SET HISTORY`, `SET SYNC`, `SET SYNC CONFLICT` and `SET MAINTAIN COUNT` replace
  that clause; `DROP RETAIN` takes `RETAIN` off. No statement removes a `HISTORY` clause: `SET
  HISTORY ALL` is the way back. A new set of counts is installed whole or not at all.
- `ALTER COLUMN v SET MAX_PARTITIONS m`, `SET SEARCH_MODE mode`, `SET INDEXING_THRESHOLD a` and
  `SET COMPACTION ...` change a `VECTOR` column; `DEFAULT` after `INDEXING_THRESHOLD` or
  `COMPACTION` puts back the default.
- `DROP TABLE` refuses a table another table references, and `DROP INDEX name ON table` names
  the table, because two tables may use one index name. `IF EXISTS` makes a missing table or
  index a no-op.

**If you know Postgres.** A key column can be neither renamed nor dropped, and `DROP COLUMN`
refuses where Postgres drops an index silently.

## Reading a table's rules back

`Database::declaration(table)` returns the declaration as text, the same text `.schema` prints, and
declaring that text again declares the same table. `Database::table_meta(table)` returns it as a
value: `columns` (each with its name, type, `nullable`, `primary_key`, `unique` and default), `unique_constraints`,
`indexes`, `immutable`, `state_machine`, `dag`, `retain`, `history`, `sync`, `conflict` and
`kept_counts`, `edge_discard`, `propagation_rules`, `vectors`, `scope_label`,
`propagate_edges` and `exclude_vector_states`. The type is `#[non_exhaustive]`. Each vector
option gives the column, partition key, partition cap, mode and quantization; indexing thresholds
and compaction clauses, plain references, rank policies, delivery manifests, Context and ACL
rules remain in the text `declaration` returns. `scope_label` is a column name and
`ScopeLabels { read, write }`: when `write` is `None`, the list stored in `read` is the
single-list, write-only rule and hides no rows. With `write: Some(...)`, `read` is the read
allowlist and `write` is the write allowlist. Metadata and audit reads use the reader budget
and its bounds, as statement reads do.
<!-- enforced by: table_meta_gives_each_tables_rules -->

```rust,no_run
use contextdb::{params, Database};

fn main() -> contextdb::Result<()> {
    let db = Database::open("./docs")?;
    db.execute(
        "CREATE TABLE docs (a INTEGER, b TEXT, PRIMARY KEY (a, b), note TEXT)
         RETAIN 30 DAYS SYNC OFF",
        &params! {},
    )?;
    let meta = db.table_meta("docs")?;
    for column in &meta.columns {
        println!("{}: nullable {}", column.name, column.nullable);
    }
    println!("retain: {:?}", meta.retain);
    println!("sync: {:?}", meta.sync);
    println!("state machine: {:?}", meta.state_machine);
    println!("{}", db.declaration("docs")?.as_str());
    db.close()
}
```

It prints (`./docs` must not exist):

```text
a: nullable false
b: nullable false
note: nullable true
retain: Some(Retain { count: 30, unit: Days, sync_safe: false })
sync: Some(Off)
state machine: None
CREATE TABLE docs (
  a INTEGER,
  b TEXT,
  note TEXT,
  PRIMARY KEY (a, b)
) RETAIN 30 DAYS SYNC OFF;
```

`table_meta` reports every column of the primary key as not nullable, however the key was
written.

## Vector search

A `VECTOR(n)` column is searched by ordering its rows by their distance from a vector and
taking the first `k`:

```sql
CREATE TABLE notes (id INTEGER PRIMARY KEY, embedding VECTOR(3));
INSERT INTO notes (id, embedding) VALUES (1, [1.0, 0.0, 0.0]);
INSERT INTO notes (id, embedding) VALUES (2, [0.0, 1.0, 0.0]);
SELECT id, embedding <=> [1.0, 0.0, 0.0] AS distance
FROM notes ORDER BY distance LIMIT 10;
-- id  distance
-- 1  0.0
-- 2  1.0
```

- `embedding <=> $q` is the cosine distance between the row's vector and `$q`, a `REAL`. Lower
  is closer: 0 for vectors that point the same way, 1 for vectors at a right angle, 2 for
  opposite ones, as Postgres with pgvector reads the operator. Rows come nearest first, and
  rows at the same distance in primary-key order.
- A search returns the columns the statement wrote and no other. To get a row's distance, write
  the expression in the select list, alone or inside another expression, and name it as you
  like: `1 - (embedding <=> $q) AS similarity` is a similarity. `ORDER BY` takes the expression
  itself or the name the select list gives it.
- The vector on the right is a `$name` bound to a `Vec<f32>`, a literal `[1.0, 0.0, 0.0]`, or
  the same as quoted text, `'[1, 0, 0]'`. It has the column's dimension and finite numbers.
- A number, in a vector or anywhere else, is read as Postgres reads one: `42`, `3.5`, `4.`,
  `.001`, `5e2` and `1.925e-3` are all numbers, so a vector the shell prints with an exponent
  pastes back as it is. Hexadecimal, underscores and a leading `+` are not read.
- `WHERE` is applied first: the answer is the `k` nearest of the rows that pass it. A row whose
  vector is NULL is in no answer. The distance to a vector of all zeros is 1.
- How much of the column a search compares with `$q` is the search's mode: `EXACT` compares
  every stored vector and returns the true nearest rows; `INDEXED` chooses among them through
  the column's index (a narrowed handle needs wholly visible partitions, as described above); `AUTO`, the default, is `EXACT` until a column is large and then uses the
  index (the paragraph on modes below).

Only `<=>`, cosine, is here: pgvector's `<->` and `<#>` are a parse error. A distance is a value
of the row, so `<=>` stands wherever an expression may in a `SELECT`: in `WHERE`, in the select
list, farthest first (`DESC`), as one of several sort keys, between two columns, and with no
`LIMIT`, which orders every row that has a vector. A search takes `OFFSET` after its `LIMIT`. It is
refused in an `UPDATE`'s value and in a `DELETE`'s filter, and on a table that
declares `PROPAGATE ON STATE ... EXCLUDE VECTOR` it is answered only as
`ORDER BY column <=> vector LIMIT n`. `USE VECTOR` on an `ORDER BY` that is not a distance is refused (`UseVectorRequiresVectorOrder`).

Typed search refusals (`UnknownVectorIndex`, `VectorIndexDimensionMismatch`,
`InvalidVectorValue`) apply to a single-table, nearest-first distance search with `LIMIT`,
without joins, unions, grouping or `DISTINCT`, whose query vector reads no row column.
Other distance expressions use the ordinary expression evaluator: a non-vector operand gives
`PlanError`; a dimension mismatch gives `PlanError` when a non-NULL row is evaluated, so an
empty result need not detect it. That expression path does not reject non-finite query
components and can yield NaN. Validate query components and dimensions in the host before
using those expression forms.

`ORDER BY column <=> vector USE RANK name LIMIT n` orders the rows by the formula of the
column's `RANK_POLICY` instead of by distance, highest rank first, before `LIMIT` and `OFFSET`
are applied. The policy gives each row the row of the table named after `JOIN` that is found
through the column named after `ON` (the joined table needs an index that leads with it): of
the rows found that the reader may see, the one of lowest key; with none, its columns are NULL.
`FORMULA` is numbers and `{column}` names of either table, and `{vector_score}`, which is one
minus the distance, joined by `+` and `*` or written as `coalesce(<formula>, <number>)`. A row
with no rank comes last, and rows of one rank in key order. No rank or score column is added to
the answer. The policy is named by its `SORT_KEY`: a column `v VECTOR(2) RANK_POLICY (JOIN
results ON task_id, FORMULA '{confidence} + {vector_score} * coalesce({success}, 1.0)', SORT_KEY
effective_confidence)` is searched by `ORDER BY v <=> [1.0, 0.0] USE RANK effective_confidence
LIMIT 6`.

**Maintenance.** Beside every stored vector the engine keeps a one-byte copy of each of its numbers
and packs the copies of many vectors into a block, which a search screens quickly to rule most
rows out without reading them. A vector that was just written is on the unindexed list, the
vectors not yet in a block, which a search compares one by one by reading their rows. Once a
column is large enough (the paragraph on modes below) the engine also groups a partition's vectors:
it finds the centroids of groups of near vectors, files each vector under its nearest centroid, and
keeps a one-bit binary code of every vector, an eighth of the size of its one-byte copy. An IVF
is made once and replaced whole; a vector written after it waits in blocks of its own, each
with its binary codes, until the partition is grouped again.

`Database::run_maintenance_cycle` does this work in bounded steps; the engine worker runs it
automatically unless the open chooses `Maintenance::Caller`. One call takes one step for each
partition with an IVF due or being built, expires due retained rows, then seals at most
4,096 unindexed vectors, each
step in a commit of its own: begin, train centroids when needed, file the
vectors under them, pack the groups into blocks, measure (twice), and put the new IVF in
force. A partition is grouped again when the vectors written, removed or changed since reach
`CHANGE_PERCENT` of its IVF (20 when left out) or the removed or changed ones reach
`TOMBSTONE_PERCENT` (10); `COMPACTION NONE` never groups it again. A regroup reuses its
centres and group count until the partition doubles or its changed slots reach a turnover;
then it trains again. Each build renews the partition mean and each centre from its packed
members. `UNINDEXED_VECTOR_LIMIT` is a persisted, per-partition ceiling on vectors
not yet in blocks, inspected with `SHOW UNINDEXED_VECTOR_LIMIT` and changed with
`SET UNINDEXED_VECTOR_LIMIT`. Its default is one block of the column. A commit reaching the
ceiling seals whole blocks of the partitions it touched in that commit; index building stays
with maintenance.
<!-- enforced by: a_partition_is_grouped_again_only_when_its_changes_reach_the_declared_share, every_build_renews_the_mean_and_each_centre_from_its_members -->

The call returns a `MaintenanceReport`. On an unnarrowed handle, call again while
`work_remaining` is `Some(true)`; `Some(false)` means there is no further runnable work,
including compaction. Yield while `compaction_waiting_on` names a reader; the worker waits
for its next poll. `None` means withheld, never completed. Memory-stalled builds wait for room.
Its `vector` field describes
vector work:
`maintenance_vectors_done` and `maintenance_vectors_remaining` (vectors), `remaining_partitions`
(partitions with an IVF due or being built) and `stalled_partitions` (those whose last step
the memory limit refused; a further cycle does nothing for them until the limit is raised, and
with no limit declared none stall). All four fields are `Option<u64>`: `Some(0)` means zero
and `None` means withheld, never completed. Use an unnarrowed handle for a completion loop,
as the program below does; a narrowed caller must not loop on withheld fields.
The cycle also expires due retained rows; `expired_rows` counts removals and `deferred` counts
protected rows. `delivered` counts delivered events and `compacted_pages` counts pages moved.
A narrowed handle may drive the cycle but receives `None` in every report field.
Expiry handles newly due ranges and at most 8,192 older backlog rows per cycle, in
commits of up to 1,024 keys. Inspect retained tables for protected rows counted by `deferred`;
a deferred row can remain until its references allow deletion.
<!-- enforced by: each_retained_row_leaves_at_the_second_its_judgement_gives, a_narrowed_reader_is_told_the_same_whatever_is_done_to_rows_it_may_not_see -->

A step is worked out with no lock held, so a write waits only for a step's commit and a reader
not at all, and a search made between two cycles finds every vector: it sees the IVF in
force when the statement began, whole, or the one before it, whole, never a mix. An exact
search returns the same rows with the same distances whether or not vectors are sealed; sealed,
it reads far fewer rows, and the result's `QueryResult::rows_examined` says how many it read.

**Partitions.** `PARTITION_KEY (column, ...)` on a `VECTOR` column lays its vectors out by the
values of the named columns, one partition for each combination that has a row with a vector,
so that a search which filters on the key reads only that part. A partition is not a wall: a
search with no filter on the key reads every partition, and which rows a caller may see is the
`WHERE`'s business. A search whose `WHERE` gives the key columns values with `=` or `IN` reads
only the partitions named, up to 4,096 of them (a filter that names more reads every
partition); the whole condition is still applied to every row, so partitions change the work
and never the answer. A key column is `NOT NULL` and a `UUID`, `TEXT`,
`INTEGER`, `BOOLEAN` or `TIMESTAMP`.

`MAX_PARTITIONS m`, written after the key when the table is declared, is the most partitions
the column may have at one time, 256 when it is not written. An `INSERT` or `UPDATE` that would
take the column past it is refused with `Error::VectorPartitionLimitExceeded`, which names the
column and its limit; the statement changes nothing, and inside a transaction the transaction
goes on. A column with no `PARTITION_KEY` is one partition and has no limit.

**Modes.** `SEARCH_MODE` on the column says how its searches run when a statement does not, and
`USE VECTOR`, written after the `ORDER BY` item, says it for one statement. The mode in force is
the statement's, else the column's, else `AUTO`.

- `EXACT` compares the search vector with every stored vector of every partition the statement
  selects: the IVF's blocks, the blocks written since and the unindexed list. It returns the
  true nearest rows, and reads no centroid and no binary code.
- `INDEXED` searches the unindexed vectors of each selected partition and, of the rest, only
  the vectors the index chooses (the paragraph on the index below). A partition with no IVF,
  or with an IVF of one group, has nothing to choose among, and is searched as `EXACT`
  searches it. For the unnarrowed handle `INDEXED` is refused, before anything is read, where
  no index can answer: when a
  selected partition's writes are far ahead of maintenance (for a partition with no IVF or an
  IVF of one group, the vectors outside its IVF number at least twice
  `INDEXING_THRESHOLD`; for one with more groups, at least twice what makes it due to be grouped
  again, which under `COMPACTION NONE` is never), whatever the filter is, even a
  key; and when a filter on rows leaves it nothing to choose among, as the paragraph on filters
  below says.
- `AUTO`, which is the default, adds up the vectors of the partitions the statement selects.
  Below `INDEXING_THRESHOLD` it is `EXACT`. From there on each selected partition that has an
  IVF and is not far behind is searched as `INDEXED` searches it, and every other
  partition as `EXACT` does. `AUTO` is never refused for an IVF.

`INDEXING_THRESHOLD` is 100,000 when not written. Until a column holds that many vectors in all its
partitions together (or declares `SEARCH_MODE INDEXED`), no centroid, binary code or mean is
written, and `durable_index_bytes` is 0 unless removed or changed rows (10% by default) have
given a partition an IVF of one group, which takes 28 bytes. In a column that has reached it, a partition is grouped once it
holds a hundredth of it, 1,000 vectors when nothing is written, so a store of many middling
partitions is indexed too; a partition of a few hundred vectors is not cut into groups. A
declared `INDEXING_THRESHOLD` of any size is served. A column that should never be grouped declares
one it will not reach.

**The index.** An indexed search of a partition that has groups compares the search vector with
the centroids of its groups and takes the groups whose centroid is nearly as near as the nearest
centroid; then it estimates from the binary codes of those groups' vectors, and of the vectors
written since the IVF, which of them can be among the nearest; only those go on to the
one-byte copy and then to the row. So every row in an indexed answer has its true distance,
the answer is in the order of those distances, no row comes twice, and a search is never
answered short: it returns `LIMIT` rows whenever the selected partitions hold that many rows
the statement's filter passes. What a binary code can cost is a row that an exact search would have
put among the nearest.

An indexed search returns rows that an exact search would rank among its nearest, each with
its true distance. How far from the nearest group it scans and how far it trusts a binary code
are measured when the index is built, on questions made from the stored vectors: half are
stored vectors themselves, half lie between two stored vectors, and a binary code counts as kept
only if it would be kept both as a grouped row and as a row written after the build. That is
a measure over those questions, not a promise for any one question: a question unlike both
kinds, rows written after the build that are unlike the rows the build saw, and a `LIMIT`
other than ten are not what the measure was taken on. `USE VECTOR EXACT` or `SEARCH_MODE EXACT`
is exact.

A search with a filter on rows is answered one of two ways, and which is decided by counting.
When the filter reaches rows through an index or a key (`filter_path` in `EXPLAIN`, below; ranges joined by `OR` give no single range and read every row), the
engine first counts the rows that path reaches, from the index entries or the keys alone and
reading no row, and stops counting one past the bound. If there are at most as many as a 128th of
the vectors of the selected partitions that an index with several groups answers plus a 16th of
the vectors of the others, and never fewer than the rows the statement asks for (`LIMIT` plus
`OFFSET`, one more for a second sort key), those rows are read and compared with the search
vector one by one, whatever the mode: the answer is exact, and no index is searched. A row the
reader may not see still counts. A filter that reads the whole table, or no index or key, is
never counted so, and neither is one that reaches more rows than the bound. Otherwise the search
goes through the index, or exactly, and the filter is asked of each row it visits; the search
widens until it holds as many allowed rows as were asked for: a filter that passes most rows
costs what the unfiltered search costs, and one that passes few costs more, up to the work of
`EXACT`. For the unnarrowed handle `INDEXED` is refused by name only when a filter passes too few rows for the index to
choose among, because being sure of the answer would mean asking the filter of the whole table
under the index's name; `AUTO` does that work and returns every row there is. `EXPLAIN` prints
`small_set: yes` when the filter reaches few enough rows to be compared one by one, and `no`
when it reaches more; the line is left out when the filter has no index or key to read by, and
then `recovery:` names the index to create.

**The distance bound.** A `WHERE` part at its top level that compares the distance the search
orders by with a number, `v <=> $q < 0.5` or `<=` (written either way about, several taken at
their tightest), is a bound when the distance is the one sort key and the number is finite
before any row is read; otherwise it stays a condition each row is asked. The search ends once
it holds nothing nearer than the bound, and a row past it is turned away, so a question unlike
everything stored reads no row. `EXPLAIN` names the cut-off as `distance_bound: < 0.5` or
`distance_bound: <= 0.5`, with the bound's value (`1.0` prints `1`), and prints no such line for a search without one. Under AUTO
and INDEXED a search with a distance bound answers as the search does without it, at the
published recall, so a row inside the bound can be absent; USE VECTOR EXACT returns every row
inside the bound. The recall promised is 95 in 100 of the exact scan's rows inside the bound,
over a set of questions. A bound that cuts nothing changes no row: `INDEXED` is refused for it as
it is without the bound.
<!-- enforced by: a_distance_bound_cuts_the_search_and_answers_as_the_exact_scan_does, explain_names_the_distance_bound_a_search_is_cut_off_at, a_bound_finds_what_is_stored_and_nothing_for_a_face_never_stored -->

```text
notes.embedding cannot be searched INDEXED for this statement now: its index is behind its writes, or the statement's filter leaves the index nothing to choose among; run a maintenance cycle, or use EXACT or AUTO
```

`QueryResult::distance_evaluations` counts the work of a similarity search: the centroids it
compared with the search vector, the binary codes it estimated, the one-byte copies it screened and
the rows whose distance it worked out. It is an exact count, the same on every run over the
same store, and 0 for any other statement. An indexed search usually counts far fewer than an
exact one, and can count more when it leaves few groups out, because it estimates a binary code of
every vector it keeps; each of those counts an eighth of the bytes. `Database::explain`, and
`ReadSession::explain`, return what a statement would do, without running it (the `EXPLAIN`
statement under Statements gives the same lines as the rows of a `QUERY PLAN` column). This
program builds a store of 2,000 vectors, runs maintenance as above, and searches it both ways:

```rust,no_run
use contextdb::{params, Database, DatabaseOpenOptions, Maintenance};

// A number from -1 to 1 for each seed, the same on every machine.
fn spread(seed: u64) -> f32 {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 29;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 32;
    (x % 2001) as f32 / 1000.0 - 1.0
}

// Eight numbers for each id: the topic's (fifty topics) and a little of the id's own.
fn vector(id: u64) -> Vec<f32> {
    (0..8u64)
        .map(|j| spread((id % 50) * 8 + j + 1) + 0.15 * spread(id * 8 + j + 100_000))
        .collect()
}

fn main() -> contextdb::Result<()> {
    let mut options = DatabaseOpenOptions::default();
    options.maintenance = Maintenance::Caller;
    let db = Database::open_with_options("./notes-index", options)?;
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, embedding VECTOR(8) INDEXING_THRESHOLD 1000)",
        &params! {},
    )?;
    let tx = db.transaction()?;
    for id in 0..2000u64 {
        tx.execute(
            "INSERT INTO notes (id, embedding) VALUES ($id, $embedding)",
            &params! { "id" => id as i64, "embedding" => vector(id) },
        )?;
    }
    tx.commit()?;

    // Maintenance: seal the new vectors and group them, a bounded step at a time.
    let mut cycles = 0;
    loop {
        let report = db.run_maintenance_cycle()?;
        cycles += 1;
        let Some(more) = report.work_remaining else {
            return Err(contextdb::Error::AdminRequired {
                statement: "maintenance completion".into(),
            });
        };
        if !more {
            break;
        }
    }
    println!("{cycles} cycles");

    let mut ids = Vec::new();
    for mode in ["EXACT", "INDEXED"] {
        let sql = format!(
            "SELECT id FROM notes ORDER BY embedding <=> $q USE VECTOR {mode} LIMIT 10"
        );
        let found = db.execute(&sql, &params! { "q" => vector(5000) })?;
        println!("{mode}: {} distance evaluations", found.distance_evaluations());
        ids.push(found.rows().to_vec());
    }
    println!("the same ten rows: {}", ids[0] == ids[1]);
    println!("{}", db.explain("SELECT id FROM notes ORDER BY embedding <=> $q LIMIT 10")?);
    db.close()
}
```

It prints, for the first run (`./notes-index` must not exist):

```text
17 cycles
EXACT: 2019 distance evaluations
INDEXED: 225 distance evaluations
the same ten rows: true
requested_mode: AUTO
resolved_mode: AUTO
effective_indexing_threshold: 1000
aggregate_allowed_vectors: 2000
partition_key_columns: []
partitions: 1
partition: {}
  route: indexed
  lists: 45
  probes: 8
  live_rows: 2000
  unindexed_rows: 0
refusal: none
```

`lists` is the groups of the IVF in force and `probes` the most of them a search scanned
for the questions its build made, `unindexed_rows` the slots in blocks written since (stale ones
included) plus the vectors still unindexed, and `route` is `exact` or `indexed`; `refusal` is `none` or, for INDEXED over a partition that is
behind its writes, the sentence the search would be refused with (the refusal for a filter is
known only when the search runs); a narrowed handle is given no `refusal` line. A statement that names a partition with a `$name` has nothing to select
with, when `Database::explain` is given no value for it: it lists every partition of the column and says so,
`partitions: every partition ($t is not bound)`.

When the search has a filter on rows whose every name has a value, the lines before `refusal`
also say how the filter is read: `filter_path:` with the index or key it goes through, or `none`; the
`predicates_pushed` and `indexes_considered` lines of any other statement; `small_set: yes` or
`no` (the paragraph on filters above), only when the filter has a path; and, when no index
serves, `recovery:` with the `CREATE INDEX` statement that would. Of a statement that is not a
similarity search, `explain` says the path each table is read by: `IndexScan { index: name }` with the
columns it pushes into the index and why each other index was passed over, or `Scan`. A range
read from its far end, because the order asked is the reverse of the index's (or of the key's), is
`IndexScan Backward { index: name }`. A filter no row can pass is `NoScan { filter: false }`
and reads nothing. The reasons an index was passed over are `fewer predicate columns matched
than chosen index`, `lower selectivity than chosen index`, `does not supply requested order`,
`tied with chosen index; lost by creation order`, and, for an index whose first column the filter
does not fix, why: `first column not in WHERE`, `OR arms reach the table by different paths`,
`LIKE is residual-only`, `arithmetic in predicate`, `function call in predicate` or
`non-literal rhs`. A join
says it for each table; a grouped count says `maintained count: used (...)` when a count the
table keeps serves it and otherwise `maintained count: not declared` with the `ALTER TABLE ... SET
MAINTAIN COUNT` that would. An `INSERT`, `UPDATE` or `DELETE` is explained as it would run
and changes nothing. What is wrong with the statement itself (an unknown column, a
value count that does not match the columns, a conflict rule the table cannot keep) is refused
as the statement is, before anything is read; what is wrong only with the rows it would meet
(a subquery that gives several rows where one is wanted) is not.

**Quantization.** `WITH (quantization = 'SQ8')` on a column keeps each vector as one scale and
one whole number from -127 to 127 for each of its numbers, so a vector of `n` numbers takes
`n + 13` bytes in place of `4n + 9`. The cost is in the answers. The vector the row keeps is the rounded one, and it is the
one a `SELECT` returns and every search, `EXACT` included, ranks by (`[0.1, 0.2, 0.3]` is
read back as `[0.0992126, 0.2007874, 0.3]`). Rounding can change which rows are nearest.
It is an option a table chooses and never a default: a
column keeps `F32` unless it says otherwise. `SQ4` is refused by name, and the refusal says to write `'SQ8'` or leave the clause out.

**Inspection.** `SHOW VECTOR_INDEXES` returns one row for each `VECTOR` column of the store,
in the order of table name and column, and `SHOW VECTOR_PARTITIONS` one row for each partition
of the column after `FOR`, or of every `VECTOR` column, in the order of table, column and key
tuple; `LIMIT` takes the first rows and `OFFSET`, only after `LIMIT`, skips some. Both read
the vector part's small records and no block, no centroid and no row, change nothing, and work in
a writer, a transaction and a read session alike. The shell prints their rows one field to a
line, with an empty line between rows. For the store above, `contextdb ./notes-index` prints
these (the lines of `SHOW VECTOR_INDEXES` that tell the column's declaration, size and
state, with `…` for the lines left out):

```text
table: notes
column: embedding
dimension: 8
quantization: F32
vector_count: 2000
…
search_mode: AUTO
declared_indexing_threshold: 1000
effective_indexing_threshold: 1000
…
durable_vector_bytes: 32000
durable_index_bytes: 7856
indexed_available: true
maintenance_state: idle
…
recovery_action: none
```

and for `SHOW VECTOR_PARTITIONS`:

```text
table: notes
column: embedding
partition_key: {}
live_rows: 2000
pending_tuples: 0
tombstones: 0
durable_vector_bytes: 32000
durable_index_bytes: 7856
indexed_available: true
availability_reason: none
maintenance_state: idle
maintenance_reason: none
maintenance_vectors_total: 0
maintenance_vectors_done: 0
maintenance_vectors_remaining: 0
lists: 45
probes: 8
recovery_action: none
```

`.explain <sql>` in the shell prints what `Database::explain` returns, the text shown above. A
statement with two faults may be refused for either.

Inspection tells the groups a partition has (`lists`) and the most groups a search scanned
while the index was measured (`probes`); it does not print the measured distance or width. A
column with no `PARTITION_KEY` has one row in `SHOW VECTOR_PARTITIONS`, with `partition_key`
`{}`, and a partitioned column with no vector has none. A partition's `maintenance_state` is `idle`, `pending` (an IVF is due and not begun),
`building` or `stalled`, and its `maintenance_reason` says why maintenance has work: `unindexed`,
`changes`, `tombstones`, `damaged` or `memory_limit`. For `pending` and `building` the
`recovery_action` is `run a maintenance cycle`, and for `stalled` it is `raise the memory limit`.
The action is computed from state alone: `maintenance_reason = damaged` can accompany
`idle` and `recovery_action = none`. In that case drive maintenance anyway: its work list
checks the damaged IVF and queues a rebuild. Use the writing worker or an embedded
`Database::run_maintenance_cycle`; the shell has no command for it. This repairs a derived
IVF only. A `StoreCorrupted` error from another record needs the store-damage procedure
below, rather than repeated maintenance calls.

## Reading in order, paging and what a route decides

A read in an order an index gives only the first keys of is finished in runs: rows tied on the
keys the index gives are held back, put in the order of the keys it does not, and handed on, so
the whole answer is never sorted. Only an index does this; the table's own key gives an order
whole or not at all. A range the order asks for from its far end is walked from that end
(`IndexScan Backward`). A page of a cursor costs the rows it gives and at most one run of tied rows,
never the answer built at the first page, and its charge goes back when the page is dropped.
Rows an `ORDER BY` leaves tied, and the rows of a statement with no `ORDER BY`, come in the order
of the walk: entries by index key and then row key, or the exact reverse from the far end. The same
rows give the same order on every machine, but a statement that needs one order names it, and a
`LIMIT` over tied rows can return any of them. A filter no row can pass (`WHERE FALSE`, `a = 1
AND a = 2`, `a > 5 AND a < 3`, a parameter bound to FALSE) answers no rows in every kind of
statement and reads nothing. A row the handle may not see, named whole by its key or a UNIQUE
rule given by `=`, is refused by the rule that hid it, however the filter writes the value
(`id = 1 + 1`, `(a, b) = (1, 2)`, `id = 2 OR id = 2`); a list never names a row, and leaves it out.
A value of another type for an `INTEGER` key (`id = 1.5`) gives no key, and a filter of more than
4,096 cases is left unread; the statement then reads rows and asks each of them, and a row the
handle may not see gives a no-row answer, never the row.
<!-- enforced by: pages_in_an_order_with_ties_cost_their_rows_and_one_run, pages_of_one_indexed_value_read_backward_cost_their_rows, a_long_paged_read_in_an_order_with_ties_does_not_creep_toward_the_memory_limit, a_filter_no_row_can_pass_reads_nothing_by_a_parameter_as_by_its_text, a_hidden_row_named_whole_is_refused_and_a_list_read_leaves_it_out -->

## Binding values

`$name` in a statement is the entry `"name"` of the map passed with it. `params!` writes the
map: `params! { "id" => Uuid::new_v4(), "body" => "buy milk" }`, or `params! {}` for none. It
takes `bool`, `i64`, `f64`, `&str`, `String`, `Uuid`, a `serde_json::Value` and a `Vec<f32>`;
any other value is written as a `Value`, for example `Value::Timestamp(1_767_225_601_000)` or
`Value::Null`. `contextdb::Uuid` and `contextdb::serde_json` are the types the library uses, so
`Uuid::new_v4()` and JSON values need no dependency of your own.

## Refusals

Every call that can fail returns `contextdb::Result` with one `contextdb::Error`.
`Database::memory_limit`, `memory_used`, `table_names` and `committed_watermark` return
`DatabaseClosed` on a handle that was closed. Their signatures are
`memory_limit() -> Result<Option<usize>>`, `memory_used() -> Result<usize>`,
`table_names() -> Result<Vec<String>>` and `committed_watermark() -> Result<TxId>`;
use `?` before inspecting the answer. `memory_limit()?` gives `Some(resolved_ceiling)`
on an undeclared store; `None` means an unlimited ceiling, including explicit `NONE`.
Maintenance commits can advance the watermark without
a host write. `memory_used` includes plan-cache, trigger and request-buffer reservations.
Database control and inspection calls also return `CallbackActive` on the callback handle
or the firing thread during dispatch. The callback handle permits SQL through its active
transaction; it is not an ordinary database control handle.
`registered_trigger_callbacks`, `trigger_cascade_depth_cap`, `plugin_health` and
`plugin_describe` are infallible signatures
that panic on a closed handle or during callback dispatch.

`Error` is `#[non_exhaustive]`: include a wildcard arm when matching it. A program
matches on the kind (`Error::UniqueViolation { table, column, key }`, `Error::ColumnNotNullable`,
`Error::TableNotFound`, `Error::DatabaseLocked`, `Error::MemoryBudgetExceeded`, and the rest
listed on the `Error` type); a person reads the sentence it prints:

```text
unique constraint violation: notes (id) = (5b1f0a9e-3c47-4d2a-9e51-8f0c6a2d7b14)
column notes.body is NOT NULL
column not found: notes.nothing
parse error: syntax error at line 1, column 1: unexpected 'SELEC'
```

## One writer, many readers

One process at a time holds a store for writing. A second `Database::open` on the same folder
waits until the first lets go. Supply `DatabaseOpenOptions::deadline` on its clock or `cancel`
(an `Arc<AtomicBool>`) to stop waiting. A passed deadline still opens a free store; otherwise
it returns `DatabaseLocked` while a writer holds the store, or `HeldByReaders` while readers do:
each names the store's path, never a process number. Cancellation returns `OpenCancelled`.
Opening again from any thread in the same process waits too; if the caller cannot let the
first handle close, opening without either option waits forever. To try once without waiting
on contention, use `open_with_options` with `deadline` set to the current time of its clock.
Set `maintenance = Maintenance::Caller` as well if the host must drive all maintenance;
default options start a worker thread.
The shell says that a writer holds the store, with its path and no process number, and waits too.
<!-- enforced by: a_second_writer_waits_once_until_the_first_closes_or_the_callers_deadline_or_cancel -->

Any number of processes read meanwhile with `ReadSession::open(path, limits)`, which never creates anything.
Each statement of a reader sees the newest commit. A reader never makes the writer fail, and
holds it up only for the instant a statement takes to note which commit it reads. A reader keeps
answering when the writer stops, is killed or starts again. At reader open there can be a
short admission wait while a writer starts and checks reader holds; the CLI prints no
reader-wait status. A second writer waiting for ownership does not block readers.
The writer-open deadline and cancellation also bound its reader-gate wait.

Read sessions default to 500 result rows, 4 MiB of result bytes, 5,000 active milliseconds and
16 MiB of memory. Work (rows read plus distance evaluations) has no ceiling unless a caller
declares one.
Equality is accepted; exceeding a result or work ceiling gives `ReadLimitExceeded` with its
name, exceeding memory gives `MemoryBudgetExceeded`, and time or cancellation gives
`QueryCanceled` (SQLSTATE 57014). The session remains usable. `ReadSession::cancel()` ends
statements already running through it; later statements can run. The shell lifts the result-row
and result-byte ceilings, while retaining the time and memory defaults.
<!-- enforced by: each_read_limit_refuses_by_name, progress_reports_the_read_and_time_or_cancel_ends_only_its_statement -->

An embedded reader is opened with `db.read_session(limits)`; use
`db.read_session_with_progress(limits, Arc::new(observer))` to receive inline `ReadProgress`
reports and one `completed` total through `ReadProgressObserver`. Observer work counts as part
of the read. Both methods inherit the handle's contexts, labels and principal. For example,
make a handle with `db.scoped_with_contexts(contexts)` and call its `read_session`.
Each session reopens the files and reserves its own caches: a memory limit below 10 MiB
is refused. Standalone `ReadSession::open` is unnarrowed; options for `open_with_options`
can declare contexts, scope labels and a principal.

`ReadSessionOptions` has `limits: ReadLimits`, `contexts`, `scope_labels`, `principal`,
`clock` and `storage`; start from its default
and assign fields before `ReadSession::open_with_options(path, options)`. The enforced
`ReadLimits` fields include `result_rows`, `result_bytes`, `work`, `active_ms`, `memory`,
the cursor page and lifetime ceilings, and `tables`. `ReadLimitExceeded.field` names
the exceeded ceiling, including table admission and cursor page limits. Set a ceiling to
`u64::MAX` to lift it; for memory that exact value means no ceiling, whereas another huge
value still sizes a page cache from a quarter of the limit and can fail to open.
<!-- enforced by: progress_reports_the_read_and_time_or_cancel_ends_only_its_statement -->

`QueryResult::rows()` materializes the answer. `open_cursor` pages `SELECT` and supported
`SHOW` answers under the same read limits and declared tables, keeping one committed moment.
To page by a
unique scalar key, use a first query ordered by that key with `LIMIT`, then
`WHERE id > $last ORDER BY id LIMIT $page_size`, binding the last key of the preceding page.
The declared index must lead with the key. For composite keys spell out the lexicographic
predicate, such as `a > $a OR (a = $a AND b > $b)`; row-value `<` and `>` are refused.
`OFFSET` rescans skipped rows against `work`, and a sort or vector search can retain
`LIMIT + OFFSET` candidates in memory. Each session call sees a new commit, so concurrent
changes can skip or repeat rows across pages. If stable pages are needed in an embedded
writer host, use one `Database::transaction()` for the reads and drop it when finished;
it keeps its starting view and staged changes until then.

A host may register `db.serve_requests(namespace, handler)` to receive opaque requests on four
listeners. Another process calls `request_owner(path, namespace, bytes, &limits)` with its own
`ReadLimits`; request and answer buffers obey its memory ceiling, and `result_bytes` caps the
answer. Handlers implement `OwnerRequestHandler` and return when `OwnerReadCancellation` fires.
Closing or dropping the last writing handle cancels handlers and joins listeners before releasing the store.
`DatabaseOpenOptions::channel_dir` relocates the channel; requesters still use the store path.
Channel startup errors leave ordinary database operations available.

## The memory limit

`db.set_memory_limit(Some(bytes))` or `SET MEMORY_LIMIT 512M` declares how much memory the
engine may use for data in the writing process, and the store remembers it: every later open
for writing that names no limit of its own is under it, until it is changed or set to `NONE`.
A statement that would pass the limit is refused and committed rows stay as they were:

```text
memory limit reached during transaction: 30000210 bytes were needed and 7340032 bytes of the 16777216-byte limit were free. select or write less at once, or raise the limit with SET MEMORY_LIMIT or the memory_limit open option
```

SQL memory literals are digits followed by uppercase `K`, `M` or `G` (powers of 1024),
or `NONE`; `=` and single quotes are optional. Raw bytes and lowercase suffixes
produce the unsupported-setting `PlanError`; `64MiB` instead gives `ParseError` after
matching its `64M` prefix. Neither form is accepted.
The CLI `--memory-limit` separately accepts raw bytes.

With nothing declared, a writer takes a quarter of known Linux machine memory (including
the current cgroup v2 limit when known), with a 10 MiB minimum, or 2 GiB when unknown.
Memory detection reads Linux procfs only; macOS therefore uses the 2 GiB fallback.
`SET MEMORY_LIMIT NONE` explicitly declares no ceiling and survives reopen; it is distinct
from never declaring a limit.
<!-- enforced by: a_declared_limit_survives_reopen_however_it_was_set -->

A limit given when a store is opened, as
`DatabaseOpenOptions::memory_limit` or the shell's `--memory-limit`, is that session's limit and
no more: it holds until the session closes, and the store does not remember it. The options
type is `#[non_exhaustive]`, so start from its default and assign:

```rust,no_run
use contextdb::{Database, DatabaseOpenOptions};

fn main() -> contextdb::Result<()> {
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(64 << 20); // 64 MiB, for this session only
    let db = Database::open_with_options("./notes", options)?;
    db.close()
}
```

A reader has its own limits, given through `ReadLimits` when it opens. Its default memory
ceiling is 16 MiB; it never takes the limit the store remembers.

Every open counts 1 MiB for small fixed things, a page cache and a statement-plan cache as
used from the start. Both caches keep their opening size. The page cache takes a quarter of an
explicit opening or remembered limit, at least 8 MiB; the plan cache takes a thirty-second,
between 1 and 8 MiB. With no declaration both take their minima, even when the writer's
machine default supplies its ceiling. So 10 MiB is the smallest limit that opens a store,
and a statement needs room above what is set aside. Repeated row-write and SELECT text reuses
an unbound plan within the reserved cache; bound values are supplied on every call.
<!-- enforced by: a_declared_limit_survives_reopen_however_it_was_set, each_read_limit_refuses_by_name -->

A limit declared in an open session has to hold with the cache that session already has: the
smallest one it takes includes the 1 MiB allowance, both caches, and what its open transactions
and results hold. In a store opened with neither an opening nor remembered limit, that is
10 MiB plus what is held, so
`SET MEMORY_LIMIT 64M` is accepted at once. A smaller limit is refused with
`Error::MemoryBudgetExceeded`, changes nothing and is not remembered; the bytes the refusal
says were needed are the smallest limit that session takes. Any other limit holds at once for
what statements take, and the next open sizes its cache from it.

Only a limit below the cache of a session opened under a larger limit needs two steps: open the
store with the smaller limit, which sizes the cache from it, and declare it in that session,
which makes the store remember it:

```text
echo "SET MEMORY_LIMIT 64M;" | contextdb ./notes --write --memory-limit 64M
```

`SHOW MEMORY_LIMIT` returns one row about the session that asks, a writer's or a reader's:
`limit` (its limit in bytes, or `none`), `used` (bytes in use now, the 1 MiB and the page cache
included), `available` (what is left under the limit), `startup_ceiling` (the limit the session
was opened under) and `ceiling_refresh_failures` (always 0 in this version). In a session with
no limit it reports `none` as the limit and includes both caches in `used`: at least
10 MiB, plus live allocations. A narrowed handle receives the same columns for its own limit,
with `QueryResult::peak_bytes()` withheld as `None`.
<!-- enforced by: a_declared_limit_survives_reopen_however_it_was_set, a_narrowed_reader_is_told_the_same_whatever_is_done_to_rows_it_may_not_see -->

`QueryResult::peak_bytes()` reports the highest charged memory while a statement ran. Reads
measure their statement reservations; writes include the handle's other charges and commit
work. It is `None` on narrowed handles. `rows_examined()` counts the visible rows a statement
read, and, for a `COUNT(*)` answered from the keys or index entries its path reaches with no row
read, each key or entry counted, so that such a count is charged against the statement's work
limit as the rows it names would be; a count the table keeps (`MAINTAIN COUNT`) and the count of
a whole table examine none. `distance_evaluations()` counts the search comparisons. `Database::work_done()` gives running
`rows_read` and `vectors_compared` for the writing open, including hidden reads and maintenance's
row reads and comparisons; a narrowed handle is refused with `AdminRequired`.
<!-- enforced by: a_statements_peak_bytes_do_not_grow_with_the_store, a_writes_peak_bytes_cover_its_commit, statement_work_totals_include_hidden_reads_and_vector_distances -->

## Statement statistics

The open store keeps one line for each form of `SELECT`, `INSERT`, `UPDATE` and `DELETE` it has
run. A form is the statement with each literal written as `?`, so `WHERE id = 1` and
`WHERE id = 2` are one form. `SHOW STATEMENT_STATISTICS [LIMIT count [OFFSET start]]` gives
the lines, the form with the most rows examined plus distances compared first (forms with equal work in the order of their text), as `statement`, `calls`, `rows`, `rows_examined`,
`distance_evaluations`, `most_rows`, `most_rows_examined`, `most_distance_evaluations` and
`unused_index`. `rows` is the rows a form gave or changed, added over its calls; `most_...` is
the most in one call; `unused_index` names an index the last call could have used and did not,
with the reason, or is NULL. A cursor is one call, and each fetch adds its counts to it; a cursor records counts but never an
`unused_index` line.
`RESET STATEMENT_STATISTICS` empties the record. `SET STATEMENT_STATISTICS_LIMIT n` keeps at most
`n` forms (1024 by default; `0` keeps none), dropping the form with the fewest calls when the
record is full, and keeps the first 2,048 bytes of a form's text. The record is held in memory,
charged to the memory limit, and never written to the store, so a reopened store starts empty;
the limit is a setting and persists. A narrowed handle is refused the statements with
`AdminRequired`; a read session made from an open handle shares that handle's record.

```sql
CREATE TABLE statement_demo (id INTEGER PRIMARY KEY, tag TEXT);
INSERT INTO statement_demo (id, tag) VALUES (1, 'a');
INSERT INTO statement_demo (id, tag) VALUES (2, 'b');
INSERT INTO statement_demo (id, tag) VALUES (3, 'c');
RESET STATEMENT_STATISTICS;
SELECT tag FROM statement_demo WHERE id = 1;
SELECT tag FROM statement_demo WHERE id = 2;
SELECT tag FROM statement_demo WHERE id = 3;
SELECT tag FROM statement_demo;
SHOW STATEMENT_STATISTICS;
-- statement  calls  rows  rows_examined  distance_evaluations  most_rows  most_rows_examined  most_distance_evaluations  unused_index
-- SELECT tag FROM statement_demo  1  3  3  0  3  3  0  NULL
-- SELECT tag FROM statement_demo WHERE id = ?  3  3  3  0  1  1  0  NULL
```

The two forms did the same work, three rows examined each, so they come in the order of their text.
<!-- enforced by: the_statement_record_has_one_line_per_form_and_adds_up_its_runs, every_sql_example_of_the_documentation_does_what_its_page_shows -->

## Data from earlier development builds

Use a fresh store for this development engine. Data from an earlier build is recreated
from source; there is no in-place format upgrade or migration command. A schema record
from an incompatible build can be refused as `StoreCorrupted`; that refusal alone does
not establish that the old file suffered physical damage. Preserve any old folder before
creating the new one.

## The store on disk

```text
notes/
  data       everything committed
  lock       names the process that holds the store for writing, or held it last
  writer     short writer-startup admission marker
  writer.sock  optional request-channel socket on Linux, while registered
  readers/   one small file per reader, while a statement of its runs
```

A file under `readers/` contains the decimal commit id its live view holds. Its lock marks
the reader as alive; it is removed when the view is released. The oldest live commit keeps
its pages from being reused for the whole statement, after the short registration lock is
released. An empty directory does not prove nobody is waiting to be admitted. These files
are coordination records, not a reader-status interface.

The optional request channel can be relocated with `DatabaseOpenOptions::channel_dir`;
requesters still address the store path. On macOS its short socket address is in the system
temporary directory, to fit the operating system's path limit. These channel artifacts carry
requests; committed data remains in `data`.
<!-- enforced by: a_request_is_answered_between_two_processes, a_two_hundred_character_store_path_reaches_the_writer -->

Text and JSON values from about 1 KiB up to 1 MiB are stored
compressed when that saves an eighth or more, and read back byte for byte, with nothing to
switch on or off, so a store of long text is usually smaller than the text. While no process has the folder open for writing it is a plain
folder: copy it, move it or delete it with ordinary tools. Do not copy it while a writer is
running; this version has no command that copies a store beside a live writer.

**When a process is killed.** Nothing committed is lost: a commit that had returned is there,
and one that was in flight is whole or absent. A killed writer leaves its `lock` file and a
reader killed inside a statement leaves its file in `readers/`; the next open for writing clears
both, and nobody cleans anything by hand. Readers open and answer at once. The next open for writing after a kill
first reads every page the store uses, so recovery work grows with the store. The flat-recovery
implementation is not present in this version. The historical figures and their source revisions
belong in the [performance results](../../docs/performance/README.md).

## Retention and checking a store

Declare `RETAIN n SECONDS`, `MINUTES`, `HOURS` or `DAYS` to expire rows during maintenance.
The period starts at the commit that writes the row; existing rows start at the declaration of
retention when that is later. A `TIMESTAMP EXPIRES` column overrides the period when non-NULL:
its timestamp is when the row is due, and `i64::MAX` pins it. NULL uses the period. With no
`RETAIN`, `EXPIRES` removes nothing. Reads never remove rows. The cycle removes due rows through
DELETE's row-removal path and defers a parent still protected by references.
`SYNC SAFE` is retained in the declaration; its sync-aware retention fence is not active
in this version. It does not protect unsynced rows from expiry here.
<!-- enforced by: each_retained_row_leaves_at_the_second_its_judgement_gives, every_script_gives_its_expected_output -->

`Database::verify()` checks a committed view without modifying it and returns a `VerifyReport`
with tables in name order. Each `ContentSize` counts key/value bytes for `rows`, `large_values`,
`indexes`, `vectors` and `blocks`; `rows` is bytes, not a row count. These are content bytes,
not physical disk allocation or free space. A damaged record or inconsistent index/count gives
`StoreCorrupted`. A narrowed handle is refused with `AdminRequired`. The shell has no
`verify` or physical-size command. `verify` is a Rust API; `SHOW VECTOR_*` byte fields count
vector payload/index structures, not the whole file or free space.

To measure a stopped store from a shell, use `wc -c < ./notes/data` for file length and
`du -k ./notes/data` for allocated KiB, then compare against your application's own current
data-byte total. Neither is per-table or free-space accounting. Deleted and expired pages
are reused by later writes, and the maintenance cycle compacts them: once the pages that can be
reclaimed reach `START_RATIO` of the file (0.05 when the store declares none) it moves pages
toward the start of the file, at most 256 in one cycle, until they fall to `STOP_RATIO` (0.02),
and the free pages left at the end of the file are cut off `data`. `compacted_pages` counts the
pages a cycle moved, and a cycle reports `compaction_waiting_on` while a reader still holds pages
it moved.

If the store reports `StoreCorrupted`, stop its writers and preserve the folder before
attempting recovery. Reopening after a kill checks pages and clears abandoned coordination
files; it does not rewrite a damaged row, index or schema record. There is no CLI repair,
reset or salvage command. Restore a known-good stopped-store copy into a separate folder,
or recreate the store from the application's source data. Keep the damaged copy for
diagnosis; never delete coordination files to try to repair content.
<!-- enforced by: every_damaged_record_is_told_as_damage_by_each_path_that_meets_it, every_damaged_vector_record_is_told_as_damage_by_each_path_that_meets_it, every_damaged_schema_record_is_told_as_damage_or_read_as_tables_that_print -->

## The `contextdb` program

The `contextdb-cli` package beside this one builds `contextdb`, a SQL shell for a store.
Install it from the checkout, then open a store with it:

```text
cargo install --path <your checkout>/v2/contextdb-cli --locked
contextdb ./notes --write
```

```text
contextdb ./notes            read the store, also while another process writes it
contextdb ./notes --write    read and write it, creating it if it is missing
```

A statement ends at `;`. `BEGIN;`, `COMMIT;` and `ROLLBACK;` run a transaction, which needs
`--write`; a `BEGIN` inside a transaction, and a `COMMIT` or `ROLLBACK` outside one, change nothing,
answer `ok` and print a `warning:` line on the error stream; `.tables` lists
the tables, `.schema <table>` prints a table's declaration, `.explain <sql>` prints how a
statement would be answered without running it, and `.help` lists the commands. With
statements piped in there is no prompt, and a refusal does not stop what comes after it. A
refused statement or command is one line on the error stream, `Error: ` and then the refusal's
sentence, or the program's own sentence where one of its options is the way on; a line that
starts like a command and is not one is answered with `Unknown command:`. The rest still run. A
piped run exits 1 if any statement or command was refused, an unknown command and `.schema` of
a table that is not there included, and 0 otherwise. A piped run that meets a line that is not
text ends there with `Error: line 4 of the input is not text, so the run ends there` (the line's
number), exit status 1, and a transaction it had open is not committed. An output pipe closed
by the reader is not an error: the program stops and exits 0. However the input arrives, the exit status is 1 when the
store could not be opened or closed, and 2 when the command line itself is refused. `--json`
prints one JSON document per statement or command and each refusal as a JSON document on the
error stream. A vector in a result is a JSON array of numbers, each written at the full
precision of the 32-bit value that is stored, so a number may print with more digits than
were typed, where the table a person reads shows `[0.1, ...]`. `contextdb --help` has the
options, the exit statuses and the JSON forms.

## Numbers

[Performance](../../docs/performance/README.md) lists each speed or size promise, its counted
guard and the last measured result. Each result names its machine, measurement date and source
commit. A new figure is measured at landing; the earlier results remain dated historical evidence.
`v2/numbers.sh` prints descriptive figures when run on a machine with the stated fixtures.

## Questions

**Do a row, its relationships and its vectors commit together?** Yes. A relationship is a row in
a table of edges (`source_id`, `target_id`, `edge_type`) and a vector is a column of a row, so
all three are rows of tables, and one commit writes every row of it, the values kept beside
them, the index entries, the counts, the schema records and the vector part's records in one
storage transaction: after a kill either all of it is there or none. A statement run on its own
is a commit of its own; to write a row, its edges and its vector together, run the statements in
one `Database::transaction` and call `commit`, and a transaction dropped or rolled back writes
none of them.

`SET DISK_LIMIT 512M` and `db.set_disk_limit(Some(bytes))` persist the same ceiling;
`SHOW DISK_LIMIT` and `db.disk_limit()` inspect it. NONE clears it. The setter commits under
the prior ceiling; its new ceiling governs following writes. A growth refusal is
`DiskLimitExceeded` and leaves the handle usable. `DatabaseOpenOptions::disk_limit` applies
only to that open. `parse_size` reads byte counts for CLI options through the shared reader.
