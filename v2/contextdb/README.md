# contextdb

An embedded SQL database: tables in one folder, inside your own process.

An application that keeps its records in one store and its embeddings in a separate vector
library has to keep the two consistent by hand; here a vector is a column, a row and its vector
commit together, and one `SELECT` filters rows and orders them by distance.

This is the first version of contextdb 2. What it has:

- tables with typed columns, a primary key, `NOT NULL`, `UNIQUE` and `DEFAULT`, and indexes;
- `INSERT`, `UPDATE`, `DELETE` and `SELECT` over one table;
- vector search: the rows nearest a vector by cosine distance, each with its distance, exact,
  over vectors that a column may keep in partitions;
- transactions;
- one process writing a store while any number of other processes read it;
- a memory limit that holds.

What it does not have yet: an index for vector search (every search is exact), relationships
between rows, joins, grouping, and sync between machines. A statement that uses one of them is
refused and changes nothing.

It is not on crates.io yet. Clone <https://github.com/context-graph-ai/contextdb> and use it from that checkout:

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

It prints `buy oat milk: 0.006` and then `call the plumber: 1.000`. After writing vectors, a host
calls `db.run_maintenance_cycle()?` until its report's `vector.maintenance_vectors_remaining` is
0, because this version does not do it by itself; a search before that returns the same rows,
slower ([Sealing](#vector-search) below). This page is also the crate's
documentation, and the documentation tests compile this program as it is written here. Rust 1.89 or newer; Linux and macOS. Licence:
Apache-2.0. It is developed in the open at <https://github.com/context-graph-ai/contextdb> by the context-graph-ai organisation.

## Statements

```sql
CREATE TABLE [IF NOT EXISTS] name (
  column type [PRIMARY KEY] [NOT NULL] [UNIQUE] [DEFAULT value],
  column VECTOR(n) [PARTITION_KEY (column, ...) [MAX_PARTITIONS m]]  -- m is 256 when left out
                   [SEARCH_MODE AUTO | EXACT | INDEXED],
  ...,
  [UNIQUE (column, ...)]
);
CREATE INDEX name ON table (column [ASC | DESC], ...);
INSERT INTO table (column, ...) VALUES (value, ...), (value, ...);
UPDATE table SET column = value, ... [WHERE condition];
DELETE FROM table [WHERE condition];
SELECT column, ... | * FROM table [WHERE condition] [ORDER BY column [ASC | DESC], ...] [LIMIT n];
SELECT column, ... | * FROM table [WHERE condition]
  ORDER BY column <=> vector [USE VECTOR AUTO | EXACT | INDEXED] LIMIT k;
SET MEMORY_LIMIT 512M;   SET MEMORY_LIMIT NONE;   SHOW MEMORY_LIMIT;
```

- Every table has exactly one `PRIMARY KEY` column. A default is a literal or `NOW()`.
- A condition or a value is built from column names, literals, `$name` values,
  `=  !=  <>  <  <=  >  >=`, `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`, `+  -  *  /`,
  parentheses and `NOW()`. A comparison with NULL is not true, as in standard SQL.
- `x IN (a, b, ...)` and `x NOT IN (a, b, ...)` test a value against a list, and
  `(x, y) IN ((a, b), (c, d), ...)` tests several columns at once, both as standard SQL has
  them. A list is written out: `IN (SELECT ...)` is refused.
- `SELECT` also takes `COUNT(*)`, `COUNT(column)`, `SUM`, `MIN` and `MAX` over the rows that
  match, and `AS` to name a column of the result.
- `ORDER BY` puts NULL last ascending and first descending.
- The second `SELECT` is a vector search; [Vector search](#vector-search) below says what it
  returns.
- `--` and `/* */` comments are read past.

Not in this version, and refused: `JOIN`, `GROUP BY`, `DISTINCT`, `LIKE`, `BETWEEN`,
`OFFSET`, `UNION`, subqueries, table aliases, `ALTER`, `DROP`, and a primary key over several
columns. Most are refused by name; this is the sentence the refusal prints:

```text
plan error: LIKE is not supported in this version of the library
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
| `JSON` | a JSON value, written as quoted text; the value is kept, not the text's spacing | `Value::Json` |
| `VECTOR(n)` | a vector of exactly `n` finite numbers, each kept as an `f32`, written `[0.1, 0.2, 0.3]`; searched with `<=>` | `Value::Vector` |

A value must be of its column's type: text into an `INTEGER` column is refused, and so is NULL
into a `NOT NULL` column, on `INSERT` and on `UPDATE` alike. A whole number is accepted into a
`REAL` column.

## Vector search

A `VECTOR(n)` column is searched by ordering its rows by their distance from a vector and
taking the first `k`:

```sql
SELECT id, embedding <=> $q AS distance FROM notes ORDER BY distance LIMIT 10;
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
- `WHERE` is applied first: the answer is the `k` nearest of the rows that pass it. A row whose
  vector is NULL is in no answer. The distance to a vector of all zeros is 1.
- Every search is exact: it returns the rows that comparing `$q` with every stored vector
  returns.

Only `<=>`, cosine, is here: pgvector's `<->` and `<#>` are a parse error. `<=>` has a value
only for the rows of a search, so it is refused everywhere else: in `WHERE`,
in the select list of a statement that does not order by it, in an `INSERT`, `UPDATE` or
`DELETE`, with a vector other than the one the statement orders by, and in an `ORDER BY` that
is `DESC`, has a second item or has no `LIMIT`; a search takes no `OFFSET` either.

**Sealing.** Beside every stored vector the engine keeps a one-byte copy of each of its numbers
and packs the copies of many vectors into a block, which a search screens quickly to rule most
rows out without reading them. A vector that was just written is on the waiting list, the
vectors not yet in a block, which a search compares one by one by reading their rows.
`Database::run_maintenance_cycle` seals them, which is to move them from the list into blocks:
one call seals at most 4,096 in one commit and returns how many it sealed and how many still
wait; nothing in this version calls it on its own. A search returns the same rows with the same distances whether or not vectors are sealed; sealed,
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
`USE VECTOR`, written after the `ORDER BY` item, says it for one statement. `AUTO`, which is
the default, and `EXACT` both run the exact search. `INDEXED` asks for an index, and this
version has none: a column may be declared with it, and a search under it is refused before
anything is read, unless the statement itself says `USE VECTOR EXACT` or `AUTO`.

```text
no vector index ships in this version, so notes.embedding cannot be searched INDEXED; use EXACT or AUTO
```

## Binding values

`$name` in a statement is the entry `"name"` of the map passed with it. `params!` writes the
map: `params! { "id" => Uuid::new_v4(), "body" => "buy milk" }`, or `params! {}` for none. It
takes `bool`, `i64`, `f64`, `&str`, `String`, `Uuid`, a `serde_json::Value` and a `Vec<f32>`;
any other value is written as a `Value`, for example `Value::Timestamp(1_767_225_601_000)` or
`Value::Null`. `contextdb::Uuid` and `contextdb::serde_json` are the types the library uses, so
`Uuid::new_v4()` and JSON values need no dependency of your own.

## Refusals

Every call that can fail returns `contextdb::Result` with one `contextdb::Error`, except
`Database::memory_limit`, `memory_used`, `table_names` and `committed_watermark`, which return
a plain value and panic on a handle that was closed. A program
matches on the kind (`Error::UniqueViolation { table, column }`, `Error::ColumnNotNullable`,
`Error::TableNotFound`, `Error::DatabaseLocked`, `Error::MemoryBudgetExceeded`, and the rest
listed on the `Error` type); a person reads the sentence it prints:

```text
unique constraint violation: notes (id)
column notes.body is NOT NULL
column not found: notes.nothing
parse error: syntax error at line 1, column 1: unexpected 'SELEC'
```

A refused statement changes nothing. Inside a transaction it leaves the transaction usable.

## Transactions

`db.execute` is a transaction of one statement: when it returns, the change is on disk.
`db.transaction()` begins a longer one. Its statements see what it has written and nobody else
does until `commit` returns; dropping it, or `rollback`, discards it. A transaction either
declares tables and indexes or writes rows, not both. When two open transactions insert the same
key, or change the same row, the first to commit wins and the other's `commit` is refused.
`SET MEMORY_LIMIT` is not part of a transaction: run inside one, it takes effect and the store
remembers it at once, and a rollback does not undo it.

## One writer, many readers

One process at a time holds a store for writing. A second `Database::open` on the same folder is
refused at once, naming the holder:

```text
another session holds this store for writing: process 1983518 holds ./notes; open a read session to read it
```

Any number of processes read meanwhile with `ReadSession::open`, which never creates anything.
Each statement of a reader sees the newest commit. A reader never makes the writer fail, and
holds it up only for the instant a statement takes to note which commit it reads. A reader keeps
answering when the writer stops, is killed or starts again.

## The memory limit

`db.set_memory_limit(Some(bytes))` or `SET MEMORY_LIMIT 512M` declares how much memory the
engine may use for data in the writing process, and the store remembers it: every later open
for writing that names no limit of its own is under it, until it is changed or set to `NONE`.
A statement that would pass the limit is refused and committed rows stay as they were:

```text
memory limit reached during transaction: 30000210 bytes were needed and 7340032 bytes of the 16777216-byte limit were free. select or write less at once, or raise the limit with SET MEMORY_LIMIT or the memory_limit open option
```

With nothing declared there is no ceiling. A limit given when a store is opened, as
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

A reader has a
limit of its own, given when it opens, and none unless one is given: it never takes the limit
the store remembers.

Every open sets aside 1 MiB for small fixed things and its page cache, and counts both as used
from the start. The cache is sized once, when the store is opened, and keeps that size until
the session closes: a quarter of the limit in force and never less than 8 MiB, which is also
its size when there is no limit. So 9 MiB is the smallest limit that opens a store, and a
statement needs room above what is set aside.

A limit declared in an open session has to hold with the cache that session already has: the
smallest one it takes is 1 MiB, plus its cache, plus what its open transactions and results
hold. In a store opened with no limit in force that is 9 MiB plus what is held, so
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
no limit it reports `none` as the limit and 9437184 bytes, which is 9 MiB, as used.

## The store on disk

```text
notes/
  data       everything committed
  lock       names the process that holds the store for writing, or held it last
  readers/   one small file per reader, while a statement of its runs
```

Nothing is written anywhere else. Text and JSON values from about 1 KiB up to 1 MiB are stored
compressed when that saves an eighth or more, and read back byte for byte, with nothing to
switch on or off, so a store of long text is usually smaller than the text. While no process has the folder open for writing it is a plain
folder: copy it, move it or delete it with ordinary tools. Do not copy it while a writer is
running; this version has no command that copies a store beside a live writer.

**When a process is killed.** Nothing committed is lost: a commit that had returned is there,
and one that was in flight is whole or absent. A killed writer leaves its `lock` file and a
reader killed inside a statement leaves its file in `readers/`; the next open for writing clears
both, and nobody cleans anything by hand. Readers open and answer at once. The next open for writing after a kill
first reads every page the store uses, which takes time that grows with the store, measured on
the machine named below with the file out of the operating system's cache:

| Store size | Open for writing after a kill | Open after a clean close |
|---|---|---|
| 257 MiB | 72 ms | 0.9 ms |
| 2 GiB | about 0.5 s | 1.2 ms |
| 8 GiB | about 3.4 s | 1.7 ms |

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
`--write`; `.tables` lists
the tables, `.schema <table>` prints a table's declaration and `.help` lists the commands. With
statements piped in there is no prompt, and a refusal does not stop what comes after it. A
refused statement or command is one line on the error stream, `Error: ` and then the refusal's
sentence, or the program's own sentence where one of its options is the way on; a line that
starts like a command and is not one is answered with `Unknown command:`. The rest still run. A
piped run exits 1 if any statement or command was refused, an unknown command and `.schema` of
a table that is not there included, and 0 otherwise. However the input arrives, the exit status is 1 when the
store could not be opened or closed, and 2 when the command line itself is refused. `--json`
prints one JSON document per statement or command and each refusal as a JSON document on the
error stream. A vector in a result is a JSON array of numbers, each written at the full
precision of the 32-bit value that is stored, so a number may print with more digits than
were typed, where the table a person reads shows `[0.1, ...]`. `contextdb --help` has the
options, the exit statuses and the JSON forms.

## Numbers

Measured at one commit by `v2/numbers.sh`, which prints them again on any machine: a release
build on a 16-core AMD Ryzen 9 9955HX with 64 GB of memory and an NVMe drive, running Linux and
nothing else.

| What | Measured |
|---|---|
| 20,000 rows of 1.6 KB, one commit per row: time to insert | 11.0 s |
| That store on disk (its rows repeat their text, which compresses far better than real text) | 2.4 MiB |
| Open that store | 0.606 ms |
| `SELECT COUNT(*)` over it | 0.025 ms, 0 rows read |
| A million rows under a 64 MiB limit: time to load | 9.7 s |
| Scan the million rows | 239.167 ms |
| Most memory the engine had allocated during that load and scan | 16.4 MiB |
| Most memory the process held, as the operating system reports it | 21.3 MiB |
| First build of a program that uses the library | 6 s, 47 packages, a 3.0 MiB program |
| From a fresh clone to a passing `cargo test` | 2.7 min |
| Ten nearest of 20,000 vectors, sealed, through SQL: 95th percentile | 1.500 ms |
| That vector store on disk, and how many times the bytes it holds | 44.3 MiB, 1.50 times |
| Lines of Rust and grammar in the library and the program | 10,809 |
| Tests | 171 |
| Lines that differ from redb 4.1.0 in the storage library under the engine, which is redb 4.1.0 with the changes listed in `v2/redb/README.md` | 626 |
