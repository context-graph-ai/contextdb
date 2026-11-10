# contextdb

An embedded SQL database: tables in one folder, inside your own process.

This is the first version of contextdb 2. What it has:

- tables with typed columns, a primary key, `NOT NULL`, `UNIQUE` and `DEFAULT`, and indexes;
- `INSERT`, `UPDATE`, `DELETE` and `SELECT` over one table;
- transactions;
- one process writing a store while any number of other processes read it;
- a memory limit that holds.

What it does not have yet: vector search, relationships between rows, joins, grouping, and sync
between machines. A statement that uses one of them is refused and changes nothing.

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
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT NOT NULL, made TIMESTAMP DEFAULT NOW())",
        &params! {},
    )?;
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &params! { "id" => Uuid::new_v4(), "body" => "buy milk" },
    )?;

    let tx = db.transaction()?; // rolls back if dropped
    tx.execute("UPDATE notes SET body = 'buy oat milk' WHERE body = 'buy milk'", &params! {})?;
    tx.commit()?;

    let found = db.execute("SELECT body, made FROM notes ORDER BY made LIMIT 10", &params! {})?;
    for row in found.rows() {
        if let Value::Text(body) = &row[0] {
            println!("{body}");
        }
    }
    Ok(())
}
```

It prints `buy oat milk`. This page is also the crate's documentation, and the documentation
tests compile this program as it is written here. Rust 1.89 or newer; Linux and macOS. Licence:
Apache-2.0. It is developed in the open at <https://github.com/context-graph-ai/contextdb> by the context-graph-ai organisation.

## Statements

```sql
CREATE TABLE [IF NOT EXISTS] name (
  column type [PRIMARY KEY] [NOT NULL] [UNIQUE] [DEFAULT value],
  ...,
  [UNIQUE (column, ...)]
);
CREATE INDEX name ON table (column [ASC | DESC], ...);
INSERT INTO table (column, ...) VALUES (value, ...), (value, ...);
UPDATE table SET column = value, ... [WHERE condition];
DELETE FROM table [WHERE condition];
SELECT column, ... | * FROM table [WHERE condition] [ORDER BY column [ASC | DESC], ...] [LIMIT n];
SET MEMORY_LIMIT 512M;   SET MEMORY_LIMIT NONE;   SHOW MEMORY_LIMIT;
```

- Every table has exactly one `PRIMARY KEY` column. A default is a literal or `NOW()`.
- A condition or a value is built from column names, literals, `$name` values,
  `=  !=  <>  <  <=  >  >=`, `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`, `+  -  *  /`,
  parentheses and `NOW()`. A comparison with NULL is not true, as in standard SQL.
- `SELECT` also takes `COUNT(*)`, `COUNT(column)`, `SUM`, `MIN` and `MAX` over the rows that
  match, and `AS` to name a column of the result.
- `ORDER BY` puts NULL last ascending and first descending.
- `--` and `/* */` comments are read past.

Not in this version, and refused: `JOIN`, `GROUP BY`, `DISTINCT`, `IN`, `LIKE`, `BETWEEN`,
`OFFSET`, `UNION`, subqueries, table aliases, `ALTER`, `DROP`, a primary key over several
columns, and similarity search (`<=>`). Most are refused by name; this is the sentence the
refusal prints:

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
| `VECTOR(n)` | a vector of exactly `n` numbers, written `[0.1, 0.2, 0.3]`; stored and returned, not searchable in this version | `Value::Vector` |

A value must be of its column's type: text into an `INTEGER` column is refused, and so is NULL
into a `NOT NULL` column, on `INSERT` and on `UPDATE` alike. A whole number is accepted into a
`REAL` column.

## Binding values

`$name` in a statement is the entry `"name"` of the map passed with it. `params!` writes the
map: `params! { "id" => Uuid::new_v4(), "body" => "buy milk" }`, or `params! {}` for none. It
takes `bool`, `i64`, `f64`, `&str`, `String`, `Uuid`, a `serde_json::Value` and a `Vec<f32>`;
any other value is written as a `Value`, for example `Value::Timestamp(1_767_225_601_000)` or
`Value::Null`. `contextdb::Uuid` and `contextdb::serde_json` are the types the library uses, so
`Uuid::new_v4()` and JSON values need no dependency of your own.

## Refusals

Every call that can fail returns `contextdb::Result` with one `contextdb::Error`. A program
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
engine may use for data in the writing process, and the store remembers it until it is changed
or set to `NONE`. A statement that would pass the limit is refused and committed rows stay as
they were:

```text
memory limit reached during transaction: 30000210 bytes were needed and 7340032 bytes of the 16777216-byte limit were free. select or write less at once, or raise the limit with SET MEMORY_LIMIT or the memory_limit open option
```

With nothing declared there is no ceiling. A reader has a limit of its own, given when it opens,
and none unless one is given. Every open sets 9 MiB of the limit aside, 8 MiB for the page cache
and 1 MiB for small fixed things, so 9 MiB is the smallest limit that opens a store and a
statement needs room above it.

`SHOW MEMORY_LIMIT` returns one row about the session that asks, a writer's or a reader's:
`limit` (its limit in bytes, or `none`), `used` (bytes in use now, the 9 MiB included),
`available` (what is left under the limit), `startup_ceiling` (the limit the session was opened
under) and `ceiling_refresh_failures` (always 0 in this version).

## The store on disk

```text
notes/
  data       everything committed
  lock       names the process that holds the store for writing, or held it last
  readers/   one small file per reader, while a statement of its runs
```

Nothing is written anywhere else. While no process has the folder open for writing it is a plain
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
error stream. `contextdb --help` has the
options, the exit statuses and the JSON forms.

## Numbers

Measured at one commit by `v2/numbers.sh`, which prints them again on any machine: a release
build on a 16-core AMD Ryzen 9 9955HX with 64 GB of memory and an NVMe drive, running Linux and
nothing else.

| What | Measured |
|---|---|
| 20,000 rows of 1.6 KB, one commit per row: time to insert | 17.2 s |
| That store on disk | 60.3 MiB |
| Open that store | 0.631 ms |
| `SELECT COUNT(*)` over it | 0.026 ms, 0 rows read |
| A million rows under a 64 MiB limit: time to load | 11.9 s |
| Scan the million rows | 246 ms |
| Most memory the engine had allocated during that load and scan | 13.4 MiB |
| Most memory the process held, as the operating system reports it | 18.0 MiB |
| First build of a program that uses the library | 5 s, 46 packages, a 2.7 MiB program |
| From a fresh clone to a passing `cargo test` | 2.1 min |
| Lines of Rust and grammar in the library and the program | 7,369 |
| Tests | 98 |
| Lines that differ from redb 4.1.0 in the storage library under the engine, which is redb 4.1.0 with the changes listed in `v2/redb/README.md` | 577 |
