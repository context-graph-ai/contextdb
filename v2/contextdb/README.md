# contextdb

An embedded SQL database: tables in one folder, inside your own process.

An application that keeps its records in one store and its embeddings in a separate vector
library has to keep the two consistent by hand; here a vector is a column, a row and its vector
commit together, and one `SELECT` filters rows and orders them by distance.

This is the first version of contextdb 2. What it has:

- tables with typed columns, a primary key, `NOT NULL`, `UNIQUE` and `DEFAULT`, and indexes;
- `INSERT`, `UPDATE`, `DELETE` and `SELECT` over one table;
- vector search: the rows nearest a vector by cosine distance, each with its distance, over
  vectors that a column may keep in partitions; exact on request, and through an index that
  `run_maintenance_cycle` builds and brings up to date once a column holds many vectors;
- transactions;
- one process writing a store while any number of other processes read it;
- a memory limit that holds.

What it does not have yet: relationships between rows, joins, grouping, and sync between
machines. A statement that uses one of them is
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
calls `db.run_maintenance_cycle()?` again and again, as the paragraph on upkeep under Vector search says, because
this version does not do it by itself; a search before that still finds every vector, and reads
more to do it. This page is also the crate's
documentation, and the documentation tests compile this program as it is written here. Rust 1.89 or newer; Linux and macOS. Licence:
Apache-2.0. It is developed in the open at <https://github.com/context-graph-ai/contextdb> by the context-graph-ai organisation.

## Statements

```sql
CREATE TABLE [IF NOT EXISTS] name (
  column type [PRIMARY KEY] [NOT NULL] [UNIQUE] [DEFAULT value],
  column VECTOR(n) [WITH (quantization = 'F32' | 'SQ8')]             -- F32 when left out
                   [PARTITION_KEY (column, ...) [MAX_PARTITIONS m]]  -- m is 256 when left out
                   [SEARCH_MODE AUTO | EXACT | INDEXED]
                   [AUTO_INDEX_AT a]                                 -- a is 100000 when left out
                   [CONSOLIDATION NONE | CONSOLIDATION (CHANGE_PERCENT = c, TOMBSTONE_PERCENT = t)],
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
SHOW VECTOR_INDEXES;
SHOW VECTOR_PARTITIONS [FOR table.column] [LIMIT count [OFFSET start]];
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
  returns. The clauses of a `VECTOR` column are described under Vector search (quantization, modes, upkeep),
  and the two `SHOW` statements there too, under inspection.
- `--` and `/* */` comments are read past.

Not in this version, and refused: `JOIN`, `GROUP BY`, `DISTINCT`, `LIKE`, `BETWEEN`,
`OFFSET`, `UNION`, subqueries, table aliases, `ALTER`, `DROP`, and a primary key over several
columns; and the vector clauses `IVF`, `SEARCH (...)` and `WITH (quantization = 'SQ4')`. Most are refused by name; this is the sentence the refusal prints:

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
| `VECTOR(n)` | a vector of exactly `n` finite numbers, each kept as an `f32` (a column declared `SQ8` keeps a rounded vector, see the paragraph on quantization under Vector search), written `[0.1, 0.2, 0.3]`; searched with `<=>` | `Value::Vector` |

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
- How much of the column a search compares with `$q` is the search's mode: `EXACT` compares
  every stored vector and returns the true nearest rows; `INDEXED` chooses among them through
  the column's index; `AUTO`, the default, is `EXACT` until a column is large and then uses the
  index (the paragraph on modes below).

Only `<=>`, cosine, is here: pgvector's `<->` and `<#>` are a parse error. `<=>` has a value
only for the rows of a search, so it is refused everywhere else: in `WHERE`,
in the select list of a statement that does not order by it, in an `INSERT`, `UPDATE` or
`DELETE`, with a vector other than the one the statement orders by, and in an `ORDER BY` that
is `DESC`, has a second item or has no `LIMIT`; a search takes no `OFFSET` either.

**Upkeep.** Beside every stored vector the engine keeps a one-byte copy of each of its numbers
and packs the copies of many vectors into a block, which a search screens quickly to rule most
rows out without reading them. A vector that was just written is on the waiting list, the
vectors not yet in a block, which a search compares one by one by reading their rows. Once a
column is large enough (the paragraph on modes below) the engine also groups a partition's vectors:
it finds the centres of groups of near vectors, files each vector under its nearest centre, and
keeps a one-bit sketch of every vector, an eighth of the size of its one-byte copy. A grouping
is made once and replaced whole; a vector written after it waits in blocks of its own, each
with its sketches, until the partition is grouped again.

`Database::run_maintenance_cycle` does this work in bounded steps, and nothing in this version
calls it on its own. One call first seals: it moves at most 4,096 waiting vectors into blocks
in one commit. Then it takes one step for each partition that has a grouping due or being
built, each step in a commit of its own: begin, ten rounds that find the centres, file the
vectors under them, pack the groups into blocks, measure (twice), and put the new grouping in
force. A partition is grouped again when the vectors written, removed or changed since reach
`CHANGE_PERCENT` of its grouping (20 when left out) or the removed or changed ones reach
`TOMBSTONE_PERCENT` (10); `CONSOLIDATION NONE` never groups it again. The call returns a
`MaintenanceReport`, and its `vector` field says what is left:
`maintenance_vectors_done` and `maintenance_vectors_remaining` (vectors), `remaining_partitions`
(partitions with a grouping due or being built) and `stalled_partitions` (those whose last step
the memory limit refused; a further cycle does nothing for them until the limit is raised, and
with no limit declared none stall). A host runs cycles while `remaining_partitions` is above
`stalled_partitions` or `maintenance_vectors_remaining` is not zero, as the program in the paragraph on the index below does.

A step is worked out with no lock held, so a write waits only for a step's commit and a reader
not at all, and a search made between two cycles finds every vector: it sees the grouping in
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
  selects: the grouping's blocks, the blocks written since and the waiting list. It returns the
  true nearest rows, and reads no centre and no sketch.
- `INDEXED` searches the waiting vectors of each selected partition and, of the rest, only
  the vectors the index chooses (the paragraph on the index below). A partition with no grouping,
  or with a grouping of one group, has nothing to choose among, and is searched as `EXACT`
  searches it. `INDEXED` is refused, before anything is read, when a selected partition's
  writes are far ahead of upkeep (for a partition with no grouping or a grouping of one group,
  the vectors outside its grouping number at least twice `AUTO_INDEX_AT`; for one with more
  groups, at least twice what makes it due to be grouped again, which under
  `CONSOLIDATION NONE` is never), and when a filter on rows leaves it nothing to choose among, as the
  paragraph on filters below says.
- `AUTO`, which is the default, adds up the vectors of the partitions the statement selects.
  Below `AUTO_INDEX_AT` it is `EXACT`. From there on each selected partition that has a
  grouping and is not far behind is searched as `INDEXED` searches it, and every other
  partition as `EXACT` does. `AUTO` is never refused for a grouping.

`AUTO_INDEX_AT` is 100,000 when not written. Until a column holds that many vectors in all its
partitions together (or declares `SEARCH_MODE INDEXED`), no centre, sketch or mean is
written, and `durable_index_bytes` is 0 unless removed or changed rows (10% by default) have
given a partition a grouping of one group, which takes 28 bytes. In a column that has reached it, a partition is grouped once it
holds a hundredth of it, 1,000 vectors when nothing is written, so a store of many middling
partitions is indexed too; a partition of a few hundred vectors is not cut into groups. A
declared `AUTO_INDEX_AT` of any size is served. A column that should never be grouped declares
one it will not reach.

**The index.** An indexed search of a partition that has groups compares the search vector with
the centres of its groups and takes the groups whose centre is nearly as near as the nearest
centre; then it estimates from the sketches of those groups' vectors, and of the vectors
written since the grouping, which of them can be among the nearest; only those go on to the
one-byte copy and then to the row. So every row in an indexed answer has its true distance,
the answer is in the order of those distances, no row comes twice, and a search is never
answered short: it returns `LIMIT` rows whenever the selected partitions hold that many rows
the statement's filter passes. What a sketch can cost is a row that an exact search would have
put among the nearest.

An indexed search returns rows that an exact search would rank among its nearest, each with
its true distance. How far from the nearest group it scans and how far it trusts a sketch
are measured when the index is built, on questions made from the stored vectors: half are
stored vectors themselves, half lie between two stored vectors, and a sketch counts as kept
only if it would be kept both as a grouped row and as a row written after the build. That is
a measure over those questions, not a promise for any one question: a question unlike both
kinds, rows written after the build that are unlike the rows the build saw, and a `LIMIT`
other than ten are not what the measure was taken on. `USE VECTOR EXACT` or `SEARCH_MODE EXACT`
is exact.

A search with a filter on rows uses the index too. The filter is asked of each row the search
visits, and the search widens until it holds as many allowed rows as were asked for: a filter
that passes most rows costs what the unfiltered search costs, and one that passes few costs
more, up to the work of `EXACT`. `INDEXED` is refused by name only when a filter passes too few
rows for the index to choose among, because being sure of the answer would mean asking the filter
of the whole table under the index's name; `AUTO` does that work and returns every row there
is.

```text
notes.embedding cannot be searched INDEXED for this statement now: its index is behind its writes, or the statement's filter leaves the index nothing to choose among; run a maintenance cycle, or use EXACT or AUTO
```

`QueryResult::distance_evaluations` counts the work of a similarity search: the centres it
compared with the search vector, the sketches it estimated, the one-byte copies it screened and
the rows whose distance it worked out. It is an exact count, the same on every run over the
same store, and 0 for any other statement. An indexed search usually counts far fewer than an
exact one, and can count more when it leaves few groups out, because it estimates a sketch of
every vector it keeps; each of those counts an eighth of the bytes. `Database::explain`, and
`ReadSession::explain`, return what a similarity search would do, without running it. This
program builds a store of 2,000 vectors, runs upkeep as above, and searches it both ways:

```rust,no_run
use contextdb::{params, Database};

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
    let db = Database::open("./notes-index")?;
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, embedding VECTOR(8) AUTO_INDEX_AT 1000)",
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

    // Upkeep: seal the new vectors and group them, a bounded step at a time.
    let mut cycles = 0;
    loop {
        let report = db.run_maintenance_cycle()?.vector;
        cycles += 1;
        if report.remaining_partitions <= report.stalled_partitions
            && report.maintenance_vectors_remaining == 0
        {
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
16 cycles
EXACT: 2019 distance evaluations
INDEXED: 225 distance evaluations
the same ten rows: true
requested_mode: AUTO
resolved_mode: AUTO
effective_auto_index_at: 1000
aggregate_allowed_vectors: 2000
partition_key_columns: []
partitions: 1
partition: {}
  route: indexed
  lists: 45
  probes: 8
  live_rows: 2000
  outside_grouping: 0
refusal: none
```

`lists` is the groups of the grouping in force and `probes` the most of them a search scanned
for the questions its build made, `outside_grouping` the slots in blocks written since (stale ones
included) plus the vectors still waiting, and `route` is `exact` or `indexed`; `refusal` is `none` or, for INDEXED over a partition that is
behind its writes, the sentence the search would be refused with (the refusal for a filter is
known only when the search runs). A statement that names a partition with a `$name` has nothing to select
with, since `explain` binds nothing: it lists every partition of the column and says so,
`partitions: every partition ($t is not bound)`. `explain` of a statement that is not a
similarity search is refused as a later version's.

**Quantization.** `WITH (quantization = 'SQ8')` on a column keeps each vector as one scale and
one whole number from -127 to 127 for each of its numbers, so a vector of `n` numbers takes
`n + 13` bytes in place of `4n + 9`: in the two stores of real passages under Numbers below a passage
takes about 2.0 KB in place of 4.4 and 4.9, and the store 0.45 times the room. The cost is in the answers. The vector the row keeps is the rounded one, and it is the
one a `SELECT` returns and every search, `EXACT` included, ranks by (`[0.1, 0.2, 0.3]` is
read back as `[0.0992126, 0.2007874, 0.3]`); and on those stores, searched exactly, the ten nearest rows of an
`SQ8` column differ from the full-precision column's on 12 to 21 questions in 100, by whether the
search covers one, several or all of its partitions. It is an option a table chooses and never a default: a
column keeps `F32` unless it says otherwise. `SQ4` is refused by name.

**Inspection.** `SHOW VECTOR_INDEXES` returns one row for each `VECTOR` column of the store,
in the order of table name and column, and `SHOW VECTOR_PARTITIONS` one row for each partition
of the column after `FOR`, or of every `VECTOR` column, in the order of table, column and key
tuple; `LIMIT` takes the first rows and `OFFSET`, only after `LIMIT`, skips some. Both read
the vector part's small records and no block, no centre and no row, change nothing, and work in
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
declared_auto_index_at: 1000
effective_auto_index_at: 1000
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
pending_inserts: 0
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

`.explain <sql>` in the shell prints what `Database::explain` returns; `.explain SELECT id FROM notes
ORDER BY embedding <=> $q LIMIT 10` prints:

```text
requested_mode: AUTO
resolved_mode: AUTO
effective_auto_index_at: 1000
aggregate_allowed_vectors: 2000
partition_key_columns: []
partitions: 1
partition: {}
  route: indexed
  lists: 45
  probes: 8
  live_rows: 2000
  outside_grouping: 0
refusal: none
```

Inspection tells the groups a partition has (`lists`) and the most groups a search scanned
while the index was measured (`probes`); it does not print the measured distance or width. A
column with no `PARTITION_KEY` has one row in `SHOW VECTOR_PARTITIONS`, with `partition_key`
`{}`, and a partitioned column with no vector has none. A partition's `maintenance_state` is `idle`, `pending` (a grouping is due and not begun),
`building` or `stalled`, and its `maintenance_reason` says why upkeep has work: `first_grouping`,
`changes`, `tombstones`, `damaged` or `memory_limit`. For `pending` and `building` the
`recovery_action` is `run a maintenance cycle`, and for `stalled` it is `raise the memory limit`.

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
the tables, `.schema <table>` prints a table's declaration, `.explain <sql>` prints how a
similarity search would be answered without running it (anything else it refuses like a statement
of a later version), and `.help` lists the commands. With
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
nothing else. Every search time is the 95th percentile of 100 questions asked one after the
other by one thread, in a read session under a 1 GiB memory limit unless a row says 64 MiB, in
an ordinary release build with no processor flag (the counting loop uses the processor's
bit-count instruction, which this processor has). Recall is the share of an exact search's ten
nearest rows that the search returned, averaged over the 100 questions, and the next number is
how many of the 100 questions got all ten.

| What | Measured |
|---|---|
| 20,000 rows of 1.6 KB, one commit per row: time to insert | 11.1 s |
| That store on disk (its rows repeat their text, which compresses far better than real text) | 2.4 MiB |
| Open that store | 0.561 ms |
| `SELECT COUNT(*)` over it | 0.030 ms, 0 rows read |
| A million rows under a 64 MiB limit: time to load | 10.5 s |
| Scan the million rows | 236.582 ms |
| Most memory the engine had allocated during that load and scan | 16.4 MiB |
| Most memory the process held, as the operating system reports it | 21.5 MiB |
| First build of a program that uses the library | 8 s, 47 packages, a 3.2 MiB program |
| From a fresh clone to a passing `cargo test` | 3.2 min |
| Ten nearest of 20,000 vectors, sealed, through SQL, exact: 95th percentile | 1.548 ms |
| That vector store on disk, and how many times the bytes it holds | 44.3 MiB, 1.50 times |
| Lines of Rust and grammar in the library and the program | 15,440 |
| Tests | 253 |
| Lines that differ from redb 4.1.0 in the storage library under the engine, which is redb 4.1.0 with the changes listed in `v2/redb/README.md` | 626 |

Searches for the ten nearest, indexed beside exact from the same run. Where a store is below
`AUTO_INDEX_AT` nothing is grouped and an indexed search is the exact one.

| Search | Indexed | Exact | Recall, questions with all ten |
|---|---|---|---|
| 10,000 generated vectors, one partition, 100 groups (`AUTO_INDEX_AT 1000`) | 0.541 ms | 1.185 ms | 0.9960, 96 of 100 |
| 100,008 real passages, eight partitions, 884 groups: one partition | 0.682 ms | 1.411 ms | 1.0000, 100 of 100 |
| the same: several partitions | 1.537 ms | 3.841 ms | 0.9990, 99 of 100 |
| the same: all eight | 4.052 ms | 10.880 ms | 0.9990, 99 of 100 |
| the same: all eight, under 64 MiB | 11.383 ms | not taken | 0.9990, 99 of 100 |
| the same: all eight, a filter that passes 992 of the 100,008 rows | 32.411 ms | 38.453 ms | 1.0000, 100 of 100 |
| the same, 9,996 more passages written after the build and sealed | 3.749 ms | 10.781 ms | 1.0000, 100 of 100 |
| 1,000,000 generated vectors of 768 numbers around 300 hidden centres, one partition, 1,000 groups | 1.109 ms | 180.929 ms | 1.0000, 100 of 100 |
| 1,000,000 generated camera sightings, one partition, 1,000 groups | 30.348 ms | 175.409 ms | 1.0000, 100 of 100 |
| 11,588 real passages, eight partitions, below the size: one / several / all | 0.303 / 0.724 / 1.440 ms | 0.302 / 0.724 / 1.440 ms | 1.0000, 100 of 100 each |

Getting a store to those groupings takes `run_maintenance_cycle` calls: 34 cycles and 1.8 s for
the 10,000, 56 cycles and 26.9 s for the 100,008 passages, 1,150 cycles and 362.8 s for the
million. The index takes 129, 125 and 101 bytes a vector there. Right after a build a store's
file is 1.20, 1.13 and 1.31 times the size it had sealed, which is more than the index holds,
because the pages the build freed stay in the file in this version; a store that is never
grouped stays at 1.00 times (the 11,588 passages).

With `WITH (quantization = 'SQ8')` the same two stores of real passages take 0.45 times the room
(a passage 1,968 bytes in place of 4,411 in the 11,588, and 1,992 in place of 4,946 in the
100,008). Searched exactly, the same ten rows come back as from the full-precision store on 88,
84 and 79 questions of 100 in the 11,588 (one, several, all partitions) and on 86, 83 and 85 in
the 100,008.
