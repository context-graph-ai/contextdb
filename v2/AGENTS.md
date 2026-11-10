# contextdb 2: working in `v2/`

This workspace is contextdb 2: an embedded SQL database kept in one folder, the `contextdb`
program over it, and the storage library both are built on. Repository-wide policy (commit
messages, licence, contribution rules) is in the root [`AGENTS.md`](../AGENTS.md) and is not
repeated here; every command is in [`README.md`](README.md).

## Packages and parts

| Package | What it is |
|---|---|
| `contextdb/` | The library. Its public calls are the re-exports in `contextdb/src/lib.rs`; `contextdb/README.md` is also its documentation page, and the program in it is compiled as a documentation test. |
| `contextdb-cli/` | The `contextdb` program, a SQL shell. Every statement and command goes through the library's public calls; it never opens a store's files itself. |
| `redb/` | redb 4.1.0 with the changes listed at the top of `redb/README.md`. It keeps its own manifest and lock file and is not a workspace member. |

The library is ten parts, one folder each under `contextdb/src/`:

| Part | Does | Never |
|---|---|---|
| `types` | `Value`, the one `Error`, ids, table descriptions | holds behaviour: reads no store, checks no rule, decides no answer |
| `system` | the time of day, through `Clock`; the one place in the library that reads a clock | decides anything |
| `memory` | the budget and the reservations taken from it | allocates for another part: it counts what they are about to take and says no first |
| `sql` | grammar, parser, planner: text to a statement, a statement to a plan | touches storage or knows what a table holds |
| `store` | the folder and data file, how each record is keyed, views of one moment, the reader files (one in `readers/` per reader that is inside a statement, naming the commit it reads) and the locks | interprets SQL or a table's rules |
| `schema` | table declarations: checking, storing, printing them back | changes a row |
| `query` | answers a SELECT from one table; holds the one expression evaluator; counts the rows it reads | changes anything |
| `write` | applies INSERT, UPDATE and DELETE to a change set; checks types, NOT NULL, the primary key and UNIQUE rules; keeps index entries and row counts | returns a query's result to a caller |
| `database` | the handle a store is opened for writing with, its transactions, the one commit function | has a second way to commit |
| `read` | a session that reads a store without holding it for writing, from any process | changes anything a writer stored |

## Where the parts meet

A part reaches another only through what that part makes visible: the `pub(crate)` items of its
`mod.rs`, and for `sql` its `ast` and `plan` modules. The types handed across:

| Type | Defined in | Who takes it |
|---|---|---|
| `Plan` (with `Effect`, `Expr`, `Statement`) | `sql` | `schema`, `write` and `query` each take a planned statement |
| `Read` (implemented by `View`, the store as of one commit, and by `ChangeSet`) | `store` | `query` and `write` read through it |
| `ChangeSet`, everything one transaction means to change, held in memory over a `View` | `store` | `schema` and `write` stage into it; `database` hands it to `Store::commit` |
| `Store`, `Key`, `Row`, `Cell`, `TableId`, `IndexId` | `store` | `database` and `read` open a `Store`; the rest name rows and indexes |
| `Budget`, `Reservation` | `memory` | everything whose memory grows with data holds a reservation |
| `Catalog`, `Table` | `schema` | `query`, `write`, `database` and `read` look tables up in it |
| `eval`, `matching` | `query` | `write` finds the rows of an UPDATE or DELETE with `query::matching` and works out values with `query::eval` |
| `QueryResult` | `query` | `database` and `read` build it and hand it to the caller |

## Invariants and the tests that guard them

Every path in this guide is written from `v2/`.

| Invariant | Test |
|---|---|
| A commit is all or nothing under a kill at any storage call | `contextdb/tests/crash/main.rs`: `a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing`, `a_kill_inside_a_commit_of_many_rows_keeps_rows_index_and_count_together` |
| What was committed is there after reopen | `contextdb/tests/store.rs`: `what_was_committed_is_there_after_close_and_reopen` |
| A commit that reached the disk is reported as committed | `contextdb/tests/crash/main.rs`: `a_commit_on_disk_is_reported_and_served_though_the_read_after_it_fails` |
| A reader in another process reads while the writer commits, and keeps answering when the writer is killed and restarted | `contextdb/tests/readers.rs`: `a_reader_in_another_process_reads_while_the_writer_commits`, `a_reader_keeps_answering_when_the_writer_is_killed_and_follows_the_restarted_writer` |
| A reader that reads the file header inside a commit gets the last whole commit | `redb/tests/contextdb_changes.rs`: `a_reader_that_catches_the_header_inside_a_commit_reads_the_last_whole_commit` |
| One writer; the second is refused at once | `contextdb/tests/store.rs`: `a_second_writer_is_refused_at_once_naming_the_holder_and_the_next_open_needs_no_retry` |
| The memory limit holds | `contextdb/tests/memory_ceiling.rs`: `a_million_rows_load_and_scan_under_64_mib` |
| A refused statement changes nothing | `contextdb/tests/memory.rs`: `a_statement_that_would_cross_the_limit_is_refused_by_name_and_committed_rows_stay`; `contextdb/tests/transactions.rs`: `a_refused_statement_changes_nothing_and_leaves_the_transaction_usable` |
| A damaged page is reported as damage | `contextdb/tests/crash/main.rs`: `a_damaged_page_is_refused_as_damage_by_name`; `redb/tests/contextdb_changes.rs`: `a_page_with_a_wrong_checksum_is_refused_when_read` |
| Counted work: a count reads no rows, a key lookup reads one | `contextdb/tests/counted_work.rs`: `count_of_a_whole_table_reads_no_rows`, `a_lookup_by_primary_key_reads_one_row` |

## Where a change lives

| Change | Folder | Test it needs |
|---|---|---|
| A new SQL statement or clause | `contextdb/src/sql/` (`grammar.pest`, `parser/`, the `Statement` type in `ast.rs`, `plan.rs`, `planner.rs`; take its first words out of the grammar's list of what a later version reads), then the part that runs it: `schema`, `write` or `query` | a script under `contextdb/tests/sql/` |
| A new refusal (error kind) | `Error` in `contextdb/src/types/mod.rs`; how the program prints it in `contextdb-cli/src/output.rs` | `contextdb/tests/refusals.rs` for its fields and that nothing changed; `contextdb/tests/sql/refusals.sql` for its name |
| What a statement returns | `contextdb/src/query/` for a SELECT, `contextdb/src/write/` for a row count | the expected output of the scripts under `contextdb/tests/sql/`, rewritten and the diff read |
| How a store is laid out on disk | `contextdb/src/store/` (keys, records, the data file, the folder's files) | `contextdb/tests/values.rs` and `contextdb/tests/store.rs` for reopen, `contextdb/tests/crash/` for a kill |
| The storage library | `redb/src/`; `redb/README.md` lists every change against upstream, so add yours there. The storage library decides which pages may be reused; the `store` part only tells it the oldest commit a reader still holds, and keeps the reader files and the lock | `redb/tests/contextdb_changes.rs` |
| A new shell option or dot command | `contextdb-cli/src/main.rs` for an option, `contextdb-cli/src/shell.rs` for a command | `contextdb-cli/tests/shell.rs` |
| A new public call | the part that owns it, re-exported in `contextdb/src/lib.rs`, described in `contextdb/README.md` | a Rust test under `contextdb/tests/` |

## How tests are written here

- A test goes through a public call of the library or through the built program, never an inner item.
- SQL behaviour is a script under `contextdb/tests/sql/` with its expected output; tests that kill a process and reopen the store live in `contextdb/tests/crash/`.
- A unit test of one pure function may sit beside it in a file named `tests.rs`.
- A test asserts states, values, error kinds and counts, never a duration, and waits only on something a process announces.
- Its data is fixed, and each test has its own store folder (`support::folder` in `contextdb/tests/support/mod.rs`).
- Nothing is retried (`.config/nextest.toml` sets `retries = 0`) and no test is ignored.

Two lint settings hold this up:

- `clippy.toml` refuses `std::thread::sleep`, starting a thread, `Instant::now`, `SystemTime::now` and `uuid::Uuid::new_v4`: the engine does not sleep, start a thread or draw a random number, and reads the time only through `Clock`.
- `Cargo.toml` denies `clippy::iter_over_hash_type` for both members: a hash map is walked in a different order in every process, so nothing is built by walking one.

## Do not add

- A second way to do one thing: there is one commit function, one expression evaluator, one `Error`.
- A test-only item in shipped code: no test-only name, feature or `pub` door; tests use what a user has.
- A check another part already makes: `write` checks a table's rules, `memory` says no, `store` only keeps.
- A lint switched off. Outside `redb/` there are four `allow` attributes and they stay the only ones: the machine clock in `contextdb/src/system/mod.rs` (the one place the time of day is read); the timer in `contextdb/examples/measure.rs` (measuring elapsed time is what that program is for); and two in `contextdb/tests/support/mod.rs`, one on its one thread (it moves a child's output lines to a channel so that a wait can give up) and `dead_code` for the whole file (each test program uses only some of its helpers).

## Fast commands

Every command is run from the repository's root folder, the one that contains `v2/`. The whole check, then one Rust test, then one SQL script:

```sh
cargo fmt --manifest-path v2/Cargo.toml --all --check
cargo clippy --manifest-path v2/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo nextest run --manifest-path v2/Cargo.toml --workspace --locked
cargo test --manifest-path v2/Cargo.toml --workspace --locked --doc
cargo test --manifest-path v2/redb/Cargo.toml --locked --lib --tests
cargo +1.89 check --manifest-path v2/Cargo.toml --workspace --locked

cargo nextest run --manifest-path v2/Cargo.toml -p contextdb a_fresh_store_is_one_folder

SQL_SCRIPT=keys.sql cargo test --manifest-path v2/Cargo.toml -p contextdb --test sql
```
