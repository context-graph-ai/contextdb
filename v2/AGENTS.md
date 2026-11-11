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

The library is twelve parts, one folder each under `contextdb/src/`:

| Part | Does | Never |
|---|---|---|
| `types` | `Value`, the one `Error`, ids, table descriptions | holds behaviour: reads no store, checks no rule, decides no answer |
| `system` | the time of day, through `Clock`; the one place in the library that reads a clock | decides anything |
| `memory` | the budget and the reservations taken from it | allocates for another part: it counts what they are about to take and says no first |
| `sql` | grammar, parser, planner: text to a statement, a statement to a plan | touches storage or knows what a table holds |
| `store` | the folder and data file, how each record is keyed, a row's large text, JSON and vectors kept beside the row in slices (text and JSON compressed there when that saves an eighth, `store/record.rs`), views of one moment, the reader files (one in `readers/` per reader that is inside a statement, naming the commit it reads) and the locks | interprets SQL or a table's rules |
| `schema` | table declarations: checking, storing, printing them back | changes a row |
| `query` | answers a SELECT from one table; holds the one expression evaluator; counts the rows it reads | changes anything |
| `write` | applies INSERT, UPDATE and DELETE to a change set; checks types, NOT NULL, the primary key and UNIQUE rules; keeps index entries and row counts | returns a query's result to a caller |
| `database` | the handle a store is opened for writing with, its transactions, the one commit function | has a second way to commit |
| `read` | a session that reads a store without holding it for writing, from any process | changes anything a writer stored |
| `vector` | the blocks of a VECTOR column (a one-byte copy of every vector), the lists of vectors not yet in a block, the column's partitions and their count, exact search over them, and sealing | decides which rows a reader may see: `query` tells it, row by row, and a row is in an answer only with the distance `query` worked out from the row's stored vector |
| `upkeep` | one cycle of background work when it is called: sealing at most 4,096 waiting vectors into blocks, in one commit; the 4,096 is `SEAL_ROWS` in `upkeep/mod.rs` | starts a thread, reads a clock, or runs unless called |

The files of `vector/`, each as its opening comment says:

- `slot.rs`: one vector as a block keeps it (a scale, a rounding error and a signed byte for each number), the range a search gets from it, and the distance every answer is ordered by.
- `block.rs`: the slots of as many vectors of one partition as fit in the block's bytes, each with the key of its row.
- `directory.rs`: the partitions of a VECTOR column, one record for each key tuple under which a row has a vector.
- `search.rs`: the one search: scores the rows in no block exactly, screens the slots of the selected partitions, asks for the true distance of the rows that can reach the answer, and proves the answer.
- `seal.rs`: moving the vectors on the waiting lists into blocks.
- `stage.rs`: what a commit's row changes mean for the vector part, worked out once inside the commit function.
- `tally.rs`: the partitions a transaction's statements have made and emptied, so that the statement that would give a column one partition too many is refused when it runs.

## Where the parts meet

A part reaches another only through what that part makes visible: the `pub(crate)` items of its
`mod.rs`, and for `sql` its `ast` and `plan` modules. A statement form this version does not have is refused in `sql` (`not_supported` in `sql/mod.rs`, called by the parser and the planner), before any table is looked up. The types handed across:

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
| `PhysicalPlan::VectorSearch`; `Expr::Distance`, `Expr::Row`, `Expr::InList` | `sql` | `query` runs the search, puts its distance wherever the select list writes `<=>`, and evaluates `IN` lists and row values |
| `VectorColumn` (a VECTOR column's number, partition key, limit and mode), from `Table::vectors`, `Table::vector` and `Catalog::vector` | `schema` | `query`, `write` and `vector` |
| `staged`, `vectors` and `vector_number` on `Read`; `put_vector`, `delete_vector` and `put_vector_number` on `ChangeSet`; `VectorRecord`, `VectorNumber`, `VectorId` | `store` | `vector` reads and stages its blocks, waiting entries, partition entries and counts through them; `store` keeps them and never interprets one |
| `search`, `Nearest`, `distance` | `vector` | `query` calls `search` with the partitions the filter selects and answers it, row by row, with `distance` of the row's stored vector, or with nothing for a row that is gone, filtered out or has no vector |
| `Tally`, the partitions a transaction has started and emptied | `vector` | `database` holds one beside each change set and forgets a refused statement's part of it; `write` notes every row it stages and settles the tally as a statement's last act, which is where a statement over `MAX_PARTITIONS` is refused |
| `stage`, `seal` | `vector` | `database` calls `stage` inside the one commit function, after the commit's other rules have passed; `upkeep` calls `seal` |
| `cycle`, `MaintenanceReport` | `upkeep` | `database` runs one cycle for `run_maintenance_cycle` and commits it through the one commit function |
| `Store::count_cache_under` | `store` | `database` has the store check its page cache against a new limit, on the one path that sets the memory limit |

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
| The stored bytes of a large value, a vector record or a table's vector declaration that disagree with themselves are reported as damage, never a panic or another value | `contextdb/src/store/tests.rs`: `a_plain_text_whose_slices_hold_fewer_bytes_than_its_row_records_is_damage`, `a_compressed_text_that_decodes_to_bytes_that_are_not_text_is_damage`; `contextdb/src/vector/tests.rs`: `a_pending_record_of_a_row_with_no_vector_is_reported_as_damage`, `a_write_to_a_vector_column_that_is_no_column_of_its_table_is_reported_as_damage` |
| Counted work: a count reads no rows, a key lookup reads one | `contextdb/tests/counted_work.rs`: `count_of_a_whole_table_reads_no_rows`, `a_lookup_by_primary_key_reads_one_row` |
| Every row with a vector is found by a search, whether its vector waits on a list or is in a block, and after a kill inside a commit or a seal | `contextdb/tests/vectors.rs`: `exact_search_returns_the_true_nearest_vectors`; `contextdb/tests/vectors_upkeep.rs`: `sealing_advances_only_when_asked_and_in_bounded_cycles`; `contextdb/tests/crash/main.rs`: `a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together`, `a_kill_at_any_storage_call_of_a_seal_leaves_every_vector_found` |
| A row whose vector is NULL, and a deleted row, is never in an answer | `contextdb/tests/vectors.rs`: `exact_search_returns_the_true_nearest_vectors`, `exact_search_reads_its_own_snapshot` |
| A partition exists exactly while a row with a vector is in it, and `MAX_PARTITIONS` counts those and nothing else | `contextdb/tests/vectors_partitions.rs`: `vector_partition_limit_counts_and_refuses`, `the_partition_cap_counts_live_partitions_while_a_reader_holds_an_earlier_state` |
| No answer holds a row twice, whatever the row's vector has been | `contextdb/tests/vectors.rs`: `a_row_is_in_an_answer_once_after_its_vector_changes` |
| A store's data file is no longer than a recorded number of bytes for a fixed content | `contextdb/tests/vectors_upkeep.rs`: `a_small_fixed_store_stays_within_its_recorded_size` |
| A text or JSON value reads back byte for byte whether or not it is stored compressed; one that does not shrink by an eighth takes the room it takes uncompressed, and text that repeats takes less than its own bytes | `contextdb/tests/large_values.rs`: `large_text_and_json_come_back_byte_for_byte`, `a_value_that_does_not_shrink_is_kept_as_it_is`, `a_store_of_repeating_text_is_smaller_than_the_text` |
| No value is produced past the memory limit: a compressed value whose real size does not fit is refused before its bytes exist, and compression never refuses a commit | `contextdb/tests/large_values.rs`: `a_compressed_value_too_large_for_the_limit_is_refused`, `a_commit_with_no_room_to_compress_is_written_plain`; `contextdb/tests/memory_ceiling.rs`: `a_refused_compressed_value_is_never_produced` |
| A compressed value whose stored bytes do not give the length it records is reported as store damage | `contextdb/tests/large_values.rs`: `a_damaged_length_of_a_compressed_value_is_reported_as_damage` |

## Where a change lives

| Change | Folder | Test it needs |
|---|---|---|
| A new SQL statement or clause | `contextdb/src/sql/` (`grammar.pest`, `parser/`, the `Statement` type in `ast.rs`, `plan.rs`, `planner.rs`; take its first words out of the grammar's list of what a later version reads), then the part that runs it: `schema`, `write` or `query` | a script under `contextdb/tests/sql/` |
| A new vector clause | `contextdb/src/sql/` reads it, as any clause; `contextdb/src/schema/` checks, stores and prints back one that is declared with the column; `contextdb/src/query/search.rs` settles what a search is asked (mode, vector, partitions, filter); `contextdb/src/vector/` only when blocks, partitions or the search itself change | `contextdb/tests/vectors_statements.rs` for what is read and refused, `contextdb/tests/vectors.rs` for the rows a search returns |
| A new refusal (error kind) | `Error` in `contextdb/src/types/mod.rs`; how the program prints it in `contextdb-cli/src/output.rs` | `contextdb/tests/refusals.rs` for its fields and that nothing changed; `contextdb/tests/sql/refusals.sql` for its name |
| What a statement returns | `contextdb/src/query/` for a SELECT, `contextdb/src/write/` for a row count | the expected output of the scripts under `contextdb/tests/sql/`, rewritten and the diff read |
| How a store is laid out on disk | `contextdb/src/store/` (keys, records, the data file, the folder's files) | `contextdb/tests/values.rs` and `contextdb/tests/store.rs` for reopen, `contextdb/tests/crash/` for a kill |
| The storage library | `redb/src/`; `redb/README.md` lists every change against upstream, so add yours there. The storage library decides which pages may be reused; the `store` part only tells it the oldest commit a reader still holds, and keeps the reader files and the lock. Two of its rules keep a store close to the size of what it holds, and a change there keeps both: a leaf written in key order is left full, and past 1 MiB the file grows by a sixteenth of its usable bytes (`a_table_written_in_key_order_has_full_leaves`, `a_file_is_at_most_a_sixteenth_longer_than_its_pages`) | `redb/tests/contextdb_changes.rs` |
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

## Making a change

A defect is fixed by changing or removing code where its promise is owned, not by adding
around it. Before writing a line:

1. **Name what broke.** Find the row of "Invariants and the tests that guard them" that the
   defect breaks, or write the one sentence that should be a row. A defect that cannot be
   said in one sentence is not understood yet.
2. **Find the part that owns it** in the parts table, and fix it there, once. A check added
   at each caller, a flag, an option, a retry, a second path or a special case for one input
   means the fix is in the wrong part: stop and look again.
3. **Fix the kind, not the one.** Look for the same mistake beside it in that part; a defect
   is usually one of several of its kind.
4. **Remove what the fix makes unnecessary** in the same change: a guard an earlier fix
   added, a helper nothing calls, a comment that is no longer true. Prefer making a wrong
   state impossible where it enters (a type, a limit, a refusal when it is written) to
   handling it wherever it could arrive.
5. **Extend a test that guards the invariant** rather than add one beside it. A new test is
   for an invariant, or a case of one, that no test covers, and the table's row names it. A
   test changes only when the promise it states has changed, never to let a fix pass.
6. **Keep it small.** A fix that adds more than about fifty shipped lines, or changes more
   than two parts, is a redesign: stop, and write in the change's description what the
   invariant is and why its owner cannot hold the fix.

The check holds the line where no judgement is needed (`.github/workflows/check.yml`):

- It states the size of the shipped code, rounded up to 500 lines, and the number of tests,
  rounded up to 10. A change that crosses either edits the number in the same change, and
  says why in its description; a change that removes enough lowers it. Neither number ever
  passes 150,000 lines or 1,000 tests.
- It refuses a shipped file longer than 1,500 lines, a test-only name in shipped code, an
  ignored test, and an `allow` attribute beyond the four named below.
- The lint settings above refuse a sleep, a thread, a clock read, a random id and a walk
  over a hash map.

## Do not add

- A second way to do one thing: there is one commit function, one expression evaluator, one `Error`.
- A test-only item in shipped code: no test-only name, feature or `pub` door; tests use what a user has.
- A check another part already makes: `write` checks a table's rules, `memory` says no, `store` only keeps.
- A lint switched off. Outside `redb/` there are four `allow` attributes and they stay the only ones: the machine clock in `contextdb/src/system/mod.rs` (the one place the time of day is read); the timer in `contextdb/examples/support/mod.rs` (measuring elapsed time is what the programs under `contextdb/examples/` are for); and two in `contextdb/tests/support/mod.rs`, one on its one thread (it moves a child's output lines to a channel so that a wait can give up) and `dead_code` for the whole file (each test program uses only some of its helpers).

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
