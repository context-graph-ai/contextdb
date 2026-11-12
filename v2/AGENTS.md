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
| `types` | `Value`, the one `Error`, ids, table descriptions | holds behaviour: reads no store, checks no rule of a table, decides no answer; the one refusal it makes is `typed`, which turns away a value that is not of its column's type |
| `system` | the time of day, through `Clock`; the one place in the library that reads a clock | decides anything |
| `memory` | the budget and the reservations taken from it | allocates for another part: it counts what they are about to take and says no first |
| `sql` | grammar, parser, planner: text to a statement, a statement to a plan; reads every table option and column constraint (including `STATE MACHINE`, `PROPAGATE`, `CONTEXT_ID`, `SCOPE_LABEL`, `ACL` and the `ALTER TABLE` forms); `sql::formula` reads a rank formula, the one reading of it | touches storage or knows what a table holds |
| `store` | the folder and data file, how each record is keyed, a row's large text, JSON and vectors kept beside the row in slices (text and JSON compressed there when that saves an eighth, `store/record.rs`), views of one moment, the reader files (one in `readers/` per reader that is inside a statement, naming the commit it reads) and the locks | interprets SQL or a table's rules |
| `schema` | table declarations: checking, storing, printing them back; the `Rules` of a table (references, immutable columns, propagation, the Context, label and access columns, counts) worked out once from its declaration, and `ALTER TABLE` and `DROP` (`schema/alter.rs`) with the refusals of what a change would leave dangling | changes a row |
| `query` | answers a SELECT from one table; holds the one expression evaluator and the one filter of who may see a row (`Reader`, `Visible`, in `query/visible.rs` with its tests in `query/visible/`); a narrowed `Reader` is handed to every read and every search; counts the rows it reads and a search's distance evaluations; answers `SHOW VECTOR_INDEXES`, `SHOW VECTOR_PARTITIONS` and `explain` (which takes the `Reader`) from the vector part's small records | changes anything |
| `write` | applies INSERT, UPDATE and DELETE to a change set; checks types, NOT NULL, the primary key (also the hidden key of a table with none) and UNIQUE rules, `IMMUTABLE`, the state machine, references both ways, a `DAG`, and the rows a `PROPAGATE` moves in the same statement; asks `Visible` whether the handle may write the row; keeps index entries and row counts; checks at commit what only the whole transaction answers (`write/commit.rs`) | returns a query's result to a caller |
| `database` | the handle a store is opened for writing with, the handles narrowed from it (`scoped_with_constraints`), the refusal of a statement by the kind of handle (`Database::execute` refuses what `Plan::admin_only` names when the `Reader` is narrowed; the Rust call `set_memory_limit` makes the same refusal; `run_maintenance_cycle` does not look at the narrowing), its transactions and their numbers, row guards, the one commit function (where two transactions' conflicts are decided: first commit wins, no waiting); `table_meta` and `declaration`; `run_maintenance_cycle` (the sealing commit, then one step for every partition with work, each worked out with no lock held and committed through the one commit function); `explain` | has a second way to commit |
| `read` | a session that reads a store without holding it for writing, from any process; its `explain` | changes anything a writer stored |
| `vector` | the blocks of a VECTOR column (a one-byte copy of every vector), the lists of vectors not yet in a block, the column's partitions and their count, the grouping of a partition (its centres, a one-bit sketch of every slot, the partition's mean), search over them (every slot, or the sketches of the nearest groups and of the tail, then the slots those keep), building a grouping in steps and putting it in force, sealing, and what inspection reads | decides which rows a reader may see: `query` tells it, row by row, and a row is in an answer only with the distance `query` worked out from the row's stored vector; reads a clock, takes the commit lock, or starts a thread |
| `upkeep` | the sealing of one cycle, at most 4,096 waiting vectors moved into blocks in one commit (the 4,096 is `SEAL_ROWS` in `upkeep/mod.rs`), and the report of a cycle; `database` drives the grouping steps that follow it | starts a thread, reads a clock, or runs unless called |

The files of `vector/`, each as its opening comment says:

- `slot.rs`: one vector as a block keeps it (a scale, a rounding error and a signed byte for each number), the range a search gets from it, and the distance every answer is ordered by; `Reduced`, the one place that turns a vector into one byte a number for a column declared `SQ8`.
- `block.rs`: the slots of as many vectors of one partition as fit in the block's bytes, each with the key of its row.
- `directory.rs`: the partitions of a VECTOR column, one record for each key tuple under which a row has a vector.
- `grouping.rs`: the grouping of a partition (its slots sorted by the centre each is nearest, packed into full blocks, with the centres, the place where each group begins and a sketch of every slot) and the partition's mean; written once by the build that made it and never changed.
- `sketch.rs`: one vector as a sketch keeps it (a bit for each number and a factor, taken from a reference: the centre of its group, or the partition's mean for a tail block), and the range a search gets from a sketch; the counting loop.
- `train.rs`: finding centres, the arithmetic of a build over slots alone; nothing there reads a store.
- `build.rs`: when a grouping is due, the record of how far a build has come, and the steps that find centres and say which group each slot goes to.
- `pack.rs`: the last steps of a build: packing the slots of each group into the blocks of the new grouping with a sketch of each, measuring how far a search scans and how wide a sketch's estimate is taken to be, and putting the grouping in force.
- `search.rs`: the one search, and the plan it runs: scores the rows in no block exactly, screens the slots of the selected partitions (every slot of a partition searched whole; of one searched by its nearest groups, the slots its sketches keep), asks for the true distance of the rows that can reach the answer, and proves the answer.
- `seal.rs`: moving the vectors on the waiting lists into blocks.
- `stage.rs`: what a commit's row changes mean for the vector part, worked out once inside the commit function.
- `tally.rs`: the partitions a transaction's statements have made and emptied, so that the statement that would give a column one partition too many is refused when it runs.
- `inspect.rs`: what inspection reads of a VECTOR column, its partitions as their records describe them; no block, no centre and no row.

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
| `Catalog`, `Table`, `Rules` | `schema` | `query`, `write`, `database` and `read` look tables up in it; a table's references, immutable columns, propagation edges and its Context, label and access columns are fields of `Table::rules`, the other options are in `Table::meta` |
| `Reader`, `Visible` | `query` | `database` and `read` make a `Reader` for a handle (what it is narrowed to); `write` and `query` ask `Visible` before any row is shown or staged |
| `eval`, `rows_for_write` | `query` | `write` finds the rows of an UPDATE or DELETE with `query::rows_for_write`, a batch at a time, and works out values with `query::eval` |
| `Asked`, `EvalContext`, `reads`; `Visible::new`, `Visible::reads`, `Visible::may_write`, `Reader::admin` | `query` | `write` is also handed these to evaluate and to ask who may write; from `store` it uses `Beside`, `Entry`, `EntryShape`, `KEY_LIMIT`, `RowGuard`, `View` and `CountId`, and from `schema` the fields of `Table` |
| `QueryResult` | `query` | `database` and `read` build it and hand it to the caller |
| `PhysicalPlan::VectorSearch`; `Expr::Distance`, `Expr::Row`, `Expr::InList` | `sql` | `query` runs the search, puts its distance wherever the select list writes `<=>`, and evaluates `IN` lists and row values |
| `VectorColumn` (a VECTOR column's number, partition key, limit, mode, quantization, `AUTO_INDEX_AT` and consolidation, with the numbers in force), from `Table::vectors`, `Table::vector` and `Catalog::vector` | `schema` | `query`, `write` and `vector` |
| `staged`, `vectors` and `vector_number` on `Read`; `put_vector`, `delete_vector` and `put_vector_number` on `ChangeSet`; `VectorRecord`, `VectorNumber`, `VectorId` | `store` | `vector` reads and stages its blocks, waiting entries, partition entries and counts through them; `store` keeps them and never interprets one |
| `search`, `plan`, `Nearest`, `Route`, `distance` | `vector` | `query` calls `search` with the partitions the filter selects, the mode in force, the column's declaration and whether the statement has a filter on rows, and answers it, row by row, with `distance` of the row's stored vector, or with nothing for a row that is gone, filtered out or has no vector; `search` answers nothing at all only when INDEXED is refused (a selected partition is not usable, or a filter on rows leaves the index nothing to choose among), which `query` turns into the refusal. `plan` settles what a search does with each partition, from small records alone, and `search` runs exactly that plan, so `explain` cannot disagree with it |
| `Tally`, the partitions a transaction has started and emptied | `vector` | `database` holds one beside each change set and forgets a refused statement's part of it; `write` notes every row it stages, including the rows a propagation moves, and settles the tally once per statement after following, which is where a statement over `MAX_PARTITIONS` is refused; `vector::tally::tuples` decides which rows have an entry (none for a row in an `EXCLUDE VECTOR` state, with a NULL vector, or missing) |
| `stage`, `seal` | `vector` | `database` calls `stage` inside the one commit function, after the commit's other rules have passed; `upkeep` calls `seal` |
| `waiting`, `prepare`, `stage_step`, `stage_refusal`, `remaining` (and the values they return) | `vector` | `database` lists the partitions with work, has a step worked out with no lock held, and stages it through the one commit function; a step the memory limit refuses is recorded, not returned |
| `partitions`, `Inspected`, `State` | `vector` | `query` answers the two SHOW statements and `explain` with them |
| `Reduced` | `vector` | `write` gives a column declared `SQ8` the vector `Reduced` makes of the one written, and hands the store its scale and codes with the row (`store::Compact`) |
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
| The stored bytes of a large value, a vector record or a table's vector declaration that disagree with themselves are reported as damage, never a panic or another value | `contextdb/src/store/tests.rs`: `a_plain_text_whose_slices_hold_fewer_bytes_than_its_row_records_is_damage`, `a_compressed_text_that_decodes_to_bytes_that_are_not_text_is_damage`; `contextdb/src/vector/tests.rs`: `a_pending_record_of_a_row_with_no_vector_is_reported_as_damage`, `a_write_to_a_vector_column_that_is_no_column_of_its_table_is_reported_as_damage`; a grouping, centres, a build, assigned groups, a sketch record or a partition entry that disagrees with itself or with its partition, likewise: `a_grouping_that_disagrees_with_its_partition_is_damage`, `centres_that_do_not_come_to_their_groups_are_damage`, `a_build_whose_cursor_is_past_its_blocks_is_damage`, `assigned_groups_that_do_not_match_their_block_are_damage`, `a_sketch_record_that_does_not_match_its_block_is_damage`, `a_partition_entry_that_disagrees_with_itself_is_damage`; a reduced vector's record of another length: `contextdb/src/store/tests.rs`: `a_reduced_record_of_another_length_is_damage` |
| Counted work: a count reads no rows, a key lookup reads one | `contextdb/tests/counted_work.rs`: `count_of_a_whole_table_reads_no_rows`, `a_lookup_by_primary_key_reads_one_row` |
| Every row with a vector is found by an exact search, whether its vector waits on a list, is in a block written since the last grouping or is in the partition's grouping once and not as a stale slot, and after a kill inside a commit, a seal or a build | `contextdb/tests/vectors.rs`: `exact_search_returns_the_true_nearest_vectors`; `contextdb/tests/vectors_upkeep.rs`: `sealing_advances_only_when_asked_and_in_bounded_cycles`, `a_grouping_holds_every_live_vector_once_and_no_stale_slot`, `vectors_sealed_while_a_first_build_starts_are_found_after_the_switch`; `contextdb/tests/crash/main.rs`: `a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together`, `a_kill_at_any_storage_call_of_a_seal_leaves_every_vector_found`, `a_kill_at_any_storage_call_of_a_build_leaves_every_vector_found`, `a_killed_build_goes_on_from_its_record` |
| A row whose vector is NULL, and a deleted row, is never in an answer | `contextdb/tests/vectors.rs`: `exact_search_returns_the_true_nearest_vectors`, `exact_search_reads_its_own_snapshot` |
| A partition exists exactly while a row with a vector is in it, and `MAX_PARTITIONS` counts those and nothing else | `contextdb/tests/vectors_partitions.rs`: `vector_partition_limit_counts_and_refuses`, `the_partition_cap_counts_live_partitions_while_a_reader_holds_an_earlier_state` |
| No answer holds a row twice, whatever the row's vector has been | `contextdb/tests/vectors.rs`: `a_row_is_in_an_answer_once_after_its_vector_changes` |
| A search under INDEXED or AUTO returns only rows an exact search of the same statement could return, each with its true distance and none twice; it is never answered short: it returns `LIMIT` rows whenever the selected partitions hold that many rows the filter passes | `contextdb/tests/vectors_index.rs`: `indexed_is_served_at_every_size`, `a_search_that_widens_returns_no_row_twice`, `a_nearer_allowed_row_in_a_group_taken_later_is_in_the_answer`, `a_search_with_a_filter_on_rows_widens_until_it_is_whole` |
| A partition with no grouping, a grouping of one group, or sketches that cannot tell its vectors apart is searched as an exact search searches it; below its size a column pays for no index | `contextdb/tests/vectors_index.rs`: `a_column_below_its_size_pays_nothing_and_is_searched_whole`, `a_grouping_whose_sketches_cannot_tell_its_vectors_apart_answers_as_exact` |
| On the fixed vectors of the tests an indexed search counts fewer distance evaluations than an exact one (on other data it can count more, each an eighth of the bytes), reads sketches before blocks, and never costs more row visits than an exact one | `contextdb/tests/vectors_index.rs`: `an_indexed_search_does_fewer_distance_evaluations_than_exact`, `an_indexed_search_reads_sketches_before_blocks`, `an_indexed_search_that_takes_every_group_does_the_exact_evaluations` |
| INDEXED is refused by name, before the filter would be asked of the whole table, when a partition is far behind its writes or a filter leaves the index nothing to choose among; AUTO is never refused for a grouping and answers exactly where one is behind or damaged | `contextdb/tests/vectors_index.rs`: `indexed_is_refused_whole_when_one_partition_is_behind`, `an_indexed_search_short_of_its_limit_is_refused_before_the_whole_table_is_read`, `an_indexed_search_whose_first_pass_takes_every_slot_is_refused_before_a_row_is_asked`, `auto_answers_exactly_where_a_grouping_is_behind_or_damaged`; `contextdb/src/vector/tests.rs`: `auto_answers_exactly_where_a_grouping_is_damaged` |
| AUTO is exact until the selected partitions hold `AUTO_INDEX_AT` vectors, then indexed in each partition that has a grouping and is not behind | `contextdb/tests/vectors_index.rs`: `auto_uses_groupings_from_auto_index_at_on`, `auto_searches_each_partition_by_what_its_grouping_saves` |
| A write after a grouping is found at once, before any cycle and after sealing | `contextdb/tests/vectors_index.rs`: `writes_after_a_grouping_are_found_at_once` |
| A grouping is built in bounded cycles and switched in by one commit; the old grouping serves until then, and a write or search runs between any two cycles | `contextdb/tests/vectors_upkeep.rs`: `a_grouping_is_built_in_bounded_cycles_and_switched_in_one`; `contextdb/tests/transactions.rs`: `a_statement_commits_while_a_cycle_works_out_a_step` |
| Every block of a grouping, and every block sealed after the partition's mean exists, has a sketch for each slot; the index takes the bytes the layout says | `contextdb/tests/vectors_upkeep.rs`: `a_grouping_keeps_a_sketch_of_every_slot_and_measures_its_width`, `sealing_after_a_grouping_keeps_a_sketch_of_every_slot` |
| A partition is grouped again only when its changes reach the declared share | `contextdb/tests/vectors_upkeep.rs`: `a_partition_is_grouped_again_only_when_its_changes_reach_the_declared_share` |
| A partition that loses its last vector leaves no record of its grouping or its build | `contextdb/src/vector/tests.rs`: `a_partition_that_empties_leaves_no_record_of_its_grouping_or_its_build` |
| A statement reads one grouping whole, the one in force at its commit, in another process too; a new process searches a built grouping at once, and a grouping is there after reopen | `contextdb/tests/readers.rs`: `a_reader_sees_one_grouping_whole_while_the_writer_builds_and_switches`, `a_new_process_searches_a_built_grouping_at_once`; `contextdb/tests/store.rs`: `a_grouping_is_there_after_close_and_reopen` |
| A build or an indexed search under the memory limit stays under it; a step the limit refuses is recorded as a stall and the old grouping serves; a commit with no room for its vector records changes nothing | `contextdb/tests/memory_ceiling.rs`: `an_indexed_search_and_a_build_stay_under_the_limit`; `contextdb/tests/vectors_upkeep.rs`: `a_cycle_under_a_tight_limit_records_the_stall_and_the_old_grouping_serves`; `contextdb/tests/vectors_memory.rs`: `a_commit_with_no_room_for_its_vector_records_changes_nothing`; `contextdb/tests/vectors_index.rs`: `a_search_with_no_room_for_its_sketches_is_refused_by_the_memory_limit` |
| Inspection and `explain` read no block and change nothing; each partition is described as its records say | `contextdb/tests/vectors_inspection.rs`: `inspection_and_explain_read_no_block_and_change_nothing`, `show_vector_indexes_describes_every_vector_column`, `show_vector_partitions_lists_partitions_by_key_with_limit_and_offset`, `explain_names_how_each_partition_is_searched` |
| A column declared `SQ8` keeps the vector rounded to one byte a number, reads it back number for number, ranks by it, and is grouped and searched as any other | `contextdb/tests/vectors_reduced.rs`: `a_reduced_column_keeps_the_nearest_one_byte_vector`, `exact_on_a_reduced_column_ranks_by_the_kept_vectors`, `a_reduced_column_is_grouped_and_searched_as_any_other`; `contextdb/src/store/tests.rs`: `a_reduced_vector_reads_back_number_for_number_in_a_quarter_of_the_room` |
| A store's data file is no longer than a recorded number of bytes for a fixed content | `contextdb/tests/vectors_upkeep.rs`: `a_small_fixed_store_stays_within_its_recorded_size` |
| A text or JSON value reads back byte for byte whether or not it is stored compressed; one that does not shrink by an eighth takes the room it takes uncompressed, and text that repeats takes less than its own bytes | `contextdb/tests/large_values.rs`: `large_text_and_json_come_back_byte_for_byte`, `a_value_that_does_not_shrink_is_kept_as_it_is`, `a_store_of_repeating_text_is_smaller_than_the_text` |
| No value is produced past the memory limit: a compressed value whose real size does not fit is refused before its bytes exist, and compression never refuses a commit | `contextdb/tests/large_values.rs`: `a_compressed_value_too_large_for_the_limit_is_refused`, `a_commit_with_no_room_to_compress_is_written_plain`; `contextdb/tests/memory_ceiling.rs`: `a_refused_compressed_value_is_never_produced` |
| A compressed value whose stored bytes do not give the length it records is reported as store damage | `contextdb/tests/large_values.rs`: `a_damaged_length_of_a_compressed_value_is_reported_as_damage` |
| A table's rules hold for every write: `IMMUTABLE` rows and columns are never changed, a state moves only along its declared arrows, a row is never left under a parent that is gone, a `DAG` never closes a cycle | `contextdb/tests/rules.rs`: `an_immutable_refusal_names_the_table_or_the_column_and_changes_nothing`, `a_state_refusal_names_the_table_the_column_and_both_states`, `a_reference_is_checked_when_its_transaction_commits_and_a_refused_commit_leaves_nothing`, `a_parent_is_not_removed_or_given_another_key_under_a_row_that_names_it`, `a_cycle_is_refused_by_the_statement_and_by_the_commit_that_would_close_it`, `a_table_another_table_references_is_not_dropped_until_the_reference_is_gone` |
| Rows follow a change of another row inside its statement, are seen and undone with its transaction, are bounded by `MAX DEPTH`, and a follower the handle may not write refuses the statement | `contextdb/tests/propagation.rs`: `rows_follow_inside_the_statement_and_are_seen_and_undone_with_its_transaction`, `an_aborting_propagation_names_its_table_and_undoes_its_statement_and_no_other`, `max_depth_reaches_rows_at_most_that_many_edges_from_the_changed_row`, `a_follower_the_handle_may_not_write_refuses_the_statement_by_kind_and_nothing_changes` |
| Of two transactions that take one key or change one row, exactly one commits; a transaction either declares or writes | `contextdb/tests/transactions.rs`: `of_two_transactions_inserting_one_key_exactly_one_commits`, `of_two_transactions_changing_one_row_the_first_commits_and_the_second_is_refused`, `a_transaction_that_declares_a_table_may_not_also_write_rows`, `a_transaction_sees_its_own_writes_and_a_dropped_or_rolled_back_one_leaves_nothing` |
| A `TXID` column holds its transaction's number, and a commit is never numbered below it | `contextdb/tests/transaction_numbers.rs`: `a_txid_column_holds_its_transactions_number_and_a_commit_is_never_numbered_below_it`, `a_stamp_above_the_transactions_own_number_and_a_stamp_written_as_a_number_are_refused` |
| A row guard is true only for a row that is visible and holds every value, and a commit is refused when a guarded value changed meanwhile | `contextdb/tests/row_guards.rs`: `a_row_guard_answers_true_only_for_a_row_that_is_there_and_holds_every_value`, `a_row_guard_answers_false_for_a_row_hidden_from_its_handle`, `a_commit_is_refused_when_a_guarded_value_was_changed_meanwhile_and_writes_nothing`, `a_guarded_row_is_not_a_changed_row` |
| A narrowed handle sees no row outside its Contexts, labels and grants: a list leaves a hidden row out, a row named by key is refused by the rule that hid it, a refusal carries no value or number of the row, an update or delete changes no hidden row | `contextdb/tests/visibility.rs`: `a_hidden_row_named_whole_is_refused_and_a_list_read_leaves_it_out`, `a_row_of_another_context_named_by_key_is_a_context_refusal_and_a_list_read_leaves_it_out`, `a_row_under_another_label_named_by_key_is_a_label_refusal_and_a_list_read_leaves_it_out`, `a_row_with_no_grant_named_by_key_is_an_access_refusal_and_a_list_read_leaves_it_out`, `an_update_or_a_delete_changes_no_row_the_handle_may_not_see`, `no_refusal_holds_a_value_or_a_number_of_a_row_the_handle_may_not_see`; `contextdb/tests/visibility_grants.rs`: `a_grant_row_the_reader_may_not_see_gives_no_access`; `contextdb/tests/table_source.rs`: `a_narrowed_reader_naming_a_hidden_row_by_its_key_is_refused_by_the_rule_that_hid_it` |
| A narrowed handle writes no row outside what it may see, is refused every declaration and setting, and a handle made from it sees no more | `contextdb/tests/visibility.rs`: `a_new_or_changed_row_outside_the_handles_contexts_is_refused_and_nothing_is_written`, `a_narrowed_handle_is_refused_every_declaration_and_setting_and_changes_nothing`, `a_handle_made_from_a_narrowed_handle_sees_no_more_than_it`, `a_narrowed_handle_with_no_principal_that_holds_grants_is_refused_an_access_controlled_table`, `a_simple_label_is_checked_when_a_row_is_written_and_hides_no_row`, `a_label_with_a_read_side_hides_rows_and_is_checked_when_a_row_is_written` |
| A search and a count on a narrowed handle count and return only rows it may see | `contextdb/tests/visibility.rs`: `a_search_returns_the_nearest_rows_the_handle_may_see_and_no_hidden_row`; `contextdb/tests/kept_counts_narrowed.rs`: `a_handle_that_may_not_see_every_row_counts_the_rows_it_sees_by_reading_them`; `contextdb/tests/kept_counts.rs`: `a_kept_count_is_the_number_of_rows_after_every_kind_of_write_and_reads_no_row` |
| `ALTER TABLE` leaves every other column's values where they were, an added column reads its default from older rows, a dropped column's bytes leave the store, a refused change names what depends and changes nothing, a table another table references is not dropped | `contextdb/tests/alter.rs`: `dropping_a_column_never_shifts_what_the_other_columns_hold`, `an_added_column_reads_its_default_from_a_row_written_before_it`, `a_dropped_columns_bytes_leave_the_store`, `each_dependent_refusal_names_what_depends_and_changes_nothing`, `an_alter_or_a_drop_in_a_transaction_takes_hold_when_it_commits`, `an_altered_table_is_printed_as_what_it_now_declares` |
| A declaration is printed back as text that declares the same table again; a refused declaration says why and declares nothing | `contextdb/tests/declarations.rs`: `every_clause_a_table_may_declare_is_printed_back_and_declares_the_same_table_again`, `a_refused_declaration_says_why_and_declares_nothing`, `a_rank_policy_finds_the_column_it_joins_by_one_rule_or_is_refused`; `contextdb/tests/table_meta.rs`: `table_meta_gives_each_tables_rules` |
| A key too long to store is refused by name, and a second `CREATE TABLE` of a name changes nothing | `contextdb/tests/long_keys.rs`: `a_key_of_2704_bytes_is_stored_and_one_of_2705_is_refused_by_name`; `contextdb/tests/refusals.rs`: `a_second_create_table_names_the_table_and_changes_nothing`, `a_unique_refusal_names_the_table_and_every_column_of_the_rule` |

## Where a change lives

| Change | Folder | Test it needs |
|---|---|---|
| A new SQL statement or clause | `contextdb/src/sql/` (`grammar.pest`, `parser/`, the `Statement` type in `ast.rs`, `plan.rs`, `planner.rs`; take its first words out of the grammar's list of what a later version reads), then the part that runs it: `schema`, `write` or `query` | a script under `contextdb/tests/sql/` |
| A new vector clause | `contextdb/src/sql/` reads it, as any clause; `contextdb/src/schema/` checks, stores and prints back one that is declared with the column; `contextdb/src/query/search.rs` settles what a search is asked (mode, vector, partitions, filter); `contextdb/src/vector/` only when blocks, partitions or the search itself change | `contextdb/tests/vectors_statements.rs` for what is read and refused, `contextdb/tests/vectors.rs` for the rows a search returns |
| When or how a partition is grouped (the size at which a column is grouped, the share of change that groups it again, the steps or the numbers a build measures) | `contextdb/src/vector/build.rs` (`due`, `usable`, the steps), `contextdb/src/vector/pack.rs` (packing and measuring), `contextdb/src/vector/train.rs` (centres); the numbers in force from `VectorColumn` in `contextdb/src/schema/`. A new record for a grouping is a `VectorRecord` kind in `contextdb/src/store/`, which keeps it and never reads it. A change that moves what a build measures changes how a search uses it in `contextdb/src/vector/search.rs`, in the same change | `contextdb/tests/vectors_upkeep.rs` for what a build does and when, `contextdb/tests/vectors_index.rs` for what a search returns and does, `contextdb/tests/crash/main.rs` for a kill inside a build; the cycle counts and sizes the tests record change only with the design they state |
| A new table option or column constraint (a declaration) | `contextdb/src/sql/` reads it; `contextdb/src/schema/` checks it (`check.rs`), works it into the table's `Rules` or `meta` once (`rules.rs`) and prints it back (`render.rs`); an option nothing acts on yet stays in `meta` | `contextdb/tests/declarations.rs` for the round trip and the refusals, a script under `contextdb/tests/sql/` |
| A write rule (what an INSERT, UPDATE or DELETE must obey) | `contextdb/src/write/`; a rule only the whole transaction answers is checked in `write/commit.rs`, at commit; a rule about a key, a `UNIQUE` value or a cycle is checked in both places (`write/mod.rs` and `write/commit.rs`, the second for a transaction another commit overtook), so a new rule of that kind is added in both; the refusal is a kind of `Error` | `contextdb/tests/rules.rs` (or `propagation.rs`), and `contextdb/tests/transactions.rs` when the rule is judged at commit |
| A visibility rule (who may see or write a row) | `Visible` in `contextdb/src/query/visible.rs`, the one filter, asked by `query` and `write`; the handle's narrowing is made in `contextdb/src/database/`; a rule answered anywhere else is a second path | `contextdb/tests/visibility.rs`, and a test of each refusal's sentence carrying no value of a hidden row |
| A change to `ALTER TABLE` | `contextdb/src/schema/alter.rs`; what dangling it would leave is refused there, by name | `contextdb/tests/alter.rs`, `contextdb/tests/sql/alter_table.sql` |
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
- The check holds every test but slow work. Slow work is listed in the `nightly` profile of `.config/nextest.toml` and runs in `Check nightly`. A test that nears its limit on a hosted runner does less work or, when the size is the promise, moves there.

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
