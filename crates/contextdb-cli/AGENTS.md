# contextdb-cli — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). The user-facing contract this crate
implements is [`docs/cli.md`](../../docs/cli.md); when the binary and that page disagree, one of
them is a bug.

**Purpose:** the `contextdb` binary — a bounded read session by default, a write session with
`--write`, and the store-maintenance subcommands — rendered for a person or, under `--json`, as
JSON Lines.

## Owns / must not own

| Module | Owns |
|---|---|
| `main.rs` | Argument parsing (clap), the `Operation` subcommand tree, opening the store or the `ReadSession`, startup/shutdown errors and the process exit code. |
| `command_registry.rs` | The one table of meta-command spellings, aliases and their store effect; `--help`, `.help` and pre-dispatch write authorization all read it. |
| `repl.rs` | `Session`, `run`, `feed_line`, statement framing (`;` outside quotes and comments), meta-commands, cursors, Ctrl-C handling. |
| `ops.rs` | `reset`, `diagnose`, `snapshot`, `inspect`, `purge`. |
| `json_output.rs` / `formatter.rs` | `--json` documents and `ErrorClass` (`#[non_exhaustive]`); human tables. |
| `sync_status.rs` / `auto_sync.rs` | `.sync status` wording in transport-neutral endpoint language; background sync. |
| `testing.rs` | Test seam, compiled only under the `test-seams` feature. |

Must not own: read routes, execution, limits or refusals (engine and `contextdb-core::read_contract`
— the CLI renders them); SQL grammar (parser); the exit-code table (defined once in
`contextdb_server::exit_codes`, re-exported here); sync wire behavior (engine). A file-backed store
never gets CLI-registered callbacks — events queue durably for the owning program.

## Seams

Below: `contextdb-engine` (`Database`, `ReadSession`, `DatabaseOpenOptions`), `contextdb-server`
(sync client, exit codes), `contextdb-parser` (statement parsing and effect classification),
`contextdb-core`. Above: operators and scripts. Library exports: `run`, `Session`, `OutputOptions`,
`ErrorClass`, `EXIT_*`, `canonical_help_signatures`, `operational_command_discovery`.

## Invariants and their guards

| Invariant | Guard |
|---|---|
| A bare path on a missing store is refused and creates nothing | `tests/read_cli_journeys_invocation.rs::bare_path_on_a_missing_store_refuses_and_creates_nothing` |
| A read session refuses every mutating statement and writer-only meta-command before running it | `tests/read_cli_journeys_invocation.rs::a_reading_session_refuses_every_mutating_statement_before_it_executes`, `::writer_only_meta_commands_are_refused_in_a_reading_session` |
| Reading an idle store leaves every byte and the folder listing unchanged | `tests/read_cli_journeys_invocation.rs::reading_an_idle_store_leaves_every_byte_and_the_folder_listing_unchanged` |
| A read of an owned store goes through the live owner, whose ceiling the caller cannot raise | `tests/read_cli_journeys_live_owner.rs::a_reading_session_routes_through_the_live_owner_and_says_so_once`, `::the_owners_ceiling_applies_and_a_caller_cannot_raise_it` |
| A result is complete or refused, never truncated | `tests/read_cli_journeys_ordinary_results.rs::one_row_past_the_ceiling_publishes_nothing_and_names_the_ceiling` |
| `--json` stdout is results only; every error and notice is one JSON document on stderr | `tests/read_cli_journeys_machine_surface.rs::stdout_carries_results_only_while_notices_traces_and_help_go_to_stderr`, `tests/json_stderr_purity_tests.rs::json_stderr_pure_across_notices_and_an_error` |
| An unavailable reader-coordination root is an I/O refusal, while channel startup failure leaves the writable store open | `src/json_output.rs::tests::error_class_routes_sync_io_and_sql`, `tests/read_cli_journeys_machine_surface.rs::owner_status_reports_its_own_exit_when_the_owner_cannot_serve` |
| Exit codes are 0 ok / 1 error / 2 usage / 3 unconfirmed push, and `docs/cli.md` lists exactly those | `tests/exit_code_contract.rs::run_continues_after_an_error_and_still_exits_one`, `tests/docs_exit_code_table.rs::docs_exit_code_table_matches_the_constants` |
| Memory and disk refusals leave the script running and recovery commands usable; a later success does not clear exit 1 | `tests/read_cli_journeys_script_continuation.rs::a_memory_refusal_is_followed_by_independent_work_and_recovery`, `::a_disk_refusal_is_followed_by_independent_work_and_recovery` |
| A usage error runs nothing and creates no store | `tests/usage_error_no_side_effects_tests.rs::usage_error_before_sync_endpoint_check_does_not_create_the_database` |
| `.explain` never applies a write | `tests/explain_non_execution_tests.rs::explain_json_delete_leaves_rows_intact` |
| A `;` inside a quoted string or comment is not a terminator | `tests/multiline_statement_tests.rs::quoted_semicolon_survives_when_the_statement_itself_spans_multiple_lines`, `tests/comment_remainder_tests.rs::trailing_line_comment_at_eof_exits_cleanly` |
| Ctrl-C cancels the statement, keeps the session and its cursor | `tests/read_cli_journeys_cancellation.rs::an_open_cursor_survives_the_interruption` |
| `.verify history <table>` checks historical indexes through the read session; dispatch and help stay in the command registry and REPL | `src/repl.rs` and `src/command_registry.rs`; guard `tests/read_cli_journeys_metadata.rs::verify_history_checks_each_historical_index_from_a_read_session` |
| Spellings come from one registry; removed spellings are refused by name | `src/command_registry.rs::contract_tests::generated_discovery_is_the_exact_filtered_canonical_registry`, `tests/removed_spellings_are_refused_by_name.rs::the_removed_repair_command_is_refused_by_name_and_points_at_diagnose` |
| `reset` and `purge` refuse without `--force`; `reset` never replaces a live owner's store | `tests/reset_repair_tests.rs::reset_without_force_uses_the_usage_exit_code`, `tests/purge_cli_verb_tests.rs::purge_without_force_refuses_and_leaves_data_untouched`, `tests/reset_live_owner_tests.rs::reset_force_refuses_a_live_owner_without_unlinking_its_store_or_lock` |
| A corrupt store yields a handled error naming `diagnose` and `reset`, never a panic | `tests/corrupt_store_open_tests.rs::opening_a_corrupt_store_error_names_the_diagnose_and_reset_commands` |
| `SHOW VECTOR_PARTITIONS` uses the ordinary bounded result and cursor | `tests/vector_partition_show_cursor_contract.rs::oversized_vector_inspection_refuses_then_the_ordinary_cursor_pages_every_row` |
| `.schema --json` speaks declared-policy words, never Rust `Debug` spellings | `tests/json_schema_sync_vocabulary_tests.rs::json_schema_emits_both_vocabularies_together` |

## Where a change lives

| Change | Where it lands |
|---|---|
| New meta-command | A row in `command_registry.rs` with its effect, handler in `repl.rs`, `--json` shape in `json_output.rs`, a line in `docs/cli.md`. |
| New store subcommand | `Operation` variant in `main.rs`, implementation in `ops.rs`. |
| New flag | clap field in `main.rs`; a bad value is exit 2 before any store is opened. |
| New error a script branches on | `ErrorClass` in `json_output.rs`, routed from the engine's typed error. |
| Continue a script after refusal, retain its failed exit status | `repl.rs` (`run`, `feed_line`); engine `database.rs` owns allowing limit inspection and adjustment when exhausted. |
| A read limit or refusal | Engine / core — the CLI only surfaces it. |

## Fast test

```bash
cargo test -p contextdb-cli   # most tests drive the built binary end to end
```
