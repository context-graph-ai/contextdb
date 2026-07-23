# Changelog

Earlier versions: see git tags.

## Unreleased

- **Breaking.** `contextdb-tx`'s `TxManager` is renamed `TransactionManager`.
- **Breaking.** `contextdb-server`'s `BlobService` is renamed `BlobStore`, and `WorkerConfig`'s `blob_service` field is renamed `blob_store`.
- **Breaking.** `contextdb_cli`'s `repl` module is now private. Consumers import `run`, `OutputOptions`, and the `EXIT_*` constants from the crate root (`contextdb_cli::{run, OutputOptions, EXIT_OK, EXIT_ERROR, EXIT_USAGE, EXIT_INTERRUPTED_PUSH_UNCONFIRMED}`) instead of `contextdb_cli::repl`.
- **Breaking.** `contextdb_core::Error` gains a new variant, `OrderByExpressionNotSupported`, returned when an `ORDER BY` clause names an expression the engine cannot evaluate rather than a column reference or SELECT-list alias. Any exhaustive match on `contextdb_core::Error` needs a new arm.
- **Behavior change.** Sync apply counts were recontracted: a pulled row whose content the node already holds is a pure no-op counted in none of `applied_rows`/`conflicts`/`skipped_rows`; `conflicts` records only genuine divergence between two machines; a row refused by policy or context scope counts as `skipped_rows` and never produces a conflict record (previously a hidden row's conflict record also disclosed its key). Consumers branching on these counts see fewer spurious conflicts and skips.
- **Behavior change.** A predicate naming a column its table (or join/CTE input) does not declare is now a plan-time `ColumnNotFound` error — in `SELECT`, `DELETE` and `UPDATE` `WHERE` clauses, `JOIN ON` conditions, and `EXPLAIN` — instead of silently treating the column as `NULL` (which returned empty or wrong results, and on `DELETE`/`UPDATE` silently affected every row).
- **Behavior change.** `ORDER BY` now resolves a SELECT-list output alias to its expression (Postgres/SQLite precedence; an alias shadowing a real column sorts by the alias target), errors on an unknown column, surfaces an ambiguous join reference, and refuses an expression sort key with the typed error above instead of silently not sorting.
- Boolean literals (`TRUE`/`FALSE`) are valid predicates everywhere a predicate is legal, including `JOIN ... ON TRUE`.
- **Fixed.** The parser could abort the process on multi-byte UTF-8 near a keyword lookahead; it now parses or rejects, never panics.
- The CLI accepts standard multi-line, semicolon-terminated SQL in both interactive and piped modes (statements end at `;` outside quotes and comments, or at end of piped input), so multi-line schema files and documentation examples run as pasted.
- The manual `workflow_dispatch` trigger was added to CI, along with a compile-only check of the deprecated feature-gated broker suites, so the full gate can be run on demand against any branch.
- Test-harness: acceptance and integration suites resolve the spawned CLI/server binary through one shared resolver that picks the most recently built profile (override with `CONTEXTDB_TEST_BIN_PROFILE`), so a stale binary can no longer produce false results.
- CLI exit codes are now one documented four-value table honored by every binary (`docs/cli.md`, "Exit Codes"): `0` success, `1` error, `2` usage, `3` an interrupted push whose outcome the hub never confirmed.
- **Behavior change.** Every error now goes to stderr and fails the run. Runtime errors (constraint violations, immutable-column writes, and the rest of the former "non-fatal" class) previously printed to stdout and left the exit code at `0`; a script branching on `$?` could not see them.
- **Behavior change.** `.sync push`, `pull`, `reconnect`, `destination`, `direction` and `policy` now fail when the CLI was started without `--tenant-id`, instead of printing "Sync not configured" to stdout and exiting `0`. `.sync status` and `.sync auto` still answer and exit `0`.
- **Behavior change.** An invalid flag value (`--memory-limit 12Q`), an incomplete flag combination (`--tenant-id` with no endpoint), and a ticket requested from a broker URL now exit `2` instead of `1`.
- **Behavior change.** The media-transfer demo's `scan-hub` now exits non-zero when it finds the marker it scans for (it printed `hub_marker_found=true` and exited `0`), and its local outcome-deviation code moved from `3` to `1`.
- An interactive REPL session exits `0` even after errors, as `psql` and `sqlite3` do; an unconfirmed push still reports `3`.
- `--json` now covers every meta-command: `.tables`, `.schema` (a structured table description including RETAIN/PROPAGATE/state-machine/sync policy, plus the exact DDL), `.explain`, `.trace`, and the whole `.sync` family. stdout under `--json` is JSON Lines.
- Errors under `--json` are written to stderr as `{"error":{"class":...,"message":...}}`, where `class` is `sql`, `sync`, `io` or `usage`.
- **Fixed.** `.trace on` wrote its per-statement trace line to stdout under `--json`, corrupting the JSON stream. Traces and `.help` now go to stderr under `--json`.
- **Fixed.** `.explain` under `--json` executed the statement it was asked to explain, so `.explain DELETE FROM t` deleted the rows. A statement that would write is now planned without being run, as it already was in human mode; the document reports which of the two happened as `runtime_trace`.
- **Behavior change.** `.schema`, `.sync direction` and `.sync policy` under `--json` now report a table's sync direction and conflict policy as declared words — `sync_off`/`push_only`/`pull_only`/`two_way` and `keep_first`/`keep_latest`/`server_wins`/`edge_wins` — instead of the Rust enum-variant spellings (`Push`, `LatestWins`) they leaked before. A consumer that matched on the old strings must move to the new ones, which are the DDL's own words and no longer change if a type is renamed.
- The CLI binary's own failures — argument validation, opening the database, and every step of the shutdown sequence — now use the same `{"error":{...}}` envelope under `--json` as errors raised inside the REPL. Warnings that are not failures (a deprecated flag, an unreachable endpoint, a final push whose outcome is unconfirmed) arrive as `{"notice":{...}}`.

## v1.1.0

- TriggerActiveSameDBProgress: same-DB cross-thread trigger contention now waits-and-proceeds inside the engine instead of surfacing retry churn to callers.
- `CallbackActiveCrossThread { Trigger }` keeps its exact Display string, but its normal trigger scope narrows to captured callback tx-bound handles used from the wrong thread and deadlock-guard timeout paths; unrelated cross-DB writers proceed independently.
- Added the `CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS` override for the bounded same-DB trigger wait guard. Default: 60 seconds; no enforced minimum.
- Deadlock-guard timeouts emit one structured `tracing::warn!` with `trigger_name`, `waited_ms`, and `surface`.
- Class A callback-thread misuse returns `CallbackReentry`; cron same-DB callback contention remains an immediate typed callback-active error.
