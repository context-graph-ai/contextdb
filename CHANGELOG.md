# Changelog

Earlier versions: see git tags.

## Unreleased

- **Breaking.** Sync policy and direction are now declared only in table DDL.
  Callers must use `SYNC CONFLICT KEEP FIRST | KEEP LATEST` and
  `SYNC OFF | PUSH ONLY | PULL ONLY | TWO WAY`, then call
  `SyncClient::pull_default`/the ordinary initial-sync path. The public
  in-memory policy maps, policy/direction setters, caller-supplied apply maps,
  and arbitrary-transport constructors were removed; authenticated endpoint
  construction remains the production path and transport injection is a
  test-seam-only capability.
- **Breaking.** Mutable `MemoryAccountant` attachment and raw accounting access
  were removed from the public Database API. Use the durable
  `Database::set_memory_limit` for live configuration. For a non-raisable
  process-start ceiling, use
  `contextdb_engine::database::open_with_startup_limits` or
  `open_memory_with_startup_limit`; the CLI uses those numeric bootstrap
  functions for `--memory-limit`/`--disk-limit`.
- **Behavior change.** Distributed-work blob bytes now live in ContextDB's own
  redb file instead of a separate upstream filesystem store. Authoritative
  `PURGE` can therefore erase selected ledger lineage and unreferenced
  engine-held blob copies in one commit. Success means no ContextDB
  query/history/serve/export/resume/backup path can reach the bytes; it does
  not promise forensic overwrite of filesystem journals, SSD remaps, or OS
  snapshots.
- **Added.** `contextdb snapshot export` publishes a transactionally consistent,
  purge-fenced artifact, and `contextdb inspect key|blob|sync-apply-state` reads bounded durable
  history, lineage, purge-frontier, media-generation, partial-transfer, and
  addressability facts from a private disposable copy. Inspection never opens
  or changes the supplied artifact and exposes no raw media bytes, tag names,
  identity material, storage paths, database handle, or repository capability.
- **Breaking.** Wire protocol version 5 → 6 completes the authenticated
  ordering, schema, conflict-evidence, and purge shape in one bump:
  `WireRowChange` gains `arrival`; `PullResponse` gains the serving-store
  `source`; `WireChangeSet` gains authenticated `ddl_provenance` plus the
  separate purge lane; `WireConflict` gains table, mutation kind, winning
  author identity, and hub acceptance position; and `PushResponse` gains the
  structured authority-refusal error. A peer speaking a different protocol version is
  refused loudly on push, pull, and the status exchange — no rows move and no
  watermark advances on either side — with a message naming the remedy
  (upgrade both ends). A mixed fleet simply stops syncing until every
  participant is on the same release; nothing is lost, because neither side's
  watermark ever advances against a refused exchange.
- **Behavior change (semantic correction).** `LatestWins` conflict resolution
  no longer compares two machines' unrelated write clocks. Previously, the
  arbiter compared the incoming row's own sender-local LSN against whatever
  was already stored — two counters from two different machines' histories,
  which is not a meaningful comparison. A quiet edge's genuinely later write
  could lose forever to a chattier edge's earlier one, and the push watermark
  then stepped over the refused row so it was never re-offered: the fleet
  converged on a stale value permanently and silently. The same defect aged a
  rebuilt node's capability advertisement out of the fleet, since
  `work_capabilities` is `LatestWins` precisely so a re-advertise can refresh
  it. The winner is now the mutation the accepting node took last: an
  incoming row with no established ordering position (freshly authored,
  never before synced) always wins over whatever is already held; a row
  carrying an ordering position at or below the one already stored is a stale
  echo (a no-op, not a conflict); a row carrying a strictly higher ordering
  position wins. Every outcome that changes under this correction is one the
  old code decided by write volume or clock offset — including cases where
  the old answer was "nobody, ever." The push watermark also no longer steps
  over a `LatestWins` refusal it can never win: after this fix, `LatestWins`
  never refuses at all (an incoming row either wins outright or is a silent
  stale echo), so the dead retry-guard that never matched its own producer's
  reason string is removed rather than "fixed" — landing the string match
  instead would have wedged every push against a hub legitimately refusing
  under `ServerWins`.
- **Fixed.** A pull cursor is now bound to the specific store that issued it
  (persisted as one `(source incarnation, lsn)` record). Previously a bare
  `Lsn` watermark carried nothing identifying which store it addressed, so
  pointing an edge at a different store for the same tenant — an operator
  handing it a new endpoint ticket, or a hub wiped and rebuilt under the same
  transport identity — silently skipped that store's history below the old
  cursor number. A page served by a store other than the one the cursor
  addresses is now discarded unapplied (surfaced via
  `SyncClient::pages_discarded_for_source_mismatch`), the cursor resets to
  `Lsn(0)`, and the client fully re-pulls the new store's history — an edge
  repointed at a rebuilt hub converges to the rebuilt hub's state, including
  keys whose old-source recorded position was numerically higher than
  anything the new source has produced yet.
- **Breaking.** `poll_and_execute_once`/`run_worker_loop` take
  `executor: Arc<dyn WorkExecutor>` instead of `&dyn WorkExecutor`. The
  executor call now runs on Tokio's blocking thread pool
  (`tokio::task::spawn_blocking`) instead of synchronously inline on the
  caller's own runtime task, so a sibling future multiplexed onto the same
  task (exactly the shape `tokio::join!` produces) can keep making progress
  while a long-running executor call is in flight — a public `async fn` that
  blocked its caller's runtime task for the whole duration of an arbitrary
  consumer callback was hostile to every consumer that links this crate
  directly as a library (cg, vigil).
  `spawn_blocking` requires a `'static` closure, which is what the owned
  `Arc` is for: a caller that previously held a borrowed `&dyn WorkExecutor`
  now wraps it once in `Arc::new` and clones the handle per poll. The
  in-execution puller (unchanged in purpose — observing a remote
  cancellation or result while a job runs) no longer needs its own nested
  runtime thread; a swallowed in-execution pull failure now surfaces one
  rate-limited warning per attempt instead of being silently indistinguishable
  from "nothing to pull."
- **Added.** A `HISTORY ALL | HISTORY CURRENT ONLY` table option, alongside `RETAIN`: `HISTORY CURRENT ONLY` declares that only a row's current version has consumer value, so the maintenance loop may reclaim superseded versions. Settable at `CREATE TABLE` and with `ALTER TABLE t SET HISTORY ALL | CURRENT ONLY`; renders in `.schema` and `--json` only when declared; travels with the table's definition over sync. Refused together with `IMMUTABLE` (parse time), and refused on any table that both delivers rows to another machine and arbitrates conflicts non-overwriting (`SYNC CONFLICT KEEP FIRST`, the default) — declare `SYNC CONFLICT KEEP LATEST` or `SYNC OFF` instead. The refusal is enforced at `CREATE TABLE`, `ALTER TABLE ... SET HISTORY`, `ALTER TABLE ... SET SYNC CONFLICT`, `ALTER TABLE ... SET SYNC ...`, and on an arriving definition from another machine.
- **Added.** `ALTER TABLE t SET SYNC CONFLICT KEEP FIRST | KEEP LATEST` — a table's conflict policy can now be changed after `CREATE TABLE`, not only declared at creation.
- **Added.** The three built-in currency tables (`work_capabilities`, `peer_directory`, `work_node_contacts`) now declare their own `HISTORY CURRENT ONLY` and conflict/sync policy in their own `CREATE TABLE` text, instead of an internal, undeclared table-name list. An existing data root reconciles its installed tables to the new declarations automatically on next install.
- **Added.** `work_inputs` (the work ledger's per-job input copies) declares `RETAIN 7 DAYS`. `Error::WorkInputExpired { job_id }` is returned by `materialize_inputs` when a job's ledger-carried inputs have aged out under that window, distinct from a job that never declared ledger inputs at all (which reads back as legitimately empty).
- **Changed.** Version cleanup (`Database::compact_currency_versions`) and retention (`Database::run_pruning_cycle`) now remove exactly the row versions, change-log entries, and (for cleanup) attached vector copies being reclaimed in bounded, point-removal redb transactions, instead of rewriting every surviving row, vector, and change-log entry of the database on every pass. Cost is proportional to what is reclaimed, not to the size of the database. The commit index is trimmed candidate-only in the same pass rather than left to grow forever. Cleanup eligibility is now driven by each table's declared `HISTORY CURRENT ONLY`, not a hardcoded table-name list — any table may opt in.
- **Behavior change.** An arriving `AlterTable` (a declaration change received over sync) now starts the maintenance loop on the receiving machine immediately, without waiting for the next process restart — this also fixes a `RETAIN` window arriving on an existing table via sync, which previously would not begin pruning until reopen.
- **Fixed.** The engine's maintenance-cycle receipt line moved from `println!` to a structured `tracing::info!`, so an embedding consumer no longer sees engine text on its own stdout.
- **Fixed.** `contextdb-core::TableMeta`'s hand-written positional decoder now genuinely tolerates an on-disk payload written before `conflict_policy`/`history_policy` existed (decoding the missing fields as undeclared) instead of hard-failing — the fix is scoped to those two newest fields; a payload predating `indexes` (or, for `ColumnDef`, predating `immutable`) still refuses to load, unchanged, since silently defaulting either of those would be a safety regression.
- **Breaking.** `contextdb-tx`'s `TxManager` is renamed `TransactionManager`.
- **Breaking.** `contextdb-server`'s `BlobService` is renamed `BlobStore`, and `WorkerConfig`'s `blob_service` field is renamed `blob_store`.
- **Breaking.** `contextdb_cli`'s `repl` module is now private. Consumers import `run`, `OutputOptions`, and the `EXIT_*` constants from the crate root (`contextdb_cli::{run, OutputOptions, EXIT_OK, EXIT_ERROR, EXIT_USAGE, EXIT_INTERRUPTED_PUSH_UNCONFIRMED}`) instead of `contextdb_cli::repl`.
- **Breaking.** `contextdb_core::Error` gains a new variant, `OrderByExpressionNotSupported`, returned when an `ORDER BY` clause names an expression the engine cannot evaluate — a plain column, a SELECT-list alias, `COALESCE(...)`, and the arithmetic operators (`+`/`-`/`*`/`/`) are all evaluated; anything else keeps the typed refusal. Any exhaustive match on `contextdb_core::Error` needs a new arm.
- **Breaking.** `contextdb_core::Error` is now `#[non_exhaustive]`: an external crate matching on it exhaustively must add a wildcard arm.
- **Behavior change.** Sync apply counts were recontracted: a pulled row whose content the node already holds is a pure no-op counted in none of `applied_rows`/`conflicts`/`skipped_rows`; `conflicts` records only genuine divergence between two machines; a row refused by policy or context scope counts as `skipped_rows` and never produces a conflict record (previously a hidden row's conflict record also disclosed its key). Consumers branching on these counts see fewer spurious conflicts and skips.
- **Behavior change.** A predicate naming a column its table (or join/CTE input) does not declare is now a plan-time `ColumnNotFound` error — in `SELECT`, `DELETE` and `UPDATE` `WHERE` clauses, `JOIN ON` conditions, and `EXPLAIN` — instead of silently treating the column as `NULL` (which returned empty or wrong results, and on `DELETE`/`UPDATE` silently affected every row). This now also covers an explicit qualifier naming a table/alias that is not in scope for the query (`WHERE badalias.col = 1`), and a `SELECT`-list reference to an unknown column (previously a differently-classed `PlanError`).
- **Behavior change.** A `WHERE`/`JOIN ON` predicate that fails to EVALUATE (e.g. negating a non-numeric column) now fails the statement as a genuine error instead of being silently swallowed into "row excluded" — this was previously indistinguishable from a predicate that legitimately matched nothing, and on `DELETE`/`UPDATE` meant a refused write reported `Ok`/`rows_affected: 0` with no signal anything was wrong. NULL-as-false comparison semantics (`WHERE col = 5` excluding a `NULL` row without erroring) are unchanged.
- **Behavior change.** A bare boolean column (`WHERE flag`, standard SQL for `WHERE flag = TRUE`), its negation (`WHERE NOT flag`), a boolean `$param` used directly as a predicate, and a literal `WHERE NULL` are now supported WHERE forms; all four previously fell through the predicate-evaluation swallow above and silently excluded every row.
- **Behavior change.** `ORDER BY` now resolves a SELECT-list output alias to its expression (Postgres/SQLite precedence; an alias shadowing a real column sorts by the alias target), errors on an unknown column, surfaces an ambiguous join reference, evaluates `COALESCE(...)` and arithmetic (`+`/`-`/`*`/`/`) sort keys, and refuses any other unsupported expression sort key with the typed error above instead of silently not sorting.
- **Behavior change.** A multi-column `UNIQUE (col, ...)` violation on `INSERT` is now a loud `Error::UniqueViolation { table, column }` (`column` names every column of the constraint) instead of a silent `Ok`/`rows_affected: 0` no-op — matching the existing composite `PRIMARY KEY` behavior. A single-column `UNIQUE` keeps its existing idempotent no-op convention on a plain `INSERT`; `ON CONFLICT ... DO UPDATE` is unaffected either way.
- **Fixed.** `SCOPE_LABEL_READ (...) WRITE (...)`'s parser located the `WRITE` keyword with a plain substring search, so a SQL comment merely containing the word "write" (e.g. "rewrite", "overwrite") — legal wherever whitespace is legal in this clause — could be mistaken for the keyword, corrupting which labels landed on the read vs. write side or spuriously rejecting valid SQL. The keyword scan now skips comment content entirely.
- Boolean literals (`TRUE`/`FALSE`) are valid predicates everywhere a predicate is legal, including `JOIN ... ON TRUE`.
- **Fixed.** The parser could abort the process on multi-byte UTF-8 near a keyword lookahead; it now parses or rejects, never panics.
- The CLI accepts standard multi-line, semicolon-terminated SQL in both interactive and piped modes (statements end at `;` outside quotes and comments, or at end of piped input), so multi-line schema files and documentation examples run as pasted.
- The manual `workflow_dispatch` trigger was added to CI so the full gate can be run on demand against any branch.
- Test-harness: acceptance and integration suites resolve the spawned CLI/server binary through one shared resolver that picks the most recently built profile (override with `CONTEXTDB_TEST_BIN_PROFILE`), so a stale binary can no longer produce false results.
- CLI exit codes are now one documented four-value table honored by every binary (`docs/cli.md`, "Exit Codes"): `0` success, `1` error, `2` usage, `3` an interrupted push whose outcome the hub never confirmed.
- **Behavior change.** Every error now goes to stderr and fails the run. Runtime errors (constraint violations, immutable-column writes, and the rest of the former "non-fatal" class) previously printed to stdout and left the exit code at `0`; a script branching on `$?` could not see them.
- **Behavior change.** `.sync push`, `pull`, `reconnect`, `destination`, `direction` and `policy` now fail when the CLI was started without `--tenant-id`, instead of printing "Sync not configured" to stdout and exiting `0`. `.sync status` and `.sync auto` still answer and exit `0`.
- **Behavior change.** An invalid flag value (`--memory-limit 12Q`) and an incomplete flag combination (`--tenant-id` with no endpoint) now exit `2` instead of `1`.
- **Behavior change.** The media-transfer demo's `scan-hub` now exits non-zero when it finds the marker it scans for (it printed `hub_marker_found=true` and exited `0`), and its local outcome-deviation code moved from `3` to `1`.
- An interactive REPL session exits `0` even after errors, as `psql` and `sqlite3` do; an unconfirmed push still reports `3`.
- `--json` now covers every meta-command: `.tables`, `.schema` (a structured table description including RETAIN/PROPAGATE/state-machine/sync policy, plus the exact DDL), `.explain`, `.trace`, and the whole `.sync` family. stdout under `--json` is JSON Lines.
- Errors under `--json` are written to stderr as `{"error":{"class":...,"message":...}}`, where `class` is `sql`, `sync`, `io` or `usage`.
- **Fixed.** `.trace on` wrote its per-statement trace line to stdout under `--json`, corrupting the JSON stream. Traces and `.help` now go to stderr under `--json`.
- **Fixed.** `.explain` under `--json` executed the statement it was asked to explain, so `.explain DELETE FROM t` deleted the rows. A statement that would write is now planned without being run, as it already was in human mode; the document reports which of the two happened as `runtime_trace`.
- **Behavior change.** `.schema` and sync status under `--json` now report a
  table's direction and conflict policy only as declared words —
  `sync_off`/`push_only`/`pull_only`/`two_way` and
  `keep_first`/`keep_latest` — instead of leaking Rust enum identifiers. A
  consumer that matched on the old strings must move to the DDL vocabulary.
- The CLI binary's own failures — argument validation, opening the database, and every step of the shutdown sequence — now use the same `{"error":{...}}` envelope under `--json` as errors raised inside the REPL. Warnings that are not failures (an unreachable endpoint or a final push whose outcome is unconfirmed) arrive as `{"notice":{...}}`.

## v1.1.0

- TriggerActiveSameDBProgress: same-DB cross-thread trigger contention now waits-and-proceeds inside the engine instead of surfacing retry churn to callers.
- `CallbackActiveCrossThread { Trigger }` keeps its exact Display string, but its normal trigger scope narrows to captured callback tx-bound handles used from the wrong thread and deadlock-guard timeout paths; unrelated cross-DB writers proceed independently.
- Added the `CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS` override for the bounded same-DB trigger wait guard. Default: 60 seconds; no enforced minimum.
- Deadlock-guard timeouts emit one structured `tracing::warn!` with `trigger_name`, `waited_ms`, and `surface`.
- Class A callback-thread misuse returns `CallbackReentry`; cron same-DB callback contention remains an immediate typed callback-active error.
