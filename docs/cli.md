# CLI Reference

contextdb is primarily used as an embedded Rust library. The CLI is for exploration, debugging,
and scripting against a database file.

Two binaries: `contextdb` (interactive client) and `contextdb-server` (sync coordinator).

---

## CLI Client (`contextdb`)

```
contextdb <PATH> [OPTIONS]
```

`<PATH>` is the database file. Use `:memory:` for an in-memory (ephemeral) database.

**Reading is the default.** `contextdb <PATH>` opens an existing store for bounded, read-only
inspection: it never creates the store, never mutates it, and leaves every byte in the store
folder unchanged. <!-- enforced by: read_cli_journeys_invocation::reading_an_idle_store_leaves_every_byte_and_the_folder_listing_unchanged, read_cli_journeys_invocation::bare_path_on_a_missing_store_refuses_and_creates_nothing --> Anything that would write — creating a
store, DML, DDL, transactions, maintenance, sync operations — takes the explicit `--write`
flag; a read session refuses it before execution with `write_requires_flag`.
<!-- enforced by: read_cli_journeys_invocation::a_reading_session_refuses_every_mutating_statement_before_it_executes, read_cli_journeys_invocation::writer_only_meta_commands_are_refused_in_a_reading_session -->

The same command works whether the store is idle or already owned by a live process:

| Situation | Command | Outcome |
|---|---|---|
| Idle existing store | `contextdb <path>` | Direct read-only file session; several direct readers may coexist. <!-- enforced by: read_cli_journeys_invocation::reading_an_idle_store_leaves_every_byte_and_the_folder_listing_unchanged --> |
| Missing store | `contextdb <path>` | Refused without creating anything (`store_not_found`); `--write` creates it. <!-- enforced by: read_cli_journeys_invocation::bare_path_on_a_missing_store_refuses_and_creates_nothing, read_cli_journeys_invocation::write_flag_creates_the_store_a_read_refused_to_create --> |
| Idle or missing store | `contextdb <path> --write` | Full read-write session; the store is created if missing. <!-- enforced by: read_cli_journeys_invocation::bare_path_on_a_missing_store_refuses_and_creates_nothing, read_cli_journeys_invocation::write_flag_creates_the_store_a_read_refused_to_create --> |
| Live file-backed owner | `contextdb <path>` | Reading session over that owner's authenticated local channel. <!-- enforced by: read_cli_journeys_live_owner::a_reading_session_routes_through_the_live_owner_and_says_so_once --> |
| Live file-backed owner | `contextdb <path> --write` | Refused with `held_by_writer`; the refusal says a read session (drop `--write`) reaches the live owner's channel. <!-- enforced by: read_cli_journeys_machine_surface::every_refusal_carries_its_ratified_class_and_kind --> |
| Readers hydrating | `contextdb <path> --write` | Refused with `held_by_readers`, listing the hydrating readers when identifiable, else "N direct readers are hydrating this store; retry in a moment". <!-- enforced by: no wall test yet — a hydrating-readers race needs a second process holding the shared lock --> |

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--write` | off | Authorizes file creation, mutation, transactions, maintenance, and sync. Also starts the local owner-reading service (see [Owner Inspection](#owner-inspection-owner-status)). |
| `--json` | off | Machine output: stdout is JSON Lines, one document per statement or meta-command; errors and notices are JSON documents on stderr. Changes rendering only — never a ceiling. <!-- enforced by: read_cli_journeys_ordinary_results::a_result_at_the_shipped_row_ceiling_succeeds_complete, read_cli_journeys_ordinary_results::one_row_past_the_ceiling_publishes_nothing_and_names_the_ceiling, read_cli_journeys_machine_surface::a_successful_select_is_one_namespaced_column_carrying_document --> |
| `--sync-endpoint <TICKET>` | *(none)* | Writer-only: server's enrollment ticket to sync with (dial-by-key). |
| `--tenant-id <ID>` | *(none)* | Writer-only: sync namespace. Omit for local-only mode. |
| `--sync-debounce-ms <MS>` | 500 | Writer-only: auto-sync batching window. |
| `--memory-limit <SIZE>` | *(unlimited)* | Whole-session memory ceiling, read or write mode. Suffixes: `K`, `M`, `G`. |
| `--disk-limit <SIZE>` | *(unlimited)* | File-backed session disk ceiling. Never authorizes writes; invalid for `:memory:`. |

Per-invocation read ceilings and deadlines (see [Declared Limits](#declared-limits)):

| Flag | Default | Governs |
|------|--------:|---------|
| `--read-result-rows` | 500 | Complete ordinary-result rows; also the maximum one cursor fetch may request |
| `--read-result-bytes` | 4 MiB | Canonical bytes of one complete result, metadata page, or metadata object |
| `--read-work` | 50,000 | Items examined by one read or one fetch |
| `--read-active-ms` | 5,000 | Active execution per read or fetch |
| `--read-memory` | 16 MiB | Temporary memory held by one read |
| `--read-cursor-page-rows` | 100 | Fetch size when the row count is omitted |
| `--read-cursor-page-bytes` | 1 MiB | Canonical bytes in one cursor page |
| `--read-cursor-idle-ms` | 300,000 | Time allowed between cursor fetches |
| `--read-cursor-lifetime-ms` | 1,800,000 | Total cursor lifetime |
| `--read-hydration-notice-ms` | 1,000 | Threshold for the loading and statement-progress notices |
| `--read-owner-connect-ms` | 1,000 | Owner-route connect + handshake deadline |
| `--read-owner-routing-retry-ms` | 1,000 | Owner startup/shutdown race retry window |
| `--read-owner-response-ms` | 11,000 | Complete owner reply after admission |

All limit values must be positive; configuration is refused before open (exit `2`) unless
`cursor_page_rows <= result_rows`, `cursor_page_bytes <= result_bytes`, and
`cursor_idle_ms <= cursor_lifetime_ms`. <!-- enforced by: read_cli_journeys_invocation::an_invalid_limit_relationship_is_refused_before_the_store_is_opened -->

**The environment is not a behavior surface.** The CLI reads exactly two declared-behavior
environment variables — `CONTEXTDB_DB_PATH` and `CONTEXTDB_OWNER_READ_RUNTIME_DIR`, the two things
a process must know before flags exist — and a flag always wins over either. One more variable is
read, but only as a platform location input, never a behavior override: `XDG_RUNTIME_DIR` locates
the per-user runtime directory the owner channel falls back to when `--owner-read-runtime-dir` is
not set (see [Owner Inspection](#owner-inspection-owner-status)) — it picks where the channel
lives, never whether it runs or how it behaves. No other environment variable exists to the CLI,
with one observability exception: `RUST_LOG` changes log verbosity on the diagnostic stream and
nothing else — never routing, limits, results, or exits.
<!-- enforced by: read_cli_journeys_invocation::a_former_behavior_environment_alias_changes_nothing_and_says_nothing, read_cli_journeys_invocation::an_explicit_path_wins_over_the_bootstrap_database_path_variable -->

#### Reading routes

The first store-reading command resolves how this session reads (`.help`, `.trace`, `.quit`,
`.exit`, `.sync status`, and `.owner status` never resolve a route). At route selection the CLI
emits exactly one structured notice on stderr, and the route never changes within the session:
<!-- enforced by: read_cli_journeys_session_shape::the_route_notice_is_emitted_once_on_stderr_at_the_first_store_reading_command, read_cli_journeys_session_shape::session_only_commands_never_resolve_a_route -->

```json
{"notice":{"class":"io","message":"…","detail":{"kind":"read_route","route":"file","snapshot_at":"2026-08-16T09:30:12Z"}}}
```

`route` is `file` (a direct read of the committed snapshot) or `owner` (a live process owns the
store and serves this session over its authenticated local channel — same commands, same
shapes). On the file route `snapshot_at` states the committed-snapshot moment this session
serves — a long-open read terminal is a snapshot, not a live view; on the owner route it is
`null` (an owner serves live committed state). <!-- enforced by: read_cli_journeys_session_shape::the_route_notice_is_emitted_once_on_stderr_at_the_first_store_reading_command, read_cli_journeys_session_shape::session_only_commands_never_resolve_a_route --> If the owner
disappears mid-session the statement fails with `owner_disconnected`, nothing of that result is
published, and the session never silently falls back to the file; a new invocation routes
afresh. <!-- enforced by: read_cli_journeys_live_owner::a_vanished_owner_fails_the_session_visibly_and_never_rereads_the_file, read_cli_journeys_live_owner::a_fresh_invocation_after_the_owner_leaves_reads_the_idle_file -->

Two liveness notices share the `--read-hydration-notice-ms` threshold (default 1 s), both on
stderr so `--json` stdout stays pure: while the first read is still loading the store, a
hydration notice with bytes loaded so far; once a statement is executing, a
`statement_progress` notice with elapsed time and the rows and bytes produced so far, refreshed
until the statement completes or refuses — a long deliberate export is never mistaken for a
hang. <!-- enforced by: read_cli_journeys_cancellation::interrupting_a_running_statement_returns_to_the_prompt_with_the_session_alive -->

#### `--json`

Under `--json`, stdout is **JSON Lines**: one complete JSON document per statement or
meta-command, and nothing else. Every successful ordinary `SELECT` is one namespaced,
column-carrying document: <!-- enforced by: read_cli_journeys_ordinary_results::a_result_at_the_shipped_row_ceiling_succeeds_complete, read_cli_journeys_ordinary_results::one_row_past_the_ceiling_publishes_nothing_and_names_the_ceiling, read_cli_journeys_machine_surface::a_successful_select_is_one_namespaced_column_carrying_document -->

```bash
$ echo "SELECT * FROM entities;" | contextdb ./my.db --json
{"result":{"columns":["id","name"],"rows":[{"id":"550e8400-…","name":"sensor-1"}]}}
```

Every meta-command emits a document whose top-level key names its payload:

| Command | Document |
|---------|----------|
| `.tables` | `{"tables":{"items":[…],"has_more":bool,"continuation":"…"\|null}}` <!-- enforced by: read_cli_journeys_metadata::tables_under_json_is_a_namespaced_page_document, read_cli_journeys_metadata::events_status_pages_under_its_own_namespaced_key, read_cli_journeys_metadata::tables_resumes_through_its_own_continuation_until_exhausted, read_cli_journeys_metadata::a_human_metadata_page_prints_the_exact_follow_up_command, read_cli_journeys_metadata::a_continuation_is_refused_by_a_command_that_did_not_issue_it, read_cli_journeys_metadata::a_complete_metadata_response_that_does_not_fit_refuses_with_the_setting_that_would --> |
| `.schema <table>` | `{"schema":{"table":…,"columns":[…],"primary_key":[…],"indexes":[…],…,"ddl":…}}` — the full declared contract, namespaced |
| `.events status` | `{"events_status":{"items":[…],"has_more":bool,"continuation":"…"\|null}}` <!-- enforced by: read_cli_journeys_metadata::tables_under_json_is_a_namespaced_page_document, read_cli_journeys_metadata::events_status_pages_under_its_own_namespaced_key, read_cli_journeys_metadata::tables_resumes_through_its_own_continuation_until_exhausted, read_cli_journeys_metadata::a_human_metadata_page_prints_the_exact_follow_up_command, read_cli_journeys_metadata::a_continuation_is_refused_by_a_command_that_did_not_issue_it, read_cli_journeys_metadata::a_complete_metadata_response_that_does_not_fit_refuses_with_the_setting_that_would --> |
| `.maintenance status` | `{"maintenance":{…}}` |
| `.explain <sql>` | `{"explain":{"physical_plan":…,"runtime_trace":…,…}}` |
| `.cursor open` / `.cursor fetch` | `{"cursor":{"columns":[…],"rows":[…],"has_more":bool}}` <!-- enforced by: read_cli_journeys_cursor::a_first_page_carries_its_columns --> |
| `.cursor close` | `{"cursor":{"closed":true}}` |
| `.owner status` | `{"owner":{"state":…,…}}` (see [Owner Inspection](#owner-inspection-owner-status)) |
| `.trace on\|off` | `{"trace":"on"}` / `{"trace":"off"}` |
| `.sync status` (read session) | `{"sync_status":{"message":"no sync in this session — …"}}` <!-- enforced by: read_cli_journeys_session_shape::read_mode_sync_status_reports_the_session_and_disclaims_the_store --> |
| `.sync push` / `.sync pull` / `.sync reconnect` / `.sync destination` / `.sync auto` (write session) | The shipped sync documents (`{"sync":…}`, `{"sync_push":…}`, `{"sync_pull":…}`, …) |
| `.help` | `{"help":["line", …]}`, on stderr |

`.schema` returns the table's declared contract as data — columns (type, nullability,
key/unique/immutable flags, `references` and propagation clauses, `acl_references` naming the
grant table and grant column an access-controlled column is authorized against, vector
quantization, rank policy), `primary_key`, `indexes`, `state_machine`, `retain`, `history`, `sync_direction`,
`conflict_policy`, `dag_edge_types`, `propagate`, and `ddl`, the exact text the human `.schema`
prints. Five fields are always present whether or not the table declared them — `immutable`
(`false`), `dag_edge_types` (`[]`), `propagate` (`[]`), `indexes` (`[]`), and each column's
`default` / `immutable` / `unique` / `expires` — so a consumer reads those as values, not as
evidence of a declaration. Every other policy — `state_machine`, `retain`, `history`,
`sync_direction`, `conflict_policy` — is absent when the table never declared it, rather than
filled with a default nobody wrote.

Everything that is not a result goes to stderr as a JSON document — errors, notices, traces,
and `.help`:

- **Errors**, one document per error:
  `{"error":{"class":"sql|sync|io|usage","message":"…","detail":{"kind":"…", …}}}`. `class` and
  `detail.kind` are the stable contract to branch on; `message` is prose for a person and its
  wording changes freely. `line` is present when the CLI knows which input line the statement
  started on. See [Error Classification](#error-classification) for every stable kind.
- **Notices** — the same envelope with `"notice"`: something worth seeing that is not a
  failure. The route, hydration, and statement-progress notices above are the notice family's
  read-surface members.
- **Execution traces** from `.trace on`, as `{"trace":{…,"rows_examined":N}}`.
- **`.help`**, as `{"help":["line", …]}` — guidance for a person, not a result.

The promise starts once the arguments have parsed: a malformed command line is rejected by the
argument parser with its own human rendering and exit `2`; everything after that point is JSON.

#### Ordinary results: complete or refused

Every successful ordinary `SELECT` publishes its complete result — no renderer truncates, and
there is no row cap to disable (`--all` does not exist). Under shipped defaults a result
succeeds only when it has at most 500 rows (`result_rows`) and its canonical encoding is at most
4 MiB (`result_bytes`). Crossing either ceiling publishes **no rows** for that statement and
refuses with `owner_limit_exceeded`, exit `1`. <!-- enforced by: read_cli_journeys_ordinary_results::a_result_at_the_shipped_row_ceiling_succeeds_complete, read_cli_journeys_ordinary_results::one_row_past_the_ceiling_publishes_nothing_and_names_the_ceiling, read_cli_journeys_machine_surface::a_successful_select_is_one_namespaced_column_carrying_document --> The refusal
carries the refused statement verbatim (`detail.statement`) and teaches both escapes,
route-aware: <!-- enforced by: read_cli_journeys_ordinary_results::the_refusal_carries_the_statement_and_a_copy_ready_cursor_command, read_cli_journeys_ordinary_results::the_file_route_refusal_names_raising_the_result_limits_as_the_export_escape -->

- **File route:** raise `--read-result-rows` / `--read-result-bytes` for a deliberate one-shot
  export (a very large export may then cross `--read-work` / `--read-active-ms` /
  `--read-memory`; the refusal names whichever flag it crossed), or page with the cursor. The
  detail carries the copy-ready `.cursor open <statement>` in `remedy_command`, and human mode
  prints the whole refusal as one line:

  ```text
  Error: the answer went past the rows this read is allowed: 500 rows; .cursor open SELECT * FROM big; raise --read-result-rows / --read-result-bytes for a deliberate one-shot export
  ```
- **Owner route:** the ceiling is owner-imposed — a caller can lower but never raise owner
  policy. The refusal names the writer-side `--owner-read-result-rows` /
  `--owner-read-result-bytes` change and offers `.cursor open` as the only in-session escape.
  <!-- enforced by: read_cli_journeys_live_owner::the_owner_route_refusal_names_the_writer_side_change_and_the_cursor, read_cli_journeys_live_owner::the_owners_ceiling_applies_and_a_caller_cannot_raise_it -->

Human mode renders a successful ordinary `SELECT` as a bordered `+---+` table — a rule line, the
column names, a rule line, one row per line, a closing rule line — and ends it with exactly one
footer: `(N rows)`, on its own line. That is the whole rendering (the REPL sample under
[REPL](#repl) is the same table), and there is no built-in pager — pipe to `less`. Cursor pages
are the one read result that is *not* boxed: `.cursor open` / `.cursor fetch` print one row per
line, values separated by ` | `, with no border, and close the page with its own footer —
`(N rows, has_more: <true|false>)` — rather than the `(N rows)` an ordinary `SELECT` ends with. <!-- enforced by: read_cli_journeys_session_shape::a_successful_human_select_ends_with_exactly_one_row_count_footer, read_cli_journeys_session_shape::an_interactive_reading_session_announces_its_mode_and_prompts_read_only, read_cli_journeys_session_shape::a_piped_session_prints_no_prompt_and_no_banner --> The same bounds apply to a `--write` session's
SELECTs — a `--write` session executes write-classified statements normally, and its reads are
bounded like anyone's; the embedded `Database::execute` API keeps its uncapped library contract.
The SQL dialect itself (types, functions, graph and vector clauses) is documented in
`docs/query-language.md`.
<!-- enforced by: read_cli_journeys_machine_surface::a_successful_select_is_one_namespaced_column_carrying_document, read_cli_journeys_ordinary_results::a_write_session_reads_under_the_same_ceiling -->

#### Large results: the session cursor

One cursor per CLI session (piped sessions included): <!-- enforced by: read_cli_journeys_cursor::a_second_cursor_in_one_session_is_refused_until_the_first_closes, read_cli_journeys_cursor::independent_sessions_hold_independent_cursors, read_cli_journeys_cursor::the_cursor_accepts_exactly_one_read_only_select -->

- `.cursor open <SELECT>` — opens one committed snapshot, returns the first page.
- `.cursor fetch [rows]` — next page; omitting `rows` uses `cursor_page_rows` (default 100); an
  explicit count above the effective `result_rows` refuses with `owner_limit_exceeded` **without
  ending the read** — the refusal carries the copy-ready `.cursor fetch <effective-limit>` that
  works, and running it pages on from exactly where the cursor stopped.
- `.cursor close` — releases the cursor.

`.cursor open` accepts exactly one read-only `SELECT`: a write refuses with
`write_requires_flag`; anything else is `cursor_invalid_statement` and never executes. A second
`.cursor open` refuses with `cursor_already_open` until the first closes; independent CLI
sessions have independent cursors. <!-- enforced by: read_cli_journeys_cursor::a_second_cursor_in_one_session_is_refused_until_the_first_closes, read_cli_journeys_cursor::independent_sessions_hold_independent_cursors, read_cli_journeys_cursor::the_cursor_accepts_exactly_one_read_only_select -->

A page contains only complete rows and stops at the requested row count or `cursor_page_bytes`;
a single row that cannot fit a page even alone refuses with `owner_limit_exceeded` naming
`cursor_page_bytes` and the advice to select fewer columns — no value is cut in half.
<!-- enforced by: read_cli_journeys_cursor::a_page_stops_on_bytes_with_complete_rows_and_a_truthful_has_more, read_cli_journeys_cursor::a_single_row_too_wide_for_a_page_refuses_and_says_what_to_do --> `has_more: false` appears only on genuine exhaustion and
closes the cursor automatically. Draining or closing past the end is never an error: a later
`.cursor fetch` returns one empty success page (`rows: [], has_more: false`) and a later
`.cursor close` succeeds as a no-op — so a fetch-until-empty loop always exits clean.
`cursor_not_found` fires for a cursor that never existed, was explicitly closed, or belongs to
another session. A mis-sized `.cursor fetch <n>` is the one refusal that does NOT end the read:
it is checked before any row is read, so nothing is consumed and the cursor keeps its place on
both the direct-file and live-owner routes. Everything else that stops a cursor ends it and
releases its snapshot and memory — exhaustion, `.cursor close`, `cursor_expired`, losing the
owner, and a single row too wide for `cursor_page_bytes`.
<!-- enforced by: read_cli_journeys_cursor::a_refused_over_limit_fetch_leaves_the_cursor_usable_for_a_smaller_page, read_cli_journeys_cursor::the_over_limit_fetch_refusal_carries_the_fetch_command_that_works, read_cli_journeys_live_owner::an_over_limit_fetch_through_the_owner_keeps_the_cursor_and_names_the_count --> <!-- enforced by: read_cli_journeys_cursor::draining_and_closing_past_the_end_stay_clean_successes, read_cli_journeys_cursor::fetching_after_an_explicit_close_is_refused, read_cli_journeys_cursor::fetching_a_cursor_that_never_existed_is_refused --> Work and active-time budgets are charged per fetch, so a
table far larger than one `work` budget pages to exhaustion under shipped defaults.
<!-- enforced by: read_cli_journeys_cursor::the_cursor_pages_a_result_the_ordinary_ceiling_refuses, read_cli_journeys_cursor::an_explicit_fetch_above_the_effective_row_limit_is_refused --> A cursor that crosses `cursor_idle_ms` or
`cursor_lifetime_ms` refuses with `cursor_expired`, naming `idle` or `lifetime` and pointing at
a fresh `.cursor open`. <!-- enforced by: read_cli_journeys_cursor::the_cursor_pages_a_result_the_ordinary_ceiling_refuses, read_cli_journeys_cursor::an_explicit_fetch_above_the_effective_row_limit_is_refused -->

A `--write` session may use the cursor while no transaction is open; `.cursor open` during an
active transaction refuses with `cursor_transaction_active` without disturbing the transaction.
<!-- enforced by: read_cli_journeys_cursor::a_write_session_may_page_outside_a_transaction_but_not_inside_one -->

#### Metadata: bounded and resumable

`.tables` and `.events status` return as many complete items as fit in `result_bytes`, plus
continuation state; they never use the SQL cursor: <!-- enforced by: read_cli_journeys_metadata::tables_under_json_is_a_namespaced_page_document, read_cli_journeys_metadata::events_status_pages_under_its_own_namespaced_key, read_cli_journeys_metadata::tables_resumes_through_its_own_continuation_until_exhausted, read_cli_journeys_metadata::a_human_metadata_page_prints_the_exact_follow_up_command, read_cli_journeys_metadata::a_continuation_is_refused_by_a_command_that_did_not_issue_it, read_cli_journeys_metadata::a_complete_metadata_response_that_does_not_fit_refuses_with_the_setting_that_would -->

```text
.tables [--continue <continuation>]
.events status [--continue <continuation>]
```

`continuation` is a string exactly when `has_more` is true and `null` when it is false; a
continuation is accepted only by the command that issued it (anything else refuses with
`invalid_continuation`). Every human page ends with a `has_more: <true|false>` line — printed on an
exhausted page too — and when it is `true` the exact follow-up command comes on the next line. <!-- enforced by: read_cli_journeys_metadata::tables_under_json_is_a_namespaced_page_document, read_cli_journeys_metadata::events_status_pages_under_its_own_namespaced_key, read_cli_journeys_metadata::tables_resumes_through_its_own_continuation_until_exhausted, read_cli_journeys_metadata::a_human_metadata_page_prints_the_exact_follow_up_command, read_cli_journeys_metadata::a_continuation_is_refused_by_a_command_that_did_not_issue_it, read_cli_journeys_metadata::a_complete_metadata_response_that_does_not_fit_refuses_with_the_setting_that_would -->

`.schema <table>` and `.maintenance status` are single complete responses. If the complete
response does not fit `result_bytes`, no partial metadata is published; the refusal is one valid
document whose detail names `result_bytes`, `required_bytes`, and `required_setting` rendered as
`effective result_bytes >= <required_bytes>`. A single `.tables`/`.events status` item that
cannot fit even an empty page refuses the same way — never a partial or oversized page. Both
routes have identical shapes, continuation mechanics, and refusal fields. Consistency across
pages differs by route: reading an idle file, your whole session is one fixed snapshot, so pages
are always mutually consistent; reading through a live owner, each page reads current committed
state in stable item order — an item never appears on two pages, an item that existed unchanged
throughout is never skipped, and an item created or dropped between pages may or may not appear
(as psql's catalog listings behave under concurrent DDL).
<!-- enforced by: read_cli_journeys_metadata::tables_under_json_is_a_namespaced_page_document, read_cli_journeys_metadata::events_status_pages_under_its_own_namespaced_key, read_cli_journeys_metadata::tables_resumes_through_its_own_continuation_until_exhausted, read_cli_journeys_metadata::a_human_metadata_page_prints_the_exact_follow_up_command, read_cli_journeys_metadata::a_continuation_is_refused_by_a_command_that_did_not_issue_it, read_cli_journeys_metadata::a_complete_metadata_response_that_does_not_fit_refuses_with_the_setting_that_would -->

### Declared limits

Every read threshold is declared configuration. A direct reader spends its own process resources
and uses its requested values; on the owner route the effective value is, field by field, the
stricter of your request and the owner's advertised maximum — you can lower owner policy, never
raise it. <!-- enforced by: read_cli_journeys_invocation::a_valid_lowered_configuration_is_accepted, read_cli_journeys_live_owner::the_owners_ceiling_applies_and_a_caller_cannot_raise_it --> The writer/server side declares its policy
with the mirrored `--owner-read-*` flags plus `--owner-read-concurrency` (default 4),
`--owner-read-request-ms` (10,000), `--owner-read-shutdown-drain-ms` (10,000),
`--no-owner-reads`, and `--owner-read-runtime-dir <absolute-path>`.
<!-- enforced by: read_cli_journeys_live_owner::owner_status_reports_the_serving_owner_as_control_data -->

### Local-Only Mode

The simplest way to start — no server, no network:

```bash
contextdb ./my.db          # read-only inspection of an existing store
contextdb ./my.db --write  # create/mutate, persisted to file
contextdb :memory:         # ephemeral writable scratch database, lost on exit
```

`:memory:` is always writable: mutation, transaction control, and sync configuration work
without `--write`, and an explicit `--write` is an accepted no-op. It creates no file, no
companion, no owner channel, emits no route notice, and reports owner state `not_applicable`.
Every `--read-*` and `--owner-read-*` flag, and `--no-owner-reads`, is invalid with `:memory:`
(exit `2`). <!-- enforced by: read_cli_journeys_invocation::memory_store_is_writable_without_the_flag_and_accepts_the_flag_as_a_no_op, read_cli_journeys_invocation::read_and_owner_read_flags_are_invalid_with_a_memory_store, read_cli_journeys_invocation::memory_store_emits_no_route_notice_and_reports_owner_state_not_applicable -->

### Sync Mode

Sync is writer capability: `--sync-endpoint` and `--tenant-id` configure a `--write` session's
edge enrollment, exactly as before. A read session's `.sync status` prints exactly: `no sync in
this session — this reports the CLI session only, not the store; a live owner's sync state
belongs to that owner process.` <!-- enforced by: read_cli_journeys_session_shape::read_mode_sync_status_reports_the_session_and_disclaims_the_store -->

For a file-backed edge, a bare pasted ticket uses the durable adjacent identity at
`<canonical-db-path>.fabric-identity.key`; an explicit `identity=<key-file>` always wins. A
memory-backed edge must name an explicit persisted identity in its endpoint. If the endpoint is
down, sync prints one clear warning line rather than failing hard.

### Logging

Logs never share a stream with machine output: CLI logs go to stderr so they don't interfere
with query output, and the server keeps its logs on stderr whenever its stdout carries a machine
document (`--json`). The default level is `ERROR`; raise it with `RUST_LOG=debug` — verbosity is
the only thing that variable can change.

---

## Store Maintenance (`migrate` / `reset` / `diagnose` / `snapshot` / `inspect` / `purge`)

*This section describes today's shipped maintenance tools, carried forward — it is not part of
the read-surface contract freeze; each tool's contract is re-decided when it is rebuilt over the
read kernel.*

Operational subcommands are part of the one typed command tree and appear in ordinary `--help`.
<!-- enforced by: read_cli_journeys_invocation::operational_commands_are_visible_in_ordinary_help_and_repair_is_gone -->

```
contextdb migrate <PATH>
contextdb reset <PATH> --force
contextdb diagnose <PATH>
contextdb snapshot export <PATH> <NEW_ARTIFACT> [--json]
contextdb inspect key <SNAPSHOT_OR_DATABASE_PATH> --table <TABLE> --key-json <NATURAL_KEY_JSON> [--column <COLUMN>]... [--json]
contextdb inspect blob <SNAPSHOT_OR_DATABASE_PATH> --hash <64_HEX_CHARS> [--json]
contextdb inspect sync-apply-state <SNAPSHOT_OR_DATABASE_PATH> [--json]
contextdb purge <PATH> --table <TABLE> --force
```

| Command | What it does |
|---------|--------------|
| `migrate <PATH>` | Bring a legacy-format store forward in place, backing up the original first. |
| `reset <PATH> --force` | Recreate a wedged or corrupt store from scratch. Destructive. |
| `diagnose <PATH>` | Report a store's format and schema layout read-only, never modifying it. |
| `snapshot export <PATH> <NEW_ARTIFACT>` | Publish a transactionally consistent, purge-fenced backup artifact. |
| `inspect <SUBCOMMAND> <SNAPSHOT_OR_DATABASE_PATH>` | Read durable key and media state from a snapshot artifact or database file. |
| `purge <PATH> --table <TABLE> --force` | Force-gated whole-table authoritative erasure. |

### `migrate` — bring a legacy-format root forward in place

Writes a `<PATH>.bak` backup of the untouched original FIRST, reads every
row/edge/vector/DDL statement out of the legacy root, writes it into a fresh current-format
root, then atomically swaps it in. Refuses (leaving the path untouched) on a root that is
already current-format; running it twice is a safe no-op. If migration fails partway, the
original path is left as it was and the `.bak` backup remains.

### `reset --force` — recreate a wedged or corrupt root from scratch

Destructive: deletes the existing file and creates a fresh, empty current-format store. Requires
the explicit `--force` flag (exit `2` without it, nothing attempted) — restore anything you need
from a backup or a healthy sync peer FIRST.

### `diagnose` — read-only diagnosis, never modifies

Reads the store's format marker and top-level schema layout through a read-only handle and
reports its diagnosis — current-format-and-readable, legacy-format, or corrupt/truncated —
without ever opening the store read-write or writing to the path. A legacy-format root is not
corrupt: its report points at `migrate`, never `reset`, and vice versa.

### `snapshot export` — publish a purge-fenced backup

Uses the engine's transactionally consistent export path. The artifact path must not already
exist. If an authoritative purge wins the race after capture, publication is refused instead of
producing a backup that silently resurrects the purged lineage.

### `inspect` — read durable key and media state from a snapshot artifact or database file

`inspect` never opens the supplied path directly: it copies into a private temporary directory,
opens only the disposable copy, and emits a bounded report with no raw media bytes, tag names,
identity material, or storage paths. `inspect key` reports a row's retained versions (newest
128, with explicit truncation), lineage, and up to 16 requested columns within a 1 MiB budget;
`inspect blob` reports an engine-held blob's fences and transfer progress, never the payload;
`inspect sync-apply-state` emits one opaque deterministic digest plus category counts for
before/after atomic-sync audits. A `snapshot export` artifact is the guaranteed-consistent
input; a live file gives whatever state exists at copy time.

`inspect key`'s `--key-json` takes the row's **natural key** as a JSON object with exactly three
fields — `column` (the leading key column), `value` (that column's value), and `rest` (the
remaining `[column, value]` pairs of a composite key, `[]` for a single-column key). `value` is a
typed value, written as a one-key object naming the type: `{"Uuid": "<uuid>"}`,
`{"Text": "<string>"}`, `{"Integer": 42}`. A bare `"<uuid>"`, a `{"id": "<uuid>"}` map, or a
`[["id", …]]` pair list are all rejected by the deserializer. One worked call:

```bash
contextdb inspect key ./my.db --table decisions \
  --key-json '{"column":"id","value":{"Uuid":"550e8400-e29b-41d4-a716-446655440000"},"rest":[]}' \
  --column description --json
# {"lineage":null,"retained_versions":[{"created_tx":1,"deleted_tx":null,"lsn":2,"row_id":1,
#   "values":{"description":{"kind":"text","omission_reason":null,"omitted":false,
#   "source_units":15,"value":{"Text":"adopt contextdb"}}}}],
#  "total_retained_versions":1,"versions_truncated":false}
```

A composite key `(tenant, id)` is `{"column":"tenant","value":{"Text":"acme"},"rest":[["id",{"Uuid":"…"}]]}`.

### `purge` — force-gated whole-table authoritative erasure

`contextdb purge <path> --table <table> --force`; narrower selection remains SQL
`PURGE ... WHERE` in a `--write` session.

---

## REPL

An interactive session prints one start banner naming its mode — read sessions
`read-only session — pass --write to mutate`, write sessions
`write session — mutations are enabled` — and prompts `contextdb(ro)> ` or `contextdb> `.
Neither the banner nor the prompt appears when the session is not interactive.
<!-- enforced by: read_cli_journeys_session_shape::a_successful_human_select_ends_with_exactly_one_row_count_footer, read_cli_journeys_session_shape::an_interactive_reading_session_announces_its_mode_and_prompts_read_only, read_cli_journeys_session_shape::a_piped_session_prints_no_prompt_and_no_banner -->

```
contextdb(ro)> SELECT * FROM entities;
+--------------------------------------+----------+
| id                                   | name     |
+--------------------------------------+----------+
| 550e8400-e29b-41d4-a716-446655440000 | sensor-1 |
+--------------------------------------+----------+
(1 rows)
```

Every statement ends with a `;`; pressing Enter before one continues the statement under the
`...>` prompt. Ctrl-C during a running statement or cursor fetch cancels only that statement and
returns to the prompt with the session and any open cursor intact; Ctrl-C at an idle prompt
clears the typed line; Ctrl-D or `.quit` ends the session. <!-- enforced by: read_cli_journeys_cancellation::interrupting_a_running_statement_returns_to_the_prompt_with_the_session_alive, read_cli_journeys_cancellation::interrupting_at_an_idle_prompt_clears_the_typed_line, read_cli_journeys_cancellation::an_open_cursor_survives_the_interruption -->

Runtime budget control is SQL-driven and classified like everything else: `SHOW MEMORY_LIMIT` /
`SHOW DISK_LIMIT` are reads; `SET MEMORY_LIMIT '512M'` / `SET DISK_LIMIT '1G'` are writes and
need a `--write` session. <!-- enforced by: read_cli_journeys_invocation::a_reading_session_refuses_every_mutating_statement_before_it_executes, read_cli_journeys_invocation::writer_only_meta_commands_are_refused_in_a_reading_session --> Input that does not parse as any
statement is refused before execution with an ordinary `sql`-class parse error naming the
offending token, identically in read and write sessions.

### Meta-Commands

<!-- command-registry: canonical_help_signatures -->

Every spelling in this table comes from one place — `canonical_help_signatures()` in the
command registry, which is also what the REPL's own `.help` is generated from. There is no
second command table anywhere, so a command cannot appear in one surface and be missing from
the other.

A meta-command consumes exactly one line — it never accumulates across newlines until a `;`.
Paste a meta-command's whole invocation, including any SQL argument, on one line.

| Command | Alias | Read/Write | Description |
|---------|-------|------------|-------------|
| `.help` / `.help vector` / `.help propagate` | `\?` | session | Show available commands / specialized grammar. |
| `.quit` / `.exit` | `\q` | session | Exit the REPL. |
| `.tables [--continue <c>]` | `\dt` | read | List table names; large listings resume via continuation. |
| `.schema <table>` | `\d <table>` | read | Show the table's full declared contract; complete-or-refuse. Per-column `IMMUTABLE`, vector quantization, and `RANK_POLICY` clauses render alongside `NOT NULL` / `PRIMARY KEY`. |
| `.explain <sql>` | | read | Show the execution plan; never executes a write argument. <!-- enforced by: read_cli_journeys_session_shape::removed_aliases_are_not_accepted_spellings, read_cli_journeys_session_shape::conventional_aliases_keep_the_classification_of_the_commands_they_spell --> |
| `.trace on` / `.trace off` | | session | Toggle one-line execution traces. |
| `.events status [--continue <c>]` | | read | Bounded, resumable event/sink/route/schedule health. |
| `.maintenance status` | | read | One complete bounded maintenance-state response. |
| `.maintenance run` / `.maintenance compact` | | **write** | Cleanup and file-space reclamation. |
| `.cursor open/fetch/close` | | read | Bounded traversal beyond an ordinary-result ceiling. |
| `.owner status` | | status | Owner policy and state; works at capacity. |
| `.sync status` | | session | Current CLI session's sync state only. |
| `.sync push/pull/reconnect/destination/auto` | | **write** | Sync operation and recovery. |

Each conventional alias is one spelling of the command beside it and carries that command's
classification exactly:

| Alias | Spells |
|-------|--------|
| `\dt` | `.tables` |
| `\d` | `.schema` |
| `\q` | `.quit` |
| `\?` | `.help` |

`\trace` and `\sync` do not exist. <!-- enforced by: read_cli_journeys_session_shape::removed_aliases_are_not_accepted_spellings, read_cli_journeys_session_shape::conventional_aliases_keep_the_classification_of_the_commands_they_spell -->

### Trace vs Explain

`.explain <sql>` shows the execution route: the physical strategy, chosen index, pushed
predicates, rejected candidates. It never applies a statement — a read-only query is run to
collect its real route; anything that would write is planned without execution, so
`.explain DELETE FROM t` leaves the rows alone (`runtime_trace: false` under `--json`). Use
`.trace on` for the runtime route and exact `rows_examined` after each successful statement.

The trace `.trace on` prints is the value a Rust caller reads from `QueryResult.trace`: the REPL
renders that `QueryTrace` field of the executed statement's result and adds nothing to it, so a
scripted session and an API caller describe the same run in the same fields.

### `.schema` and Enforced Policy

`.schema <table>` reflects the table's full *enforced* policy — `RETAIN`, `HISTORY`,
`STATE MACHINE`, `PROPAGATE`, sync direction and conflict clauses, per-column `IMMUTABLE`,
`ACL REFERENCES`, vector quantization, `RANK_POLICY` — and its printed DDL re-parses to a table
with the same policy, so `.schema` output remains a valid way to snapshot or replay a definition.

Four things the printed DDL does not reproduce literally, and what to read instead:

- **Column `DEFAULT` clauses are omitted from the rendered DDL.** A table declared
  `status TEXT NOT NULL DEFAULT 'active'` prints back as `status TEXT NOT NULL`. The default is
  still declared and still applied on `INSERT`; it is the *printed text* that drops it, so
  replaying that text alone rebuilds the table without its defaults. Read the default from
  `--json` instead — each entry of `schema.columns` carries it verbatim in `default`
  (`"'active'"`, `"NOW()"`, `null` when there is none).
- **`STATE MACHINE` from-states print in sorted order, not the declared one.**
  `STATE MACHINE (status: draft -> [active, rejected], active -> [superseded])` prints back as
  `STATE MACHINE (status: active -> [superseded], draft -> [active, rejected])` — the transitions
  are held as a map keyed by from-state, so the keys come back sorted while each state's target
  list keeps the order you declared. Both surfaces share that map, so `--json`
  `schema.state_machine.transitions` orders its keys the same way. The graph is identical; compare
  it as a set of edges rather than diffing the text against what you typed.
- **The `CONTEXT_ID` column marker is omitted from the rendered DDL.** A table declared
  `context_id UUID CONTEXT_ID` prints back as `context_id UUID`. This marker is not decoration:
  it is how the row gate finds the column holding each row's context, so a read session that
  declared a narrowed set of contexts filters by it. Replay the printed text alone and the
  rebuilt table has no `CONTEXT_ID` column at all — every row is served to every reader,
  narrowed or not. There is currently no other surface (printed DDL or `--json`) that names
  which column carries this marker; treat a `.schema` snapshot of such a table as incomplete
  for replay and keep the original `CREATE TABLE` text instead.
- **The `SCOPE_LABEL` column marker is omitted from the rendered DDL.** Same mechanism, same
  consequence: a table declared with a `SCOPE_LABEL` column prints back without the clause, the
  row gate has no column to read a scope from on the replayed table, and a read session that
  declared a narrowed set of scopes gets every row instead of only its own. As with `CONTEXT_ID`,
  neither the printed DDL nor `--json` names this column today, so replaying `.schema` output
  alone silently drops the narrowing.

### Sync Commands (write sessions)

All sync operation requires a `--write` session with `--tenant-id`. Without `--tenant-id`,
`.sync status` and `.sync auto` answer `Sync not configured` and exit `0`; the action
subcommands fail (exit `1`) so a scripted `push && shutdown` never reads "not configured" as
"pushed". Sync direction (`SYNC PUSH ONLY` / `PULL ONLY` / `TWO WAY` / `OFF`) and conflict
policy (`KEEP FIRST` / `KEEP LATEST`) are declared in DDL. Auto-sync (`.sync auto on`) debounces
background pushes (`--sync-debounce-ms`); on exit the CLI performs a final push regardless.

---

## Owner Inspection (`.owner status`)

Every file-backed writer — the `--write` CLI, `contextdb-server`, an embedding application —
serves bounded same-machine reads by default over an authenticated local channel that never
accepts a write. That channel is what a plain `contextdb <path>` session uses when a live owner
holds the store. It is an administrative plane for inspection, not an application database pool:
an application serves its own users in-process through the read kernel, and heavy remote read
serving belongs to a sync-following replica. <!-- enforced by: read_cli_journeys_live_owner::a_reading_session_routes_through_the_live_owner_and_says_so_once -->

`.owner status` describes the file-backed process owner — never sync health. It resolves no
row-reading route and stays usable when every reader slot is occupied:
<!-- enforced by: read_cli_journeys_machine_surface::owner_status_is_control_data_that_resolves_no_route, read_cli_journeys_machine_surface::owner_status_describes_the_process_owner_and_not_sync_health, read_cli_journeys_live_owner::owner_status_reports_the_serving_owner_as_control_data -->

| Situation | State | Exit |
|---|---|---:|
| No owner | `not_running` | 0 |
| Owner serving | `serving` | 0 |
| Deliberately disabled (`--no-owner-reads`) | `serving_disabled` + reason | 0 |
| Expected to serve but cannot | `not_serving` + reason | 1 |
| `:memory:` | `not_applicable` | 0 |
| Windows | `not_applicable` + reason `platform_unsupported` | 0 |

Windows has no local owner channel: a store held by a live writer cannot be inspected there —
close the writer and rerun. Direct file reading of an idle store works everywhere.

The serving JSON reports every limit and timeout as `{"value":N,"source":"default|override"}`,
plus `concurrency`, `active_readers`, and `database_memory` (`available_bytes` is `null` when
database-wide memory is unbounded). <!-- enforced by: read_cli_journeys_machine_surface::owner_status_is_control_data_that_resolves_no_route, read_cli_journeys_machine_surface::owner_status_describes_the_process_owner_and_not_sync_health, read_cli_journeys_live_owner::owner_status_reports_the_serving_owner_as_control_data -->

An owner serves at most `concurrency` (default 4) simultaneous readers; the next reader is
refused immediately with `owner_at_capacity` — retry, close another inspecting session, or raise
the writer's `--owner-read-concurrency`. No queue forms. An open owner cursor holds one slot for
its lifetime; direct readers have no owner slot ceiling. <!-- enforced by: read_cli_journeys_live_owner::owner_status_reports_the_serving_owner_as_control_data -->

The channel lives in the per-user runtime directory (`$XDG_RUNTIME_DIR/contextdb`, with
`/run/user/<uid>` as the systemd fallback; `--owner-read-runtime-dir` overrides — required for
containers and packaged services). That one directory serves BOTH sides: give the same
`--owner-read-runtime-dir` (or `CONTEXTDB_OWNER_READ_RUNTIME_DIR`) to a reading session and it
looks for the channel exactly where the writer put it. <!-- enforced by: read_cli_journeys_runtime_root::a_reader_given_the_writer_s_runtime_root_reaches_the_owner --> Owner-channel startup failure never fails the database open:
the writer keeps running and reports `not_serving` with one startup warning. Two distinct names
for the two sides of that situation: `not_serving` is the serving STATE on the owner's own
status surface; `owner_not_serving` is the refusal KIND an inspecting session receives when it
tries to reach that owner — scripts branch on each in its own place.
<!-- enforced by: read_cli_journeys_live_owner::owner_status_reports_the_serving_owner_as_control_data -->

---

## Server (`contextdb-server`)

Coordinates sync between edge clients, and (as a file-backed writer) serves the same owner
inspection channel as the writable CLI.

```
contextdb-server --tenant-id <TENANT_ID> [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--db-path <PATH>` | `:memory:` | Database file path (env bootstrap alias: `CONTEXTDB_DB_PATH`, flag wins). A `:memory:` server serves sync but never a local owner-read channel. <!-- enforced by: read_cli_journeys_invocation::memory_store_is_writable_without_the_flag_and_accepts_the_flag_as_a_no_op, read_cli_journeys_invocation::read_and_owner_read_flags_are_invalid_with_a_memory_store, read_cli_journeys_invocation::memory_store_emits_no_route_notice_and_reports_owner_state_not_applicable --> |
| `--sync-endpoint <SPEC>` | *(auto)* | `iroh:?identity=<key-file>[&port=<u16>][&relay=…][&relay-ca=…][&publish=…][&lookup=…]` — transport identity and routing only. |
| `--response-staging-bytes <N>` | unlimited | Durable unfinished-response storage bound. |
| `--pre-admission-connections <N>` | 128 | Incoming connection/handshake task bound. |
| `--pre-admission-bytes <N>` | 64 MiB | Aggregate request payload reserved before route admission. |
| `--request-read-idle-ms <MS>` | 30,000 | Closes a request making no byte progress. |
| `--owner-read-*` / `--no-owner-reads` / `--owner-read-runtime-dir` | shipped defaults | Owner-service policy, disablement, runtime directory (`CONTEXTDB_OWNER_READ_RUNTIME_DIR` bootstrap alias, flag wins). |
| `--ticket-file <PATH>` | *(none)* | Write the enrollment ticket to a file once bound. Sensitive bearer material. |
| `--show-ticket` | off | Print the bare ticket and exit. |
| `--json` | off | One JSON object with `enrollment_ticket` and `dial_command`. |
| `--tenant-id <ID>` | *(required)* | Tenant identifier. |

The four resource-policy controls are top-level flags; the endpoint string carries only
transport identity and routing (identity, port, relay, publish, lookup).
<!-- enforced by: contextdb-server::server_resource_policy_contract_tests -->

Everything else about the server — the enrollment ticket contract and its sensitivity, relay and
address-lookup configuration, restart semantics and port stickiness, files on disk
(`*.fabric-identity.key`, `.port`, `<db-stem>.db.lock` — the companion lock appends `.lock` to
the full file name), exit codes — is unchanged from the shipped behavior.

---

## Non-Interactive Mode

When stdin is not a terminal, the CLI runs in pipe mode — and a piped invocation is a **full
session**: the same dot-commands, the same single session cursor, the same one route notice, so
every instruction the CLI prints (including the cursor advice inside a refusal) is executable by
a non-interactive agent. <!-- enforced by: read_cli_journeys_session_shape::a_piped_session_is_a_full_session_including_the_cursor, read_cli_journeys_session_shape::a_terminal_session_pages_the_cursor_exactly_as_the_pipe_did -->

- No prompt, no banner
- SQL statements may span lines and end at the first `;`; meta-commands stay single-line
- Results go to stdout; every error, notice, and diagnostic goes to stderr
- An `INSERT` is echoed to stdout — its own statement text, on its own line, immediately before
  its `ok (rows_affected=N)` — so a scripted load reads as a transcript of what it wrote. Only
  `INSERT` echoes, and only here: `--json` suppresses it to keep stdout a clean machine channel,
  and an interactive session never echoes because the terminal already shows what you typed
- The session continues past a refused statement so one run reports all of its errors, and the
  process exits `1` if any statement suffered a runtime refusal

```bash
echo "SELECT 1 + 1;" | contextdb :memory:
contextdb ./my.db --write < schema.sql
echo "SELECT * FROM t;" | contextdb ./my.db && echo "OK" || echo "FAILED"
```

### Exit Codes

Every binary reports one of four codes:

| Code | Meaning | Raised by |
|------|---------|-----------|
| `0` | Success. Every statement the run attempted succeeded. | A clean run, and any interactive session — it showed you each error as it happened, so the code reports the session rather than the statements inside it, exactly as `psql` and `sqlite3` do. |
| `1` | Error. The invocation was valid; something in the run failed. | Every runtime refusal on this surface, any SQL or engine error, a failed meta-command, a definitive sync failure, a store that could not be opened or closed. |
| `2` | Usage error. The invocation itself was wrong and nothing was attempted. | An unknown flag or missing argument, an unparseable flag value such as `--memory-limit 12Q`, an invalid limit relationship, or an incomplete combination such as `--tenant-id` with no `--sync-endpoint`. |
| `3` | A `.sync push` was interrupted after sending and its outcome is unconfirmed — the hub never said whether it landed, so re-pushing is the safe move. | An interrupted push, including the automatic final push on exit. |

Precedence: a definitive error (`1`) dominates an unconfirmed push (`3`), which dominates
success (`0`). A usage error (`2`) is terminal before anything runs, so it never competes. A
non-interactive session exits `1` when any statement was refused and `0` only when everything
succeeded; an unconfirmed push still reports `3` even from an interactive session, because
nobody can act on it once the process is gone.

### Error Classification

<!-- enforced by: read_cli_journeys_ordinary_results::a_session_keeps_running_after_a_refusal_and_reports_it_in_the_exit_code, read_cli_journeys_ordinary_results::a_session_with_no_refusal_exits_zero -->

Finer classification lives on the output: each error is one stderr document whose stable
`class` and `detail.kind` are what scripts branch on. The read surface's stable kinds, each
teaching its recovery: <!-- enforced by: read_cli_journeys_machine_surface::every_refusal_carries_its_ratified_class_and_kind -->

| `detail.kind` | Class | Meaning and next action |
|---|---|---|
| `write_requires_flag` | sql | A read session got a mutating statement/command; add `--write`. |
| `store_not_found` | io | The store does not exist; `--write` creates it. |
| `held_by_writer` | io | A writer owns the store; a read session (drop `--write`) reaches its channel. The refusal names the store in `store_path` and, when the holder has published a record about itself, the holding process in `process_id`. |
| `held_by_readers` | io | Readers hold the file during hydration; the refusal lists them or says to retry in a moment. |
| `owner_not_running` | io | Rust `request_owner` was called on a direct-file route. |
| `owner_not_serving` | io | A writer holds the store but has no usable inspection channel — including a writer that has claimed the store and not published its serving decision inside the caller's declared deadlines. |
| `owner_user_mismatch` | io | The owner belongs to a different operating-system user. |
| `owner_mismatch` | io | The responder does not own this database/run. |
| `owner_at_capacity` | io | Every reader slot is occupied; retry, close a session, or raise `--owner-read-concurrency`. |
| `owner_limit_exceeded` | io | A read ceiling was crossed; carries `limit`, the ceiling in `value`, the refused `statement`, and the route-aware remedy. |
| `owner_timeout` | io | A connected owner missed a configured deadline. |
| `owner_disconnected` | io | The owner disappeared mid-session; no partial result; start a new invocation. |
| `invalid_channel_data` | io | A malformed or oversized local frame. |
| `local_protocol_mismatch` | io | Caller and owner use different local protocol versions. |
| `invalid_continuation` | usage | A continuation was malformed or given to a command that did not issue it. |
| `cursor_already_open` | sql | The session's cursor is still open; close it first. |
| `cursor_transaction_active` | sql | `.cursor open` during an active write transaction. |
| `cursor_invalid_statement` | usage | `.cursor open` received something other than one SELECT. |
| `cursor_expired` | io | Idle or lifetime crossed (`detail` names which); reopen with `.cursor open`. |
| `cursor_not_found` | io | The cursor never existed, was explicitly closed, or is another connection's (a drained cursor answers an empty success page instead). |
| `direct_read_requires_writer` | io | Safe decode found state needing a corrective writable open; close holders, rerun with `--write`. |
| `operation_already_completed` | io | The local-channel operation had already produced its outcome before this poll reached it — an internal race guard, not a condition a caller triggers by any documented action. |
| `owner_route_unsupported` | io | The requested inspection kind is not implemented over the live owner's channel (today: image-state metadata); the refusal names the kind and says a direct file session answers it once no writer holds the store. |
