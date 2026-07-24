# CLI Reference

contextdb is primarily used as an embedded Rust library. The CLI is for exploration, debugging, and scripting against a database file.

Two binaries: `contextdb` (interactive client) and `contextdb-server` (sync coordinator).

---

## CLI Client (`contextdb`)

```
contextdb <PATH> [OPTIONS]
```

`<PATH>` is the database file. Use `:memory:` for an in-memory (ephemeral) database.

### Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--sync-endpoint <TICKET>` | `CONTEXTDB_SYNC_ENDPOINT` | *(none)* | Server's enrollment ticket to sync with (dial-by-key). |
| `--tenant-id <ID>` | `CONTEXTDB_TENANT_ID` | *(none)* | Tenant ID. Omit for local-only mode. |
| `--nats-url <URL>` | `CONTEXTDB_NATS_URL` | *(none)* | **Deprecated.** NATS broker URL. Requires a build with the `nats` feature. |
| `--memory-limit <SIZE>` | `CONTEXTDB_MEMORY_LIMIT` | *(unlimited)* | Memory ceiling. Suffixes: `K`, `M`, `G`. |
| `--disk-limit <SIZE>` | `CONTEXTDB_DISK_LIMIT` | *(unlimited)* | Disk ceiling for file-backed databases. Suffixes: `K`, `M`, `G`. Ignored for `:memory:`. |
| `--json` | | off | Emit machine output for scripts and agents: stdout becomes JSON Lines — a JSON array of row objects (uncapped) per query, `{"rows_affected": N}` per non-query, and one document per meta-command. Errors, notices, traces and help are documents too, on stderr, so stdout stays pure machine data. |
| `--all` | | off | Print every row of a table result, disabling the default 100-row cap on human table output. Has no effect under `--json` (JSON output is already uncapped). |

#### `--json`

For scripting and agents, `--json` turns query results into a JSON array of row objects with no row cap, and turns non-query statements (`CREATE TABLE`, `INSERT`, etc.) into a small JSON status object:

```bash
$ echo "SELECT * FROM entities;" | contextdb ./my.db --json | jq '.[0].name'
"sensor-1"
```

```bash
$ echo "CREATE TABLE t (id UUID PRIMARY KEY);" | contextdb :memory: --json
{"rows_affected":0}
```

Under `--json`, stdout is **JSON Lines**: one complete JSON document per line, one line per statement or meta-command that produced a result, and nothing else. A run that produced no results writes nothing to stdout.

Every meta-command emits a document too. Each one's top-level key names its payload:

```bash
$ echo ".tables" | contextdb ./my.db --json
{"tables":["decisions","entities","intentions"]}
```

```bash
$ echo ".schema decisions" | contextdb ./my.db --json | jq '{pk: .primary_key, retain: .retain}'
{"pk":["id"],"retain":{"window":30,"unit":"DAYS","seconds":2592000,"sync_safe":true}}
```

`.schema` returns the table's declared contract as data — `columns` (each with type, nullability, key/unique/immutable flags, `references` and its `ON STATE ... PROPAGATE SET ...` clause, vector quantization, rank policy), `primary_key`, `indexes`, `state_machine`, `retain`, `history`, `sync_direction`, `conflict_policy`, `dag_edge_types` and `propagate` (all three rule kinds, including the foreign-key rules the DDL renders on their column) — plus `ddl`, the exact text the human `.schema` prints, so a snapshot/replay flow keeps working. A policy the table never declared is absent rather than filled in with a default nobody wrote. `history` is `{"policy":"ALL"}` or `{"policy":"CURRENT_ONLY"}` when declared — an object, like `retain`, so a future windowed form can add keys without a shape break.

`sync_direction` and `conflict_policy` speak a fixed vocabulary, the DDL's own clause words in lowercase — `sync_off`, `push_only`, `pull_only`, `two_way` for the direction, and `keep_first`, `keep_latest` for the two declarable policies. Two more policies exist on the engine's built-in distributed-contract tables and have no DDL clause to declare them: `server_wins` and `edge_wins`. The same words appear in `.sync direction` and `.sync policy` documents, so one concept reads the same everywhere.

```bash
$ echo ".sync status" | contextdb ./my.db --tenant-id acme --sync-endpoint <ticket> --json
{"sync":{"configured":true,"tenant":"acme","endpoint":"...","transport":"connected","database_lsn":42,"push_watermark":40,"pull_watermark":38,"committed_txid":17}}
```

| Command | Document |
|---------|----------|
| `.tables` | `{"tables":[...]}` |
| `.schema <table>` | `{"table":...,"columns":[...],"primary_key":[...],"indexes":[...],"ddl":...}` plus each declared policy |
| `.explain <sql>` | `{"explain":{"physical_plan":...,"runtime_trace":true,"index_used":...,"predicates_pushed":[...],"indexes_considered":[...],"sort_elided":...}}` for a read-only statement; `{"explain":{"physical_plan":...,"runtime_trace":false}}` for any other |
| `.trace on\|off` | `{"trace":"on"}` / `{"trace":"off"}` |
| `.sync status` | `{"sync":{...}}` |
| `.sync push` / `.sync pull` | `{"sync_push":{"applied_rows":N,"skipped_rows":N,"conflicts":[...],"outcome":"applied"}}` (`"unconfirmed"` for an interrupted push) |
| `.sync reconnect` / `destination` / `direction` / `policy` / `auto` | `{"sync_reconnect":...}`, `{"sync_destination":...}`, `{"sync_direction":...}`, `{"sync_policy":...}`, `{"sync_auto":...}` |
| `.help` | `{"help":["line", ...]}`, on stderr (see below) |

Under `--json`, everything that is not a result goes to stderr, so stdout stays parseable. Every line there is a JSON document too, and there are four kinds — the first two are what a consumer branches on, the last two are output a command was asked for.

The promise starts once the arguments have parsed. A malformed command line — an unrecognized flag, a missing value — is rejected by the argument parser before any of this runs, so it keeps that parser's own human rendering and exits `2`; everything after that point is JSON.


- **Errors**, as one document per error: `{"error":{"class":"sql","message":"parse error: ...","line":3}}`. `class` is `sql`, `sync`, `io` or `usage` — the branch that tells "fix the query" from "the hub is unreachable" from "fix the command line". `line` is present when the CLI knows which input line the statement started on, and the message never repeats it as a prefix. It is not only the REPL's statements that arrive this way: an unparseable flag VALUE, a database that will not open, and every failure in the shutdown sequence use the same envelope.
- **Notices**, as `{"notice":{"class":"sync","message":"..."}}` — carrying the same four class names as an error, but saying something worth seeing that is NOT a failure: a deprecated flag, a sync endpoint the CLI will keep retrying, or a final push whose outcome the hub never confirmed (that one also sets exit `3`). Reporting these as errors would say the run failed when it did not, so a consumer deciding whether the run succeeded reads `error` documents and the exit code, never `notice`.

  The class names in both documents — `sql`, `sync`, `io`, `usage` — are the stable part of this contract and are safe to branch on. The `message` beside them is prose for a person to read, and its wording changes freely; treat an unrecognized class as "something happened" rather than as a parse failure.
- **Execution traces** from `.trace on`, as `{"trace":{...,"rows_examined":N}}` — diagnostics about a result, not the result.
- **`.help`**, as one `{"help":["line", ...]}` document per invocation — the same lines the human mode prints, carried as an array so the stream stays parseable. It goes to stderr rather than stdout because it is not a result, and its LINES are prose that changes with every feature: read them to show a person, not to discover the command surface. That surface is this document and `--help`.

Operational logging (`RUST_LOG`) is a separate channel and stays human-readable on stderr; leave it at its default when parsing the stream.

#### Row cap and `--all`

Human-readable TABLE output (the default, no `--json`) is capped at 100 rows. When a result is truncated, the CLI prints a footer after the table:

```
-- showing 100 of 150 rows; narrow with WHERE/LIMIT, or pass --all to print every row
```

Pass `--all` to disable the cap and print every row. `--all` only affects the table path — under `--json` there is no cap to disable, so `--all` is a no-op there.

### Local-Only Mode

The simplest way to start — no server, no network:

```bash
contextdb :memory:         # ephemeral, lost on exit
contextdb ./my.db          # persisted to file
```

All sync commands report `Sync not configured` in this mode: `.sync status` and `.sync auto` answer and exit `0`, while the action subcommands fail (exit `1`) rather than let a script read an action that never happened as success.

### Sync Mode

To replicate with a server, provide `--tenant-id` matching the server's tenant. A tenant is a sync namespace — all clients and the server sharing the same tenant ID replicate with each other.

Point the edge at the server by pasting the server's enrollment ticket into `--sync-endpoint`. The server prints its ticket to stdout on startup as `enrollment ticket: <...>` (its cryptographic identity plus reachable addresses); dial-by-key means you reach the server through that identity, not a broker address:

```bash
contextdb ./edge.db --tenant-id dev --sync-endpoint <ticket>
contextdb ./edge.db --tenant-id production --sync-endpoint <ticket>
```

For a file-backed edge, a bare pasted ticket is automatically rewritten to the identity-pinned form using `<db-path>.fabric-identity.key`, so `.sync status` shows the rewritten `iroh:?to=…&identity=…` spec rather than the pasted ticket — this is expected. If the endpoint is down or unreachable, sync prints one clear line, `Warning: sync endpoint unreachable: …`, rather than failing hard.

This is the same configuration you'd set in Rust code when constructing a `SyncClient` — the CLI just exposes it as flags.

### Logging

CLI logs go to stderr so they don't interfere with query output (the server, by
contrast, logs to stdout — see below). The default level is `ERROR`; set
`RUST_LOG` to raise it:

```bash
RUST_LOG=debug contextdb :memory:
```

---

## Store Maintenance (`migrate` / `reset` / `repair`)

Three subcommands, dispatched by literal first argument BEFORE the normal `contextdb <PATH> [OPTIONS]` parsing runs — so they never collide with a database path (a real path is never literally `migrate`, `reset`, or `repair`):

```
contextdb migrate <PATH>
contextdb reset <PATH> --force
contextdb repair <PATH>
```

### `migrate` — bring a legacy-format root forward in place

Opening a data root written by an incompatible older release fails closed with a `LegacyVectorStoreDetected` error naming this command. `migrate` writes a `<PATH>.bak` backup of the untouched original FIRST, reads every row/edge/vector/DDL statement out of the legacy root through a dedicated legacy-format reader, writes it into a fresh current-format root, then atomically swaps it in. Rows from keyless tables (those without a PRIMARY KEY, natural_key_column, or "id" column) cannot be represented in a changeset, so their current visible state is copied separately:

```bash
contextdb migrate ./my.db
# migrated './my.db' in place (42 rows from changeset + 3 keyless-table rows from current state); the pre-migration store is backed up at './my.db.bak'.
```

If there are no keyless tables or they are empty, the output shows only the changeset row count.

Refuses (leaving the path untouched) on a root that is already current-format:

```bash
contextdb migrate ./already-current.db
# Error: './already-current.db' is already current-format; there is nothing to migrate.
```

Running `migrate` twice on the same path is safe: the second run detects the now-current-format root and refuses as a no-op — it never re-migrates or duplicates data. If migration fails partway (writing the fresh root, or the final swap), the original path is left as it was and the `.bak` backup is still there; the error message says exactly what state things are in.

### `reset --force` — recreate a wedged or corrupt root from scratch

Destructive: deletes the existing file at `<PATH>` (whatever state it's in) and creates a fresh, empty current-format store in its place. Requires the explicit `--force` flag — restore anything you need from a backup or a healthy sync peer FIRST, since this is not recoverable:

```bash
contextdb reset ./wedged.db
# Error: reset destroys the existing store, so it requires the explicit --force flag; rerun as
# `contextdb reset ./wedged.db --force` once you've restored any data you need from a backup or
# a healthy sync peer.
# (exit code 2 — a usage error: nothing was attempted)

contextdb reset ./wedged.db --force
# reset './wedged.db': a fresh, empty current-format store was created.
```

### `repair` — read-only diagnosis, never modifies

Reads the store's format marker and top-level schema layout through a read-only handle and reports its diagnosis — current-format-and-readable, legacy-format, or corrupt/truncated — without ever opening the store read-write or writing to the path. Safe to run on anything:

```bash
contextdb repair ./healthy.db
# repair: './healthy.db' is current-format and its schema layout reads cleanly; nothing to
# repair.

contextdb repair ./old-format.db
# repair: './old-format.db' is a legacy-format store (its on-disk schema layout predates this
# release), not corrupt.
# This report is read-only; the store was not modified. Run `contextdb migrate ./old-format.db`
# to migrate it in place.

contextdb repair ./maybe-corrupt.db
# repair: './maybe-corrupt.db' — corrupt vector store at './maybe-corrupt.db': metadata/format
# could not be read: ... — run `contextdb repair <path>` to see what is salvageable (it never
# modifies the store), or `contextdb reset <path> --force` to recreate it — restore from a
# backup or a healthy sync peer first if you need the existing data.
# This report is read-only; the store was not modified. Run `contextdb reset
# ./maybe-corrupt.db --force` to recreate it (after restoring any needed data from a backup or a
# healthy sync peer).
```

`repair` never recommends `reset` where `migrate` is the right command, and vice versa — a legacy-format root is not corrupt (it just predates this release's on-disk layout), so its report points at `migrate` instead.

---

## REPL

On startup the REPL prints a version banner:

```
contextdb> CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL);
ok (rows_affected=0)

contextdb> INSERT INTO entities VALUES ('550e8400-e29b-41d4-a716-446655440000', 'sensor-1');
ok (rows_affected=1)

contextdb> SELECT * FROM entities;
+--------------------------------------+----------+
| id                                   | name     |
+--------------------------------------+----------+
| 550e8400-e29b-41d4-a716-446655440000 | sensor-1 |
+--------------------------------------+----------+
```

The REPL accepts SQL statements (see [Query Language](query-language.md)) and meta-commands.

Every statement ends with a `;`. Pressing Enter before one continues the statement on the next
line under the `...>` prompt, so a multi-line `CREATE TABLE` can be pasted whole:

```
contextdb> CREATE TABLE notes (
      ...>   id UUID PRIMARY KEY,
      ...>   body TEXT
      ...> );
ok (rows_affected=0)
```

Runtime budget control is SQL-driven:

- `SET MEMORY_LIMIT '512M'` / `SHOW MEMORY_LIMIT`
- `SET DISK_LIMIT '1G'` / `SHOW DISK_LIMIT`

For file-backed databases, `SET DISK_LIMIT` persists in the database file and survives reopen. `:memory:` accepts the command but ignores it.

### Meta-Commands

Every command below emits a JSON document under `--json`; `.help` is the one whose document goes to stderr instead of stdout, because it is guidance for a person rather than a result. See [`--json`](#--json) for every shape.

| Command | Alias | Description |
|---------|-------|-------------|
| `.help` | `\?` | Show available commands. |
| `.help vector` | | Show vector index syntax, `<=>` examples, `ROW_VECTOR(...)`, and vector error variants. |
| `.quit` / `.exit` | `\q` | Exit the REPL. |
| `.tables` | `\dt` | List all table names. |
| `.schema <table>` | `\d <table>` | Show table DDL and constraints. Per-column `IMMUTABLE`, vector quantization, and `RANK_POLICY` clauses render alongside `NOT NULL` / `PRIMARY KEY`. Table-level `RETAIN`, `HISTORY`, `STATE MACHINE`, and `PROPAGATE` policy round-trips too — see below. |
| `.explain <sql>` | | Show the query execution plan (useful for seeing whether vector search uses HNSW or brute-force). |
| `.trace on` / `.trace off` | | Toggle one-line execution traces after successful SQL statements. |

### Trace vs Explain

`.explain <sql>` shows the execution route for read queries, including the
physical strategy (`Scan`, `IndexScan`, `AdjacencyProbe`, `EdgesScan`,
`GraphBfs`, etc.), the chosen index name when applicable, pushed predicates,
and rejected index candidates. Use `.trace on` when you need the runtime route
and exact `rows_examined` counter after each successful statement.

`.explain` never applies a statement. A read-only query is run to collect the
route it actually took; anything that would write — `INSERT`, `UPDATE`,
`DELETE`, DDL — is planned without being executed, so `.explain DELETE FROM t`
tells you what it would do and leaves the rows alone. Under `--json` that
distinction is on the document as `runtime_trace`: `true` means the fields
beside it were measured on a real run, `false` means the statement was only
planned and the measured fields are absent rather than empty.

For machine-readable access, every `QueryResult` returned by
`Database::execute` carries `QueryResult.trace`, a `QueryTrace` with the
physical strategy, chosen index, pushed predicates, and indexes the planner
considered and rejected. The per-query examined-row count is available
separately through the database execution counter, which `.trace on` reads
immediately after the statement and formats for terminal inspection.

### `.schema` and Enforced Policy

`.schema <table>` reflects the table's full *enforced* policy, not just column types — it round-trips `RETAIN ... [SYNC SAFE]`, `HISTORY ALL | CURRENT ONLY`, `STATE MACHINE (...)`, `PROPAGATE ON EDGE ...`, and `PROPAGATE ON STATE ... [EXCLUDE VECTOR]` at the table level, plus the column-level `... ON STATE ... PROPAGATE SET ...` foreign-key form. `HISTORY` renders only when declared, right after `RETAIN` and before the sync clauses. The printed DDL re-parses and re-creates an equivalent table, so `.schema` output is a valid way to snapshot or replay a table definition.

```
contextdb> .schema decisions
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated,
  embedding VECTOR(384)
) STATE MACHINE (status: active -> [invalidated, superseded]) RETAIN 30 DAYS SYNC SAFE PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated PROPAGATE ON STATE invalidated EXCLUDE VECTOR PROPAGATE ON STATE superseded EXCLUDE VECTOR;
```

### Sync Commands

All sync commands require `--tenant-id` at startup. Without it:

```
Sync not configured. Start with --tenant-id to enable.
```

That message is a *query's* answer, not a failure: `.sync status` and `.sync auto` print it to stdout and exit `0`. The action subcommands — `push`, `pull`, `reconnect`, `destination`, `direction`, `policy` — print it to stderr and exit `1` instead, because the action did not happen and a scripted `push && shutdown` must never read "not configured" as "pushed".

| Command | Description |
|---------|-------------|
| `.sync status` | Show tenant ID, sync endpoint, transport state (`connected`/`unreachable`), database LSN, and push/pull watermarks. |
| `.sync push` | Push local changes to server. Reports applied, skipped, conflicts. |
| `.sync pull` | Pull remote changes from server. Reports applied, skipped, conflicts. |
| `.sync reconnect` | Drop and re-establish the connection to the sync endpoint. |
| `.sync direction <table> <dir>` | Set sync direction for a table. |
| `.sync policy <table> <policy>` | Set conflict policy for a table. |
| `.sync policy default <policy>` | Set the default conflict policy for all tables. |
| `.sync auto [on\|off]` | Toggle auto-sync after writes. No argument shows current state. |

**Sync directions** (case-insensitive): `Push`, `Pull`, `Both`, `None`

- `Push` — local writes replicate to server, remote changes ignored
- `Pull` — remote changes applied locally, local writes not pushed
- `Both` — bidirectional (default)
- `None` — table excluded from sync

**Conflict policies** (case-sensitive): `InsertIfNotExists`, `ServerWins`, `EdgeWins`, `LatestWins`

- `LatestWins` — most recent write by logical timestamp wins (default)
- `ServerWins` — server version always takes precedence
- `EdgeWins` — client version always takes precedence
- `InsertIfNotExists` — insert only if the row doesn't exist; skip otherwise

**LSN** (Log Sequence Number) is the position in the change log. The push and pull watermarks shown by `.sync status` tell you how far each direction has progressed — useful for diagnosing sync lag.

### Auto-Sync

When enabled (`.sync auto on`), INSERT/UPDATE/DELETE statements trigger a background push. By default the worker debounces for `500ms` so rapid writes are batched, but you can tune that with `--sync-debounce-ms` or `CONTEXTDB_SYNC_DEBOUNCE_MS`.

If a background push fails, the CLI now reports the failure to stderr and keeps retrying in the background instead of silently dropping the pending sync.

On exit, the CLI always performs a final push to flush pending changes, regardless of auto-sync setting.

### Example: Two-Client Sync

This mirrors what happens when two edge devices sync through a server — the same `SyncClient`/`SyncServer` code the CLI uses is what your Rust application would use.

Terminal 1 — start the server and copy its enrollment ticket from stdout:
```bash
contextdb-server --tenant-id demo
# prints: enrollment ticket: <...>   — copy it for the clients below
# (or run `contextdb-server --tenant-id demo --show-ticket` to print the bare ticket and exit)
```

Terminal 2 — client A creates data and pushes:
```bash
contextdb ./a.db --tenant-id demo --sync-endpoint <ticket>
contextdb> CREATE TABLE items (id UUID PRIMARY KEY, name TEXT);
contextdb> INSERT INTO items VALUES ('aaa...', 'from client A');
contextdb> .sync push
Pushed: 1 applied, 0 skipped, 0 conflicts
```

The applied count reports data rows; the `CREATE TABLE` replicates too (client B's schema is created on pull) but does not add to the row tally.

Push, then pull on that same client, and every count reads zero: the rows coming back are ones you already hold, and re-delivered data is counted nowhere — not applied, not skipped, not a conflict. `applied` means local state changed, `conflicts` means another machine genuinely disagreed at the same key, and `skipped` means a conflict policy or the context scope turned a row away. All zeros after a push is the sync confirming you are converged, not a silent failure.

Those counts describe only rows your client can see. A row belonging to a context you are not scoped to is refused before its contents are looked at, so it lands in `skipped` and never in `conflicts` — a row you cannot see is not a row you can disagree about, and reporting it as a conflict would tell you it exists. Content-based judgements like the re-delivery no-op above apply within visible scope only.

Terminal 3 — client B pulls and sees the data:
```bash
contextdb ./b.db --tenant-id demo --sync-endpoint <ticket>
contextdb> .sync pull
Pulled: 1 applied, 0 skipped, 0 conflicts
contextdb> SELECT * FROM items;
```

---

## Server (`contextdb-server`)

Coordinates sync between edge clients. The server binds a sync endpoint reachable by its own cryptographic identity (dial-by-key) — no broker to install or expose. In production, your application would run its own server binary or embed `SyncServer` directly.

```
contextdb-server --tenant-id <TENANT_ID> [OPTIONS]
```

### Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--db-path <PATH>` | `CONTEXTDB_DB_PATH` | `:memory:` | Database file path. `:memory:` for ephemeral. |
| `--sync-endpoint <SPEC>` | `CONTEXTDB_SYNC_ENDPOINT` | *(auto)* | Sync endpoint spec, form `iroh:?identity=<key-file>[&port=<u16>][&relay=<none\|n0\|url>][&relay-ca=<cert-file>][&publish=<n0\|url>][&lookup=<n0\|mdns\|dns:origin\|url>]`. When omitted, an identity file is created next to the database file (`<db-path>.fabric-identity.key`). |
| `--ticket-file <PATH>` | | *(none)* | Write the enrollment ticket to this file once bound (overwrites), so scripts and operators can pick it up without parsing logs. The ticket is sensitive bearer material — keep the file out of version control and restrict its permissions. |
| `--show-ticket` | | off | Print the bare enrollment ticket to stdout and exit, without serving. |
| `--json` | | off | Emit one JSON object to stdout with `enrollment_ticket` and `dial_command` (plus `endpoint` and `tenant_id`), for scripts and agents enrolling a machine. |
| `--tenant-id <ID>` | `CONTEXTDB_TENANT_ID` | *(required)* | Tenant identifier. |

The server needs no external broker. By default, on bind it prints the enrollment ticket to stdout, followed by the exact ready-to-paste command to connect a client:

```
enrollment ticket: endpointabsdahrkdrgpw4fx3b3fbzhet7i7gfv5uvb3n4f74fkct5sjonipibabaafmrsabtlfqeaiad5q6jp42zmbacafmcaaadgwlaiaqckqci6aaaeuoaiaaaaaaaaaaaaosuiba
To connect a client, run:
  contextdb <client-db-path> --sync-endpoint endpointabsdahrkdrgpw4fx3b3fbzhet7i7gfv5uvb3n4f74fkct5sjonipibabaafmrsabtlfqeaiad5q6jp42zmbacafmcaaadgwlaiaqckqci6aaaeuoaiaaaaaaaaaaaaosuiba --tenant-id demo
```

This happens at any log level. Three modes adjust it for scripting:

- `--show-ticket` prints only the bare ticket to stdout and exits without serving.
- `--ticket-file <path>` writes the ticket to a file once bound (overwriting), in addition to serving normally.
- `--json` emits one JSON object instead of the two plain-text lines above, with `enrollment_ticket` and a ready-to-paste `dial_command` string:

```bash
$ contextdb-server --tenant-id demo --json
{"dial_command":"contextdb <client-db-path> --sync-endpoint endpointab...  --tenant-id demo","endpoint":"474cc91e...","enrollment_ticket":"endpointab...","tenant_id":"demo"}
```

**The ticket is sensitive bearer enrollment material, not a public identifier.** There is no
allowlist: whoever holds a valid ticket can enroll and sync with this hub, until the hub's identity
changes. Keep `--ticket-file` output (and any file or log you copy a printed ticket into) out of
version control and restrict its file permissions. If a ticket leaks, rotate by re-keying the hub —
delete `<db-path>.fabric-identity.key` and restart. That is the only thing that changes the hub's
identity and invalidates every previously issued ticket; the identity loads exclusively from that
file, so binding to a different port has no effect on it.

Machines on one LAN reach each other over direct connections with zero external infrastructure and no internet. To cross networks, set `relay=<url>` to a self-hosted `iroh-relay` (a small stateless forwarder that only carries end-to-end-encrypted bytes), or `relay=n0` to opt into the free public relays. The default configuration contacts no third-party service. When the relay presents a private or self-signed certificate, add `relay-ca=<cert-file>` pointing at its PEM bundle or single DER certificate to trust it.

A typo in the endpoint spec errors loudly rather than falling through to the deprecated broker path. An unknown parameter reports which names are accepted:

```
unknown parameter `X` in sync endpoint spec (accepted: identity, port, relay, relay-ca, publish, lookup, to)
```

Full spec grammar:

- **Bind (server):** `iroh:?identity=<key-file>[&port=<u16>][&relay=<none|n0|url>][&relay-ca=<cert-file>][&publish=...][&lookup=...]`
- **Dial (edge):** a bare ticket, `iroh:<ticket>`, or `iroh:?to=<ticket>&identity=<key-file>` — plus the same `lookup=`/`publish=` knobs

### Address Lookup (optional)

By default nothing is published anywhere and dialing uses exactly the addresses a ticket carries. Two opt-in knobs expose the transport's dynamic-resolution machinery — with them, a node is reachable by identity alone and an IP change never strands a ticket:

- `publish=` announces this node's addresses: `n0` (the free public service) or `https://...` (a self-hosted pkarr relay).
- `lookup=` resolves other nodes' current addresses when dialing: `mdns` (LAN-local broadcast — no third party, ideal for home/office networks; needs a build with `--features mdns`), `n0`, `dns:<origin-domain>` (a self-hosted DNS zone, system resolver), or `https://...` (a pkarr relay).

Example — hub and edges on one LAN, immune to DHCP changes, still zero external infrastructure:

```bash
contextdb-server --db-path ./hub.db --tenant-id prod --sync-endpoint "iroh:?identity=./hub.key&lookup=mdns"
contextdb ./edge.db --tenant-id prod --sync-endpoint "iroh:?to=<ticket>&lookup=mdns"
```

Default conflict policy is `LatestWins`. The default log level is `ERROR`; server logs go to stdout. Set `RUST_LOG=info` to see operational logs (the ticket prints regardless). Build-time options: UPnP port mapping (better direct-connection odds through home routers) via `cargo build --release -p contextdb-server --features iroh/portmapper` — not in default builds because its dependency chain carries an MPL-licensed HTTP client; LAN mDNS lookup via `--features mdns`. Transport metrics counters are on by default (local only):

```bash
RUST_LOG=info contextdb-server --tenant-id dev
```

### Server Exit Codes

`contextdb-server` honors the same table as the CLI ([Exit Codes](#exit-codes)): `0` on clean shutdown, `2` when the invocation itself is wrong (an unknown flag, or asking for an enrollment ticket from a broker URL that has none), and `1` for anything that failed while running. Exit code `3` is the CLI's alone — the server never pushes.

### Restart Semantics and Port Stickiness

A hub bound **without** `port=` records the port it chose in `<identity-file>.port`, next to the identity key, and reuses it on restart — so issued tickets survive restarts by default. An explicit `port=` always wins. If the remembered port can't be bound on restart, the server fails loudly with guidance: free the port, or pass `port=` for a specific port or `port=0` for a fresh random one. Changing the port this way does not rotate the hub's identity or revoke any ticket — it only strands an already-issued **address-only** ticket in the default no-lookup configuration, since that ticket carries the old socket address; with a `lookup=` mode enabled (see [Address Lookup](#address-lookup-optional)), the hub stays reachable by identity across a port change.

### Files on Disk

The server (and any edge that syncs) writes these next to the database file:

| File | Secret? | Purpose |
|------|---------|---------|
| `<db-path>.fabric-identity.key` | **YES** | The machine's fabric identity — a 32-byte ed25519 seed, mode `0600`, created silently on first sync use. **Back it up and never commit or share it:** losing it changes the node's identity and invalidates its tickets. For a `:memory:` server, a per-process ephemeral identity is used instead (a pid-suffixed file in a temp dir). |
| `<identity-file>.port` | No | The remembered sticky port (see above). |
| `--ticket-file` output | **Sensitive** | The enrollment ticket — bearer enrollment material; whoever holds it can enroll and sync with this hub until the hub's identity changes. Keep it out of version control and restrict its file permissions; it *is* what edges dial. |
| `<db-stem>.lock` | No | A PID lockfile. It can persist as a harmless orphan after exit. |

Add `*.fabric-identity.key` (and its `.port` sibling) to your `.gitignore`.

### Deprecated: NATS transport

The original NATS broker adapter is retained behind the `nats` cargo feature. `--nats-url` (env `CONTEXTDB_NATS_URL`) selects it and requires a build with that feature. New deployments should use the dial-by-key sync endpoint above.

---

## Non-Interactive Mode

When stdin is not a terminal, the CLI runs in pipe mode — useful for scripting, CI, and seeding databases:

- No prompt, no version banner
- Statements may span as many lines as they need: one ends at the first `;` outside a quoted string or a comment, so a real schema file runs as written. A statement left open when the input ends is run at that point
- Meta-commands (`.tables`, `.schema`, `.sync`, …) stay single-line and take no `;`
- INSERT statements are echoed to stdout before execution
- Results go to stdout; every error, warning, and diagnostic goes to stderr — no exceptions
- Any error fails the run with a non-zero exit code (see [Exit Codes](#exit-codes)). The session still continues to the next statement, so one script run reports all of its errors, but the process never reports a run that hit an error as success

```bash
echo "SELECT 1 + 1;" | contextdb :memory:

contextdb ./my.db < schema.sql

echo "SELECT * FROM t;" | contextdb ./my.db && echo "OK" || echo "FAILED"
```

For scripts and agents that want to consume results programmatically, add `--json`:

```bash
echo "SELECT * FROM decisions WHERE status = 'active';" | contextdb ./my.db --json | jq '.[].description'
```

### Exit Codes

Every contextdb binary — `contextdb`, `contextdb-server`, and the shipped demo drivers — reports one of four codes:

| Code | Meaning | Raised by |
|------|---------|-----------|
| `0` | Success. Everything the run attempted worked. | A clean run. |
| `1` | Error. The invocation was valid; something in the run failed. | Any SQL or engine error, a failed meta-command, a definitive sync failure, a database that could not be opened or closed, a demo whose observed outcome deviated from its intent. |
| `2` | Usage error. The invocation itself is wrong and nothing was attempted. | An unknown flag or missing argument (raised by the argument parser), an unparseable flag value such as `--memory-limit 12Q`, or an incomplete combination such as `--tenant-id` with no `--sync-endpoint`. |
| `3` | A `.sync push` was interrupted after sending and its outcome is unconfirmed — the hub never said whether it landed, so re-pushing is the safe move. | An interrupted push, including the automatic final push on exit. |

Precedence: a definitive error (`1`) dominates an unconfirmed push (`3`), which dominates success (`0`). A usage error (`2`) is terminal at startup — nothing runs, so it never competes.

Interactive sessions are the one exception to "any error fails the run": a terminal session showed you each error as it happened, so it exits `0` (as `psql` and `sqlite3` do). An unconfirmed push still reports `3` from an interactive session, because nobody can act on it once the process is gone.

Finer classification lives on the output rather than in the exit status: under `--json`, each error is written to stderr as a `{"error":{"class":...}}` document whose `class` separates a SQL mistake from an unreachable hub, so the table above stays small and stable.

Error routing, by example:

| Error Type | Stream | Exit Code |
|------------|--------|-----------|
| Parse error | stderr | `1` |
| Table not found | stderr | `1` |
| Runtime error (e.g. constraint violation) | stderr | `1` |
| Unknown meta-command, or a `.sync` action with no `--tenant-id` | stderr | `1` |
| Permission denied on db path | stderr | `1` |
| Unknown flag, or an invalid flag value | stderr | `2` |
| `.sync push` interrupted after send, outcome unconfirmed | stderr | `3` |

Exit code 3 is a distinct, narrow signal — it does **not** mean the push was declined or hit a conflict. It means a `.sync push` (including the automatic final push on exit) sent its changeset and was interrupted *before* the CLI could confirm whether the hub applied it. The outcome is unknown, not failed: re-pushing is safe and idempotent, and the next `.sync push` reconciles cleanly. Precedence: a definitive sync error is exit `1`; an interrupted, unconfirmed push is exit `3`; success is `0`. A scripted `push && shutdown` should treat `3` as "retry on next start," not as failure.
