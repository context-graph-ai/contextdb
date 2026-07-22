# CLI Reference

contextdb is primarily used as an embedded Rust library. The CLI is for exploration, debugging, and scripting against a database file.

Two binaries: `contextdb-cli` (interactive client) and `contextdb-server` (sync coordinator).

---

## CLI Client (`contextdb-cli`)

```
contextdb-cli <PATH> [OPTIONS]
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
| `--json` | | off | Emit query results as a JSON array of row objects (uncapped) instead of a table, for scripts and agents. Non-query statements print a small JSON status object like `{"rows_affected": N}`. Runtime errors route to stderr under this flag, so stdout stays pure machine data. |
| `--all` | | off | Print every row of a table result, disabling the default 100-row cap on human table output. Has no effect under `--json` (JSON output is already uncapped). |

#### `--json`

For scripting and agents, `--json` turns query results into a JSON array of row objects with no row cap, and turns non-query statements (`CREATE TABLE`, `INSERT`, etc.) into a small JSON status object:

```bash
$ echo "SELECT * FROM entities;" | contextdb-cli ./my.db --json | jq '.[0].name'
"sensor-1"
```

```bash
$ echo "CREATE TABLE t (id UUID PRIMARY KEY);" | contextdb-cli :memory: --json
{"rows_affected":0}
```

Under `--json`, runtime errors are written to stderr (never mixed into stdout), so a pipeline's stdout is always clean machine-readable output.

#### Row cap and `--all`

Human-readable TABLE output (the default, no `--json`) is capped at 100 rows. When a result is truncated, the CLI prints a footer after the table:

```
-- showing 100 of 150 rows; narrow with WHERE/LIMIT, or pass --all to print every row
```

Pass `--all` to disable the cap and print every row. `--all` only affects the table path — under `--json` there is no cap to disable, so `--all` is a no-op there.

### Local-Only Mode

The simplest way to start — no server, no network:

```bash
contextdb-cli :memory:         # ephemeral, lost on exit
contextdb-cli ./my.db          # persisted to file
```

All sync commands return `Sync not configured` in this mode.

### Sync Mode

To replicate with a server, provide `--tenant-id` matching the server's tenant. A tenant is a sync namespace — all clients and the server sharing the same tenant ID replicate with each other.

Point the edge at the server by pasting the server's enrollment ticket into `--sync-endpoint`. The server prints its ticket to stdout on startup as `enrollment ticket: <...>` (its cryptographic identity plus reachable addresses); dial-by-key means you reach the server through that identity, not a broker address:

```bash
contextdb-cli ./edge.db --tenant-id dev --sync-endpoint <ticket>
contextdb-cli ./edge.db --tenant-id production --sync-endpoint <ticket>
```

For a file-backed edge, a bare pasted ticket is automatically rewritten to the identity-pinned form using `<db-path>.fabric-identity.key`, so `.sync status` shows the rewritten `iroh:?to=…&identity=…` spec rather than the pasted ticket — this is expected. If the endpoint is down or unreachable, sync prints one clear line, `Warning: sync endpoint unreachable: …`, rather than failing hard.

This is the same configuration you'd set in Rust code when constructing a `SyncClient` — the CLI just exposes it as flags.

### Logging

CLI logs go to stderr so they don't interfere with query output (the server, by
contrast, logs to stdout — see below). The default level is `ERROR`; set
`RUST_LOG` to raise it:

```bash
RUST_LOG=debug contextdb-cli :memory:
```

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

Runtime budget control is SQL-driven:

- `SET MEMORY_LIMIT '512M'` / `SHOW MEMORY_LIMIT`
- `SET DISK_LIMIT '1G'` / `SHOW DISK_LIMIT`

For file-backed databases, `SET DISK_LIMIT` persists in the database file and survives reopen. `:memory:` accepts the command but ignores it.

### Meta-Commands

| Command | Alias | Description |
|---------|-------|-------------|
| `.help` | `\?` | Show available commands. |
| `.help vector` | | Show vector index syntax, `<=>` examples, `ROW_VECTOR(...)`, and vector error variants. |
| `.quit` / `.exit` | `\q` | Exit the REPL. |
| `.tables` | `\dt` | List all table names. |
| `.schema <table>` | `\d <table>` | Show table DDL and constraints. Per-column `IMMUTABLE`, vector quantization, and `RANK_POLICY` clauses render alongside `NOT NULL` / `PRIMARY KEY`. Table-level `RETAIN`, `STATE MACHINE`, and `PROPAGATE` policy round-trips too — see below. |
| `.explain <sql>` | | Show the query execution plan (useful for seeing whether vector search uses HNSW or brute-force). |
| `.trace on` / `.trace off` | | Toggle one-line execution traces after successful SQL statements. |

### Trace vs Explain

`.explain <sql>` shows the execution route for read queries, including the
physical strategy (`Scan`, `IndexScan`, `AdjacencyProbe`, `EdgesScan`,
`GraphBfs`, etc.), the chosen index name when applicable, pushed predicates,
and rejected index candidates. Use `.trace on` when you need the runtime route
and exact `rows_examined` counter after each successful statement.

For machine-readable access, every `QueryResult` returned by
`Database::execute` carries `QueryResult.trace`, a `QueryTrace` with the
physical strategy, chosen index, pushed predicates, and indexes the planner
considered and rejected. The per-query examined-row count is available
separately through the database execution counter, which `.trace on` reads
immediately after the statement and formats for terminal inspection.

### `.schema` and Enforced Policy

`.schema <table>` reflects the table's full *enforced* policy, not just column types — it round-trips `RETAIN ... [SYNC SAFE]`, `STATE MACHINE (...)`, `PROPAGATE ON EDGE ...`, and `PROPAGATE ON STATE ... [EXCLUDE VECTOR]` at the table level, plus the column-level `... ON STATE ... PROPAGATE SET ...` foreign-key form. The printed DDL re-parses and re-creates an equivalent table, so `.schema` output is a valid way to snapshot or replay a table definition.

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
contextdb-cli ./a.db --tenant-id demo --sync-endpoint <ticket>
contextdb> CREATE TABLE items (id UUID PRIMARY KEY, name TEXT);
contextdb> INSERT INTO items VALUES ('aaa...', 'from client A');
contextdb> .sync push
Pushed: 1 applied, 0 skipped, 0 conflicts
```

The applied count reports data rows; the `CREATE TABLE` replicates too (client B's schema is created on pull) but does not add to the row tally.

Terminal 3 — client B pulls and sees the data:
```bash
contextdb-cli ./b.db --tenant-id demo --sync-endpoint <ticket>
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
| `--ticket-file <PATH>` | | *(none)* | Write the enrollment ticket to this file once bound (overwrites), so scripts and operators can pick it up without parsing logs. |
| `--show-ticket` | | off | Print the bare enrollment ticket to stdout and exit, without serving. |
| `--json` | | off | Emit one JSON object to stdout with `enrollment_ticket` and `dial_command` (plus `endpoint` and `tenant_id`), for scripts and agents enrolling a machine. |
| `--tenant-id <ID>` | `CONTEXTDB_TENANT_ID` | *(required)* | Tenant identifier. |

The server needs no external broker. By default, on bind it prints the enrollment ticket to stdout, followed by the exact ready-to-paste command to connect a client:

```
enrollment ticket: endpointabsdahrkdrgpw4fx3b3fbzhet7i7gfv5uvb3n4f74fkct5sjonipibabaafmrsabtlfqeaiad5q6jp42zmbacafmcaaadgwlaiaqckqci6aaaeuoaiaaaaaaaaaaaaosuiba
To connect a client, run:
  contextdb-cli <client-db-path> --sync-endpoint endpointabsdahrkdrgpw4fx3b3fbzhet7i7gfv5uvb3n4f74fkct5sjonipibabaafmrsabtlfqeaiad5q6jp42zmbacafmcaaadgwlaiaqckqci6aaaeuoaiaaaaaaaaaaaaosuiba --tenant-id demo
```

This happens at any log level. Three modes adjust it for scripting:

- `--show-ticket` prints only the bare ticket to stdout and exits without serving.
- `--ticket-file <path>` writes the ticket to a file once bound (overwriting), in addition to serving normally.
- `--json` emits one JSON object instead of the two plain-text lines above, with `enrollment_ticket` and a ready-to-paste `dial_command` string:

```bash
$ contextdb-server --tenant-id demo --json
{"dial_command":"contextdb-cli <client-db-path> --sync-endpoint endpointab...  --tenant-id demo","endpoint":"474cc91e...","enrollment_ticket":"endpointab...","tenant_id":"demo"}
```

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
contextdb-cli ./edge.db --tenant-id prod --sync-endpoint "iroh:?to=<ticket>&lookup=mdns"
```

Default conflict policy is `LatestWins`. The default log level is `ERROR`; server logs go to stdout. Set `RUST_LOG=info` to see operational logs (the ticket prints regardless). Build-time options: UPnP port mapping (better direct-connection odds through home routers) via `cargo build --release -p contextdb-server --features iroh/portmapper` — not in default builds because its dependency chain carries an MPL-licensed HTTP client; LAN mDNS lookup via `--features mdns`. Transport metrics counters are on by default (local only):

```bash
RUST_LOG=info contextdb-server --tenant-id dev
```

### Restart Semantics and Port Stickiness

A hub bound **without** `port=` records the port it chose in `<identity-file>.port`, next to the identity key, and reuses it on restart — so issued tickets survive restarts by default. An explicit `port=` always wins. If the remembered port can't be bound on restart, the server fails loudly with guidance: free the port, or pass `port=` for a specific port or `port=0` for a fresh random one (a new random port invalidates already-issued tickets).

### Files on Disk

The server (and any edge that syncs) writes these next to the database file:

| File | Secret? | Purpose |
|------|---------|---------|
| `<db-path>.fabric-identity.key` | **YES** | The machine's fabric identity — a 32-byte ed25519 seed, mode `0600`, created silently on first sync use. **Back it up and never commit or share it:** losing it changes the node's identity and invalidates its tickets. For a `:memory:` server, a per-process ephemeral identity is used instead (a pid-suffixed file in a temp dir). |
| `<identity-file>.port` | No | The remembered sticky port (see above). |
| `--ticket-file` output | No | The enrollment ticket — public, safe to share; it *is* what edges dial. |
| `<db-stem>.lock` | No | A PID lockfile. It can persist as a harmless orphan after exit. |

Add `*.fabric-identity.key` (and its `.port` sibling) to your `.gitignore`.

### Deprecated: NATS transport

The original NATS broker adapter is retained behind the `nats` cargo feature. `--nats-url` (env `CONTEXTDB_NATS_URL`) selects it and requires a build with that feature. New deployments should use the dial-by-key sync endpoint above.

---

## Non-Interactive Mode

When stdin is not a terminal, the CLI runs in pipe mode — useful for scripting, CI, and seeding databases:

- No prompt, no version banner
- INSERT statements are echoed to stdout before execution
- Fatal errors (parse errors, missing tables) go to stderr and cause non-zero exit
- Non-fatal runtime errors print to stdout, exit code stays zero

```bash
echo "SELECT 1 + 1;" | contextdb-cli :memory:

contextdb-cli ./my.db < schema.sql

echo "SELECT * FROM t;" | contextdb-cli ./my.db && echo "OK" || echo "FAILED"
```

For scripts and agents that want to consume results programmatically, add `--json`:

```bash
echo "SELECT * FROM decisions WHERE status = 'active';" | contextdb-cli ./my.db --json | jq '.[].description'
```

### Error Routing

| Error Type | Stream | Exit Code |
|------------|--------|-----------|
| Parse error | stderr | non-zero |
| Table not found | stderr | non-zero |
| Runtime error (e.g. constraint violation) | stdout | zero |
| Permission denied on db path | stderr | non-zero |
| `.sync push` interrupted after send, outcome unconfirmed | stderr | **3** |

Exit code 3 is a distinct, narrow signal — it does **not** mean the push was declined or hit a conflict. It means a `.sync push` (including the automatic final push on exit) sent its changeset and was interrupted *before* the CLI could confirm whether the hub applied it. The outcome is unknown, not failed: re-pushing is safe and idempotent, and the next `.sync push` reconciles cleanly. Precedence: a definitive sync error is exit `1`; an interrupted, unconfirmed push is exit `3`; success is `0`. A scripted `push && shutdown` should treat `3` as "retry on next start," not as failure.
