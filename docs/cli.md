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
| `--nats-url <URL>` | `CONTEXTDB_NATS_URL` | `ws://localhost:9222` | NATS WebSocket URL for sync. |
| `--tenant-id <ID>` | `CONTEXTDB_TENANT_ID` | *(none)* | Tenant ID. Omit for local-only mode. |
| `--memory-limit <SIZE>` | `CONTEXTDB_MEMORY_LIMIT` | *(unlimited)* | Memory ceiling. Suffixes: `K`, `M`, `G`. |

### Local-Only Mode

The simplest way to start — no server, no NATS, no network:

```bash
contextdb-cli :memory:         # ephemeral, lost on exit
contextdb-cli ./my.db          # persisted to file
```

All sync commands return `Sync not configured` in this mode.

### Sync Mode

To replicate with a server, provide `--tenant-id` matching the server's tenant. A tenant is a sync namespace — all clients and the server sharing the same tenant ID replicate with each other.

The `--nats-url` must point to the NATS WebSocket endpoint (port 9222 by default):

```bash
contextdb-cli ./edge.db --tenant-id dev
contextdb-cli ./edge.db --tenant-id production --nats-url ws://nats.example.com:9222
```

This is the same configuration you'd set in Rust code when constructing a `SyncClient` — the CLI just exposes it as flags.

### Logging

Logs go to stderr so they don't interfere with query output:

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

### Meta-Commands

| Command | Alias | Description |
|---------|-------|-------------|
| `.help` | `\?` | Show available commands. |
| `.quit` / `.exit` | `\q` | Exit the REPL. |
| `.tables` | `\dt` | List all table names. |
| `.schema <table>` | `\d <table>` | Show table DDL and constraints. |
| `.explain <sql>` | | Show the query execution plan (useful for seeing whether vector search uses HNSW or brute-force). |

### Sync Commands

All sync commands require `--tenant-id` at startup. Without it:

```
Sync not configured. Start with --tenant-id to enable.
```

| Command | Description |
|---------|-------------|
| `.sync status` | Show tenant ID, NATS URL, connection state, and LSN watermarks. |
| `.sync push` | Push local changes to server. Reports applied, skipped, conflicts. |
| `.sync pull` | Pull remote changes from server. Reports applied, skipped, conflicts. |
| `.sync reconnect` | Drop and re-establish the NATS connection. |
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

Terminal 1 — start NATS and the server:
```bash
docker run -d -p 4222:4222 -p 9222:9222 nats:latest
contextdb-server --tenant-id demo
```

Terminal 2 — client A creates data and pushes:
```bash
contextdb-cli ./a.db --tenant-id demo
contextdb> CREATE TABLE items (id UUID PRIMARY KEY, name TEXT);
contextdb> INSERT INTO items VALUES ('aaa...', 'from client A');
contextdb> .sync push
Pushed: 2 applied, 0 skipped, 0 conflicts
```

Terminal 3 — client B pulls and sees the data:
```bash
contextdb-cli ./b.db --tenant-id demo
contextdb> .sync pull
Pulled: 2 applied, 0 skipped, 0 conflicts
contextdb> SELECT * FROM items;
```

---

## Server (`contextdb-server`)

Coordinates sync between edge clients via NATS. In production, your application would run its own server binary or embed `SyncServer` directly.

```
contextdb-server --tenant-id <TENANT_ID> [OPTIONS]
```

### Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--db-path <PATH>` | `CONTEXTDB_DB_PATH` | `:memory:` | Database file path. `:memory:` for ephemeral. |
| `--nats-url <URL>` | `CONTEXTDB_NATS_URL` | `nats://localhost:4222` | NATS server URL (native protocol). |
| `--tenant-id <ID>` | `CONTEXTDB_TENANT_ID` | *(required)* | Tenant identifier. |

The server requires a running NATS instance. Port 4222 is the native NATS protocol (used by the server). Port 9222 is WebSocket (used by CLI clients and edge devices).

```bash
docker run -d -p 4222:4222 -p 9222:9222 nats:latest
contextdb-server --tenant-id dev
```

Default conflict policy is `LatestWins`. Control log level with `RUST_LOG`:

```bash
RUST_LOG=info contextdb-server --tenant-id dev
```

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

### Error Routing

| Error Type | Stream | Exit Code |
|------------|--------|-----------|
| Parse error | stderr | non-zero |
| Table not found | stderr | non-zero |
| Runtime error (e.g. constraint violation) | stdout | zero |
| Permission denied on db path | stderr | non-zero |
