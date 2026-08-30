# Sync Across Two Machines

contextdb syncs by dialing a key, not by connecting to a broker. One machine runs a hub; any number of edges dial it directly using a ticket the hub prints — no message broker, no port-forwarding, and (on a LAN) no external infrastructure at all.

You need the `contextdb` CLI and the `contextdb-server` binary on the machines involved; see
[Install](getting-started.md#install). The `.sync` meta-commands and auto-sync are specified in the
[CLI Reference](cli.md#sync-commands-write-sessions); the recipe-shaped depth — enrollment, delete
propagation, conflict classes — is in [`skills/sync/SKILL.md`](../skills/sync/SKILL.md).

## Start the hub

On machine A, run a sync hub — a `contextdb-server` process the edges dial into:

```bash
contextdb-server --db-path hub.db --tenant-id demo
```

On startup it prints an enrollment ticket and the exact command an edge runs to connect (substitute your own database path for `<client-db-path>`):

```text
enrollment ticket: <ticket>
To connect a client, run:
  contextdb <client-db-path> --write --sync-endpoint <ticket> --tenant-id demo
```

The ticket *is* the hub's cryptographic identity — dial-by-key. Whoever holds it can dial the hub directly, wherever it is, including from behind NAT (the edge dials out; nothing needs to be reachable on machine A). The hub only serves — you don't type SQL at it; your data lives on the edges that dial in. For scripting, three flags skip the banner: `--show-ticket` prints the bare ticket and exits, `--ticket-file <path>` writes it to a file, and `--json` emits a JSON object with `enrollment_ticket`, `dial_command`, `endpoint`, and `tenant_id`.

**Treat the ticket as sensitive bearer material, not a public identifier.** There is no allowlist —
anyone who obtains the ticket can enroll and sync with the hub until the hub's identity changes.
Keep any file it's written to (`--ticket-file`, `--json` output, shell history) out of version
control and restrict its file permissions. If a ticket leaks, rotate by re-keying the hub — delete
`hub.db.fabric-identity.key` and restart, which changes the hub's identity and invalidates every
previously issued ticket.

## Connect two edges

On each edge machine, paste the ticket, giving each its own database file:

```bash
# machine B
contextdb edge-1.db --write --sync-endpoint <ticket> --tenant-id demo
# machine C
contextdb edge-2.db --write --sync-endpoint <ticket> --tenant-id demo
```

The ticket is pinned to each edge's identity key on first connect, so later reconnects are authenticated the same way. Two machines on one LAN sync with nothing running in the middle — no cloud relay or other third party.

## Converge in both directions

Sync is push/pull, driven from the REPL with `.sync` meta-commands. On edge 1, create a small example table (`decisions` is a table you define — contextdb ships no built-in schema), insert a row, and push:

```sql
-- on machine B (edge 1)
CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT NOT NULL, reasoning TEXT);
INSERT INTO decisions (id, status, reasoning) VALUES ('750e8400-e29b-41d4-a716-446655440020', 'active', 'captured on edge 1');
.sync push
```

On edge 2, pull and read it back — the schema and the row both arrive:

```sql
-- on machine C (edge 2)
.sync pull
SELECT * FROM decisions WHERE id = '750e8400-e29b-41d4-a716-446655440020';
-- returns the row captured on edge 1
```

Now the other direction. Insert a row on edge 2, push, and pull it onto edge 1:

```sql
-- on machine C (edge 2)
INSERT INTO decisions (id, status, reasoning) VALUES ('850e8400-e29b-41d4-a716-446655440030', 'active', 'captured on edge 2');
.sync push
```

```sql
-- on machine B (edge 1)
.sync pull
SELECT * FROM decisions WHERE id = '850e8400-e29b-41d4-a716-446655440030';
-- returns the row captured on edge 2
```

Both edges now hold the same two rows, converged through the hub. `.sync status` reports what's pending in each direction before you push or pull. Table direction and conflict behavior are durable `CREATE TABLE`/`ALTER TABLE` declarations (`SYNC ...` and `SYNC CONFLICT ...`), not session commands. The sync meta-commands and auto-sync are in the [CLI Reference](cli.md); the wire protocol is covered in the Architecture doc's Sync section.

## Restart durability

The hub keeps its identity in `<db-path>.fabric-identity.key` next to the database file (here, `hub.db.fabric-identity.key`). Restarting the hub reuses that key, so the same ticket — and every edge already pinned to it — keeps working. Back this file up like a credential; never commit it to source control.
