---
name: sync
description: Replicate contextdb across machines — start a hub, enroll an edge with its ticket, push and pull, and read the applied / skipped / conflicts counts and exit code 3 correctly.
---

# Syncing contextdb across machines

**Prerequisite:** needs the `contextdb` and `contextdb-server` binaries. In a checkout of this
repo: `cargo build --release -p contextdb-cli -p contextdb-server`. Other install options:
[`docs/getting-started.md`](../../docs/getting-started.md).

Every contextdb instance is a full read-write database that works offline. Sync moves **logical
changesets** — not WAL pages — between edges through one hub, with per-table conflict resolution.
Knowledge captured on any machine becomes available on all of them.

There is **no broker**. An edge reaches the hub by dialing the hub's own cryptographic identity
(dial-by-key), carried in an *enrollment ticket* the hub prints. The edge dials outbound, so a hub
behind NAT works, no port forwarding and no VPN. Two machines on one LAN sync with nothing running
in between and no internet; the default configuration contacts no third-party service.

## 1. Start the hub

The hub only serves. You don't type SQL at it — your data lives on the edges that dial in.

```bash
rm -f ./hub.ticket
contextdb-server --db-path ./hub.db --tenant-id demo --ticket-file ./hub.ticket &
until [ -s ./hub.ticket ]; do sleep 0.2; done
TICKET="$(cat ./hub.ticket)"
```

`--ticket-file` writes the ticket once the endpoint is bound and then serves normally. **The ticket
is sensitive bearer enrollment material, not a public identifier** — anyone who obtains it can
enroll and sync with this hub until the hub's identity changes, so keep `./hub.ticket` (and any
other file or log a ticket lands in) out of version control and restrict its file permissions. Two
other modes, when that shape doesn't fit:

```bash
# One JSON object with the ticket and a ready-to-paste dial command; then serves.
contextdb-server --db-path ./hub.db --tenant-id demo --json
# {"dial_command":"contextdb <client-db-path> --sync-endpoint endpointab... --tenant-id demo",
#  "endpoint":"474cc91e...","enrollment_ticket":"endpointab...","tenant_id":"demo"}

# Print the bare ticket and EXIT without serving — for reading an existing hub's identity.
contextdb-server --db-path ./hub.db --tenant-id demo --show-ticket
```

With no flags it prints the ticket plus the dial command as two plain lines, at any log level.

**Tenant.** `--tenant-id` is a sync namespace: every client and the hub sharing the same tenant id
replicate with each other. It is *not* the same axis as a context id, which scopes which rows are
visible inside one store. One tenant owns many contexts.

**Restart durability.** The hub keeps its identity in `<db-path>.fabric-identity.key` next to the
database (here `hub.db.fabric-identity.key`) and remembers its chosen port in
`<identity-file>.port`, so already-issued tickets keep working across restarts. **Back that key up
like a credential and never commit it** — losing it changes the node's identity and invalidates
every ticket. Add `*.fabric-identity.key` and its `.port` sibling to `.gitignore`.

**If a ticket leaks, rotate it** the same way: remove the fabric identity key and restart the hub.
That changes the hub's identity and invalidates every ticket issued under the old one, including
the leaked one — there is no per-ticket revocation, since the ticket carries no separate identity
of its own. Binding to a different port does **not** rotate identity or revoke enrollment — the
identity loads exclusively from the key file — so it is not a substitute for re-keying.

## 2. Enroll an edge and push

Each edge gets its own database file and the same ticket.

```bash
contextdb ./edge-a.db --tenant-id demo --sync-endpoint "$TICKET" <<'SQL'
CREATE TABLE items (id UUID PRIMARY KEY, name TEXT);
INSERT INTO items (id, name) VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'from edge A');
.sync push
SQL
```

```text
Pushed: 1 applied, 0 skipped, 0 conflicts
```

The `CREATE TABLE` replicates too — edge B's schema is created on pull — but DDL does not add to
the row tally, so the count reads `1`, not `2`.

A bare pasted ticket is automatically rewritten to the identity-pinned form using
`<db-path>.fabric-identity.key`, so `.sync status` afterwards shows `iroh:?to=…&identity=…` rather
than the ticket you pasted. That is expected, not a bug.

## 3. Pull it onto another edge

```bash
contextdb ./edge-b.db --tenant-id demo --sync-endpoint "$TICKET" <<'SQL'
.sync pull
SELECT id, name FROM items;
SQL
```

```text
Pulled: 1 applied, 0 skipped, 0 conflicts
+--------------------------------------+-------------+
| id                                   | name        |
+--------------------------------------+-------------+
| aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa | from edge A |
+--------------------------------------+-------------+
```

Push from edge B and pull on edge A for the other direction; both edges then hold the same rows,
converged through the hub.

## 4. Check state

```bash
printf '.sync status\n' | contextdb ./edge-a.db --tenant-id demo --sync-endpoint "$TICKET" --json
```
```json
{"sync":{"configured":true,"tenant":"demo","endpoint":"iroh:?to=...","transport":"connected","database_lsn":42,"push_watermark":40,"pull_watermark":38,"committed_txid":17}}
```

An **LSN** is a position in the change log. The push and pull watermarks say how far each direction
has progressed — the instrument for diagnosing lag. They arrive as JSON numbers, so you can compare
them without parsing.

The pull watermark is bound to the specific hub that issued it, not just a bare number. Repointing
an edge at a different hub for the same tenant — a new `--sync-endpoint`, or the same endpoint
after the hub was wiped and rebuilt — is detected automatically: the edge discards the stale cursor
and pulls the new hub's full history from the start. This is expected and safe (a full re-pull is
idempotent), not a sign of data loss on the old hub.

If the endpoint is down, sync prints one clear line —
`Warning: sync endpoint unreachable: …` — rather than failing hard, and `transport` reads
`unreachable`.

## 5. Read the counts truthfully

This is the part scripts get wrong. **All zeros is a success signal, not a silent failure.**

- `applied` — rows that **changed local state**.
- `conflicts` — rows where another machine **genuinely diverged** at the same natural key.
- `skipped` — rows a conflict policy or the context scope **turned away**.

A row that arrives carrying exactly what this node already holds is a **re-delivery**. It changed
nothing and refused nothing, so it is counted **nowhere** — not applied, not skipped, not a
conflict. The everyday case is pulling right after you pushed:

```bash
contextdb ./edge-a.db --tenant-id demo --sync-endpoint "$TICKET" <<'SQL'
.sync push
.sync pull
SQL
```
```text
Pushed: 0 applied, 0 skipped, 0 conflicts
Pulled: 0 applied, 0 skipped, 0 conflicts
```

That is the sync confirming you are converged. The decision is made **per row against that row's
own content**, so one changeset mixing a re-delivery with a genuine divergence still reports the
divergence in full.

Both judgements are made strictly within what the receiver can see. **A row outside the receiving
handle's context scope is refused before its content is ever compared** — it lands in `skipped` and
never in `conflicts`. A conflict claims two peers both saw a row and disagreed, which a receiver
that cannot see the row has no basis to claim, and recording it would name the hidden row's key and
disclose the very existence the scope exists to hide.

## 6. Handle exit code 3 — unconfirmed push

Exit `3` is narrow and specific: a `.sync push` (including the automatic final push on exit) **sent
its changeset and was interrupted before the CLI could confirm whether the hub applied it.** The
outcome is unknown, not failed. It does **not** mean declined, and it does not mean conflict.

**Re-pushing is safe and idempotent** — the next `.sync push` reconciles cleanly.

```bash
printf '.sync push\n' | contextdb ./edge-a.db --tenant-id demo --sync-endpoint "$TICKET" --json
case $? in
  0) echo "pushed" ;;
  1) echo 'push failed definitively — inspect the error envelope on stderr (class: sync)' ;;
  3) echo "unconfirmed — retry on next start, do NOT treat as failure" ;;
esac
```

A scripted `push && shutdown` must treat `3` as "retry on next start". Precedence: a definitive
sync error is `1`, an unconfirmed push is `3`, success is `0`.

## Per-table control

Two independent axes, both per table.

**Direction** (case-insensitive) — what flows where:

| | |
|---|---|
| `Both` | bidirectional (default) |
| `Push` | local writes replicate up; remote changes ignored |
| `Pull` | remote changes applied locally; local writes not pushed |
| `None` | table excluded from sync entirely |

**Conflict policy** (case-sensitive) — who wins a genuine divergence:

| | |
|---|---|
| `LatestWins` | most recent write by **logical timestamp** (default) |
| `ServerWins` | hub version takes precedence |
| `EdgeWins` | edge version takes precedence |
| `InsertIfNotExists` | insert if absent, skip otherwise — the right choice for append-only/immutable tables |

```bash
contextdb ./edge-a.db --tenant-id demo --sync-endpoint "$TICKET" <<'SQL'
CREATE TABLE audit_log (id UUID PRIMARY KEY, entry TEXT) IMMUTABLE;
.sync direction audit_log None
.sync policy items InsertIfNotExists
.sync policy default LatestWins
SQL
```

Note `LatestWins` orders by log sequence, not wall clock. Pick policy by table *class*: immutable /
append-only tables want `InsertIfNotExists`; status rows that are a state machine want a
deterministic total order (`ServerWins`).

**A table that also declares `HISTORY CURRENT ONLY` needs `SYNC CONFLICT KEEP LATEST`
(`LatestWins`) if it delivers anywhere** (`Push` or the default `Both`). `HISTORY CURRENT ONLY`
reclaims superseded versions, so the only value left to send is the newest one — a puller under
`KEEP FIRST` (the DDL default) would file that newest value as the FIRST value it has ever seen for
the key. Declare `SYNC CONFLICT KEEP LATEST` for a current-truth table, or `SYNC OFF` if it never
leaves this machine; the refusal names both fixes if you meet it.

## Auto-sync

`.sync auto on` makes INSERT/UPDATE/DELETE trigger a debounced background push (500ms by default,
tunable with `--sync-debounce-ms` / `CONTEXTDB_SYNC_DEBOUNCE_MS`). A failed background push is
reported on stderr and retried, never silently dropped. On exit the CLI always performs a final
push regardless of the auto-sync setting — which is why exit `3` can surface from a session that
never typed `.sync push`.

## Local-only mode

With no `--tenant-id`, sync is not configured. `.sync status` and `.sync auto` **answer** the
question and exit `0`; the action subcommands (`push`, `pull`, `reconnect`, `destination`,
`direction`, `policy`) print to stderr and exit `1` — because the action did not happen, and a
scripted `push && shutdown` must never read "not configured" as "pushed".

## Crossing networks

LAN peers connect directly with zero external infrastructure. To introduce peers across networks,
the operator either self-hosts a small stateless `iroh-relay` that only forwards
end-to-end-encrypted bytes (`relay=<url>`, plus `relay-ca=<cert-file>` if it presents a private
certificate) or opts into the free public relays (`relay=n0`). **Connectivity is never a paid
feature.**

Address lookup is separately opt-in, so a ticket survives an IP change:

```bash
# hub and edges on one LAN, immune to DHCP changes, still zero external infrastructure
contextdb-server --db-path ./hub.db --tenant-id prod --sync-endpoint "iroh:?identity=./hub.key&lookup=mdns"
contextdb ./edge.db --tenant-id prod --sync-endpoint "iroh:?to=$TICKET&lookup=mdns"
```

`publish=` announces this node's addresses (`n0` or a self-hosted pkarr relay); `lookup=` resolves
others (`mdns` — needs `--features mdns` — `n0`, `dns:<origin>`, or a relay URL). By default
nothing is published anywhere and dialing uses exactly the addresses the ticket carries.

A typo in an endpoint spec errors loudly and names the accepted parameters rather than falling
through to the deprecated NATS broker path (which survives only behind the `nats` cargo feature and
is not on the default path).

## Depth

- Full flag, meta-command and document reference: [`docs/cli.md`](../../docs/cli.md#sync-commands)
- Protocol, watermarks, conflict semantics, tenants vs contexts: [`docs/architecture.md`](../../docs/architecture.md#sync)
- Two-machine walkthrough: [`docs/getting-started.md`](../../docs/getting-started.md#sync-across-two-machines)
