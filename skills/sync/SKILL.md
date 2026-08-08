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
behind NAT works with no port forwarding and no VPN **once relay is enabled** (`relay=n0`, or a
self-hosted relay — see "Crossing networks" below). By default the ticket carries only the hub's
own addresses, so an edge on a genuinely different network than the hub (not just a different
device on the same LAN) needs that step before it can connect. Two machines on one LAN sync with
nothing running in between and no internet; the default configuration contacts no third-party
service.

## Recipe checklist — hub, enroll, push, pull, converge

1. Start the hub (below) and capture its ticket.
2. **If the edge is on a different network than the hub** (not just a different device on the same
   LAN — e.g. a remote or cloud edge reaching a home or office hub), the default ticket alone will
   not connect them — go to "Crossing networks" below and restart the hub with `relay=n0` (or a
   self-hosted relay) before enrolling.
3. Enroll edge A with that ticket, create a table, insert a row, `.sync push`.
4. Enroll edge B with the same ticket, `.sync pull`, confirm the row arrived.
5. Read `.sync status` and validate the counts mean what you think (§5 below) before trusting a
   green run.
6. **If a push or pull exits `3`**, that is not a failure — go to §6 ("Handle exit code 3").
7. **If you need a DELETE to survive sync and a process restart**, the push/pull walkthrough
   below is not enough by itself — go straight to the dedicated recipe in §7.

## 1. Start the hub

The hub only serves. You don't type SQL at it — your data lives on the edges that dial in.

```bash
rm -f ./hub.ticket
contextdb-server --db-path ./hub.db --tenant-id demo --ticket-file ./hub.ticket \
  > ./hub.log 2>&1 &
until [ -s ./hub.ticket ]; do sleep 0.2; done
TICKET="$(cat ./hub.ticket)"
```

Redirect stdout/stderr to a file (`./hub.log` above) rather than leaving them attached to your
shell — run this verbatim over ssh with a bare `&`, and the unredirected output both leaks the
ticket into whatever is capturing the session and holds the ssh session open.

`--ticket-file` writes the ticket once the endpoint is bound and then serves normally — but the
server still also prints the same ticket and dial command to stdout, so redirect that too (as
above) or you've recreated the exact log-hazard this section warns about. **The ticket is sensitive
bearer enrollment material, not a public identifier** — anyone who obtains it can enroll and sync
with this hub until the hub's identity changes, so keep `./hub.ticket` (and any other file or log a
ticket lands in) out of version control and restrict its file permissions. Two other modes, when
that shape doesn't fit:

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

### Worked example 2 — the other direction, machine-readable

```bash
contextdb ./edge-b.db --tenant-id demo --sync-endpoint "$TICKET" <<'SQL'
INSERT INTO items (id, name) VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'from edge B');
.sync push
SQL
```

```text
Pushed: 1 applied, 0 skipped, 0 conflicts
```

```bash
printf '.sync pull\nSELECT id, name FROM items ORDER BY name;\n' \
  | contextdb ./edge-a.db --tenant-id demo --sync-endpoint "$TICKET" --json
```

```json
{"sync_pull":{"applied_rows":1,"conflicts":[],"outcome":"applied","pull_pages_read":1,"pull_pages_read_this_pull":1,"skipped_rows":0}}
[{"id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","name":"from edge A"},{"id":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb","name":"from edge B"}]
```

Validate: both edges now read 2 rows. **If a pull reports `applied_rows:0` and you expected a
row**, run `.sync status` (§4) and compare `push_watermark`/`pull_watermark` — the row you're
waiting for may not have been pushed from its origin edge yet.

## 4. Check state

```bash
printf '.sync status\n' | contextdb ./edge-a.db --tenant-id demo --sync-endpoint "$TICKET" --json
```
```json
{"sync":{"configured":true,"tenant":"demo","endpoint":"iroh:?to=...","transport":"connected","database_lsn":42,"push_watermark":40,"pull_watermark":38,"committed_txid":17,"pull_pages_read":1,"pull_in_progress":false}}
```

An **LSN** is a position in the change log. The push and pull watermarks say how far each direction
has progressed — the instrument for diagnosing lag. They arrive as JSON numbers, so you can compare
them without parsing. `pull_pages_read` is a cumulative, monotonically increasing count of pages
this client has read across every pull it has issued — a script polling it twice tells a genuinely
working catch-up rescan from a stuck one, distinct from the watermark, which by contract only moves
once a pull fully completes. `pull_in_progress` turns `true` only while a pull issued through this
same `SyncClient` handle is actively running; a single CLI session blocks for the whole duration of
its own `.sync pull` (one statement at a time, one process), so running `.sync status` before and
after a pull in the same session only ever observes before/after, never `true` mid-pull — that
signal is for an embedding consumer sharing one client handle across threads.

The pull watermark is bound to the specific hub that issued it, not just a bare number. Repointing
an edge at a different hub for the same tenant — a new `--sync-endpoint`, or the same endpoint
after the hub was wiped and rebuilt — is detected automatically: the edge discards the stale cursor
and pulls the new hub's full history from the start. This is expected and safe (a full re-pull is
idempotent), not a sign of data loss on the old hub.

If the endpoint is down, sync prints one clear line —
`Warning: sync endpoint unreachable: …` — rather than failing hard, and `transport` reads
`unreachable`. **If the hub and edge are on different networks, this warning (with a `dial timed
out` cause) usually does not mean the hub is down** — the default ticket carries only the hub's own
addresses, so dialing across networks fails the same way. See "Crossing networks" below for the
`relay=n0` fix; retrying `.sync reconnect` / `.sync push` alone will not resolve it in that
topology.

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

## 7. A delete that stays deleted — across sync AND restart

This is the recipe to reach for whenever a delete must actually leave the machine it happened on
and survive a process restart on the receiving edge. Compressed into one pass so you don't have to
assemble it turn-by-turn from the push/pull pattern above.

1. **Declare the table `SYNC CONFLICT KEEP LATEST` up front.** This is the load-bearing step —
   under the default `SYNC CONFLICT KEEP FIRST`, a delete is arbitrated exactly like a write: the
   first accepted value for a key wins, so a delete pushed after the insert was already accepted
   does **not** propagate. The pulling edge reports `skipped:1`, the row survives, and there is no
   error to alert you — it is a silent stranding, not a refusal. `KEEP LATEST` is what makes "last
   accepted change wins" include a delete. Run
   [`scripts/sync-policy-lint.sh`](../../scripts/sync-policy-lint.sh) against a store to catch this
   before it bites — it flags exactly this combination.
2. Stand up the hub, enroll both edges, insert on edge X, push, pull on edge Y — same shape as §§1–3.
3. On edge X, `DELETE` the row and `.sync push`.
4. On edge Y, `.sync pull` and confirm `applied` includes the delete (not `skipped`) and the row is
   gone.
5. Start a genuinely **new** `contextdb` process against edge Y's file and re-read the table — the
   delete must still be gone after the restart, not just within the same session.
6. **If step 4 shows `skipped` instead of `applied`, go back to step 1** — the table is still on
   `KEEP FIRST`. Retiring the row and re-deleting it after fixing the policy is the recovery; there
   is no way to make an already-stranded delete propagate without a policy change.

### Worked example — hub, two edges, delete, restart

```bash
contextdb-server --db-path ./hub2.db --tenant-id lifecycle --ticket-file ./hub.ticket &
until [ -s ./hub.ticket ]; do sleep 0.2; done
TICKET="$(cat ./hub.ticket)"

contextdb ./edge-x.db --tenant-id lifecycle --sync-endpoint "$TICKET" <<'SQL'
CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST;
INSERT INTO records (id, body) VALUES ('cccccccc-3333-4ccc-8ccc-cccccccccccc', 'will be deleted');
.sync push
SQL
```
```text
Pushed: 1 applied, 0 skipped, 0 conflicts
```

```bash
contextdb ./edge-y.db --tenant-id lifecycle --sync-endpoint "$TICKET" <<'SQL'
.sync pull
SELECT COUNT(*) AS n FROM records;
SQL
```
```text
Pulled: 1 applied, 0 skipped, 0 conflicts
+---+
| n |
+---+
| 1 |
+---+
```

```bash
contextdb ./edge-x.db --tenant-id lifecycle --sync-endpoint "$TICKET" <<'SQL'
DELETE FROM records WHERE id = 'cccccccc-3333-4ccc-8ccc-cccccccccccc';
.sync push
SQL
```
```text
Pushed: 1 applied, 0 skipped, 0 conflicts
```

```bash
contextdb ./edge-y.db --tenant-id lifecycle --sync-endpoint "$TICKET" <<'SQL'
.sync pull
SELECT COUNT(*) AS n FROM records;
SQL
```
```text
Pulled: 1 applied, 0 skipped, 0 conflicts
+---+
| n |
+---+
| 0 |
+---+
```

Now the restart — a **fresh process**, same file:

```bash
echo "SELECT COUNT(*) AS n FROM records;" \
  | contextdb ./edge-y.db --tenant-id lifecycle --sync-endpoint "$TICKET" --json
```
```json
[{"n":0}]
```

The delete survived the restart — `n` is `0`, which is what step 5 validates. **The same fresh
process's exit code is now `0`, not `1`** — a stderr *notice* (not an error) appears alongside it:
`{"notice":{"class":"sync","message":"Final sync push re-offered records [(\"id\", Uuid(...))],
which the hub already converged on as deleted; nothing to do."}}`. This is the CLI's unconditional
final-push-on-exit (§6) re-offering the tombstone edge Y just pulled; the hub's replay guard still
refuses the re-offer internally, but the CLI now recognizes that specific refusal as benign
convergence (both sides already agree on the delete) and reports it as a notice rather than a
failure — exit code stays `0`. Confirmed live against tip debug binaries
(`target/debug/contextdb`); if you observe exit `1` with a `strict received row ... replays a
lineage terminated by an accepted delete` **error** instead of this notice, you're on an older
binary that predates this fix — the data is still correct either way (`n` is genuinely `0`), only
the exit code differs.

### Contrast: the same sequence under the default `KEEP FIRST` (what NOT to declare here)

```text
Pulled: 0 applied, 1 skipped, 0 conflicts
+---+
| n |
+---+
| 1 |
+---+
```

Same commands, only the table's conflict policy differs (`KEEP FIRST`, the default, instead of
`KEEP LATEST`). The delete is pushed successfully from edge X but edge Y's pull reports `skipped`,
and the row is still there. This is ratified engine behavior, not a bug to work around — a delete
is arbitrated by the declared policy exactly like an upsert, so the fix is declaring the right
policy up front (step 1), never retrying the push.

## Per-table control

Two independent axes, both per table.

**Direction** — declare what flows where in table DDL:

| | |
|---|---|
| `SYNC TWO WAY` | bidirectional (default) |
| `SYNC PUSH ONLY` | local writes replicate up; remote changes ignored |
| `SYNC PULL ONLY` | remote changes applied locally; local writes not pushed |
| `SYNC OFF` | table excluded from sync entirely |

**Conflict policy** — declare who wins a genuine divergence:

| | |
|---|---|
| `SYNC CONFLICT KEEP FIRST` | the hub's first accepted value remains (default) |
| `SYNC CONFLICT KEEP LATEST` | the later accepted value replaces it |

```bash
contextdb ./edge-a.db --tenant-id demo --sync-endpoint "$TICKET" <<'SQL'
CREATE TABLE audit_log (id UUID PRIMARY KEY, entry TEXT) IMMUTABLE SYNC OFF;
CREATE TABLE items (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST;
SQL
```

`KEEP LATEST` orders by the hub's accepted sequence, not wall clock. Immutable or append-only
tables usually declare `KEEP FIRST`; current-truth rows may declare `KEEP LATEST`.

**A table that also declares `HISTORY CURRENT ONLY` needs `SYNC CONFLICT KEEP LATEST`
if it delivers anywhere. `HISTORY CURRENT ONLY`
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
contextdb-server --db-path ./hub.db --tenant-id prod --sync-endpoint "iroh:?identity=./hub.db.fabric-identity.key&lookup=mdns"
contextdb ./edge.db --tenant-id prod --sync-endpoint "iroh:?to=$TICKET&lookup=mdns"
```

`publish=` announces this node's addresses (`n0` or a self-hosted pkarr relay); `lookup=` resolves
others (`mdns` — needs `--features mdns` — `n0`, `dns:<origin>`, or a relay URL). By default
nothing is published anywhere and dialing uses exactly the addresses the ticket carries.

A typo in an endpoint spec errors loudly and names the accepted parameters.

## Depth

- Full flag, meta-command and document reference: [`docs/cli.md`](../../docs/cli.md#sync-commands)
- Protocol, watermarks, conflict semantics, tenants vs contexts: [`docs/architecture.md`](../../docs/architecture.md#sync)
- Two-machine walkthrough: [`docs/getting-started.md`](../../docs/getting-started.md#sync-across-two-machines)

## Next

- Lint a store's sync policy before you ship a table declaration → [`scripts/sync-policy-lint.sh`](../../scripts/sync-policy-lint.sh)
- Open a database, run SQL, read `--json` → [`skills/using-contextdb/SKILL.md`](../using-contextdb/SKILL.md)
- Distribute jobs and blobs over this same hub → [`skills/work-fabric/SKILL.md`](../work-fabric/SKILL.md)
- Tell a healthy pull from a stuck one, purge, back up before a destructive op → [`skills/operating-a-store/SKILL.md`](../operating-a-store/SKILL.md)
