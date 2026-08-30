---
name: operating-a-store
description: Run the operator lifecycle on a contextdb store — enforce RETAIN windows on demand with .maintenance run, permanently erase rows with purge (never-connected stores only), back up before a destructive op, and read sync liveness signals (pull_pages_read) instead of guessing whether a pull is stuck.
---

# Operating a store

The recovery/maintenance verbs (`diagnose`, `migrate`, `reset --force`) and the destructive-op
confirmation rule already live in
[`AGENTS.md`'s destructive-op table and migration rehearsal](../../AGENTS.md#destructive-commands-need-explicit-confirmation) —
read that first; this skill does not repeat it. What this skill covers instead: driving `RETAIN`
expiry on demand, permanently erasing rows with `purge`, backing up before any destructive op, and
telling a healthy-but-slow sync pull apart from a genuinely stuck one.

**Prerequisite:** needs the `contextdb` binary. In a checkout of this repo:
`cargo build --release -p contextdb-cli`. Other install options:
[`docs/getting-started.md`](../../docs/getting-started.md).

## Recipe checklist

1. **Enforce a `RETAIN` window right now** (don't wait for the background cycle) → `.maintenance
   run`, recipe 1.
2. **Erase rows permanently** → `contextdb purge <path> --table <t> --force`, recipe 2. **Only
   legal on a store that has never connected to a sync hub** — read the honest caveat in recipe 2
   before you reach for this on a synced edge.
3. **Take a backup before ANY destructive op** (`reset`, `migrate`, `purge`) → `contextdb snapshot
   export <path> <dest>`, recipe 3. This is the safe prerequisite AGENTS.md's table already points
   at; the worked example here is the copy-paste form.
4. **Tell a healthy long-running pull apart from a stuck one** → recipe 4, `pull_pages_read`.
5. **If you need `diagnose`/`migrate`/`reset`**, go straight to
   [AGENTS.md's destructive-op table](../../AGENTS.md#destructive-commands-need-explicit-confirmation)
   — this skill doesn't re-derive those, it composes with them (recipe 3 is the backup step you take
   before using them).
6. **Need to just look at a store without any destructive risk?** Plain `contextdb <path>` (no
   `--write`) already is a bounded read session — see
   [AGENTS.md's "Reading is safe by default"](../../AGENTS.md#reading-is-safe-by-default).

## Recipe 1 — enforce `RETAIN` on demand with `.maintenance run`

1. Declare `RETAIN <n> <unit>` on the table (`SECONDS`, `MINUTES`, `HOURS`, `DAYS`).
2. Insert a row and let its window pass.
3. Run `.maintenance run` — this drives one on-demand maintenance cycle **synchronously**, instead
   of waiting for the background schedule. It is the only way to make retention observable inside
   a short session or test.
4. Validate: re-read the table and confirm the row is gone; the `.maintenance run` result itself
   also reports `pruned_rows`.
5. **If `pruned_rows` is `0` and you expected a prune**, the window hasn't actually elapsed yet —
   re-check the wall-clock math, not the command.

### Worked example 1 — a 1-second window, `--json`

```bash
contextdb ./life.db --write --json <<'SQL'
CREATE TABLE pings (id UUID PRIMARY KEY, note TEXT) RETAIN 1 SECONDS;
INSERT INTO pings (id, note) VALUES ('bbbbbbbb-2222-4bbb-8bbb-bbbbbbbbbbbb', 'short-lived');
SQL
sleep 1.2
printf '.maintenance run\nSELECT COUNT(*) AS n FROM pings;\n' | contextdb ./life.db --write --json
```

```json
{"maintenance_cycle":{"compaction":{"bytes_after":61440,"bytes_before":61440,"duration_micros":3095,"file_shrank":false,"fragmentation_before":0.8407264122596154,"ran":true},"currency_pruned_versions":0,"currency_redb_compacted":false,"currency_versions_deferred_for_readers":0,"file_shrank":true,"pruned_rows":1,"pruned_trigger_audit_rows":0,"reclaimed_bytes":360,"rows_deferred_for_readers":0}}
{"result":{"columns":["n"],"rows":[{"n":0}]}}
```

`pruned_rows:1` confirms the expiry actually fired; `n:0` confirms the row is gone.

### Worked example 2 — human output, and checking maintenance is actually running

```bash
printf '.maintenance status\n' | contextdb ./life.db --json
```

```json
{"maintenance":{"active_maintenance_loops":0,"currency_compaction_enabled":false,"policy":"engine_owned","retention_enabled":true,"running":false}}
```

`retention_enabled:true` is the fact to read here: some table in this store declares `RETAIN`, so
there is something for a cycle to reclaim. `running:false` with `active_maintenance_loops:0` is
expected from a one-shot CLI session — it runs no background maintenance loop of its own, which is
exactly why `.maintenance run` exists as the synchronous door. A long-lived embedding process that
owns the store reports its own loop here.

```bash
printf '.maintenance run\nSELECT COUNT(*) AS n FROM pings;\n' | contextdb ./life.db --write
```

```text
pruned_rows=0 rows_deferred_for_readers=0 currency_pruned_versions=0 pruned_trigger_audit_rows=0 auto_compact_ran=true
+---+
| n |
+---+
| 0 |
+---+
(1 rows)
```

Run right after Worked example 1 on the same `life.db`, `pruned_rows` here is honestly `0`, not `1`
— example 1's `.maintenance run` already pruned the one expired row, so this second cycle has
nothing left to reclaim; `n:0` still confirms the table stays empty. If you want to see `pruned_rows`
fire again, insert and let expire a fresh row first (repeat Worked example 1's setup).

`retention_enabled:false` on `.maintenance status` means no table in this store declared `RETAIN`
at all — that's a sign to go back to step 1, not a broken maintenance loop.

## Recipe 2 — `purge`: permanent erasure, and the honest limits on it

1. Run `contextdb purge <path> --table <t> --force`. There is **no `WHERE` support at this door** —
   it erases every row of the named table. For a narrower selection, use the engine's `PURGE FROM
   <table> WHERE ...` SQL statement directly instead of the CLI verb.
2. **`--force` is mandatory.** Omitting it refuses with a usage error (exit `2`) that names the
   exact command to rerun — this is deliberate friction, not a bug to route around.
3. Validate: re-read the table's row count; also `contextdb purge` itself prints how many rows it
   erased.
4. **The honest limit: `purge` only runs on a store that has never connected to a sync hub.** A
   store that has EVER had a `--sync-endpoint` configured refuses purge with `PURGE must originate
   at authoritative hub <hub-node-id>; run PURGE there` — purge must run at the hub, not on an
   edge, so that a purge is never invisible to peers that still hold the row. **If you hit that
   refusal on an edge, go run purge against the hub's own database file directly** (with the hub
   process stopped, or via whatever administrative path the hub operator uses) — there is no edge
   workaround.
5. **A subtlety worth knowing before you rely on purge as a privacy tool:** purging a row on a
   store that has *never* connected does NOT create a tombstone. If that store *later* connects to
   a hub for the first time and the hub independently holds a row with the same key, that row
   legitimately arrives on the first pull — purge only erases local history, it does not forbid a
   genuinely independent future arrival of the same key. This is the correct contract (a purge on
   an isolated store cannot know about or block a hub it has never talked to), not a leak — but
   don't rely on "I purged it once" as a permanent guarantee once you connect that store to
   anything.

### Worked example 1 — refused without `--force`, then erased

```bash
contextdb ./p.db --write --json <<'SQL'
CREATE TABLE scratch (id UUID PRIMARY KEY, note TEXT);
INSERT INTO scratch (id, note) VALUES ('11111111-1111-1111-1111-111111111111', 'a');
INSERT INTO scratch (id, note) VALUES ('22222222-2222-2222-2222-222222222222', 'b');
SQL
contextdb purge ./p.db --table scratch
```

```text
Error: purge permanently and irreversibly erases rows, so it requires the explicit --force flag; rerun as `contextdb purge ./p.db --table scratch --force` once you're certain.
```

Exit code `2`. Now with `--force`:

```bash
contextdb purge ./p.db --table scratch --force
echo "SELECT COUNT(*) AS n FROM scratch;" | contextdb ./p.db --json
```

```text
purge './p.db': erased 2 row(s) from 'scratch'.
```
```json
{"result":{"columns":["n"],"rows":[{"n":0}]}}
```

### Worked example 2 — the never-connected limit and the first-connect honesty case

A store that has connected refuses purge outright:

```bash
# edge.db already pushed/pulled against a hub earlier in this session
contextdb purge ./edge.db --table scratch --force
```

```text
Error: purge failed for './edge.db': PURGE must originate at authoritative hub 6bcbbbbf6b3a64ea01ec48231276958f8c074c5e34838e9c42562dd163a9a333; run PURGE there
```

Exit code `1` — **note this is `1`, not `2`**: the invocation was valid and `--force` was present,
but the operation itself is disallowed on this store. And on a store that purged a row **before
ever connecting**, then connects to a hub for the first time where an independent peer has the
same key:

```text
Pulled: 1 applied, 0 skipped, 0 conflicts
+--------------------------------------+----------------------+
| id                                   | note                 |
+--------------------------------------+----------------------+
| 11111111-1111-1111-1111-111111111111 | from-hub-independent |
+--------------------------------------+----------------------+
(1 rows)
```

The row is back — this is step 5's honesty case, verified live, not a hypothetical. **If you need
a row to stay gone across every future sync, `purge` alone is not that primitive** — pair it with
never connecting that store to a hub that could reintroduce the key, or purge at the hub itself so
every current and future peer converges on the erasure.

## Recipe 3 — back up before any destructive op

1. `contextdb snapshot export <path> <dest>` before `migrate`, `reset --force`, or `purge`.
2. Validate: the command's own output line states the row/edge/vector counts and byte size it
   captured — compare that against what you expect before proceeding to the destructive step.

### Worked example — export, then read the exported counts back

```bash
contextdb ./snap.db --write --json <<'SQL'
CREATE TABLE t (id UUID PRIMARY KEY, x TEXT);
INSERT INTO t (id, x) VALUES ('11111111-1111-1111-1111-111111111111', 'v');
SQL
contextdb snapshot export ./snap.db ./snap-backup
```

```text
snapshot exported to './snap-backup' at LSN 2 (1 rows, 0 edges, 0 vectors, 77824 bytes)
```

`1 rows` matches the one row inserted — that's the validation. **If the row count in the export
line doesn't match what you expect, stop before running `migrate`/`reset`/`purge`** — the backup
itself may be incomplete (a concurrent writer, a wrong path), and a destructive op after a bad
backup has no recovery path.

## Recipe 4 — is this pull alive or stuck? `pull_pages_read`

1. In an **interactive** terminal session, you don't need to do anything: a `.sync pull` that
   takes a while prints its own periodic `Pulling... N page(s) read so far` lines automatically —
   this is a background printer the CLI runs for you when stdin is a real terminal.
2. In a **scripted/piped** session, or from your own Rust code driving a `SyncClient` directly, the
   counter is exposed as `pull_pages_read` — cumulative pages this client process has read across
   every pull it has issued, reset only when the process restarts. Read it from `.sync status` in a
   `--json` session after a pull completes, or poll it from your own code on a background thread
   the way the CLI's own interactive printer does (`client.pull_pages_read()` before and after a
   delay while a pull runs on another thread) — **a genuinely live two-shell-process poll won't
   work**. A second process can certainly read the store while the puller holds it (that read is
   served by the puller over its owner channel), but `pull_pages_read` counts what *this client
   process* has read; it is not store state, so no other process can observe it.
3. Validate a completed pull's liveness retroactively: `pull_pages_read > 0` on a pull that
   `applied` more than a handful of rows means real multi-page work happened, not an instant no-op.

Both examples below assume a hub is already up and `$TICKET` holds its enrollment ticket — see the
`sync` skill's §1 if you need to stand one up first.

### Worked example 1 — `.sync status` under `--json`, after a push and pull

```bash
printf '.sync status\n' | contextdb ./edge-a.db --write --tenant-id p --sync-endpoint "$TICKET" --json
```

```json
{"sync":{"committed_txid":1,"configured":true,"database_lsn":2,"endpoint":"iroh:?to=...&identity=./edge-a.db.fabric-identity.key","pull_in_progress":false,"pull_pages_read":0,"pull_watermark":0,"push_watermark":2,"tenant":"p","transport":"connected"}}
```

`pull_pages_read:0` here is correct and not a bug — this edge has only ever pushed, never pulled.
`pull_in_progress` is a live cross-thread signal, not a durable fact — it turns `true` only while a
pull sharing this exact `SyncClient` handle is actively running, so a one-shot CLI process (which
blocks until its own pull returns) always reports `false` here regardless of history.

### Worked example 2 — after a real pull, human output

```bash
contextdb ./edge-b.db --write --tenant-id p --sync-endpoint "$TICKET" <<'SQL'
.sync pull
.sync status
SQL
```

```text
Pulled: 1 applied, 0 skipped, 0 conflicts
Sync: tenant=p, endpoint=iroh:?to=...&identity=./edge-b.db.fabric-identity.key
Transport: connected
Database LSN: 3
Push watermark: LSN 0
Pull watermark: LSN 8
Pull pages read: 1
Committed TxId: 4
```

**If `Pull pages read` stays at `0` after a pull you expected to move real data**, check
`Pull watermark` against the sender's `push_watermark` (§4 of the `sync` skill) before assuming a
hang — a `0`-page pull with `applied:0` is often just "already converged," not stuck.

## Gotchas

- **`purge` has no `WHERE`.** It erases the whole table. Use `PURGE FROM <table> WHERE ...` SQL
  directly (not the CLI verb) for a narrower erasure.
- **`purge`'s refusal on a connected store is exit `1`, not `2`** — `--force` was present and
  correctly formed, but the operation itself is disallowed; don't treat this the same as a missing
  flag.
- **A purge is local history, not a fleet-wide tombstone**, unless it runs at the hub. See recipe
  2, step 5.
- **`pull_pages_read` is per-process, not per-database.** It resets to `0` every time you start a
  new `contextdb` process — it is not a durable counter you can compare across separate CLI
  invocations days apart.

## Next

- The destructive-op table, migration rehearsal, and the safe-reading-by-default contract this skill builds on → [`AGENTS.md`](../../AGENTS.md)
- Open a database, run SQL, read `--json`, branch on exit codes → [`skills/using-contextdb/SKILL.md`](../using-contextdb/SKILL.md)
- Push/pull, watermarks, the delete-durability recipe → [`skills/sync/SKILL.md`](../sync/SKILL.md)
- Wire up sinks/routes/schedules so there's something to observe here → [`skills/running-triggers-and-schedules/SKILL.md`](../running-triggers-and-schedules/SKILL.md)
