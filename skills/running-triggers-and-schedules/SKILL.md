---
name: running-triggers-and-schedules
description: Declare CREATE EVENT TYPE / SINK / ROUTE / SCHEDULE, see them fire observably in a :memory: CLI session, understand why a file-backed store queues events durably instead of delivering them, and wire the Rust callbacks (register_sink / register_cron_callback) that make a file-backed store actually deliver.
---

# Running triggers and schedules

Four DDL statements — `CREATE EVENT TYPE`, `CREATE SINK`, `CREATE ROUTE`, `CREATE SCHEDULE` — parse
and execute against any store. What differs is **whether anything is listening**: in an ephemeral
`:memory:` CLI session the CLI registers default callbacks for you, so routes deliver and
schedules fire immediately, observable in the same session. On a **file-backed** store, nothing
delivers until *your own program* registers a callback with `register_sink` /
`register_cron_callback` — until then, matching events queue durably and wait. That queueing is
the contract, not a missing feature; this skill teaches both halves.

**Prerequisite:** needs the `contextdb` binary. In a checkout of this repo:
`cargo build --release -p contextdb-cli`. Other install options:
[`docs/getting-started.md`](../../docs/getting-started.md).

## Recipe checklist

1. **Want to see it fire right now, no Rust code?** Use a `:memory:` session — recipe 1 (routes)
   and recipe 2 (schedules). This is the fastest way to confirm your DDL is correct before wiring
   real callbacks.
2. **Building something that must actually deliver in production** (a file-backed store)? The
   `:memory:` trick does not apply — go straight to recipe 3 and wire `register_sink` /
   `register_cron_callback` in your embedding program.
3. **Reading `.events status` and not sure what a field means?** Recipe 4 is the field-by-field
   reference with real captured output.
4. **If `.events status` shows `queued` growing and `delivered` staying at `0`** on a file-backed
   store, that's expected, not broken — go to recipe 3, not back to the DDL.

## Recipe 1 — a route that delivers observably, in one `:memory:` session

1. Create the table the event type watches, then `CREATE EVENT TYPE ... WHEN <INSERT|UPDATE> ON
   <table>`, then `CREATE SINK <name> TYPE callback`, then `CREATE ROUTE <name> EVENT <event_type>
   TO <sink>`.
2. Trigger it: run the `INSERT` (or `UPDATE`) the event type watches for.
3. Validate with `.events status` — the sink's `delivered` counter must have incremented by
   exactly the number of matching statements you ran.
4. **If `delivered` stays `0` in a `:memory:` session**, check `event_type`/`table`/`trigger` in
   `.events status`'s `event_types` array against what you declared — a mismatched table name or
   trigger verb (`INSERT` vs `UPDATE`) is the most common miss, not a broken sink.

### Worked example 1 — an invalidation fires a route, `--json`

```bash
contextdb :memory: --json <<'SQL'
CREATE TABLE invalidations (id UUID PRIMARY KEY, reason TEXT);
CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations;
CREATE SINK slack TYPE callback;
CREATE ROUTE inv_to_slack EVENT inv_match TO slack;
INSERT INTO invalidations (id, reason) VALUES ('11111111-1111-1111-1111-111111111111', 'basis changed');
.events status
SQL
```

```json
{"rows_affected":0}
{"rows_affected":0}
{"rows_affected":0}
{"rows_affected":0}
{"rows_affected":1}
{"events":{"event_types":[{"name":"inv_match","table":"invalidations","trigger":"INSERT"}],"routes":[{"event_type":"inv_match","name":"inv_to_slack","sink":"slack"}],"schedules":[],"sinks":[{"callback_registered":true,"delivered":1,"examined":1,"name":"slack","permanent_failures":0,"queued":0,"retried":0,"type":"CALLBACK"}]}}
```

`callback_registered:true` and `delivered:1` together are the proof — the CLI's `:memory:` default
callback fired for real, not just parsed the DDL.

### Worked example 2 — two matching inserts, one non-matching, human output

```bash
contextdb :memory: <<'SQL'
CREATE TABLE invalidations (id UUID PRIMARY KEY, reason TEXT);
CREATE TABLE other (id UUID PRIMARY KEY);
CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations;
CREATE SINK slack TYPE callback;
CREATE ROUTE inv_to_slack EVENT inv_match TO slack;
INSERT INTO invalidations (id, reason) VALUES ('11111111-1111-1111-1111-111111111111', 'first');
INSERT INTO invalidations (id, reason) VALUES ('22222222-2222-2222-2222-222222222222', 'second');
INSERT INTO other (id) VALUES ('33333333-3333-3333-3333-333333333333');
.events status
SQL
```

```text
Event types:
  inv_match WHEN INSERT ON invalidations
Sinks:
  slack TYPE CALLBACK registered=true delivered=2 queued=0 retried=0 permanent_failures=0 examined=2
Routes:
  inv_to_slack EVENT inv_match TO slack
Schedules:
  (none)
```

`examined=2, delivered=2` — the insert into `other` correctly produced no event at all (it isn't
watched by `inv_match`), so it doesn't even count as examined-and-skipped.

## Recipe 2 — a schedule that fires on its own

1. `CREATE SCHEDULE <name> EVERY '<n> <UNIT>' TX (<callback_name>)`.
2. In a `:memory:` interactive-feeling session, let real wall-clock time pass — a schedule fires on
   a timer, not on a statement you type, so you need the process to stay alive across the interval.
3. Validate with `.events status` — `fire_count` must have incremented, and `last_fire_at_ms` must
   be set (not `None`).
4. **If `fire_count` is `0`**, not enough wall-clock time has passed yet — this is not a
   fire-on-create primitive; check the interval you declared, not the DDL syntax.

### Worked example 1 — a 200ms heartbeat, ~1 second of runtime, human output

```bash
{ echo "CREATE SCHEDULE heartbeat EVERY '200 MILLISECONDS' TX (heartbeat_cb);"; sleep 1; echo ".events status"; } \
  | contextdb :memory:
```

```text
ok (rows_affected=0)
Event types:
  (none)
Sinks:
  (none)
Routes:
  (none)
Schedules:
  heartbeat EVERY 200 MILLISECONDS TX (heartbeat_cb) registered=true fired=4 next_fire_at_ms=1785985880032 last_fire_at_ms=Some(1785985894766)
```

~1 second at a 200ms period gives `fired=4` — consistent, not exact, since scheduling has jitter;
validate `fired > 0` and increasing, not an exact count.

### Worked example 2 — the same schedule, `--json`

```bash
{ echo "CREATE SCHEDULE heartbeat EVERY '200 MILLISECONDS' TX (heartbeat_cb);"; sleep 1; echo ".events status"; } \
  | contextdb :memory: --json
```

```json
{"rows_affected":0}
{"events":{"event_types":[],"routes":[],"schedules":[{"callback":"heartbeat_cb","callback_registered":true,"every":"200 MILLISECONDS","fire_count":4,"last_fire_at_ms":1785985899370,"name":"heartbeat","next_fire_at_ms":1785985899570}],"sinks":[]}}
```

## Recipe 3 — the file-backed store: durable queueing, then real delivery

1. Run the identical DDL against a **file-backed** path instead of `:memory:`. Confirm the durable
   queueing first — `.events status` shows `queued` growing and `delivered` staying `0`, and
   `callback_registered:false`.
2. Confirm durability: reopen the store in a **fresh process** and re-check `.events status` — the
   queue must still be there, not lost when the CLI session ended.
3. In your embedding program (not the CLI — there is no CLI verb for this), call
   `Database::register_sink(name, principal, deliver_fn)` for a sink, or
   `Database::register_cron_callback(name, callback_fn)` for a schedule's `TX (...)` callback,
   **before** opening the window where you need delivery. Registering replays the durable backlog
   in addition to future events.
4. **If `.events status` still shows `queued > 0` and `delivered` unmoving after you registered a
   callback**, check you registered against the SAME open `Database` handle the events queued
   against, and that the sink/callback name in Rust matches the DDL's name exactly — registration
   is by name, and a typo silently registers a callback nothing routes to.

### Worked example 1 — durable queueing, observed and confirmed to survive a restart

```bash
contextdb ./events.db --json <<'SQL'
CREATE TABLE invalidations (id UUID PRIMARY KEY, reason TEXT);
CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations;
CREATE SINK slack TYPE callback;
CREATE ROUTE inv_to_slack EVENT inv_match TO slack;
INSERT INTO invalidations (id, reason) VALUES ('11111111-1111-1111-1111-111111111111', 'basis changed');
.events status
SQL
```

```json
{"rows_affected":0}
{"rows_affected":0}
{"rows_affected":0}
{"rows_affected":0}
{"rows_affected":1}
{"events":{"event_types":[{"name":"inv_match","table":"invalidations","trigger":"INSERT"}],"routes":[{"event_type":"inv_match","name":"inv_to_slack","sink":"slack"}],"schedules":[],"sinks":[{"callback_registered":false,"delivered":0,"examined":0,"name":"slack","permanent_failures":0,"queued":1,"retried":0,"type":"CALLBACK"}]}}
```

`callback_registered:false, delivered:0, queued:1` — this is the queued-not-delivered state, and it
is correct: nobody has registered a callback yet.

```bash
echo ".events status" | contextdb ./events.db --json
```

```json
{"events":{"event_types":[{"name":"inv_match","table":"invalidations","trigger":"INSERT"}],"routes":[{"event_type":"inv_match","name":"inv_to_slack","sink":"slack"}],"schedules":[],"sinks":[{"callback_registered":false,"delivered":0,"examined":0,"name":"slack","permanent_failures":0,"queued":1,"retried":0,"type":"CALLBACK"}]}}
```

A **fresh process**, same file — `queued:1` survived. This is the durability the contract promises.

### Worked example 2 — registering the callback (Rust, the embedding pattern)

This is the pattern that makes a file-backed store actually deliver, adapted from the shipped test
suite's own round-trip proof (`crates/contextdb-engine/tests/checkpoint_export_tests.rs`,
`register_sink`/`register_cron_callback` call sites) — not hand-invented, since the CLI has no verb
for this and there's no way to execute it from a shell recipe:

```rust
use contextdb_engine::Database;
use std::sync::{Arc, Mutex};

let db = Database::open(std::path::Path::new("./events.db"))?;

// A sink's callback — fires once per queued event AND replays the durable
// backlog that accumulated before this callback existed.
let received = Arc::new(Mutex::new(Vec::new()));
let received_cb = received.clone();
db.register_sink("slack", None, move |event| {
    received_cb.lock().unwrap().push(event.clone());
    Ok(())
})?;

// A schedule's callback — the `TX (heartbeat_cb)` name from CREATE SCHEDULE
// binds to this by matching name.
db.register_cron_callback("heartbeat_cb", |handle| {
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), contextdb_core::Value::Uuid(uuid::Uuid::new_v4()));
    handle.execute("INSERT INTO cron_log (id) VALUES ($id)", &params)?;
    Ok(())
})?;
```

After this, `.events status` against the same store shows `callback_registered:true` and
`delivered` counting up — the exact shape recipe 1's worked examples show for `:memory:`, now on a
real file.

## Recipe 4 — `.events status` field reference

Run `.events status --json` and read this shape:

```json
{"events":{
  "event_types":[{"name":"...","table":"...","trigger":"INSERT|UPDATE"}],
  "sinks":[{"name":"...","type":"CALLBACK","callback_registered":true,"examined":N,"delivered":N,"queued":N,"retried":N,"permanent_failures":N}],
  "routes":[{"name":"...","event_type":"...","sink":"..."}],
  "schedules":[{"name":"...","every":"...","callback":"...","callback_registered":true,"fire_count":N,"last_fire_at_ms":N,"next_fire_at_ms":N}]
}}
```

- `examined` — every write that matched the event type's table+trigger, whether or not it was
  ultimately delivered.
- `delivered` — examined events whose callback ran successfully. `delivered <= examined` always.
- `queued` — examined events waiting for a callback to be registered (or waiting on a retry).
  Non-zero `queued` on a file-backed store with no callback registered is expected, per recipe 3.
- `retried` / `permanent_failures` — a registered callback that returned `Err` gets retried up to
  the engine's own retry policy; `permanent_failures` is what exhausted it, not what merely queued.
- `callback_registered` — `true` only once *something in this process* called `register_sink` /
  `register_cron_callback` for that name. It resets to `false` on a fresh process even though
  `queued` durably survives — registration is a runtime fact, the queue is a durable one.

**If you see `permanent_failures > 0`**, that is a real delivery failure your callback returned
`Err` for — go read your callback's error path, not the DDL.

## Gotchas

- **There is no CLI verb to register a real callback.** `:memory:` sessions get one for free from
  the CLI itself, purely so you can observe delivery without writing Rust; a file-backed
  production store needs your own program calling `register_sink`/`register_cron_callback`.
- **A schedule does not fire on `CREATE SCHEDULE` itself** — it fires on its own timer. Checking
  `.events status` immediately after creating one will show `fired=0`; that's correct, not a bug.
- **`queued` growing forever on a file-backed store with no registered callback is the intended
  steady state**, not a leak to chase — it is exactly the durable backlog recipe 3's callback
  registration is designed to drain (and replay) once you wire it.
- **Registering under the wrong name is a silent no-op**, not an error — `register_sink`/
  `register_cron_callback` succeed for any name; nothing checks it against declared DDL at
  registration time, only at delivery/fire time via `.events status`'s counters.

## Next

- The destructive-op and maintenance operator verbs (`.maintenance run`, `purge`, backups) → [`skills/operating-a-store/SKILL.md`](../operating-a-store/SKILL.md)
- Open a database, run SQL, read `--json` → [`skills/using-contextdb/SKILL.md`](../using-contextdb/SKILL.md)
- `PROPAGATE ON EDGE` cascades — the declarative graph-native alternative to a callback for state changes → [`skills/querying-the-graph/SKILL.md`](../querying-the-graph/SKILL.md)
