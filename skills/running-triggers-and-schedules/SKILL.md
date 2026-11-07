---
name: running-triggers-and-schedules
description: Declare events, sinks, routes and schedules, inspect durable queueing from the CLI, and register embedded host callbacks for delivery and schedule execution.
---

# Running triggers and schedules

The CLI declares `CREATE EVENT TYPE`, `CREATE SINK`, `CREATE ROUTE` and `CREATE SCHEDULE`,
and inspects them with `.events status`. It registers no callbacks. Matching events queue
durably until an embedded host registers `register_sink`; a schedule runs only while a host
with its named `register_cron_callback` is active. There is no in-memory store mode.

**Prerequisite:** needs the `contextdb` binary. Build with
`cargo build --release -p contextdb-cli`, or see
[getting started](../../docs/getting-started.md).

## Recipe 1 — declare a route and inspect its durable queue

Create the watched table, event type, callback sink and route, then insert a matching row.
With no host callback, `.events status` reports `callback_registered:false`, `delivered:0`
and a queued event. Reopen the same temporary-file path to inspect the durable backlog.
The transcript below illustrates the shape; timestamps vary.

### Durable queueing across restart

```bash
db="$(mktemp -d)/events.db"
contextdb "$db" --write --json <<'SQL'
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
{"events_status":{"continuation":null,"has_more":false,"items":[{"kind":"event_type","name":"inv_match","table":"invalidations","trigger":"INSERT"},{"callback_registered":false,"delivered":0,"examined":0,"kind":"sink","name":"slack","permanent_failures":0,"queued":1,"retried":0,"type":"CALLBACK","uncertain":0},{"event_type":"inv_match","kind":"route","name":"inv_to_slack","sink":"slack"}]}}
```

`callback_registered:false, delivered:0, queued:1` — this is the queued-not-delivered state, and it
is correct: nobody has registered a callback yet.

```bash
echo ".events status" | contextdb "$db" --json
```

```json
{"notice":{"class":"io","detail":{"kind":"read_route","route":"file","snapshot_at":"2026-08-30T02:41:07Z"},"message":"reading the committed snapshot taken at 2026-08-30T02:41:07Z"}}
{"events_status":{"continuation":null,"has_more":false,"items":[{"kind":"event_type","name":"inv_match","table":"invalidations","trigger":"INSERT"},{"callback_registered":false,"delivered":0,"examined":0,"kind":"sink","name":"slack","permanent_failures":0,"queued":1,"retried":0,"type":"CALLBACK","uncertain":0},{"event_type":"inv_match","kind":"route","name":"inv_to_slack","sink":"slack"}]}}
```

Both lines are shown because both reach the terminal. The first is the read-route notice every
read-only session prints, carrying its own timestamp; it goes to **stderr**, so a stdout-only
capture — or a `2>/dev/null` — sees just the `events_status` document.

A **fresh process**, same file — `queued:1` survived. This is the durability the contract promises.

## Recipe 2 — declare a schedule

```bash
contextdb "$(mktemp -d)/schedule.db" --write --json <<'SQL'
CREATE SCHEDULE heartbeat EVERY '200 MILLISECONDS' TX (heartbeat_cb);
.events status
SQL
```

This declares a durable schedule; it does not register `heartbeat_cb` or prove that it fired.
The host in recipe 3 must register that name and stay alive across the declared interval.
Inspect `callback_registered`, `fire_count` and `last_fire_at_ms` through the host.

## Recipe 3 — register callbacks in an embedded host

This is the pattern that makes a file-backed store actually deliver, adapted from the shipped test
suite's own round-trip proof (`crates/contextdb-engine/tests/engine/checkpoint_export_tests.rs`,
`register_sink`/`register_cron_callback` call sites) — not hand-invented, since the CLI has no verb
for this and there's no way to execute it from a shell recipe.

This half is a Rust program of your own, not the CLI, so your crate needs the dependencies —
`contextdb-engine` does not re-export `contextdb-core` or `uuid`:

```toml
[dependencies]
contextdb-engine = "1.0.0"
contextdb-core = "1.0.0"
uuid = { version = "1", features = ["v4"] }
```

```rust
use contextdb_engine::Database;
use std::sync::{Arc, Mutex};

// Pass the same durable path created above as the first program argument.
let events_path = std::env::args().nth(1).expect("pass the events.db path");
let db = Database::open(std::path::Path::new(&events_path))?;
db.execute("CREATE TABLE cron_log (id UUID PRIMARY KEY)", &Default::default())?;
db.execute("CREATE SCHEDULE heartbeat EVERY '200 MILLISECONDS' TX (heartbeat_cb)", &Default::default())?;

// A sink's callback — receives queued events and replays the durable
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

Keep this host and its `Database` handle alive for delivery and schedule execution. Inspect
status through that host to observe registered callbacks and successful delivery. Registration
is process-local: a separate CLI process does not register or inherit these callbacks. External
delivery may retry; handlers should deduplicate by event identity.

## Recipe 4 — `.events status` field reference

Run `.events status` in a `--json` session and read this shape — one bounded page whose `items`
are tagged by `kind`, so a consumer filters rather than indexing four parallel arrays:

```json
{"events_status":{
  "items":[
    {"kind":"event_type","name":"...","table":"...","trigger":"INSERT|UPDATE"},
    {"kind":"sink","name":"...","type":"CALLBACK","callback_registered":true,"examined":N,"delivered":N,"queued":N,"retried":N,"uncertain":N,"permanent_failures":N},
    {"kind":"route","name":"...","event_type":"...","sink":"..."},
    {"kind":"schedule","name":"...","every":"...","callback":"...","callback_registered":true,"fire_count":N,"last_fire_at_ms":N,"next_fire_at_ms":N}
  ],
  "has_more":false,
  "continuation":null
}}
```

`items` is a page, not the whole world: it carries as many complete items as fit the declared
`result_bytes`, and `continuation` is a string exactly when `has_more` is true. Resume with
`.events status --continue <continuation>` — that continuation is accepted only by
`.events status`. Pick one kind out with `jq`, e.g.
`jq '.events_status.items[] | select(.kind == "sink")'`.

- `examined` — every write that matched the event type's table+trigger, whether or not it was
  ultimately delivered.
- `delivered` — examined events whose callback ran successfully. `delivered <= examined` always.
- `queued` — examined events waiting for a callback to be registered (or waiting on a retry).
  Non-zero `queued` on a file-backed store with no callback registered is expected, per recipe 3.
- `retried` — attempts that returned `SinkError::Transient` or `SinkError::OutcomeUncertain` (or
  panicked) and stay pending; the dispatcher retries them with backoff and the same
  `SinkEvent::action_id`, so the queue entry is never dropped for being retried.
- `uncertain` — external attempts whose outcome could not be confirmed: the callback returned
  `SinkError::OutcomeUncertain`, panicked after delivery began, or the process died mid-delivery.
  The action stays pending and is retried with the same `action_id`; this count does not say
  whether the recipient saw it, so a receiver that deduplicates on `action_id` absorbs the retry.
  It is reported separately from `delivered` and `permanent_failures`, and a read-only session
  reports it too.
  <!-- enforced by: crates/contextdb-cli/src/json_output.rs::event_status_json_reports_uncertainty_separately, tests/integration/transactional_callback_delivery.rs::lost_acknowledgement_retries_same_identity_and_deduplicates_visible_event, tests/integration/transactional_callback_delivery.rs::crash_during_external_delivery_recovers_uncertainty_before_retry -->
- `permanent_failures` — attempts the callback refused with `SinkError::Permanent`; the entry is
  acknowledged and removed, not retried.
- `callback_registered` — `true` only once *something in this process* called `register_sink` /
  `register_cron_callback` for that name. It resets to `false` on a fresh process even though
  `queued` durably survives — registration is a runtime fact, the queue is a durable one.

**If you see `permanent_failures > 0`**, that is a real delivery failure your callback returned
`SinkError::Permanent` for — go read your callback's error path, not the DDL.

## Gotchas

- **There is no CLI verb to register a callback.** Your running host must call
  `register_sink`/`register_cron_callback` on its open `Database`.
- **A schedule does not fire on `CREATE SCHEDULE` itself** — it fires on its own timer. Checking
  `.events status` immediately after creating one will show `fired=0`; that's correct, not a bug.
- **`queued` growing forever on a file-backed store with no registered callback is the intended
  steady state**, not a leak to chase — it is exactly the durable backlog recipe 3's callback
  registration is designed to drain (and replay) once you wire it.
- **Registering under the wrong name is a silent no-op**, not an error — `register_sink`/
  `register_cron_callback` succeed for any name; nothing checks it against declared DDL at
  registration time, only at delivery/fire time via `.events status`'s counters.

A trigger callback cannot remove its own trigger with `DROP TRIGGER`, including through the
transaction-bound handle: it receives `Error::TriggerRequiresAdmin`, and the declaration stays
active. Drop triggers from an administrative handle outside the callback.
<!-- enforced by: tests/acceptance/trigger.rs::t3_reg_introspection_and_delete_event_rejection -->

## Next

- The destructive-op and maintenance operator verbs (`.maintenance run`, `purge`, backups) → [`skills/operating-a-store/SKILL.md`](../operating-a-store/SKILL.md)
- Open a database, run SQL, read `--json` → [`skills/using-contextdb/SKILL.md`](../using-contextdb/SKILL.md)
- `PROPAGATE ON EDGE` cascades — the declarative graph-native alternative to a callback for state changes → [`skills/querying-the-graph/SKILL.md`](../querying-the-graph/SKILL.md)
