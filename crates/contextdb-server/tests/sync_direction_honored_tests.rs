//! Sync-policy direction — the declared direction is what actually happens.
//!
//! The declaration is worthless if it only renders. These drive the real client
//! and a real running hub over the in-process broker and assert what crossed the
//! wire: a table declared `SYNC OFF` or `SYNC PULL ONLY` never leaves the edge,
//! a table with no direction clause does (two-way is the default), and a
//! retained `SYNC TWO WAY` table's definition is accepted by the receiving hub.
//! No test here calls the runtime `set_table_direction` setter — the point is
//! that the DECLARATION alone decides, with no cooperation from the embedding
//! application.
//!
//! Before this change, the direction clause does not parse, and a retained
//! table is force-converted to push-only.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.
//! Assertions read rows and metadata after a driven exchange.

use contextdb_core::Lsn;
use contextdb_core::types::ContextId;
use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const TENANT: &str = "direction";
const HUB_NODE: &str = "hub-node-a";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// A bound on a hung exchange, not a timing assertion: every assertion below
/// reads state after the exchange returns.
async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"))
        .rows
        .len()
}

fn insert_rows(db: &Database, table: &str, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("{table}-{id}")));
        db.execute(
            &format!("INSERT INTO {table} (id, body) VALUES ($id, $body)"),
            &row,
        )
        .unwrap_or_else(|err| panic!("insert into {table} must succeed: {err}"));
    }
}

struct RunningHub {
    db: Arc<Database>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

/// A hub that has never heard of any of these tables, so what it ends up
/// holding is exactly what the edge sent it.
fn start_empty_hub(broker: &InProcessBroker) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        broker.server_as(HUB_NODE),
        TenantId::from(TENANT),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub { db, shutdown, task }
}

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, node_id: &str) -> SyncClient {
    SyncClient::with_transport(
        db.clone(),
        broker.client_as(node_id),
        TenantId::from(TENANT),
    )
}

/// A table declared `SYNC OFF` stays on the machine that wrote it — the
/// colocated-installation setting. The control table alongside it proves the
/// push itself worked, so this is not an empty-hub tautology.
#[tokio::test]
async fn c1d_a_declared_sync_off_table_never_leaves_the_edge() {
    let broker = InProcessBroker::new();
    let hub = start_empty_hub(&broker);

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE local_only (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
        &p(),
    )
    .expect("SYNC OFF must be declarable");
    edge.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &p(),
    )
    .expect("control table");
    insert_rows(&edge, "local_only", 1..6);
    insert_rows(&edge, "notes", 1..4);

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("edge push");

    assert_eq!(
        row_count(&hub.db, "notes"),
        3,
        "the control table proves the push path carried rows"
    );
    assert!(
        hub.db.table_meta("local_only").is_none(),
        "a SYNC OFF table's definition and rows never leave the edge — no runtime \
         setter was called, the declaration alone decided"
    );

    hub.stop().await;
}

/// `SYNC PULL ONLY` is the dashboard-shaped setting: this machine reads what
/// others wrote and sends nothing of its own.
#[tokio::test]
async fn c1d_a_declared_pull_only_table_never_leaves_the_edge() {
    let broker = InProcessBroker::new();
    let hub = start_empty_hub(&broker);

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE readings (id INTEGER PRIMARY KEY, body TEXT) SYNC PULL ONLY",
        &p(),
    )
    .expect("SYNC PULL ONLY must be declarable");
    edge.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &p(),
    )
    .expect("control table");
    insert_rows(&edge, "readings", 1..6);
    insert_rows(&edge, "notes", 1..4);

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("edge push");

    assert_eq!(
        row_count(&hub.db, "notes"),
        3,
        "the control table travelled"
    );
    assert!(
        hub.db.table_meta("readings").is_none(),
        "a PULL ONLY table sends nothing outbound, by declaration alone"
    );

    hub.stop().await;
}

/// A table with no direction clause is two-way, so it travels — the recovery
/// contract's default, stated as behavior rather than as a rendered word.
#[tokio::test]
async fn c1d_a_table_with_no_direction_clause_travels_by_default() {
    let broker = InProcessBroker::new();
    let hub = start_empty_hub(&broker);

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS",
        &p(),
    )
    .expect("plain retention, no direction declared");
    insert_rows(&edge, "notes", 1..4);

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("edge push");

    assert_eq!(
        row_count(&hub.db, "notes"),
        3,
        "declaring retention alone must not stop the rows travelling"
    );

    hub.stop().await;
}

/// The definition of a retained two-way table is accepted by the machine it
/// arrives at, and arrives carrying both declarations. Today the writing edge
/// cannot even declare it, and the arrival door refuses it outright.
#[tokio::test]
async fn c1f_a_retained_two_way_definition_is_accepted_by_the_receiving_hub() {
    let broker = InProcessBroker::new();
    let hub = start_empty_hub(&broker);

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY",
        &p(),
    )
    .expect("a retained two-way table must be declarable");
    insert_rows(&edge, "windows", 1..6);

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("edge push");

    let meta = hub
        .db
        .table_meta("windows")
        .expect("the arriving definition must be accepted, not refused");
    let rendered = contextdb_engine::cli_render::render_table_meta("windows", &meta);
    assert!(
        rendered.contains("RETAIN 48 HOURS SYNC SAFE") && rendered.contains("SYNC TWO WAY"),
        "the receiving machine must hold the declaration the writer made, got:\n{rendered}"
    );
    assert_eq!(
        row_count(&hub.db, "windows"),
        5,
        "and the rows travelled with it"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// The pull side — without it, "not outbound" cannot tell SYNC OFF apart from
// SYNC PULL ONLY, and "outbound" cannot tell SYNC PUSH ONLY apart from two-way.
// An implementation collapsing the four settings into two buckets would keep
// the very coupling this change removes and still pass a push-only suite.
// ---------------------------------------------------------------------------

/// A hub already holding rows, so a pull has something to deliver.
fn start_hub_holding(broker: &InProcessBroker, tables: &[&str]) -> RunningHub {
    let db = Arc::new(Database::open_memory());
    for table in tables {
        db.execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT)"),
            &p(),
        )
        .unwrap_or_else(|err| panic!("hub table {table} must create: {err}"));
        insert_rows(&db, table, 1..4);
    }
    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        broker.server_as(HUB_NODE),
        TenantId::from(TENANT),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub { db, shutdown, task }
}

/// `SYNC PULL ONLY` receives; `SYNC OFF` does not. Both are "never outbound",
/// so only the pull tells them apart.
#[tokio::test]
async fn c1d_a_pull_only_table_receives_while_a_sync_off_table_does_not() {
    let broker = InProcessBroker::new();
    let hub = start_hub_holding(&broker, &["readings", "local_only"]);

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE readings (id INTEGER PRIMARY KEY, body TEXT) SYNC PULL ONLY",
        &p(),
    )
    .expect("SYNC PULL ONLY must be declarable");
    edge.execute(
        "CREATE TABLE local_only (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
        &p(),
    )
    .expect("SYNC OFF must be declarable");

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.pull_default()).await.expect("edge pull");

    assert_eq!(
        row_count(&edge, "readings"),
        3,
        "a PULL ONLY table receives what the hub holds — that is the whole point \
         of the setting"
    );
    assert_eq!(
        row_count(&edge, "local_only"),
        0,
        "a SYNC OFF table receives nothing; it is not the same setting as PULL ONLY"
    );

    hub.stop().await;
}

/// Two-way receives; `SYNC PUSH ONLY` does not. Both are "outbound", so only the
/// pull tells them apart — and this is the recovery contract's default.
#[tokio::test]
async fn c1d_a_two_way_table_receives_while_a_push_only_table_does_not() {
    let broker = InProcessBroker::new();
    let hub = start_hub_holding(&broker, &["notes", "uploads"]);

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
        &p(),
    )
    .expect("SYNC TWO WAY must be declarable");
    edge.execute(
        "CREATE TABLE uploads (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY",
        &p(),
    )
    .expect("SYNC PUSH ONLY must be declarable");

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.pull_default()).await.expect("edge pull");

    assert_eq!(
        row_count(&edge, "notes"),
        3,
        "a two-way table reads back what other machines wrote"
    );
    assert_eq!(
        row_count(&edge, "uploads"),
        0,
        "a PUSH ONLY table sends and never receives; it is not the same setting \
         as two-way"
    );

    hub.stop().await;
}

/// The retained two-way table WITHOUT the delivery promise, honored on the wire:
/// its rows travel outbound like any two-way table's.
#[tokio::test]
async fn c1f_a_retained_two_way_table_without_the_delivery_promise_is_honored() {
    let broker = InProcessBroker::new();
    let hub = start_empty_hub(&broker);

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS SYNC TWO WAY",
        &p(),
    )
    .expect("plain retention with an explicit two-way direction must be declarable");
    insert_rows(&edge, "windows", 1..6);

    let client = edge_client(&edge, &broker, "edge-a");
    within(client.push()).await.expect("edge push");

    assert!(
        hub.db.table_meta("windows").is_some(),
        "the definition must be accepted by the receiving machine"
    );
    assert_eq!(
        row_count(&hub.db, "windows"),
        5,
        "and a two-way retained table's rows travel"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// The new direction filtering does not become a way around the scoping
// that was already there
// ---------------------------------------------------------------------------

/// Tenant scoping still holds. A machine declaring a perfectly ordinary two-way
/// table, but talking for a different tenant, does not get its rows into this
/// hub's database.
#[tokio::test]
async fn c1g_a_declared_direction_does_not_bypass_tenant_scoping() {
    let broker = InProcessBroker::new();
    let hub = start_empty_hub(&broker);

    let stranger = Arc::new(Database::open_memory());
    stranger
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &p(),
        )
        .expect("SYNC TWO WAY must be declarable");
    insert_rows(&stranger, "notes", 1..4);

    let other_tenant = SyncClient::with_transport(
        stranger.clone(),
        broker.client_as("edge-stranger"),
        TenantId::from("other-tenant"),
    );
    // The push may fail outright (nobody is listening for that tenant) or return;
    // either way what matters is what the hub ends up holding.
    let _ = within(other_tenant.push()).await;

    assert!(
        hub.db.table_meta("notes").is_none(),
        "rows pushed for a different tenant must not land in this tenant's hub, \
         however the table declares its direction"
    );

    hub.stop().await;
}

/// Context isolation still holds under the declared-direction path. A machine
/// scoped to context B does not receive rows another machine wrote in context A,
/// while a machine scoped to context A does — so this is isolation, not a broken
/// apply.
///
/// Two engine constraints shape how this is expressed, and neither is negotiable
/// here: DDL requires an admin handle (`executor.rs`), and — the one my first
/// draft did not account for — sync DDL apply ALSO requires an admin handle
/// (`database.rs`), so a context-scoped handle cannot receive a table's
/// definition over sync at all. The engine-native pattern (the one a downstream
/// consumer's cross-machine sync uses) is therefore: the schema is provisioned by an admin
/// handle, and only ROWS move onto the context-scoped handle, where the
/// apply-side `CONTEXT_ID` guard drops any row outside the handle's context.
/// This test provisions a DIRECTION-DECLARED table by admin on every machine,
/// then applies the writer's real synced row changeset — the same public
/// `apply_changes` door `SyncClient::pull` drives internally — onto each scoped
/// receiver. The direction clause is on the table throughout; the point is that
/// adding it does not become a way around the context guard.
#[test]
fn c1g_a_declared_direction_does_not_bypass_context_isolation() {
    let context_a = ContextId::new(Uuid::from_u128(0xA));
    let context_b = ContextId::new(Uuid::from_u128(0xB));

    // A direction-declared table with a context column, so the apply-side guard
    // has a context to check each arriving row against.
    let create_notes = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, \
                        context_id UUID CONTEXT_ID) SYNC TWO WAY";

    let dir = tempfile::tempdir().expect("tempdir");
    let writer_path = dir.path().join("writer.db");
    let outsider_path = dir.path().join("outsider.db");
    let sibling_path = dir.path().join("sibling.db");

    // The writer creates the table and seeds three rows, all in context A, then
    // hands out its synced row changeset.
    let writer = Database::open(&writer_path).expect("open writer");
    writer
        .execute(create_notes, &p())
        .expect("a direction-declared table with a context column must be declarable");
    for id in 1..4i64 {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("note-{id}")));
        row.insert("ctx".to_string(), Value::Uuid(context_a.0));
        writer
            .execute(
                "INSERT INTO notes (id, body, context_id) VALUES ($id, $body, $ctx)",
                &row,
            )
            .expect("seed a context-A row");
    }
    // Rows only — the schema is provisioned on each receiver by its own admin
    // handle, exactly as an operator provisions it out of band; a context-scoped
    // handle cannot take DDL over sync.
    let full = writer.changes_since(Lsn(0));
    let rows_only = ChangeSet {
        rows: full.rows,
        ..Default::default()
    };
    assert_eq!(rows_only.rows.len(), 3, "three real context-A row changes");

    // The outsider provisions the same table and applies the changeset on a
    // handle scoped to context B: the guard drops every context-A row.
    let outsider_admin = Database::open(&outsider_path).expect("open outsider admin");
    outsider_admin
        .execute(create_notes, &p())
        .expect("create on outsider");
    outsider_admin.close().expect("close outsider admin");
    let outsider = Database::open_with_contexts(&outsider_path, BTreeSet::from([context_b]))
        .expect("reopen outsider scoped to context B");
    outsider
        .apply_changes(
            rows_only.clone(),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("applying rows on a scoped handle skips out-of-context rows, it does not error");
    assert_eq!(
        row_count(&outsider, "notes"),
        0,
        "a machine scoped to another context receives none of those rows, direction \
         clause or not"
    );

    // A second machine scoped to context A applies the same changeset and gets
    // all three — proving the zero above is isolation, not a dropped changeset.
    let sibling_admin = Database::open(&sibling_path).expect("open sibling admin");
    sibling_admin
        .execute(create_notes, &p())
        .expect("create on sibling");
    sibling_admin.close().expect("close sibling admin");
    let sibling = Database::open_with_contexts(&sibling_path, BTreeSet::from([context_a]))
        .expect("reopen sibling scoped to context A");
    sibling
        .apply_changes(
            rows_only,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("applying rows on a same-context handle succeeds");
    assert_eq!(
        row_count(&sibling, "notes"),
        3,
        "while a machine in the SAME context receives them — the apply path works"
    );
}

/// A NON-retained `SYNC PUSH ONLY` table must be suppressed at the
/// SERVER serve path too, not only by a receiving edge that already declared the
/// table. A FRESH edge that has never heard of the table learns its DDL from the
/// pull and has an EMPTY direction map (an undeclared table defaults to two-way),
/// so the client-side filter cannot drop the rows — only the hub's serve-time
/// drop can. Before this change the drop gates on retention (`default_ttl_seconds.is_some()`),
/// so a non-retained push-only table escapes it and its rows are replanted on the
/// fresh edge. Distinct from `c1d_a_two_way_table_receives_while_a_push_only_table_does_not`,
/// which PRE-DECLARES the table and passes on the client filter.
#[tokio::test]
async fn a_non_retained_push_only_table_is_suppressed_at_the_server_for_a_fresh_edge() {
    let broker = InProcessBroker::new();

    // The hub itself holds a NON-retained SYNC PUSH ONLY table with rows.
    let hub_db = Arc::new(Database::open_memory());
    hub_db
        .execute(
            "CREATE TABLE uploads (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY",
            &p(),
        )
        .expect("hub declares a non-retained push-only table");
    insert_rows(&hub_db, "uploads", 1..4);
    assert_eq!(
        hub_db.table_meta("uploads").and_then(|m| m.sync_direction),
        Some(contextdb_core::SyncDirection::Push),
        "fixture: the hub's table is push-only and NOT retained",
    );
    assert!(
        hub_db
            .table_meta("uploads")
            .and_then(|m| m.default_ttl_seconds)
            .is_none(),
        "fixture: the table declares no retention, so the pre-fix drop skips it",
    );
    let server = Arc::new(SyncServer::with_transport(
        hub_db.clone(),
        broker.server_as(HUB_NODE),
        TenantId::from(TENANT),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    let hub = RunningHub {
        db: hub_db,
        shutdown,
        task,
    };

    // A brand-new edge that has never declared `uploads` pulls from the hub.
    let edge = Arc::new(Database::open_memory());
    let client = edge_client(&edge, &broker, "fresh-edge");
    within(client.pull_default())
        .await
        .expect("fresh edge pull");

    // The DDL still travels (a table must be able to ARRIVE by sync) ...
    assert!(
        edge.table_meta("uploads").is_some(),
        "the push-only table's DDL still reaches a fresh edge so the table exists",
    );
    // ... but not one push-only row is served back.
    assert_eq!(
        row_count(&edge, "uploads"),
        0,
        "a push-only table is never served back — the hub suppresses its rows at \
         serve time, regardless of retention",
    );

    hub.stop().await;
}
