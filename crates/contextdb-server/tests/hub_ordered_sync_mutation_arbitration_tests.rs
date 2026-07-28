//! Hub-ordered sync mutation arbitration journeys: declared policy, real
//! SyncClient/Server exchange, and exact hub plus edge state.  No policy map
//! is injected per scenario; each database reads `SYNC CONFLICT` from its
//! declared DDL.

use contextdb_core::{TenantId, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "hub-ordered-mutations";
const LATEST: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST HISTORY CURRENT ONLY";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("bounded production sync exchange exceeded 60s")
}
async fn pull_live(
    client: &SyncClient,
) -> contextdb_core::Result<contextdb_engine::sync_types::ApplyResult> {
    for _ in 0..10_000 {
        match client.pull_default().await {
            Err(error) if error.to_string().contains("no responder") => {
                tokio::task::yield_now().await
            }
            result => return result,
        }
    }
    panic!("hub did not register")
}

struct Hub {
    db: Arc<Database>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

fn start_hub(broker: &InProcessBroker) -> Hub {
    start_hub_ddl(broker, LATEST)
}
fn start_hub_ddl(broker: &InProcessBroker, ddl: &str) -> Hub {
    let db = Arc::new(Database::open_memory());
    db.execute(ddl, &p()).expect("hub declared table");
    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        broker.server_as("mutation-arbitration-hub"),
        TenantId::from(TENANT),
        db.conflict_policies(),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub { db, stop, task }
}
fn start_hub_db(broker: &InProcessBroker, identity: &str, db: Arc<Database>) -> Hub {
    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        broker.server_as(identity),
        TenantId::from(TENANT),
        db.conflict_policies(),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub { db, stop, task }
}

fn edge(broker: &InProcessBroker, node: &str) -> (Arc<Database>, SyncClient) {
    edge_ddl(broker, node, LATEST)
}
fn edge_ddl(broker: &InProcessBroker, node: &str, ddl: &str) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(Database::open_memory());
    db.execute(ddl, &p()).expect("edge declared table");
    let client =
        SyncClient::with_transport(db.clone(), broker.client_as(node), TenantId::from(TENANT));
    (db, client)
}

async fn seed(a: &Database, ac: &SyncClient, b: &Database, bc: &SyncClient) {
    write(a, "first");
    within(ac.push()).await.expect("seed push");
    within(bc.pull_default()).await.expect("seed pull");
    assert_eq!(body(b).as_deref(), Some("first"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_latest_edit_then_delete_is_absent_everywhere() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let (a, ac) = edge(&broker, "ord-ed");
    let (b, bc) = edge(&broker, "ord-ed-b");
    seed(&a, &ac, &b, &bc).await;
    delete(&a);
    within(ac.push()).await.expect("delete push");
    within(bc.pull_default()).await.expect("delete pull");
    within(ac.pull_default()).await.expect("settle");
    assert_all(&hub.db, &a, &b, None);
    hub.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_latest_delete_then_edit_keeps_the_later_value_everywhere() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let (a, ac) = edge(&broker, "ord-de");
    let (b, bc) = edge(&broker, "ord-de-b");
    seed(&a, &ac, &b, &bc).await;
    delete(&a);
    within(ac.push()).await.expect("delete push");
    within(bc.pull_default()).await.expect("delete pull");
    write(&b, "later");
    within(bc.push()).await.expect("later edit");
    within(ac.pull_default()).await.expect("pull later");
    assert_all(&hub.db, &a, &b, Some("later"));
    hub.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_keep_first_later_synced_delete_keeps_the_first_value_everywhere() {
    let ddl = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST";
    let broker = InProcessBroker::new();
    let hub = start_hub_ddl(&broker, ddl);
    let (a, ac) = edge_ddl(&broker, "first-a", ddl);
    let (b, bc) = edge_ddl(&broker, "first-b", ddl);
    seed(&a, &ac, &b, &bc).await;
    delete(&b);
    within(bc.push()).await.expect("later delete push");
    within(ac.pull_default()).await.expect("first pull");
    within(bc.pull_default()).await.expect("restore first");
    assert_all(&hub.db, &a, &b, Some("first"));
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_key_synced_delete_removes_the_whole_natural_key_everywhere() {
    let ddl = "CREATE TABLE notes (a INTEGER, b INTEGER, body TEXT, PRIMARY KEY (a,b)) SYNC CONFLICT KEEP LATEST";
    let broker = InProcessBroker::new();
    let hub = start_hub_ddl(&broker, ddl);
    let (a, ac) = edge_ddl(&broker, "cmp-a", ddl);
    let (b, bc) = edge_ddl(&broker, "cmp-b", ddl);
    a.execute("INSERT INTO notes VALUES (1,2,'present')", &p())
        .unwrap();
    within(ac.push()).await.unwrap();
    within(bc.pull_default()).await.unwrap();
    b.execute("DELETE FROM notes WHERE a=1 AND b=2", &p())
        .unwrap();
    within(bc.push()).await.unwrap();
    within(ac.pull_default()).await.unwrap();
    for db in [&hub.db, &a, &b] {
        assert!(
            db.execute("SELECT * FROM notes WHERE a=1 AND b=2", &p())
                .unwrap()
                .rows
                .is_empty(),
            "whole composite key must be absent"
        );
    }
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_vector_owner_delete_reaches_row_and_ann_absence_everywhere() {
    let ddl = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3)) SYNC CONFLICT KEEP LATEST";
    let broker = InProcessBroker::new();
    let hub = start_hub_ddl(&broker, ddl);
    let (a, ac) = edge_ddl(&broker, "vec-init-a", ddl);
    let (b, bc) = edge_ddl(&broker, "vec-init-b", ddl);
    let id = "00000000-0000-0000-0000-000000000002";
    a.execute(
        &format!("INSERT INTO notes VALUES ('{id}','present','[1,0,0]')"),
        &p(),
    )
    .unwrap();
    within(ac.push())
        .await
        .expect("initial vector-bearing owner push");
    within(bc.pull_default())
        .await
        .expect("initial vector-bearing owner pull");
    for db in [&hub.db, &a, &b] {
        assert!(
            !db.execute(
                &format!("SELECT body,embedding FROM notes WHERE id='{id}'"),
                &p()
            )
            .unwrap()
            .rows
            .is_empty(),
            "fixture owner/vector row exists before delete"
        );
        let ann = db
            .query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[1.0, 0.0, 0.0],
                10,
                None,
                db.snapshot(),
            )
            .expect("ANN query before delete");
        assert!(
            !ann.is_empty() && ann[0].1 > 0.99,
            "owner vector must be ANN-visible before delete: {ann:?}"
        );
    }
    b.execute(&format!("DELETE FROM notes WHERE id='{id}'"), &p())
        .unwrap();
    within(bc.push()).await.expect("vector owner delete push");
    within(ac.pull_default())
        .await
        .expect("vector owner delete pull");
    for db in [&hub.db, &a, &b] {
        assert!(
            db.execute(
                &format!("SELECT body,embedding FROM notes WHERE id='{id}'"),
                &p()
            )
            .unwrap()
            .rows
            .is_empty(),
            "row and vector payload absent after delete"
        );
        assert!(
            db.query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[1.0, 0.0, 0.0],
                10,
                None,
                db.snapshot()
            )
            .expect("ANN query after delete")
            .is_empty(),
            "ANN owner must be absent after delete"
        );
    }
    hub.stop().await;
}

fn write(db: &Database, body: &str) {
    let mut values = p();
    values.insert("body".into(), Value::Text(body.into()));
    db.execute("INSERT INTO notes (id, body) VALUES (1, $body) ON CONFLICT (id) DO UPDATE SET body = $body", &values)
        .unwrap_or_else(|error| panic!("write {body}: {error}"));
}
fn delete(db: &Database) {
    db.execute("DELETE FROM notes WHERE id = 1", &p())
        .expect("delete notes row");
}
fn body(db: &Database) -> Option<String> {
    db.execute("SELECT body FROM notes WHERE id = 1", &p())
        .expect("scan notes")
        .rows
        .first()
        .map(|row| match &row[0] {
            Value::Text(v) => v.clone(),
            other => panic!("body value: {other:?}"),
        })
}
fn assert_all(hub: &Database, a: &Database, b: &Database, expected: Option<&str>) {
    for (name, db) in [("hub", hub), ("edge-a", a), ("edge-b", b)] {
        assert_eq!(body(db).as_deref(), expected, "{name} exact final value");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_latest_distinguishes_hub_accepted_echo_from_an_unpushed_local_mutation() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let (a, a_client) = edge(&broker, "accepted-echo-edge-a");
    let (b, b_client) = edge(&broker, "accepted-echo-edge-b");

    // A's local clock is high but the churn table is declared SYNC OFF, so
    // none of it can manufacture a hub arrival position.
    a.execute("CREATE TABLE churn (id INTEGER PRIMARY KEY) SYNC OFF", &p())
        .expect("declared local-only churn");
    for id in 1..=128 {
        a.execute(&format!("INSERT INTO churn VALUES ({id})"), &p())
            .expect("high local position");
    }
    write(&a, "a-accepted");
    within(a_client.push()).await.expect("A hub-accepted write");
    within(b_client.pull_default())
        .await
        .expect("B sees accepted A");
    write(&b, "hub-later");
    within(b_client.push()).await.expect("B later hub write");
    within(a_client.pull_default())
        .await
        .expect("A pulls B later hub value");
    assert_eq!(
        body(&a).as_deref(),
        Some("hub-later"),
        "a hub-accepted echo pending locally must not reject a later hub value"
    );

    write(&a, "a-unpushed");
    assert!(
        a_client
            .has_pending_push_changes()
            .expect("pending local mutation"),
        "the unpushed mutation must remain offerable"
    );
    write(&b, "hub-while-a-pending");
    within(b_client.push())
        .await
        .expect("B commits while A is pending");
    within(a_client.pull_default())
        .await
        .expect("A pulls real newer hub mutation while still pending");
    assert_eq!(
        body(&a).as_deref(),
        Some("a-unpushed"),
        "pull must not discard a still-unpushed local mutation"
    );
    assert!(
        a_client
            .has_pending_push_changes()
            .expect("pending mutation survives pull"),
        "pull must not advance A's push watermark past its own pending write"
    );
    within(a_client.push())
        .await
        .expect("A offers still-pending write for hub ordering");
    within(b_client.pull_default())
        .await
        .expect("B pulls A hub-accepted final");
    within(a_client.pull_default())
        .await
        .expect("A receives settled hub state");
    assert_all(&hub.db, &a, &b, Some("a-unpushed"));
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_pulled_delete_is_not_reoffered_to_erase_a_later_hub_edit() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    let (a, a_client) = edge(&broker, "pulled-tombstone-edge-a");
    let (b, b_client) = edge(&broker, "pulled-tombstone-edge-b");
    write(&a, "before-delete");
    within(a_client.push()).await.expect("seed hub");
    within(b_client.pull_default())
        .await
        .expect("B receives seed");
    delete(&a);
    within(a_client.push()).await.expect("hub accepts delete");
    within(b_client.pull_default())
        .await
        .expect("B receives pulled tombstone");
    assert_all(&hub.db, &a, &b, None);
    // Keep B untouched: it still holds the tombstone that ARRIVED by sync.
    // A authors the later hub edit; B's subsequent push must not offer the
    // older pulled tombstone back to erase it.
    write(&a, "later-edit");
    assert_eq!(
        body(&a).as_deref(),
        Some("later-edit"),
        "fixture: A has the later local edit before hub ordering"
    );
    within(a_client.push())
        .await
        .expect("hub accepts edit after delete");
    assert_eq!(
        body(&hub.db).as_deref(),
        Some("later-edit"),
        "fixture: hub accepted the newer edit before B can echo its tombstone"
    );
    within(b_client.push())
        .await
        .expect("B does not reoffer pulled tombstone");
    within(a_client.pull_default())
        .await
        .expect("A receives settled hub state");
    within(b_client.pull_default())
        .await
        .expect("B receives newer edit after its empty push");
    assert_all(&hub.db, &a, &b, Some("later-edit"));
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_rebind_then_ordinary_later_write_converges_everywhere() {
    let broker = InProcessBroker::new();
    let identity = "source-rebind-hub";
    let hub1db = Arc::new(Database::open_memory());
    hub1db.execute(LATEST, &p()).unwrap();
    let hub1 = start_hub_db(&broker, identity, hub1db);
    let (a, ac) = edge(&broker, "rebind-a");
    let (b, bc) = edge(&broker, "rebind-b");
    write(&a, "generation-one");
    within(ac.push()).await.unwrap();
    pull_live(&ac).await.expect("A binds generation-one source");
    within(bc.pull_default()).await.unwrap();
    hub1.stop().await;
    let hub2db = Arc::new(Database::open_memory());
    hub2db.execute(LATEST, &p()).unwrap();
    write(&hub2db, "rebuilt");
    for id in 2..40 {
        hub2db
            .execute(&format!("INSERT INTO notes VALUES ({id},'filler')"), &p())
            .unwrap();
    }
    let hub2 = start_hub_db(&broker, identity, hub2db.clone());
    pull_live(&ac).await.expect("A source re-adopts");
    pull_live(&bc).await.expect("B source re-adopts");
    assert_eq!(body(&a).as_deref(), Some("rebuilt"));
    assert_eq!(body(&b).as_deref(), Some("rebuilt"));
    write(&b, "later-after-rebind");
    within(bc.push()).await.unwrap();
    pull_live(&ac).await.unwrap();
    pull_live(&bc).await.unwrap();
    assert_all(&hub2.db, &a, &b, Some("later-after-rebind"));
    hub2.stop().await;
}
