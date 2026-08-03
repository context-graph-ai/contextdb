//! A dependency-complete memory larger than one Iroh frame still arrives whole.
//!
//! The production ticketed-Iroh path is intentional here.  This test does not
//! use the blob plane, an in-process broker, or a transport double: the encoded
//! source batch must exceed the real 64-MiB framed-request ceiling before it is
//! offered to the hub.

use contextdb_core::{Lsn, TenantId, Value, VersionedRow};
use contextdb_engine::Database;
use contextdb_server::protocol::{MessageType, PushRequest, encode, wire_changeset_with_arrivals};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{
    FabricIdentity, SyncClient, SyncServer, acceptance_stamped_push_batches_for_test,
    peer_dial_spec,
};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const TENANT: &str = "oversized-dependency-unit";
const IROH_FRAME_CEILING_BYTES: usize = 64 * 1024 * 1024;

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded production ticketed-Iroh exchange")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn declare_tables(db: &Database) {
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare parent table");
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare child table");
}

fn insert_parent(db: &Database, id: Uuid, body: String) {
    db.execute(
        "INSERT INTO parents (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body)),
        ]),
    )
    .expect("insert parent");
}

fn update_parent(db: &Database, id: Uuid, body: String) {
    db.execute(
        "UPDATE parents SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body)),
        ]),
    )
    .expect("update parent");
}

fn insert_child(db: &Database, id: Uuid, parent_id: Uuid) {
    db.execute(
        "INSERT INTO children (id, parent_id, body) VALUES ($id, $parent_id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
            ("body".to_string(), Value::Text("outcome".to_string())),
        ]),
    )
    .expect("insert child");
}

fn row(db: &Database, table: &str, id: Uuid) -> Option<VersionedRow> {
    db.point_lookup(table, "id", &Value::Uuid(id), db.snapshot())
        .expect("point lookup")
}

struct Hub {
    db: Arc<Database>,
    endpoint: IrohServer,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind authenticated Iroh hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub database"));
    declare_tables(&db);
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(TENANT),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        endpoint,
        ticket,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task)
            .await
            .expect("authenticated Iroh hub stops within the test bound");
        self.endpoint.close().await;
    }
}

/// The source unit contains a parent revision whose real encoded PushRequest
/// exceeds Iroh's 64-MiB ceiling, plus its outcome child. The source first
/// bootstraps its DDL, then updates its pending parent before the first data
/// sync, so the child-before-parent ordering defect cannot mask the real
/// frame-boundary transport path.
#[tokio::test]
async fn oversized_dependency_unit_uses_the_authenticated_sync_fallback_and_commits_once() {
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let edge_node_id = FabricIdentity::load_or_generate(&identity_path)
        .expect("persist edge identity")
        .node_id();
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
    declare_tables(&edge);
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT));

    within(client.push())
        .await
        .expect("bootstrap DDL reaches the authenticated hub before the oversized unit");
    let parent_id = Uuid::new_v4();
    insert_parent(&edge, parent_id, "decision-before-evidence".to_string());
    let source_before = client.push_watermark();
    let hub_before = hub.db.current_lsn();
    let incarnation = edge
        .sync_incarnation(&TenantId::from(TENANT))
        .expect("read durable edge incarnation");
    let hub_edge_watermark_before = hub
        .db
        .persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(TENANT),
            &edge_node_id,
            incarnation,
        )
        .expect("read hub per-edge watermark");

    let child_id = Uuid::new_v4();
    insert_child(&edge, child_id, parent_id);
    update_parent(&edge, parent_id, "x".repeat(IROH_FRAME_CEILING_BYTES));
    let source_final_lsn = edge.current_lsn();

    let (changes, arrivals) = edge.changes_since_with_arrivals(source_before);
    let largest_request = acceptance_stamped_push_batches_for_test(changes)
        .into_iter()
        .map(|batch| {
            encode(
                MessageType::PushRequest,
                &PushRequest {
                    changeset: wire_changeset_with_arrivals(batch, &arrivals),
                    incarnation,
                },
            )
            .expect("encode the exact production PushRequest shape")
        })
        .max_by_key(Vec::len)
        .expect("oversized source writes create a production push batch");
    assert!(
        largest_request.len() > IROH_FRAME_CEILING_BYTES,
        "the test must drive an actual encoded dependency batch above Iroh's frame ceiling; got {} bytes",
        largest_request.len()
    );
    drop(largest_request);

    let pushed = within(client.push()).await;
    assert!(
        pushed.is_ok(),
        "an oversized dependency unit must use authenticated ordinary-sync fallback instead of failing at Iroh's frame ceiling: {pushed:?}"
    );

    let hub_parent = row(&hub.db, "parents", parent_id)
        .expect("the hub receives the final parent with its outcome child");
    let hub_child = row(&hub.db, "children", child_id)
        .expect("the hub never exposes the outcome child without its final parent");
    assert!(
        matches!(
            hub_parent.values.get("body"),
            Some(Value::Text(body)) if body.len() == IROH_FRAME_CEILING_BYTES
        ),
        "the hub retains the final oversized parent revision"
    );
    assert_eq!(
        hub_child.values.get("parent_id"),
        Some(&Value::Uuid(parent_id)),
        "the outcome retains its declared parent reference"
    );

    let dependency_lsns = hub
        .db
        .changes_since(hub_before)
        .rows
        .iter()
        .filter(|change| {
            (change.table == "parents"
                && change
                    .natural_key
                    .key_values()
                    .contains(&Value::Uuid(parent_id)))
                || (change.table == "children"
                    && change
                        .natural_key
                        .key_values()
                        .contains(&Value::Uuid(child_id)))
        })
        .map(|change| change.lsn)
        .collect::<BTreeSet<Lsn>>();
    assert_eq!(
        dependency_lsns.len(),
        1,
        "the complete dependency unit receives one hub acceptance position"
    );
    assert_eq!(
        client.push_watermark(),
        source_final_lsn,
        "source progress advances only after the complete oversized unit is accepted"
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &TenantId::from(TENANT),
                &edge_node_id,
                incarnation,
            )
            .expect("read final hub per-edge watermark"),
        Some(source_final_lsn),
        "the hub advances the per-edge receipt only for the completed unit"
    );
    assert_ne!(
        hub_edge_watermark_before,
        Some(source_final_lsn),
        "fixture: the source final LSN was not already acknowledged before the oversized unit"
    );

    client.shutdown().await;
    hub.stop().await;
}
