//! A vector added after its owner row already synchronized reaches every real Iroh peer.

use contextdb_core::{RowId, TenantId, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{SyncClient, SyncServer};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded Iroh operation")
}

fn spec(path: &Path) -> String {
    format!("iroh:?identity={}", path.display())
}
struct Hub {
    db: Arc<Database>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn hub(root: &Path, tenant: &str) -> Hub {
    let (ticket, transport) = {
        let endpoint = IrohServer::bind(&spec(&root.join("hub.identity")))
            .await
            .expect("bind hub");
        (endpoint.ticket(), endpoint.transport())
    };
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub"));
    db.execute("CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3)) SYNC CONFLICT KEEP LATEST", &HashMap::new()).expect("hub schema");
    let server = Arc::new(SyncServer::with_authenticated_transport_for_test(
        db.clone(),
        transport,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        ticket,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

fn edge(root: &Path, name: &str, ticket: &str, tenant: &str) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(Database::open(root.join(format!("{name}.db"))).expect("open edge"));
    db.execute("CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3)) SYNC CONFLICT KEEP LATEST", &HashMap::new()).expect("edge schema");
    let client = SyncClient::new(db.clone(), ticket, TenantId::from(tenant));
    (db, client)
}

fn exact_owner_row_id(db: &Database, id: Uuid) -> RowId {
    let rows = db.scan("notes", db.snapshot()).expect("scan owner row");
    let matching = rows
        .into_iter()
        .filter(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "one exact owner row exists");
    let row = &matching[0];
    assert_eq!(
        row.values.get("body"),
        Some(&Value::Text("owner first".to_string())),
        "owner text remains exact",
    );
    assert_eq!(
        row.values.get("embedding"),
        Some(&Value::Vector(vec![1.0, 0.0, 0.0])),
        "the later vector update is exact",
    );
    row.row_id
}

#[tokio::test]
async fn established_owner_vector_update_syncs_before_delete_arbitration() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "vector-enrichment";
    let hub = hub(root.path(), tenant).await;
    let (edge_a, client_a) = edge(root.path(), "edge-a", &hub.ticket, tenant);
    let (edge_b, client_b) = edge(root.path(), "edge-b", &hub.ticket, tenant);
    let id = Uuid::new_v4();
    edge_a
        .execute(
            &format!("INSERT INTO notes (id, body) VALUES ('{id}', 'owner first')"),
            &HashMap::new(),
        )
        .expect("insert owner without vector");
    within(client_a.push()).await.expect("initial owner push");
    within(client_b.pull_default())
        .await
        .expect("initial owner pull");
    edge_a
        .execute(
            &format!("UPDATE notes SET embedding = '[1,0,0]' WHERE id = '{id}'"),
            &HashMap::new(),
        )
        .expect("later vector update");
    within(client_a.push())
        .await
        .expect("later vector update push");
    within(client_b.pull_default())
        .await
        .expect("later vector update pull");
    for db in [&hub.db, &edge_a, &edge_b] {
        let sql = db
            .execute(
                &format!("SELECT body, embedding FROM notes WHERE id = '{id}'"),
                &HashMap::new(),
            )
            .expect("SQL owner and vector");
        assert_eq!(sql.rows.len(), 1, "owner remains present with exact key");
        assert_eq!(
            sql.rows[0],
            vec![
                Value::Text("owner first".to_string()),
                Value::Vector(vec![1.0, 0.0, 0.0]),
            ],
            "SQL observes the exact owner and vector",
        );
        let owner_row_id = exact_owner_row_id(db, id);
        let ann = db
            .query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[1.0, 0.0, 0.0],
                1,
                None,
                db.snapshot(),
            )
            .expect("ANN query");
        assert_eq!(ann.len(), 1, "no unrelated vector can satisfy this proof");
        assert_eq!(ann[0].0, owner_row_id, "ANN returns the established owner");
        assert!(
            (ann[0].1 - 1.0).abs() <= 1e-6,
            "exact embedding has cosine score 1.0: {ann:?}"
        );
    }
    hub.stop().await;
}
