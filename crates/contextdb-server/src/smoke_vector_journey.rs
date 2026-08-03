//! Fixed installed-release proof that a vector added after its owner row has
//! already synchronized remains attached to that owner on every participant.

use contextdb_core::{RowId, TenantId, Value, VectorIndexRef};
use contextdb_engine::{Database, SyncClient, SyncServer};
use contextdb_server::{PeerEndpoint, peer_bind_spec, peer_dial_spec};
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3)) \
    SYNC TWO WAY SYNC CONFLICT KEEP LATEST";
const TENANT: &str = "installed-vector-enrichment";

struct Hub {
    db: Arc<Database>,
    endpoint: PeerEndpoint,
    ticket: String,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Hub {
    async fn start(root: &Path) -> Result<Self, String> {
        let directory = root.join("hub");
        std::fs::create_dir_all(&directory)
            .map_err(|_| "cannot create vector verifier hub directory".to_string())?;
        let endpoint = PeerEndpoint::bind(&peer_bind_spec(&directory.join("hub.identity")))
            .await
            .map_err(|_| "cannot bind vector verifier hub".to_string())?;
        let ticket = endpoint.ticket();
        let db = Arc::new(
            Database::open(directory.join("hub.db"))
                .map_err(|_| "cannot open vector verifier hub database".to_string())?,
        );
        declare(&db)?;
        let server = SyncServer::new(db.clone(), &endpoint, TenantId::from(TENANT));
        let shutdown = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let shutdown = shutdown.clone();
            async move { server.run_until(shutdown).await }
        });
        Ok(Self {
            db,
            endpoint,
            ticket,
            shutdown,
            task,
        })
    }

    async fn stop(self) -> Result<(), String> {
        self.shutdown.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .map_err(|_| "vector verifier hub did not stop".to_string())?
            .map_err(|_| "vector verifier hub task failed".to_string())?;
        self.endpoint.close().await;
        self.db
            .close()
            .map_err(|_| "vector verifier hub database did not close".to_string())
    }
}

struct Edge {
    db: Arc<Database>,
    client: SyncClient,
}

async fn within<F: std::future::Future>(future: F) -> Result<F::Output, String> {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .map_err(|_| "vector verifier transport operation timed out".to_string())
}

fn edge(root: &Path, name: &str, ticket: &str) -> Result<Edge, String> {
    let directory = root.join(name);
    std::fs::create_dir_all(&directory)
        .map_err(|_| "cannot create vector verifier edge directory".to_string())?;
    let db = Arc::new(
        Database::open(directory.join("edge.db"))
            .map_err(|_| "cannot open vector verifier edge database".to_string())?,
    );
    declare(&db)?;
    let client = SyncClient::new(
        db.clone(),
        &peer_dial_spec(&ticket, &directory.join("edge.identity")),
        TenantId::from(TENANT),
    );
    Ok(Edge { db, client })
}

fn declare(db: &Database) -> Result<(), String> {
    if db.table_meta("notes").is_none() {
        db.execute(DDL, &HashMap::new())
            .map_err(|_| "cannot declare vector verifier table".to_string())?;
    }
    Ok(())
}

fn inspect(db: &Database, id: Uuid, place: &str) -> Result<(RowId, f32), String> {
    let sql = db
        .execute(
            "SELECT body, embedding FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .map_err(|_| format!("cannot read enriched owner at {place}"))?;
    if sql.rows
        != [vec![
            Value::Text("owner-first".to_string()),
            Value::Vector(vec![1.0, 0.0, 0.0]),
        ]]
    {
        return Err(format!(
            "SQL did not return the exact enriched owner at {place}"
        ));
    }
    let rows = db
        .scan("notes", db.snapshot())
        .map_err(|_| format!("cannot scan enriched owner at {place}"))?;
    let owners = rows
        .into_iter()
        .filter(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .collect::<Vec<_>>();
    if owners.len() != 1 {
        return Err(format!("expected one enriched owner at {place}"));
    }
    let owner = owners[0].row_id;
    let hits = db
        .query_vector(
            VectorIndexRef::new("notes", "embedding"),
            &[1.0, 0.0, 0.0],
            1,
            None,
            db.snapshot(),
        )
        .map_err(|_| format!("ANN query failed at {place}"))?;
    if hits.len() != 1 || hits[0].0 != owner || (hits[0].1 - 1.0).abs() > 1e-6 {
        return Err(format!(
            "ANN did not return the exact enriched owner at {place}"
        ));
    }
    Ok((owner, hits[0].1))
}

fn inspect_unenriched(db: &Database, id: Uuid, place: &str) -> Result<RowId, String> {
    let sql = db
        .execute(
            "SELECT body, embedding FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .map_err(|_| format!("cannot read owner before enrichment at {place}"))?;
    if sql.rows != [vec![Value::Text("owner-first".to_string()), Value::Null]] {
        return Err(format!(
            "owner was not present without an embedding at {place}"
        ));
    }
    let owners = db
        .scan("notes", db.snapshot())
        .map_err(|_| format!("cannot scan owner before enrichment at {place}"))?
        .into_iter()
        .filter(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .collect::<Vec<_>>();
    if owners.len() != 1 {
        return Err(format!("expected one owner before enrichment at {place}"));
    }
    Ok(owners[0].row_id)
}

pub async fn run(root: &Path) -> Result<(), String> {
    if !root.is_dir() {
        return Err("vector verifier root must already exist".to_string());
    }
    let hub = Hub::start(root).await?;
    let edge_a = edge(root, "edge-a", &hub.ticket)?;
    let edge_b = edge(root, "edge-b", &hub.ticket)?;
    let id = Uuid::from_u128(0xbbbbbbbbbbbb4bbb8bbbbbbbbbbbbbbb);
    edge_a
        .db
        .execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("body".to_string(), Value::Text("owner-first".to_string())),
            ]),
        )
        .map_err(|_| "cannot insert owner before enrichment".to_string())?;
    within(edge_a.client.push())
        .await?
        .map_err(|_| "cannot push owner before enrichment".to_string())?;
    within(edge_b.client.pull_default())
        .await?
        .map_err(|_| "cannot pull owner before enrichment".to_string())?;
    let hub_row_before = inspect_unenriched(&hub.db, id, "hub")?;
    let edge_a_row_before = inspect_unenriched(&edge_a.db, id, "edge-a")?;
    let edge_b_row_before = inspect_unenriched(&edge_b.db, id, "edge-b")?;
    edge_a
        .db
        .execute(
            "UPDATE notes SET embedding = $embedding WHERE id = $id",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("embedding".to_string(), Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .map_err(|_| "cannot add vector to established owner".to_string())?;
    within(edge_a.client.push())
        .await?
        .map_err(|_| "cannot push established-owner vector".to_string())?;
    within(edge_b.client.pull_default())
        .await?
        .map_err(|_| "cannot pull established-owner vector".to_string())?;
    let (hub_row, hub_score) = inspect(&hub.db, id, "hub")?;
    let (edge_a_row, edge_a_score) = inspect(&edge_a.db, id, "edge-a")?;
    let (edge_b_row, edge_b_score) = inspect(&edge_b.db, id, "edge-b")?;
    if (hub_row, edge_a_row, edge_b_row) != (hub_row_before, edge_a_row_before, edge_b_row_before) {
        return Err("vector enrichment replaced an established owner row".to_string());
    }
    println!(
        "{}",
        json!({
            "event":"vector_enrichment_complete",
            "id":id,
            "sql_exact":{"hub":true,"edge_a":true,"edge_b":true},
            "owner_present_before_enrichment":{"hub":true,"edge_a":true,"edge_b":true},
            "ann":{"hub":{"row_id":hub_row.0,"score":hub_score},
                   "edge_a":{"row_id":edge_a_row.0,"score":edge_a_score},
                   "edge_b":{"row_id":edge_b_row.0,"score":edge_b_score}},
        })
    );
    edge_a.client.shutdown().await;
    edge_b.client.shutdown().await;
    hub.stop().await
}
