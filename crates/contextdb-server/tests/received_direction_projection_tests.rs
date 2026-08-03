use contextdb_core::{SyncDirection, TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::protocol::{
    DependencyCompletePullResponse, MessageType, PullResponse, WireChangeSet,
    canonical_ddl_provenance_digest, decode, encode, validate_wire_ddl_provenance,
};
use contextdb_server::subjects::pull_subject;
use contextdb_server::transport::{ClientTransport, TransportFuture, TransportStatusFuture};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const TENANT: &str = "received-direction-projection";

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("bounded exchange exceeded 60s")
}

fn body(db: &Database, table: &str, id: i64) -> Option<String> {
    let values = HashMap::from([("id".to_string(), Value::Int64(id))]);
    let result = db
        .execute(&format!("SELECT body FROM {table} WHERE id = $id"), &values)
        .unwrap_or_else(|err| panic!("read {table} must succeed: {err}"));
    result.rows.first().and_then(|row| match row.first() {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    })
}

fn insert(db: &Database, table: &str, id: i64, value: &str) {
    let values = HashMap::from([
        ("id".to_string(), Value::Int64(id)),
        ("body".to_string(), Value::Text(value.to_string())),
    ]);
    db.execute(
        &format!("INSERT INTO {table} (id, body) VALUES ($id, $body)"),
        &values,
    )
    .unwrap_or_else(|err| panic!("write {table} must succeed: {err}"));
}

struct Hub {
    db: Arc<Database>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        self.task.await.expect("hub task must stop");
    }
}

fn start_hub(broker: &InProcessBroker) -> Hub {
    let db = Arc::new(Database::open_memory());
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db.clone(),
            broker.server_as(&node_id),
            TenantId::from(TENANT),
            node_id,
            identity,
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub { db, stop, task }
}

fn create_push_only(db: &Database, table: &str) {
    db.execute(
        &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY"),
        &params(),
    )
    .unwrap_or_else(|err| panic!("create {table} must succeed: {err}"));
}

fn create_two_way(db: &Database, table: &str) {
    db.execute(
        &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY"),
        &params(),
    )
    .unwrap_or_else(|err| panic!("create {table} must succeed: {err}"));
}

fn edge_client(db: Arc<Database>, broker: &InProcessBroker) -> SyncClient {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db,
        broker.client_as(&node_id),
        TenantId::from(TENANT),
        identity,
    )
}

struct ProjectedDirectionPull {
    inner: Arc<dyn ClientTransport>,
    pull_subject: String,
    before_projection: Arc<Mutex<Option<Vec<WireChangeSet>>>>,
}

fn project_final_directions(changeset: &mut WireChangeSet) {
    for ((ddl, ddl_lsn), provenance) in changeset
        .ddl
        .iter_mut()
        .zip(changeset.ddl_lsn.iter())
        .zip(changeset.ddl_provenance.iter_mut())
    {
        let mut encoded_ddl = serde_json::to_value(&*ddl).expect("encode schema entry");
        let fields = encoded_ddl
            .as_object_mut()
            .and_then(|entry| entry.get_mut("CreateTable"))
            .and_then(serde_json::Value::as_object_mut);
        let final_direction = fields.and_then(|fields| {
            match fields.get("name").and_then(serde_json::Value::as_str) {
                Some("wire_push") => Some("SYNC PUSH ONLY"),
                Some("wire_off") => Some("SYNC OFF"),
                _ => None,
            }
            .map(|direction| (fields, direction))
        });
        if let Some((fields, final_direction)) = final_direction {
            let constraints = fields
                .get_mut("constraints")
                .and_then(serde_json::Value::as_array_mut)
                .expect("deliverable source table has direction constraints");
            let prior = constraints
                .iter_mut()
                .find(|item| {
                    item.as_str()
                        .is_some_and(|constraint| constraint.starts_with("SYNC "))
                })
                .expect("deliverable source table has a direction constraint");
            *prior = serde_json::Value::String(final_direction.to_string());
            *ddl = serde_json::from_value(encoded_ddl).expect("decode schema entry");
            provenance.digest = canonical_ddl_provenance_digest(
                ddl,
                *ddl_lsn,
                provenance.ordinal,
                provenance.table.as_deref(),
                provenance.table_generation,
            )
            .expect("canonical schema provenance digest");
        }
    }
    validate_wire_ddl_provenance(changeset).expect("projected schema provenance remains valid");
}

impl ClientTransport for ProjectedDirectionPull {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn reconnect<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.reconnect()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject != self.pull_subject {
            return self.inner.request(subject, request_bytes, timeout);
        }
        let inner = self.inner.clone();
        let subject = subject.to_string();
        let before_projection = self.before_projection.clone();
        Box::pin(async move {
            let response_bytes = inner.request(&subject, request_bytes, timeout).await?;
            let envelope = decode(&response_bytes).expect("decode pull response envelope");
            match envelope.message_type {
                MessageType::PullResponse => {
                    let mut response: PullResponse =
                        rmp_serde::from_slice(&envelope.payload).expect("decode pull response");
                    *before_projection
                        .lock()
                        .expect("wire capture lock must not poison") =
                        Some(vec![response.changeset.clone()]);
                    project_final_directions(&mut response.changeset);
                    Ok(encode(MessageType::PullResponse, &response).expect("encode pull response"))
                }
                MessageType::DependencyCompletePullResponse => {
                    let mut response: DependencyCompletePullResponse =
                        rmp_serde::from_slice(&envelope.payload)
                            .expect("decode dependency pull response");
                    let mut source_pages = vec![response.ordinary.changeset.clone()];
                    source_pages.extend(response.units.iter().cloned());
                    *before_projection
                        .lock()
                        .expect("wire capture lock must not poison") = Some(source_pages);
                    project_final_directions(&mut response.ordinary.changeset);
                    for unit in &mut response.units {
                        project_final_directions(unit);
                    }
                    Ok(
                        encode(MessageType::DependencyCompletePullResponse, &response)
                            .expect("encode dependency pull response"),
                    )
                }
                received => panic!("expected pull response, got {received:?}"),
            }
        })
    }
}

#[tokio::test]
async fn received_schema_direction_controls_rows_from_the_same_pull_page() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    for table in ["becomes_pull", "becomes_both", "recreated_default"] {
        create_push_only(&hub.db, table);
    }
    create_two_way(&hub.db, "remains_push");
    create_two_way(&hub.db, "remains_off");

    let edge = Arc::new(Database::open_memory());
    let client = edge_client(edge.clone(), &broker);
    within(client.pull_default())
        .await
        .expect("initial authenticated declarations arrive");

    hub.db
        .execute("ALTER TABLE becomes_pull SET SYNC PULL ONLY", &params())
        .expect("pull declaration");
    hub.db
        .execute("ALTER TABLE becomes_both SET SYNC TWO WAY", &params())
        .expect("two-way declaration");
    hub.db
        .execute("DROP TABLE recreated_default", &params())
        .expect("drop old declaration");
    hub.db
        .execute(
            "CREATE TABLE recreated_default (id INTEGER PRIMARY KEY, body TEXT)",
            &params(),
        )
        .expect("create default declaration");
    hub.db
        .execute("ALTER TABLE remains_push SET SYNC PUSH ONLY", &params())
        .expect("push declaration");
    hub.db
        .execute("ALTER TABLE remains_off SET SYNC OFF", &params())
        .expect("sync off declaration");

    insert(&hub.db, "becomes_pull", 11, "pull-now");
    insert(&hub.db, "becomes_both", 12, "two-way-now");
    insert(&hub.db, "recreated_default", 13, "default-now");
    insert(&hub.db, "remains_push", 14, "must-stay-excluded");
    insert(&hub.db, "remains_off", 15, "must-stay-excluded");

    within(client.pull_default())
        .await
        .expect("declarations and rows arrive together");

    assert_eq!(
        body(&edge, "becomes_pull", 11),
        Some("pull-now".to_string()),
        "the received PULL ONLY declaration admits its accompanying row"
    );
    assert_eq!(
        edge.table_meta("becomes_pull")
            .expect("received table metadata")
            .sync_direction,
        Some(SyncDirection::Pull),
        "the receiver retained the incoming PULL ONLY declaration"
    );
    assert_eq!(
        body(&edge, "becomes_both", 12),
        Some("two-way-now".to_string()),
        "the received TWO WAY declaration admits its accompanying row"
    );
    assert_eq!(
        edge.table_meta("becomes_both")
            .expect("received table metadata")
            .sync_direction,
        Some(SyncDirection::Both),
        "the receiver retained the incoming TWO WAY declaration"
    );
    assert_eq!(
        body(&edge, "recreated_default", 13),
        Some("default-now".to_string()),
        "a dropped and re-created default TWO WAY table admits its accompanying row"
    );
    assert_eq!(
        edge.table_meta("recreated_default")
            .expect("re-created table metadata")
            .sync_direction,
        None,
        "the re-created table keeps the default direction rather than stale metadata"
    );
    assert_eq!(
        body(&edge, "remains_push", 14),
        None,
        "a final PUSH ONLY declaration still excludes its row"
    );
    assert_eq!(
        body(&edge, "remains_off", 15),
        None,
        "a final SYNC OFF declaration still excludes its row"
    );

    hub.stop().await;
}

#[tokio::test]
async fn received_page_projection_excludes_wire_rows_after_final_direction_change() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker);
    create_two_way(&hub.db, "wire_push");
    create_two_way(&hub.db, "wire_off");
    insert(&hub.db, "wire_push", 31, "wire-push-value");
    insert(&hub.db, "wire_off", 32, "wire-off-value");

    let before_projection = Arc::new(Mutex::new(None));
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let transport = Arc::new(ProjectedDirectionPull {
        inner: broker.client_as(&node_id),
        pull_subject: pull_subject(TENANT),
        before_projection: before_projection.clone(),
    });
    let edge = Arc::new(Database::open_memory());
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        transport,
        TenantId::from(TENANT),
        identity,
    );

    within(client.pull_default())
        .await
        .expect("authenticated projected response applies");

    let source_pages = before_projection
        .lock()
        .expect("wire capture lock must not poison")
        .take()
        .expect("one authenticated pull response arrived");
    for table in ["wire_push", "wire_off"] {
        assert!(
            source_pages.iter().flat_map(|page| &page.ddl).any(|ddl| {
                serde_json::to_value(ddl)
                    .expect("encode source schema entry")
                    .get("CreateTable")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|fields| {
                        fields.get("name").and_then(serde_json::Value::as_str) == Some(table)
                            && fields
                                .get("constraints")
                                .and_then(serde_json::Value::as_array)
                                .is_some_and(|constraints| {
                                    constraints
                                        .iter()
                                        .any(|item| item.as_str() == Some("SYNC TWO WAY"))
                                })
                    })
            }),
            "the real source page contains deliverable TWO WAY schema for {table}"
        );
    }
    for (table, id, value) in [
        ("wire_push", 31, "wire-push-value"),
        ("wire_off", 32, "wire-off-value"),
    ] {
        assert!(
            source_pages.iter().flat_map(|page| &page.rows).any(|row| {
                row.table == table
                    && row.values.get("id") == Some(&Value::Int64(id))
                    && row.values.get("body") == Some(&Value::Text(value.to_string()))
                    && row.lineage.is_some()
            }),
            "the decoded authenticated response contains {table} row {id} before receiver apply"
        );
    }
    assert_eq!(
        edge.table_meta("wire_push")
            .expect("wire push metadata")
            .sync_direction,
        Some(SyncDirection::Push),
        "the incoming final PUSH ONLY declaration installs"
    );
    assert_eq!(
        edge.table_meta("wire_off")
            .expect("wire off metadata")
            .sync_direction,
        Some(SyncDirection::None),
        "the incoming final SYNC OFF declaration installs"
    );
    assert_eq!(
        body(&edge, "wire_push", 31),
        None,
        "the final PUSH ONLY declaration excludes a row present on the wire"
    );
    assert_eq!(
        body(&edge, "wire_off", 32),
        None,
        "the final SYNC OFF declaration excludes a row present on the wire"
    );

    hub.stop().await;
}
