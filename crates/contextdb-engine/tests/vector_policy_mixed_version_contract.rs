//! A schema author may use vector policy on one table without stopping every
//! compatible table from syncing to a node that has not upgraded yet.

#![cfg(feature = "test-seams")]

use contextdb_core::{Lsn, TenantId, Value, VectorIndexRef};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::sync_types::{DdlChange, SchemaSyncHoldback, VectorChange};
use contextdb_engine::{Database, QueryResult};
use contextdb_server::protocol::PROTOCOL_VERSION;
use contextdb_server::subjects::pull_subject;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const TENANT: &str = "vector-policy-mixed-version";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn ids(result: QueryResult) -> Vec<Uuid> {
    result
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(Value::Uuid(id)) => id,
            value => panic!("expected one UUID value, got {value:?}"),
        })
        .collect()
}

fn schema(db: &Database, table: &str) -> String {
    render_table_meta(
        table,
        &db.table_meta(table)
            .unwrap_or_else(|| panic!("table metadata exists for {table}")),
    )
}

fn create_table_ddl_bytes(db: &Database, table: &str) -> Vec<u8> {
    let declaration = db
        .changes_since(Lsn(0))
        .ddl
        .into_iter()
        .find(|change| matches!(change, DdlChange::CreateTable { name, .. } if name == table))
        .unwrap_or_else(|| panic!("outbound changes include CREATE TABLE for {table}"));
    rmp_serde::to_vec(&declaration).expect("CREATE TABLE declaration has stable wire bytes")
}

fn holdbacks_by_table(holdbacks: Vec<SchemaSyncHoldback>) -> BTreeMap<String, SchemaSyncHoldback> {
    holdbacks
        .into_iter()
        .map(|holdback| (holdback.table.clone(), holdback))
        .collect()
}

fn assert_actionable_holdback(holdback: &SchemaSyncHoldback, table: &str, node: &str) {
    assert_eq!(holdback.table, table, "the held table is named exactly");
    assert_eq!(
        holdback.node_to_upgrade, node,
        "the diagnostic names the protocol-7 node that must upgrade"
    );
    let rendered = holdback.to_string();
    assert!(
        rendered.contains(table)
            && rendered.contains(node)
            && !holdback.capability.to_string().is_empty(),
        "the diagnostic names table, capability, and node rather than reporting parse damage: {rendered}"
    );
}

fn golden_vector_change(observed: &VectorChange) -> VectorChange {
    VectorChange {
        index: VectorIndexRef::new(observed.index.table.clone(), observed.index.column.clone()),
        row_id: observed.row_id,
        vector: observed.vector.clone(),
        lsn: observed.lsn,
    }
}

struct RunningServer {
    server: Arc<SyncServer>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.await.expect("server task stops");
    }
}

async fn start_server(
    broker: &InProcessBroker,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
) -> RunningServer {
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            broker.server_as(&node_id),
            TenantId::from(TENANT),
            node_id,
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&pull_subject(TENANT))
        .await;
    RunningServer {
        server,
        shutdown,
        task,
    }
}

#[tokio::test]
async fn current_transport_holds_missing_capabilities_until_the_receiver_gains_them() {
    assert_eq!(
        contextdb_server::protocol::OLDEST_SUPPORTED_PROTOCOL_VERSION,
        7,
        "the first release has no supported development predecessor"
    );
    assert_eq!(
        PROTOCOL_VERSION, 7,
        "declared vector vocabulary advances the schema capability boundary once"
    );

    let root = tempfile::tempdir().expect("temporary durable mixed-version stores");
    let source =
        Arc::new(Database::open(root.path().join("source.db")).expect("open durable v7 source"));
    let receiver = Arc::new(
        Database::open(root.path().join("receiver.db"))
            .expect("open durable missing-capability receiver"),
    );

    source
        .execute(
            "CREATE TABLE untouched_notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &empty(),
        )
        .expect("the untouched table is executable");
    let untouched_schema = schema(&source, "untouched_notes");
    let untouched_ddl_bytes = create_table_ddl_bytes(&source, "untouched_notes");
    assert_eq!(
        untouched_schema,
        "CREATE TABLE untouched_notes (\n  id UUID PRIMARY KEY,\n  body TEXT\n) SYNC TWO WAY;\n"
    );

    source
        .execute(
            "CREATE TABLE auto_policy_vectors (id UUID PRIMARY KEY, embedding VECTOR(3) AUTO_INDEX_AT 1000) SYNC TWO WAY",
            &empty(),
        )
        .expect("the source accepts AUTO_INDEX_AT policy vocabulary");
    source
        .execute(
            "CREATE TABLE hnsw_policy_vectors (id UUID PRIMARY KEY, embedding VECTOR(3) HNSW (M = 24, EF_CONSTRUCTION = 400, EF_SEARCH = 401)) SYNC TWO WAY",
            &empty(),
        )
        .expect("the source accepts grouped HNSW policy vocabulary");

    let untouched = Uuid::from_u128(0xB501);
    let auto = Uuid::from_u128(0xB502);
    let hnsw = Uuid::from_u128(0xB503);
    let watermark = source.current_lsn();
    source
        .execute(
            "INSERT INTO untouched_notes (id, body) VALUES ($id, $body)",
            &params([
                ("id", Value::Uuid(untouched)),
                ("body", Value::Text("continuous".to_owned())),
            ]),
        )
        .expect("insert ordinary work that must remain continuous");
    for (table, id, vector) in [
        ("auto_policy_vectors", auto, vec![0.0, 1.0, 0.0]),
        ("hnsw_policy_vectors", hnsw, vec![0.0, 0.0, 1.0]),
    ] {
        source
            .execute(
                &format!("INSERT INTO {table} (id, embedding) VALUES ($id, $embedding)"),
                &params([
                    ("id", Value::Uuid(id)),
                    ("embedding", Value::Vector(vector)),
                ]),
            )
            .expect("insert one vector through the ordinary SQL path");
    }
    let vector_changes = source.changes_since(watermark).vectors;
    assert_eq!(
        vector_changes.len(),
        2,
        "each inserted vector has one shipped payload"
    );
    for observed in &vector_changes {
        assert_eq!(
            rmp_serde::to_vec(observed).expect("serialize observed VectorChange"),
            rmp_serde::to_vec(&golden_vector_change(observed))
                .expect("serialize old VectorChange shape"),
            "schema policy is not a new VectorChange field or wire byte"
        );
    }

    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let running = start_server(&broker, source.clone(), hub_identity.clone()).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        broker.client_as(&receiver_node),
        TenantId::from(TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("policy holdback is per table, never a whole-connection or parser refusal");

    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM untouched_notes", &empty())
            .expect("the untouched table continues to sync")),
        vec![untouched]
    );
    assert_eq!(
        schema(&receiver, "untouched_notes"),
        untouched_schema,
        "the untouched table renders byte-for-byte as it did before policy vocabulary"
    );
    assert_eq!(
        create_table_ddl_bytes(&receiver, "untouched_notes"),
        untouched_ddl_bytes,
        "the untouched table's shipped CREATE TABLE payload is byte-identical"
    );
    assert!(receiver.table_meta("auto_policy_vectors").is_none());
    assert!(receiver.table_meta("hnsw_policy_vectors").is_none());

    let holdbacks = holdbacks_by_table(running.server.schema_sync_holdbacks());
    assert_eq!(holdbacks.len(), 2, "only the two adopting tables are held");
    let auto_holdback = holdbacks
        .get("auto_policy_vectors")
        .expect("AUTO_INDEX_AT table is held independently");
    let hnsw_holdback = holdbacks
        .get("hnsw_policy_vectors")
        .expect("HNSW table is held independently");
    assert_actionable_holdback(auto_holdback, "auto_policy_vectors", &receiver_node);
    assert_actionable_holdback(hnsw_holdback, "hnsw_policy_vectors", &receiver_node);
    assert_ne!(
        auto_holdback.capability, hnsw_holdback.capability,
        "AUTO_INDEX_AT and HNSW are independently actionable schema capabilities"
    );

    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("upgrading to v7 catches up held tables without manual repair");
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM auto_policy_vectors", &empty())
            .expect("held AUTO_INDEX_AT row arrives after upgrade")),
        vec![auto]
    );
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM hnsw_policy_vectors", &empty())
            .expect("held HNSW row arrives after upgrade")),
        vec![hnsw]
    );
    assert!(running.server.schema_sync_holdbacks().is_empty());
    assert_eq!(
        ids(receiver
            .execute("SELECT id FROM untouched_notes", &empty())
            .expect("the untouched table remains continuous after recovery")),
        vec![untouched]
    );
    running.stop().await;
}
