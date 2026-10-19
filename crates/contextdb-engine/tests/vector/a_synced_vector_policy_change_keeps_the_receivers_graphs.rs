//! A vector column's search policy can change over sync without costing the
//! receiver its search graphs.
//!
//! Changing `SEARCH_MODE` rebuilds no vector data or graph, and changing
//! `AUTO_INDEX_AT` changes when a graph is built, not the graph already built.
//! So a receiver that pulls only such an ALTER keeps answering `USE VECTOR
//! INDEXED` from the graphs it maintained before, with no maintenance run in
//! between, and adopts the new policy; a later ordinary vector update leaves
//! the other partitions' graphs serving. A changed HNSW topology keeps the old
//! graph serving until maintenance replaces it. Driven through the
//! authenticated pull path, the same received-schema transaction a real edge
//! uses.

use contextdb_core::{TenantId, Value, VectorSearchMode};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const MAX_MAINTENANCE_CYCLES: usize = 32;
const TENANT: &str = "synced-vector-policy-change";
const SCHEMA: &str = "CREATE TABLE policy_items (
    id UUID PRIMARY KEY,
    scope_id UUID NOT NULL,
    embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE INDEXED
) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector search projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector search returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn indexed_search(
    db: &Database,
    scope: Uuid,
    query: Vec<f32>,
) -> contextdb_core::Result<Vec<Uuid>> {
    db.execute(
        "SELECT id FROM policy_items WHERE scope_id = $scope \
         ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
        &params([
            ("scope", Value::Uuid(scope)),
            ("query", Value::Vector(query)),
        ]),
    )
    .map(|result| ids(&result))
}

fn exact_search_at(
    db: &Database,
    snapshot: contextdb_core::SnapshotId,
    scope: Uuid,
    query: Vec<f32>,
) -> Vec<Uuid> {
    ids(&db
        .execute_at_snapshot(
            "SELECT id FROM policy_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
            snapshot,
        )
        .expect("exact search at a retained snapshot"))
}

fn upsert(db: &Database, id: Uuid, scope: Uuid, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO policy_items (id, scope_id, embedding) VALUES ($id, $scope, $embedding) \
         ON CONFLICT (id) DO UPDATE SET embedding = $embedding",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(embedding)),
        ]),
    )
    .expect("write one source vector");
}

fn embedding_column(db: &Database) -> contextdb_core::ColumnDef {
    db.table_meta("policy_items")
        .expect("the receiver holds the table")
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .expect("the vector column")
}

/// (desired HNSW M, serving HNSW M) for every partition of the receiver.
fn hnsw_m(db: &Database) -> Vec<(Value, Value)> {
    let result = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR policy_items.embedding",
            &HashMap::new(),
        )
        .expect("inspect the receiver's partitions");
    let column = |name: &str| {
        result
            .columns
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("SHOW VECTOR_PARTITIONS has {name}"))
    };
    let (desired, serving) = (column("desired_hnsw_m"), column("serving_hnsw_m"));
    result
        .rows
        .iter()
        .map(|row| (row[desired].clone(), row[serving].clone()))
        .collect()
}

const ROWS: [(u128, u128, [f32; 3]); 2] = [
    (0xA101, 0xA1, [1.0, 0.0, 0.0]),
    (0xA102, 0xA2, [0.0, 0.0, 1.0]),
];

fn row(index: usize) -> (Uuid, Uuid, Vec<f32>) {
    let (id, scope, embedding) = ROWS[index];
    (
        Uuid::from_u128(id),
        Uuid::from_u128(scope),
        embedding.to_vec(),
    )
}

/// A source and a receiver joined by the authenticated in-process transport.
/// The receiver has pulled the table and built a graph for each partition.
struct Synced {
    source: Arc<Database>,
    edge: Arc<Database>,
    client: SyncClient,
    server: Arc<SyncServer>,
    shutdown: Arc<AtomicBool>,
    server_task: tokio::task::JoinHandle<()>,
}

impl Synced {
    async fn start() -> Self {
        let broker = InProcessBroker::new();
        let source = Arc::new(Database::open_memory());
        let edge = Arc::new(Database::open_memory());
        source
            .execute(SCHEMA, &HashMap::new())
            .expect("declare the source vector table");
        for index in 0..ROWS.len() {
            let (id, scope, embedding) = row(index);
            upsert(&source, id, scope, embedding);
        }
        let hub_identity = Arc::new(FabricIdentity::generate());
        let hub_node_id = hub_identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                source.clone(),
                broker.server_as(&hub_node_id),
                TenantId::from(TENANT),
                hub_node_id,
                hub_identity,
            )
            .expect("construct the authenticated sync endpoint"),
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_task = tokio::spawn({
            let server = server.clone();
            let shutdown = shutdown.clone();
            async move { server.run_until(shutdown).await }
        });
        let edge_identity = Arc::new(FabricIdentity::generate());
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            broker.client_as(&edge_identity.node_id()),
            TenantId::from(TENANT),
            edge_identity,
        )
        .expect("construct the authenticated sync client");
        let synced = Self {
            source,
            edge,
            client,
            server,
            shutdown,
            server_task,
        };
        synced.pull().await;
        synced
            .edge
            .set_maintenance_policy(MaintenancePolicy::CallerDriven);
        synced.maintain();
        assert_indexed_answers(
            &synced.edge,
            "the receiver has its graphs before the policy change",
        );
        synced
    }

    async fn pull(&self) {
        self.client
            .pull_default()
            .await
            .expect("an authenticated pull");
    }

    fn maintain(&self) {
        for _ in 0..MAX_MAINTENANCE_CYCLES {
            self.edge
                .run_maintenance_cycle()
                .expect("one caller-driven maintenance cycle succeeds");
        }
    }

    async fn alter(&self, alter: &str) {
        self.source
            .execute(alter, &HashMap::new())
            .expect("change only the vector column's policy at the source");
        self.pull().await;
    }

    async fn stop(self) -> Arc<Database> {
        drop(self.client);
        self.shutdown.store(true, Ordering::SeqCst);
        self.server_task.await.expect("server task stops");
        drop(self.server);
        self.edge
    }
}

fn assert_indexed_answers(edge: &Database, why: &str) {
    for index in 0..ROWS.len() {
        let (id, scope, embedding) = row(index);
        assert_eq!(
            indexed_search(edge, scope, embedding).unwrap_or_else(|error| panic!("{why}: {error}")),
            vec![id]
        );
    }
}

#[tokio::test]
async fn a_synced_search_mode_change_keeps_indexed_answering_without_maintenance() {
    let synced = Synced::start().await;
    synced
        .alter("ALTER TABLE policy_items ALTER COLUMN embedding SET SEARCH_MODE AUTO")
        .await;
    let edge = synced.stop().await;
    assert_eq!(
        embedding_column(&edge).search_mode,
        VectorSearchMode::Auto,
        "the receiver adopts the new search mode"
    );
    assert_indexed_answers(
        &edge,
        "a search-mode change rebuilds no graph, so INDEXED still answers without maintenance",
    );
}

#[tokio::test]
async fn a_synced_auto_index_at_change_keeps_indexed_answering_without_maintenance() {
    let synced = Synced::start().await;
    synced
        .alter("ALTER TABLE policy_items ALTER COLUMN embedding SET AUTO_INDEX_AT 7")
        .await;
    let edge = synced.stop().await;
    assert_eq!(
        embedding_column(&edge).auto_index_at,
        Some(7),
        "the receiver adopts the new AUTO_INDEX_AT"
    );
    assert_indexed_answers(
        &edge,
        "an AUTO_INDEX_AT change keeps the built graph, so INDEXED still answers without \
         maintenance",
    );
}

/// After a synced policy-only ALTER, an ordinary later vector update in one
/// partition is applied as an update: the other partition's graph keeps
/// serving without maintenance, and a snapshot taken before the update still
/// sees the old vector.
#[tokio::test]
async fn a_vector_update_after_a_synced_policy_change_keeps_the_other_partitions_graph() {
    for alter in [
        "ALTER TABLE policy_items ALTER COLUMN embedding SET SEARCH_MODE AUTO",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET AUTO_INDEX_AT 7",
    ] {
        let synced = Synced::start().await;
        synced.alter(alter).await;
        let (changed_id, changed_scope, old_embedding) = row(0);
        let before = synced.edge.snapshot();
        let pin = synced
            .edge
            .pin_snapshot(before)
            .expect("hold the pre-update snapshot");
        upsert(
            &synced.source,
            changed_id,
            changed_scope,
            vec![0.0, 1.0, 0.0],
        );
        synced.pull().await;
        let (other_id, other_scope, other_embedding) = row(1);
        assert_eq!(
            indexed_search(&synced.edge, other_scope, other_embedding).unwrap_or_else(
                |error| panic!(
                    "{alter}: a later update in another partition leaves this partition's \
                     graph serving: {error}"
                )
            ),
            vec![other_id]
        );
        assert_eq!(
            exact_search_at(&synced.edge, before, changed_scope, old_embedding),
            vec![changed_id],
            "{alter}: the pre-update snapshot still sees the old vector"
        );
        drop(pin);
        synced.stop().await;
    }
}

/// A synced HNSW topology change keeps the old graph serving INDEXED until
/// maintenance builds the new topology, which then serves.
#[tokio::test]
async fn a_synced_hnsw_topology_change_serves_the_old_graph_until_maintenance_replaces_it() {
    let synced = Synced::start().await;
    let before = hnsw_m(&synced.edge);
    assert!(
        before.iter().all(|(desired, serving)| desired == serving),
        "every partition serves its declared topology before the change: {before:?}"
    );
    synced
        .alter(
            "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW \
             (M = 24, EF_CONSTRUCTION = 300)",
        )
        .await;
    let pending = hnsw_m(&synced.edge);
    assert!(
        pending
            .iter()
            .all(|(desired, serving)| *desired == Value::Int64(24) && serving != desired),
        "the receiver adopts the new topology but still serves the old one: {pending:?}"
    );
    assert_indexed_answers(
        &synced.edge,
        "the old graph serves INDEXED until maintenance replaces it",
    );
    synced.maintain();
    let replaced = hnsw_m(&synced.edge);
    assert!(
        replaced
            .iter()
            .all(|(desired, serving)| *desired == Value::Int64(24) && serving == desired),
        "bounded maintenance serves the new topology: {replaced:?}"
    );
    assert_indexed_answers(&synced.edge, "the new graph serves INDEXED");
    synced.stop().await;
}

/// The ordinary order "ALTER, then write, then one pull": the source changes
/// only the vector column's policy and then updates a vector in the same
/// table, and the edge pulls both at once. Each is its own authored
/// transaction and the sender ships the ALTER as its own unit, so the
/// received schema stage carries no vector rows and applies the policy in
/// place; the update takes the ordinary path. The receiver adopts the policy
/// without discarding its graphs: the untouched partition still answers
/// INDEXED with no maintenance, a snapshot held from before the pull still
/// sees the old vector, and a changed HNSW topology keeps the old one serving
/// until maintenance.
#[tokio::test]
async fn a_synced_policy_change_and_a_vector_update_in_one_pull_keep_the_receivers_graphs() {
    for (alter, adopted) in [
        (
            "ALTER TABLE policy_items ALTER COLUMN embedding SET SEARCH_MODE AUTO",
            "search_mode",
        ),
        (
            "ALTER TABLE policy_items ALTER COLUMN embedding SET AUTO_INDEX_AT 7",
            "auto_index_at",
        ),
        (
            "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW \
             (M = 24, EF_CONSTRUCTION = 300)",
            "hnsw",
        ),
    ] {
        let synced = Synced::start().await;
        let (changed_id, changed_scope, old_embedding) = row(0);
        let new_embedding = vec![0.0, 1.0, 0.0];
        synced
            .source
            .execute(alter, &HashMap::new())
            .expect("change only the vector column's policy at the source");
        upsert(
            &synced.source,
            changed_id,
            changed_scope,
            new_embedding.clone(),
        );
        let before = synced.edge.snapshot();
        let pin = synced
            .edge
            .pin_snapshot(before)
            .expect("hold the pre-pull snapshot");
        let observer = contextdb_engine::database::ReceivedSchemaApplyObserverForTest::new();
        {
            let _scope = synced
                .edge
                .observe_received_schema_applies_for_test(&observer);
            synced.pull().await;
        }
        let observation = observer
            .observation()
            .expect("the one pull applied the ALTER as a received schema unit");
        assert!(
            observation
                .profile_work
                .get("received-unit-vectors")
                .is_some_and(|vectors| *vectors == 0),
            "{alter}: the sender ships the ALTER apart from the vector update, so the \
             received schema stage carries no vector rows: {:?}",
            observation.profile_work
        );
        match adopted {
            "search_mode" => assert_eq!(
                embedding_column(&synced.edge).search_mode,
                VectorSearchMode::Auto
            ),
            "auto_index_at" => assert_eq!(embedding_column(&synced.edge).auto_index_at, Some(7)),
            _ => assert!(
                hnsw_m(&synced.edge)
                    .iter()
                    .all(|(desired, _)| *desired == Value::Int64(24)),
                "{alter}: the receiver adopts the new topology"
            ),
        }
        let (other_id, other_scope, other_embedding) = row(1);
        assert_eq!(
            indexed_search(&synced.edge, other_scope, other_embedding).unwrap_or_else(
                |error| panic!(
                    "{alter}: the untouched partition keeps its graph through a unit that \
                     also carries rows: {error}"
                )
            ),
            vec![other_id]
        );
        if adopted == "hnsw" {
            assert!(
                hnsw_m(&synced.edge)
                    .iter()
                    .any(|(desired, serving)| serving != desired),
                "{alter}: the old topology keeps serving until maintenance"
            );
        }
        assert_eq!(
            exact_search_at(&synced.edge, before, changed_scope, old_embedding),
            vec![changed_id],
            "{alter}: the pre-pull snapshot still sees the old vector"
        );
        drop(pin);
        synced.maintain();
        assert_eq!(
            indexed_search(&synced.edge, changed_scope, new_embedding)
                .expect("the changed partition answers after maintenance"),
            vec![changed_id]
        );
        synced.stop().await;
    }
}

/// A synced policy change rechecks a recorded maintenance failure exactly as a
/// local ALTER does: when the partition's healthy route needs no work under
/// the new policy, inspection stops reporting the old refusal.
#[tokio::test]
async fn a_synced_policy_change_clears_a_failure_the_new_policy_no_longer_needs() {
    let synced = Synced::start().await;
    let (_, scope, _) = row(0);
    let index = contextdb_core::VectorIndexRef::new("policy_items", "embedding");
    let partition = contextdb_vector::VectorPartitionRef::new(
        index.clone(),
        contextdb_core::VectorPartitionKey::from_values(&[Value::Uuid(scope)])
            .expect("one UUID partition key"),
    );
    let failure = || {
        let mut owner = contextdb_engine::vector_read_memory::admission(
            synced
                .edge
                .declared_reader_memory()
                .expect("admit the partition observation reader"),
        );
        synced
            .edge
            .vector_store_for_test()
            .partition_info(&index, &partition.partition_key, &mut owner)
            .expect("inspect the receiver's partition")
            .and_then(|info| info.maintenance_failure)
    };
    synced
        .edge
        .vector_store_for_test()
        .record_partition_maintenance_failure(
            &partition,
            contextdb_vector::VectorMaintenanceFailure::MemoryLimit,
        )
        .expect("record a maintenance refusal on the receiver's healthy partition");
    assert_eq!(
        failure(),
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );

    synced
        .alter("ALTER TABLE policy_items ALTER COLUMN embedding SET SEARCH_MODE AUTO")
        .await;
    assert_eq!(
        embedding_column(&synced.edge).search_mode,
        VectorSearchMode::Auto
    );
    assert_eq!(
        failure(),
        None,
        "the healthy route needs no work under the synced policy, so the old refusal is gone"
    );
    synced.stop().await;
}
