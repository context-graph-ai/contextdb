//! Mixed-version holdback cannot weaken erasure or make a private bookmark
//! mean more than the current peer actually received.
//!
//! These journeys keep one schema-incompatible table held while compatible
//! work continues. They then exercise permanent purge delivery, rebuilding a
//! receiver or hub under the same authenticated identity, and a commit that
//! lands after a schema-only outbound snapshot. Every public bookmark must
//! describe only history delivered to the current store.

#![cfg(feature = "test-seams")]

use contextdb_core::{TenantId, Value};
use contextdb_engine::sync_types::{SchemaSyncCapability, SchemaSyncHoldback};
use contextdb_engine::{Database, QueryResult};
use contextdb_server::subjects::{pull_subject, push_subject};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const PURGE_TENANT: &str = "vector-schema-held-purge";
const REBUILT_EDGE_TENANT: &str = "vector-schema-rebuilt-edge";
const REBUILT_HUB_TENANT: &str = "vector-schema-rebuilt-hub";
const SCHEMA_RACE_TENANT: &str = "vector-schema-only-race";
const POLICY_SYNC_TENANT: &str = "vector-schema-policy-sync";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn declare_mixed_tables(db: &Database) {
    db.execute(
        "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT NOT NULL) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare the compatible table");
    db.execute(
        "CREATE TABLE partitioned_notes (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id)\
         ) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare the table held from a peer lacking vector vocabulary");
}

fn insert_ordinary(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
        &params([
            ("id", Value::Uuid(id)),
            ("body", Value::Text(body.to_string())),
        ]),
    )
    .expect("insert one compatible row");
}

fn insert_partitioned(db: &Database, id: Uuid, scope_id: Uuid) {
    db.execute(
        "INSERT INTO partitioned_notes (id, scope_id, embedding) \
         VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope_id)),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("insert one held-table row");
}

fn one_column_uuids(result: QueryResult) -> Vec<Uuid> {
    result
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(Value::Uuid(value)) => value,
            value => panic!("expected one UUID value, got {value:?}"),
        })
        .collect()
}

fn ordinary_ids(db: &Database) -> Vec<Uuid> {
    one_column_uuids(
        db.execute("SELECT id FROM ordinary_notes ORDER BY id", &empty())
            .expect("the compatible table and its rows must exist"),
    )
}

fn expected_holdback(
    table: &str,
    capability: SchemaSyncCapability,
    node: &str,
) -> Vec<SchemaSyncHoldback> {
    vec![SchemaSyncHoldback {
        table: table.to_string(),
        capability,
        node_to_upgrade: node.to_string(),
    }]
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
    tenant: &str,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
) -> RunningServer {
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            broker.server_as(&node_id),
            TenantId::from(tenant),
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
        .wait_for_registered_route_for_test(&pull_subject(tenant))
        .await;
    broker
        .wait_for_registered_route_for_test(&push_subject(tenant))
        .await;
    RunningServer {
        server,
        shutdown,
        task,
    }
}

#[tokio::test]
async fn a_held_tables_authoritative_purge_reaches_the_old_copy_after_upgrade() {
    let root = tempfile::TempDir::new().expect("temporary purge stores");
    let source = Arc::new(
        Database::open(root.path().join("purge-hub.redb")).expect("open file-backed purge hub"),
    );
    let receiver = Arc::new(
        Database::open(root.path().join("purge-edge.redb"))
            .expect("open file-backed purge receiver"),
    );
    source
        .execute(
            "CREATE TABLE docs (\
             id UUID PRIMARY KEY, \
             body TEXT NOT NULL, \
             embedding VECTOR(3)\
             ) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare a missing-capability-compatible vector table");
    let erased_id = Uuid::from_u128(0xE2A5_E001);
    source
        .execute(
            "INSERT INTO docs (id, body, embedding) VALUES ($id, $body, $embedding)",
            &params([
                ("id", Value::Uuid(erased_id)),
                ("body", Value::Text("forget me".to_string())),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("insert the row before any schema hold");

    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let running = start_server(&broker, PURGE_TENANT, source.clone(), hub_identity).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        broker.client_as(&receiver_node),
        TenantId::from(PURGE_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    client
        .pull_default()
        .await
        .expect("the compatible table and row reach the old receiver");
    assert_eq!(
        one_column_uuids(
            receiver
                .execute("SELECT id FROM docs", &empty())
                .expect("the old receiver can read its initial copy")
        ),
        vec![erased_id]
    );

    source
        .execute(
            "ALTER TABLE docs ALTER COLUMN embedding SET SEARCH_MODE INDEXED",
            &empty(),
        )
        .expect("adopt newer search vocabulary on the source");
    let purge = source
        .execute(
            "PURGE FROM docs WHERE id = $id",
            &params([("id", Value::Uuid(erased_id))]),
        )
        .expect("authoritatively erase the row on the source");
    assert_eq!(purge.rows_affected, 1);
    client
        .pull_default()
        .await
        .expect("an old receiver keeps other work flowing while docs is held");
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        expected_holdback(
            "docs",
            SchemaSyncCapability::VectorSearchMode,
            &receiver_node,
        )
    );
    assert_eq!(
        one_column_uuids(
            receiver
                .execute("SELECT id FROM docs", &empty())
                .expect("the held receiver still has the pre-purge copy")
        ),
        vec![erased_id]
    );

    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, true);
    client
        .pull_default()
        .await
        .expect("upgrade automatically delivers the held schema and purge");
    assert!(
        one_column_uuids(
            source
                .execute("SELECT id FROM docs", &empty())
                .expect("source remains readable after purge")
        )
        .is_empty()
    );
    assert!(
        one_column_uuids(
            receiver
                .execute("SELECT id FROM docs", &empty())
                .expect("receiver remains readable after recovery")
        )
        .is_empty(),
        "a search-speed schema change must not cancel a permanent erasure"
    );
    assert!(running.server.schema_sync_holdbacks().is_empty());

    running.stop().await;
    source.close().expect("close purge hub");
    receiver.close().expect("close purge receiver");
}

#[tokio::test]
async fn a_rebuilt_edge_with_the_same_identity_receives_compatible_history_from_zero() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    declare_mixed_tables(&source);
    let first_id = Uuid::from_u128(0xE2A5_1001);
    insert_ordinary(&source, first_id, "first");
    insert_partitioned(
        &source,
        Uuid::from_u128(0xE2A5_1002),
        Uuid::from_u128(0xE2A5_10FF),
    );

    let hub_identity = Arc::new(FabricIdentity::generate());
    let running = start_server(&broker, REBUILT_EDGE_TENANT, source, hub_identity).await;
    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node = receiver_identity.node_id();
    let first_receiver = Arc::new(Database::open_memory());
    let first_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        first_receiver.clone(),
        broker.client_as(&receiver_node),
        TenantId::from(REBUILT_EDGE_TENANT),
        receiver_identity.clone(),
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    first_client
        .pull_default()
        .await
        .expect("first edge receives compatible history");
    assert_eq!(ordinary_ids(&first_receiver), vec![first_id]);
    first_client
        .pull_default()
        .await
        .expect("the next request confirms the compatible frontier");
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        expected_holdback(
            "partitioned_notes",
            SchemaSyncCapability::VectorPartitioning,
            &receiver_node,
        )
    );
    drop(first_client);
    drop(first_receiver);

    let rebuilt_receiver = Arc::new(Database::open_memory());
    let rebuilt_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        rebuilt_receiver.clone(),
        broker.client_as(&receiver_node),
        TenantId::from(REBUILT_EDGE_TENANT),
        receiver_identity,
    );
    running
        .server
        .set_peer_vector_schema_support_for_test(&receiver_node, false);
    rebuilt_client
        .pull_default()
        .await
        .expect("a rebuilt edge asks from zero under its existing identity");
    assert_eq!(
        ordinary_ids(&rebuilt_receiver),
        vec![first_id],
        "sender-private progress from the destroyed store must not skip its replacement"
    );

    running.stop().await;
}

#[tokio::test]
async fn a_rebuilt_hub_with_the_same_identity_receives_compatible_history_from_zero() {
    let broker = InProcessBroker::new();
    let edge = Arc::new(Database::open_memory());
    declare_mixed_tables(&edge);
    let first_id = Uuid::from_u128(0xE2A5_2001);
    insert_ordinary(&edge, first_id, "first");
    insert_partitioned(
        &edge,
        Uuid::from_u128(0xE2A5_2002),
        Uuid::from_u128(0xE2A5_20FF),
    );

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node = hub_identity.node_id();
    let first_hub = Arc::new(Database::open_memory());
    let first_server = start_server(
        &broker,
        REBUILT_HUB_TENANT,
        first_hub.clone(),
        hub_identity.clone(),
    )
    .await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge,
        broker.client_as(&edge_node),
        TenantId::from(REBUILT_HUB_TENANT),
        edge_identity,
    );
    client.set_peer_vector_schema_support_for_test(false);
    client
        .push()
        .await
        .expect("first hub receives compatible history");
    assert_eq!(ordinary_ids(&first_hub), vec![first_id]);
    assert_eq!(
        client.schema_sync_holdbacks(),
        expected_holdback(
            "partitioned_notes",
            SchemaSyncCapability::VectorPartitioning,
            &hub_node,
        )
    );
    first_server.stop().await;
    drop(first_hub);

    let rebuilt_hub = Arc::new(Database::open_memory());
    let rebuilt_server = start_server(
        &broker,
        REBUILT_HUB_TENANT,
        rebuilt_hub.clone(),
        hub_identity,
    )
    .await;
    client
        .push()
        .await
        .expect("the rebuilt hub advertises a reset public frontier");
    assert_eq!(
        ordinary_ids(&rebuilt_hub),
        vec![first_id],
        "edge-private progress from the destroyed hub must not skip its replacement"
    );

    rebuilt_server.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_committed_during_schema_only_push_remains_pending_for_the_next_push() {
    let root = tempfile::TempDir::new().expect("temporary schema-only race store");
    let edge_path = root.path().join("schema-race-edge.redb");
    {
        let edge = Database::open(&edge_path).expect("open initial edge");
        declare_mixed_tables(&edge);
        edge.close()
            .expect("close edge so outbound state is reconstructed");
    }
    let edge = Arc::new(Database::open(&edge_path).expect("reopen edge with empty live logs"));
    let hub = Arc::new(Database::open_memory());
    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let running = start_server(&broker, SCHEMA_RACE_TENANT, hub.clone(), hub_identity).await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node = edge_identity.node_id();
    let client = Arc::new(
        SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            broker.client_as(&edge_node),
            TenantId::from(SCHEMA_RACE_TENANT),
            edge_identity,
        ),
    );

    let socket_root = tempfile::Builder::new()
        .prefix("s")
        .tempdir_in(".")
        .expect("short relative Unix-socket directory");
    let socket_path = socket_root.path().join("p");
    let listener = UnixListener::bind(&socket_path).expect("bind deterministic race seam");
    edge.arm_persisted_state_schema_frontier_pause_for_test(socket_path.clone());
    let first_push = tokio::spawn({
        let client = client.clone();
        async move { client.push().await }
    });
    let mut release = tokio::task::spawn_blocking(move || {
        let (mut stream, _) = listener
            .accept()
            .expect("outbound extraction reaches pause");
        let mut reached = [0u8; 1];
        stream
            .read_exact(&mut reached)
            .expect("pause seam announces the frozen snapshot");
        assert_eq!(reached, [1]);
        stream
    })
    .await
    .expect("pause listener does not panic");

    let raced_id = Uuid::from_u128(0xE2A5_3001);
    insert_ordinary(&edge, raced_id, "committed after snapshot");
    release
        .write_all(&[1])
        .expect("release outbound extraction after the concurrent commit");
    first_push
        .await
        .expect("first push task does not panic")
        .expect("schema-only push succeeds");
    assert!(
        ordinary_ids(&hub).is_empty(),
        "the row committed after the outbound snapshot was not part of the first push"
    );

    client
        .push()
        .await
        .expect("the next push must still see the concurrent row");
    assert_eq!(
        ordinary_ids(&hub),
        vec![raced_id],
        "a private bookmark may advance only to work that was actually in the snapshot"
    );

    running.stop().await;
    edge.close().expect("close schema-race edge");
}

// ---------------------------------------------------------------------------
// Synced vector policy applies on the receiver.
//
// Vector build policy is schema: a `SET HNSW (...)` or `SET AUTO_INDEX_AT`
// declared once on the hub must govern every node that can read the clause.
// The receiver's existing-column merge must carry `auto_index_at`, every
// HNSW member, and the desired `vector_policy_revision`, not only
// `partition_key_columns`, `max_partitions`, and `search_mode`: leg 4 checks
// the declared values replicate, leg 5 that a revision advance is scheduled,
// and leg 6 that the edge serves the new crossover threshold.
// ---------------------------------------------------------------------------

fn column_index(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|candidate| candidate == name)
        .unwrap_or_else(|| panic!("column {name} not found: {:?}", result.columns))
}

fn value_at<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a Value {
    &result.rows[row][column_index(result, name)]
}

fn int_or_null_at(result: &QueryResult, row: usize, name: &str) -> Option<i64> {
    match value_at(result, row, name) {
        Value::Int64(value) => Some(*value),
        Value::Null => None,
        other => panic!("{name} is an integer or NULL, got {other:?}"),
    }
}

fn text_at<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a str {
    match value_at(result, row, name) {
        Value::Text(value) => value,
        other => panic!("{name} is text, got {other:?}"),
    }
}

fn show_vector_indexes(db: &Database) -> QueryResult {
    db.execute("SHOW VECTOR_INDEXES", &empty())
        .expect("SHOW VECTOR_INDEXES is an admin inspection surface")
}

fn index_row_for(result: &QueryResult, table: &str, column: &str) -> usize {
    let table_col = column_index(result, "table");
    let column_col = column_index(result, "column");
    result
        .rows
        .iter()
        .position(|values| {
            values[table_col] == Value::Text(table.to_owned())
                && values[column_col] == Value::Text(column.to_owned())
        })
        .unwrap_or_else(|| panic!("SHOW VECTOR_INDEXES describes {table}.{column}: {result:?}"))
}

fn show_vector_partitions(db: &Database, table: &str, column: &str) -> QueryResult {
    db.execute(
        &format!("SHOW VECTOR_PARTITIONS FOR {table}.{column}"),
        &empty(),
    )
    .expect("SHOW VECTOR_PARTITIONS is an admin inspection surface")
}

#[tokio::test]
async fn a_synced_vector_policy_change_reaches_the_receivers_existing_column() {
    let root = tempfile::TempDir::new().expect("temporary policy-sync stores");
    let hub = Arc::new(
        Database::open(root.path().join("policy-hub.redb")).expect("open file-backed hub"),
    );
    let edge = Arc::new(
        Database::open(root.path().join("policy-edge.redb")).expect("open file-backed edge"),
    );
    hub.execute(
        "CREATE TABLE docs (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO\
         ) SYNC TWO WAY",
        &empty(),
    )
    .expect("declare the policy-synced vector table on the hub");
    let scope_a = Uuid::from_u128(0xA911_0000_0000_0000_0000_0000_0000_0001);
    let scope_b = Uuid::from_u128(0xA911_0000_0000_0000_0000_0000_0000_0002);
    for (offset, scope) in [(0u128, scope_a), (1u128, scope_b)] {
        hub.execute(
            "INSERT INTO docs (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(
                        0xA911_0000_0000_0000_0000_0000_0000_0010 + offset,
                    )),
                ),
                ("scope", Value::Uuid(scope)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("seed one row per scope");
    }

    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let running = start_server(&broker, POLICY_SYNC_TENANT, hub.clone(), hub_identity).await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_node),
        TenantId::from(POLICY_SYNC_TENANT),
        edge_identity,
    );
    client.pull_default().await.expect(
        "both peers run at PROTOCOL_VERSION: the vector policy vocabulary is not held back",
    );
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        Vec::<SchemaSyncHoldback>::new(),
        "both peers at PROTOCOL_VERSION must hold nothing back"
    );

    // Leg 2 -- baseline on the edge.
    let baseline = show_vector_indexes(&edge);
    let baseline_row = index_row_for(&baseline, "docs", "embedding");
    assert_eq!(
        int_or_null_at(&baseline, baseline_row, "declared_hnsw_m"),
        None
    );
    assert_eq!(
        int_or_null_at(&baseline, baseline_row, "declared_hnsw_ef_construction"),
        None
    );
    assert_eq!(
        int_or_null_at(&baseline, baseline_row, "declared_auto_index_at"),
        None
    );
    let baseline_partitions = show_vector_partitions(&edge, "docs", "embedding");
    let baseline_partition_row = 0;
    let baseline_desired_revision = int_or_null_at(
        &baseline_partitions,
        baseline_partition_row,
        "desired_policy_revision",
    )
    .expect("desired_policy_revision is always populated");

    // Leg 3 -- hub declares HNSW and AUTO_INDEX_AT.
    hub.execute(
        "ALTER TABLE docs ALTER COLUMN embedding SET HNSW (M = 24, EF_CONSTRUCTION = 400, EF_SEARCH = 128)",
        &empty(),
    )
    .expect("declare HNSW policy on the hub");
    hub.execute(
        "ALTER TABLE docs ALTER COLUMN embedding SET AUTO_INDEX_AT 2500",
        &empty(),
    )
    .expect("declare the auto-index crossover on the hub");
    client
        .pull_default()
        .await
        .expect("the edge pulls the policy-only ALTERs");
    assert_eq!(
        running.server.schema_sync_holdbacks(),
        Vec::<SchemaSyncHoldback>::new()
    );

    // Leg 4 -- declared values replicate, compared against the hub's own row.
    let hub_indexes = show_vector_indexes(&hub);
    let hub_row = index_row_for(&hub_indexes, "docs", "embedding");
    let edge_indexes = show_vector_indexes(&edge);
    let edge_row = index_row_for(&edge_indexes, "docs", "embedding");
    for field in [
        "declared_hnsw_m",
        "declared_hnsw_ef_construction",
        "declared_hnsw_ef_search",
        "declared_auto_index_at",
    ] {
        assert_eq!(
            int_or_null_at(&edge_indexes, edge_row, field),
            int_or_null_at(&hub_indexes, hub_row, field),
            "{field} must replicate from the hub's own declaration, not a constant"
        );
    }
    assert_eq!(
        int_or_null_at(&edge_indexes, edge_row, "declared_hnsw_m"),
        Some(24)
    );

    // Leg 5 -- a replacement build is scheduled: the edge's desired revision
    // strictly advances past the baseline and past its own serving revision.
    let after_partitions = show_vector_partitions(&edge, "docs", "embedding");
    for row in 0..after_partitions.rows.len() {
        let desired = int_or_null_at(&after_partitions, row, "desired_policy_revision")
            .expect("desired_policy_revision is always populated");
        assert!(
            desired > baseline_desired_revision,
            "the edge's desired policy revision must strictly advance: baseline {baseline_desired_revision}, now {desired}"
        );
        let expected_reason = if let Some(serving) =
            int_or_null_at(&after_partitions, row, "serving_policy_revision")
        {
            assert!(
                desired > serving,
                "a scheduled replacement build means desired outstrips serving: desired {desired}, serving {serving}"
            );
            // Already served under an earlier policy: the outstanding work is a
            // replacement build.
            "new_changes"
        } else {
            // Never yet built: the outstanding work is the first build, not a
            // replacement.
            "initial_build"
        };
        assert_eq!(
            text_at(&after_partitions, row, "maintenance_reason"),
            expected_reason,
            "the outstanding maintenance work must be named for the affected partition, \
             not a departure from the idle steady state: {after_partitions:?}"
        );
    }

    // Leg 6 -- the new crossover is in effect.
    let explained = edge
        .explain_output(
            "SELECT id FROM docs WHERE scope_id = 'A9110000-0000-0000-0000-000000000001' \
             ORDER BY embedding <=> [1,0,0] LIMIT 2",
        )
        .expect("explain must succeed on the edge");
    let disclosure = explained
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");
    assert_eq!(disclosure.effective_auto_index_at, 2500);
    assert_ne!(
        disclosure.auto_index_at_source, "compatibility_profile",
        "the effective crossover must be sourced from the declaration, not the compatibility profile: {disclosure:?}"
    );

    // Leg 7 -- EF_SEARCH-only does not schedule a rebuild.
    let revision_after_hnsw = int_or_null_at(
        &show_vector_partitions(&edge, "docs", "embedding"),
        0,
        "desired_policy_revision",
    )
    .expect("desired_policy_revision is always populated");
    hub.execute(
        "ALTER TABLE docs ALTER COLUMN embedding SET HNSW (EF_SEARCH = 200)",
        &empty(),
    )
    .expect("declare an EF_SEARCH-only change on the hub");
    client
        .pull_default()
        .await
        .expect("the edge pulls the EF_SEARCH-only ALTER");
    let ef_search_indexes = show_vector_indexes(&edge);
    let ef_search_row = index_row_for(&ef_search_indexes, "docs", "embedding");
    assert_eq!(
        int_or_null_at(&ef_search_indexes, ef_search_row, "declared_hnsw_ef_search"),
        Some(200)
    );
    let revision_after_ef_search = int_or_null_at(
        &show_vector_partitions(&edge, "docs", "embedding"),
        0,
        "desired_policy_revision",
    )
    .expect("desired_policy_revision is always populated");
    assert_eq!(
        revision_after_ef_search, revision_after_hnsw,
        "an EF_SEARCH-only change must not advance the desired policy revision"
    );

    // Leg 8 -- clearing replicates.
    hub.execute(
        "ALTER TABLE docs ALTER COLUMN embedding SET HNSW DEFAULT",
        &empty(),
    )
    .expect("clear the HNSW declaration on the hub");
    hub.execute(
        "ALTER TABLE docs ALTER COLUMN embedding SET AUTO_INDEX_AT DEFAULT",
        &empty(),
    )
    .expect("clear the auto-index declaration on the hub");
    client
        .pull_default()
        .await
        .expect("the edge pulls the DEFAULT clauses");
    let cleared_indexes = show_vector_indexes(&edge);
    let cleared_row = index_row_for(&cleared_indexes, "docs", "embedding");
    for field in [
        "declared_hnsw_m",
        "declared_hnsw_ef_construction",
        "declared_hnsw_ef_search",
        "declared_auto_index_at",
    ] {
        assert_eq!(
            int_or_null_at(&cleared_indexes, cleared_row, field),
            None,
            "{field} must go silent again once the hub clears it"
        );
    }
    let revision_after_clear = int_or_null_at(
        &show_vector_partitions(&edge, "docs", "embedding"),
        0,
        "desired_policy_revision",
    )
    .expect("desired_policy_revision is always populated");
    assert!(
        revision_after_clear > revision_after_ef_search,
        "clearing M and EF_CONSTRUCTION must advance the desired policy revision again"
    );

    // Leg 9 -- rows keep flowing: an ordinary insert after every ALTER reaches the edge.
    hub.execute(
        "INSERT INTO docs (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params([
            (
                "id",
                Value::Uuid(Uuid::from_u128(0xA911_0000_0000_0000_0000_0000_0000_0099)),
            ),
            ("scope", Value::Uuid(scope_a)),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
        ]),
    )
    .expect("an ordinary insert after the policy DDL must succeed");
    client
        .pull_default()
        .await
        .expect("the policy DDL never wedges the page");
    assert_eq!(
        edge.execute("SELECT id FROM docs", &empty())
            .expect("the edge can read its rows")
            .rows
            .len(),
        3,
        "the row inserted after every ALTER must reach the edge"
    );

    running.stop().await;
    edge.close().expect("close policy-sync edge");
}
