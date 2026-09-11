//! A partial authoritative purge must replace, not merely invalidate, the
//! affected partition's durable generation. The user has already paid for a
//! maintained indexed route; permanently forgetting one row cannot make every
//! surviving row unavailable until a later maintenance call.

#![cfg(feature = "test-seams")]

use contextdb_core::{Error, TenantId, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{
    Database, DatabaseOpenOptions, DeliveryManifest, DeliveryOutcomeKind, MaintenancePolicy,
    QueryResult,
};
use contextdb_server::subjects::push_subject;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const MAX_MAINTENANCE_CYCLES: usize = 16;
const ERASURE_VECTOR_DIMENSION: usize = 256;
const ERASURE_VECTOR_ROWS: usize = 96;
const DISCARD_MEMORY_HEADROOM: usize = 2_500_000;
const ERASURE_TENANT: &str = "quantized-erasure-admission";
const ERASURE_ROOT_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE \
     DELIVERY MANIFEST OVER erasure_vectors EDGE DISCARD ALWAYS";
const ERASURE_VECTOR_CLAUSES: &str =
    "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE EDGE DISCARD ALWAYS";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("the authenticated fixture exchange completes within 60 seconds")
}

fn open_erasure_store(path: &std::path::Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the erasure store with caller-driven maintenance from its first instruction")
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("partial_purge_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("one UUID partition component has one canonical key")
}

fn partition_ref(scope: Uuid) -> VectorPartitionRef {
    VectorPartitionRef::new(index(), partition(scope))
}

fn durable_generation_id(db: &Database, scope: Uuid) -> Option<u64> {
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition_ref(scope))?;
    status
        .base
        .or(status.dormant_base)
        .map(|generation| generation.generation_id)
}

fn graph_available(db: &Database, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .partition_info(&index(), &partition(scope))
        .is_some_and(|info| info.graph_available)
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

fn indexed_search(db: &Database, scope: Uuid) -> Vec<Uuid> {
    ids(&db
        .execute(
            "SELECT id FROM partial_purge_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 2",
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("the maintained partition answers through INDEXED"))
}

fn drive_maintenance_until_ready(db: &Database, scope: Uuid) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if graph_available(db, scope) {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!("maintenance did not publish the route within {MAX_MAINTENANCE_CYCLES} cycles");
}

fn erasure_index(column: &str) -> VectorIndexRef {
    VectorIndexRef::new("erasure_vectors", column)
}

fn erasure_partition_ref(column: &str, scope: Uuid) -> VectorPartitionRef {
    VectorPartitionRef::new(erasure_index(column), partition(scope))
}

fn erasure_routes_ready(db: &Database, scope: Uuid) -> bool {
    ["compact", "tiny"].into_iter().all(|column| {
        db.vector_store_for_test()
            .partition_graph_generation_status(&erasure_partition_ref(column, scope))
            .is_some_and(|status| status.base.is_some() || status.dormant_base.is_some())
    })
}

fn drive_erasure_maintenance_until_ready(db: &Database, scope: Uuid) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if erasure_routes_ready(db, scope) {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!("quantized erasure routes did not publish within {MAX_MAINTENANCE_CYCLES} cycles");
}

fn erasure_vector(ordinal: usize) -> Vec<f32> {
    let mut vector = vec![0.0; ERASURE_VECTOR_DIMENSION];
    vector[ordinal % ERASURE_VECTOR_DIMENSION] = 1.0;
    vector[(ordinal.saturating_mul(17).saturating_add(1)) % ERASURE_VECTOR_DIMENSION] = 0.25;
    vector
}

fn erasure_key(id: Uuid) -> NaturalKey {
    NaturalKey::single("id".to_string(), Value::Uuid(id))
}

fn erasure_policy() -> ApplicationTablePolicyExpectation {
    ApplicationTablePolicyExpectation::new()
        .expect_table("erasure_roots", ERASURE_ROOT_CLAUSES)
        .expect("the root policy is canonical declaration text")
        .expect_table("erasure_vectors", ERASURE_VECTOR_CLAUSES)
        .expect("the vector-member policy is canonical declaration text")
}

fn create_erasure_tables(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE erasure_roots (id UUID PRIMARY KEY, batch_ref TEXT NOT NULL) \
             {ERASURE_ROOT_CLAUSES}"
        ),
        &empty(),
    )
    .expect("create the non-vector root table");
    db.execute(
        &format!(
            "CREATE TABLE erasure_vectors (\
             id UUID PRIMARY KEY, \
             root_id UUID NOT NULL REFERENCES erasure_roots(id), \
             scope_id UUID NOT NULL, \
             batch_ref TEXT NOT NULL, \
             compact VECTOR({ERASURE_VECTOR_DIMENSION}) WITH (quantization = 'SQ8') \
               PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE INDEXED AUTO_INDEX_AT 1, \
             tiny VECTOR({ERASURE_VECTOR_DIMENSION}) WITH (quantization = 'SQ4') \
               PARTITION_KEY (scope_id) MAX_PARTITIONS 4 SEARCH_MODE INDEXED AUTO_INDEX_AT 1\
             ) {ERASURE_VECTOR_CLAUSES}"
        ),
        &empty(),
    )
    .expect("create the later quantized vector table");
}

async fn build_quantized_erasure_fixture(
    hub_path: &std::path::Path,
    edge_path: &std::path::Path,
) -> (Uuid, Uuid, Uuid, Uuid) {
    let scope = Uuid::from_u128(0xE7A5_0000);
    let selected_root = Uuid::from_u128(0xE7A5_1000);
    let surviving_root = Uuid::from_u128(0xE7A5_1001);
    let selected_vector = Uuid::from_u128(0xE7A5_2000);
    let broker = InProcessBroker::new();

    let hub = Arc::new(open_erasure_store(hub_path));
    create_erasure_tables(&hub);
    hub.__seed_tenant_table_policies_for_test(TenantId::from(ERASURE_TENANT), erasure_policy())
        .expect("install the hub's durable table-policy prerequisites");
    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub.clone(),
            broker.server_as(&hub_node),
            TenantId::from(ERASURE_TENANT),
            hub_node,
            hub_identity,
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    within(broker.wait_for_registered_route_for_test(&push_subject(ERASURE_TENANT))).await;

    let edge = Arc::new(open_erasure_store(edge_path));
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&node_id),
        TenantId::from(ERASURE_TENANT),
        identity,
    );
    within(client.__seed_application_table_policy_binding_for_test(erasure_policy()))
        .await
        .expect("bind both edge declarations to the authoritative hub policy");
    create_erasure_tables(&edge);

    let selected_tx = edge.begin().expect("begin the selected custody unit");
    edge.insert_row(
        selected_tx,
        "erasure_roots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(selected_root)),
            ("batch_ref".to_string(), Value::Text("selected".to_string())),
        ]),
    )
    .expect("insert the selected non-vector root");
    let selected_body = erasure_vector(0);
    edge.insert_row(
        selected_tx,
        "erasure_vectors",
        HashMap::from([
            ("id".to_string(), Value::Uuid(selected_vector)),
            ("root_id".to_string(), Value::Uuid(selected_root)),
            ("scope_id".to_string(), Value::Uuid(scope)),
            ("batch_ref".to_string(), Value::Text("selected".to_string())),
            ("compact".to_string(), Value::Vector(selected_body.clone())),
            ("tiny".to_string(), Value::Vector(selected_body)),
        ]),
    )
    .expect("insert the selected SQ8/SQ4 row");
    client
        .__stage_delivery_manifest_for_test(
            selected_tx,
            DeliveryManifest {
                root_table: "erasure_roots",
                root_key: erasure_key(selected_root),
                members: vec![],
            },
        )
        .expect("register the selected row's durable custody manifest");
    edge.commit(selected_tx)
        .expect("commit the selected row and custody manifest together");

    let survivor_tx = edge.begin().expect("begin survivor population");
    edge.insert_row(
        survivor_tx,
        "erasure_roots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(surviving_root)),
            ("batch_ref".to_string(), Value::Text("survivor".to_string())),
        ]),
    )
    .expect("insert the surviving non-vector root");
    for ordinal in 1..ERASURE_VECTOR_ROWS {
        let id = Uuid::from_u128(selected_vector.as_u128() + ordinal as u128);
        let body = erasure_vector(ordinal);
        edge.insert_row(
            survivor_tx,
            "erasure_vectors",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("root_id".to_string(), Value::Uuid(surviving_root)),
                ("scope_id".to_string(), Value::Uuid(scope)),
                ("batch_ref".to_string(), Value::Text("survivor".to_string())),
                ("compact".to_string(), Value::Vector(body.clone())),
                ("tiny".to_string(), Value::Vector(body)),
            ]),
        )
        .expect("insert one surviving SQ8/SQ4 row");
    }
    client
        .__stage_delivery_manifest_for_test(
            survivor_tx,
            DeliveryManifest {
                root_table: "erasure_roots",
                root_key: erasure_key(surviving_root),
                members: vec![],
            },
        )
        .expect("register the surviving root's explicit empty member set");
    edge.commit(survivor_tx)
        .expect("commit the surviving rows and custody manifest together");

    within(client.__seed_delivery_outcome_for_test(
        "erasure_roots",
        &erasure_key(selected_root),
        DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .expect("materialize and answer the selected unit through the custody fixture seam");
    within(client.__seed_delivery_outcome_for_test(
        "erasure_roots",
        &erasure_key(surviving_root),
        DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .expect("materialize and answer the surviving unit through the custody fixture seam");

    let hub_vectors = hub.begin().expect("begin authoritative vector population");
    for ordinal in 0..ERASURE_VECTOR_ROWS {
        let id = Uuid::from_u128(selected_vector.as_u128() + ordinal as u128);
        let (root_id, batch_ref) = if ordinal == 0 {
            (selected_root, "selected")
        } else {
            (surviving_root, "survivor")
        };
        let body = erasure_vector(ordinal);
        hub.insert_row(
            hub_vectors,
            "erasure_vectors",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("root_id".to_string(), Value::Uuid(root_id)),
                ("scope_id".to_string(), Value::Uuid(scope)),
                ("batch_ref".to_string(), Value::Text(batch_ref.to_string())),
                ("compact".to_string(), Value::Vector(body.clone())),
                ("tiny".to_string(), Value::Vector(body)),
            ]),
        )
        .expect("insert one authoritative SQ8/SQ4 row");
    }
    hub.commit(hub_vectors)
        .expect("commit the authoritative quantized rows");
    assert_eq!(
        (
            hub.execute("SELECT id FROM erasure_roots", &empty())
                .unwrap()
                .rows
                .len(),
            hub.execute("SELECT id FROM erasure_vectors", &empty())
                .unwrap()
                .rows
                .len(),
        ),
        (2, ERASURE_VECTOR_ROWS),
        "the hub materializes both roots and every vector member"
    );
    assert_eq!(
        edge.delivery_status("erasure_roots").unwrap().accepted,
        2,
        "the edge retains both authenticated custody outcomes"
    );
    assert!(
        [selected_root, surviving_root].into_iter().all(|root| hub
            .delivery_outcome("erasure_roots", &erasure_key(root))
            .unwrap()
            .is_some()),
        "the hub retains both authoritative answers without counting received roots as eligible"
    );

    drive_erasure_maintenance_until_ready(&edge, scope);
    drive_erasure_maintenance_until_ready(&hub, scope);

    within(client.shutdown()).await;
    stop.store(true, Ordering::SeqCst);
    within(server_task)
        .await
        .expect("the in-process fixture hub stops cleanly");
    drop(server);
    drop(client);
    edge.close().expect("close the maintained edge fixture");
    hub.close().expect("close the maintained hub fixture");
    (scope, selected_root, surviving_root, selected_vector)
}

#[derive(Debug, PartialEq)]
struct ErasureState {
    roots: Vec<Vec<Value>>,
    vectors: Vec<Vec<Value>>,
    vector_directory: Vec<String>,
    generations: Vec<String>,
    delivery_status: contextdb_engine::DeliveryStatusCounts,
    selected_has_outcome: bool,
}

fn erasure_state(db: &Database, scope: Uuid, selected_root: Uuid) -> ErasureState {
    let roots = db
        .execute(
            "SELECT id, batch_ref FROM erasure_roots ORDER BY id",
            &empty(),
        )
        .expect("read all root rows")
        .rows;
    let vectors = db
        .execute(
            "SELECT id, root_id, scope_id, batch_ref FROM erasure_vectors ORDER BY id",
            &empty(),
        )
        .expect("read all vector-owner rows")
        .rows;
    let mut vector_directory = db
        .vector_store_for_test()
        .raw_directory_entries()
        .into_iter()
        .filter(|(index, _)| index.table == "erasure_vectors")
        .map(|(index, entry)| format!("{}:{entry:?}", index.column))
        .collect::<Vec<_>>();
    vector_directory.sort();
    let generations = ["compact", "tiny"]
        .into_iter()
        .map(|column| {
            let status = db
                .vector_store_for_test()
                .partition_graph_generation_status(&erasure_partition_ref(column, scope));
            format!("{column}:{status:?}")
        })
        .collect();
    ErasureState {
        roots,
        vectors,
        vector_directory,
        generations,
        delivery_status: db
            .delivery_status("erasure_roots")
            .expect("read durable custody status"),
        selected_has_outcome: db
            .delivery_outcome("erasure_roots", &erasure_key(selected_root))
            .expect("read the selected root's outcome")
            .is_some(),
    }
}

fn assert_quantized_membership(
    db: &Database,
    scope: Uuid,
    selected_vector: Uuid,
    selected_expected: bool,
) {
    for column in ["compact", "tiny"] {
        let result = db
            .execute(
                &format!(
                    "SELECT id FROM erasure_vectors WHERE scope_id = $scope \
                     ORDER BY {column} <=> $query USE VECTOR INDEXED LIMIT {ERASURE_VECTOR_ROWS}"
                ),
                &params([
                    ("scope", Value::Uuid(scope)),
                    ("query", Value::Vector(erasure_vector(0))),
                ]),
            )
            .unwrap_or_else(|error| panic!("{column} survivor route must answer: {error}"));
        let returned = ids(&result);
        assert_eq!(
            returned.len(),
            ERASURE_VECTOR_ROWS - usize::from(!selected_expected),
            "{column} returns every surviving vector"
        );
        assert_eq!(
            returned.contains(&selected_vector),
            selected_expected,
            "{column} membership agrees with the erasure outcome"
        );
    }
}

fn assert_erasure_routes_dormant(db: &Database, scope: Uuid) {
    for column in ["compact", "tiny"] {
        let status = db
            .vector_store_for_test()
            .partition_graph_generation_status(&erasure_partition_ref(column, scope))
            .unwrap_or_else(|| panic!("{column} retains its durable generation descriptor"));
        assert!(
            status.base.is_none() && status.dormant_base.is_some(),
            "{column} starts dormant so erasure must admit loading and replacement: {status:?}"
        );
    }
}

#[tokio::test]
async fn multi_table_erasure_admits_every_dormant_quantized_replacement_before_mutation() {
    let root = TempDir::new().expect("temporary store directory");
    let purge_path = root.path().join("quantized-erasure-hub.db");
    let discard_path = root.path().join("quantized-erasure-edge.db");
    let (scope, selected_root, surviving_root, selected_vector) =
        build_quantized_erasure_fixture(&purge_path, &discard_path).await;
    let statement = "FROM erasure_roots WHERE batch_ref = $batch, \
                     erasure_vectors WHERE batch_ref = $batch";
    let selected = params([("batch", Value::Text("selected".to_string()))]);

    let purge = open_erasure_store(&purge_path);
    assert_erasure_routes_dormant(&purge, scope);
    purge
        .set_disk_limit(Some(1))
        .expect("install a finite disk limit below generation headroom");
    let purge_before = erasure_state(&purge, scope, selected_root);
    let purge_result = purge.execute(&format!("PURGE {statement}"), &selected);
    let purge_after = erasure_state(&purge, scope, selected_root);
    let purge_workspace_after = purge
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    purge.close().expect("close after the purge refusal");
    let purge = open_erasure_store(&purge_path);
    let purge_reopened = erasure_state(&purge, scope, selected_root);

    let discard = open_erasure_store(&discard_path);
    assert_erasure_routes_dormant(&discard, scope);
    let used = discard.accountant().usage().used;
    discard
        .set_memory_limit(Some(used.saturating_add(DISCARD_MEMORY_HEADROOM)))
        .expect("install finite headroom that admits dormant loading but not the full replacement");
    let discard_before = erasure_state(&discard, scope, selected_root);
    let discard_result = discard.execute(&format!("DISCARD {statement}"), &selected);
    let discard_after = erasure_state(&discard, scope, selected_root);
    let discard_workspace_after = discard
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    discard.close().expect("close after the discard refusal");
    let discard = open_erasure_store(&discard_path);
    let discard_reopened = erasure_state(&discard, scope, selected_root);

    let purge_refused = matches!(
        &purge_result,
        Err(Error::DiskBudgetExceeded { operation, .. }) if operation == "prepare_vector_generation"
    );
    let discard_refused = matches!(
        &discard_result,
        Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "prepare_authoritative_vector_purge"
    );
    assert!(
        purge_refused && discard_refused,
        "both erasure paths must refuse at their complete outer admission: \
         purge={purge_result:?}; discard={discard_result:?}"
    );
    assert_eq!(
        purge_before, purge_after,
        "purge refusal is atomic in memory"
    );
    assert_eq!(
        purge_before, purge_reopened,
        "purge refusal leaves rows, vectors, generations, and custody durable state unchanged"
    );
    assert_eq!(
        purge_workspace_after, 0,
        "purge returns its failed workspace"
    );
    assert_eq!(
        discard_before, discard_after,
        "discard refusal rolls its ordinary transaction back in memory"
    );
    assert_eq!(
        discard_before, discard_reopened,
        "discard refusal leaves rows, vectors, generations, and custody durable state unchanged"
    );
    assert_eq!(
        discard_workspace_after, 0,
        "discard returns its failed workspace instead of stranding a staged owner"
    );

    purge
        .set_disk_limit(None)
        .expect("restore disk headroom for the admitted retry");
    purge
        .execute(&format!("PURGE {statement}"), &selected)
        .expect("the same complete purge succeeds with sufficient headroom");
    assert_eq!(
        purge
            .execute("SELECT id FROM erasure_roots ORDER BY id", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Uuid(surviving_root)]]
    );
    assert_quantized_membership(&purge, scope, selected_vector, false);
    assert_eq!(
        purge
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0,
        "successful purge releases its workspace after publication"
    );

    discard
        .set_memory_limit(None)
        .expect("restore memory headroom for the admitted retry");
    discard
        .execute(&format!("DISCARD {statement}"), &selected)
        .expect("the same complete local discard succeeds with sufficient headroom");
    assert_eq!(
        discard
            .execute("SELECT id FROM erasure_roots ORDER BY id", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Uuid(surviving_root)]]
    );
    assert_quantized_membership(&discard, scope, selected_vector, false);
    assert_eq!(
        {
            let status = discard
                .delivery_status("erasure_roots")
                .expect("read custody after local discard");
            (status.eligible, status.accepted, status.pending)
        },
        (1, 1, 0),
        "local discard erases only the selected custody manifest and outcome"
    );
    assert!(
        discard
            .delivery_outcome("erasure_roots", &erasure_key(selected_root))
            .expect("read the discarded root's outcome")
            .is_none(),
        "local discard takes the selected outcome with its rows"
    );
    assert_eq!(
        discard
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0,
        "successful discard releases its staged workspace after publication"
    );
}

#[test]
fn partial_purge_atomically_publishes_a_survivor_generation_from_a_dormant_route() {
    let root = TempDir::new().expect("temporary store directory");
    let path = root
        .path()
        .join("partial-authoritative-purge-generation.db");
    let scope = Uuid::from_u128(0xA550);
    let forgotten = Uuid::from_u128(0xA500_0000);
    let nearest_survivor = Uuid::from_u128(0xA500_0001);

    {
        let db = Database::open(&path).expect("open the partial-purge fixture");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE partial_purge_items (\
             id UUID PRIMARY KEY, \
             scope_id UUID NOT NULL, \
             embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 4 \
             SEARCH_MODE INDEXED AUTO_INDEX_AT 3 \
             HNSW (M = 24, EF_CONSTRUCTION = 128, EF_SEARCH = 7)\
             )",
            &empty(),
        )
        .expect("create the partitioned vector table");
        for ordinal in 0..8_u128 {
            let id = Uuid::from_u128(forgotten.as_u128() + ordinal);
            let vector = match ordinal {
                0 => vec![1.0, 0.0, 0.0],
                1 => vec![0.95, 0.05, 0.0],
                _ => vec![0.0, 0.0, 1.0],
            };
            db.execute(
                "INSERT INTO partial_purge_items (id, scope_id, embedding) \
                 VALUES ($id, $scope, $embedding)",
                &params([
                    ("id", Value::Uuid(id)),
                    ("scope", Value::Uuid(scope)),
                    ("embedding", Value::Vector(vector)),
                ]),
            )
            .expect("insert one vector row");
        }
        drive_maintenance_until_ready(&db, scope);
        db.close().expect("seal and close the maintained fixture");
    }

    let replacement_generation = {
        let db = Database::open(&path).expect("reopen with a dormant saved route");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let old_generation = durable_generation_id(&db, scope)
            .expect("clean reopen registers the old generation without loading it");

        // Warm a parameterized write plan without changing or loading the dormant
        // vector route. Cached DML must resolve membership again after erasure.
        let cached_update = "UPDATE partial_purge_items SET scope_id = $scope WHERE id = $id";
        assert_eq!(
            db.execute(
                cached_update,
                &params([
                    ("scope", Value::Uuid(scope)),
                    ("id", Value::Uuid(Uuid::nil())),
                ])
            )
            .unwrap()
            .rows_affected,
            0
        );
        assert!(db.__statement_cache_len() > 0);

        let purged = db
            .execute(
                "PURGE FROM partial_purge_items WHERE id = $id",
                &params([("id", Value::Uuid(forgotten))]),
            )
            .expect("partial authoritative purge commits");
        assert_eq!(purged.rows_affected, 1);
        assert_eq!(
            db.execute(
                cached_update,
                &params([
                    ("scope", Value::Uuid(scope)),
                    ("id", Value::Uuid(forgotten)),
                ])
            )
            .unwrap()
            .rows_affected,
            0,
            "a cached predicate cannot find the purged lineage"
        );
        let replacement_generation = durable_generation_id(&db, scope)
            .expect("successful purge publishes a complete replacement generation");
        assert!(
            replacement_generation > old_generation,
            "the survivor route has a new durable identity instead of the stale pre-purge one"
        );
        assert_eq!(
            indexed_search(&db, scope).first().copied(),
            Some(nearest_survivor),
            "survivors remain indexed immediately, without a maintenance call"
        );
        assert!(
            !indexed_search(&db, scope).contains(&forgotten),
            "the replacement generation cannot address the permanently purged row"
        );
        assert_eq!(
            durable_generation_id(&db, scope),
            Some(replacement_generation),
            "the first query consumes the prepared generation instead of rebuilding it"
        );
        let layout = db.vector_store_for_test().index_layout(&index()).unwrap();
        let desired = layout.resolve_policy(7, 2);
        let disclosure = db
            .execute(
                "SELECT id FROM partial_purge_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 2",
                &params([
                    ("scope", Value::Uuid(scope)),
                    ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
                ]),
            )
            .unwrap()
            .trace
            .vector_search
            .unwrap();
        let serving = &disclosure.partition_hnsw[0];
        assert_eq!(
            (
                serving.hnsw_m,
                serving.hnsw_ef_construction,
                serving.hnsw_ef_search
            ),
            (24, 128, 7)
        );
        assert_eq!(serving.policy_revision, desired.policy_revision);
        db.close().expect("close after partial purge");
        replacement_generation
    };

    let db = Database::open(&path).expect("clean reopen after partial purge");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        durable_generation_id(&db, scope),
        Some(replacement_generation),
        "restart registers exactly the replacement generation before query"
    );
    assert_eq!(
        indexed_search(&db, scope).first().copied(),
        Some(nearest_survivor),
        "restart retains the immediate indexed survivor route"
    );
    assert!(!indexed_search(&db, scope).contains(&forgotten));
    assert_eq!(
        durable_generation_id(&db, scope),
        Some(replacement_generation),
        "restart query loads the saved generation without changing its identity"
    );
}
