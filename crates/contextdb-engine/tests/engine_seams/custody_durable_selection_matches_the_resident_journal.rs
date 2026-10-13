//! A file-backed writer answers every custody selection from the durable group
//! index with exactly the records the resident journal would have answered:
//! the same signed bytes, in the same order, for every selector shape.
use contextdb_core::{TenantId, Value};
use contextdb_engine::database::DeliveryManifest;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_server::subjects::push_subject;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const TENANT: &str = "custody-parity";
const ROOT_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER parts";
const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}
fn key(id: Uuid) -> NaturalKey {
    NaturalKey::single("id".into(), Value::Uuid(id))
}
fn expectation() -> ApplicationTablePolicyExpectation {
    ApplicationTablePolicyExpectation::new()
        .expect_table("records", ROOT_CLAUSES)
        .unwrap()
        .expect_table("parts", MEMBER_CLAUSES)
        .unwrap()
}
fn declare(db: &Database) {
    db.execute(
        &format!("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {ROOT_CLAUSES}"),
        &p(),
    )
    .unwrap();
    db.execute(
        &format!("CREATE TABLE parts (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) {MEMBER_CLAUSES}"),
        &p(),
    )
    .unwrap();
}
fn commit_unit(db: &Database, root: Uuid, member: Option<Uuid>) {
    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "records",
        HashMap::from([
            ("id".into(), Value::Uuid(root)),
            ("body".into(), Value::Text("root".into())),
        ]),
    )
    .unwrap();
    let mut members = Vec::new();
    if let Some(member) = member {
        db.insert_row(
            tx,
            "parts",
            HashMap::from([
                ("id".into(), Value::Uuid(member)),
                ("record_id".into(), Value::Uuid(root)),
                ("body".into(), Value::Text("member".into())),
            ]),
        )
        .unwrap();
        members.push(("parts", key(member)));
    }
    db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: "records",
            root_key: key(root),
            members,
        },
    )
    .unwrap();
    db.commit(tx).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn custody_durable_selection_matches_the_resident_journal() {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    declare(&hub_db);
    hub_db
        .__seed_tenant_table_policies_for_test(TenantId::from(TENANT), expectation())
        .unwrap();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub_db.clone(),
            broker.server_as(&hub_node),
            TenantId::from(TENANT),
            hub_node,
            hub_identity,
        )
        .unwrap(),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&push_subject(TENANT))
        .await;

    let edge = Arc::new(
        Database::open_with_options(
            dir.path().join("edge.db"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..DatabaseOpenOptions::default()
            },
        )
        .unwrap(),
    );
    let identity = Arc::new(FabricIdentity::generate());
    let node = identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&node),
        TenantId::from(TENANT),
        identity,
    )
    .unwrap();
    client
        .__seed_application_table_policy_binding_for_test(
            expectation(),
            edge.declared_reader_memory().unwrap(),
        )
        .await
        .unwrap();
    declare(&edge);

    // Delivered units carry hub outcomes (terminal and object records);
    // later units stay pending; one delivered and one pending root are then
    // updated, retiring their ownership.
    let roots = (0..8u128)
        .map(|i| Uuid::from_u128(0x6100_0000_0000_0000_0000_0000_0000_0000 + i))
        .collect::<Vec<_>>();
    for (i, root) in roots.iter().enumerate() {
        let member = i
            .is_multiple_of(2)
            .then(|| Uuid::from_u128(0x6200_0000_0000_0000_0000_0000_0000_0000 + i as u128));
        commit_unit(&edge, *root, member);
        if i < 4 {
            client.push().await.unwrap();
        }
    }
    for root in [roots[1], roots[6]] {
        edge.execute(
            "UPDATE records SET body = 'changed' WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(root))]),
        )
        .unwrap();
    }

    let keys = roots.iter().map(|r| key(*r)).collect::<Vec<_>>();
    let answers = edge
        .__custody_selection_parity_for_test("records", &keys)
        .unwrap()
        .expect("a store created empty carries a complete durable group index");
    for (selector, resident, durable) in &answers {
        assert_eq!(
            resident, durable,
            "durable answer for {selector} differs from the resident journal"
        );
    }
    let count = |label: &str| {
        answers
            .iter()
            .find(|(selector, _, _)| selector == label)
            .map(|(_, resident, _)| resident.len())
            .unwrap()
    };
    // Controls: the fixture really exercises every shape.
    assert!(count("delivery_") > 8 * 2, "{answers:?}");
    assert!(count("@authority") > 0);
    assert!(count("rows") > 8, "rows reach submissions and objects");
    assert!(count("table:records") > 8);
    assert_eq!(
        count("delivery_root.v1."),
        6,
        "two of eight roots retired their ownership"
    );
    assert_eq!(count("pending"), 3, "units 4, 5 and 7 remain pending");

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}
