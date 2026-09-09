//! Explicit unit membership and every committed application value cross sync together.

use contextdb_core::{TenantId, Value};
use contextdb_engine::database::DeliveryManifest;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{Database, DeliveryOutcomeKind};
use contextdb_server::protocol::{
    MessageType, PushRequest, PushResponse, WireDeliveryManifest, decode, encode,
};
use contextdb_server::subjects::push_subject;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

/// The root table and its one declared member table. Neutral engine vocabulary:
/// a root row and the member rows that belong with it, nothing about what a
/// consumer stores in them.
const ROOT_TABLE: &str = "records";
const MEMBER_TABLE: &str = "record_parts";

const ROOT_DDL: &str = "CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE DELIVERY MANIFEST OVER record_parts";
const MEMBER_DDL: &str = "CREATE TABLE record_parts \
     (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
/// A second root table, declared with no `DELIVERY MANIFEST` clause, used to
/// prove a manifest cannot be registered over an undeclared root.
const UNDECLARED_ROOT_DDL: &str = "CREATE TABLE records_undeclared (id UUID PRIMARY KEY, body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
/// A second member table the root's declaration does NOT name.
const UNNAMED_MEMBER_DDL: &str = "CREATE TABLE record_marks \
     (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";

const ROOT_CLAUSES: &str =
    "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE DELIVERY MANIFEST OVER record_parts";
const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const BINDING_MISMATCH_ROOT_CLAUSES: &str =
    "SYNC PUSH ONLY SYNC CONFLICT KEEP LATEST IMMUTABLE DELIVERY MANIFEST OVER record_parts";

const IROH_FRAME_CEILING_BYTES: usize = 64 * 1024 * 1024;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// Bounded so a red state can never hang the suite. This is a deadline on a
/// transport exchange, never a sleep and never a timing assertion.
async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("bounded authenticated sync exchange")
}

fn declare_edge_tables(db: &Database) {
    db.execute(ROOT_DDL, &p()).expect("declare root table");
    db.execute(MEMBER_DDL, &p()).expect("declare member table");
}

fn declare_hub_tables(db: &Database) {
    declare_edge_tables(db);
}

fn expectation() -> ApplicationTablePolicyExpectation {
    ApplicationTablePolicyExpectation::new()
        .expect_table(ROOT_TABLE, ROOT_CLAUSES)
        .expect("the root expectation parses as declaration clause text")
        .expect_table(MEMBER_TABLE, MEMBER_CLAUSES)
        .expect("the member expectation parses as declaration clause text")
}

struct RunningHub {
    db: Arc<Database>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

async fn start_hub(broker: &InProcessBroker, tenant: &str, db: Arc<Database>) -> RunningHub {
    start_hub_with_expectation(broker, tenant, db, expectation()).await
}

async fn start_hub_with_expectation(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
    policy: ApplicationTablePolicyExpectation,
) -> RunningHub {
    db.__seed_tenant_table_policies_for_test(TenantId::from(tenant), policy)
        .expect("install durable declaration prerequisites for manifest tests");
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db.clone(),
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
    within(broker.wait_for_registered_route_for_test(&push_subject(tenant))).await;
    RunningHub { db, shutdown, task }
}

fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, tenant: &str) -> (SyncClient, String) {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&node_id),
        TenantId::from(tenant),
        identity,
    );
    (client, node_id)
}

fn insert_root(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO records (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert root row");
}

fn root_key(id: Uuid) -> NaturalKey {
    NaturalKey::single("id".to_string(), Value::Uuid(id))
}

fn seed_committed_manifest(
    client: &SyncClient,
    tx: contextdb_core::TxId,
    root: Uuid,
    members: &[Uuid],
) {
    client
        .__stage_delivery_manifest_for_test(
            tx,
            DeliveryManifest {
                root_table: ROOT_TABLE,
                root_key: root_key(root),
                members: members
                    .iter()
                    .map(|member| (MEMBER_TABLE, root_key(*member)))
                    .collect(),
            },
        )
        .expect("prepare a committed unit before exercising push");
}

/// Write one root row and its member rows, and register the unit's membership,
/// all inside one transaction — the only shape the contract admits.
fn commit_unit(
    db: &Database,
    root_id: Uuid,
    root_body: &str,
    members: &[(Uuid, &str)],
) -> Result<(), contextdb_core::Error> {
    let tx = db.begin()?;
    db.insert_row(
        tx,
        ROOT_TABLE,
        HashMap::from([
            ("id".to_string(), Value::Uuid(root_id)),
            ("body".to_string(), Value::Text(root_body.to_string())),
        ]),
    )?;
    let mut named = Vec::new();
    for (member_id, member_body) in members {
        db.insert_row(
            tx,
            MEMBER_TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(*member_id)),
                ("record_id".to_string(), Value::Uuid(root_id)),
                ("body".to_string(), Value::Text((*member_body).to_string())),
            ]),
        )?;
        named.push((
            MEMBER_TABLE,
            NaturalKey::single("id".to_string(), Value::Uuid(*member_id)),
        ));
    }
    db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: ROOT_TABLE,
            root_key: root_key(root_id),
            members: named,
        },
    )?;
    db.commit(tx)
}

// Commit only source rows; canonical metadata is arranged explicitly by the caller.
fn stage_fixture_rows(
    db: &Database,
    root_id: Uuid,
    root_body: &str,
    members: &[(Uuid, &str)],
) -> Result<contextdb_core::TxId, contextdb_core::Error> {
    let tx = db.begin()?;
    db.insert_row(
        tx,
        ROOT_TABLE,
        HashMap::from([
            ("id".to_string(), Value::Uuid(root_id)),
            ("body".to_string(), Value::Text(root_body.to_string())),
        ]),
    )?;
    let mut named = Vec::new();
    for (member_id, member_body) in members {
        db.insert_row(
            tx,
            MEMBER_TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(*member_id)),
                ("record_id".to_string(), Value::Uuid(root_id)),
                ("body".to_string(), Value::Text((*member_body).to_string())),
            ]),
        )?;
        named.push((
            MEMBER_TABLE,
            NaturalKey::single("id".to_string(), Value::Uuid(*member_id)),
        ));
    }
    Ok(tx)
}

/// Statements 9/10: a damaged committed unit enters the actual ordinary push receiver.
async fn replay_push(
    broker: &InProcessBroker,
    tenant: &str,
    node_id: &str,
    message_type: MessageType,
    request: &PushRequest,
) -> PushResponse {
    let bytes = encode(message_type, request).expect("encode the replayed push request");
    let transport = broker.client_as(node_id);
    let reply = within(transport.request_single_reply(
        &push_subject(tenant),
        bytes,
        Duration::from_secs(60),
    ))
    .await
    .expect("the hub answers the push subject");
    let envelope = decode(&reply).expect("decode push reply envelope");
    assert_eq!(envelope.message_type, MessageType::PushResponse);
    rmp_serde::from_slice(&envelope.payload).expect("decode push response payload")
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT id FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"))
        .rows
        .len()
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

// Statement 7: Observe today's canonical encoding locally, without adding a public accessor
// or making its byte layout a product contract. These fixtures contain only UUID/text values.
fn canonical_string(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend((value.len() as u64).to_be_bytes());
    bytes.extend(value.as_bytes());
}

// Statement 7: Independently hash every application column read from the committed row.
fn committed_row_digest(db: &Database, table: &str, id: Uuid) -> [u8; 32] {
    let result = db
        .execute(
            &format!("SELECT * FROM {table} WHERE id=$id"),
            &HashMap::from([("id".into(), Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let mut columns: Vec<_> = result.columns.iter().zip(&result.rows[0]).collect();
    columns.sort_by_key(|(name, _)| *name);
    let mut bytes = Vec::new();
    canonical_string(&mut bytes, "row.v1");
    bytes.extend((columns.len() as u64).to_be_bytes());
    for (name, value) in columns {
        canonical_string(&mut bytes, name);
        match value {
            Value::Text(value) => {
                bytes.push(4);
                canonical_string(&mut bytes, value);
            }
            Value::Uuid(value) => {
                bytes.push(5);
                bytes.extend(value.as_bytes());
            }
            other => panic!("unexpected fixture column type: {other:?}"),
        }
    }
    *blake3::hash(&bytes).as_bytes()
}

// Statement 7: Read real root/member references and digests from the encoded wire manifest.
fn assert_manifest_row_digests(
    db: &Database,
    manifest: &WireDeliveryManifest,
    root: Uuid,
    members: &[Uuid],
) {
    let reference = |table: &str, id: Uuid| {
        let mut bytes = Vec::new();
        canonical_string(&mut bytes, table);
        bytes.extend(1_u64.to_be_bytes());
        canonical_string(&mut bytes, "id");
        bytes.push(5);
        bytes.extend(id.as_bytes());
        bytes
    };
    let root_ref = reference(ROOT_TABLE, root);
    let seal = manifest
        .seal
        .strip_prefix(root_ref.as_slice())
        .expect("the encoded manifest names the actual committed root key");
    assert_eq!(
        seal.get(..32)
            .expect("the manifest carries a root-row digest"),
        committed_row_digest(db, ROOT_TABLE, root),
        "the root-row digest is BLAKE3 over every committed application column"
    );
    let mut ordered: Vec<_> = members
        .iter()
        .map(|id| (reference(MEMBER_TABLE, *id), *id))
        .collect();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(manifest.member_count_for_test(), Some(ordered.len() as u64));
    assert_eq!(manifest.retained_slots.len(), ordered.len());
    for (slot, (member_ref, id)) in manifest.retained_slots.iter().zip(ordered) {
        let slot = slot
            .strip_prefix(member_ref.as_slice())
            .expect("member references are complete and canonically ordered");
        // Today's slot stores two tags, the materialized reference, then a digest tag.
        let row = slot
            .get(2..)
            .unwrap()
            .strip_prefix(member_ref.as_slice())
            .expect("the digest belongs to this member's actual row reference");
        assert_eq!(
            row.get(1..33)
                .expect("each member carries a complete digest"),
            committed_row_digest(db, MEMBER_TABLE, id),
            "each member digest is BLAKE3 over every committed application column"
        );
    }
}

// Statement 7: Rows and explicit membership commit atomically with engine-computed whole-row digests.
#[tokio::test]
async fn a_manifest_commits_with_its_rows_carries_engine_digests_and_no_column_value_and_an_empty_member_set_has_its_own_digest()
 {
    let tenant = "manifest-commits-with-its-rows";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    declare_hub_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db).await;

    let edge_db = Arc::new(Database::open_memory());
    let (edge, _edge_node_id) = edge_client(&edge_db, &broker, tenant);
    within(edge.__seed_application_table_policy_binding_for_test(expectation()))
        .await
        .expect("the edge binds the hub's declaration before it writes a unit");
    declare_edge_tables(&edge_db);

    // A unit whose membership is two rows, committed with its rows.
    let root_id = Uuid::from_u128(0x1111_0000_0000_0000_0000_0000_0000_0001);
    let member_a = Uuid::from_u128(0x1111_0000_0000_0000_0000_0000_0000_0002);
    let member_b = Uuid::from_u128(0x1111_0000_0000_0000_0000_0000_0000_0003);
    commit_unit(
        &edge_db,
        root_id,
        "root-body",
        &[(member_a, "member-alpha"), (member_b, "member-beta")],
    )
    .expect("a manifest registered over rows the same transaction writes commits with them");

    let status = edge_db
        .delivery_status(ROOT_TABLE)
        .expect("read the local delivery status");
    assert_eq!(
        status.eligible, 1,
        "the committed unit is one manifested root this table sends to the hub"
    );
    assert_eq!(
        status.pending, 1,
        "with no outcome from the hub yet, the unit is pending"
    );
    assert_eq!(
        (status.accepted, status.equivalent, status.refused),
        (0, 0, 0),
        "a local commit is not a hub verdict"
    );

    // The rows landed with the manifest, in one commit.
    assert_eq!(row_count(&edge_db, ROOT_TABLE), 1);
    assert_eq!(row_count(&edge_db, MEMBER_TABLE), 2);

    let rolled_back = Uuid::new_v4();
    let tx = stage_fixture_rows(&edge_db, rolled_back, "uncommitted-root", &[]).unwrap();
    edge_db
        .register_delivery_manifest(
            tx,
            DeliveryManifest {
                root_table: ROOT_TABLE,
                root_key: root_key(rolled_back),
                members: vec![],
            },
        )
        .unwrap();
    edge_db.rollback(tx).unwrap();
    assert_eq!(row_count(&edge_db, ROOT_TABLE), 1);
    assert_eq!(
        edge_db.delivery_status(ROOT_TABLE).unwrap().eligible,
        1,
        "rollback removes rows and their registration together"
    );

    // A second unit with an EXPLICIT empty member set.
    let empty_root = Uuid::from_u128(0x1111_0000_0000_0000_0000_0000_0000_0004);
    commit_unit(&edge_db, empty_root, "root-body", &[])
        .expect("an explicit empty member set is a complete unit");

    let manifests: Vec<WireDeliveryManifest> = [root_id, empty_root]
        .into_iter()
        .flat_map(|id| {
            edge.__delivery_push_request_for_test(ROOT_TABLE, &root_key(id))
                .unwrap()
                .changeset
                .manifests
        })
        .collect();
    assert_eq!(
        manifests.len(),
        2,
        "each manifested root travels as its own unit with its own manifest"
    );
    let two_member = manifests
        .iter()
        .find(|manifest| manifest.member_count_for_test() == Some(2))
        .expect("the two-member unit's manifest crosses the wire");
    let empty_member = manifests
        .iter()
        .find(|manifest| manifest.member_count_for_test() == Some(0))
        .expect("the empty-member unit's manifest crosses the wire");

    assert_eq!(
        two_member.member_count_for_test(),
        Some(2),
        "the manifest names every member row of the unit"
    );
    assert_eq!(
        two_member.root_table_for_test(),
        Some(ROOT_TABLE),
        "the manifest names the declared root table"
    );
    // Statement 7: Counts and a unit digest alone cannot prove the required row digests.
    assert_manifest_row_digests(&edge_db, two_member, root_id, &[member_a, member_b]);
    assert_manifest_row_digests(&edge_db, empty_member, empty_root, &[]);
    assert!(two_member.unit_digest_for_test().is_some());
    assert_eq!(
        empty_member.member_count_for_test(),
        Some(0),
        "an empty member set carries no member references"
    );
    assert!(
        empty_member.unit_digest_for_test().is_some(),
        "an empty member set has a real unit digest, not an absent one"
    );
    assert_ne!(
        empty_member.unit_digest_for_test(),
        two_member.unit_digest_for_test(),
        "an empty member set's digest is distinct from any non-empty one"
    );

    // No column value rides the manifest. The bodies are the only text values
    // these rows carry, and none of them appears anywhere in the encoded lane.
    let encoded = rmp_serde::to_vec(&manifests).expect("encode the manifest lane");
    for body in ["root-body", "member-alpha", "member-beta"] {
        assert!(
            !encoded
                .windows(body.len())
                .any(|window| window == body.as_bytes()),
            "a manifest carries references and digests, never the column value {body}"
        );
    }

    // Machine independence: a second, independent edge importing byte-identical
    // content produces byte-identical digests.
    let other_edge_db = Arc::new(Database::open_memory());
    let (other_edge, _other_node_id) = edge_client(&other_edge_db, &broker, tenant);
    within(other_edge.__seed_application_table_policy_binding_for_test(expectation()))
        .await
        .expect("the second edge binds the same declaration");
    declare_edge_tables(&other_edge_db);
    commit_unit(
        &other_edge_db,
        root_id,
        "root-body",
        &[(member_b, "member-beta"), (member_a, "member-alpha")],
    )
    .expect("the same content, registered in the opposite member order");
    commit_unit(&other_edge_db, empty_root, "root-body", &[]).expect("the same memberless content");
    let other_manifests: Vec<WireDeliveryManifest> = [root_id, empty_root]
        .into_iter()
        .flat_map(|id| {
            other_edge
                .__delivery_push_request_for_test(ROOT_TABLE, &root_key(id))
                .unwrap()
                .changeset
                .manifests
        })
        .collect();
    let other_two_member = other_manifests
        .iter()
        .find(|manifest| manifest.member_count_for_test() == Some(2))
        .expect("the second edge's two-member manifest crosses the wire");
    let other_empty_member = other_manifests
        .iter()
        .find(|manifest| manifest.member_count_for_test() == Some(0))
        .expect("the second edge's memberless manifest crosses the wire");
    // Statement 7: Reversing registration order preserves actual ordered references and row digests.
    assert_manifest_row_digests(
        &other_edge_db,
        other_two_member,
        root_id,
        &[member_a, member_b],
    );
    assert_manifest_row_digests(&other_edge_db, other_empty_member, empty_root, &[]);
    assert_eq!(
        other_two_member.unit_digest_for_test(),
        two_member.unit_digest_for_test(),
        "two edges importing identical content produce the identical unit digest, \
         whatever order the writer named the members in"
    );
    assert_eq!(
        other_empty_member.unit_digest_for_test(),
        empty_member.unit_digest_for_test(),
        "an explicit empty member set's digest is stable across machines"
    );

    // Hold every root column and key fixed: only explicit membership differs.
    let empty_db = Arc::new(Database::open_memory());
    let (empty_edge, _) = edge_client(&empty_db, &broker, tenant);
    within(empty_edge.__seed_application_table_policy_binding_for_test(expectation()))
        .await
        .unwrap();
    declare_edge_tables(&empty_db);
    commit_unit(&empty_db, root_id, "root-body", &[]).unwrap();
    let same_root_empty = empty_edge
        .__delivery_push_request_for_test(ROOT_TABLE, &root_key(root_id))
        .unwrap();
    assert_eq!(
        same_root_empty.changeset.manifests[0].member_count_for_test(),
        Some(0)
    );
    assert_ne!(
        same_root_empty.changeset.manifests[0].unit_digest_for_test(),
        two_member.unit_digest_for_test(),
        "an explicit empty membership differs from a nonempty one for the exact same root row"
    );
    drop(empty_edge);
    drop(edge);
    drop(other_edge);
    hub.stop().await;
}

// Statement 8: Illegal registration aborts the entire writing transaction.
#[tokio::test]
async fn registering_a_manifest_for_an_undeclared_root_an_unnamed_member_table_an_absent_member_or_a_wrong_foreign_key_aborts_the_transaction()
 {
    let tenant = "manifest-registration-refusals";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    declare_hub_tables(&hub_db);
    hub_db
        .execute(UNDECLARED_ROOT_DDL, &p())
        .expect("declare the undeclared root table");
    hub_db
        .execute(UNNAMED_MEMBER_DDL, &p())
        .expect("declare the unnamed member table");
    let hub = start_hub(&broker, tenant, hub_db).await;

    let edge_db = Arc::new(Database::open_memory());
    edge_db
        .execute(UNDECLARED_ROOT_DDL, &p())
        .expect("edge undeclared root table");
    let (edge, _edge_node_id) = edge_client(&edge_db, &broker, tenant);
    within(edge.__seed_application_table_policy_binding_for_test(expectation()))
        .await
        .expect("the edge binds before it writes");
    declare_edge_tables(&edge_db);
    edge_db
        .execute(UNNAMED_MEMBER_DDL, &p())
        .expect("edge unnamed member table");

    // 1. A root table whose declaration carries no DELIVERY MANIFEST clause.
    let undeclared_root = Uuid::from_u128(0x2222_0000_0000_0000_0000_0000_0000_0001);
    let tx = edge_db.begin().expect("open a transaction");
    edge_db
        .insert_row(
            tx,
            "records_undeclared",
            HashMap::from([
                ("id".to_string(), Value::Uuid(undeclared_root)),
                ("body".to_string(), Value::Text("body".to_string())),
            ]),
        )
        .expect("stage the undeclared root row");
    let registered = edge_db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: "records_undeclared",
            root_key: NaturalKey::single("id".to_string(), Value::Uuid(undeclared_root)),
            members: Vec::new(),
        },
    );
    assert!(
        matches!(
            registered,
            Err(contextdb_core::Error::ManifestRequired { .. }
                | contextdb_core::Error::ManifestIncomplete { .. }
                | contextdb_core::Error::ManifestMemberOutsideTransaction { .. }
                | contextdb_core::Error::SchemaInvalid { .. })
        ),
        "a root table with no delivery-manifest declaration cannot carry a manifest"
    );
    assert!(
        edge_db.commit(tx).is_err(),
        "catching a registration refusal cannot make the poisoned transaction committable"
    );
    assert_eq!(
        row_count(&edge_db, "records_undeclared"),
        0,
        "the refusal aborts the transaction, so its row is not committed"
    );

    // 2. A member table the root's declaration does not name.
    let root_id = Uuid::from_u128(0x2222_0000_0000_0000_0000_0000_0000_0002);
    let unnamed_member = Uuid::from_u128(0x2222_0000_0000_0000_0000_0000_0000_0003);
    let tx = edge_db.begin().expect("open a transaction");
    edge_db
        .insert_row(
            tx,
            ROOT_TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(root_id)),
                ("body".to_string(), Value::Text("body".to_string())),
            ]),
        )
        .expect("stage the root row");
    edge_db
        .insert_row(
            tx,
            "record_marks",
            HashMap::from([
                ("id".to_string(), Value::Uuid(unnamed_member)),
                ("record_id".to_string(), Value::Uuid(root_id)),
                ("body".to_string(), Value::Text("body".to_string())),
            ]),
        )
        .expect("stage the unnamed member row");
    let registered = edge_db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: ROOT_TABLE,
            root_key: root_key(root_id),
            members: vec![(
                "record_marks",
                NaturalKey::single("id".to_string(), Value::Uuid(unnamed_member)),
            )],
        },
    );
    assert!(
        matches!(
            registered,
            Err(contextdb_core::Error::ManifestRequired { .. }
                | contextdb_core::Error::ManifestIncomplete { .. }
                | contextdb_core::Error::ManifestMemberOutsideTransaction { .. }
                | contextdb_core::Error::SchemaInvalid { .. })
        ),
        "a member table the declaration does not name cannot be a member"
    );
    assert!(
        edge_db.commit(tx).is_err(),
        "catching a registration refusal cannot make the poisoned transaction committable"
    );
    assert_eq!(row_count(&edge_db, ROOT_TABLE), 0);
    assert_eq!(row_count(&edge_db, "record_marks"), 0);

    // 3. A member the transaction does not contain.
    let tx = edge_db.begin().expect("open a transaction");
    edge_db
        .insert_row(
            tx,
            ROOT_TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(root_id)),
                ("body".to_string(), Value::Text("body".to_string())),
            ]),
        )
        .expect("stage the root row");
    let absent_member = Uuid::from_u128(0x2222_0000_0000_0000_0000_0000_0000_0004);
    let registered = edge_db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: ROOT_TABLE,
            root_key: root_key(root_id),
            members: vec![(
                MEMBER_TABLE,
                NaturalKey::single("id".to_string(), Value::Uuid(absent_member)),
            )],
        },
    );
    assert!(
        matches!(
            registered,
            Err(contextdb_core::Error::ManifestRequired { .. }
                | contextdb_core::Error::ManifestIncomplete { .. }
                | contextdb_core::Error::ManifestMemberOutsideTransaction { .. }
                | contextdb_core::Error::SchemaInvalid { .. })
        ),
        "a member row absent from the writing transaction cannot be named"
    );
    assert!(
        edge_db.commit(tx).is_err(),
        "catching a registration refusal cannot make the poisoned transaction committable"
    );
    assert_eq!(row_count(&edge_db, ROOT_TABLE), 0);

    // 4. A member whose declared foreign key points at a DIFFERENT root.
    let other_root = Uuid::from_u128(0x2222_0000_0000_0000_0000_0000_0000_0005);
    insert_root(&edge_db, other_root, "the other root");
    let wrong_member = Uuid::from_u128(0x2222_0000_0000_0000_0000_0000_0000_0006);
    let tx = edge_db.begin().expect("open a transaction");
    edge_db
        .insert_row(
            tx,
            ROOT_TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(root_id)),
                ("body".to_string(), Value::Text("body".to_string())),
            ]),
        )
        .expect("stage the root row");
    edge_db
        .insert_row(
            tx,
            MEMBER_TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(wrong_member)),
                // Points at the other root, not the one being registered.
                ("record_id".to_string(), Value::Uuid(other_root)),
                ("body".to_string(), Value::Text("body".to_string())),
            ]),
        )
        .expect("stage the mis-pointed member row");
    let registered = edge_db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: ROOT_TABLE,
            root_key: root_key(root_id),
            members: vec![(
                MEMBER_TABLE,
                NaturalKey::single("id".to_string(), Value::Uuid(wrong_member)),
            )],
        },
    );
    assert!(
        matches!(
            registered,
            Err(contextdb_core::Error::ManifestRequired { .. }
                | contextdb_core::Error::ManifestIncomplete { .. }
                | contextdb_core::Error::ManifestMemberOutsideTransaction { .. }
                | contextdb_core::Error::SchemaInvalid { .. })
        ),
        "a member whose foreign key targets a different root is not this unit's member"
    );
    assert!(
        edge_db.commit(tx).is_err(),
        "catching a registration refusal cannot make the poisoned transaction committable"
    );
    assert_eq!(
        row_count(&edge_db, ROOT_TABLE),
        1,
        "only the earlier committed other root survives all four refusals"
    );
    assert_eq!(row_count(&edge_db, MEMBER_TABLE), 0);
    assert_eq!(
        edge_db
            .delivery_status(ROOT_TABLE)
            .expect("read status after four refusals")
            .eligible,
        0,
        "no refused registration left a manifest behind"
    );

    drop(edge);
    hub.stop().await;
}

// Statement 9: An exact manifested unit uses ordinary or oversized push and never leaks to pulling peers.
#[tokio::test]
async fn a_manifested_unit_travels_on_the_ordinary_push_and_the_oversized_staging_path_and_is_never_served_to_a_pulling_peer()
 {
    // --- The ordinary authenticated push. ---
    let tenant = "manifested-unit-travels";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    declare_hub_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db).await;

    let edge_db = Arc::new(Database::open_memory());
    let (edge, _edge_node_id) = edge_client(&edge_db, &broker, tenant);
    within(edge.__seed_application_table_policy_binding_for_test(expectation()))
        .await
        .expect("the edge binds before it writes");
    declare_edge_tables(&edge_db);
    // Statements 9/14: a manifested unit and ordinary accepted/conflicting rows
    // actually share one source transaction and one authenticated push envelope.
    for db in [&edge_db, &hub.db] {
        db.execute("CREATE TABLE ordinary_siblings (id UUID PRIMARY KEY, body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST", &p()).unwrap();
    }
    // An unlisted FK child travels ordinarily; it cannot enlarge the signed unit.
    for db in [&edge_db, &hub.db] {
        db.execute(UNNAMED_MEMBER_DDL, &p()).unwrap();
    }
    let ordinary_child = Uuid::new_v4();
    let ordinary = Uuid::new_v4();
    let collision = Uuid::new_v4();
    hub.db
        .execute(
            "INSERT INTO ordinary_siblings VALUES ($id, 'hub winner')",
            &HashMap::from([("id".into(), Value::Uuid(collision))]),
        )
        .unwrap();
    let root_id = Uuid::from_u128(0x3333_0000_0000_0000_0000_0000_0000_0001);
    let member_a = Uuid::from_u128(0x3333_0000_0000_0000_0000_0000_0000_0002);
    let member_b = Uuid::from_u128(0x3333_0000_0000_0000_0000_0000_0000_0003);
    let tx = stage_fixture_rows(
        &edge_db,
        root_id,
        "root-body",
        &[(member_a, "member-alpha"), (member_b, "member-beta")],
    )
    .expect("commit the unit");
    for id in [ordinary, collision] {
        edge_db
            .insert_row(
                tx,
                "ordinary_siblings",
                HashMap::from([
                    ("id".into(), Value::Uuid(id)),
                    ("body".into(), Value::Text("edge sibling".into())),
                ]),
            )
            .unwrap();
    }
    edge_db
        .insert_row(
            tx,
            "record_marks",
            HashMap::from([
                ("id".into(), Value::Uuid(ordinary_child)),
                ("record_id".into(), Value::Uuid(root_id)),
                (
                    "body".into(),
                    Value::Text("outside the signed membership".into()),
                ),
            ]),
        )
        .unwrap();
    seed_committed_manifest(&edge, tx, root_id, &[member_a, member_b]);
    edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    let source_frontier = edge_db.current_lsn();
    let pushed = within(edge.push())
        .await
        .expect("the ordinary push succeeds");
    assert_eq!(
        (
            pushed.applied_rows,
            pushed.skipped_rows,
            pushed.conflicts.len()
        ),
        (5, 1, 1)
    );
    assert_eq!(
        pushed.conflicts[0].table.as_deref(),
        Some("ordinary_siblings")
    );
    assert!(pushed.conflicts[0].winning_author_node_id.is_some());
    assert!(pushed.conflicts[0].hub_acceptance_position.is_some());
    assert_eq!(
        hub.db
            .execute(
                "SELECT body FROM ordinary_siblings WHERE id = $id",
                &HashMap::from([("id".into(), Value::Uuid(ordinary))])
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("edge sibling".into())]]
    );
    assert_eq!(
        hub.db
            .execute(
                "SELECT body FROM ordinary_siblings WHERE id = $id",
                &HashMap::from([("id".into(), Value::Uuid(collision))])
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("hub winner".into())]]
    );
    assert_eq!(row_count(&hub.db, "record_marks"), 1);
    assert_eq!(
        hub.db
            .execute(
                "SELECT body FROM record_marks WHERE id=$id",
                &HashMap::from([("id".into(), Value::Uuid(ordinary_child))])
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("outside the signed membership".into())]]
    );
    assert!(edge.push_watermark() >= source_frontier);
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &TenantId::from(tenant),
                &_edge_node_id,
                edge_db.sync_incarnation(&TenantId::from(tenant)).unwrap()
            )
            .unwrap(),
        Some(edge.push_watermark())
    );
    assert_eq!(edge.pending_push_change_count().unwrap(), 0);
    let resent = within(edge.push()).await.unwrap();
    assert_eq!(
        (
            resent.applied_rows,
            resent.skipped_rows,
            resent.conflicts.len()
        ),
        (0, 0, 0)
    );
    let accepted = edge_db
        .delivery_outcome(ROOT_TABLE, &root_key(root_id))
        .unwrap()
        .expect("ordinary push must deliver the manifest and durable outcome with its unit");
    assert_eq!(
        accepted.kind(),
        DeliveryOutcomeKind::Accepted,
        "a real push delivers the manifest needed for the hub to verify and accept the unit"
    );
    assert_eq!(row_count(&hub.db, ROOT_TABLE), 1);
    assert_eq!(row_count(&hub.db, MEMBER_TABLE), 2);
    for table in [ROOT_TABLE, MEMBER_TABLE] {
        assert_eq!(
            hub.db
                .execute(&format!("SELECT * FROM {table} ORDER BY id"), &p())
                .unwrap()
                .rows,
            edge_db
                .execute(&format!("SELECT * FROM {table} ORDER BY id"), &p())
                .unwrap()
                .rows
        );
    }

    // Statement 9 ordinary neighbour: keep-first refusal remains component-complete
    // while a same-transaction manifested unit and an unrelated sibling progress.
    for db in [&edge_db, &hub.db] {
        db.execute("CREATE TABLE ordinary_parents (id UUID PRIMARY KEY, body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",&p()).unwrap();
        db.execute("CREATE TABLE ordinary_children (id UUID PRIMARY KEY, parent_id UUID REFERENCES ordinary_parents(id), body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",&p()).unwrap();
    }
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    let sibling = Uuid::new_v4();
    let other_root = Uuid::new_v4();
    hub.db
        .execute(
            "INSERT INTO ordinary_parents VALUES ($id,'winner')",
            &HashMap::from([("id".into(), Value::Uuid(parent))]),
        )
        .unwrap();
    let tx = stage_fixture_rows(&edge_db, other_root, "another manifested unit", &[]).unwrap();
    edge_db
        .insert_row(
            tx,
            "ordinary_parents",
            HashMap::from([
                ("id".into(), Value::Uuid(parent)),
                ("body".into(), Value::Text("loser".into())),
            ]),
        )
        .unwrap();
    edge_db
        .insert_row(
            tx,
            "ordinary_children",
            HashMap::from([
                ("id".into(), Value::Uuid(child)),
                ("parent_id".into(), Value::Uuid(parent)),
                ("body".into(), Value::Text("must not be stranded".into())),
            ]),
        )
        .unwrap();
    edge_db
        .insert_row(
            tx,
            "ordinary_siblings",
            HashMap::from([
                ("id".into(), Value::Uuid(sibling)),
                ("body".into(), Value::Text("independent".into())),
            ]),
        )
        .unwrap();
    seed_committed_manifest(&edge, tx, other_root, &[]);
    edge_db.commit(tx).unwrap();
    let expected_frontier = edge_db.current_lsn();
    let mixed = within(edge.push()).await.unwrap();
    assert_eq!(
        (
            mixed.applied_rows,
            mixed.skipped_rows,
            mixed.conflicts.len()
        ),
        (2, 2, 2)
    );
    assert_eq!(row_count(&hub.db, "ordinary_children"), 0);
    assert_eq!(
        hub.db
            .execute("SELECT body FROM ordinary_parents", &p())
            .unwrap()
            .rows,
        vec![vec![Value::Text("winner".into())]]
    );
    assert_eq!(
        hub.db
            .execute(
                "SELECT body FROM ordinary_siblings WHERE id=$id",
                &HashMap::from([("id".into(), Value::Uuid(sibling))])
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("independent".into())]]
    );
    assert_eq!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(other_root))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted
    );
    assert!(edge.push_watermark() >= expected_frontier);
    assert_eq!(edge.pending_push_change_count().unwrap(), 0);
    assert_eq!(within(edge.push()).await.unwrap().applied_rows, 0);

    // --- Never served to a pulling peer on a push-only table. ---
    let peer_db = Arc::new(Database::open_memory());
    declare_edge_tables(&peer_db);
    let (peer, _peer_node_id) = edge_client(&peer_db, &broker, tenant);
    within(peer.pull_default())
        .await
        .expect("a peer may pull the tenant's other traffic");
    assert_eq!(
        row_count(&peer_db, ROOT_TABLE),
        0,
        "a push-only root never reaches a pulling peer, so neither does its unit"
    );
    assert_eq!(
        peer_db
            .delivery_status(ROOT_TABLE)
            .expect("read the pulling peer's status")
            .eligible,
        0,
        "a pulling peer is handed no manifest, so it counts no eligible unit"
    );
    drop(peer);
    drop(edge);
    hub.stop().await;
    oversized_manifested_unit().await;
}

async fn oversized_manifested_unit() {
    // --- The oversized staging path, over the production authenticated Iroh
    // transport, exactly as the existing oversized-unit proof drives it. ---
    let root = tempfile::tempdir().expect("tempdir");
    let iroh_tenant = "manifested-unit-oversized";
    let hub_identity_path = root.path().join("hub.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&hub_identity_path).expect("persist the hub identity");
    let endpoint = IrohServer::bind(&bind_spec(&hub_identity_path))
        .await
        .expect("bind the authenticated hub endpoint");
    let ticket = endpoint.ticket();
    let iroh_hub_db = Arc::new(Database::open(root.path().join("hub.db")).expect("open hub store"));
    declare_hub_tables(&iroh_hub_db);
    iroh_hub_db
        .__seed_tenant_table_policies_for_test(TenantId::from(iroh_tenant), expectation())
        .expect("canonical hub declarations");
    let server = Arc::new(SyncServer::new(
        iroh_hub_db.clone(),
        &endpoint,
        TenantId::from(iroh_tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });

    let edge_identity_path = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&edge_identity_path).expect("persist the edge identity");
    let dial_spec = peer_dial_spec(&ticket, &edge_identity_path);
    let iroh_edge_db =
        Arc::new(Database::open(root.path().join("edge.db")).expect("open edge store"));
    let iroh_edge = SyncClient::new(
        iroh_edge_db.clone(),
        &dial_spec,
        TenantId::from(iroh_tenant),
    );
    within(iroh_edge.__seed_application_table_policy_binding_for_test(expectation()))
        .await
        .expect("the edge binds over the authenticated transport");
    declare_edge_tables(&iroh_edge_db);
    let big_root = Uuid::from_u128(0x3333_0000_0000_0000_0000_0000_0000_0011);
    let big_member = Uuid::from_u128(0x3333_0000_0000_0000_0000_0000_0000_0012);
    let oversized_body = "x".repeat(IROH_FRAME_CEILING_BYTES);
    let tx = stage_fixture_rows(
        &iroh_edge_db,
        big_root,
        "root-body",
        &[(big_member, oversized_body.as_str())],
    )
    .expect("commit a unit larger than one frame");
    seed_committed_manifest(&iroh_edge, tx, big_root, &[big_member]);
    iroh_edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    within(iroh_edge.push())
        .await
        .expect("a manifested unit larger than one frame rides the staging path whole");

    assert_eq!(
        row_count(&iroh_hub_db, ROOT_TABLE),
        1,
        "the oversized unit's root lands at the hub"
    );
    assert_eq!(
        row_count(&iroh_hub_db, MEMBER_TABLE),
        1,
        "the oversized unit's member lands with it"
    );
    for table in [ROOT_TABLE, MEMBER_TABLE] {
        assert_eq!(
            iroh_hub_db
                .execute(&format!("SELECT * FROM {table} ORDER BY id"), &p())
                .unwrap()
                .rows,
            iroh_edge_db
                .execute(&format!("SELECT * FROM {table} ORDER BY id"), &p())
                .unwrap()
                .rows,
            "oversized push preserves every actual row value"
        );
    }
    let outcome = iroh_edge_db
        .delivery_outcome(ROOT_TABLE, &root_key(big_root))
        .expect("read the oversized unit's durable outcome")
        .expect("the oversized unit has a durable outcome");
    assert_eq!(
        outcome.kind(),
        DeliveryOutcomeKind::Accepted,
        "the staging path delivers the manifest with the unit, so the hub can accept it"
    );
    assert_eq!(
        iroh_edge_db
            .delivery_status(ROOT_TABLE)
            .expect("read the edge's status")
            .pending,
        0,
        "nothing of the oversized unit is left pending"
    );

    drop(iroh_edge);
    stop.store(true, Ordering::SeqCst);
    let _ = task.await;
}

async fn receiver_refusal(case: &str) {
    let tenant = "unit-verification";
    let broker = InProcessBroker::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("hub.db");
    let hub_db = Arc::new(Database::open(&path).unwrap());
    declare_hub_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db.clone()).await;
    let edge_db = Arc::new(Database::open_memory());
    let (edge, node) = edge_client(&edge_db, &broker, tenant);
    let source_policy = if case == "binding_mismatch" {
        ApplicationTablePolicyExpectation::new()
            .expect_table(ROOT_TABLE, BINDING_MISMATCH_ROOT_CLAUSES)
            .unwrap()
    } else {
        expectation()
    };
    within(edge.__seed_application_table_policy_binding_for_test(source_policy))
        .await
        .unwrap();
    // Statement 10: bind the edge's actual root declaration to a policy that
    // differs from the hub's declaration before installing its manifested unit.
    // The existing signed binding fixture makes this an authenticated source
    // prerequisite, not an omitted member binding.
    if case == "binding_mismatch" {
        edge_db
            .execute(
                &format!(
                    "CREATE TABLE {ROOT_TABLE} (id UUID PRIMARY KEY, body TEXT) {BINDING_MISMATCH_ROOT_CLAUSES}"
                ),
                &p(),
            )
            .expect("the source installs the policy it bound before the unit");
        edge_db
            .execute(MEMBER_DDL, &p())
            .expect("the source installs the bound member table before the unit");
    } else {
        declare_edge_tables(&edge_db);
    }
    for db in [&edge_db, &hub_db] {
        db.execute(
            "CREATE TABLE ordinary_sibling (id UUID PRIMARY KEY, body TEXT) SYNC PUSH ONLY",
            &p(),
        )
        .unwrap();
    }
    let ordinary = Uuid::new_v4();
    let root = Uuid::new_v4();
    let members = [Uuid::new_v4(), Uuid::new_v4()];
    let tx = stage_fixture_rows(
        &edge_db,
        root,
        "root-body",
        &[(members[0], "member-alpha"), (members[1], "member-beta")],
    )
    .unwrap();
    edge_db
        .insert_row(
            tx,
            "ordinary_sibling",
            HashMap::from([
                ("id".into(), Value::Uuid(ordinary)),
                ("body".into(), Value::Text("ordinary progresses".into())),
            ]),
        )
        .unwrap();
    seed_committed_manifest(&edge, tx, root, &members);
    edge_db.commit(tx).unwrap();
    let mut request = edge
        .__delivery_push_request_for_test(ROOT_TABLE, &root_key(root))
        .unwrap();
    assert_eq!(
        request.changeset.rows.len(),
        3,
        "the original unit contains its real rows"
    );
    match case {
        "manifest_incomplete" => request
            .changeset
            .rows
            .retain(|r| r.natural_key.value != Value::Uuid(members[0])),
        "member_digest_mismatch" => {
            let member = request
                .changeset
                .rows
                .iter_mut()
                .find(|r| r.natural_key.value == Value::Uuid(members[0]))
                .unwrap();
            assert_eq!(
                member
                    .values
                    .insert("body".into(), Value::Text("changed-in-transit".into())),
                Some(Value::Text("member-alpha".into()))
            );
        }
        "manifest_required" => request.changeset.manifests.clear(),
        "binding_mismatch" => {}
        _ => unreachable!(),
    }
    // Statement 10 adapter: add the same-transaction ordinary row and its real
    // authenticated lineage to the deliberately incomplete unit request.
    let all = edge.__ordinary_push_request_for_test().unwrap();
    request.changeset.rows.extend(
        all.changeset
            .rows
            .into_iter()
            .filter(|r| r.table == "ordinary_sibling"),
    );
    let response = replay_push(&broker, tenant, &node, MessageType::PushRequest, &request).await;
    assert_eq!(
        row_count(&hub_db, ROOT_TABLE),
        0,
        "{case} cannot commit a root prefix"
    );
    assert_eq!(
        row_count(&hub_db, MEMBER_TABLE),
        0,
        "{case} cannot commit a member prefix"
    );
    // Statement 10: the ordinary response adjudicates the unit, including semantic refusals.
    assert!(
        response.result.is_some(),
        "the public receiver must adjudicate {case}, got {:?}",
        response.error
    );
    assert_eq!(
        row_count(&hub_db, "ordinary_sibling"),
        1,
        "{case}: ordinary sibling is independently applied"
    );
    assert_eq!(response.result.as_ref().unwrap().applied_rows, 1);
    assert!(
        hub_db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &TenantId::from(tenant),
                &node,
                request.incarnation
            )
            .unwrap()
            .is_some()
    );
    let replayed = replay_push(&broker, tenant, &node, MessageType::PushRequest, &request).await;
    assert_eq!(replayed.result.as_ref().unwrap().applied_rows, 0);
    assert_eq!(row_count(&hub_db, "ordinary_sibling"), 1);
    let outcomes = response.outcomes;
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].outcome_for_test(), Some("refused"));
    assert_eq!(outcomes[0].cause_for_test(), Some(case));
    within(edge.shutdown()).await;
    hub.stop().await;
    drop(hub_db);
    let reopened = Database::open(path).unwrap();
    assert_eq!(row_count(&reopened, ROOT_TABLE), 0);
    assert_eq!(row_count(&reopened, MEMBER_TABLE), 0);
}

// Statement 10: The hub verifies complete membership, every row digest, and binding before any apply.
#[tokio::test]
async fn the_hub_verifies_complete_membership_content_and_binding_before_applying_any_row() {
    for cause in [
        "manifest_incomplete",
        "member_digest_mismatch",
        "manifest_required",
        "binding_mismatch",
    ] {
        receiver_refusal(cause).await;
    }
}
