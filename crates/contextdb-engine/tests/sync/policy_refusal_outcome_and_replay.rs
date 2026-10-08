//! A policy refusal is a durable authored-unit outcome, while dependent data
//! remains owed until the author supplies a compatible correction.

use contextdb_core::{TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const T0: u64 = 1_700_000_000_000;
const TENANT: &str = "policy-refusal-outcome-replay";
const FIRST: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
    SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("the authenticated exchange completes")
}

struct Hub {
    db: Arc<Database>,
    endpoint: Option<IrohServer>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Hub {
    async fn stop(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.endpoint
            .take()
            .expect("live hub endpoint")
            .close()
            .await;
        within(self.task).await.expect("hub stops");
    }
}

async fn start_hub(root: &Path) -> Hub {
    let identity = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&format!("iroh:?identity={}", identity.display()))
        .await
        .expect("bind hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub"));
    let server = Arc::new(
        SyncServer::new(db.clone(), &endpoint, TenantId::from(TENANT))
            .expect("read or construct the declared fixture result"),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        endpoint: Some(endpoint),
        ticket,
        stop,
        task,
    }
}

fn make_client(edge: &Arc<Database>, ticket: &str, identity: &Path) -> SyncClient {
    SyncClient::new(
        edge.clone(),
        &peer_dial_spec(ticket, identity),
        TenantId::from(TENANT),
    )
    .expect("read or construct the declared fixture result")
}

async fn accepted_base(edge: &Arc<Database>, hub: &Hub, identity: &Path) {
    edge.execute(FIRST, &params())
        .expect("author the matching table");
    let client = make_client(edge, &hub.ticket, identity);
    let result = within(client.push())
        .await
        .expect("push matching declaration");
    assert!(
        result.conflicts.is_empty(),
        "matching declaration accepts: {result:?}"
    );
    within(client.shutdown()).await;
}

fn author_refused_alteration(edge: &Database) {
    edge.execute("ALTER TABLE notes SET SYNC CONFLICT KEEP LATEST", &params())
        .expect("author the policy the hub will refuse");
}

fn assert_one_policy_refusal(result: &contextdb_engine::sync_types::ApplyResult) {
    assert_eq!(
        result.conflicts.len(),
        1,
        "one whole unit is refused: {:?}",
        result.conflicts
    );
    let reason = result.conflicts[0]
        .reason
        .as_deref()
        .expect("typed refusal reason");
    assert!(
        reason.contains("whole authored transaction is refused"),
        "{reason}"
    );
    assert!(
        reason.contains("notes") && reason.contains("conflict"),
        "{reason}"
    );
    assert_eq!(result.applied_rows, 0);
}

#[tokio::test]
async fn refused_policy_unit_has_one_durable_author_visible_outcome_without_receipt_or_reoffer() {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().unwrap();
    let hub = start_hub(root.path()).await;
    hub.db.execute(FIRST, &params()).unwrap();
    let identity = root.path().join("edge.db.fabric-identity.key");
    let edge_path = root.path().join("edge.db");
    let node = FabricIdentity::load_or_generate(&identity)
        .unwrap()
        .node_id();
    let edge = Arc::new(Database::open(&edge_path).unwrap());
    accepted_base(&edge, &hub, &identity).await;
    author_refused_alteration(&edge);
    let receipt_before = hub
        .db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), &node)
        .unwrap();
    assert!(
        receipt_before.is_some(),
        "the matching base unit was receipted"
    );
    let progress_before = hub
        .db
        .persisted_sync_watermarks(&TenantId::from(TENANT))
        .unwrap();
    let client = make_client(&edge, &hub.ticket, &identity);
    let refused = within(client.push())
        .await
        .expect("author sees the hub refusal");
    assert_one_policy_refusal(&refused);
    let outcomes = client.refused_schema_units_for_test();
    assert_eq!(
        outcomes.len(),
        1,
        "the author receives a typed REFUSED outcome"
    );
    let outcome = outcomes[0].clone();
    assert_eq!(outcome.tenant_id, TenantId::from(TENANT));
    assert_eq!(outcome.pushing_node_id, node);
    assert_eq!(outcome.author_node_id, node);
    assert_eq!(outcome.table, "notes");
    assert!(
        outcome
            .reason
            .contains("whole authored transaction is refused")
    );
    assert_ne!(outcome.unit_digest, [0; 32]);
    assert_eq!(
        Some(client.push_watermark()),
        receipt_before,
        "a refused unit never advances the author's accepted watermark"
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), &node)
            .unwrap(),
        receipt_before,
        "REFUSED does not become an acceptance receipt"
    );
    assert_eq!(
        hub.db
            .persisted_sync_watermarks(&TenantId::from(TENANT))
            .unwrap(),
        progress_before,
        "REFUSED does not advance hub application progress"
    );
    within(client.shutdown()).await;
    drop(client);
    hub.stop().await;

    let edge = Arc::try_unwrap(edge).unwrap_or_else(|_| panic!("the author has one owner"));
    edge.close().expect("close author after refusal");
    drop(edge);
    let edge = Arc::new(Database::open(&edge_path).expect("reopen author"));

    let hub = start_hub(root.path()).await;
    let client = make_client(&edge, &hub.ticket, &identity);
    let quiet = within(client.push())
        .await
        .expect("read durable refused outcome after restart");
    assert!(
        quiet.conflicts.is_empty(),
        "the author must not re-offer its refused unit: {:?}",
        quiet.conflicts
    );
    assert_eq!(quiet.applied_rows, 0);
    assert_eq!(
        client.refused_schema_units_for_test(),
        vec![outcome.clone()],
        "authenticated status returns the same immutable typed outcome after both restarts"
    );
    let quiet_again = within(client.push())
        .await
        .expect("repeat refusal status read");
    assert!(quiet_again.conflicts.is_empty());
    assert_eq!(client.refused_schema_units_for_test(), vec![outcome]);
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), &node)
            .unwrap(),
        receipt_before
    );
    edge.execute("ALTER TABLE notes SET SYNC CONFLICT KEEP FIRST", &params())
        .expect("author a corrected unit");
    let corrected = within(client.push()).await.expect("push corrected unit");
    assert!(
        corrected.conflicts.is_empty(),
        "corrected unit accepts: {:?}",
        corrected.conflicts
    );
    assert!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), &node)
            .unwrap()
            > receipt_before,
        "only the corrected unit receives acceptance progress"
    );
    within(client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn data_held_after_policy_refusal_replays_when_a_corrected_unit_releases_the_hold() {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().unwrap();
    let hub = start_hub(root.path()).await;
    hub.db.execute(FIRST, &params()).unwrap();
    let identity = root.path().join("edge.db.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).unwrap());
    accepted_base(&edge, &hub, &identity).await;
    author_refused_alteration(&edge);
    let client = make_client(&edge, &hub.ticket, &identity);
    assert_one_policy_refusal(&within(client.push()).await.unwrap());
    let row_id = uuid::Uuid::from_u128(0xA11CE);
    edge.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(row_id)),
            (
                "body".to_string(),
                Value::Text("held until correction".into()),
            ),
        ]),
    )
    .unwrap();
    let accepted_before_held = client.push_watermark();
    let held = within(client.push()).await.expect("hold dependent row");
    assert_eq!(
        client.push_watermark(),
        accepted_before_held,
        "held data remains pending without claiming an accepted frontier"
    );
    assert_eq!(
        held.conflicts.len(),
        1,
        "one held row and no repeated refusal: {:?}",
        held.conflicts
    );
    assert!(
        held.conflicts[0]
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("held back")
    );
    assert!(
        hub.db
            .execute("SELECT body FROM notes", &params())
            .unwrap()
            .rows
            .is_empty()
    );
    within(client.shutdown()).await;
    hub.stop().await;

    let hub = start_hub(root.path()).await;
    edge.execute("ALTER TABLE notes SET SYNC CONFLICT KEEP FIRST", &params())
        .expect("correct the author's policy");
    let client = make_client(&edge, &hub.ticket, &identity);
    let corrected = within(client.push())
        .await
        .expect("correct and release hold");
    assert!(
        corrected.conflicts.is_empty(),
        "the older row's transient hold diagnostic clears only after this push accepts the later correction and replays it: {:?}",
        corrected.conflicts
    );
    assert!(
        hub.db.policy_held_received_tables().unwrap().is_empty(),
        "the hold is released"
    );
    assert_eq!(
        hub.db
            .execute("SELECT body FROM notes", &params())
            .unwrap()
            .rows,
        vec![vec![Value::Text("held until correction".into())]],
        "the earlier row remains owed after its cursor passed and must replay exactly once"
    );
    let quiet = within(client.push())
        .await
        .expect("repeat push after replay");
    assert!(quiet.conflicts.is_empty());
    assert_eq!(
        hub.db
            .execute("SELECT body FROM notes", &params())
            .unwrap()
            .rows
            .len(),
        1
    );
    within(client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn every_refused_unit_is_read_across_status_pages_before_any_reoffer() {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().unwrap();
    let hub = start_hub(root.path()).await;
    hub.db.execute(FIRST, &params()).unwrap();
    let identity = root.path().join("edge.db.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).unwrap());
    accepted_base(&edge, &hub, &identity).await;
    // Each statement has its own signed source unit. One more than a status
    // page forces the author to consume a continuation before preparing work.
    for _ in 0..129 {
        author_refused_alteration(&edge);
    }
    let client = make_client(&edge, &hub.ticket, &identity);
    let before = client.push_watermark();
    let refused = within(client.push()).await.unwrap();
    assert_eq!(refused.conflicts.len(), 129);
    assert_eq!(client.refused_schema_units_for_test().len(), 129);
    assert_eq!(client.push_watermark(), before);
    within(client.shutdown()).await;
    drop(client);
    let client = make_client(&edge, &hub.ticket, &identity);
    let quiet = within(client.push()).await.unwrap();
    assert!(
        quiet.conflicts.is_empty(),
        "all pages suppress exact refused units"
    );
    assert_eq!(client.refused_schema_units_for_test().len(), 129);
    assert_eq!(client.push_watermark(), before);

    edge.execute("ALTER TABLE notes SET SYNC CONFLICT KEEP FIRST", &params())
        .expect("author a compatible correction after the refused units");
    let corrected = within(client.push()).await.unwrap();
    assert!(
        corrected.conflicts.is_empty(),
        "the correction accepts after all earlier refusals: {corrected:?}"
    );
    assert!(
        client.push_watermark() > before,
        "the accepted correction moves the outbound frontier past the historical refusals"
    );

    author_refused_alteration(&edge);
    let current_lsn = edge.current_lsn();
    let current_refusal = within(client.push()).await.unwrap();
    assert_one_policy_refusal(&current_refusal);
    let current_outcome = client
        .refused_schema_units_for_test()
        .into_iter()
        .find(|outcome| outcome.source_ddl_lsn == current_lsn)
        .expect("the current refused unit has a typed outcome");
    within(client.shutdown()).await;
    drop(client);

    let client = make_client(&edge, &hub.ticket, &identity);
    let quiet = within(client.push()).await.unwrap();
    assert!(
        quiet.conflicts.is_empty(),
        "the current exact refusal suppresses a reoffer: {quiet:?}"
    );
    assert_eq!(quiet.applied_rows, 0);
    assert_eq!(
        client.refused_schema_units_for_test(),
        vec![current_outcome.clone()],
        "status retains only the one refused unit still eligible for this outbound push"
    );
    let quiet_again = within(client.push()).await.unwrap();
    assert!(quiet_again.conflicts.is_empty());
    assert_eq!(
        client.refused_schema_units_for_test(),
        vec![current_outcome]
    );
    within(client.shutdown()).await;

    hub.stop().await;
}

const LINKS: &str = "CREATE TABLE links (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('HELD') SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

async fn push_edge_payload(
    hub: &Hub,
    identity_path: &Path,
    author: &Database,
    changes: contextdb_engine::sync_types::ChangeSet,
    dependency_complete: bool,
) -> contextdb_engine::protocol::PushResponse {
    use contextdb_engine::protocol::{MessageType, PushRequest, WireChangeSet, decode, encode};
    let identity = FabricIdentity::load_or_generate(identity_path).unwrap();
    let tenant = TenantId::from(TENANT);
    let incarnation = author.sync_incarnation_for_test(&tenant).unwrap();
    let fixture_operation_reader_1 = author
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let provenance = author
        .outbound_ddl_provenance_for_test(
            &contextdb_engine::sync_types::ChangeSet {
                ddl: changes.ddl.clone(),
                ddl_lsn: changes.ddl_lsn.clone(),
                ..Default::default()
            },
            &tenant,
            &identity.node_id(),
            incarnation,
            &|bytes| Ok(identity.sign_for_test(bytes)),
            fixture_operation_reader_1.clone(),
        )
        .unwrap();
    let mut wire = WireChangeSet::from(changes);
    wire.ddl_provenance = provenance;
    let request = PushRequest {
        changeset: wire,
        incarnation,
    };
    let transport =
        contextdb_engine::transport::client_transport(&peer_dial_spec(&hub.ticket, identity_path));
    let bytes = encode(
        if dependency_complete {
            MessageType::DependencyCompletePushRequest
        } else {
            MessageType::PushRequest
        },
        &request,
    )
    .unwrap();
    let reply = within(transport.request(
        &contextdb_engine::subjects::push_subject(TENANT),
        bytes,
        Duration::from_secs(60),
    ))
    .await
    .expect("the real authenticated endpoint replies to the edge payload");
    within(transport.shutdown()).await.unwrap();
    let envelope = decode(&reply).unwrap();
    let response: contextdb_engine::protocol::PushResponse =
        rmp_serde::from_slice(&envelope.payload).unwrap();
    assert!(response.error.is_none(), "{response:?}");
    assert!(response.application_error.is_none(), "{response:?}");
    response
}

fn edge_change(
    lsn: contextdb_core::Lsn,
    edge_type: &str,
    target: u128,
) -> contextdb_engine::sync_types::EdgeChange {
    contextdb_engine::sync_types::EdgeChange {
        source: uuid::Uuid::from_u128(0xE000),
        target: uuid::Uuid::from_u128(target),
        edge_type: edge_type.into(),
        properties: HashMap::new(),
        lsn,
    }
}

async fn refused_schema_edges_remain_owed(dependency_complete: bool, same_unit_edge: bool) {
    use contextdb_core::Lsn;
    use contextdb_engine::sync_types::ChangeSet;
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().unwrap();
    let hub = start_hub(root.path()).await;
    hub.db.execute(LINKS, &params()).unwrap();
    let author = Database::open(root.path().join("edge.db")).unwrap();
    author.execute(LINKS, &params()).unwrap();
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let node = FabricIdentity::load_or_generate(&identity_path)
        .unwrap()
        .node_id();
    let base = author
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result");
    push_edge_payload(&hub, &identity_path, &author, base, dependency_complete).await;
    let before = author.current_lsn();
    author
        .execute("ALTER TABLE links SET SYNC CONFLICT KEEP LATEST", &params())
        .unwrap();
    let mut refused = author
        .changes_since(before)
        .expect("read or construct the declared fixture result");
    let refused_lsn = *refused.ddl_lsn.last().unwrap();
    // Received schema permits connected graph data even though local schema
    // transactions cannot mix DDL and DML. The signed schema is unchanged.
    if same_unit_edge {
        refused
            .edges
            .push(edge_change(refused_lsn, "SAME_UNIT", 0xE001));
    }
    assert!(refused.rows.is_empty() && refused.vectors.is_empty());
    let receipt_before = hub
        .db
        .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), &node)
        .unwrap();
    let progress_before = hub
        .db
        .persisted_sync_watermarks(&TenantId::from(TENANT))
        .unwrap();
    let response =
        push_edge_payload(&hub, &identity_path, &author, refused, dependency_complete).await;
    assert!(!response.result.unwrap().conflicts.is_empty());
    assert_eq!(
        hub.db
            .edge_count(
                uuid::Uuid::from_u128(0xE000),
                "SAME_UNIT",
                hub.db.snapshot()
            )
            .unwrap(),
        0,
        "whole signed-unit refusal must hold a same-unit edge even without row/vector dependencies"
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), &node)
            .unwrap(),
        receipt_before,
        "a held edge is never an acceptance receipt"
    );
    assert_eq!(
        hub.db
            .persisted_sync_watermarks(&TenantId::from(TENANT))
            .unwrap(),
        progress_before,
        "a held edge never advances application progress"
    );

    // An edge-only later page depends explicitly on links through DAG('HELD').
    let later_lsn = Lsn(refused_lsn.0 + 1);
    let later = ChangeSet {
        edges: vec![edge_change(later_lsn, "HELD", 0xE002)],
        ..ChangeSet::default()
    };
    push_edge_payload(&hub, &identity_path, &author, later, false).await;
    assert_eq!(
        hub.db
            .edge_count(uuid::Uuid::from_u128(0xE000), "HELD", hub.db.snapshot())
            .unwrap(),
        0,
        "later edge-only pages remain behind their owning table's refusal"
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node(&TenantId::from(TENANT), &node)
            .unwrap(),
        receipt_before
    );

    let sibling = ChangeSet {
        edges: vec![edge_change(Lsn(later_lsn.0 + 1), "UNRELATED", 0xE003)],
        ..ChangeSet::default()
    };
    push_edge_payload(&hub, &identity_path, &author, sibling, false).await;
    assert_eq!(
        hub.db
            .edge_count(
                uuid::Uuid::from_u128(0xE000),
                "UNRELATED",
                hub.db.snapshot()
            )
            .unwrap(),
        1,
        "an unrelated edge unit must not inherit another table's hold"
    );
    hub.stop().await;
    let hub = start_hub(root.path()).await;
    assert_eq!(
        hub.db
            .edge_count(uuid::Uuid::from_u128(0xE000), "HELD", hub.db.snapshot())
            .unwrap(),
        0
    );
    // Obtain a genuine later schema source position beyond the data pages.
    author
        .execute(
            "CREATE TABLE source_padding_one (id UUID PRIMARY KEY)",
            &params(),
        )
        .unwrap();
    author
        .execute(
            "CREATE TABLE source_padding_two (id UUID PRIMARY KEY)",
            &params(),
        )
        .unwrap();
    let before_correction = author.current_lsn();
    author
        .execute("ALTER TABLE links SET SYNC CONFLICT KEEP FIRST", &params())
        .unwrap();
    let correction = author
        .changes_since(before_correction)
        .expect("read or construct the declared fixture result");
    push_edge_payload(
        &hub,
        &identity_path,
        &author,
        correction.clone(),
        dependency_complete,
    )
    .await;
    assert!(hub.db.policy_held_received_tables().unwrap().is_empty());
    for kind in ["SAME_UNIT", "HELD", "UNRELATED"] {
        if kind == "SAME_UNIT" && !same_unit_edge {
            continue;
        }
        assert_eq!(
            hub.db
                .edge_count(uuid::Uuid::from_u128(0xE000), kind, hub.db.snapshot())
                .unwrap(),
            1,
            "{kind} must be visible exactly once after the compatible correction"
        );
    }
    let after_replay = hub.db.current_lsn();
    push_edge_payload(
        &hub,
        &identity_path,
        &author,
        correction,
        dependency_complete,
    )
    .await;
    assert_eq!(
        hub.db.current_lsn(),
        after_replay,
        "repeated correction cannot replay a held edge twice"
    );
    hub.stop().await;
}

#[tokio::test]
async fn refused_schema_holds_same_unit_and_later_edge_only_pages_until_correction() {
    refused_schema_edges_remain_owed(false, true).await;
}

#[tokio::test]
async fn dependency_complete_refusal_holds_edge_only_data_until_correction() {
    refused_schema_edges_remain_owed(true, true).await;
}

#[tokio::test]
async fn later_edge_only_page_is_held_by_its_declared_table_until_correction() {
    refused_schema_edges_remain_owed(false, false).await;
}

#[tokio::test]
async fn a_recreated_edges_new_life_does_not_inherit_or_release_its_old_policy_hold() {
    use contextdb_core::Lsn;
    use contextdb_engine::sync_types::ChangeSet;

    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().unwrap();
    let hub = start_hub(root.path()).await;
    hub.db.execute(LINKS, &params()).unwrap();
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let node = FabricIdentity::load_or_generate(&identity_path)
        .unwrap()
        .node_id();

    let old_edge = Arc::new(Database::open(&edge_path).unwrap());
    let old_life = old_edge
        .sync_incarnation_for_test(&TenantId::from(TENANT))
        .unwrap();
    old_edge.execute(LINKS, &params()).unwrap();
    let old_client = make_client(&old_edge, &hub.ticket, &identity_path);
    assert!(
        within(old_client.push())
            .await
            .unwrap()
            .conflicts
            .is_empty()
    );
    old_edge
        .execute("ALTER TABLE links SET SYNC CONFLICT KEEP LATEST", &params())
        .unwrap();
    let refusal = within(old_client.push()).await.unwrap();
    assert_eq!(refusal.conflicts.len(), 1, "{refusal:?}");
    assert!(
        refusal.conflicts[0]
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("whole authored transaction is refused")
    );
    old_edge
        .execute(
            "INSERT INTO links (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, 'HELD')",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(0xA001))),
                ("source".into(), Value::Uuid(uuid::Uuid::from_u128(0xA002))),
                ("target".into(), Value::Uuid(uuid::Uuid::from_u128(0xA003))),
            ]),
        )
        .unwrap();
    let held = within(old_client.push()).await.unwrap();
    assert_eq!(held.conflicts.len(), 1, "{held:?}");
    assert!(
        held.conflicts[0]
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("held back")
    );
    let old_hold = hub.db.policy_held_received_tables().unwrap();
    assert_eq!(old_hold.len(), 1, "the old life has one durable hold");
    assert_eq!(old_hold[0].source_node_id, node);
    assert_eq!(old_hold[0].table, "links");
    within(old_client.shutdown()).await;
    drop(old_client);
    Arc::try_unwrap(old_edge)
        .unwrap_or_else(|_| panic!("the old edge has one owner"))
        .close()
        .unwrap();

    // The database and its companion belong to the retired edge life. The
    // fabric identity key survives so the authenticated node stays the same.
    std::fs::remove_file(&edge_path).unwrap();
    std::fs::remove_file(contextdb_engine::persistence::store_companion_path(
        &edge_path,
    ))
    .unwrap();
    let new_edge = Arc::new(Database::open(&edge_path).unwrap());
    let new_life = new_edge
        .sync_incarnation_for_test(&TenantId::from(TENANT))
        .unwrap();
    assert_ne!(new_life, old_life, "the recreated store has a new life");
    assert_eq!(
        FabricIdentity::load_or_generate(&identity_path)
            .unwrap()
            .node_id(),
        node,
        "the new life authenticates as the same node"
    );

    new_edge.execute(LINKS, &params()).unwrap();
    let new_client = make_client(&new_edge, &hub.ticket, &identity_path);
    let declaration = within(new_client.push()).await.unwrap();
    assert!(
        declaration.conflicts.is_empty(),
        "the compatible new-life declaration is accepted: {declaration:?}"
    );
    assert!(
        new_client.refused_schema_units_for_test().is_empty(),
        "the old life's refusal is not reported as a new-life outcome"
    );
    new_edge
        .execute(
            "INSERT INTO links (id, source_id, target_id, edge_type) \
             VALUES ($id, $source, $target, 'HELD')",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(0xB001))),
                ("source".into(), Value::Uuid(uuid::Uuid::from_u128(0xB002))),
                ("target".into(), Value::Uuid(uuid::Uuid::from_u128(0xB003))),
            ]),
        )
        .unwrap();
    let row = within(new_client.push()).await.unwrap();
    assert!(
        row.conflicts.is_empty(),
        "the new life's row does not inherit the old hold: {row:?}"
    );
    assert_eq!(
        hub.db
            .execute("SELECT id FROM links", &params())
            .unwrap()
            .rows,
        vec![vec![Value::Uuid(uuid::Uuid::from_u128(0xB001))]],
        "only the new life's row reaches the hub"
    );
    within(new_client.shutdown()).await;
    drop(new_client);

    let edge = ChangeSet {
        edges: vec![edge_change(
            Lsn(new_edge.current_lsn().0 + 1),
            "HELD",
            0xB004,
        )],
        ..ChangeSet::default()
    };
    let edge_response = push_edge_payload(&hub, &identity_path, &new_edge, edge, false).await;
    assert!(
        edge_response.result.unwrap().conflicts.is_empty(),
        "the new life's dependent edge is accepted"
    );
    assert_eq!(
        hub.db
            .edge_count(uuid::Uuid::from_u128(0xE000), "HELD", hub.db.snapshot())
            .unwrap(),
        1
    );
    assert_eq!(
        hub.db.policy_held_received_tables().unwrap(),
        old_hold,
        "a new-life declaration cannot retire the old life's hold"
    );
    hub.stop().await;
    let hub = start_hub(root.path()).await;
    assert_eq!(
        hub.db.policy_held_received_tables().unwrap(),
        old_hold,
        "the old-life hold remains durable after hub restart"
    );
    hub.stop().await;
}
