#![allow(clippy::needless_update)] // Existing compatibility and test seam shape.
//! A long refusal audit must not prevent a later authored unit from syncing.
//! The scripted hub generates pages on demand; no large durable fixture is needed.

use contextdb_core::{Incarnation, Lsn, TenantId};
use contextdb_engine::Database;
use contextdb_engine::protocol::{
    MessageType, PROTOCOL_VERSION, PushRequest, PushResponse, RefusedSchemaUnitOutcome,
    SyncStatusRequest, SyncStatusResponse, WireApplyResult, WireDdlChange, decode_for_version,
    encode_for_version,
};
use contextdb_engine::sync_client::SyncClient;
use contextdb_engine::sync_types::ChangeSet;
use contextdb_engine::transport::{ClientTransport, TransportError, TransportFuture};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncServer};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const TENANT: &str = "refusal-history-forward-progress";

/// An exact-lookup hub with more historical refusals than the former page ceiling.
struct RefusalHistoryTransport {
    tenant: TenantId,
    edge_node: String,
    edge_life: Incarnation,
    hub_node: String,
    hub_life: Incarnation,
    pages: usize,
    exact_first: Mutex<Option<(Lsn, [u8; 32])>>,
    interrupt_after_first: AtomicBool,
    status_requests: AtomicUsize,
    exact_queries: Mutex<Vec<Vec<[u8; 32]>>>,
    ignore_exact_queries: AtomicBool,
    pushed_ddl: Mutex<Vec<WireDdlChange>>,
    pushes: AtomicUsize,
    accepted_source_lsn: AtomicU64,
}

impl RefusalHistoryTransport {
    fn new(edge: &Database, edge_node: String, hub_node: String, pages: usize) -> Self {
        let tenant = TenantId::from(TENANT);
        Self {
            edge_life: edge.sync_incarnation_for_test(&tenant).unwrap(),
            tenant,
            edge_node,
            hub_node,
            hub_life: Incarnation::mint(),
            pages,
            exact_first: Mutex::new(None),
            interrupt_after_first: AtomicBool::new(false),
            status_requests: AtomicUsize::new(0),
            exact_queries: Mutex::new(Vec::new()),
            ignore_exact_queries: AtomicBool::new(false),
            pushed_ddl: Mutex::new(Vec::new()),
            pushes: AtomicUsize::new(0),
            accepted_source_lsn: AtomicU64::new(0),
        }
    }

    fn outcome(&self, page: usize) -> RefusedSchemaUnitOutcome {
        let exact = *self.exact_first.lock().unwrap();
        let (source_ddl_lsn, unit_digest) = if page == 0
            && let Some(exact) = exact
        {
            exact
        } else {
            let old_index = page - usize::from(exact.is_some());
            let mut digest = [0xff; 32];
            digest[28..].copy_from_slice(&(old_index as u32).to_be_bytes());
            (Lsn(10_000 + old_index as u64), digest)
        };
        RefusedSchemaUnitOutcome {
            tenant_id: self.tenant.clone(),
            pushing_node_id: self.edge_node.clone(),
            pushing_incarnation: self.edge_life,
            author_node_id: self.edge_node.clone(),
            author_incarnation: self.edge_life,
            source_ddl_lsn,
            unit_digest,
            hub_node_id: self.hub_node.clone(),
            hub_incarnation: self.hub_life,
            table: "older_refused_unit".into(),
            reason: "historical policy refusal".into(),
        }
    }
}

impl ClientTransport for RefusalHistoryTransport {
    fn peer_node_id(&self) -> Option<String> {
        Some(self.hub_node.clone())
    }

    fn local_node_id(&self) -> Option<String> {
        Some(self.edge_node.clone())
    }

    fn has_stable_edge_identity(&self) -> bool {
        true
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let envelope = decode_for_version(&request_bytes, PROTOCOL_VERSION)
                .map_err(|error| TransportError::Other(error.to_string()))?;
            if subject.contains("status") {
                assert_eq!(envelope.message_type, MessageType::StatusRequest);
                let request: SyncStatusRequest = rmp_serde::from_slice(&envelope.payload).unwrap();
                self.status_requests.fetch_add(1, Ordering::SeqCst);
                let (outcomes, ack) = if !self.ignore_exact_queries.load(Ordering::SeqCst) {
                    let digests = &request.refused_schema_unit_digests;
                    assert!(digests.len() <= 128);
                    let mut queries = self.exact_queries.lock().unwrap();
                    if !digests.is_empty() {
                        queries.push(digests.clone());
                    }
                    if !digests.is_empty()
                        && queries.len() == 2
                        && self.interrupt_after_first.swap(false, Ordering::SeqCst)
                    {
                        return Err(TransportError::Unreachable(
                            "scripted candidate-page interruption".into(),
                        ));
                    }
                    let exact = *self.exact_first.lock().unwrap();
                    let outcomes = exact
                        .filter(|(_, digest)| digests.contains(digest))
                        .map(|_| vec![self.outcome(0)])
                        .unwrap_or_default();
                    (outcomes, Some(digests.clone()))
                } else {
                    (Vec::new(), None)
                };
                let response = SyncStatusResponse {
                    applied_push_watermark: Some(Lsn(self
                        .accepted_source_lsn
                        .load(Ordering::SeqCst))),
                    server_current_lsn: Some(Lsn(1)),
                    hub_incarnation: Some(self.hub_life),
                    refused_schema_units: outcomes,
                    refused_schema_unit_digests_ack: ack,
                };
                encode_for_version(PROTOCOL_VERSION, MessageType::StatusResponse, &response)
                    .map_err(|error| TransportError::Other(error.to_string()))
            } else if subject.contains("push") {
                assert!(matches!(
                    envelope.message_type,
                    MessageType::PushRequest | MessageType::DependencyCompletePushRequest
                ));
                let request: PushRequest = rmp_serde::from_slice(&envelope.payload).unwrap();
                if let Some(source_lsn) = request.changeset.ddl_lsn.iter().max() {
                    self.accepted_source_lsn
                        .store(source_lsn.0, Ordering::SeqCst);
                }
                self.pushed_ddl
                    .lock()
                    .unwrap()
                    .extend(request.changeset.ddl.iter().cloned());
                self.pushes.fetch_add(1, Ordering::SeqCst);
                let response = PushResponse {
                    result: Some(WireApplyResult {
                        new_lsn: Lsn(1),
                        ..Default::default()
                    }),
                    hub_incarnation: Some(self.hub_life),
                    ..Default::default()
                };
                encode_for_version(PROTOCOL_VERSION, MessageType::PushResponse, &response)
                    .map_err(|error| TransportError::Other(error.to_string()))
            } else {
                panic!("unexpected sync subject: {subject}");
            }
        })
    }
}

fn current_authored_unit(db: &Database, identity: &FabricIdentity) -> (Lsn, [u8; 32]) {
    let history = db
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result");
    let ddl = history.ddl.last().unwrap().clone();
    let lsn = *history.ddl_lsn.last().unwrap();
    let unit = ChangeSet {
        ddl: vec![ddl],
        ddl_lsn: vec![lsn],
        ..Default::default()
    };
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let authors = db
        .outbound_ddl_provenance_for_test(
            &unit,
            &TenantId::from(TENANT),
            &identity.node_id(),
            db.sync_incarnation_for_test(&TenantId::from(TENANT))
                .unwrap(),
            &|bytes| Ok(identity.sign_for_test(bytes)),
            fixture_operation_reader_1.clone(),
        )
        .unwrap();
    let author = authors[0].author.as_ref().unwrap();
    let bytes = rmp_serde::to_vec(&vec![author]).unwrap();
    (lsn, *blake3::hash(&bytes).as_bytes())
}

fn setup(
    pages: usize,
) -> (
    Arc<Database>,
    Arc<FabricIdentity>,
    Arc<RefusalHistoryTransport>,
) {
    let db = Arc::new(Database::open_memory());
    db.complete_initialization().unwrap();
    let edge_identity = Arc::new(FabricIdentity::generate());
    let hub_identity = FabricIdentity::generate();
    let transport = Arc::new(RefusalHistoryTransport::new(
        &db,
        edge_identity.node_id(),
        hub_identity.node_id(),
        pages,
    ));
    (db, edge_identity, transport)
}

fn client(
    db: &Arc<Database>,
    identity: &Arc<FabricIdentity>,
    transport: &Arc<RefusalHistoryTransport>,
) -> SyncClient {
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        transport.clone(),
        TenantId::from(TENANT),
        identity.clone(),
    )
    .expect("read or construct the declared fixture result")
}

#[tokio::test]
async fn old_refusals_past_the_former_page_ceiling_do_not_block_a_new_unit() {
    let (db, identity, transport) = setup(4097);
    db.execute(
        "CREATE TABLE fresh (id UUID PRIMARY KEY) SYNC TWO WAY",
        &HashMap::new(),
    )
    .unwrap();
    let author = client(&db, &identity, &transport);
    let result = author
        .push()
        .await
        .expect("historical refusals permit a valid push");
    assert!(result.conflicts.is_empty());
    assert_eq!(transport.pushes.load(Ordering::SeqCst), 1);
    assert!(
        transport.pages > 4096,
        "the fixture exceeds the old history ceiling"
    );
    assert_eq!(transport.exact_queries.lock().unwrap().len(), 1);
    assert!(
        transport.status_requests.load(Ordering::SeqCst) <= 2,
        "exact lookup work is independent of the audit history size"
    );

    // This new refusal sorts before a previously consumed high digest. An
    // audit cursor reused as a candidate cursor would skip it and re-offer it.
    db.execute(
        "CREATE TABLE later (id UUID PRIMARY KEY) SYNC TWO WAY",
        &HashMap::new(),
    )
    .unwrap();
    let exact = current_authored_unit(&db, &identity);
    assert!(exact.1 < transport.outcome(0).unit_digest);
    *transport.exact_first.lock().unwrap() = Some(exact);
    let before = author.push_watermark();
    let quiet = author
        .push()
        .await
        .expect("new exact refusal is discovered");
    assert!(quiet.conflicts.is_empty());
    assert_eq!(transport.pushes.load(Ordering::SeqCst), 1);
    assert_eq!(author.push_watermark(), before);
    assert_eq!(author.refused_schema_units_for_test().len(), 1);
    let queried = transport.exact_queries.lock().unwrap().len();
    author
        .push()
        .await
        .expect("settled refusal stays locally known");
    assert_eq!(
        transport.exact_queries.lock().unwrap().len(),
        queried,
        "a settled exact refusal must not consume another hub lookup"
    );
}

#[tokio::test]
async fn interrupted_candidate_paging_never_sends_and_keeps_its_pending_refusal() {
    let (db, identity, transport) = setup(3);
    db.execute(
        "CREATE TABLE refused (id UUID PRIMARY KEY) SYNC TWO WAY",
        &HashMap::new(),
    )
    .unwrap();
    *transport.exact_first.lock().unwrap() = Some(current_authored_unit(&db, &identity));
    for i in 0..128 {
        db.execute(
            &format!("CREATE TABLE later_{i} (id UUID PRIMARY KEY) SYNC TWO WAY"),
            &HashMap::new(),
        )
        .unwrap();
    }
    transport
        .interrupt_after_first
        .store(true, Ordering::SeqCst);
    let first = client(&db, &identity, &transport);
    assert!(
        first.push().await.is_err(),
        "an incomplete candidate image is not authority to send"
    );
    assert_eq!(transport.pushes.load(Ordering::SeqCst), 0);
    drop(first);

    let restored = client(&db, &identity, &transport);
    let quiet = restored
        .push()
        .await
        .expect("finish candidate paging after recreation");
    assert!(quiet.conflicts.is_empty());
    assert!(transport.pushes.load(Ordering::SeqCst) > 0);
    assert!(
        !transport
            .pushed_ddl
            .lock()
            .unwrap()
            .iter()
            .any(|ddl| matches!(ddl, WireDdlChange::CreateTable { name, .. } if name == "refused"))
    );
    assert_eq!(restored.refused_schema_units_for_test().len(), 1);
    let queries = transport.exact_queries.lock().unwrap();
    assert_eq!(
        queries.len(),
        3,
        "the completed first batch is not queried again"
    );
    assert_eq!(queries[0].len(), 128);
    assert_eq!(queries[1].len(), 1);
    assert_eq!(
        queries[1], queries[2],
        "retry resumes at the interrupted candidate batch"
    );
}

#[tokio::test]
async fn a_hub_without_exact_query_acknowledgment_cannot_authorize_sending() {
    let (db, identity, transport) = setup(3);
    db.execute(
        "CREATE TABLE fresh (id UUID PRIMARY KEY) SYNC TWO WAY",
        &HashMap::new(),
    )
    .unwrap();
    transport.ignore_exact_queries.store(true, Ordering::SeqCst);
    let author = client(&db, &identity, &transport);
    let error = author
        .push()
        .await
        .expect_err("old hub cannot confirm candidate absence");
    assert!(
        error
            .to_string()
            .contains("did not acknowledge exact refused schema lookup")
    );
    assert_eq!(transport.pushes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_changed_candidate_image_or_hub_life_restarts_the_candidate_position() {
    for change_hub_life in [false, true] {
        let (db, identity, mut transport) = setup(3);
        db.execute(
            "CREATE TABLE refused (id UUID PRIMARY KEY) SYNC TWO WAY",
            &HashMap::new(),
        )
        .unwrap();
        *transport.exact_first.lock().unwrap() = Some(current_authored_unit(&db, &identity));
        for i in 0..128 {
            db.execute(
                &format!("CREATE TABLE later_{i} (id UUID PRIMARY KEY) SYNC TWO WAY"),
                &HashMap::new(),
            )
            .unwrap();
        }
        transport
            .interrupt_after_first
            .store(true, Ordering::SeqCst);
        let first = client(&db, &identity, &transport);
        assert!(first.push().await.is_err());
        assert_eq!(transport.pushes.load(Ordering::SeqCst), 0);
        drop(first);
        if change_hub_life {
            Arc::get_mut(&mut transport).unwrap().hub_life = Incarnation::mint();
        } else {
            db.execute(
                "CREATE TABLE new_candidate (id UUID PRIMARY KEY) SYNC TWO WAY",
                &HashMap::new(),
            )
            .unwrap();
        }
        let restored = client(&db, &identity, &transport);
        restored
            .push()
            .await
            .expect("changed image restarts safely");
        let queries = transport.exact_queries.lock().unwrap();
        assert_eq!(queries.len(), 4);
        if change_hub_life {
            assert_eq!(
                queries[0], queries[2],
                "a new hub life rechecks the full batch"
            );
        } else {
            assert_eq!(
                queries[2].len(),
                127,
                "the changed image rechecks only unsettled units"
            );
            assert!(
                !queries[2].contains(&transport.exact_first.lock().unwrap().unwrap().1),
                "the durable positive fact remains settled for this hub life"
            );
        }
        assert_eq!(queries[3].len(), if change_hub_life { 1 } else { 2 });
        assert_eq!(restored.refused_schema_units_for_test().len(), 1);
        assert!(!transport.pushed_ddl.lock().unwrap().iter().any(
            |ddl| matches!(ddl, WireDdlChange::CreateTable { name, .. } if name == "refused")
        ));
    }
}

#[tokio::test]
async fn an_empty_exact_query_reports_the_fresh_hub_life_it_initializes() {
    let db = Arc::new(Database::open_memory());
    let broker = InProcessBroker::new();
    let identity = Arc::new(FabricIdentity::generate());
    let hub_node = identity.node_id();
    let edge_node = FabricIdentity::generate().node_id();
    let server = SyncServer::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.server_as(&hub_node),
        TenantId::from(TENANT),
        hub_node,
        identity,
    )
    .expect("read or construct the declared fixture result");
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    let subject = contextdb_engine::subjects::status_subject(TENANT);
    broker.wait_for_registered_route_for_test(&subject).await;
    let request = SyncStatusRequest {
        incarnation: Incarnation::mint(),
        refused_schema_unit_digests: Vec::new(),
        ..Default::default()
    };
    let reply = broker
        .client_as(&edge_node)
        .request(
            &subject,
            encode_for_version(PROTOCOL_VERSION, MessageType::StatusRequest, &request).unwrap(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    let envelope = decode_for_version(&reply, PROTOCOL_VERSION).unwrap();
    assert_eq!(envelope.message_type, MessageType::StatusResponse);
    let status: SyncStatusResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(status.refused_schema_unit_digests_ack, Some(Vec::new()));
    assert!(status.refused_schema_units.is_empty());
    assert_eq!(
        status.hub_incarnation,
        Some(
            db.sync_incarnation_for_test(&TenantId::from(TENANT))
                .unwrap()
        )
    );
    shutdown.store(true, Ordering::SeqCst);
    task.await.unwrap();
}

#[tokio::test]
async fn interrupted_candidate_position_and_positive_outcome_survive_a_store_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("edge.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let identity = Arc::new(FabricIdentity::generate());
    let transport = Arc::new(RefusalHistoryTransport::new(
        &db,
        identity.node_id(),
        FabricIdentity::generate().node_id(),
        3,
    ));
    db.execute(
        "CREATE TABLE refused (id UUID PRIMARY KEY) SYNC TWO WAY",
        &HashMap::new(),
    )
    .unwrap();
    *transport.exact_first.lock().unwrap() = Some(current_authored_unit(&db, &identity));
    for i in 0..128 {
        db.execute(
            &format!("CREATE TABLE later_{i} (id UUID PRIMARY KEY) SYNC TWO WAY"),
            &HashMap::new(),
        )
        .unwrap();
    }
    transport
        .interrupt_after_first
        .store(true, Ordering::SeqCst);
    let first = client(&db, &identity, &transport);
    assert!(first.push().await.is_err());
    assert_eq!(transport.pushes.load(Ordering::SeqCst), 0);
    drop(first);
    drop(db);

    let reopened = Arc::new(Database::open(&path).unwrap());
    assert_eq!(
        reopened
            .sync_incarnation_for_test(&TenantId::from(TENANT))
            .unwrap(),
        transport.edge_life,
    );
    let restored = client(&reopened, &identity, &transport);
    restored
        .push()
        .await
        .expect("resume the durable candidate pass");
    let queries = transport.exact_queries.lock().unwrap();
    assert_eq!(
        queries.len(),
        3,
        "the committed first batch survives store reopen"
    );
    assert_eq!(queries[0].len(), 128);
    assert_eq!(queries[1], queries[2]);
    assert_eq!(queries[2].len(), 1);
    assert_eq!(restored.refused_schema_units_for_test().len(), 1);
    assert!(transport.pushes.load(Ordering::SeqCst) > 0);
    assert!(
        !transport
            .pushed_ddl
            .lock()
            .unwrap()
            .iter()
            .any(|ddl| matches!(ddl, WireDdlChange::CreateTable { name, .. } if name == "refused"))
    );
}
