//! Hub-authored outcomes, complete readback, truthful pending, and automatic restore.

use contextdb_core::{Lsn, TenantId, Value, Wallclock};
use contextdb_engine::database::DeliveryManifest;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{Database, DeliveryOutcome, DeliveryOutcomeKind};
use contextdb_server::protocol::{
    MessageType, PushRequest, PushResponse, WireDeliveryOutcome, decode, encode,
};
use contextdb_server::subjects::push_subject;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportStatusFuture,
};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

const ROOT_TABLE: &str = "records";
const MEMBER_TABLE: &str = "record_parts";
const OFFLINE_ROOT_TABLE: &str = "offline_records";
const ROOT_DDL: &str = "CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER record_parts";
const MEMBER_DDL: &str = "CREATE TABLE record_parts \
     (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const UNRELATED_DDL: &str = "CREATE TABLE unrelated_rows (id UUID PRIMARY KEY, body TEXT) \
     SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const OFFLINE_ROOT_DDL: &str = "CREATE TABLE offline_records (id UUID PRIMARY KEY, body TEXT) \
     SYNC OFF DELIVERY MANIFEST OVER offline_record_parts";
const OFFLINE_MEMBER_DDL: &str = "CREATE TABLE offline_record_parts \
     (id UUID PRIMARY KEY, record_id UUID REFERENCES offline_records(id), body TEXT) SYNC OFF";
const ROOT_CLAUSES: &str =
    "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER record_parts";
const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const OFFLINE_ROOT_CLAUSES: &str = "SYNC OFF DELIVERY MANIFEST OVER offline_record_parts";
const OFFLINE_MEMBER_CLAUSES: &str = "SYNC OFF";
/// A fixed instant for the mocked clock. Retention is driven by advancing this
/// value and running the pruning cycle synchronously on the test thread.
const T0: u64 = 1_700_000_000_000;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("bounded authenticated sync exchange")
}

fn create_core_tables(db: &Database) {
    db.execute(ROOT_DDL, &p()).expect("root table");
    db.execute(MEMBER_DDL, &p()).expect("member table");
    db.execute(UNRELATED_DDL, &p()).expect("unrelated table");
}

fn create_all_tables(db: &Database) {
    create_core_tables(db);
    db.execute(OFFLINE_ROOT_DDL, &p()).expect("offline root");
    db.execute(OFFLINE_MEMBER_DDL, &p())
        .expect("offline member");
}

fn core_expectation() -> ApplicationTablePolicyExpectation {
    ApplicationTablePolicyExpectation::new()
        .expect_table(ROOT_TABLE, ROOT_CLAUSES)
        .expect("root clauses parse")
        .expect_table(MEMBER_TABLE, MEMBER_CLAUSES)
        .expect("member clauses parse")
}

fn full_expectation() -> ApplicationTablePolicyExpectation {
    core_expectation()
        .expect_table(OFFLINE_ROOT_TABLE, OFFLINE_ROOT_CLAUSES)
        .expect("offline root clauses parse")
        .expect_table("offline_record_parts", OFFLINE_MEMBER_CLAUSES)
        .expect("offline member clauses parse")
}

struct RunningHub {
    db: Arc<Database>,
    node_id: String,
    identity: Arc<FabricIdentity>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) -> (Arc<Database>, Arc<FabricIdentity>) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
        (self.db, self.identity)
    }
}

async fn start_hub_with_identity(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
) -> RunningHub {
    start_hub_with_policy(broker, tenant, db, identity, full_expectation()).await
}

async fn start_hub_with_policy(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
    policy: ApplicationTablePolicyExpectation,
) -> RunningHub {
    db.__seed_tenant_table_policies_for_test(TenantId::from(tenant), policy)
        .expect("canonical declaration prerequisites");
    start_hub_without_policy_seeding(broker, tenant, db, identity).await
}

// Restoring a snapshot must not silently backfill missing declarations.
async fn start_hub_without_policy_seeding(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
) -> RunningHub {
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db.clone(),
            broker.server_as(&node_id),
            TenantId::from(tenant),
            node_id.clone(),
            identity.clone(),
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    within(broker.wait_for_registered_route_for_test(&push_subject(tenant))).await;
    RunningHub {
        db,
        node_id,
        identity,
        shutdown,
        task,
    }
}

async fn start_hub(broker: &InProcessBroker, tenant: &str, db: Arc<Database>) -> RunningHub {
    start_hub_with_identity(broker, tenant, db, Arc::new(FabricIdentity::generate())).await
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

// Retain the pending unit locally while observing ordinary traffic
// that the same authenticated edge actually offers to the hub.
#[derive(Clone, Debug)]
struct ObservedOutgoingPush {
    sender_node_id: String,
    incarnation: contextdb_core::Incarnation,
    rows: Vec<(String, Option<String>)>,
}

// This test-only transport removes the pending table's rows from
// its particular ordinary pushes; it does not introduce a production selector.
struct FilterPendingUnitTraffic {
    inner: Arc<dyn ClientTransport>,
    observed: Arc<Mutex<Vec<ObservedOutgoingPush>>>,
    active: Arc<AtomicBool>,
}

impl FilterPendingUnitTraffic {
    fn adapt_push(&self, request_bytes: Vec<u8>) -> Vec<u8> {
        if !self.active.load(Ordering::SeqCst) {
            return request_bytes;
        }
        let Ok(envelope) = decode(&request_bytes) else {
            return request_bytes;
        };
        if !matches!(
            envelope.message_type,
            MessageType::PushRequest | MessageType::DependencyCompletePushRequest
        ) {
            return request_bytes;
        }
        let mut request: PushRequest = rmp_serde::from_slice(&envelope.payload)
            .expect("decode the edge's actual outgoing push");
        request.changeset.ddl.clear();
        request.changeset.ddl_lsn.clear();
        request.changeset.ddl_provenance.clear();
        request.changeset.edges.clear();
        request.changeset.vectors.clear();
        request.changeset.purges.clear();
        // Adapter: holding back the real pending unit also holds
        // its now-populated ordinary manifest lane; only unrelated work travels.
        request.changeset.manifests.clear();
        request
            .changeset
            .rows
            .retain(|row| row.table == "unrelated_rows");
        if !request.changeset.rows.is_empty() {
            self.observed
                .lock()
                .expect("outgoing-push observation lock")
                .push(ObservedOutgoingPush {
                    sender_node_id: self
                        .inner
                        .local_node_id()
                        .expect("the authenticated edge has a sender identity"),
                    incarnation: request.incarnation,
                    rows: request
                        .changeset
                        .rows
                        .iter()
                        .map(|row| {
                            (
                                row.table.clone(),
                                row.values.get("body").and_then(|value| match value {
                                    Value::Text(body) => Some(body.clone()),
                                    _ => None,
                                }),
                            )
                        })
                        .collect(),
                });
        }
        encode(MessageType::PushRequest, &request).expect("re-encode filtered ordinary test push")
    }
}

impl ClientTransport for FilterPendingUnitTraffic {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let request_bytes = self.adapt_push(request_bytes);
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let request_bytes = self.adapt_push(request_bytes);
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }
}

/// A transport that FORWARDS the push to the real hub — so the hub verifies,
/// applies and commits the unit with its verdict — and then loses the reply on
/// the way back. The edge cannot tell that from a hub that died before
/// answering, which is exactly the recovery this shape exists to drive. Every
/// other subject forwards untouched.
struct ForwardPushThenLoseTheAcknowledgement {
    inner: Arc<dyn ClientTransport>,
    lose_next: Arc<AtomicBool>,
    offline: Arc<AtomicBool>,
}

impl ClientTransport for ForwardPushThenLoseTheAcknowledgement {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        if self.offline.load(Ordering::SeqCst) {
            return Box::pin(async { Err(TransportError::Unreachable("test outage".into())) });
        }
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if self.offline.load(Ordering::SeqCst) {
            return Box::pin(async { Err(TransportError::Unreachable("test outage".into())) });
        }
        if is_push_request(&request_bytes) && self.lose_next.swap(false, Ordering::SeqCst) {
            return Box::pin(async move {
                let _applied = self.inner.request(subject, request_bytes, timeout).await?;
                self.offline.store(true, Ordering::SeqCst);
                Err(TransportError::IndeterminateComplete(
                    "the hub applied this push and its acknowledgement was lost".to_string(),
                ))
            });
        }
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if self.offline.load(Ordering::SeqCst) {
            return Box::pin(async { Err(TransportError::Unreachable("test outage".into())) });
        }
        if is_push_request(&request_bytes) && self.lose_next.swap(false, Ordering::SeqCst) {
            return Box::pin(async move {
                let _applied = self
                    .inner
                    .request_single_reply(subject, request_bytes, timeout)
                    .await?;
                self.offline.store(true, Ordering::SeqCst);
                Err(TransportError::IndeterminateComplete(
                    "the hub applied this push and its acknowledgement was lost".to_string(),
                ))
            });
        }
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }
}

fn is_push_request(bytes: &[u8]) -> bool {
    decode(bytes).is_ok_and(|envelope| {
        matches!(
            envelope.message_type,
            MessageType::PushRequest | MessageType::DependencyCompletePushRequest
        )
    })
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
        .expect("install a signed manifested-unit prerequisite");
}

/// `NaturalKey` carries no total order, so a set of root references is compared
/// by each key's canonical encoding.
fn encoded_key(id: Uuid) -> Vec<u8> {
    rmp_serde::to_vec(&root_key(id)).expect("encode a root reference")
}

fn served_roots(outcomes: &[contextdb_engine::database::DeliveryOutcome]) -> BTreeSet<Vec<u8>> {
    outcomes
        .iter()
        .map(|outcome| rmp_serde::to_vec(outcome.root_key()).expect("encode a root reference"))
        .collect()
}

trait OutcomeContractView {
    fn kind(&self) -> DeliveryOutcomeKind;
    fn cause(&self) -> Option<&str>;
    fn tenant_id(&self) -> &str;
    fn hub_node_id(&self) -> &str;
    fn hub_incarnation(&self) -> contextdb_core::Incarnation;
    fn edge_node_id(&self) -> &str;
    fn edge_incarnation(&self) -> contextdb_core::Incarnation;
    fn root_table(&self) -> &str;
    fn root_key(&self) -> &NaturalKey;
    fn unit_digest(&self) -> [u8; 32];
    fn hub_acceptance_position(&self) -> Lsn;
    fn winning_author_node_id(&self) -> Option<&str>;
}

impl OutcomeContractView for DeliveryOutcome {
    fn kind(&self) -> DeliveryOutcomeKind {
        DeliveryOutcome::kind(self)
    }
    fn cause(&self) -> Option<&str> {
        DeliveryOutcome::cause(self)
    }
    fn tenant_id(&self) -> &str {
        self.tenant_id().as_str()
    }
    fn hub_node_id(&self) -> &str {
        DeliveryOutcome::hub_node_id(self)
    }
    fn hub_incarnation(&self) -> contextdb_core::Incarnation {
        DeliveryOutcome::hub_incarnation(self)
    }
    fn edge_node_id(&self) -> &str {
        DeliveryOutcome::edge_node_id(self)
    }
    fn edge_incarnation(&self) -> contextdb_core::Incarnation {
        DeliveryOutcome::edge_incarnation(self)
    }
    fn root_table(&self) -> &str {
        DeliveryOutcome::root_table(self)
    }
    fn root_key(&self) -> &NaturalKey {
        DeliveryOutcome::root_key(self)
    }
    fn unit_digest(&self) -> [u8; 32] {
        DeliveryOutcome::unit_digest(self).expect("terminal unit digest")
    }
    fn hub_acceptance_position(&self) -> Lsn {
        self.acceptance_position()
    }
    fn winning_author_node_id(&self) -> Option<&str> {
        self.conflicts()
            .and_then(|conflicts| conflicts.first())
            .and_then(|conflict| conflict.winning_author_node_id.as_deref())
    }
}

impl OutcomeContractView for Option<DeliveryOutcome> {
    fn kind(&self) -> DeliveryOutcomeKind {
        self.as_ref().expect("delivery outcome exists").kind()
    }
    fn cause(&self) -> Option<&str> {
        self.as_ref().expect("delivery outcome exists").cause()
    }
    fn tenant_id(&self) -> &str {
        self.as_ref()
            .expect("delivery outcome exists")
            .tenant_id()
            .as_str()
    }
    fn hub_node_id(&self) -> &str {
        self.as_ref()
            .expect("delivery outcome exists")
            .hub_node_id()
    }
    fn hub_incarnation(&self) -> contextdb_core::Incarnation {
        self.as_ref()
            .expect("delivery outcome exists")
            .hub_incarnation()
    }
    fn edge_node_id(&self) -> &str {
        self.as_ref()
            .expect("delivery outcome exists")
            .edge_node_id()
    }
    fn edge_incarnation(&self) -> contextdb_core::Incarnation {
        self.as_ref()
            .expect("delivery outcome exists")
            .edge_incarnation()
    }
    fn root_table(&self) -> &str {
        self.as_ref().expect("delivery outcome exists").root_table()
    }
    fn root_key(&self) -> &NaturalKey {
        self.as_ref().expect("delivery outcome exists").root_key()
    }
    fn unit_digest(&self) -> [u8; 32] {
        self.as_ref()
            .expect("delivery outcome exists")
            .unit_digest()
            .expect("terminal unit digest")
    }
    fn hub_acceptance_position(&self) -> Lsn {
        self.as_ref()
            .expect("delivery outcome exists")
            .acceptance_position()
    }
    fn winning_author_node_id(&self) -> Option<&str> {
        self.as_ref()
            .expect("delivery outcome exists")
            .conflicts()
            .and_then(|conflicts| conflicts.first())
            .and_then(|conflict| conflict.winning_author_node_id.as_deref())
    }
}

fn stage_unit_on(
    db: &Database,
    root_table: &str,
    member_table: &str,
    root_id: Uuid,
    root_body: &str,
    members: &[(Uuid, &str)],
) -> Result<contextdb_core::TxId, contextdb_core::Error> {
    let tx = db.begin()?;
    db.insert_row(
        tx,
        root_table,
        HashMap::from([
            ("id".to_string(), Value::Uuid(root_id)),
            ("body".to_string(), Value::Text(root_body.to_string())),
        ]),
    )?;
    let mut named = Vec::new();
    for (member_id, member_body) in members {
        db.insert_row(
            tx,
            member_table,
            HashMap::from([
                ("id".to_string(), Value::Uuid(*member_id)),
                ("record_id".to_string(), Value::Uuid(root_id)),
                ("body".to_string(), Value::Text((*member_body).to_string())),
            ]),
        )?;
        named.push((
            member_table,
            NaturalKey::single("id".to_string(), Value::Uuid(*member_id)),
        ));
    }
    Ok(tx)
}

fn stage_unit(
    db: &Database,
    root_id: Uuid,
    root_body: &str,
    members: &[(Uuid, &str)],
) -> Result<contextdb_core::TxId, contextdb_core::Error> {
    stage_unit_on(db, ROOT_TABLE, MEMBER_TABLE, root_id, root_body, members)
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT id FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"))
        .rows
        .len()
}

fn bodies(db: &Database, table: &str) -> BTreeSet<String> {
    let result = db
        .execute(&format!("SELECT body FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"));
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(text) => text.clone(),
            other => panic!("body must be TEXT, got {other:?}"),
        })
        .collect()
}

/// The outcome lane of every push reply the broker carried after `from`.
fn echoed_outcomes(broker: &InProcessBroker, from: usize) -> Vec<WireDeliveryOutcome> {
    broker
        .recorded_exchanges()
        .into_iter()
        .skip(from)
        .filter(|exchange| is_push_request(&exchange.request_bytes))
        .flat_map(|exchange| {
            let envelope = decode(&exchange.response_bytes).expect("decode push reply envelope");
            // Outcomes are echoed by the actual ordinary push response.
            assert_eq!(envelope.message_type, MessageType::PushResponse);
            let response: PushResponse =
                rmp_serde::from_slice(&envelope.payload).expect("decode ordinary push response");
            response.outcomes
        })
        .collect()
}

fn file_backed(root: &Path, name: &str) -> Arc<Database> {
    Arc::new(Database::open(root.join(name)).unwrap_or_else(|err| panic!("open {name}: {err}")))
}

// The hub commits one durable outcome with its rows; current-image reopen retains receipts.
// The durability fault targets this unit across the ordinary receiver's worker thread.
#[tokio::test(flavor = "current_thread")]
async fn every_applied_unit_records_exactly_one_durable_outcome_bound_to_both_identities_that_survives_restart_and_restore()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let tenant = "one-durable-outcome";
    let store = tempfile::tempdir().expect("tempdir");
    let broker = InProcessBroker::new();

    let hub_db = file_backed(store.path(), "hub.db");
    create_core_tables(&hub_db);
    let hub_incarnation = hub_db
        .sync_incarnation(&TenantId::from(tenant))
        .expect("read the hub's durable life");
    // Moved, not cloned: this test reopens the hub's file, so no other handle
    // may outlive the running server.
    let hub = start_hub(&broker, tenant, hub_db).await;
    let hub_node_id = hub.node_id.clone();

    let edge_db = file_backed(store.path(), "edge.db");
    let (edge, edge_node_id) = edge_client(&edge_db, &broker, tenant);
    let edge_incarnation = edge_db
        .sync_incarnation(&TenantId::from(tenant))
        .expect("read the edge's durable life");
    within(edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind before writing");
    create_core_tables(&edge_db);

    let root_id = Uuid::from_u128(0x5101_0000_0000_0000_0000_0000_0000_0001);
    let member_id = Uuid::from_u128(0x5101_0000_0000_0000_0000_0000_0000_0002);
    let tx = stage_unit(
        &edge_db,
        root_id,
        "root-body",
        &[(member_id, "member-body")],
    )
    .expect("commit the unit");
    seed_committed_manifest(&edge, tx, root_id, &[member_id]);
    edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    // Finish binding, status-independent request preparation and source commits
    // before arming the storage fault. Send only this prepared unit to the real public handler;
    // SyncClient::push's earlier bind/status/preparation writes cannot consume the fault.
    let request = edge
        .__delivery_push_request_for_test(ROOT_TABLE, &root_key(root_id))
        .unwrap();
    let source_digest = request.changeset.manifests[0]
        .unit_digest_for_test()
        .unwrap();
    let bytes = contextdb_server::protocol::encode(MessageType::PushRequest, &request)
        .expect("encode the actual source unit before arming the commit fault");
    let transport = broker.client_as(&edge_node_id);
    let subject = contextdb_server::subjects::push_subject(tenant);
    let before_failure = whole_unit_rows(&hub.db);
    assert_eq!(row_count(&hub.db, ROOT_TABLE), 0);
    assert_eq!(row_count(&hub.db, MEMBER_TABLE), 0);
    assert!(
        hub.db
            .__delivery_prerequisite_wires_for_test()
            .unwrap()
            .is_empty()
    );
    hub.db
        .__arm_delivery_commit_fault_for_test(ROOT_TABLE, &root_key(root_id), source_digest);
    let failed =
        within(transport.request_single_reply(&subject, bytes, Duration::from_secs(60))).await;
    // Decode the real failure so a missing target is distinguishable from injection.
    let commit_reached = hub.db.__delivery_commit_fault_reached_for_test();
    let failure = match failed {
        Ok(bytes) => {
            let envelope = decode(&bytes).expect("decode the failed push response");
            assert_eq!(envelope.message_type, MessageType::PushResponse);
            let response: PushResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
            assert!(
                response.result.is_none(),
                "the failed commit cannot report success"
            );
            response.error.expect("the hub reports its storage failure")
        }
        Err(error) => error.to_string(),
    };
    assert!(
        commit_reached,
        "the real unit storage commit must be reached, not a preflight refusal: {failure}"
    );
    // Reached + caller-visible failure is the guarantee, not storage wording.
    assert!(
        failure.contains("storage error"),
        "the reached storage fault must be reported to the caller: {failure}"
    );
    let (failed_db, hub_identity) = hub.stop().await;
    drop(failed_db);
    let reopened_db = file_backed(store.path(), "hub.db");
    assert_eq!(
        whole_unit_rows(&reopened_db),
        before_failure,
        "neither root nor members survive a failed unit commit"
    );
    assert!(
        reopened_db
            .__delivery_prerequisite_wires_for_test()
            .unwrap()
            .is_empty(),
        "no terminal outcome survives the failed unit commit either"
    );
    assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().pending, 1);
    let hub = start_hub_with_identity(&broker, tenant, reopened_db, hub_identity).await;
    // Preserve the successful one-outcome, restart and snapshot flow after failure.
    let mark = broker.recorded_exchanges().len();
    within(edge.push()).await.expect("push the unit");

    let outcome = edge_db
        .delivery_outcome(ROOT_TABLE, &root_key(root_id))
        .expect("the applied unit has a durable outcome at the edge");
    assert_eq!(
        outcome.kind(),
        DeliveryOutcomeKind::Accepted,
        "the hub committed the complete unit"
    );
    assert_eq!(outcome.cause(), None, "an accepted unit has no cause");
    assert_eq!(outcome.tenant_id(), tenant);
    assert_eq!(
        outcome.hub_node_id(),
        hub_node_id,
        "the verdict names the hub that made it"
    );
    assert_eq!(
        outcome.hub_incarnation(),
        hub_incarnation,
        "the verdict names the hub life that made it"
    );
    assert_eq!(
        outcome.edge_node_id(),
        edge_node_id,
        "the verdict names the edge it was made for"
    );
    assert_eq!(outcome.edge_incarnation(), edge_incarnation);
    assert_eq!(outcome.root_table(), ROOT_TABLE);
    assert_eq!(outcome.root_key(), &root_key(root_id));
    assert_eq!(
        outcome.unit_digest().len(),
        32,
        "the verdict names the exact unit it decided"
    );

    assert_eq!(
        outcome.unit_digest(),
        source_digest,
        "the receipt identifies the unit actually committed at the source"
    );

    // Exactly one, and no column value anywhere in what crossed the wire.
    let echoed = echoed_outcomes(&broker, mark);
    assert_eq!(echoed.len(), 1, "one applied unit, one verdict");
    let encoded = rmp_serde::to_vec(&echoed).expect("encode the outcome lane");
    for body in ["root-body", "member-body"] {
        assert!(
            !encoded
                .windows(body.len())
                .any(|window| window == body.as_bytes()),
            "an outcome carries references and digests, never the column value {body}"
        );
    }

    let served = within(edge.fetch_delivery_outcomes(None))
        .await
        .expect("the hub serves this edge its own outcomes");
    assert_eq!(served.len(), 1, "the hub holds exactly one verdict for it");
    let position = served[0].hub_acceptance_position();
    // The durable verdict names the root and members' actual accepting commit,
    // so a successful rows-first/outcome-second commit cannot substitute a later position.
    for table in [ROOT_TABLE, MEMBER_TABLE] {
        let rows = hub.db.scan(table, hub.db.snapshot()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].lsn, position,
            "{table} and its outcome share the accepting commit"
        );
    }
    let before_retry = whole_unit_rows(&hub.db);
    let retry = within(edge.push()).await.unwrap();
    assert_eq!(
        retry.applied_rows, 0,
        "retry cannot reapply a committed unit"
    );
    assert!(retry.conflicts.is_empty());
    assert_eq!(whole_unit_rows(&hub.db), before_retry);

    // --- Hub restart: the same store, the same identity, the same life. ---
    let (hub_db, hub_identity) = hub.stop().await;
    drop(hub_db);
    let restarted_db = file_backed(store.path(), "hub.db");
    let restarted = start_hub_with_identity(&broker, tenant, restarted_db, hub_identity).await;
    let after_restart = within(edge.fetch_delivery_outcomes(None))
        .await
        .expect("the restarted hub still serves the verdict");
    assert_eq!(
        after_restart.len(),
        1,
        "a verdict committed with its unit survives a hub restart"
    );
    assert_eq!(after_restart[0].kind(), DeliveryOutcomeKind::Accepted);
    assert_eq!(after_restart[0].hub_incarnation(), hub_incarnation);
    assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().pending, 0);
    assert_eq!(
        after_restart[0].hub_acceptance_position(),
        position,
        "the verdict names the same position it always named"
    );
    assert_eq!(after_restart[0].unit_digest(), served[0].unit_digest());

    // --- Restore: the hub's own snapshot, opened and served. ---
    let snapshot = store.path().join("hub-snapshot.db");
    restarted
        .db
        .export_snapshot(&snapshot)
        .expect("export a snapshot of the hub");
    let (hub_db, hub_identity) = restarted.stop().await;
    drop(hub_db);
    let restored_db = Arc::new(Database::open(&snapshot).expect("open the restored hub store"));
    let restored = start_hub_with_identity(&broker, tenant, restored_db, hub_identity).await;
    let after_restore = within(edge.fetch_delivery_outcomes(None))
        .await
        .expect("the restored hub still serves the verdict");
    assert_eq!(
        after_restore.len(),
        1,
        "a verdict is carried by the store's own snapshot"
    );
    assert_eq!(after_restore[0].kind(), DeliveryOutcomeKind::Accepted);
    assert_eq!(after_restore[0].hub_incarnation(), hub_incarnation);
    assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().pending, 0);
    assert_eq!(whole_unit_rows(&restored.db), before_retry);
    assert_eq!(after_restore[0].unit_digest(), served[0].unit_digest());

    drop(edge);
    let _ = restored.stop().await;
}

// Whole-row and membership equality earns equivalence; differences earn complete refusal.
#[tokio::test]
async fn an_identical_repush_is_equivalent_a_differing_digest_is_refused_with_the_keep_first_diagnostic_and_a_memberless_root_is_accepted()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let tenant = "equivalent-and-differing";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    create_core_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db.clone()).await;

    let root_id = Uuid::from_u128(0x5201_0000_0000_0000_0000_0000_0000_0001);
    let member_id = Uuid::from_u128(0x5201_0000_0000_0000_0000_0000_0000_0002);

    // The first writer's unit is accepted.
    let first_db = Arc::new(Database::open_memory());
    let (first_edge, first_node_id) = edge_client(&first_db, &broker, tenant);
    within(first_edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind the first writer");
    create_core_tables(&first_db);
    let tx = stage_unit(
        &first_db,
        root_id,
        "root-body",
        &[(member_id, "member-body")],
    )
    .expect("commit the unit");
    seed_committed_manifest(&first_edge, tx, root_id, &[member_id]);
    first_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    within(first_edge.__seed_delivery_outcome_for_test(
        ROOT_TABLE,
        &root_key(root_id),
        DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .expect("genuine incumbent terminal before equivalence probe");
    let first_digest = first_edge
        .__delivery_push_request_for_test(ROOT_TABLE, &root_key(root_id))
        .unwrap()
        .changeset
        .manifests[0]
        .unit_digest_for_test()
        .unwrap();
    let first_rows = whole_unit_rows(&hub_db);

    // A second machine imported byte-identical content: equivalent, no write,
    // no conflict.
    let second_db = Arc::new(Database::open_memory());
    let (second_edge, _second_node_id) = edge_client(&second_db, &broker, tenant);
    within(second_edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind the second writer");
    create_core_tables(&second_db);
    let tx = stage_unit(
        &second_db,
        root_id,
        "root-body",
        &[(member_id, "member-body")],
    )
    .expect("commit identical content");
    seed_committed_manifest(&second_edge, tx, root_id, &[member_id]);
    second_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    let hub_rows_before = row_count(&hub_db, ROOT_TABLE) + row_count(&hub_db, MEMBER_TABLE);
    let applied = within(second_edge.push()).await.expect("second push");
    assert!(
        applied.conflicts.is_empty(),
        "an identical re-push is not a conflict: {:?}",
        applied.conflicts
    );
    assert_eq!(
        applied.applied_rows, 0,
        "the hub already holds this exact unit, so it writes nothing"
    );
    let second = second_db
        .delivery_outcome(ROOT_TABLE, &root_key(root_id))
        .expect("the identical unit has a verdict of its own");
    assert_eq!(
        second.kind(),
        DeliveryOutcomeKind::Equivalent,
        "the hub already held a unit with this digest"
    );
    assert_eq!(second.cause(), None);
    assert_eq!(
        second.unit_digest(),
        first_digest,
        "the two machines computed the same unit digest for the same content"
    );
    assert_eq!(
        row_count(&hub_db, ROOT_TABLE) + row_count(&hub_db, MEMBER_TABLE),
        hub_rows_before,
        "nothing was written for an equivalent unit"
    );
    assert_eq!(
        second_db
            .delivery_status(ROOT_TABLE)
            .expect("second writer's status")
            .pending,
        0,
        "an equivalent verdict ends the resend obligation"
    );

    assert_eq!(
        whole_unit_rows(&hub_db),
        first_rows,
        "equivalence preserves every application value"
    );
    // A third machine holding the same key with different content: refused,
    // with the complete keep-first diagnostic.
    let third_db = Arc::new(Database::open_memory());
    let (third_edge, _third_node_id) = edge_client(&third_db, &broker, tenant);
    within(third_edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind the third writer");
    create_core_tables(&third_db);
    let tx = stage_unit(
        &third_db,
        root_id,
        "root-body",
        &[(member_id, "member-body-differs")],
    )
    .expect("commit differing content under the same root key");
    seed_committed_manifest(&third_edge, tx, root_id, &[member_id]);
    third_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    let applied = within(third_edge.push()).await.expect("third push");
    let third = third_db
        .delivery_outcome(ROOT_TABLE, &root_key(root_id))
        .expect("the differing unit has a verdict");
    assert_eq!(third.kind(), DeliveryOutcomeKind::Refused);
    assert_eq!(
        third.cause(),
        Some("unit_digest_mismatch"),
        "the same key with a different unit is a keep-first loss, named as one"
    );
    assert_eq!(
        third.winning_author_node_id(),
        Some(first_node_id.as_str()),
        "the diagnostic names the writer whose unit the hub kept"
    );
    let root_conflict = applied
        .conflicts
        .iter()
        .find(|conflict| conflict.table.as_deref() == Some(ROOT_TABLE))
        .expect("the refused unit's root carries a conflict");
    assert_eq!(
        root_conflict.reason.as_deref(),
        Some("unit_digest_mismatch")
    );
    assert_eq!(
        root_conflict.winning_author_node_id.as_deref(),
        Some(first_node_id.as_str()),
        "the root conflict carries the winning author"
    );
    assert!(
        root_conflict.hub_acceptance_position.is_some(),
        "the root conflict carries the position the winner was accepted at"
    );
    assert_ne!(
        bodies(&hub_db, MEMBER_TABLE),
        BTreeSet::from(["member-body-differs".to_string()]),
        "the hub kept the first unit's member content"
    );

    assert_eq!(
        whole_unit_rows(&hub_db),
        first_rows,
        "a refused unit changes no application value"
    );
    for (body, members) in [
        ("different root value", vec![(member_id, "member-body")]),
        (
            "root-body",
            vec![(member_id, "member-body"), (Uuid::new_v4(), "extra member")],
        ),
    ] {
        let db = Arc::new(Database::open_memory());
        let (client, _) = edge_client(&db, &broker, tenant);
        within(client.__seed_application_table_policy_binding_for_test(core_expectation()))
            .await
            .unwrap();
        create_core_tables(&db);
        let tx = stage_unit(&db, root_id, body, &members).unwrap();
        seed_committed_manifest(
            &client,
            tx,
            root_id,
            &members.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        );
        db.commit(tx).unwrap();
        within(client.push()).await.unwrap();
        let outcome = db
            .delivery_outcome(ROOT_TABLE, &root_key(root_id))
            .unwrap()
            .unwrap();
        assert_eq!(outcome.kind(), DeliveryOutcomeKind::Refused);
        assert_eq!(outcome.cause(), Some("unit_digest_mismatch"));
        assert_eq!(
            whole_unit_rows(&hub_db),
            first_rows,
            "neither a different root column nor a different complete membership may overwrite the winner"
        );
        within(client.shutdown()).await;
    }

    // A root with no members at all is accepted with an explicit empty membership.
    let memberless_root = Uuid::from_u128(0x5201_0000_0000_0000_0000_0000_0000_0009);
    let tx = stage_unit(&first_db, memberless_root, "root-body", &[])
        .expect("commit an explicitly memberless unit");
    seed_committed_manifest(&first_edge, tx, memberless_root, &[]);
    first_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    within(first_edge.push())
        .await
        .expect("push the memberless unit");
    let memberless = first_db
        .delivery_outcome(ROOT_TABLE, &root_key(memberless_root))
        .expect("the memberless unit has a verdict");
    assert_eq!(
        memberless.kind(),
        DeliveryOutcomeKind::Accepted,
        "a root with no members is still one complete unit"
    );
    assert_ne!(
        memberless.unit_digest(),
        first_digest,
        "an empty member set's digest is its own"
    );
    assert_eq!(
        first_db
            .delivery_status(ROOT_TABLE)
            .expect("first writer's status")
            .pending,
        0
    );

    drop(first_edge);
    drop(second_edge);
    drop(third_edge);
    let _ = hub.stop().await;
    hub_written_incumbents_receive_terminal_outcomes().await;
}

// A hub-local winner needs no earlier custody terminal.
async fn hub_written_incumbents_receive_terminal_outcomes() {
    use contextdb_server::transport::iroh::IrohServer;

    let directory = tempfile::tempdir().unwrap();
    let tenant = TenantId::from("hub-local-incumbents");
    let hub_path = directory.path().join("hub.db");
    let endpoint = IrohServer::bind(&format!(
        "iroh:?identity={}",
        directory.path().join("hub.key").display()
    ))
    .await
    .unwrap();
    let hub_node = endpoint.node_id();
    let hub_db = Arc::new(Database::open(&hub_path).unwrap());
    let server = Arc::new(SyncServer::new(hub_db.clone(), &endpoint, tenant.clone()));
    for (table, clauses) in [(ROOT_TABLE, ROOT_CLAUSES), (MEMBER_TABLE, MEMBER_CLAUSES)] {
        hub_db
            .execute(
                &format!("DECLARE TENANT TABLE POLICY {table} {clauses}"),
                &p(),
            )
            .unwrap();
    }
    create_core_tables(&hub_db);
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });

    for (index, root_body, held_count, offered_count, member_body, expected) in [
        (0, "held", 1, 1, "member", DeliveryOutcomeKind::Equivalent),
        (1, "held", 1, 1, "different", DeliveryOutcomeKind::Refused),
        (2, "different", 1, 1, "member", DeliveryOutcomeKind::Refused),
        (3, "held", 1, 2, "member", DeliveryOutcomeKind::Refused),
        (4, "held", 1, 0, "member", DeliveryOutcomeKind::Refused),
        (5, "held", 0, 1, "member", DeliveryOutcomeKind::Refused),
        (6, "held", 0, 0, "member", DeliveryOutcomeKind::Equivalent),
    ] {
        let root = Uuid::from_u128(0x6000 + index * 10);
        let member = Uuid::from_u128(0x6001 + index * 10);
        let extra = Uuid::from_u128(0x6002 + index * 10);
        let held_members = if held_count == 0 {
            vec![]
        } else {
            vec![(member, "member")]
        };
        let tx = stage_unit(&hub_db, root, "held", &held_members).unwrap();
        hub_db.commit(tx).unwrap();
        let winner_position = hub_db.current_lsn();
        assert_eq!(
            hub_db
                .execute("SHOW DELIVERY OUTCOMES FOR records", &p())
                .unwrap()
                .rows
                .len(),
            index as usize,
            "only the preceding units have outcomes; this hub-written unit has none"
        );
        let held_rows = whole_unit_rows(&hub_db);

        let edge_path = directory.path().join(format!("edge-{index}.db"));
        let edge_db = Arc::new(Database::open(&edge_path).unwrap());
        let client = SyncClient::new(
            edge_db.clone(),
            &contextdb_server::peer_dial_spec(
                &endpoint.ticket(),
                &directory.path().join(format!("edge-{index}.key")),
            ),
            tenant.clone(),
        );
        within(client.bind_application_table_policy(core_expectation()))
            .await
            .unwrap();
        create_core_tables(&edge_db);
        let offered = match offered_count {
            0 => vec![],
            1 => vec![(member, member_body)],
            _ => vec![(member, member_body), (extra, "extra")],
        };
        let tx = stage_unit(&edge_db, root, root_body, &offered).unwrap();
        edge_db
            .register_delivery_manifest(
                tx,
                DeliveryManifest {
                    root_table: ROOT_TABLE,
                    root_key: root_key(root),
                    members: offered
                        .iter()
                        .map(|(id, _)| (MEMBER_TABLE, root_key(*id)))
                        .collect(),
                },
            )
            .unwrap();
        edge_db.commit(tx).unwrap();
        edge_db
            .execute(
                "INSERT INTO unrelated_rows VALUES ($id, 'independent')",
                &HashMap::from([("id".into(), Value::Uuid(root))]),
            )
            .unwrap();
        let result = within(client.push())
            .await
            .expect("held units receive a durable answer through authenticated push");
        let outcome = edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(root))
            .unwrap()
            .unwrap();
        assert_eq!(outcome.kind(), expected);
        assert_eq!(
            whole_unit_rows(&hub_db),
            held_rows,
            "adjudication writes none of the arriving unit"
        );
        assert_eq!(result.applied_rows, 1, "only the independent sibling lands");
        if expected == DeliveryOutcomeKind::Equivalent {
            assert!(result.conflicts.is_empty());
            assert_eq!(outcome.cause(), None);
        } else {
            assert_eq!(outcome.cause(), Some("unit_digest_mismatch"));
            assert_eq!(result.conflicts.len(), 1 + offered.len());
            assert_eq!(outcome.winning_author_node_id(), Some(hub_node.as_str()));
            let root_conflict = &result.conflicts[0];
            assert_eq!(root_conflict.table.as_deref(), Some(ROOT_TABLE));
            assert_eq!(root_conflict.natural_key, root_key(root));
            for conflict in &result.conflicts {
                assert_eq!(conflict.reason.as_deref(), Some("unit_digest_mismatch"));
                assert_eq!(conflict.mutation_kind.as_deref(), Some("edit"));
                if conflict.table.as_deref() == Some(MEMBER_TABLE)
                    && (held_count == 0 || conflict.natural_key == root_key(extra))
                {
                    assert_eq!(conflict.winning_author_node_id, None);
                    assert_eq!(conflict.hub_acceptance_position, None);
                    let cause = conflict
                        .refusal_cause
                        .as_ref()
                        .expect("a winnerless member names its conflicting root");
                    assert_eq!(cause.table, ROOT_TABLE);
                    assert_eq!(cause.natural_key, root_key(root));
                } else {
                    assert_eq!(
                        conflict.winning_author_node_id.as_deref(),
                        Some(hub_node.as_str())
                    );
                    assert_eq!(conflict.hub_acceptance_position, Some(winner_position));
                    assert_eq!(conflict.refusal_cause, None);
                }
            }
            assert_eq!(
                whole_unit_rows(&edge_db)[0].len(),
                1,
                "the refused root remains locatable"
            );
            assert_eq!(whole_unit_rows(&edge_db)[1].len(), offered.len());
        }
        assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().pending, 0);
        assert_eq!(client.pending_push_change_count().unwrap(), 0);
        assert!(!client.has_pending_push_changes().unwrap());
        let metadata = hub_db.__delivery_metadata_bytes_for_test().unwrap();
        let again = within(client.push()).await.unwrap();
        assert_eq!(again.applied_rows, 0);
        assert!(again.conflicts.is_empty());
        assert_eq!(
            hub_db.__delivery_metadata_bytes_for_test().unwrap(),
            metadata,
            "a terminal unit is not re-offered"
        );
        assert_eq!(
            within(client.fetch_delivery_outcomes(None))
                .await
                .unwrap()
                .len(),
            1
        );
        within(client.shutdown()).await;
        drop(client);
        drop(edge_db);
        let reopened = Database::open(edge_path).unwrap();
        assert_eq!(
            reopened
                .delivery_outcome(ROOT_TABLE, &root_key(root))
                .unwrap()
                .unwrap()
                .kind(),
            expected
        );
        assert_eq!(reopened.delivery_status(ROOT_TABLE).unwrap().pending, 0);
    }
    let metadata = hub_db.__delivery_metadata_bytes_for_test().unwrap();
    shutdown.store(true, Ordering::SeqCst);
    within(task).await.unwrap();
    drop(server);
    drop(hub_db);
    let reopened = Database::open(hub_path).unwrap();
    assert_eq!(
        reopened.__delivery_metadata_bytes_for_test().unwrap(),
        metadata,
        "hub outcomes survive reopening"
    );
}

// Push echoes outcomes; lost replies recover completely and only for the asking edge.
#[tokio::test]
async fn the_push_response_echoes_outcomes_and_a_lost_acknowledgement_recovers_only_this_edges_outcomes_without_reapplying()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let tenant = "echo-and-recovery";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    create_core_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db.clone()).await;

    // --- The push response echoes the verdict. ---
    let edge_db = Arc::new(Database::open_memory());
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let edge_inner = broker.client_as(&edge_node_id);
    let lose_next = Arc::new(AtomicBool::new(false));
    let offline = Arc::new(AtomicBool::new(false));
    let edge_transport = Arc::new(ForwardPushThenLoseTheAcknowledgement {
        inner: edge_inner,
        lose_next: lose_next.clone(),
        offline: offline.clone(),
    });
    let edge = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        edge_transport,
        TenantId::from(tenant),
        edge_identity,
    );
    within(edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind the edge");
    create_core_tables(&edge_db);

    let echoed_root = Uuid::from_u128(0x5301_0000_0000_0000_0000_0000_0000_0001);
    let echoed_member = Uuid::from_u128(0x5301_0000_0000_0000_0000_0000_0000_0002);
    let tx = stage_unit(
        &edge_db,
        echoed_root,
        "root-body",
        &[(echoed_member, "member-body")],
    )
    .expect("commit the echoed unit");
    seed_committed_manifest(&edge, tx, echoed_root, &[echoed_member]);
    edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    let mark = broker.recorded_exchanges().len();
    within(edge.push()).await.expect("the push succeeds");
    let echoed = echoed_outcomes(&broker, mark);
    assert_eq!(
        echoed.len(),
        1,
        "the push response carries the verdict for the unit in that push"
    );
    assert_eq!(echoed[0].outcome_for_test(), Some("accepted"));
    assert!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(echoed_root))
            .expect("read echoed outcome")
            .is_some(),
        "the edge learned the verdict from the reply itself, with no further exchange"
    );

    // --- A lost acknowledgement. The hub applied and committed the verdict;
    // the edge never saw the reply. ---
    let lost_root = Uuid::from_u128(0x5301_0000_0000_0000_0000_0000_0000_0011);
    let lost_member = Uuid::from_u128(0x5301_0000_0000_0000_0000_0000_0000_0012);
    let tx = stage_unit(
        &edge_db,
        lost_root,
        "root-body",
        &[(lost_member, "member-body")],
    )
    .expect("commit the unit whose acknowledgement is lost");
    seed_committed_manifest(&edge, tx, lost_root, &[lost_member]);
    edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    lose_next.store(true, Ordering::SeqCst);
    let lost = within(edge.push()).await;
    assert!(
        lost.is_err(),
        "the edge cannot claim a push it never got an answer to"
    );
    assert!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(lost_root))
            .expect("read lost-reply outcome")
            .is_none(),
        "no reply, no verdict at the edge"
    );
    let hub_rows_after_apply = row_count(&hub_db, ROOT_TABLE) + row_count(&hub_db, MEMBER_TABLE);
    assert_eq!(
        hub_rows_after_apply, 4,
        "the hub did apply both units: two roots and two members"
    );

    offline.store(false, Ordering::SeqCst); // Restore transport before the explicit public fetch.
    let recovered = within(edge.fetch_delivery_outcomes(None))
        .await
        .expect("the edge asks the hub for its own verdicts");
    assert_eq!(
        recovered.len(),
        2,
        "both verdicts come back, in hub acceptance order"
    );
    assert!(
        recovered[0].hub_acceptance_position() <= recovered[1].hub_acceptance_position(),
        "outcomes are served in hub acceptance order"
    );
    let recovered_lost = edge_db
        .delivery_outcome(ROOT_TABLE, &root_key(lost_root))
        .expect("the lost verdict is recovered and persisted");
    assert_eq!(recovered_lost.kind(), DeliveryOutcomeKind::Accepted);
    assert_eq!(
        row_count(&hub_db, ROOT_TABLE) + row_count(&hub_db, MEMBER_TABLE),
        hub_rows_after_apply,
        "recovering a verdict re-applies nothing"
    );
    assert_eq!(
        edge_db
            .delivery_status(ROOT_TABLE)
            .expect("edge status after recovery")
            .pending,
        0
    );

    // --- Another edge's verdicts are never served to this one. ---
    let other_db = Arc::new(Database::open_memory());
    let (other_edge, _other_node_id) = edge_client(&other_db, &broker, tenant);
    within(other_edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind the other edge");
    create_core_tables(&other_db);
    let other_root = Uuid::from_u128(0x5301_0000_0000_0000_0000_0000_0000_0021);
    let tx = stage_unit(&other_db, other_root, "root-body", &[])
        .expect("the other edge commits its own unit");
    seed_committed_manifest(&other_edge, tx, other_root, &[]);
    other_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    within(other_edge.push())
        .await
        .expect("the other edge pushes");

    let mine = within(edge.fetch_delivery_outcomes(None))
        .await
        .expect("this edge asks again");
    assert_eq!(
        served_roots(&mine),
        BTreeSet::from([encoded_key(echoed_root), encoded_key(lost_root)]),
        "an edge is served its own verdicts and never another edge's"
    );
    let theirs = within(other_edge.fetch_delivery_outcomes(None))
        .await
        .expect("the other edge asks");
    assert_eq!(
        served_roots(&theirs),
        BTreeSet::from([encoded_key(other_root)]),
        "and the other edge is served only its own"
    );

    drop(edge);
    drop(other_edge);
    let _ = hub.stop().await;
    complete_outcome_readback().await;
}

// Only a durable hub outcome changes pending; counts reconcile and SYNC OFF is disabled.
#[tokio::test]
async fn delivery_status_reconciles_with_manifests_and_outcomes_and_only_an_outcome_moves_a_unit_out_of_pending()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let tenant = "status-reconciles";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    create_all_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db.clone()).await;

    let edge_db = Arc::new(Database::open_memory());
    // The pending owner also sends the unrelated keep-first traffic.
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let observed_pushes = Arc::new(Mutex::new(Vec::new()));
    let filter_traffic = Arc::new(AtomicBool::new(false));
    let edge = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        Arc::new(FilterPendingUnitTraffic {
            inner: broker.client_as(&edge_node_id),
            observed: observed_pushes.clone(),
            active: filter_traffic.clone(),
        }),
        TenantId::from(tenant),
        edge_identity,
    );
    within(edge.__seed_application_table_policy_binding_for_test(full_expectation()))
        .await
        .expect("bind every declared table");
    create_all_tables(&edge_db);

    // A second writer whose unit the hub keeps first, so this edge's same-key
    // unit is refused.
    let contested_root = Uuid::from_u128(0x5401_0000_0000_0000_0000_0000_0000_0001);
    let contested_member = Uuid::from_u128(0x5401_0000_0000_0000_0000_0000_0000_0002);
    let winner_db = Arc::new(Database::open_memory());
    let (winner_edge, _winner_node_id) = edge_client(&winner_db, &broker, tenant);
    within(winner_edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind the winning writer");
    create_core_tables(&winner_db);
    let tx = stage_unit(
        &winner_db,
        contested_root,
        "root-body",
        &[(contested_member, "winning-member")],
    )
    .expect("the winner commits first");
    seed_committed_manifest(&winner_edge, tx, contested_root, &[contested_member]);
    winner_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    within(winner_edge.__seed_delivery_outcome_for_test(
        ROOT_TABLE,
        &root_key(contested_root),
        DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .unwrap();

    // Four local units on the ordinary table: one accepted, one equivalent, one
    // refused, one never answered.
    let accepted_root = Uuid::from_u128(0x5401_0000_0000_0000_0000_0000_0000_0011);
    let equivalent_root = Uuid::from_u128(0x5401_0000_0000_0000_0000_0000_0000_0021);
    let equivalent_member = Uuid::from_u128(0x5401_0000_0000_0000_0000_0000_0000_0022);
    let tx =
        stage_unit(&edge_db, accepted_root, "root-body", &[]).expect("a unit only this edge holds");
    seed_committed_manifest(&edge, tx, accepted_root, &[]);
    edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    let tx = stage_unit(
        &edge_db,
        contested_root,
        "root-body",
        &[(contested_member, "losing-member")],
    )
    .expect("a unit the hub will keep-first refuse");
    seed_committed_manifest(&edge, tx, contested_root, &[contested_member]);
    edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    // The equivalent one: identical to a unit the winner already delivered.
    let tx = stage_unit(
        &winner_db,
        equivalent_root,
        "root-body",
        &[(equivalent_member, "member-body")],
    )
    .expect("the winner commits it");
    seed_committed_manifest(&winner_edge, tx, equivalent_root, &[equivalent_member]);
    winner_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    within(winner_edge.__seed_delivery_outcome_for_test(
        ROOT_TABLE,
        &root_key(equivalent_root),
        DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .unwrap();
    let tx = stage_unit(
        &edge_db,
        equivalent_root,
        "root-body",
        &[(equivalent_member, "member-body")],
    )
    .expect("this edge holds byte-identical content");
    seed_committed_manifest(&edge, tx, equivalent_root, &[equivalent_member]);
    edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    for (root, kind, cause) in [
        (accepted_root, DeliveryOutcomeKind::Accepted, None),
        (equivalent_root, DeliveryOutcomeKind::Equivalent, None),
        (
            contested_root,
            DeliveryOutcomeKind::Refused,
            Some("keep_first_refused"),
        ),
    ] {
        within(edge.__seed_delivery_outcome_for_test(ROOT_TABLE, &root_key(root), kind, cause))
            .await
            .expect("authentic independently validated status prerequisites");
    }
    // The fourth unit's acknowledgement is lost: the hub decided, this edge has
    // no verdict.
    let unanswered_root = Uuid::from_u128(0x5401_0000_0000_0000_0000_0000_0000_0031);
    let tx = stage_unit(&edge_db, unanswered_root, "root-body", &[])
        .expect("a unit whose answer is lost");
    let same_transaction_sibling = Uuid::new_v4();
    edge_db
        .insert_row(
            tx,
            "unrelated_rows",
            HashMap::from([
                ("id".into(), Value::Uuid(same_transaction_sibling)),
                (
                    "body".into(),
                    Value::Text("same transaction sibling".into()),
                ),
            ]),
        )
        .unwrap();
    seed_committed_manifest(&edge, tx, unanswered_root, &[]);
    edge_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    let mut pending_request = edge
        .__delivery_push_request_for_test(ROOT_TABLE, &root_key(unanswered_root))
        .unwrap();
    let ordinary = edge.__ordinary_push_request_for_test().unwrap();
    pending_request.changeset.rows.extend(
        ordinary
            .changeset
            .rows
            .into_iter()
            .filter(|r| r.table == "unrelated_rows"),
    );
    let bytes = within(broker.client_as(&edge_node_id).request_single_reply(
        &push_subject(tenant),
        encode(MessageType::PushRequest, &pending_request).unwrap(),
        Duration::from_secs(10),
    ))
    .await
    .unwrap();
    let envelope = decode(&bytes).unwrap();
    let reply: PushResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(reply.result.as_ref().unwrap().applied_rows, 2);
    assert_eq!(reply.outcomes.len(), 1);
    assert_eq!(
        hub_db
            .execute(
                "SELECT body FROM unrelated_rows WHERE id=$id",
                &HashMap::from([("id".into(), Value::Uuid(same_transaction_sibling))])
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("same transaction sibling".into())]]
    );
    // The actual reply above is deliberately not admitted at the edge.

    let status = edge_db
        .delivery_status(ROOT_TABLE)
        .expect("read the edge's status");
    assert_eq!(
        status.eligible, 4,
        "every manifested root committed locally on a table this edge sends is eligible"
    );
    assert_eq!(status.accepted, 1);
    assert_eq!(status.equivalent, 1);
    assert_eq!(status.refused, 1);
    assert_eq!(status.pending, 1);
    assert_eq!(
        status.accepted + status.equivalent + status.refused + status.pending,
        status.eligible,
        "the counts reconcile with the local manifests and the persisted verdicts"
    );
    assert!(!status.disabled);

    // The hub's own applied-push frontier for this edge HAS moved past the
    // unanswered unit — the hub applied it and only the answer was lost. A pull
    // and the status confirmation it performs read that frontier. None of them
    // is a verdict, so none of them may move the unit out of pending. The
    // manifested unit is deliberately not offered again here: a re-push is a
    // delivery, and the verdict it earns is proven separately. Its ordinary
    // same-transaction sibling remains unacknowledged because the raw request
    // above bypassed SyncClient's exact-reply admission, so it must be offered
    // again by the vector-safe ordinary retry path.
    let hub_frontier = hub_db
        .persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(tenant),
            &edge_node_id,
            edge_db
                .sync_incarnation(&TenantId::from(tenant))
                .expect("read the edge's life"),
        )
        .expect("read the hub's per-edge frontier");
    assert!(
        hub_frontier.is_some(),
        "the hub applied the unit, so its frontier for this edge moved"
    );
    within(edge.pull_default())
        .await
        .expect("a pull and its status confirmation");
    // The same pending owner sends unrelated rows and then a
    // real keep-first conflict; the test-only transport holds its unit back.
    filter_traffic.store(true, Ordering::SeqCst);
    let traffic_id = Uuid::new_v4();
    edge_db
        .execute(
            "INSERT INTO unrelated_rows (id, body) VALUES ($id, 'first')",
            &HashMap::from([("id".into(), Value::Uuid(traffic_id))]),
        )
        .unwrap();
    within(edge.push()).await.unwrap();
    let after_unrelated_receipt = edge_db
        .delivery_status(ROOT_TABLE)
        .expect("the ordinary receipt leaves pending unchanged");
    assert_eq!(
        after_unrelated_receipt.pending, 1,
        "an unrelated ordinary-push receipt never clears the pending unit"
    );
    edge_db
        .execute(
            "UPDATE unrelated_rows SET body='different' WHERE id=$id",
            &HashMap::from([("id".into(), Value::Uuid(traffic_id))]),
        )
        .unwrap();
    let conflict = within(edge.push()).await.unwrap();
    assert!(
        !conflict.conflicts.is_empty(),
        "ordinary keep-first traffic has a real refusal"
    );
    let after_conflict_receipt = edge_db
        .delivery_status(ROOT_TABLE)
        .expect("the conflict receipt leaves pending unchanged");
    assert_eq!(
        after_conflict_receipt.pending, 1,
        "a keep-first conflict receipt never clears the pending unit"
    );
    let observed_pushes = observed_pushes
        .lock()
        .expect("outgoing-push observation lock")
        .clone();
    assert_eq!(
        observed_pushes.len(),
        3,
        "the pending owner re-offered the unacknowledged sibling and sent both later ordinary pushes"
    );
    assert!(
        observed_pushes.iter().all(|push| {
            push.sender_node_id == edge_node_id
                && push.incarnation
                    == edge_db
                        .sync_incarnation(&TenantId::from(tenant))
                        .expect("read the pending owner's life")
                && push.rows.iter().all(|(table, _)| table == "unrelated_rows")
        }),
        "all observed outgoing payloads belong to the pending owner and omit its unit"
    );
    let observed_bodies = observed_pushes
        .iter()
        .flat_map(|push| push.rows.iter().filter_map(|(_, body)| body.clone()))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        observed_bodies,
        BTreeSet::from([
            "different".to_string(),
            "first".to_string(),
            "same transaction sibling".to_string(),
        ]),
        "the same pending owner retried the unacknowledged sibling, then sent the later write and conflict"
    );
    let after_traffic = edge_db
        .delivery_status(ROOT_TABLE)
        .expect("read the status again");
    assert_eq!(
        after_traffic.pending, 1,
        "a push watermark, a pull and a status probe never move a unit out of pending"
    );
    assert_eq!(after_traffic.accepted, 1);
    assert_eq!(after_traffic.equivalent, 1);
    assert_eq!(after_traffic.refused, 1);

    // Only the durable verdict moves it.
    within(edge.fetch_delivery_outcomes(None))
        .await
        .expect("recover the missing verdict");
    let after_recovery = edge_db
        .delivery_status(ROOT_TABLE)
        .expect("read the status after recovery");
    assert_eq!(
        after_recovery.pending, 0,
        "only a durable verdict from the registered hub moves a unit out of pending"
    );
    assert_eq!(after_recovery.accepted, 2);

    // A table the tenant switched off has no hub side at all.
    let offline_root = Uuid::from_u128(0x5401_0000_0000_0000_0000_0000_0000_0051);
    let tx = stage_unit_on(
        &edge_db,
        OFFLINE_ROOT_TABLE,
        "offline_record_parts",
        offline_root,
        "root-body",
        &[],
    )
    .unwrap();
    edge.__stage_delivery_manifest_for_test(
        tx,
        DeliveryManifest {
            root_table: OFFLINE_ROOT_TABLE,
            root_key: root_key(offline_root),
            members: vec![],
        },
    )
    .expect("the switched-off root has an actual committed manifest");
    edge_db.commit(tx).unwrap();
    let offline_status = edge_db
        .delivery_status(OFFLINE_ROOT_TABLE)
        .expect("read the switched-off table's status");
    assert!(
        offline_status.disabled,
        "a table whose policy is switched off reports itself so"
    );
    assert_eq!(
        (
            offline_status.eligible,
            offline_status.accepted,
            offline_status.equivalent,
            offline_status.refused,
            offline_status.pending
        ),
        (0, 0, 0, 0, 0),
        "a switched-off table has no units to deliver, so every count is zero"
    );

    drop(edge);
    drop(winner_edge);
    let _ = hub.stop().await;
    scoped_custody_reads_follow_visible_roots().await;
    mutable_manifest_rows_do_not_block_other_units().await;
}

// Per-root reads and counts honor context, label and principal together.
async fn scoped_custody_reads_follow_visible_roots() {
    use contextdb_core::{ContextId, Principal, ScopeLabel};
    let tenant = "scoped-custody-status";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    let edge_db = Arc::new(Database::open_memory());
    let context = Uuid::from_u128(0x5410);
    let foreign_context = Uuid::from_u128(0x5411);
    let granted_acl = Uuid::from_u128(0x5412);
    let denied_acl = Uuid::from_u128(0x5413);
    let root_ddl = ROOT_DDL.replace(
        "body TEXT",
        "body TEXT, context_id UUID CONTEXT_ID, scope TEXT SCOPE_LABEL_READ ('visible', 'hidden') WRITE ('visible', 'hidden'), acl_id UUID ACL REFERENCES custody_grants(acl_id)",
    );
    for db in [&hub_db, &edge_db] {
        db.execute("CREATE TABLE custody_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID) SYNC OFF", &p()).unwrap();
        db.execute(&root_ddl, &p()).unwrap();
        db.execute(MEMBER_DDL, &p()).unwrap();
        db.execute(UNRELATED_DDL, &p()).unwrap();
        db.execute("INSERT INTO custody_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, 'Agent', 'reader', $acl)", &HashMap::from([
            ("id".into(), Value::Uuid(Uuid::from_u128(0x5414))),
            ("acl".into(), Value::Uuid(granted_acl)),
        ])).unwrap();
    }
    let hub = start_hub(&broker, tenant, hub_db).await;
    let (edge, _) = edge_client(&edge_db, &broker, tenant);
    within(edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .unwrap();
    let roots = (0..5)
        .map(|i| Uuid::from_u128(0x5420 + i))
        .collect::<Vec<_>>();
    for (i, root) in roots.iter().enumerate() {
        let tx = edge_db.begin().unwrap();
        edge_db
            .insert_row(
                tx,
                ROOT_TABLE,
                HashMap::from([
                    ("id".into(), Value::Uuid(*root)),
                    ("body".into(), Value::Text(format!("scoped-root-{i}"))),
                    (
                        "context_id".into(),
                        Value::Uuid(if i == 1 { foreign_context } else { context }),
                    ),
                    (
                        "scope".into(),
                        Value::Text(if i == 2 { "hidden" } else { "visible" }.into()),
                    ),
                    (
                        "acl_id".into(),
                        Value::Uuid(if i == 3 { denied_acl } else { granted_acl }),
                    ),
                ]),
            )
            .unwrap();
        seed_committed_manifest(&edge, tx, *root, &[]);
        edge_db.commit(tx).unwrap();
        if i < 2 {
            within(edge.__seed_delivery_outcome_for_test(
                ROOT_TABLE,
                &root_key(*root),
                DeliveryOutcomeKind::Accepted,
                None,
            ))
            .await
            .unwrap();
        }
    }
    let scoped = edge_db.scoped_with_constraints(
        Some(BTreeSet::from([ContextId::new(context)])),
        Some(BTreeSet::from([ScopeLabel::new("visible")])),
        Some(Principal::Agent("reader".into())),
    );
    let rows = scoped
        .execute("SELECT id FROM records ORDER BY id", &p())
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![Value::Uuid(roots[0])], vec![Value::Uuid(roots[4])]],
        "the ordinary row gate independently selects the two visible roots"
    );
    let status = scoped.delivery_status(ROOT_TABLE).unwrap();
    assert_eq!(
        (
            status.eligible,
            status.accepted,
            status.equivalent,
            status.refused,
            status.pending
        ),
        (2, 1, 0, 0, 1),
        "scoped metadata counts no hidden root or hidden terminal credit"
    );
    assert_eq!(
        scoped
            .delivery_outcome(ROOT_TABLE, &root_key(roots[0]))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted
    );
    for root in &roots[1..] {
        assert!(
            scoped
                .delivery_outcome(ROOT_TABLE, &root_key(*root))
                .unwrap()
                .is_none(),
            "hidden or pending roots have no readable terminal"
        );
    }
    assert_eq!(
        edge_db.delivery_status(ROOT_TABLE).unwrap().eligible,
        5,
        "scoped inspection leaves administrative accounting intact"
    );
    drop(edge);
    let _ = hub.stop().await;
}

// Old-backup restore and rebind revoke stale credit automatically without changing edge rows.
#[tokio::test]
async fn restoring_an_old_hub_image_and_rebinding_revoke_credit_without_changing_edge_rows() {
    let directory = tempfile::tempdir().unwrap();
    let tenant = "automatic-old-backup";
    let broker = InProcessBroker::new();
    let hub_db = file_backed(directory.path(), "hub.db");
    create_core_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db.clone()).await;
    let edge_db = file_backed(directory.path(), "edge.db");
    let (edge, _) = edge_client(&edge_db, &broker, tenant);
    within(edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .unwrap();
    create_core_tables(&edge_db);
    let present = Uuid::new_v4();
    let absent = Uuid::new_v4();
    let old = directory.path().join("old.db");
    for id in [present, absent] {
        let member = Uuid::new_v4();
        let tx = stage_unit(&edge_db, id, "root value", &[(member, "member value")]).unwrap();
        seed_committed_manifest(&edge, tx, id, &[member]);
        edge_db.commit(tx).unwrap();
        within(edge.push()).await.unwrap();
        if id == present {
            hub_db.export_snapshot(&old).unwrap();
        }
    }
    let before = whole_unit_rows(&edge_db);
    let previous_life = hub_db.sync_incarnation(&TenantId::from(tenant)).unwrap();
    let (closed, identity) = hub.stop().await;
    drop(closed);
    drop(hub_db);
    let restored_db = Arc::new(Database::open(&old).unwrap());
    assert_eq!(
        row_count(&restored_db, ROOT_TABLE),
        1,
        "the backup predates the second unit"
    );
    let restored = start_hub_with_identity(&broker, tenant, restored_db.clone(), identity).await;

    within(edge.pull_default())
        .await
        .expect("an ordinary exchange automatically resolves the restored hub");
    let pending = edge_db.delivery_status(ROOT_TABLE).unwrap();
    assert_eq!(
        (
            pending.eligible,
            pending.pending,
            pending.accepted,
            pending.equivalent,
            pending.refused
        ),
        (2, 2, 0, 0, 0),
        "every receipt from the previous hub image loses credit before reoffer"
    );
    assert_eq!(whole_unit_rows(&edge_db), before);
    // SHOW also revokes expired credit on both nodes.
    for db in [&edge_db, &restored_db] {
        assert!(
            db.execute("SHOW DELIVERY OUTCOMES FOR records", &p())
                .unwrap()
                .rows
                .is_empty(),
            "restored authority leaves no current SHOW terminal before reoffer"
        );
    }
    assert!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(present))
            .unwrap()
            .is_none()
    );
    assert!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(absent))
            .unwrap()
            .is_none()
    );
    // Inspecting the binding after restore shows the new authority.
    let bindings = edge_db.execute("SHOW SYNC BINDINGS", &p()).unwrap();
    let life_column = bindings
        .columns
        .iter()
        .position(|c| c == "hub_incarnation")
        .unwrap();
    let current_life = restored_db
        .sync_incarnation(&TenantId::from(tenant))
        .unwrap();
    assert_eq!(bindings.rows.len(), 2);
    assert!(
        bindings
            .rows
            .iter()
            .all(|row| row[life_column] == Value::Text(current_life.to_hex()))
    );

    // A failed bind after restore is still the public clause mismatch, without a policy write.
    let positions = (restored_db.current_lsn(), edge_db.current_lsn());
    let wrong = ApplicationTablePolicyExpectation::new()
        .expect_table(
            ROOT_TABLE,
            "SYNC PUSH ONLY SYNC CONFLICT KEEP LATEST DELIVERY MANIFEST OVER record_parts",
        )
        .unwrap();
    let mismatch = within(edge.bind_application_table_policy(wrong)).await;
    assert!(
        matches!(&mismatch, Err(contextdb_core::Error::TenantPolicyMismatch { table, clause })
        if table == ROOT_TABLE && clause == "conflict"),
        "restored hub compares public policy clauses: {mismatch:?}"
    );
    assert_eq!(
        (restored_db.current_lsn(), edge_db.current_lsn()),
        positions
    );

    within(edge.push()).await.unwrap();
    let equivalent = edge_db
        .delivery_outcome(ROOT_TABLE, &root_key(present))
        .unwrap()
        .unwrap();
    let accepted = edge_db
        .delivery_outcome(ROOT_TABLE, &root_key(absent))
        .unwrap()
        .unwrap();
    assert_eq!(equivalent.kind(), DeliveryOutcomeKind::Equivalent);
    assert_eq!(accepted.kind(), DeliveryOutcomeKind::Accepted);
    assert_ne!(accepted.hub_incarnation(), previous_life);
    assert_eq!(accepted.hub_incarnation(), equivalent.hub_incarnation());
    assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().pending, 0);
    assert_eq!(whole_unit_rows(&edge_db), before);

    let (closed, _) = restored.stop().await;
    drop(closed);
    let other_db = file_backed(directory.path(), "other.db");
    create_core_tables(&other_db);
    let other = start_hub(&broker, tenant, other_db).await;
    // Changing the destination must be followed by the actual authenticated bind.
    edge.change_destination(&other.node_id).unwrap();
    let rebound = within(edge.bind_application_table_policy(core_expectation()))
        .await
        .expect("the edge publicly binds its existing tables to the different hub");
    assert_eq!(rebound.hub_node_id(), other.node_id);
    let pending = edge_db.delivery_status(ROOT_TABLE).unwrap();
    assert_eq!(
        (
            pending.eligible,
            pending.pending,
            pending.accepted,
            pending.equivalent,
            pending.refused
        ),
        (2, 2, 0, 0, 0)
    );
    assert_eq!(whole_unit_rows(&edge_db), before);
    within(edge.shutdown()).await;
    // The new binding and pending status survive restart without altering any row.
    drop(edge);
    drop(edge_db);
    let reopened = file_backed(directory.path(), "edge.db");
    assert_eq!(
        reopened.retention_sync_peer().as_deref(),
        Some(other.node_id.as_str())
    );
    let bindings = reopened.execute("SHOW SYNC BINDINGS", &p()).unwrap();
    let hub_column = bindings
        .columns
        .iter()
        .position(|name| name == "hub_node_id")
        .unwrap();
    assert_eq!(bindings.rows.len(), 2);
    assert!(
        bindings
            .rows
            .iter()
            .all(|row| row[hub_column] == Value::Text(other.node_id.clone()))
    );
    let pending = reopened.delivery_status(ROOT_TABLE).unwrap();
    assert_eq!(
        (
            pending.eligible,
            pending.pending,
            pending.accepted,
            pending.equivalent,
            pending.refused
        ),
        (2, 2, 0, 0, 0)
    );
    assert_eq!(whole_unit_rows(&reopened), before);
    let _ = other.stop().await;
    restore_verification_is_bounded_and_covers_purge_gaps().await;
    declared_snapshot_before_first_binding_recovers().await;
}

// Refusal ends resend and leaves the losing rows locatable by the diagnostic key.
#[tokio::test]
async fn a_refused_unit_is_terminal_the_resend_obligation_ends_status_is_clean_and_the_rows_stay_locatable()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let tenant = "refusal-is-terminal";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    create_core_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db.clone()).await;

    let root_id = Uuid::from_u128(0x5601_0000_0000_0000_0000_0000_0000_0001);
    let member_id = Uuid::from_u128(0x5601_0000_0000_0000_0000_0000_0000_0002);

    let winner_db = Arc::new(Database::open_memory());
    let (winner_edge, _winner_node_id) = edge_client(&winner_db, &broker, tenant);
    within(winner_edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind the winner");
    create_core_tables(&winner_db);
    let tx = stage_unit(
        &winner_db,
        root_id,
        "root-body",
        &[(member_id, "winning-member")],
    )
    .expect("the winner commits first");
    seed_committed_manifest(&winner_edge, tx, root_id, &[member_id]);
    winner_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    within(winner_edge.__seed_delivery_outcome_for_test(
        ROOT_TABLE,
        &root_key(root_id),
        DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .unwrap();

    let loser_db = Arc::new(Database::open_memory());
    let (loser_edge, _loser_node_id) = edge_client(&loser_db, &broker, tenant);
    within(loser_edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .expect("bind the loser");
    create_core_tables(&loser_db);
    let tx = stage_unit(
        &loser_db,
        root_id,
        "root-body",
        &[(member_id, "losing-member")],
    )
    .expect("the loser commits differing content under the same key");
    seed_committed_manifest(&loser_edge, tx, root_id, &[member_id]);
    loser_db
        .commit(tx)
        .expect("commit rows and delivery metadata atomically");

    let applied = within(loser_edge.push())
        .await
        .expect("a refusal is an answer, not a transport failure");
    let outcome = loser_db
        .delivery_outcome(ROOT_TABLE, &root_key(root_id))
        .expect("the refused unit has a durable verdict");
    assert_eq!(outcome.kind(), DeliveryOutcomeKind::Refused);
    assert_eq!(outcome.cause(), Some("unit_digest_mismatch"));

    // The resend obligation ends.
    assert!(
        !loser_edge
            .has_pending_push_changes()
            .expect("read the pending frontier"),
        "a refused unit is never offered again"
    );
    assert_eq!(
        loser_edge
            .pending_push_change_count()
            .expect("count the pending frontier"),
        0
    );
    let mark = broker.recorded_exchanges().len();
    within(loser_edge.push())
        .await
        .expect("a second push has nothing to send");
    assert!(
        echoed_outcomes(&broker, mark).is_empty(),
        "the refused unit is not re-offered on the next sync"
    );

    // Status is clean.
    let status = loser_db
        .delivery_status(ROOT_TABLE)
        .expect("read the loser's status");
    assert_eq!(status.eligible, 1);
    assert_eq!(status.refused, 1);
    assert_eq!(
        status.pending, 0,
        "a refusal is terminal, so nothing is left waiting"
    );

    // The rows stay locatable by the diagnostic's table and key.
    let root_conflict = applied
        .conflicts
        .iter()
        .find(|conflict| conflict.table.as_deref() == Some(ROOT_TABLE))
        .expect("the diagnostic names the refused root");
    let key_values = root_conflict.natural_key.key_values();
    assert_eq!(key_values, vec![Value::Uuid(root_id)]);
    let located = loser_db
        .execute(
            "SELECT body FROM records WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(root_id))]),
        )
        .expect("the refused root is still readable on the edge");
    assert_eq!(located.rows.len(), 1);
    assert_eq!(
        bodies(&loser_db, MEMBER_TABLE),
        BTreeSet::from(["losing-member".to_string()]),
        "the refused unit's member rows stay on the edge too"
    );
    assert_eq!(
        bodies(&hub_db, MEMBER_TABLE),
        BTreeSet::from(["winning-member".to_string()]),
        "and nothing of the refused unit reached the hub"
    );

    drop(winner_edge);
    drop(loser_edge);
    let _ = hub.stop().await;
}

fn whole_unit_rows(db: &Database) -> Vec<Vec<Vec<Value>>> {
    [ROOT_TABLE, MEMBER_TABLE]
        .into_iter()
        .map(|table| {
            db.execute(&format!("SELECT * FROM {table} ORDER BY id"), &p())
                .unwrap()
                .rows
        })
        .collect()
}

async fn complete_outcome_readback() {
    let tenant = "complete-outcome-readback";
    let broker = InProcessBroker::new();
    let hub_db = Arc::new(Database::open_memory());
    create_core_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db.clone()).await;
    let db = Arc::new(Database::open_memory());
    let (client, _) = edge_client(&db, &broker, tenant);
    within(client.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .unwrap();
    create_core_tables(&db);
    // One source transaction and one actual hub commit place many outcomes at the same position.
    let roots: Vec<_> = (0..503).map(|_| Uuid::new_v4()).collect();
    let tx = db.begin().unwrap();
    for root in &roots {
        db.insert_row(
            tx,
            ROOT_TABLE,
            HashMap::from([
                ("id".into(), Value::Uuid(*root)),
                ("body".into(), Value::Text("complete cohort".into())),
            ]),
        )
        .unwrap();
        seed_committed_manifest(&client, tx, *root, &[]);
    }
    db.commit(tx).unwrap();
    assert_eq!(
        within(client.__seed_hub_delivery_batch_for_test(ROOT_TABLE))
            .await
            .unwrap(),
        roots.len()
    );
    let before = (hub_db.current_lsn(), whole_unit_rows(&hub_db));
    let fetched = within(client.fetch_delivery_outcomes(None))
        .await
        .expect("fetch returns the entire cohort");
    assert_eq!(
        served_roots(&fetched),
        roots.iter().map(|id| encoded_key(*id)).collect()
    );
    assert_eq!(
        fetched.len(),
        roots.len(),
        "no truncation or duplication at a shared acceptance position"
    );
    assert!(
        fetched
            .windows(2)
            .all(|pair| pair[0].hub_acceptance_position() <= pair[1].hub_acceptance_position())
    );
    // Prove the cohort really shares an acceptance position.
    assert!(
        fetched.iter().all(
            |outcome| outcome.hub_acceptance_position() == fetched[0].hub_acceptance_position()
        )
    );
    // Resume uses the current opaque continuation adapter, without asserting its encoding.
    // Every later answer must remain readable, even at the same acceptance position.
    let tail = within(client.fetch_delivery_outcomes(Some(fetched[0].cursor())))
        .await
        .unwrap();
    assert_eq!(served_roots(&tail), served_roots(&fetched[1..]));
    assert!(
        within(client.fetch_delivery_outcomes(Some(fetched.last().unwrap().cursor())))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        (hub_db.current_lsn(), whole_unit_rows(&hub_db)),
        before,
        "readback never reapplies rows"
    );
    assert_eq!(
        db.delivery_status(ROOT_TABLE).unwrap().accepted,
        roots.len() as u64
    );
    within(client.shutdown()).await;
    let _ = hub.stop().await;
}

#[derive(Clone, Debug)]
struct RestoreProbeObservation {
    count: usize,
    bytes: usize,
}

// Observe bounded continuation on the real status transport, without a new request.
struct ObserveRestoreStatus {
    pulls: Arc<std::sync::atomic::AtomicUsize>,
    inner: Arc<dyn ClientTransport>,
    observed: Arc<Mutex<Vec<RestoreProbeObservation>>>,
    fail: Arc<AtomicBool>,
}
impl ObserveRestoreStatus {
    // Adapter: vary only the order of genuine signed ordinary
    // push outcomes; identity, whole-unit digests and public targets are unchanged.
    fn outcome_order(bytes: Vec<u8>) -> Vec<u8> {
        if let Ok(envelope) = decode(&bytes)
            && envelope.message_type == MessageType::PushResponse
        {
            let mut reply: PushResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
            reply.outcomes.reverse();
            return encode(MessageType::PushResponse, &reply).unwrap();
        }
        bytes
    }
    fn observe(&self, bytes: Vec<u8>) -> Vec<u8> {
        if decode(&bytes).is_ok_and(|e| e.message_type == MessageType::PullRequest) {
            self.pulls.fetch_add(1, Ordering::SeqCst);
        }
        if let Ok(envelope) = decode(&bytes)
            && matches!(envelope.message_type, MessageType::StatusRequest)
        {
            #[derive(serde::Deserialize)]
            struct Probe {
                #[serde(rename = "incarnation")]
                _incarnation: contextdb_core::Incarnation,
                #[serde(default)]
                checkpoint: Option<serde::de::IgnoredAny>,
            }
            let probe: Probe = rmp_serde::from_slice(&envelope.payload).unwrap();
            self.observed.lock().unwrap().push(RestoreProbeObservation {
                count: usize::from(probe.checkpoint.is_some()),
                bytes: bytes.len(),
            });
        }
        bytes
    }
}
impl ClientTransport for ObserveRestoreStatus {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let request_bytes = self.observe(request_bytes);
        Box::pin(async move {
            self.inner
                .request(subject, request_bytes, timeout)
                .await
                .map(Self::outcome_order)
        })
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let is_status = decode(&request_bytes)
            .is_ok_and(|e| matches!(e.message_type, MessageType::StatusRequest));
        let request_bytes = self.observe(request_bytes);
        if is_status && self.fail.load(Ordering::SeqCst) {
            return Box::pin(async { Ok(vec![0]) });
        }
        Box::pin(async move {
            self.inner
                .request_single_reply(subject, request_bytes, timeout)
                .await
                .map(Self::outcome_order)
        })
    }
}

// All recent receipts can be legitimately purged while an older receipt was lost.
async fn restore_verification_is_bounded_and_covers_purge_gaps() {
    let root = tempfile::tempdir().unwrap();
    let tenant = "restore-purge-gaps";
    let broker = InProcessBroker::new();
    let db = file_backed(root.path(), "hub.db");
    create_core_tables(&db);
    let hub = start_hub(&broker, tenant, db.clone()).await;
    let edge_db = file_backed(root.path(), "edge.db");
    let identity = Arc::new(FabricIdentity::generate());
    let observed = Arc::new(Mutex::new(Vec::new()));
    let fail = Arc::new(AtomicBool::new(false));
    let pulls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let transport = Arc::new(ObserveRestoreStatus {
        pulls: pulls.clone(),
        inner: broker.client_as(&identity.node_id()),
        observed: observed.clone(),
        fail: fail.clone(),
    });
    let edge = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        transport,
        TenantId::from(tenant),
        identity,
    );
    within(edge.__seed_application_table_policy_binding_for_test(core_expectation()))
        .await
        .unwrap();
    create_core_tables(&edge_db);
    let ids = (0..72).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
    let old = root.path().join("old.db");
    let first = stage_unit(&edge_db, ids[0], "retained", &[]).unwrap();
    seed_committed_manifest(&edge, first, ids[0], &[]);
    edge_db.commit(first).unwrap();
    within(edge.push()).await.unwrap();
    db.export_snapshot(&old).unwrap();
    let tx = edge_db.begin().unwrap();
    for (i, id) in ids.iter().enumerate().skip(1) {
        edge_db
            .insert_row(
                tx,
                ROOT_TABLE,
                HashMap::from([
                    ("id".into(), Value::Uuid(*id)),
                    (
                        "body".into(),
                        Value::Text(if i < 2 { "retained" } else { "recent" }.into()),
                    ),
                ]),
            )
            .unwrap();
        seed_committed_manifest(&edge, tx, *id, &[]);
    }
    edge_db.commit(tx).unwrap();
    within(edge.push()).await.unwrap();
    within(edge.fetch_delivery_outcomes(None)).await.unwrap();
    eprintln!("arranged all 72 receipts, including the one absent from the old image");
    let before = whole_unit_rows(&edge_db);
    let incarnation = db.sync_incarnation(&TenantId::from(tenant)).unwrap();
    fail.store(true, Ordering::SeqCst);
    assert!(
        within(edge.pull_default()).await.is_err(),
        "failed receipt verification cannot be silently treated as success"
    );
    fail.store(false, Ordering::SeqCst);
    observed.lock().unwrap().clear();
    within(edge.push()).await.unwrap();
    assert_eq!(
        db.sync_incarnation(&TenantId::from(tenant)).unwrap(),
        incarnation,
        "the current committed image keeps its incarnation"
    );
    assert_eq!(whole_unit_rows(&edge_db), before);
    let probes = observed.lock().unwrap().clone();
    assert_eq!(
        probes.len(),
        1,
        "one status exchange irrespective of held history: {probes:?}"
    );
    assert!(
        probes.iter().all(|p| p.count == 1 && p.bytes < 2048),
        "one signed prefix rather than a receipt replay: {probes:?}"
    );
    eprintln!("bounded status after 72 outcomes: {probes:?}");
    // Empty pulls retain the public cursor and need one actual
    // pull exchange plus the signed status exchange, including a warm hidden tail.
    within(edge.pull_default()).await.unwrap();
    let public_cursor = edge.pull_watermark();
    let pull_start = pulls.load(Ordering::SeqCst);
    observed.lock().unwrap().clear();
    for _ in 0..3 {
        within(edge.pull_default()).await.unwrap();
    }
    assert_eq!(pulls.load(Ordering::SeqCst) - pull_start, 3);
    assert_eq!(observed.lock().unwrap().len(), 3);
    assert_eq!(
        edge.pull_watermark(),
        public_cursor,
        "hidden custody rows are not public progress"
    );
    assert_eq!(
        db.sync_incarnation(&TenantId::from(tenant)).unwrap(),
        incarnation
    );
    // Ordinary work after the hidden prefix remains reachable across a declaration change.
    db.execute(
        "CREATE TABLE after_hidden (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY",
        &p(),
    )
    .unwrap();
    within(edge.pull_default()).await.unwrap();
    within(edge.pull_default()).await.unwrap();
    db.execute("ALTER TABLE after_hidden SET SYNC TWO WAY", &p())
        .unwrap();
    db.execute(
        "INSERT INTO after_hidden VALUES (1,'reachable after hidden custody history')",
        &p(),
    )
    .unwrap();
    within(edge.pull_default()).await.unwrap();
    assert_eq!(
        edge_db
            .execute("SELECT body FROM after_hidden WHERE id=1", &p())
            .unwrap()
            .rows,
        vec![vec![Value::Text(
            "reachable after hidden custody history".into()
        )]]
    );
    let previous_position = db.current_lsn();
    let (closed, identity) = hub.stop().await;
    drop(closed);
    drop(db);
    let restored_db = Arc::new(Database::open(&old).unwrap());
    let restored = start_hub_with_identity(&broker, tenant, restored_db.clone(), identity).await;
    // Recreate then actually purge only the newest units on the restored hub.
    // The edge retains its older signed receipts until the ordinary status exchange below.
    let recent = ids
        .iter()
        .skip(2)
        .map(|id| root_key(*id))
        .collect::<Vec<_>>();
    assert_eq!(
        within(edge.__seed_hub_delivery_roots_for_test(ROOT_TABLE, Some(&recent)))
            .await
            .unwrap(),
        70
    );
    // Catch up even the custody count on a different branch;
    // only a committed-prefix proof distinguishes it from the lost image.
    let extra = Uuid::new_v4();
    let tx = stage_unit(&edge_db, extra, "recent", &[]).unwrap();
    seed_committed_manifest(&edge, tx, extra, &[]);
    edge_db.commit(tx).unwrap();
    within(edge.__seed_hub_delivery_outcome_for_test(
        ROOT_TABLE,
        &root_key(extra),
        DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .unwrap();
    eprintln!("restored image has all recent receipts, but the second retained receipt is lost");
    restored_db
        .execute("PURGE FROM records WHERE body = 'recent'", &p())
        .unwrap();
    while restored_db.current_lsn() <= previous_position {
        restored_db
            .execute(
                "INSERT INTO unrelated_rows VALUES ($id, 'unrelated')",
                &HashMap::from([("id".into(), Value::Uuid(Uuid::new_v4()))]),
            )
            .unwrap();
    }
    observed.lock().unwrap().clear();
    within(edge.pull_default()).await.unwrap();
    assert_ne!(
        restored_db
            .sync_incarnation(&TenantId::from(tenant))
            .unwrap(),
        incarnation,
        "purged recent receipts and later unrelated writes cannot mask the older lost receipt"
    );
    let status = edge_db.delivery_status(ROOT_TABLE).unwrap();
    assert_eq!(
        (
            status.eligible,
            status.pending,
            status.accepted,
            status.equivalent,
            status.refused
        ),
        (2, 2, 0, 0, 0)
    );
    assert_eq!(
        row_count(&edge_db, ROOT_TABLE),
        2,
        "restore revokes credit; only the explicit delivered purge removes rows"
    );
    for id in ids.iter().take(2) {
        assert_eq!(
            edge_db
                .execute(
                    "SELECT body FROM records WHERE id = $id",
                    &HashMap::from([("id".into(), Value::Uuid(*id))])
                )
                .unwrap()
                .rows,
            vec![vec![Value::Text("retained".into())]]
        );
    }
    within(edge.push()).await.unwrap();
    assert_eq!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(ids[0]))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Equivalent
    );
    assert_eq!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(ids[1]))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted
    );
    within(edge.shutdown()).await;
    restored.stop().await;
}

// Mutable rows use ordinary typed refusal and delete
// arbitration, while current manifested siblings and unrelated tables progress.
async fn mutable_manifest_rows_do_not_block_other_units() {
    let directory = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let tenant = "mutable-manifest-rows";
    let hub_db = file_backed(directory.path(), "mutable-hub.db");
    create_core_tables(&hub_db);
    let hub = start_hub(&broker, tenant, hub_db.clone()).await;
    let edge_db = file_backed(directory.path(), "mutable-edge.db");
    let (edge, _) = edge_client(&edge_db, &broker, tenant);
    within(edge.bind_application_table_policy(core_expectation()))
        .await
        .unwrap();
    create_core_tables(&edge_db);
    let changed = Uuid::new_v4();
    let deleted = Uuid::new_v4();
    let sibling = Uuid::new_v4();
    for id in [changed, sibling] {
        let tx = stage_unit(&edge_db, id, "original", &[]).unwrap();
        edge_db
            .register_delivery_manifest(
                tx,
                DeliveryManifest {
                    root_table: ROOT_TABLE,
                    root_key: root_key(id),
                    members: vec![],
                },
            )
            .unwrap();
        edge_db.commit(tx).unwrap();
    }
    assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().pending, 2);
    edge_db
        .execute(
            "UPDATE records SET body = 'replacement' WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(changed))]),
        )
        .unwrap();
    // Keep both before-fix failures observable within this existing proof case.
    let updated_push = within(edge.push()).await;
    let tx = stage_unit(&edge_db, deleted, "never offered", &[]).unwrap();
    edge_db
        .register_delivery_manifest(
            tx,
            DeliveryManifest {
                root_table: ROOT_TABLE,
                root_key: root_key(deleted),
                members: vec![],
            },
        )
        .unwrap();
    edge_db.commit(tx).unwrap();
    edge_db
        .execute(
            "DELETE FROM records WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(deleted))]),
        )
        .unwrap();
    let unrelated = Uuid::new_v4();
    edge_db
        .execute(
            "INSERT INTO unrelated_rows (id, body) VALUES ($id, 'independent')",
            &HashMap::from([("id".into(), Value::Uuid(unrelated))]),
        )
        .unwrap();
    let pending_after_delete = edge.pending_push_change_count();
    let deleted_push = within(edge.push()).await;
    assert!(
        updated_push.is_ok() && deleted_push.is_ok() && pending_after_delete.is_ok(),
        "ordinary mutations cannot wedge push: UPDATE={updated_push:?}; DELETE={deleted_push:?}; pending={pending_after_delete:?}"
    );
    assert_eq!(row_count(&hub_db, "unrelated_rows"), 1);
    assert!(
        hub_db
            .point_lookup(ROOT_TABLE, "id", &Value::Uuid(changed), hub_db.snapshot())
            .unwrap()
            .is_none()
    );
    assert!(
        edge_db
            .point_lookup(ROOT_TABLE, "id", &Value::Uuid(deleted), edge_db.snapshot())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(sibling))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted
    );
    let fetched = within(edge.fetch_delivery_outcomes(None)).await.unwrap();
    assert!(fetched.iter().any(|o| o.root_key() == &root_key(changed)
        && o.kind() == DeliveryOutcomeKind::Refused
        && o.cause() == Some("manifest_required")));
    let counts = edge_db.delivery_status(ROOT_TABLE).unwrap();
    assert_eq!(
        (
            counts.eligible,
            counts.accepted,
            counts.pending,
            counts.refused
        ),
        (1, 1, 0, 0)
    );
    assert_eq!(edge.pending_push_change_count().unwrap(), 0);

    // Register the replacement through the writing transaction.
    let tx = edge_db.begin().unwrap();
    edge_db
        .execute_in_tx(
            tx,
            "UPDATE records SET body = 'registered replacement' WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(changed))]),
        )
        .unwrap();
    edge_db
        .register_delivery_manifest(
            tx,
            DeliveryManifest {
                root_table: ROOT_TABLE,
                root_key: root_key(changed),
                members: vec![],
            },
        )
        .unwrap();
    edge_db.commit(tx).unwrap();
    assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().pending, 1);
    within(edge.push()).await.unwrap();
    assert_eq!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(changed))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted
    );
    // An old terminal must not suppress the established
    // authenticated fresh-creator delete continuation.
    edge_db
        .execute(
            "DELETE FROM records WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(changed))]),
        )
        .unwrap();
    let deletion = within(edge.push()).await.unwrap();
    assert_eq!(deletion.applied_rows, 1);
    assert!(deletion.conflicts.is_empty());
    assert!(
        hub_db
            .point_lookup(ROOT_TABLE, "id", &Value::Uuid(changed), hub_db.snapshot())
            .unwrap()
            .is_none()
    );
    assert!(
        edge_db
            .delivery_outcome(ROOT_TABLE, &root_key(changed))
            .unwrap()
            .is_none()
    );
    let after_delete = edge_db.delivery_status(ROOT_TABLE).unwrap();
    assert_eq!(
        (
            after_delete.eligible,
            after_delete.accepted,
            after_delete.pending,
            after_delete.refused
        ),
        (1, 1, 0, 0),
        "the deleted root is no longer eligible; only the accepted sibling remains"
    );
    assert_eq!(edge.pending_push_change_count().unwrap(), 0);
    assert!(
        !edge_db
            .execute("SHOW DELIVERY OUTCOMES FOR records", &p())
            .unwrap()
            .rows
            .is_empty(),
        "ordinary deletion preserves historical terminal inspection"
    );
    edge.shutdown().await;
    hub.stop().await;
}

// Declarations survive a backup older than every binding; the
// ordinary pull learns the rotated incarnation before any unit is reoffered.
async fn declared_snapshot_before_first_binding_recovers() {
    let directory = tempfile::tempdir().unwrap();
    let tenant = "declared-before-binding";
    let broker = InProcessBroker::new();
    let hub_db = file_backed(directory.path(), "declared-hub.db");
    create_core_tables(&hub_db);
    let identity = Arc::new(FabricIdentity::generate());
    let hub =
        start_hub_without_policy_seeding(&broker, tenant, hub_db.clone(), identity.clone()).await;
    hub_db.execute("DECLARE TENANT TABLE POLICY records SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER record_parts", &p()).unwrap();
    hub_db
        .execute(
            "DECLARE TENANT TABLE POLICY record_parts SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .unwrap();
    let previous = hub_db.sync_incarnation(&TenantId::from(tenant)).unwrap();
    let snapshot = directory.path().join("declared-snapshot.db");
    assert!(
        hub_db
            .execute("SHOW SYNC BINDINGS", &p())
            .unwrap()
            .rows
            .is_empty()
    );
    hub_db.export_snapshot(&snapshot).unwrap();
    let edge_db = file_backed(directory.path(), "declared-edge.db");
    let (edge, _) = edge_client(&edge_db, &broker, tenant);
    within(edge.bind_application_table_policy(core_expectation()))
        .await
        .unwrap();
    create_core_tables(&edge_db);
    let root = Uuid::new_v4();
    let member = Uuid::new_v4();
    let tx = stage_unit(&edge_db, root, "root", &[(member, "member")]).unwrap();
    edge_db
        .register_delivery_manifest(
            tx,
            DeliveryManifest {
                root_table: ROOT_TABLE,
                root_key: root_key(root),
                members: vec![(MEMBER_TABLE, root_key(member))],
            },
        )
        .unwrap();
    edge_db.commit(tx).unwrap();
    within(edge.push()).await.unwrap();
    assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().accepted, 1);
    let before = whole_unit_rows(&edge_db);
    let (closed, _) = hub.stop().await;
    drop(closed);
    drop(hub_db);
    let restored_db = Arc::new(Database::open(&snapshot).unwrap());
    assert!(
        restored_db
            .execute("SHOW SYNC BINDINGS", &p())
            .unwrap()
            .rows
            .is_empty()
    );
    let restored =
        start_hub_without_policy_seeding(&broker, tenant, restored_db.clone(), identity).await;
    within(edge.pull_default())
        .await
        .expect("ordinary pull heals the pre-binding restore");
    let current = restored_db
        .sync_incarnation(&TenantId::from(tenant))
        .unwrap();
    assert_ne!(current, previous);
    let counts = edge_db.delivery_status(ROOT_TABLE).unwrap();
    assert_eq!(
        (
            counts.eligible,
            counts.pending,
            counts.accepted,
            counts.equivalent,
            counts.refused
        ),
        (1, 1, 0, 0, 0)
    );
    assert_eq!(whole_unit_rows(&edge_db), before);
    within(edge.push()).await.unwrap();
    let outcome = edge_db
        .delivery_outcome(ROOT_TABLE, &root_key(root))
        .unwrap()
        .unwrap();
    assert_eq!(outcome.kind(), DeliveryOutcomeKind::Accepted);
    assert_eq!(outcome.hub_incarnation(), current);
    assert_eq!(edge_db.delivery_status(ROOT_TABLE).unwrap().pending, 0);
    assert_eq!(whole_unit_rows(&edge_db), before);
    assert_eq!(whole_unit_rows(&restored_db), before);
    edge.shutdown().await;
    restored.stop().await;
}
