// Canonical authenticated-sync implementation, re-exported by contextdb-server.
use crate::protocol::{
    DependencyCompletePullResponse, MessageType, PullRequest, PullResponse, PushRequest,
    PushResponse, SchemaRecoveryPage, SchemaRecoveryRequest, SyncStatusResponse, WirePurgeChange,
    WirePushError, decode, encode_for_version, row_payload_bytes,
};
use crate::subjects::{pull_subject, push_subject, status_subject};
use crate::sync_client::refuse_keyless_tables_with_no_identity_fallback;
use crate::transfer_receipts::{TransferDirection, TransferLedger, TransferPlane, TransferReceipt};
use crate::transport::{
    HandlerRegistration, IncomingRequest, LineageSigner, RequestHandler, Responder,
    ServerTransport, TransportError,
};
use contextdb_core::{
    AtomicLsn, ColumnType, Incarnation, Lsn, TableMeta, TenantId, VectorSearchMode,
};
use contextdb_engine::sync_types::{
    ChangeSet, DdlChange, DurableSchemaSyncHoldback, NaturalKey, SchemaSyncCapability,
    SchemaSyncHoldback, SyncAdoption, SyncDirection,
};
use contextdb_engine::{Conflict, Database};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio::sync::Semaphore;

/// Max distinct accepted push applies retained by the server at once.
const MAX_IN_FLIGHT_PUSH_APPLIES: usize = 64;
/// Max reply handles joined to one in-flight push apply.
const MAX_REPLIES_PER_IN_FLIGHT_PUSH: usize = 128;
/// Max push applies actively executing blocking engine work at once.
const MAX_CONCURRENT_PUSH_APPLIES: usize = 16;

// Reply fanout belongs to one authenticated peer, even when two peers send
// byte-identical requests. The durable outcome cache has the same ownership.
type PushRequestKey = (String, Vec<u8>);
type InFlightPushApplies = Arc<tokio::sync::Mutex<HashMap<PushRequestKey, Vec<Responder>>>>;
type ApplyTasks = Arc<ApplyTracker>;
type SchemaSyncHoldbackStates = Arc<std::sync::Mutex<HashMap<String, PeerSchemaSyncHoldbackState>>>;

#[derive(Debug, Clone, Default)]
struct PeerSchemaSyncHoldbackState {
    capabilities: BTreeMap<String, BTreeSet<SchemaSyncCapability>>,
    /// Highest source frontier the current receiver has publicly acknowledged
    /// for compatible work. A lower request resets it because a rebuilt store
    /// cannot inherit its predecessor's private progress.
    compatible_through: Lsn,
    /// Highest source frontier inspected while the receiver was old. Recovery
    /// must cover held-table state through this point, but this number must
    /// never make compatible work disappear after a lost response.
    held_through: Lsn,
    /// Cross-call held-table page continuation. This is only a live transport
    /// cursor; a restart safely begins the bounded recovery image again.
    recovery_after: Option<Lsn>,
}

/// The first exact-byte request owns validation and apply. A retry joins that
/// owner before touching the database, so it receives the original outcome
/// instead of validating against the just-committed result as a zero-row replay.
enum PushAdmission {
    Leader,
    Duplicate,
    Rejected,
}

struct ApplyTracker {
    active: AtomicUsize,
    idle: Notify,
}

struct ApplyTaskGuard {
    tracker: ApplyTasks,
}

impl ApplyTracker {
    fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }

    fn start(self: &Arc<Self>) -> ApplyTaskGuard {
        self.active.fetch_add(1, Ordering::SeqCst);
        ApplyTaskGuard {
            tracker: self.clone(),
        }
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for ApplyTaskGuard {
    fn drop(&mut self) {
        if self.tracker.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.tracker.idle.notify_waiters();
        }
    }
}

/// The hub's received-up-to record kept per `(tenant, edge, incarnation)`.
///
/// The per-tenant record is raised by whichever edge pushed last, so it cannot
/// answer "what do you hold from ME?" once more than one edge shares a tenant:
/// a busy edge's progress would confirm a quiet edge's batch that the hub never
/// stored, and a hub restored from an older copy would go unnoticed by every
/// edge but the busiest. Keying by edge alone is still not enough: a
/// wiped-and-recreated edge reusing its node id — its LSNs reset near zero —
/// would be false-confirmed by the prior life's stale-high watermark. So the
/// record is keyed by the edge's per-life incarnation too, stored durably (the
/// engine stores it beside the per-tenant one, so a hub restart or a restored
/// artifact answers from the same position it did before) with an in-memory
/// cache in front of it.
///
/// An edge life with no record here is answered `Lsn(0)` — "I hold nothing from
/// you" — which makes the edge resend or re-upload. A rebuilt edge (fresh
/// incarnation) is such an unknown life by construction; it is never answered
/// with another life's number.
#[derive(Default)]
struct PerEdgeAppliedPushWatermarks {
    cache: std::sync::Mutex<HashMap<(String, Incarnation), Lsn>>,
}

impl PerEdgeAppliedPushWatermarks {
    /// What this hub holds from this life of `node_id`, `Lsn(0)` when it holds
    /// nothing.
    fn load(
        &self,
        db: &Database,
        tenant_id: &TenantId,
        node_id: &str,
        incarnation: Incarnation,
    ) -> Lsn {
        let mut cache = self.cache.lock().unwrap_or_else(|err| err.into_inner());
        Self::load_locked(&mut cache, db, tenant_id, node_id, incarnation)
    }

    /// The engine has already committed this authenticated receipt in the
    /// same transaction as its data. Publishing this cache entry cannot make
    /// status run ahead of durable state.
    fn publish_committed(&self, node_id: &str, incarnation: Incarnation, candidate: Lsn) {
        let mut cache = self.cache.lock().unwrap_or_else(|err| err.into_inner());
        cache
            .entry((node_id.to_string(), incarnation))
            .and_modify(|current| *current = (*current).max(candidate))
            .or_insert(candidate);
    }

    fn load_locked(
        cache: &mut HashMap<(String, Incarnation), Lsn>,
        db: &Database,
        tenant_id: &TenantId,
        node_id: &str,
        incarnation: Incarnation,
    ) -> Lsn {
        if let Some(known) = cache.get(&(node_id.to_string(), incarnation)) {
            return *known;
        }
        let stored = db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                tenant_id,
                node_id,
                incarnation,
            )
            .unwrap_or_else(|err| {
                tracing::warn!(
                    %tenant_id,
                    %node_id,
                    %incarnation,
                    error = %err,
                    "failed to load per-edge applied-push watermark"
                );
                None
            })
            .unwrap_or(Lsn(0));
        cache.insert((node_id.to_string(), incarnation), stored);
        stored
    }
}

struct PushApplyWork {
    db: Arc<Database>,
    local_node_id: Option<String>,
    peer_node_id: Option<String>,
    incarnation: Incarnation,
    dependency_complete: bool,
    receipts: Arc<TransferLedger>,
    request_key: PushRequestKey,
    custody: Option<PushRequest>,
    outcome_key: contextdb_engine::database::SyncPushOutcomeKey,
    changeset: ChangeSet,
    received_ddl: Option<crate::protocol::ReceivedDdlContext>,
    terminal_conflicts: Option<Vec<Conflict>>,
    lineages: Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>,
    arrivals: HashMap<Lsn, Option<Lsn>>,
    tenant_id: TenantId,
    applied_push_watermark: Arc<AtomicLsn>,
    per_edge_watermarks: Arc<PerEdgeAppliedPushWatermarks>,
    apply_tasks: ApplyTasks,
    in_flight_push_applies: InFlightPushApplies,
    apply_permits: Arc<Semaphore>,
    protocol_version: u8,
}

struct PushHandlerState {
    db: Arc<Database>,
    local_node_id: Option<String>,
    receipts: Arc<TransferLedger>,
    tenant_id: TenantId,
    applied_push_watermark: Arc<AtomicLsn>,
    per_edge_watermarks: Arc<PerEdgeAppliedPushWatermarks>,
    apply_tasks: ApplyTasks,
    in_flight_push_applies: InFlightPushApplies,
    apply_permits: Arc<Semaphore>,
}

async fn maybe_wait_for_test_push_barrier(row_count: usize) {
    let Some(min_rows) = std::env::var("CONTEXTDB_TEST_PUSH_BARRIER_MIN_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return;
    };
    if row_count < min_rows {
        return;
    }

    let Some(barrier_path) = std::env::var_os("CONTEXTDB_TEST_PUSH_BARRIER_FILE") else {
        return;
    };
    let Some(release_path) = std::env::var_os("CONTEXTDB_TEST_PUSH_RELEASE_FILE") else {
        return;
    };

    let barrier_path = std::path::PathBuf::from(barrier_path);
    let release_path = std::path::PathBuf::from(release_path);
    let _ = std::fs::write(&barrier_path, b"push-handler-started");
    tokio::task::spawn_blocking(move || std::fs::read(&release_path))
        .await
        .expect("push barrier reader must not panic")
        .expect("push barrier release channel must be readable");
}

pub struct SyncServer {
    db: Arc<Database>,
    transport: Arc<dyn ServerTransport>,
    lineage_signer: Option<LineageSigner>,
    local_node_id: Option<String>,
    tenant_id: TenantId,
    /// Highest edge-LSN applied from pushes for this tenant. `Lsn(0)`
    /// means "no record" at the storage layer. The status surface reports it
    /// as `Some(Lsn(0))` so restored artifacts can still signal regression.
    applied_push_watermark: Arc<AtomicLsn>,
    /// What the hub holds from each authenticated edge — the number the status
    /// exchange answers with, so one edge's progress never confirms another's
    /// batch.
    per_edge_watermarks: Arc<PerEdgeAppliedPushWatermarks>,
    /// Per-peer transfer counters for the sync plane. In memory only.
    receipts: Arc<TransferLedger>,
    /// Tables withheld from an immediately previous receiver, keyed by the
    /// transport-authenticated node that needs the upgrade. This is sender
    /// state: the older parser is never handed vocabulary it cannot read.
    schema_sync_holdbacks: SchemaSyncHoldbackStates,
    peer_schema_capabilities: PeerSchemaCapabilities,
}

impl SyncServer {
    crate::transport::peer_endpoint_available! {
    pub fn new(
        db: Arc<Database>,
        endpoint: &crate::transport::PeerEndpoint,
        tenant_id: TenantId,
    ) -> Self {
        Self::build(
            db,
            endpoint.transport(),
            Some(endpoint.lineage_signer()),
            tenant_id,
            Some(endpoint.node_id()),
        )
    }
    }

    fn build(
        db: Arc<Database>,
        transport: Arc<dyn ServerTransport>,
        lineage_signer: Option<LineageSigner>,
        tenant_id: TenantId,
        local_node_id: Option<String>,
    ) -> Self {
        assert!(
            !tenant_id.as_str().is_empty()
                && tenant_id
                    .as_str()
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '-' || c == '_'),
            "tenant_id must be non-empty and alphanumeric (hyphens and underscores allowed): {tenant_id}"
        );
        // Declarations belong to the served tenant and authenticated hub.
        if let (Some(node), Some(signer)) = (&local_node_id, &lineage_signer) {
            db.set_custody_runtime(tenant_id.clone(), node.clone(), signer.clone());
        }
        db.enable_sync_relay_mode();
        let applied_push_watermark = db
            .persisted_sync_applied_push_watermark(&tenant_id)
            .unwrap_or_else(|err| {
                tracing::warn!(
                    %tenant_id,
                    error = %err,
                    "failed to load persisted applied-push watermark"
                );
                None
            })
            .unwrap_or(Lsn(0));
        let schema_sync_holdbacks = db
            .persisted_inbound_schema_sync_holdbacks(&tenant_id)
            .unwrap_or_else(|err| {
                tracing::warn!(
                    %tenant_id,
                    error = %err,
                    "failed to load persisted schema compatibility holdbacks"
                );
                Vec::new()
            })
            .into_iter()
            .map(|state| {
                (
                    state.peer_node_id,
                    PeerSchemaSyncHoldbackState {
                        capabilities: state.capabilities,
                        compatible_through: state.through,
                        held_through: state.held_through.max(state.through),
                        recovery_after: None,
                    },
                )
            })
            .collect();
        Self {
            db,
            transport,
            lineage_signer,
            local_node_id,
            tenant_id,
            applied_push_watermark: Arc::new(AtomicLsn::new(applied_push_watermark)),
            per_edge_watermarks: Arc::new(PerEdgeAppliedPushWatermarks::default()),
            receipts: Arc::new(TransferLedger::new()),
            schema_sync_holdbacks: Arc::new(std::sync::Mutex::new(schema_sync_holdbacks)),
            peer_schema_capabilities: PeerSchemaCapabilities::default(),
        }
    }

    /// Test-only authenticated transport injection. This is deliberately not
    /// available to normal downstream builds: production server construction
    /// owns its transport and reads ordinary arbitration from declarations.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn with_authenticated_transport_for_test(
        db: Arc<Database>,
        transport: Arc<dyn crate::transport::ServerTransport>,
        tenant_id: TenantId,
    ) -> Self {
        Self::build(db, transport, None, tenant_id, None)
    }

    /// Test-only construction with a fabric key matching the hub identity
    /// advertised by the injected transport.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn with_authenticated_transport_and_identity_for_test(
        db: Arc<Database>,
        transport: Arc<dyn crate::transport::ServerTransport>,
        tenant_id: TenantId,
        local_node_id: String,
        identity: Arc<crate::identity::FabricIdentity>,
    ) -> Self {
        let signer: LineageSigner = Arc::new(move |bytes| Ok(identity.sign_lineage(bytes)));
        Self::build(db, transport, Some(signer), tenant_id, Some(local_node_id))
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    /// What this hub has moved, per authenticated peer and direction, since it
    /// was constructed. Monotonic and in memory only — nothing is persisted,
    /// and a peer the transport did not authenticate has no receipt at all.
    pub fn transfer_receipts(&self) -> Vec<TransferReceipt> {
        self.receipts.receipts()
    }

    /// Current per-table upgrade messages. Compatible tables are not listed
    /// because they continue to move normally.
    pub fn schema_sync_holdbacks(&self) -> Vec<SchemaSyncHoldback> {
        let states = self
            .schema_sync_holdbacks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut holdbacks = states
            .iter()
            .flat_map(|(node_to_upgrade, state)| {
                state
                    .capabilities
                    .iter()
                    .flat_map(move |(table, capabilities)| {
                        capabilities
                            .iter()
                            .map(move |capability| SchemaSyncHoldback {
                                table: table.clone(),
                                capability: *capability,
                                node_to_upgrade: node_to_upgrade.clone(),
                            })
                    })
            })
            .collect::<Vec<_>>();
        holdbacks.sort();
        holdbacks
    }

    /// Exercise forward capability holdback on the real current transport.
    /// This changes only the test sender's knowledge of the named receiver.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn set_peer_vector_schema_support_for_test(&self, peer: &str, supported: bool) {
        self.peer_schema_capabilities.set_for_test(peer, supported);
    }

    pub async fn run(&self) {
        self.run_until(Arc::new(AtomicBool::new(false))).await;
    }

    pub async fn run_until(&self, shutdown: Arc<AtomicBool>) {
        let apply_tasks = Arc::new(ApplyTracker::new());
        let in_flight_push_applies: InFlightPushApplies =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let apply_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_PUSH_APPLIES));

        let handlers = self.handlers(apply_tasks.clone(), in_flight_push_applies, apply_permits);

        if let Err(err) = self.transport.serve(handlers, shutdown).await {
            tracing::error!(error = %err, "sync server transport loop failed");
        }

        apply_tasks.wait_idle().await;
    }

    fn handlers(
        &self,
        apply_tasks: ApplyTasks,
        in_flight_push_applies: InFlightPushApplies,
        apply_permits: Arc<Semaphore>,
    ) -> Vec<HandlerRegistration> {
        let push_handler = {
            let state = Arc::new(PushHandlerState {
                db: self.db.clone(),
                local_node_id: self.local_node_id.clone(),
                receipts: self.receipts.clone(),
                tenant_id: self.tenant_id.clone(),
                applied_push_watermark: self.applied_push_watermark.clone(),
                per_edge_watermarks: self.per_edge_watermarks.clone(),
                apply_tasks: apply_tasks.clone(),
                in_flight_push_applies: in_flight_push_applies.clone(),
                apply_permits: apply_permits.clone(),
            });
            Arc::new(move |req: IncomingRequest| {
                let state = Arc::clone(&state);
                Box::pin(async move { handle_push(state, req).await.map_err(to_transport_error) })
                    as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        let pull_handler = {
            let db = self.db.clone();
            let lineage_signer = self.lineage_signer.clone();
            let receipts = self.receipts.clone();
            let tenant_id = self.tenant_id.clone();
            let local_node_id = self.local_node_id.clone();
            let schema_sync_holdbacks = self.schema_sync_holdbacks.clone();
            let peer_schema_capabilities = self.peer_schema_capabilities.clone();
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let lineage_signer = lineage_signer.clone();
                let receipts = receipts.clone();
                let tenant_id = tenant_id.clone();
                let local_node_id = local_node_id.clone();
                let schema_sync_holdbacks = schema_sync_holdbacks.clone();
                let peer_schema_capabilities = peer_schema_capabilities.clone();
                Box::pin(async move {
                    handle_pull(
                        db,
                        lineage_signer,
                        receipts,
                        tenant_id,
                        local_node_id,
                        schema_sync_holdbacks,
                        peer_schema_capabilities,
                        req,
                    )
                    .await
                    .map_err(to_transport_error)
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        let status_handler = {
            let db = self.db.clone();
            let tenant_id = self.tenant_id.clone();
            let applied_push_watermark = self.applied_push_watermark.clone();
            let per_edge_watermarks = self.per_edge_watermarks.clone();
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let tenant_id = tenant_id.clone();
                let applied_push_watermark = applied_push_watermark.clone();
                let per_edge_watermarks = per_edge_watermarks.clone();
                Box::pin(async move {
                    handle_status(
                        db,
                        tenant_id,
                        applied_push_watermark,
                        per_edge_watermarks,
                        req,
                    )
                    .await
                    .map_err(to_transport_error)
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        // Manifested requests enter the ordinary push handler above.
        // Authenticate the requester, compare/freeze, and return signed authority.
        let binding_handler = {
            let tenant = self.tenant_id.clone();
            let db = self.db.clone();
            let hub = self.local_node_id.clone();
            let signer = self.lineage_signer.clone();
            Arc::new(move |req: IncomingRequest| {
                let (tenant, db, hub, signer) =
                    (tenant.clone(), db.clone(), hub.clone(), signer.clone());
                Box::pin(async move {
                    use crate::protocol::*;
                    let envelope =
                        decode(&req.bytes).map_err(|e| TransportError::Other(e.to_string()))?;
                    let request: BindApplicationTablePolicyRequest =
                        rmp_serde::from_slice(&envelope.payload)
                            .map_err(|e| TransportError::Other(e.to_string()))?;
                    if envelope.message_type != MessageType::BindApplicationTablePolicyRequest
                        || request.format != 1
                        || request.tenant_id != tenant.as_str()
                    {
                        return Err(TransportError::Other(
                            "invalid authenticated policy request".into(),
                        ));
                    }
                    let edge = req.node_id.ok_or_else(|| {
                        TransportError::Other("authenticated edge required".into())
                    })?;
                    let hub = hub.ok_or_else(|| {
                        TransportError::Other("authenticated hub required".into())
                    })?;
                    let signer = signer.ok_or_else(|| {
                        TransportError::Other("authenticated hub signer required".into())
                    })?;
                    let result = crate::custody::preparation::commit_binding(
                        &db,
                        tenant.clone(),
                        hub.clone(),
                        edge.clone(),
                        crate::custody::preparation::BindingInput {
                            tenant_id: tenant.clone(),
                            edge_incarnation: request.edge_incarnation,
                            expectation: request.expectation,
                        },
                        &signer,
                    );
                    let response = match result {
                        Ok(packet) => {
                            BindApplicationTablePolicyResponse::Success(BindPolicySuccess {
                                format: 1,
                                request_nonce: request.request_nonce,
                                hub_node_id: hub,
                                hub_incarnation: packet.binding.namespace.hub_incarnation,
                                tenant_id: tenant.as_str().into(),
                                edge_node_id: edge,
                                edge_incarnation: request.edge_incarnation,
                                binding: rmp_serde::to_vec_named(&packet)
                                    .map_err(|e| TransportError::Other(e.to_string()))?,
                            })
                        }
                        Err(error) => {
                            BindApplicationTablePolicyResponse::Error(BindPolicyFailure {
                                format: 1,
                                request_nonce: request.request_nonce,
                                error: match error {
                                    contextdb_core::Error::TenantPolicyNotDeclared { table } => {
                                        BindPolicyError::TenantPolicyNotDeclared { table }
                                    }
                                    contextdb_core::Error::TenantPolicyMismatch {
                                        table,
                                        clause,
                                    } => BindPolicyError::TenantPolicyMismatch { table, clause },
                                    other => BindPolicyError::Operational {
                                        message: other.to_string(),
                                    },
                                },
                            })
                        }
                    };
                    (req.responder)(
                        encode_named(MessageType::BindApplicationTablePolicyResponse, &response)
                            .map_err(|e| TransportError::Other(e.to_string()))?,
                    )
                    .await
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };
        let outcomes_handler = {
            let db = self.db.clone();
            let tenant = self.tenant_id.clone();
            let hub = self.local_node_id.clone();
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let tenant = tenant.clone();
                let hub = hub.clone();
                Box::pin(async move {
                    use crate::protocol::*;
                    let envelope =
                        decode(&req.bytes).map_err(|e| TransportError::Other(e.to_string()))?;
                    let request: FetchDeliveryOutcomesRequest =
                        rmp_serde::from_slice(&envelope.payload)
                            .map_err(|e| TransportError::Other(e.to_string()))?;
                    if envelope.message_type != MessageType::FetchDeliveryOutcomesRequest
                        || request.format != 1
                        || request.tenant_id != tenant.as_str()
                    {
                        return Err(TransportError::Other(
                            "invalid authenticated outcome request".into(),
                        ));
                    }
                    let hub = hub.ok_or_else(|| {
                        TransportError::Other("authenticated hub required".into())
                    })?;
                    let edge = req.node_id.ok_or_else(|| {
                        TransportError::Other("authenticated edge required".into())
                    })?;
                    let outcomes = crate::custody::delivery::fetch(
                        &db,
                        &tenant,
                        &hub,
                        &edge,
                        request.edge_incarnation,
                        request.since.as_deref(),
                    )
                    .map_err(|e| TransportError::Other(e.to_string()))?;
                    let response = FetchDeliveryOutcomesResponse {
                        format: 1,
                        request_nonce: request.request_nonce,
                        hub_node_id: hub,
                        hub_incarnation: db
                            .existing_sync_incarnation(&tenant)
                            .map_err(|e| TransportError::Other(e.to_string()))?
                            .ok_or_else(|| {
                                TransportError::Other("hub authority is not initialized".into())
                            })?,
                        tenant_id: tenant.as_str().into(),
                        edge_node_id: edge,
                        edge_incarnation: request.edge_incarnation,
                        result: Some(DeliveryOutcomePage { outcomes }),
                        error: None,
                    };
                    let bytes = encode_named(MessageType::FetchDeliveryOutcomesResponse, &response)
                        .map_err(|e| TransportError::Other(e.to_string()))?;
                    (req.responder)(bytes).await
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        #[cfg(feature = "test-seams")]
        let fixture_binding_handler = {
            let db = self.db.clone();
            let tenant_id = self.tenant_id.clone();
            let local_node_id = self.local_node_id.clone();
            let signer = self.lineage_signer.clone();
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let tenant_id = tenant_id.clone();
                let local_node_id = local_node_id.clone();
                let signer = signer.clone();
                Box::pin(async move {
                    let edge_node_id = req.node_id.clone().ok_or_else(|| {
                        TransportError::Other(
                            "fixture binding requires an authenticated edge".to_string(),
                        )
                    })?;
                    let hub_node_id = local_node_id.ok_or_else(|| {
                        TransportError::Other(
                            "fixture binding requires an authenticated hub".to_string(),
                        )
                    })?;
                    let signer = signer.ok_or_else(|| {
                        TransportError::Other(
                            "fixture binding requires the hub signing key".to_string(),
                        )
                    })?;
                    let request = rmp_serde::from_slice(&req.bytes)
                        .map_err(|error| TransportError::Other(error.to_string()))?;
                    let record = crate::custody::fixtures::issue_fixture_binding(
                        &db,
                        tenant_id,
                        hub_node_id,
                        edge_node_id,
                        request,
                        &signer,
                    )
                    .map_err(|e| e.to_string());
                    let response = rmp_serde::to_vec_named(&record)
                        .map_err(|error| TransportError::Other(error.to_string()))?;
                    (req.responder)(response).await
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        #[cfg(feature = "test-seams")]
        let fixture_outcome_handler = {
            let db = self.db.clone();
            let tenant_id = self.tenant_id.clone();
            let local_node_id = self.local_node_id.clone();
            let signer = self.lineage_signer.clone();
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let tenant_id = tenant_id.clone();
                let local_node_id = local_node_id.clone();
                let signer = signer.clone();
                Box::pin(async move {
                    let edge_node_id = req.node_id.clone().ok_or_else(|| {
                        TransportError::Other(
                            "fixture outcome requires an authenticated edge".to_string(),
                        )
                    })?;
                    let hub_node_id = local_node_id.ok_or_else(|| {
                        TransportError::Other(
                            "fixture outcome requires an authenticated hub".to_string(),
                        )
                    })?;
                    let signer = signer.ok_or_else(|| {
                        TransportError::Other(
                            "fixture outcome requires the hub signing key".to_string(),
                        )
                    })?;
                    let request = rmp_serde::from_slice(&req.bytes)
                        .map_err(|error| TransportError::Other(error.to_string()))?;
                    let record = crate::custody::fixtures::issue_fixture_outcome(
                        &db,
                        tenant_id,
                        hub_node_id,
                        edge_node_id,
                        request,
                        &signer,
                    )
                    .map_err(|e| e.to_string());
                    let response = rmp_serde::to_vec_named(&record)
                        .map_err(|error| TransportError::Other(error.to_string()))?;
                    (req.responder)(response).await
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        #[cfg(feature = "test-seams")]
        let fixture_outcome_batch_handler = {
            let db = self.db.clone();
            let tenant_id = self.tenant_id.clone();
            let local_node_id = self.local_node_id.clone();
            let signer = self.lineage_signer.clone();
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let tenant_id = tenant_id.clone();
                let local_node_id = local_node_id.clone();
                let signer = signer.clone();
                Box::pin(async move {
                    let edge_node_id = req.node_id.clone().ok_or_else(|| {
                        TransportError::Other(
                            "fixture outcome requires an authenticated edge".to_string(),
                        )
                    })?;
                    let hub_node_id = local_node_id.ok_or_else(|| {
                        TransportError::Other(
                            "fixture outcome requires an authenticated hub".to_string(),
                        )
                    })?;
                    let signer = signer.ok_or_else(|| {
                        TransportError::Other(
                            "fixture outcome requires the hub signing key".to_string(),
                        )
                    })?;
                    let request = rmp_serde::from_slice(&req.bytes)
                        .map_err(|error| TransportError::Other(error.to_string()))?;
                    let record = crate::custody::preparation::commit_terminal_batch(
                        &db,
                        tenant_id,
                        hub_node_id,
                        edge_node_id,
                        request,
                        &signer,
                    )
                    .map_err(|e| e.to_string());
                    let response = rmp_serde::to_vec_named(&record)
                        .map_err(|error| TransportError::Other(error.to_string()))?;
                    (req.responder)(response).await
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        // Contact bookkeeping covers authenticated pull/status exchanges and
        // pushes that reach exact-byte admission. Pull and status use
        // `record_contact`; push records after the admission decision so a
        // database write cannot strand a retry outside its leader's fanout.
        // An unauthenticated request records nothing.
        #[allow(unused_mut)]
        let mut handlers = vec![
            HandlerRegistration {
                subject: crate::subjects::binding_subject(self.tenant_id.as_str()),
                handler: binding_handler,
            },
            HandlerRegistration {
                subject: crate::subjects::delivery_outcomes_subject(self.tenant_id.as_str()),
                handler: outcomes_handler,
            },
            HandlerRegistration {
                subject: push_subject(self.tenant_id.as_str()),
                // Push admission must happen before contact recording: a
                // contact write can wait behind a schema-bearing apply, which
                // would let an exact retry reach replay validation after the
                // original in-flight entry had already completed.
                handler: push_handler,
            },
            HandlerRegistration {
                subject: pull_subject(self.tenant_id.as_str()),
                handler: self.record_contact(pull_handler),
            },
            HandlerRegistration {
                subject: status_subject(self.tenant_id.as_str()),
                handler: self.record_contact(status_handler),
            },
        ];
        #[cfg(feature = "test-seams")]
        {
            handlers.push(HandlerRegistration {
                subject: crate::custody::fixtures::fixture_binding_subject(&self.tenant_id),
                handler: fixture_binding_handler,
            });
            handlers.push(HandlerRegistration {
                subject: crate::custody::fixtures::fixture_outcome_subject(&self.tenant_id),
                handler: fixture_outcome_handler,
            });
            handlers.push(HandlerRegistration {
                subject: crate::custody::fixtures::fixture_outcome_batch_subject(&self.tenant_id),
                handler: fixture_outcome_batch_handler,
            });
        }
        handlers
    }

    /// Wrap pull and status handlers so the hub records the requesting node's
    /// last-contact before dispatching. Pushes that reach exact-byte admission
    /// record afterward in `handle_push` to preserve duplicate-response fanout.
    fn record_contact(&self, inner: RequestHandler) -> RequestHandler {
        let db = self.db.clone();
        Arc::new(move |req: IncomingRequest| {
            let db = db.clone();
            let inner = inner.clone();
            Box::pin(async move {
                if let Some(node_id) = req.node_id.clone()
                    && let Err(err) =
                        crate::sync_system_tables::record_node_contact(&db, &node_id, hub_now_ms())
                {
                    tracing::warn!(%node_id, error = %err, "failed to record node last-contact");
                }
                inner(req).await
            }) as crate::transport::TransportFuture<'static, ()>
        }) as RequestHandler
    }
}

/// The hub's wall clock in ms since the Unix epoch — the timestamp stamped on
/// a recorded node contact. A clock before the epoch records 0 (never panics).
fn hub_now_ms() -> i64 {
    i64::try_from(contextdb_core::Wallclock::now().0).unwrap_or(i64::MAX)
}

fn to_transport_error(err: contextdb_core::Error) -> TransportError {
    TransportError::Other(err.to_string())
}

/// Answer the status exchange FOR THE EDGE THAT ASKED. The asking identity is
/// the transport-authenticated one on the connection — the request bytes are
/// byte-identical for every edge and nothing on the wire changes. A transport
/// with no authenticated identity is answered from
/// the per-tenant record, which is what it has always been answered with.
async fn handle_status(
    db: Arc<Database>,
    tenant_id: TenantId,
    applied_push_watermark: Arc<AtomicLsn>,
    per_edge_watermarks: Arc<PerEdgeAppliedPushWatermarks>,
    req: IncomingRequest,
) -> contextdb_core::Result<()> {
    let envelope = decode_for_server(&req.bytes)?;
    let protocol_version = envelope.version;
    if !matches!(envelope.message_type, MessageType::StatusRequest) {
        return Err(contextdb_core::Error::SyncError(
            "unexpected message type on status subject".to_string(),
        ));
    }
    let request: crate::custody::incarnation::StatusProbe =
        rmp_serde::from_slice(&envelope.payload)
            .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

    let applied = match req.node_id.as_deref() {
        Some(node_id) => per_edge_watermarks.load(&db, &tenant_id, node_id, request.incarnation),
        None => applied_push_watermark.load(Ordering::SeqCst),
    };
    let hub_incarnation =
        if let (Some(runtime), Some(edge)) = (db.custody_runtime(), req.node_id.as_deref()) {
            crate::custody::incarnation::check(&db, &tenant_id, &runtime.node, edge, &request)?
        } else {
            db.existing_sync_incarnation(&tenant_id)?
        };
    let response = SyncStatusResponse {
        applied_push_watermark: Some(applied),
        server_current_lsn: Some(db.current_lsn()),
        hub_incarnation,
    };
    let payload = encode_for_version(protocol_version, MessageType::StatusResponse, &response)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    (req.responder)(payload)
        .await
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    Ok(())
}

async fn handle_push(
    state: Arc<PushHandlerState>,
    req: IncomingRequest,
) -> contextdb_core::Result<()> {
    let envelope = decode_for_server(&req.bytes)?;
    let protocol_version = envelope.version;
    let dependency_complete = match envelope.message_type {
        MessageType::PushRequest => false,
        MessageType::DependencyCompletePushRequest => true,
        _ => {
            return Err(contextdb_core::Error::SyncError(
                "unexpected message type on push subject".to_string(),
            ));
        }
    };
    let request: PushRequest = rmp_serde::from_slice(&envelope.payload)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    let Some(authenticated_peer) = req.node_id.clone() else {
        let response = PushResponse {
            result: None,
            error: Some(
                "authenticated sync push requires an authenticated peer identity".to_string(),
            ),
            application_error: None,
            // Ordinary replies carry the outcome lane.
            ..Default::default()
        };
        publish_push_response(req.responder, response, protocol_version).await?;
        return Ok(());
    };
    if !request.changeset.purges.is_empty() {
        let hub_node_id = state.local_node_id.clone().ok_or_else(|| {
            contextdb_core::Error::SyncError(
                "authenticated sync push requires the hub's authenticated identity".to_string(),
            )
        })?;
        let response = PushResponse {
            result: None,
            error: None,
            application_error: Some(WirePushError::PurgeRequiresAuthoritativeHub { hub_node_id }),
            // No outcomes accompany a failed push.
            ..Default::default()
        };
        publish_push_response(req.responder, response, protocol_version).await?;
        return Ok(());
    }
    let custody = !request.changeset.manifests.is_empty()
        || request.changeset.rows.iter().any(|row| {
            state
                .db
                .table_meta(&row.table)
                .is_some_and(|meta| meta.delivery_manifest_tables.is_some())
        });
    let incarnation = request.incarnation;
    let outcome_key = Database::sync_push_outcome_key(
        &state.tenant_id,
        &authenticated_peer,
        &request,
        dependency_complete,
    )?;
    let request_key = (authenticated_peer.clone(), req.bytes);
    let admission = admit_push_request(
        &state.in_flight_push_applies,
        request_key.clone(),
        req.responder,
        protocol_version,
    )
    .await?;

    // This authenticated push has reached an admission decision. Recording
    // contact afterward cannot delay a duplicate joining the in-flight fanout.
    if let Err(err) =
        crate::sync_system_tables::record_node_contact(&state.db, &authenticated_peer, hub_now_ms())
    {
        tracing::warn!(%authenticated_peer, error = %err, "failed to record node last-contact");
    }
    if !matches!(admission, PushAdmission::Leader) {
        return Ok(());
    }

    // Custody shares exact-request admission, the bounded
    // apply worker, and its installed-release post-commit checkpoint.
    if custody {
        // Decode the row payload once, then move it through
        // the custody apply. Retain only the small wire-only DDL sidecar and
        // signed manifests needed after conversion.
        let mut wire = request.changeset;
        let arrivals = crate::protocol::wire_row_arrivals(&wire);
        let lineages = crate::protocol::wire_row_lineages(&wire);
        let manifests = std::mem::take(&mut wire.manifests);
        let custody_request = PushRequest {
            incarnation,
            changeset: crate::protocol::WireChangeSet {
                ddl: wire.ddl.clone(),
                ddl_lsn: wire.ddl_lsn.clone(),
                ddl_provenance: wire.ddl_provenance.clone(),
                manifests,
                ..Default::default()
            },
        };
        let changeset = ChangeSet::try_from(wire)
            .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
        return spawn_apply_and_reply(PushApplyWork {
            db: state.db.clone(),
            local_node_id: state.local_node_id.clone(),
            peer_node_id: Some(authenticated_peer),
            incarnation,
            dependency_complete: true,
            custody: Some(custody_request),
            outcome_key,
            receipts: state.receipts.clone(),
            request_key,
            changeset,
            received_ddl: None,
            terminal_conflicts: None,
            lineages,
            arrivals,
            tenant_id: state.tenant_id.clone(),
            applied_push_watermark: state.applied_push_watermark.clone(),
            per_edge_watermarks: state.per_edge_watermarks.clone(),
            apply_tasks: state.apply_tasks.clone(),
            in_flight_push_applies: state.in_flight_push_applies.clone(),
            apply_permits: state.apply_permits.clone(),
            protocol_version,
        })
        .await;
    }

    // Ordinary pushes retain an exact durable apply result. Manifested units
    // use their signed custody journal so a retry can return the same per-unit
    // outcome packets instead of dropping them from the response.
    match state.db.replay_sync_push_outcome(&outcome_key) {
        Ok(Some(result)) => {
            publish_in_flight_push_response(
                state.in_flight_push_applies.clone(),
                request_key,
                PushResponse {
                    result: Some(result.into()),
                    error: None,
                    application_error: None,
                    ..Default::default()
                },
                protocol_version,
            )
            .await;
            return Ok(());
        }
        Err(err) => {
            publish_in_flight_push_response(
                state.in_flight_push_applies.clone(),
                request_key,
                PushResponse {
                    result: None,
                    error: Some(err.to_string()),
                    application_error: None,
                    ..Default::default()
                },
                protocol_version,
            )
            .await;
            return Ok(());
        }
        Ok(None) => {}
    }
    let arrivals = crate::protocol::wire_row_arrivals(&request.changeset);
    let lineages = crate::protocol::wire_row_lineages(&request.changeset);
    match (|| {
        let ddl_context = crate::protocol::received_ddl_context(
            &request.changeset,
            &state.tenant_id,
            &authenticated_peer,
            incarnation,
        )
        .map_err(|err| contextdb_core::Error::SyncError(err.to_string()))?;
        let changeset = ChangeSet::try_from(request.changeset)
            .map_err(|err| contextdb_core::Error::SyncError(err.to_string()))?;
        if ddl_context.is_none() {
            let conflicts = state.db.retired_generation_refusals(
                &state.tenant_id,
                &changeset,
                &lineages,
                &authenticated_peer,
                incarnation,
            )?;
            if !changeset.rows.is_empty() && conflicts.len() == changeset.rows.len() {
                return Ok::<_, contextdb_core::Error>((changeset, ddl_context, Some(conflicts)));
            }
        }
        let original_changes = changeset.clone();
        let checked = if let Some(received_ddl) = ddl_context.as_ref() {
            state.db.validate_incoming_push_lineages_with_received_ddl(
                &state.tenant_id,
                &changeset,
                &lineages,
                &authenticated_peer,
                incarnation,
                received_ddl,
            )?;
            state.db.reject_accepted_lineage_replays_with_received_ddl(
                &state.tenant_id,
                changeset,
                &lineages,
                received_ddl,
            )
        } else {
            state.db.validate_incoming_push_lineages(
                &state.tenant_id,
                &changeset,
                &lineages,
                &authenticated_peer,
                incarnation,
            )?;
            state
                .db
                .reject_accepted_lineage_replays(&state.tenant_id, changeset, &lineages)
        };
        match checked {
            Ok(changeset) => Ok((changeset, ddl_context, None)),
            Err(contextdb_core::Error::SyncReplayOfAcceptedDelete { table, key }) => {
                let conflicts =
                    state
                        .db
                        .accepted_delete_replay_conflicts(&original_changes, &table, &key);
                Ok((original_changes, ddl_context, Some(conflicts)))
            }
            Err(error) => Err(error),
        }
    })() {
        Ok((changeset, received_ddl, terminal_conflicts)) => {
            spawn_apply_and_reply(PushApplyWork {
                db: state.db.clone(),
                local_node_id: state.local_node_id.clone(),
                peer_node_id: req.node_id.clone(),
                incarnation,
                dependency_complete,
                receipts: state.receipts.clone(),
                request_key,
                custody: None,
                outcome_key,
                changeset,
                received_ddl,
                terminal_conflicts,
                lineages,
                arrivals,
                tenant_id: state.tenant_id.clone(),
                applied_push_watermark: state.applied_push_watermark.clone(),
                per_edge_watermarks: state.per_edge_watermarks.clone(),
                apply_tasks: state.apply_tasks.clone(),
                in_flight_push_applies: state.in_flight_push_applies.clone(),
                apply_permits: state.apply_permits.clone(),
                protocol_version,
            })
            .await?;
        }
        Err(err) => {
            let response =
                if let contextdb_core::Error::SyncReplayOfAcceptedDelete { table, key } = &err {
                    PushResponse {
                        result: None,
                        error: None,
                        application_error: Some(WirePushError::ReplaysAcceptedDelete {
                            table: table.clone(),
                            key: key.clone(),
                        }),
                        // No outcomes accompany a failed push.
                        ..Default::default()
                    }
                } else {
                    PushResponse {
                        result: None,
                        error: Some(err.to_string()),
                        application_error: None,
                        // Ordinary replies carry the outcome lane.
                        ..Default::default()
                    }
                };
            publish_in_flight_push_response(
                state.in_flight_push_applies.clone(),
                request_key,
                response,
                protocol_version,
            )
            .await;
        }
    }
    Ok(())
}

// These separate inputs jointly determine one atomic sync-page decision;
// keeping them explicit preserves the page's holdback, cursor, and wire rules.
#[allow(clippy::too_many_arguments)]
async fn handle_pull(
    db: Arc<Database>,
    lineage_signer: Option<LineageSigner>,
    receipts: Arc<TransferLedger>,
    tenant_id: TenantId,
    local_node_id: Option<String>,
    schema_sync_holdbacks: SchemaSyncHoldbackStates,
    peer_schema_capabilities: PeerSchemaCapabilities,
    req: IncomingRequest,
) -> contextdb_core::Result<()> {
    let envelope = decode_for_server(&req.bytes)?;
    let protocol_version = envelope.version;
    if !matches!(envelope.message_type, MessageType::PullRequest) {
        return Err(contextdb_core::Error::SyncError(
            "unexpected message type on pull subject".to_string(),
        ));
    }

    let request: PullRequest = rmp_serde::from_slice(&envelope.payload)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    let peer_node_id = req.node_id.clone();
    let capabilities = peer_schema_capabilities.resolve(peer_node_id.as_deref(), protocol_version);
    let mut prior_holdback = peer_node_id.as_deref().and_then(|peer| {
        schema_sync_holdbacks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(peer)
            .cloned()
    });
    if !capabilities.supports_all()
        && let Some(state) = prior_holdback.as_mut()
        && request.since_lsn < state.compatible_through
    {
        // This authenticated receiver is asking from history it no longer
        // claims to own. The peer name may have survived a disk rebuild, but
        // its private compatible frontier did not: trust the receiver's
        // public bookmark and retain the held-table recovery target separately.
        state.compatible_through = request.since_lsn;
    }
    if let Some(state) = prior_holdback.as_mut()
        && state.recovery_after.is_none()
    {
        // The authenticated receiver's public bookmark is stronger evidence
        // than the sender's last observed request. Capture it before the first
        // upgraded recovery page so compatible work delivered by the preceding
        // response is not replayed beside the held table.
        state.compatible_through = state.compatible_through.max(request.since_lsn);
    }
    if let Some(SchemaRecoveryRequest::Acknowledge { target_lsn }) =
        request.schema_recovery.as_ref()
        && let Some(state) = prior_holdback.as_ref()
    {
        if state.held_through != *target_lsn {
            return Err(contextdb_core::Error::SyncError(format!(
                "schema-recovery acknowledgement targets source frontier {}, but the sender is holding frontier {}",
                target_lsn.0, state.held_through.0
            )));
        }
        let peer = peer_node_id.as_deref().ok_or_else(|| {
            contextdb_core::Error::SyncError(
                "schema-recovery acknowledgement requires an authenticated receiver".to_string(),
            )
        })?;
        let (recovered_tables, remaining_capabilities) =
            holdback_for_capabilities(&db, state, capabilities);
        if recovered_tables.is_empty() {
            if state.compatible_through < *target_lsn {
                return Err(contextdb_core::Error::SyncError(
                    "schema-recovery acknowledgement names no capability this protocol can recover"
                        .to_string(),
                ));
            }
            // A final reply can be lost after the partial-upgrade state
            // commits. Repeating that exact acknowledgement is harmless.
            prior_holdback = Some(state.clone());
        } else {
            // The request itself proves that the receiver durably applied
            // the final page. Remove only the capabilities this protocol
            // gained; any capabilities still missing retain their own
            // table holdbacks.
            let remaining = (!remaining_capabilities.is_empty()).then(|| {
                let mut remaining = state.clone();
                remaining.capabilities = remaining_capabilities;
                remaining.compatible_through = remaining.compatible_through.max(*target_lsn);
                remaining
            });
            let durable = remaining
                .as_ref()
                .map(|remaining| DurableSchemaSyncHoldback {
                    peer_node_id: peer.to_string(),
                    capabilities: remaining.capabilities.clone(),
                    through: remaining.compatible_through,
                    held_through: remaining.held_through,
                });
            db.persist_inbound_schema_sync_holdback(&tenant_id, peer, durable.as_ref())?;
            let mut states = schema_sync_holdbacks
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(remaining) = remaining.clone() {
                states.insert(peer.to_string(), remaining);
            } else {
                states.remove(peer);
            }
            prior_holdback = remaining;
        }
    }
    let recovering_tables = prior_holdback
        .as_ref()
        .map(|state| {
            let (recoverable, _) = holdback_for_capabilities(&db, state, capabilities);
            recoverable
        })
        .filter(|tables| !tables.is_empty());
    let recovery_target = recovering_tables
        .as_ref()
        .and_then(|_| prior_holdback.as_ref().map(|state| state.held_through));
    let recovery_since = if let Some(target_lsn) = recovery_target {
        match request.schema_recovery.as_ref() {
            None => prior_holdback
                .as_ref()
                .and_then(|state| state.recovery_after)
                .unwrap_or(Lsn(0)),
            Some(SchemaRecoveryRequest::Continue {
                target_lsn: requested_target,
                after_lsn,
            }) => {
                if *requested_target != target_lsn || *after_lsn > target_lsn {
                    return Err(contextdb_core::Error::SyncError(format!(
                        "schema-recovery continuation does not match held frontier {}",
                        target_lsn.0
                    )));
                }
                *after_lsn
            }
            Some(SchemaRecoveryRequest::Acknowledge { .. }) => {
                return Err(contextdb_core::Error::SyncError(
                    "schema-recovery acknowledgement did not clear its held image".to_string(),
                ));
            }
        }
    } else {
        if matches!(
            request.schema_recovery,
            Some(SchemaRecoveryRequest::Continue { .. })
        ) {
            return Err(contextdb_core::Error::SyncError(
                "schema-recovery continuation has no held table on this sender".to_string(),
            ));
        }
        request.since_lsn
    };
    let effective_since = if recovering_tables.is_some() {
        recovery_since
    } else if !capabilities.supports_all() {
        prior_holdback
            .as_ref()
            .map(|state| state.compatible_through.max(request.since_lsn))
            .unwrap_or(request.since_lsn)
    } else {
        request.since_lsn
    };

    // Keep extraction, all serve-time shaping, and its schema-instance
    // evidence on one published schema.  The lease is released before the
    // response is encoded or handed to the transport below.
    let schema_read = db.enter_outbound_sync_schema_read();

    // Refuse pull if the hub has keyless tables that would sync. The changeset
    // cannot represent their rows (which lack a natural key), so silently omitting
    // them would make the pull incomplete. The fix-up is the same as for push:
    // declare a PRIMARY KEY, add an indexed `id` column, or set SYNC OFF.
    refuse_keyless_tables_with_no_identity_fallback(&db, &HashMap::new())?;

    // Never paginate retained custody history that this
    // declaration cannot serve. Hidden progress is private and schema-bound;
    // the public cursor still advances only for deliverable content.
    let hidden_scan = if protocol_version == crate::protocol::PROTOCOL_VERSION {
        db.custody_pull_scan(&tenant_id, req.node_id.as_deref(), effective_since)?
    } else {
        None
    };
    let scan_since = hidden_scan
        .as_ref()
        .map_or(effective_since, |scan| scan.since);
    let (mut changes, arrivals, ddl_provenance_source) =
        db.checked_changes_since_with_arrivals(scan_since)?;
    let mut purge_items = db.authoritative_purge_delivery_items_since(scan_since)?;
    let consumed_hidden_frontier = hidden_scan.as_ref().and_then(|_| changes.max_lsn());
    if let Some(scan) = hidden_scan {
        changes = changes.filter_by_direction_history(
            &db.sync_direction_history(),
            &[
                SyncDirection::Push,
                SyncDirection::Pull,
                SyncDirection::Both,
            ],
        );
        changes = crate::sync_client::drop_push_only_retained_rows(&db, changes);
        if changes.is_empty() && purge_items.is_empty() {
            db.remember_hidden_custody_pull(scan)?;
        }
    }

    if let Some(tables) = recovering_tables.as_ref() {
        let target = recovery_target.expect("recovery target accompanies held tables");
        let compatible_through = prior_holdback
            .as_ref()
            .expect("recovery tables come from a schema holdback")
            .compatible_through;
        changes = changes
            .for_schema_recovery(tables, compatible_through)
            .through_lsn(target);
        purge_items.retain(|item| tables.contains(&item.table) && item.frontier <= target);
    }

    let mut next_holdback = prior_holdback.unwrap_or_default();
    if !capabilities.supports_all() && recovering_tables.is_none() {
        next_holdback.recovery_after = None;
        next_holdback.compatible_through = next_holdback.compatible_through.max(request.since_lsn);
        for (table, capabilities) in
            unsupported_schema_capabilities_in_changes(&db, &changes, capabilities)
        {
            next_holdback.capabilities.remove(&table);
            if !capabilities.is_empty() {
                next_holdback.capabilities.insert(table, capabilities);
            }
        }
        let held_tables = next_holdback
            .capabilities
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        if !held_tables.is_empty() {
            // Freeze the complete held image before removing it from the old
            // receiver's page. Pagination and purge ordering below then see
            // only compatible work, so an erasure waiting on one table cannot
            // block later healthy tables or advance the public bookmark.
            if let Some(frontier) = changes
                .max_lsn()
                .into_iter()
                .chain(purge_items.iter().map(|item| item.frontier))
                .max()
            {
                next_holdback.held_through = next_holdback.held_through.max(frontier);
            }
            changes = changes.without_tables(&held_tables);
            purge_items.retain(|item| !held_tables.contains(&item.table));
        }
    }

    let mut has_more = false;
    if let Some(max_entries) = request.max_entries {
        let max = max_entries as usize;
        let change_groups = changes
            .clone()
            .split_by_data_lsn()
            .into_iter()
            .filter(|group| group.data_entry_count() > 0 || !group.ddl.is_empty())
            .collect::<Vec<_>>();
        let mut groups = BTreeMap::<Lsn, (Vec<ChangeSet>, Vec<_>)>::new();
        for group in change_groups {
            let frontier = group.max_lsn().unwrap_or_else(|| db.current_lsn());
            groups.entry(frontier).or_default().0.push(group);
        }
        for item in purge_items {
            groups.entry(item.frontier).or_default().1.push(item);
        }
        let total_entries = groups
            .values()
            .map(|(change_groups, purges)| {
                change_groups
                    .iter()
                    .map(|group| group.data_entry_count().max(group.ddl.len()).max(1))
                    .sum::<usize>()
                    .saturating_add(purges.len())
            })
            .sum::<usize>();
        if total_entries > max {
            let mut selected_changes = Vec::new();
            let mut selected_purges = Vec::new();
            let mut selected_entries = 0usize;
            let mut selected_frontiers = 0usize;
            for (change_groups, purges) in groups.values() {
                let group_entries = change_groups
                    .iter()
                    .map(|group| group.data_entry_count().max(group.ddl.len()).max(1))
                    .sum::<usize>()
                    .saturating_add(purges.len());
                if selected_frontiers != 0 && selected_entries + group_entries > max {
                    break;
                }
                selected_entries = selected_entries.saturating_add(group_entries);
                selected_changes.extend(change_groups.iter().cloned());
                selected_purges.extend(purges.iter().cloned());
                selected_frontiers += 1;
                if selected_entries >= max {
                    break;
                }
            }
            has_more = selected_frontiers < groups.len();
            changes = merge_changeset_groups(selected_changes);
            purge_items = selected_purges;
        } else {
            purge_items = groups
                .into_values()
                .flat_map(|(_, purges)| purges)
                .collect();
        }
    }
    // A purge is an irreversible source-ordered frontier. Do not serve schema
    // or data from a later frontier in the same page: the client applies one
    // page under one schema-publication lease, and allowing a later table
    // generation to land first would make the earlier purge look foreign to
    // the very generation it is meant to erase. End this page at the earliest
    // purge frontier; the cursor then requests later work on the next page.
    if let Some(purge_frontier) = purge_items.iter().map(|item| item.frontier).min() {
        // A cold persisted-state fallback reconstructs only the CURRENT
        // schema. If an older purge names a retired table generation, serving
        // that synthetic schema first would make the purge foreign at the
        // destination; cutting the schema out without replay support would
        // lose it forever. Serve the retired-generation purge alone, then let
        // the cursor request the replayable synthetic current snapshot.
        let synthetic_retired_generation = if ddl_provenance_source.is_synthetic_snapshot() {
            purge_items.iter().try_fold(false, |found, item| {
                db.authoritative_purge_targets_retired_generation(item)
                    .map(|retired| found || retired)
            })?
        } else {
            false
        };
        if synthetic_retired_generation {
            changes = ChangeSet::default();
            purge_items.retain(|item| item.frontier == purge_frontier);
            has_more = true;
        }
        let groups = changes.clone().split_by_data_lsn();
        if groups.iter().any(|group| {
            group
                .max_lsn()
                .is_some_and(|frontier| frontier > purge_frontier)
        }) {
            changes = merge_changeset_groups(
                groups
                    .into_iter()
                    .filter(|group| {
                        group
                            .max_lsn()
                            .is_none_or(|frontier| frontier <= purge_frontier)
                    })
                    .collect(),
            );
            purge_items.retain(|item| item.frontier <= purge_frontier);
            has_more = true;
        }
    }
    let mut bootstrap_batches = changes.clone().split_at_trigger_bootstrap_barriers();
    if bootstrap_batches.len() > 1 {
        changes = bootstrap_batches.remove(0);
        if let Some(frontier) = changes.max_lsn() {
            purge_items.retain(|item| item.frontier <= frontier);
        } else {
            purge_items.clear();
        }
        has_more = true;
    }
    // Always carry the consumed frontier as the cursor whenever anything is
    // served. The serve-time filters below (direction, and the retention-window
    // exclusion) can drop the highest-LSN row from the wire, so a reader that
    // fell back to the max LSN of the FILTERED bytes would strand its watermark
    // below an excluded row and re-request it forever. The cursor is taken from
    // the pre-filter frontier here, so the watermark advances past excluded rows.
    let mut cursor = changes
        .max_lsn()
        .into_iter()
        .chain(purge_items.iter().map(|item| item.frontier))
        .max()
        .or_else(|| {
            if changes.ddl.is_empty() {
                None
            } else {
                Some(db.current_lsn())
            }
        });
    if !has_more {
        let bounded_hidden_frontier = consumed_hidden_frontier
            .map(|frontier| recovery_target.map_or(frontier, |target| frontier.min(target)));
        cursor = cursor.into_iter().chain(bounded_hidden_frontier).max();
    }

    let recovery_page = recovery_target.map(|target_lsn| SchemaRecoveryPage {
        target_lsn,
        next_lsn: cursor.unwrap_or(effective_since),
        complete: !has_more,
    });
    if let Some(recovery_page) = recovery_page.as_ref() {
        // Recovery progress is private, but the ordinary cursor presented to
        // existing apply code must remain monotonic. Every recovery page asks
        // for one more request; the final one is followed by an explicit ACK.
        cursor = Some(request.since_lsn.max(recovery_page.next_lsn));
        has_more = true;
    }
    let recovery_after = recovery_page
        .as_ref()
        .and_then(|page| (!page.complete).then_some(page.next_lsn));

    // The cursor is already computed from the full frontier, so declaration
    // filtering excludes `SYNC OFF` rows without stranding the edge's pull
    // watermark.
    let changes = changes.filter_by_direction_history(
        &db.sync_direction_history(),
        &[
            SyncDirection::Push,
            SyncDirection::Pull,
            SyncDirection::Both,
        ],
    );

    // A retained table is delivered one way. The hub holds the rows an edge
    // pushed it, but it never sends them back: the edge deletes its own copy
    // once delivered, and a hub that replied with them would replant exactly
    // what aged out. DDL still travels, so a fresh edge still LEARNS the table.
    let changes = crate::sync_client::drop_push_only_retained_rows(&db, changes);

    // A two-way retained table IS served back — recovery and the ordinary
    // dashboard read — but only its still-live rows: a row whose retention
    // window has already passed is excluded here at serve time, so a wiped or
    // fresh edge is never re-planted with history that has aged out, even while
    // the hub itself still stores that row (its own pruning runs on its own
    // clock). The cursor was computed from the full frontier above, so an
    // excluded row still advances the reader's watermark and is not re-requested.
    let changes = drop_rows_past_retention_window(&db, changes);
    if !capabilities.supports_all() && recovering_tables.is_none() {
        cursor = changes
            .max_lsn()
            .into_iter()
            .chain(purge_items.iter().map(|item| item.frontier))
            .max()
            .or(cursor.filter(|_| changes.is_empty() && purge_items.is_empty()));
    }
    let units = db.dependency_complete_outbound_units(changes, effective_since)?;
    let mut ordinary = ChangeSet::default();
    let mut dependency_units = Vec::new();
    for unit in units {
        if unit.dependency_complete {
            dependency_units.push(unit.changes);
        } else {
            ordinary.rows.extend(unit.changes.rows);
            ordinary.edges.extend(unit.changes.edges);
            ordinary.vectors.extend(unit.changes.vectors);
            ordinary.ddl.extend(unit.changes.ddl);
            ordinary.ddl_lsn.extend(unit.changes.ddl_lsn);
        }
    }
    receipts.record(
        req.node_id.as_deref(),
        TransferPlane::Sync,
        TransferDirection::Sent,
        ordinary.rows.len() as u64
            + dependency_units
                .iter()
                .map(|unit: &ChangeSet| unit.rows.len() as u64)
                .sum::<u64>(),
        row_payload_bytes(&ordinary.rows)
            + dependency_units
                .iter()
                .map(|unit: &ChangeSet| row_payload_bytes(&unit.rows))
                .sum::<u64>(),
    );
    // Propagate sync_incarnation errors rather than silently converting to None --
    // a client holding a source-bound cursor cannot validate the identity if
    // the response carries no source. Missing source identity is not idempotent
    // with the stored cursor binding.
    let source = db.sync_incarnation(&tenant_id)?;
    let local_incarnation = db.sync_incarnation(&tenant_id)?;
    let served_row_count = ordinary.rows.len()
        + dependency_units
            .iter()
            .map(|unit: &ChangeSet| unit.rows.len())
            .sum::<usize>();
    let authenticated_local_node = if served_row_count == 0 {
        None
    } else {
        Some(local_node_id.as_deref().ok_or_else(|| {
            contextdb_core::Error::SyncError(
                "authenticated sync pull requires the hub's authenticated identity".to_string(),
            )
        })?)
    };
    let signer = lineage_signer.as_ref().ok_or_else(|| {
        contextdb_core::Error::SyncError(
            "authenticated sync pull requires the hub transport's creator signer".to_string(),
        )
    })?;
    let ordinary_lineages = authenticated_local_node
        .map(|node_id| {
            db.outbound_row_lineages(
                &ordinary,
                &tenant_id,
                node_id,
                local_incarnation,
                signer.as_ref(),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let ordinary_ddl_provenance = db.outbound_ddl_provenance(&ordinary, &ddl_provenance_source)?;
    let mut ordinary_changeset =
        crate::protocol::wire_changeset_with_arrivals_lineages_and_ddl_provenance(
            ordinary,
            &arrivals,
            &ordinary_lineages,
            ordinary_ddl_provenance,
        );
    ordinary_changeset.purges = purge_items
        .into_iter()
        .map(|item| WirePurgeChange {
            table: item.table,
            table_generation: item.table_generation,
            natural_key: item.natural_key.into(),
            purged_lineage_roots: item.purged_lineage_roots,
            purge_frontier: item.frontier,
            node_local_predicate: item.node_local_predicate,
        })
        .collect();
    let ordinary_response = PullResponse {
        changeset: ordinary_changeset,
        has_more,
        cursor,
        source: Some(source),
        schema_recovery: recovery_page,
    };
    let dependency_units = dependency_units
        .into_iter()
        .map(|unit| {
            let lineages = authenticated_local_node
                .map(|node_id| {
                    db.outbound_row_lineages(
                        &unit,
                        &tenant_id,
                        node_id,
                        local_incarnation,
                        signer.as_ref(),
                    )
                })
                .transpose()?
                .unwrap_or_default();
            let ddl_provenance = db.outbound_ddl_provenance(&unit, &ddl_provenance_source)?;
            let wire = crate::protocol::wire_changeset_with_arrivals_lineages_and_ddl_provenance(
                unit,
                &arrivals,
                &lineages,
                ddl_provenance,
            );
            Ok(wire)
        })
        .collect::<contextdb_core::Result<Vec<_>>>()?;
    drop(schema_read);
    let (message_type, payload) = if dependency_units.is_empty() {
        (
            MessageType::PullResponse,
            encode_for_version(
                protocol_version,
                MessageType::PullResponse,
                &ordinary_response,
            ),
        )
    } else {
        let response = DependencyCompletePullResponse {
            ordinary: ordinary_response,
            units: dependency_units,
        };
        (
            MessageType::DependencyCompletePullResponse,
            encode_for_version(
                protocol_version,
                MessageType::DependencyCompletePullResponse,
                &response,
            ),
        )
    };
    let _ = message_type;
    let payload = payload.map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    if let Some(peer) = peer_node_id.as_deref() {
        let mut states = schema_sync_holdbacks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !capabilities.supports_all() && recovering_tables.is_none() {
            if next_holdback.capabilities.is_empty() {
                db.persist_inbound_schema_sync_holdback(&tenant_id, peer, None)?;
                states.remove(peer);
            } else {
                let durable = DurableSchemaSyncHoldback {
                    peer_node_id: peer.to_string(),
                    capabilities: next_holdback.capabilities.clone(),
                    through: next_holdback.compatible_through,
                    held_through: next_holdback.held_through,
                };
                db.persist_inbound_schema_sync_holdback(&tenant_id, peer, Some(&durable))?;
                states.insert(peer.to_string(), next_holdback);
            }
        } else if recovering_tables.is_some()
            && let Some(state) = states.get_mut(peer)
        {
            state.recovery_after = recovery_after;
        }
    }
    (req.responder)(payload)
        .await
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    Ok(())
}

fn decode_for_server(data: &[u8]) -> contextdb_core::Result<crate::protocol::Envelope> {
    decode(data).map_err(|error| contextdb_core::Error::SyncError(error.to_string()))
}

const fn schema_capability_minimum_protocol(_capability: SchemaSyncCapability) -> u8 {
    7
}

#[derive(Clone, Copy)]
pub(crate) struct SchemaCapabilities {
    protocol_version: u8,
    vector_schema_supported: bool,
}

impl SchemaCapabilities {
    fn supports(self, capability: SchemaSyncCapability) -> bool {
        self.vector_schema_supported
            && self.protocol_version >= schema_capability_minimum_protocol(capability)
    }

    pub(crate) fn supports_all(self) -> bool {
        [
            SchemaSyncCapability::VectorPartitioning,
            SchemaSyncCapability::VectorSearchMode,
            SchemaSyncCapability::VectorAutoIndexAt,
            SchemaSyncCapability::VectorHnswPolicy,
        ]
        .into_iter()
        .all(|capability| self.supports(capability))
    }
}

/// Production capabilities come exclusively from the accepted protocol.
/// Tests may model a future gap without enabling an unreleased wire version.
#[derive(Clone, Default)]
pub(crate) struct PeerSchemaCapabilities {
    #[cfg(feature = "test-seams")]
    overrides: Arc<std::sync::Mutex<HashMap<String, bool>>>,
}

impl PeerSchemaCapabilities {
    pub(crate) fn resolve(&self, _peer: Option<&str>, protocol_version: u8) -> SchemaCapabilities {
        #[cfg(feature = "test-seams")]
        let vector_schema_supported = _peer
            .and_then(|peer| {
                self.overrides
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(peer)
                    .copied()
            })
            .unwrap_or(true);
        #[cfg(not(feature = "test-seams"))]
        let vector_schema_supported = true;
        SchemaCapabilities {
            protocol_version,
            vector_schema_supported,
        }
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn set_for_test(&self, peer: &str, supported: bool) {
        self.overrides
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(peer.to_string(), supported);
    }
}

pub(crate) fn unsupported_schema_capabilities_for_table(
    db: &Database,
    table: &str,
    capabilities: SchemaCapabilities,
) -> BTreeSet<SchemaSyncCapability> {
    let mut declared = db.authored_schema_capabilities(table);
    if let Some(meta) = db.table_meta(table) {
        declared.extend(schema_capabilities_in_meta(&meta));
    }
    declared
        .into_iter()
        .filter(|capability| !capabilities.supports(*capability))
        .collect()
}

/// Include the vocabulary of immutable authored history, even when a later
/// ALTER or DROP removed it from the current schema. A peer must be able to
/// replay the entire source vector before this table can leave holdback.
pub(crate) fn unsupported_schema_capabilities_in_changes(
    db: &Database,
    changes: &ChangeSet,
    capabilities: SchemaCapabilities,
) -> BTreeMap<String, BTreeSet<SchemaSyncCapability>> {
    let mut tables = HashSet::new();
    tables.extend(changes.rows.iter().map(|row| row.table.clone()));
    tables.extend(
        changes
            .vectors
            .iter()
            .map(|vector| vector.index.table.clone()),
    );
    tables.extend(
        changes
            .ddl
            .iter()
            .filter_map(DdlChange::table_name)
            .map(str::to_string),
    );

    tables
        .into_iter()
        .map(|table| {
            let unsupported = unsupported_schema_capabilities_for_table(db, &table, capabilities);
            (table, unsupported)
        })
        .collect()
}

fn holdback_for_capabilities(
    db: &Database,
    state: &PeerSchemaSyncHoldbackState,
    capabilities: SchemaCapabilities,
) -> (
    HashSet<String>,
    BTreeMap<String, BTreeSet<SchemaSyncCapability>>,
) {
    let mut recoverable = HashSet::new();
    let mut remaining = BTreeMap::new();
    for table in state.capabilities.keys() {
        let unsupported = unsupported_schema_capabilities_for_table(db, table, capabilities);
        if unsupported.is_empty() {
            recoverable.insert(table.clone());
        } else {
            remaining.insert(table.clone(), unsupported);
        }
    }
    (recoverable, remaining)
}

fn schema_capabilities_in_meta(meta: &TableMeta) -> BTreeSet<SchemaSyncCapability> {
    schema_capabilities_in_columns(&meta.columns)
}

pub(crate) fn schema_capabilities_in_columns(
    columns: &[contextdb_core::ColumnDef],
) -> BTreeSet<SchemaSyncCapability> {
    let mut capabilities = BTreeSet::new();
    for column in columns {
        if !matches!(column.column_type, ColumnType::Vector(_)) {
            continue;
        }
        if column
            .partition_key_columns
            .as_ref()
            .is_some_and(|columns| !columns.is_empty())
            || column.max_partitions.is_some()
        {
            capabilities.insert(SchemaSyncCapability::VectorPartitioning);
        }
        if column.search_mode != VectorSearchMode::Auto {
            capabilities.insert(SchemaSyncCapability::VectorSearchMode);
        }
        if column.auto_index_at.is_some() {
            capabilities.insert(SchemaSyncCapability::VectorAutoIndexAt);
        }
        if column.hnsw_m.is_some()
            || column.hnsw_ef_construction.is_some()
            || column.hnsw_ef_search.is_some()
            || column.consolidation_change_percent.is_some()
            || column.consolidation_tombstone_percent.is_some()
            || column.consolidation_disabled
        {
            capabilities.insert(SchemaSyncCapability::VectorHnswPolicy);
        }
    }
    capabilities
}

/// Exclude the rows of retained tables whose retention window has already
/// passed from a changeset about to leave the hub. This is a SERVE-time content
/// filter: each row is judged by the SAME expiry rule the local prune runs
/// (`TableMeta::retained_row_has_expired` — a per-row `EXPIRES` timestamp taking
/// precedence over the `RETAIN` window, then the `created_at` + window
/// fallback), so an edge is never served history that has already aged out AND
/// a never-expire row with an aged creation stamp is never wrongly withheld.
/// It touches ROW CONTENT only — a delete record carries no creation stamp and
/// always travels, so ordinary two-way deletion keeps working; a row on a
/// non-retained table, or one carrying no creation stamp and no `EXPIRES`
/// override to judge, is kept. The shared helper deliberately does NOT consult
/// the `SYNC SAFE` delete-after-delivery pin: that is delete safety, not expiry,
/// and withholding an un-confirmed row from a two-way reader would strand it.
fn drop_rows_past_retention_window(db: &Database, changes: ChangeSet) -> ChangeSet {
    let now = contextdb_core::Wallclock::now();
    let mut changes = changes;
    changes.rows.retain(|row| {
        if row.deleted {
            return true;
        }
        let Some(meta) = db.table_meta(&row.table) else {
            return true;
        };
        !meta.retained_row_has_expired(&row.values, row.created_at, now)
    });
    changes
}

async fn spawn_apply_and_reply(work: PushApplyWork) -> contextdb_core::Result<()> {
    let PushApplyWork {
        db,
        local_node_id,
        peer_node_id,
        incarnation,
        dependency_complete,
        receipts,
        request_key,
        custody,
        outcome_key,
        changeset,
        received_ddl,
        terminal_conflicts,
        lineages,
        arrivals,
        tenant_id,
        applied_push_watermark,
        per_edge_watermarks,
        apply_tasks,
        in_flight_push_applies,
        apply_permits,
        protocol_version,
    } = work;

    let row_count = changeset.rows.len();
    let push_payload_bytes = row_payload_bytes(&changeset.rows);
    let push_max_lsn = changeset.max_lsn();
    #[cfg(feature = "production-smoke-driver")]
    let request_digest = *blake3::hash(&request_key.1).as_bytes();
    #[cfg(feature = "production-smoke-driver")]
    let checkpoint_node_id = peer_node_id.clone().unwrap_or_default();
    let guard = apply_tasks.start();
    tokio::spawn(async move {
        let _guard = guard;
        maybe_wait_for_test_push_barrier(row_count).await;
        let applying_node_id = peer_node_id.clone();
        let (response, committed_checkpoint) = match apply_permits.acquire_owned().await {
            Ok(_permit) => {
                match tokio::task::spawn_blocking(move || {
                    // A push has no cursor to re-adopt against — a hub never
                    // detects "my own source changed," only a pulling client
                    // does (see `SyncClient::pull`). Every push apply is the
                    // ordinary, continuing case.
                    if dependency_complete && custody.is_none() {
                        db.validate_dependency_complete_unit(&changeset)?;
                    }
                    let mut outcomes = Vec::new();
                    let result = if let Some(request) = custody {
                        let (result, committed) = crate::custody::delivery::apply(
                            &db,
                            crate::custody::delivery::DeliveryRoute {
                                tenant: &tenant_id,
                                hub: local_node_id
                                    .as_deref()
                                    .ok_or_else(crate::custody::canonical::invalid)?,
                                edge: applying_node_id
                                    .as_deref()
                                    .ok_or_else(crate::custody::canonical::invalid)?,
                            },
                            request,
                            changeset,
                            &arrivals,
                            &lineages,
                        )?;
                        outcomes = committed;
                        result
                    } else if let Some(conflicts) = terminal_conflicts {
                        let (Some(node_id), Some(max_lsn)) =
                            (applying_node_id.as_deref(), push_max_lsn)
                        else {
                            return Err(contextdb_core::Error::SyncError(
                                "authenticated terminal push refusal lacks an edge receipt identity"
                                    .to_string(),
                            ));
                        };
                        db.commit_terminal_sync_refusals_with_receipt(
                            contextdb_engine::database::SyncApplyReceipt {
                                tenant_id: tenant_id.clone(),
                                node_id: node_id.to_string(),
                                incarnation,
                                source_lsn: max_lsn,
                                dependency_complete,
                            },
                            row_count,
                            conflicts,
                            &outcome_key,
                        )?
                    } else if let (Some(node_id), Some(max_lsn)) =
                        (applying_node_id.as_deref(), push_max_lsn)
                    {
                        db.apply_authenticated_received_changes_with_outcome(
                            changeset,
                            &arrivals,
                            SyncAdoption::Continuing,
                            contextdb_engine::database::SyncApplyReceipt {
                                tenant_id: tenant_id.clone(),
                                node_id: node_id.to_string(),
                                incarnation,
                                source_lsn: max_lsn,
                                dependency_complete,
                            },
                            local_node_id.as_deref(),
                            &lineages,
                            received_ddl.as_ref(),
                            Some(&outcome_key),
                        )?
                    } else {
                        db.apply_authenticated_received_changes_with_lineages_as_hub_push(
                            changeset,
                            &arrivals,
                            SyncAdoption::Continuing,
                            None,
                            &tenant_id,
                            &lineages,
                            received_ddl.as_ref(),
                            dependency_complete,
                        )?
                    };
                    if let Some(max_lsn) = push_max_lsn {
                        // What this hub now holds FROM THIS EDGE. Raised only
                        // after the apply committed, so the number the status
                        // exchange answers with never runs ahead of the data.
                        if let Some(node_id) = applying_node_id.as_deref() {
                            per_edge_watermarks.publish_committed(node_id, incarnation, max_lsn);
                        }
                        applied_push_watermark.fetch_max(max_lsn, Ordering::SeqCst);
                        let watermark = applied_push_watermark.load(Ordering::SeqCst);
                        if let Err(err) =
                            db.persist_sync_applied_push_watermark(&tenant_id, watermark)
                        {
                            tracing::warn!(
                                %tenant_id,
                                error = %err,
                                "failed to persist applied-push watermark"
                            );
                        }
                    }
                    Ok::<_, contextdb_core::Error>((
                        result,
                        outcomes,
                        db.existing_sync_incarnation(&tenant_id)?,
                    ))
                })
                .await
                {
                    Ok(Ok((result, outcomes, hub_incarnation))) => {
                        // The rows that CROSSED THE WIRE, against the peer the
                        // transport authenticated. Counted from the transmitted
                        // set, not from the apply result: a row the conflict
                        // policy skipped still moved, and its bytes are already
                        // in `push_payload_bytes` — taking the item count from
                        // `applied_rows` would pair the two figures with
                        // different row sets, which is exactly what the
                        // counters' contract forbids. An unauthenticated
                        // exchange records nothing at all.
                        receipts.record(
                            peer_node_id.as_deref(),
                            TransferPlane::Sync,
                            TransferDirection::Received,
                            row_count as u64,
                            push_payload_bytes,
                        );
                        #[cfg(feature = "production-smoke-driver")]
                        let checkpoint = push_max_lsn.map(|source_lsn| {
                            (
                                request_digest,
                                checkpoint_node_id,
                                source_lsn,
                                result.new_lsn,
                            )
                        });
                        #[cfg(not(feature = "production-smoke-driver"))]
                        let checkpoint: Option<()> = None;
                        (
                            PushResponse {
                                result: Some(result.into()),
                                error: None,
                                application_error: None,
                                outcomes,
                                hub_incarnation,
                            },
                            checkpoint,
                        )
                    }
                    Ok(Err(err)) => (
                        PushResponse {
                            result: None,
                            error: Some(err.to_string()),
                            application_error: None,
                            // Ordinary replies carry the outcome lane.
                            ..Default::default()
                        },
                        None,
                    ),
                    Err(err) => (
                        PushResponse {
                            result: None,
                            error: Some(format!("push apply task failed: {err}")),
                            application_error: None,
                            // Ordinary replies carry the outcome lane.
                            ..Default::default()
                        },
                        None,
                    ),
                }
            }
            Err(err) => (
                PushResponse {
                    result: None,
                    error: Some(format!("push apply semaphore closed: {err}")),
                    application_error: None,
                    // Ordinary replies carry the outcome lane.
                    ..Default::default()
                },
                None,
            ),
        };

        #[cfg(feature = "production-smoke-driver")]
        if let Some((request_digest, authenticated_node_id, source_lsn, hub_lsn)) =
            committed_checkpoint
        {
            crate::transport::production_smoke_completed_apply_before_reply(
                request_digest,
                authenticated_node_id,
                source_lsn.0,
                hub_lsn.0,
                dependency_complete,
            );
        }
        #[cfg(not(feature = "production-smoke-driver"))]
        let _ = committed_checkpoint;

        publish_in_flight_push_response(
            in_flight_push_applies,
            request_key,
            response,
            protocol_version,
        )
        .await;
    });
    Ok(())
}

/// Admit a request before any database access. The entry remains present until
/// every attached responder has been sent the leader's result.
async fn admit_push_request(
    in_flight_push_applies: &InFlightPushApplies,
    request_key: PushRequestKey,
    responder: Responder,
    protocol_version: u8,
) -> contextdb_core::Result<PushAdmission> {
    let mut in_flight = in_flight_push_applies.lock().await;
    if let Some(responders) = in_flight.get_mut(&request_key) {
        if responders.len() >= MAX_REPLIES_PER_IN_FLIGHT_PUSH {
            drop(in_flight);
            let response = PushResponse {
                result: None,
                error: Some("sync server push apply duplicate reply fanout full".to_string()),
                application_error: None,
                // Ordinary replies carry the outcome lane.
                ..Default::default()
            };
            publish_push_response(responder, response, protocol_version).await?;
            return Ok(PushAdmission::Rejected);
        }
        responders.push(responder);
        return Ok(PushAdmission::Duplicate);
    }
    if in_flight.len() >= MAX_IN_FLIGHT_PUSH_APPLIES {
        drop(in_flight);
        let response = PushResponse {
            result: None,
            error: Some("sync server push apply backlog full".to_string()),
            application_error: None,
            // Ordinary replies carry the outcome lane.
            ..Default::default()
        };
        publish_push_response(responder, response, protocol_version).await?;
        return Ok(PushAdmission::Rejected);
    }
    in_flight.insert(request_key, vec![responder]);
    Ok(PushAdmission::Leader)
}

/// Send one leader outcome to every exact-byte retry while keeping the request
/// admitted through delivery. A retry that arrives during fanout is drained by
/// the next loop rather than becoming a post-commit zero-row replay.
async fn publish_in_flight_push_response(
    in_flight_push_applies: InFlightPushApplies,
    request_key: PushRequestKey,
    response: PushResponse,
    protocol_version: u8,
) {
    loop {
        let responders = {
            let mut in_flight = in_flight_push_applies.lock().await;
            let Some(responders) = in_flight.get_mut(&request_key) else {
                return;
            };
            if responders.is_empty() {
                in_flight.remove(&request_key);
                return;
            }
            std::mem::take(responders)
        };

        for responder in responders {
            if let Err(err) =
                publish_push_response(responder, response.clone(), protocol_version).await
            {
                tracing::error!(error = %err, "failed to publish push response");
            }
        }
    }
}

async fn publish_push_response(
    responder: Responder,
    response: PushResponse,
    protocol_version: u8,
) -> contextdb_core::Result<()> {
    let payload = encode_for_version(protocol_version, MessageType::PushResponse, &response)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    responder(payload)
        .await
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    Ok(())
}

fn merge_changeset_groups(groups: Vec<ChangeSet>) -> ChangeSet {
    let mut merged = ChangeSet::default();
    for group in groups {
        merged.rows.extend(group.rows);
        merged.edges.extend(group.edges);
        merged.vectors.extend(group.vectors);
        merged.ddl.extend(group.ddl);
        merged.ddl_lsn.extend(group.ddl_lsn);
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn identical_pushes_from_distinct_authenticated_edges_deliver_independently() {
        let tasks = Arc::new(ApplyTracker::new());
        let state = Arc::new(PushHandlerState {
            db: Arc::new(Database::open_memory()),
            local_node_id: Some("1".repeat(64)),
            receipts: Arc::new(TransferLedger::new()),
            tenant_id: TenantId::from("independent-edges"),
            applied_push_watermark: Arc::new(AtomicLsn::new(Lsn(0))),
            per_edge_watermarks: Arc::new(PerEdgeAppliedPushWatermarks::default()),
            apply_tasks: tasks.clone(),
            in_flight_push_applies: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            apply_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_PUSH_APPLIES)),
        });
        let bytes = crate::protocol::encode(MessageType::PushRequest, &PushRequest::default())
            .expect("encode empty ordinary push");
        let (first_sent, first_received) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        handle_push(
            state.clone(),
            IncomingRequest {
                bytes: bytes.clone(),
                node_id: Some("2".repeat(64)),
                responder: Box::new(move |response| {
                    Box::pin(async move {
                        first_sent.send(response).expect("first reply receiver");
                        released.await.expect("release first reply");
                        Ok(())
                    })
                }),
            },
        )
        .await
        .expect("first request admitted");
        let first = first_received
            .await
            .expect("first result reached transport");
        let first: PushResponse =
            rmp_serde::from_slice(&decode(&first).expect("first envelope").payload)
                .expect("first response");
        assert!(first.result.is_some(), "{first:?}");
        let (second_sent, second_received) = tokio::sync::oneshot::channel();
        handle_push(
            state,
            IncomingRequest {
                bytes,
                node_id: Some("3".repeat(64)),
                responder: Box::new(move |response| {
                    Box::pin(async move {
                        let _ = second_sent.send(response);
                        Ok(())
                    })
                }),
            },
        )
        .await
        .expect("second request admitted");
        let second = tokio::time::timeout(std::time::Duration::from_secs(5), second_received).await;
        release.send(()).expect("release blocked first transport");
        tasks.wait_idle().await;
        let second = second
            .expect("another authenticated edge must not wait on the first edge's reply")
            .expect("second result");
        let second: PushResponse =
            rmp_serde::from_slice(&decode(&second).expect("second envelope").payload)
                .expect("second response");
        assert!(second.result.is_some(), "{second:?}");
    }
}
