// Canonical authenticated-sync implementation. The server-path mirror is
// intentionally byte-identical and audited by `sync_source_mirror_tests`.
use crate::protocol::{
    DependencyCompletePullResponse, MessageType, PullRequest, PullResponse, PushRequest,
    PushResponse, SyncStatusRequest, SyncStatusResponse, WirePurgeChange, WirePushError, decode,
    encode, row_payload_bytes,
};
use crate::subjects::{pull_subject, push_subject, status_subject};
use crate::sync_client::refuse_keyless_tables_with_no_identity_fallback;
use crate::transfer_receipts::{TransferDirection, TransferLedger, TransferPlane, TransferReceipt};
use crate::transport::{
    HandlerRegistration, IncomingRequest, LineageSigner, RequestHandler, Responder,
    ServerTransport, TransportError,
};
use contextdb_core::{AtomicLsn, Incarnation, Lsn, TenantId};
use contextdb_engine::sync_types::{ChangeSet, NaturalKey, SyncAdoption, SyncDirection};
use contextdb_engine::{Conflict, Database};
use std::collections::{BTreeMap, HashMap};
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

type PushRequestKey = Vec<u8>;
type InFlightPushApplies = Arc<tokio::sync::Mutex<HashMap<PushRequestKey, Vec<Responder>>>>;
type ApplyTasks = Arc<ApplyTracker>;

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
        Self {
            db,
            transport,
            lineage_signer,
            local_node_id,
            tenant_id,
            applied_push_watermark: Arc::new(AtomicLsn::new(applied_push_watermark)),
            per_edge_watermarks: Arc::new(PerEdgeAppliedPushWatermarks::default()),
            receipts: Arc::new(TransferLedger::new()),
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
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let lineage_signer = lineage_signer.clone();
                let receipts = receipts.clone();
                let tenant_id = tenant_id.clone();
                let local_node_id = local_node_id.clone();
                Box::pin(async move {
                    handle_pull(db, lineage_signer, receipts, tenant_id, local_node_id, req)
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

        // Contact bookkeeping covers authenticated pull/status exchanges and
        // pushes that reach exact-byte admission. Pull and status use
        // `record_contact`; push records after the admission decision so a
        // database write cannot strand a retry outside its leader's fanout.
        // An unauthenticated request records nothing.
        vec![
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
        ]
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
    let envelope =
        decode(&req.bytes).map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    if !matches!(envelope.message_type, MessageType::StatusRequest) {
        return Err(contextdb_core::Error::SyncError(
            "unexpected message type on status subject".to_string(),
        ));
    }
    let request: SyncStatusRequest = rmp_serde::from_slice(&envelope.payload)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

    let applied = match req.node_id.as_deref() {
        Some(node_id) => per_edge_watermarks.load(&db, &tenant_id, node_id, request.incarnation),
        None => applied_push_watermark.load(Ordering::SeqCst),
    };
    let response = SyncStatusResponse {
        applied_push_watermark: Some(applied),
        server_current_lsn: Some(db.current_lsn()),
    };
    let payload = encode(MessageType::StatusResponse, &response)
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
    let envelope =
        decode(&req.bytes).map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
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
                "protocol v6 production push requires an authenticated peer identity".to_string(),
            ),
            application_error: None,
        };
        publish_push_response(req.responder, response).await?;
        return Ok(());
    };
    if !request.changeset.purges.is_empty() {
        let hub_node_id = state.local_node_id.clone().ok_or_else(|| {
            contextdb_core::Error::SyncError(
                "protocol v6 production push requires the hub's authenticated identity".to_string(),
            )
        })?;
        let response = PushResponse {
            result: None,
            error: None,
            application_error: Some(WirePushError::PurgeRequiresAuthoritativeHub { hub_node_id }),
        };
        publish_push_response(req.responder, response).await?;
        return Ok(());
    }
    let incarnation = request.incarnation;
    let request_key = req.bytes;
    let admission = admit_push_request(
        &state.in_flight_push_applies,
        request_key.clone(),
        req.responder,
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
        let changeset = if let Some(received_ddl) = ddl_context.as_ref() {
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
            )?
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
                .reject_accepted_lineage_replays(&state.tenant_id, changeset, &lineages)?
        };
        Ok::<_, contextdb_core::Error>((changeset, ddl_context, None))
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
                    }
                } else {
                    PushResponse {
                        result: None,
                        error: Some(err.to_string()),
                        application_error: None,
                    }
                };
            publish_in_flight_push_response(
                state.in_flight_push_applies.clone(),
                request_key,
                response,
            )
            .await;
        }
    }
    Ok(())
}

async fn handle_pull(
    db: Arc<Database>,
    lineage_signer: Option<LineageSigner>,
    receipts: Arc<TransferLedger>,
    tenant_id: TenantId,
    local_node_id: Option<String>,
    req: IncomingRequest,
) -> contextdb_core::Result<()> {
    let envelope =
        decode(&req.bytes).map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    if !matches!(envelope.message_type, MessageType::PullRequest) {
        return Err(contextdb_core::Error::SyncError(
            "unexpected message type on pull subject".to_string(),
        ));
    }

    let request: PullRequest = rmp_serde::from_slice(&envelope.payload)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

    // Keep extraction, all serve-time shaping, and its schema-instance
    // evidence on one published schema.  The lease is released before the
    // response is encoded or handed to the transport below.
    let schema_read = db.enter_outbound_sync_schema_read();

    // Refuse pull if the hub has keyless tables that would sync. The changeset
    // cannot represent their rows (which lack a natural key), so silently omitting
    // them would make the pull incomplete. The fix-up is the same as for push:
    // declare a PRIMARY KEY, add an indexed `id` column, or set SYNC OFF.
    refuse_keyless_tables_with_no_identity_fallback(&db, &HashMap::new())?;

    let (mut changes, arrivals, ddl_provenance_source) =
        db.checked_changes_since_with_arrivals(request.since_lsn)?;
    let mut purge_items = db.authoritative_purge_delivery_items_since(request.since_lsn)?;

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
    let cursor = changes
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
    let units = db.dependency_complete_outbound_units(changes, request.since_lsn)?;
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
                "protocol v6 production pull requires the hub's authenticated identity".to_string(),
            )
        })?)
    };
    let signer = lineage_signer.as_ref().ok_or_else(|| {
        contextdb_core::Error::SyncError(
            "protocol v6 production pull requires the hub transport's creator signer".to_string(),
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
        })
        .collect();
    let ordinary_response = PullResponse {
        changeset: ordinary_changeset,
        has_more,
        cursor,
        source: Some(source),
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
            Ok(
                crate::protocol::wire_changeset_with_arrivals_lineages_and_ddl_provenance(
                    unit,
                    &arrivals,
                    &lineages,
                    ddl_provenance,
                ),
            )
        })
        .collect::<contextdb_core::Result<Vec<_>>>()?;
    drop(schema_read);
    let (message_type, payload) = if dependency_units.is_empty() {
        (
            MessageType::PullResponse,
            encode(MessageType::PullResponse, &ordinary_response),
        )
    } else {
        let response = DependencyCompletePullResponse {
            ordinary: ordinary_response,
            units: dependency_units,
        };
        (
            MessageType::DependencyCompletePullResponse,
            encode(MessageType::DependencyCompletePullResponse, &response),
        )
    };
    let _ = message_type;
    let payload = payload.map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    (req.responder)(payload)
        .await
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    Ok(())
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
    } = work;

    let row_count = changeset.rows.len();
    let push_payload_bytes = row_payload_bytes(&changeset.rows);
    let push_max_lsn = changeset.max_lsn();
    #[cfg(feature = "production-smoke-driver")]
    let checkpoint_request_digest = *blake3::hash(&request_key).as_bytes();
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
                    if dependency_complete {
                        db.validate_dependency_complete_unit(&changeset)?;
                    }
                    let result = if let Some(conflicts) = terminal_conflicts {
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
                        )?
                    } else if let (Some(node_id), Some(max_lsn)) =
                        (applying_node_id.as_deref(), push_max_lsn)
                    {
                        db.apply_authenticated_received_changes_with_receipt_and_lineages(
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
                    Ok::<_, contextdb_core::Error>(result)
                })
                .await
                {
                    Ok(Ok(result)) => {
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
                                checkpoint_request_digest,
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
                            },
                            checkpoint,
                        )
                    }
                    Ok(Err(err)) => (
                        PushResponse {
                            result: None,
                            error: Some(err.to_string()),
                            application_error: None,
                        },
                        None,
                    ),
                    Err(err) => (
                        PushResponse {
                            result: None,
                            error: Some(format!("push apply task failed: {err}")),
                            application_error: None,
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

        publish_in_flight_push_response(in_flight_push_applies, request_key, response).await;
    });
    Ok(())
}

/// Admit a request before any database access. The entry remains present until
/// every attached responder has been sent the leader's result.
async fn admit_push_request(
    in_flight_push_applies: &InFlightPushApplies,
    request_key: PushRequestKey,
    responder: Responder,
) -> contextdb_core::Result<PushAdmission> {
    let mut in_flight = in_flight_push_applies.lock().await;
    if let Some(responders) = in_flight.get_mut(&request_key) {
        if responders.len() >= MAX_REPLIES_PER_IN_FLIGHT_PUSH {
            drop(in_flight);
            let response = PushResponse {
                result: None,
                error: Some("sync server push apply duplicate reply fanout full".to_string()),
                application_error: None,
            };
            publish_push_response(responder, response).await?;
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
        };
        publish_push_response(responder, response).await?;
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
            if let Err(err) = publish_push_response(responder, response.clone()).await {
                tracing::error!(error = %err, "failed to publish push response");
            }
        }
    }
}

async fn publish_push_response(
    responder: Responder,
    response: PushResponse,
) -> contextdb_core::Result<()> {
    let payload = encode(MessageType::PushResponse, &response)
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
