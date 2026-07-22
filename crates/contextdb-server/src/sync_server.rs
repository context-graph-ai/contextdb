use crate::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, SyncStatusRequest,
    SyncStatusResponse, decode, encode, row_payload_bytes,
};
use crate::subjects::{pull_subject, push_subject, status_subject};
use crate::transfer_receipts::{TransferDirection, TransferLedger, TransferPlane, TransferReceipt};
use crate::transport::{
    HandlerRegistration, IncomingRequest, RequestHandler, Responder, ServerTransport,
    TransportError,
};
use contextdb_core::{AtomicLsn, Lsn, TenantId};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, SyncDirection};
use std::collections::HashMap;
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

struct PushApplyWork {
    db: Arc<Database>,
    peer_node_id: Option<String>,
    receipts: Arc<TransferLedger>,
    policies: ConflictPolicies,
    responder: Responder,
    request_key: PushRequestKey,
    changeset: ChangeSet,
    tenant_id: TenantId,
    applied_push_watermark: Arc<AtomicLsn>,
    apply_tasks: ApplyTasks,
    in_flight_push_applies: InFlightPushApplies,
    apply_permits: Arc<Semaphore>,
}

struct PushHandlerState {
    db: Arc<Database>,
    receipts: Arc<TransferLedger>,
    policies: ConflictPolicies,
    tenant_id: TenantId,
    applied_push_watermark: Arc<AtomicLsn>,
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

    while !release_path.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

pub struct SyncServer {
    db: Arc<Database>,
    transport: Arc<dyn ServerTransport>,
    tenant_id: TenantId,
    policies: ConflictPolicies,
    /// Highest edge-LSN applied from pushes for this tenant. `Lsn(0)`
    /// means "no record" at the storage layer. The status surface reports it
    /// as `Some(Lsn(0))` so restored artifacts can still signal regression.
    applied_push_watermark: Arc<AtomicLsn>,
    /// Per-peer transfer counters for the sync plane. In memory only.
    receipts: Arc<TransferLedger>,
}

impl SyncServer {
    pub fn new(
        db: Arc<Database>,
        nats_url: &str,
        tenant_id: TenantId,
        policies: ConflictPolicies,
    ) -> Self {
        Self::build(
            db,
            crate::transport::server_transport(nats_url),
            tenant_id,
            policies,
        )
    }

    fn build(
        db: Arc<Database>,
        transport: Arc<dyn ServerTransport>,
        tenant_id: TenantId,
        mut policies: ConflictPolicies,
    ) -> Self {
        // The work ledger's per-table conflict rules are part of its
        // distributed contract, not an operator tunable: a hub arbitrating
        // work_claims with anything but the ledger's policy breaks the lease.
        // Baked here on the shared constructor path so every hub binary
        // (OSS and enterprise) inherits it with no per-binary change.
        contextdb_engine::work_ledger::apply_work_ledger_policy_overrides(&mut policies);
        contextdb_engine::peer_directory::apply_peer_directory_policy_overrides(&mut policies);
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
            tenant_id,
            policies,
            applied_push_watermark: Arc::new(AtomicLsn::new(applied_push_watermark)),
            receipts: Arc::new(TransferLedger::new()),
        }
    }

    /// Construct a server that serves over `transport` instead of the default
    /// transport. Used to drive sync with no broker.
    pub fn with_transport(
        db: Arc<Database>,
        transport: Arc<dyn crate::transport::ServerTransport>,
        tenant_id: TenantId,
        policies: ConflictPolicies,
    ) -> Self {
        Self::build(db, transport, tenant_id, policies)
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
                receipts: self.receipts.clone(),
                policies: self.policies.clone(),
                tenant_id: self.tenant_id.clone(),
                applied_push_watermark: self.applied_push_watermark.clone(),
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
            let receipts = self.receipts.clone();
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let receipts = receipts.clone();
                Box::pin(async move {
                    handle_pull(db, receipts, req)
                        .await
                        .map_err(to_transport_error)
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        let status_handler = {
            let db = self.db.clone();
            let applied_push_watermark = self.applied_push_watermark.clone();
            Arc::new(move |req: IncomingRequest| {
                let db = db.clone();
                let applied_push_watermark = applied_push_watermark.clone();
                Box::pin(async move {
                    handle_status(db, applied_push_watermark, req)
                        .await
                        .map_err(to_transport_error)
                }) as crate::transport::TransportFuture<'static, ()>
            }) as RequestHandler
        };

        // Single serve-path chokepoint: every exchange type (push / pull /
        // status) passes through `record_contact` before its own handler, so
        // the hub's per-node last-contact clock advances on ANY contact — no
        // exchange type can miss it. Recording is keyed on the transport-
        // authenticated node id; an exchange with no authenticated identity
        // (the deprecated broker path) records nothing.
        vec![
            HandlerRegistration {
                subject: push_subject(self.tenant_id.as_str()),
                handler: self.record_contact(push_handler),
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

    /// Wrap `inner` so the hub records the requesting node's last-contact
    /// before dispatching. This is the one place last-contact is advanced, so
    /// every served exchange is covered by construction.
    fn record_contact(&self, inner: RequestHandler) -> RequestHandler {
        let db = self.db.clone();
        Arc::new(move |req: IncomingRequest| {
            let db = db.clone();
            let inner = inner.clone();
            Box::pin(async move {
                if let Some(node_id) = req.node_id.clone()
                    && let Err(err) =
                        crate::work_ledger::record_node_contact(&db, &node_id, hub_now_ms())
                {
                    tracing::warn!(%node_id, error = %err, "failed to record node last-contact");
                }
                inner(req).await
            }) as crate::transport::TransportFuture<'static, ()>
        }) as RequestHandler
    }
}

/// Tables the hub keeps to itself: mapped to `SyncDirection::None` so the pull
/// filter drops their rows and DDL before any changeset leaves the hub. Today
/// this is only `work_node_contacts` (the per-node liveness clock).
fn hub_never_sync_directions() -> HashMap<String, SyncDirection> {
    let mut directions = HashMap::new();
    directions.insert(
        crate::work_ledger::WORK_NODE_CONTACTS_TABLE.to_string(),
        SyncDirection::None,
    );
    directions
}

/// The hub's wall clock in ms since the Unix epoch — the timestamp stamped on
/// a recorded node contact. A clock before the epoch records 0 (never panics).
fn hub_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn to_transport_error(err: contextdb_core::Error) -> TransportError {
    TransportError::Other(err.to_string())
}

async fn handle_status(
    db: Arc<Database>,
    applied_push_watermark: Arc<AtomicLsn>,
    req: IncomingRequest,
) -> contextdb_core::Result<()> {
    let envelope =
        decode(&req.bytes).map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    if !matches!(envelope.message_type, MessageType::StatusRequest) {
        return Err(contextdb_core::Error::SyncError(
            "unexpected message type on status subject".to_string(),
        ));
    }
    let _request: SyncStatusRequest = rmp_serde::from_slice(&envelope.payload)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

    let applied = applied_push_watermark.load(Ordering::SeqCst);
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
    if !matches!(envelope.message_type, MessageType::PushRequest) {
        return Err(contextdb_core::Error::SyncError(
            "unexpected message type on push subject".to_string(),
        ));
    }
    let request: PushRequest = rmp_serde::from_slice(&envelope.payload)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    let request_key = req.bytes;

    match ChangeSet::try_from(request.changeset) {
        Ok(changeset) => {
            spawn_apply_and_reply(PushApplyWork {
                db: state.db.clone(),
                peer_node_id: req.node_id.clone(),
                receipts: state.receipts.clone(),
                policies: state.policies.clone(),
                responder: req.responder,
                request_key,
                changeset,
                tenant_id: state.tenant_id.clone(),
                applied_push_watermark: state.applied_push_watermark.clone(),
                apply_tasks: state.apply_tasks.clone(),
                in_flight_push_applies: state.in_flight_push_applies.clone(),
                apply_permits: state.apply_permits.clone(),
            })
            .await?;
        }
        Err(err) => {
            let response = PushResponse {
                result: None,
                error: Some(err.to_string()),
            };
            publish_push_response(req.responder, response).await?;
        }
    }
    Ok(())
}

async fn handle_pull(
    db: Arc<Database>,
    receipts: Arc<TransferLedger>,
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
    let mut changes = db.changes_since(request.since_lsn);

    let mut has_more = false;
    let mut cursor = None;
    if let Some(max_entries) = request.max_entries {
        let max = max_entries as usize;
        let groups = changes
            .clone()
            .split_by_data_lsn()
            .into_iter()
            .filter(|group| group.data_entry_count() > 0 || !group.ddl.is_empty())
            .collect::<Vec<_>>();

        if !groups.is_empty()
            && groups
                .iter()
                .map(ChangeSet::data_entry_count)
                .sum::<usize>()
                > max
        {
            let mut selected = Vec::new();
            let mut selected_entries = 0usize;
            for group in &groups {
                let group_entries = group.data_entry_count().max(group.ddl.len()).max(1);
                if !selected.is_empty() && selected_entries + group_entries > max {
                    break;
                }
                selected_entries = selected_entries.saturating_add(group_entries);
                selected.push(group.clone());
                if selected_entries >= max {
                    break;
                }
            }
            has_more = selected.len() < groups.len();
            changes = merge_changeset_groups(selected);
        }
    }
    let mut bootstrap_batches = changes.clone().split_at_trigger_bootstrap_barriers();
    if bootstrap_batches.len() > 1 {
        changes = bootstrap_batches.remove(0);
        has_more = true;
    }
    if request.max_entries.is_some() || has_more {
        cursor = changes.max_lsn().or_else(|| {
            if changes.ddl.is_empty() {
                None
            } else {
                Some(db.current_lsn())
            }
        });
    }

    // Never ship hub-local tables to an edge. `work_node_contacts` is the
    // hub's private per-node liveness clock; the cursor above was already
    // computed from the full frontier, so dropping these rows here excludes
    // them from the wire WITHOUT stranding the edge's pull watermark.
    let changes = changes.filter_by_direction(
        &hub_never_sync_directions(),
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
    receipts.record(
        req.node_id.as_deref(),
        TransferPlane::Sync,
        TransferDirection::Sent,
        changes.rows.len() as u64,
        row_payload_bytes(&changes.rows),
    );
    let response = PullResponse {
        changeset: changes.into(),
        has_more,
        cursor,
    };
    let payload = encode(MessageType::PullResponse, &response)
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    (req.responder)(payload)
        .await
        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
    Ok(())
}

async fn spawn_apply_and_reply(work: PushApplyWork) -> contextdb_core::Result<()> {
    let PushApplyWork {
        db,
        peer_node_id,
        receipts,
        policies,
        responder,
        request_key,
        changeset,
        tenant_id,
        applied_push_watermark,
        apply_tasks,
        in_flight_push_applies,
        apply_permits,
    } = work;

    {
        let mut in_flight = in_flight_push_applies.lock().await;
        if let Some(responders) = in_flight.get_mut(&request_key) {
            if responders.len() >= MAX_REPLIES_PER_IN_FLIGHT_PUSH {
                drop(in_flight);
                let response = PushResponse {
                    result: None,
                    error: Some("sync server push apply duplicate reply fanout full".to_string()),
                };
                return publish_push_response(responder, response).await;
            }
            responders.push(responder);
            return Ok(());
        }
        if in_flight.len() >= MAX_IN_FLIGHT_PUSH_APPLIES {
            drop(in_flight);
            let response = PushResponse {
                result: None,
                error: Some("sync server push apply backlog full".to_string()),
            };
            return publish_push_response(responder, response).await;
        }
        in_flight.insert(request_key.clone(), vec![responder]);
    }

    let row_count = changeset.rows.len();
    let push_payload_bytes = row_payload_bytes(&changeset.rows);
    let push_max_lsn = changeset.max_lsn();
    let guard = apply_tasks.start();
    tokio::spawn(async move {
        let _guard = guard;
        maybe_wait_for_test_push_barrier(row_count).await;
        let response = match apply_permits.acquire_owned().await {
            Ok(_permit) => {
                match tokio::task::spawn_blocking(move || {
                    let result = db.apply_changes(changeset, &policies)?;
                    if let Some(max_lsn) = push_max_lsn {
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
                        PushResponse {
                            result: Some(result.into()),
                            error: None,
                        }
                    }
                    Ok(Err(err)) => PushResponse {
                        result: None,
                        error: Some(err.to_string()),
                    },
                    Err(err) => PushResponse {
                        result: None,
                        error: Some(format!("push apply task failed: {err}")),
                    },
                }
            }
            Err(err) => PushResponse {
                result: None,
                error: Some(format!("push apply semaphore closed: {err}")),
            },
        };

        let responders = {
            let mut in_flight = in_flight_push_applies.lock().await;
            in_flight.remove(&request_key).unwrap_or_default()
        };

        for responder in responders {
            if let Err(err) = publish_push_response(responder, response.clone()).await {
                tracing::error!(error = %err, "failed to publish push response");
            }
        }
    });
    Ok(())
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
