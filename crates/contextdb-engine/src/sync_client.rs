// Canonical authenticated-sync implementation. The server-path mirror is
// intentionally byte-identical and audited by `sync_source_mirror_tests`.
use crate::protocol::{
    DependencyCompletePullResponse, MessageType, PullRequest, PullResponse, PushRequest,
    PushResponse, SyncStatusRequest, SyncStatusResponse, WireChangeSet, WirePurgeChange,
    WirePushError, decode, encode, row_payload_bytes,
};
use crate::subjects::{pull_subject, push_subject, status_subject};
use crate::transfer_receipts::{TransferDirection, TransferLedger, TransferPlane, TransferReceipt};
use crate::transport::{ClientTransport, LineageSigner, TransportError};
use contextdb_core::{AtomicLsn, Error, Incarnation, Lsn, TableMeta, TenantId};
use contextdb_engine::Database;
use contextdb_engine::database::{AuthoritativePurgeDeliveryItem, TerminalRefusalPullContext};
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, PURGED_LINEAGE_CONFLICT_REASON, REMOVED_GENERATION_CONFLICT_REASON,
    SyncAdoption, SyncDirection,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
#[cfg(feature = "test-seams")]
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "test-seams")]
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;
#[cfg(feature = "test-seams")]
use tokio::sync::Notify;

const SYNC_TIMEOUT: Duration = Duration::from_secs(60);
// A legitimate dependency-complete batch can spend more than four seconds in
// one atomic commit on debug, embedded, or loaded nodes. Reuse the declared
// sync operation ceiling so the client does not reset an accepted stream and
// manufacture an unconfirmed outcome while the hub is still committing it.
const PUSH_REQUEST_TIMEOUT: Duration = SYNC_TIMEOUT;
/// Bound on the sync-status probe. No responder / timeout / malformed
/// reply all degrade to "no status" and the sync proceeds exactly as before
/// the probe existed (contract item 5: never hang against an old server).
const STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const PULL_PAGE_SIZE: u32 = 500;
const MAX_BATCH_BYTES: usize = 800 * 1024;
const BATCH_ESTIMATE_SAFETY_MARGIN: usize = 32 * 1024;
const TARGET_BATCH_BYTES: usize = MAX_BATCH_BYTES - BATCH_ESTIMATE_SAFETY_MARGIN;
const MAX_BATCH_DATA_LSN_GROUPS: usize = 100;

/// Database-dependent push evidence captured under one schema publication
/// read. The raw changes stay unencoded so transport retries retain their
/// existing per-request encoding and never hold the schema lock over I/O.
struct PreparedPushBatch {
    batch: ChangeSet,
    dependency_complete: bool,
    batch_max_lsn: Lsn,
    batch_items: u64,
    batch_payload_bytes: u64,
    arrivals: HashMap<Lsn, Option<Lsn>>,
    lineages: HashMap<(String, Vec<u8>, Lsn), crate::protocol::WireRowLineage>,
    ddl_provenance: Vec<crate::protocol::WireDdlProvenance>,
}

pub struct SyncClient {
    db: Arc<Database>,
    transport: Arc<dyn ClientTransport>,
    lineage_signer: Option<LineageSigner>,
    endpoint: String,
    tenant_id: TenantId,
    push_watermark: AtomicLsn,
    /// A confirmed hub push whose exact ordering still needs a pull. This is
    /// in memory for memory databases and persisted for file-backed ones.
    pending_push_confirmation: AtomicLsn,
    pull_watermark: AtomicLsn,
    /// Pages served during a pull, then discarded unapplied because they
    /// reported an incarnation other than the one this client's cursor
    /// addresses. Zero today: nothing yet detects the mismatch, so a
    /// mismatched page is applied as if it were a legitimate continuation.
    /// Cumulative across every `pull`/`pull_default` call on this client.
    pages_discarded_for_source_mismatch: AtomicUsize,
    /// The store this client's pull cursor addresses — the serving store's
    /// incarnation last seen on a pull response. `None` until the first
    /// successful pull binds it. Loaded from the persisted `(source, lsn)`
    /// pair at construction, so this survives a restart bound to the SAME
    /// store as the cursor it accompanies.
    pull_source: std::sync::RwLock<Option<Incarnation>>,
    /// Per-peer transfer counters for the sync plane. In memory only.
    receipts: Arc<TransferLedger>,
    #[cfg(feature = "test-seams")]
    post_push_reply_effects_pause: Mutex<Option<(Arc<Notify>, Receiver<()>)>>,
    #[cfg(feature = "test-seams")]
    post_pull_response_pause: Mutex<Option<(Arc<Notify>, Receiver<()>)>>,
}

/// Production-dead deterministic fence after a decoded authenticated response
/// and before its authoritative-peer state transition.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub struct PostPushReplyEffectsPause {
    reached: Arc<Notify>,
    release: Sender<()>,
}

#[cfg(feature = "test-seams")]
impl PostPushReplyEffectsPause {
    pub async fn wait_until_reached(&self) {
        self.reached.notified().await;
    }

    pub fn release(&self) {
        let _ = self.release.send(());
    }
}

#[cfg(feature = "test-seams")]
impl Drop for PostPushReplyEffectsPause {
    fn drop(&mut self) {
        self.release();
    }
}

struct PullPage {
    ordinary: PullResponse,
    dependency_units: Vec<WireChangeSet>,
}

struct AppliedPullPage {
    result: ApplyResult,
    suppressed_live_replay_only: bool,
    has_deliverable_changes: bool,
    has_create_trigger: bool,
    received_items: u64,
    received_payload_bytes: u64,
}

fn malformed_purge_page(detail: impl Into<String>) -> Error {
    Error::SyncError(format!(
        "malformed authoritative purge page: {}",
        detail.into()
    ))
}

fn take_validated_ordinary_purges(
    changeset: &mut WireChangeSet,
    since_lsn: Lsn,
    cursor: Option<Lsn>,
) -> Result<Vec<AuthoritativePurgeDeliveryItem>, Error> {
    let purges = std::mem::take(&mut changeset.purges);
    if purges.is_empty() {
        return Ok(Vec::new());
    }
    let page_cursor = cursor.ok_or_else(|| {
        malformed_purge_page("a nonempty purge page omitted its consumed frontier cursor")
    })?;
    let mut previous_frontier = None;
    let mut seen = Vec::<WirePurgeChange>::with_capacity(purges.len());
    let mut validated = Vec::with_capacity(purges.len());
    for (ordinal, purge) in purges.into_iter().enumerate() {
        if purge.table.is_empty() {
            return Err(malformed_purge_page("purge table is empty"));
        }
        if purge.table_generation == 0 {
            return Err(malformed_purge_page(format!(
                "purge table {} has generation zero",
                purge.table
            )));
        }
        if purge.natural_key.column.is_empty()
            || purge
                .natural_key
                .rest
                .iter()
                .any(|(column, _)| column.is_empty())
        {
            return Err(malformed_purge_page(format!(
                "purge table {} has an empty natural-key column",
                purge.table
            )));
        }
        let mut key_columns = BTreeSet::new();
        key_columns.insert(purge.natural_key.column.as_str());
        if purge
            .natural_key
            .rest
            .iter()
            .any(|(column, _)| !key_columns.insert(column.as_str()))
        {
            return Err(malformed_purge_page(format!(
                "purge table {} repeats a natural-key column",
                purge.table
            )));
        }
        if purge.purged_lineage_roots.is_empty()
            || purge
                .purged_lineage_roots
                .iter()
                .any(|root| root.is_empty())
        {
            return Err(malformed_purge_page(format!(
                "purge table {} has no complete lineage-root evidence",
                purge.table
            )));
        }
        let mut unique_roots = BTreeSet::new();
        if purge
            .purged_lineage_roots
            .iter()
            .any(|root| !unique_roots.insert(root.as_str()))
        {
            return Err(malformed_purge_page(format!(
                "purge table {} repeats lineage-root evidence",
                purge.table
            )));
        }
        if purge.purge_frontier == Lsn(0) {
            return Err(malformed_purge_page(format!(
                "purge table {} has frontier zero",
                purge.table
            )));
        }
        if purge.purge_frontier <= since_lsn || purge.purge_frontier > page_cursor {
            return Err(malformed_purge_page(format!(
                "purge frontier {} is outside page ({}, {}]",
                purge.purge_frontier.0, since_lsn.0, page_cursor.0
            )));
        }
        if previous_frontier.is_some_and(|previous| purge.purge_frontier < previous) {
            return Err(malformed_purge_page(format!(
                "purge frontier {} follows a later frontier",
                purge.purge_frontier.0
            )));
        }
        if seen.iter().any(|previous| {
            previous.table == purge.table
                && previous.table_generation == purge.table_generation
                && previous.natural_key == purge.natural_key
        }) {
            return Err(malformed_purge_page(format!(
                "purge table {} repeats a natural-key target",
                purge.table
            )));
        }
        previous_frontier = Some(purge.purge_frontier);
        let ordinal = u32::try_from(ordinal)
            .map_err(|_| malformed_purge_page("purge page contains too many delivery items"))?;
        validated.push(AuthoritativePurgeDeliveryItem {
            frontier: purge.purge_frontier,
            ordinal,
            table: purge.table.clone(),
            table_generation: purge.table_generation,
            natural_key: purge.natural_key.clone().into(),
            purged_lineage_roots: purge.purged_lineage_roots.clone(),
        });
        seen.push(purge);
    }
    Ok(validated)
}

impl std::fmt::Debug for SyncClient {
    /// `db` is the engine handle and `transport` is the connection object —
    /// neither implements `Debug`, so both are elided as placeholders. Every
    /// other field is safe to print in full: no secrets live on this struct.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncClient")
            .field("db", &"..")
            .field("transport", &"..")
            .field("endpoint", &self.endpoint)
            .field("tenant_id", &self.tenant_id)
            .field(
                "push_watermark",
                &self.push_watermark.load(Ordering::Relaxed),
            )
            .field(
                "pending_push_confirmation",
                &self.pending_push_confirmation.load(Ordering::Relaxed),
            )
            .field(
                "pull_watermark",
                &self.pull_watermark.load(Ordering::Relaxed),
            )
            .field(
                "pages_discarded_for_source_mismatch",
                &self
                    .pages_discarded_for_source_mismatch
                    .load(Ordering::Relaxed),
            )
            .field("pull_source", &self.pull_source.read().map(|g| *g).ok())
            .finish()
    }
}

impl SyncClient {
    pub fn new(db: Arc<Database>, endpoint: &str, tenant_id: TenantId) -> Self {
        let (transport, lineage_signer) =
            crate::transport::sync_client_transport_with_lineage_signer(
                endpoint,
                db.sync_identity_path(),
            );
        Self::build(
            db,
            transport,
            lineage_signer,
            endpoint.to_string(),
            tenant_id,
        )
    }

    fn build(
        db: Arc<Database>,
        transport: Arc<dyn ClientTransport>,
        lineage_signer: Option<LineageSigner>,
        endpoint: String,
        tenant_id: TenantId,
    ) -> Self {
        assert!(
            !tenant_id.as_str().is_empty()
                && tenant_id
                    .as_str()
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '-' || c == '_'),
            "tenant_id must be non-empty and alphanumeric (hyphens and underscores allowed): {tenant_id}"
        );
        let (push_watermark, pull_watermark) = db
            .persisted_sync_watermarks(&tenant_id)
            .unwrap_or_else(|err| {
                tracing::warn!(%tenant_id, error = %err, "failed to load persisted sync watermarks");
                (Lsn(0), Lsn(0))
        });
        // Only the SOURCE half loads here — the lsn half of the persisted
        // pair is not consulted at construction so an edge upgrading from a
        // build with no combined-cursor record yet keeps resuming from its
        // existing `pull_watermark` unchanged; the first pull after upgrade
        // simply starts recording source identity going forward.
        let pull_source = db
            .persisted_sync_pull_cursor(&tenant_id)
            .unwrap_or_else(|err| {
                tracing::warn!(%tenant_id, error = %err, "failed to load persisted pull cursor source");
                None
            })
            .map(|(source, _lsn)| source);
        let pending_push_confirmation = db
            .persisted_sync_pending_push_confirmation(&tenant_id)
            .unwrap_or_else(|err| {
                tracing::warn!(%tenant_id, error = %err, "failed to load pending sync push confirmation");
                None
            })
            .unwrap_or(Lsn(0));
        // Preserve construction-time binding for the ordinary first-use
        // path, but never cache its result: an operator can explicitly move
        // the durable destination on this same long-lived client. Push and
        // pull repeat the authoritative durable read immediately before I/O.
        if let Some(hub) = transport.peer_node_id() {
            let _ = db.register_retention_sync_peer(&hub);
        }
        Self {
            db,
            transport,
            lineage_signer,
            endpoint,
            tenant_id,
            push_watermark: AtomicLsn::new(push_watermark),
            pending_push_confirmation: AtomicLsn::new(pending_push_confirmation),
            pull_watermark: AtomicLsn::new(pull_watermark),
            pages_discarded_for_source_mismatch: AtomicUsize::new(0),
            pull_source: std::sync::RwLock::new(pull_source),
            receipts: Arc::new(TransferLedger::new()),
            #[cfg(feature = "test-seams")]
            post_push_reply_effects_pause: Mutex::new(None),
            #[cfg(feature = "test-seams")]
            post_pull_response_pause: Mutex::new(None),
        }
    }

    /// Test-only authenticated transport injection. The supplied transport
    /// remains the source of the client's peer identity.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn with_authenticated_transport_for_test(
        db: Arc<Database>,
        transport: Arc<dyn ClientTransport>,
        tenant_id: TenantId,
    ) -> Self {
        Self::build(
            db,
            transport,
            None,
            "authenticated-test-transport".to_string(),
            tenant_id,
        )
    }

    /// Test-only construction with the fabric key matching the injected
    /// transport's advertised node id. Production construction obtains this
    /// internally from the authenticated transport endpoint.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn with_authenticated_transport_and_identity_for_test(
        db: Arc<Database>,
        transport: Arc<dyn ClientTransport>,
        tenant_id: TenantId,
        identity: Arc<crate::identity::FabricIdentity>,
    ) -> Self {
        let signer: LineageSigner = Arc::new(move |bytes| Ok(identity.sign_lineage(bytes)));
        Self::build(
            db,
            transport,
            Some(signer),
            "authenticated-test-transport".to_string(),
            tenant_id,
        )
    }

    /// Lazily connect the configured transport and reuse its connection.
    pub async fn ensure_connected(&self) -> Result<(), String> {
        self.transport
            .ensure_connected()
            .await
            .map_err(|err| err.to_string())
    }

    /// Drop existing connection and reconnect.
    pub async fn reconnect(&self) {
        let _ = self.transport.reconnect().await;
    }

    pub async fn is_connected(&self) -> bool {
        self.transport.is_connected().await
    }

    /// Release the transport's resources gracefully before process exit
    /// (closes the sync connection and its endpoint).
    pub async fn shutdown(&self) {
        let _ = self.transport.shutdown().await;
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn pause_after_push_response_for_test(&self) -> PostPushReplyEffectsPause {
        let reached = Arc::new(Notify::new());
        let (release, release_receiver) = mpsc::channel();
        *self
            .post_push_reply_effects_pause
            .lock()
            .expect("push reply pause mutex is not poisoned") =
            Some((reached.clone(), release_receiver));
        PostPushReplyEffectsPause { reached, release }
    }

    #[cfg(feature = "test-seams")]
    fn pause_after_push_response_for_test_if_armed(&self) {
        let pause = self
            .post_push_reply_effects_pause
            .lock()
            .expect("push reply pause mutex is not poisoned")
            .take();
        if let Some((reached, release)) = pause {
            reached.notify_one();
            let _ = release.recv();
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn pause_after_pull_response_for_test(&self) -> PostPushReplyEffectsPause {
        let reached = Arc::new(Notify::new());
        let (release, release_receiver) = mpsc::channel();
        *self
            .post_pull_response_pause
            .lock()
            .expect("pull response pause mutex is not poisoned") =
            Some((reached.clone(), release_receiver));
        PostPushReplyEffectsPause { reached, release }
    }

    #[cfg(feature = "test-seams")]
    fn pause_after_pull_response_for_test_if_armed(&self) {
        let pause = self
            .post_pull_response_pause
            .lock()
            .expect("pull response pause mutex is not poisoned")
            .take();
        if let Some((reached, release)) = pause {
            reached.notify_one();
            let _ = release.recv();
        }
    }

    #[cfg(not(feature = "test-seams"))]
    fn pause_after_push_response_for_test_if_armed(&self) {}

    #[cfg(not(feature = "test-seams"))]
    fn pause_after_pull_response_for_test_if_armed(&self) {}

    /// What this edge has moved with its hub, per direction, since the client
    /// was constructed. Monotonic and in memory only; the engine persists none
    /// of it, so a fresh client on the same database starts from zero.
    pub fn transfer_receipts(&self) -> Vec<TransferReceipt> {
        self.receipts.receipts()
    }

    /// The hub's transport-authenticated node id, when the transport
    /// authenticates one. The default transport dials by key, so this is the
    /// hub the edge is provably talking to.
    fn hub_node_id(&self) -> Option<String> {
        self.transport.peer_node_id()
    }

    /// One durable authenticated hub governs sync authority, not only
    /// retained delivery. The existing retention binding is that authority's
    /// single source of truth; a different peer is refused before connection
    /// activity or row mutation.
    fn bind_authenticated_hub(&self) -> Result<(), Error> {
        let hub = self.hub_node_id().ok_or_else(|| {
            Error::SyncError(
                "sync requires the transport-authenticated authoritative hub identity".to_string(),
            )
        })?;
        self.db.register_retention_sync_peer(&hub)
    }

    pub fn has_pending_push_changes(&self) -> Result<bool, Error> {
        Ok(!self.pending_push_changes()?.is_empty())
    }

    /// The exact number of entries this client currently considers eligible
    /// for push from its durable/in-memory push watermark.
    pub fn pending_push_change_count(&self) -> Result<usize, Error> {
        let changes = self.pending_push_changes()?;
        Ok(changes.data_entry_count() + changes.ddl.len())
    }

    /// One local snapshot feeds both pending-work inspection APIs, so the
    /// boolean and exact count cannot diverge on direction or arrival policy.
    fn pending_push_changes(&self) -> Result<ChangeSet, Error> {
        let _schema_read = self.db.enter_outbound_sync_schema_read();
        let since = self.push_watermark.load(Ordering::SeqCst);
        let (changes, _, ddl_provenance_source) =
            self.db.checked_changes_since_with_arrivals(since)?;
        let history = self.db.sync_direction_history();
        let changes = changes
            .filter_by_direction_history(&history, &[SyncDirection::Push, SyncDirection::Both]);
        let reupload_epoch = self.destination_reupload_epoch()?;
        let changes = self.db.filter_outbound_received_ddl(
            changes,
            &ddl_provenance_source,
            reupload_epoch.map(|(through, _)| through),
        )?;
        let changes = match reupload_epoch {
            Some((through, _)) => self
                .db
                .append_destination_reupload_deletes(changes, through)?,
            None => changes,
        };
        Ok(match reupload_epoch {
            Some((through, _)) => {
                drop_rows_that_arrived_by_sync_after(&self.db, changes, Some(through))
            }
            None => drop_rows_that_arrived_by_sync(&self.db, changes),
        })
    }

    fn destination_reupload_epoch(&self) -> Result<Option<(Lsn, uuid::Uuid)>, Error> {
        let Some(hub_node_id) = self.hub_node_id() else {
            return Ok(None);
        };
        self.db
            .destination_reupload_epoch(&self.tenant_id, &hub_node_id)
    }

    /// Stale-restore probe: best-effort, bounded status exchange on the
    /// dedicated per-tenant status subject. The request carries this edge's
    /// incarnation so the hub answers with the record for THIS life of the edge.
    /// Every failure mode (no responder, timeout, transport status reply,
    /// malformed payload) yields `None`, and the caller proceeds exactly as
    /// today.
    async fn fetch_sync_status(&self, incarnation: Incarnation) -> Option<SyncStatusResponse> {
        let encoded = encode(
            MessageType::StatusRequest,
            &SyncStatusRequest { incarnation },
        )
        .ok()?;
        let reply = self
            .transport
            .request_single_reply(
                &status_subject(self.tenant_id.as_str()),
                encoded,
                STATUS_REQUEST_TIMEOUT,
            )
            .await
            .ok()?;
        let envelope = decode(&reply).ok()?;
        if !matches!(envelope.message_type, MessageType::StatusResponse) {
            return None;
        }
        rmp_serde::from_slice(&envelope.payload).ok()
    }

    async fn request_push_once(&self, encoded: Vec<u8>) -> Result<ApplyResult, PushRequestError> {
        let reply = self
            .transport
            .request(
                &push_subject(self.tenant_id.as_str()),
                encoded,
                SYNC_TIMEOUT,
            )
            .await
            .map_err(|err| {
                PushRequestError::Ambiguous(Error::SyncError(format!(
                    "single-attempt push failed: {err}"
                )))
            })?;
        decode_push_response(&reply).map_err(PushReplyError::into_request_error)
    }

    async fn request_push(&self, encoded: Vec<u8>) -> Result<ApplyResult, PushRequestError> {
        match self.transport.ensure_single_reply_retry_safe(&encoded) {
            Ok(()) => {}
            Err(TransportError::RetryUnsafe(detail)) => {
                tracing::debug!(
                    %detail,
                    "push request requires a single-attempt transport send"
                );
                return self.request_push_once(encoded).await;
            }
            Err(err) => {
                return Err(PushRequestError::Terminal(Error::SyncError(format!(
                    "push failed: {err}"
                ))));
            }
        }

        let mut push_result = None;
        for attempt in 0..5u32 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }

            match self
                .transport
                .request_single_reply(
                    &push_subject(self.tenant_id.as_str()),
                    encoded.clone(),
                    PUSH_REQUEST_TIMEOUT,
                )
                .await
            {
                Ok(reply) => match decode_push_response(&reply) {
                    Ok(result) => {
                        push_result = Some(result);
                        break;
                    }
                    Err(PushReplyError::Malformed(err)) if attempt < 4 => {
                        tracing::debug!(attempt, error = %err, "push got malformed reply, retrying");
                        continue;
                    }
                    Err(PushReplyError::Malformed(err)) => {
                        return Err(PushRequestError::Ambiguous(err));
                    }
                    Err(PushReplyError::Terminal(err)) => {
                        return Err(PushRequestError::Terminal(err));
                    }
                },
                Err(
                    TransportError::NoResponder
                    | TransportError::Status(_)
                    | TransportError::Timeout,
                ) if attempt < 4 => {
                    tracing::debug!(attempt, "push got retryable transport miss, retrying");
                    continue;
                }
                Err(TransportError::IncompleteReply(detail)) => {
                    return Err(PushRequestError::Ambiguous(Error::SyncError(format!(
                        "push response incomplete: {detail}"
                    ))));
                }
                Err(err) => {
                    return Err(PushRequestError::Ambiguous(Error::SyncError(format!(
                        "push failed: {err}"
                    ))));
                }
            }
        }
        push_result.ok_or_else(|| {
            PushRequestError::Ambiguous(Error::SyncError(
                "push failed after retries: no response from server".to_string(),
            ))
        })
    }

    async fn request_pull(&self, request: PullRequest) -> Result<PullPage, Error> {
        let encoded = encode(MessageType::PullRequest, &request)
            .map_err(|e| Error::SyncError(e.to_string()))?;

        let mut first_attempt_response = None;
        for attempt in 0..5u32 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }
            let timeout = if attempt < 2 {
                Duration::from_secs(2)
            } else {
                SYNC_TIMEOUT
            };

            match self
                .transport
                .request(
                    &pull_subject(self.tenant_id.as_str()),
                    encoded.clone(),
                    timeout,
                )
                .await
            {
                Ok(reply) => {
                    first_attempt_response = Some(reply);
                    break;
                }
                Err(TransportError::Timeout) if attempt < 4 => {
                    tracing::debug!(attempt, "pull timed out, retrying");
                    continue;
                }
                Err(TransportError::IncompleteReply(detail)) => {
                    return Err(Error::SyncError(format!(
                        "pull response incomplete: {detail}"
                    )));
                }
                Err(err) => return Err(Error::SyncError(format!("pull failed: {err}"))),
            }
        }

        let reply = first_attempt_response.ok_or_else(|| {
            Error::SyncError("pull request timed out waiting for response".to_string())
        })?;
        let envelope = decode(&reply).map_err(|e| Error::SyncError(e.to_string()))?;
        match envelope.message_type {
            MessageType::PullResponse => rmp_serde::from_slice(&envelope.payload)
                .map(|ordinary| PullPage {
                    ordinary,
                    dependency_units: Vec::new(),
                })
                .map_err(|e| Error::SyncError(e.to_string())),
            MessageType::DependencyCompletePullResponse => {
                let response: DependencyCompletePullResponse =
                    rmp_serde::from_slice(&envelope.payload)
                        .map_err(|e| Error::SyncError(e.to_string()))?;
                Ok(PullPage {
                    ordinary: response.ordinary,
                    dependency_units: response.units,
                })
            }
            _ => Err(Error::SyncError(
                "unexpected message type in pull response".to_string(),
            )),
        }
    }

    pub async fn push(&self) -> Result<ApplyResult, Error> {
        self.bind_authenticated_hub()?;
        let authenticated_hub = self.hub_node_id().ok_or_else(|| {
            Error::SyncError(
                "sync requires the transport-authenticated authoritative hub identity".to_string(),
            )
        })?;
        // Verify connectivity only after the durable authority gate accepts
        // this authenticated ticket, even for an otherwise empty push.
        self.ensure_connected().await.map_err(Error::SyncError)?;

        // This life's incarnation, stamped on both the status probe and every
        // push below so the hub keys its per-edge record by (node_id,
        // incarnation). A wiped-and-recreated edge reusing its node id mints a
        // fresh one, so the hub answers Lsn(0) for it and its interrupted push
        // is never confirmed by the prior life's stale-high watermark.
        let incarnation = self
            .db
            .sync_incarnation(&self.tenant_id)
            .map_err(|err| Error::SyncError(err.to_string()))?;

        // Exchange status with the server before computing the changeset
        // — including when there is nothing locally new (contract item 2). A
        // server whose applied-push watermark is behind ours was restored from
        // a stale artifact and silently lost acked commits; regress the local
        // push watermark so the changeset recomputation re-pushes them.
        let mut local = self.push_watermark.load(Ordering::SeqCst);
        let pre_push_server_watermark = self
            .fetch_sync_status(incarnation)
            .await
            .and_then(|status| status.applied_push_watermark);
        if let Some(server_applied) = pre_push_server_watermark
            && server_applied < local
        {
            tracing::info!(
                tenant_id = %self.tenant_id,
                local_watermark = local.0,
                server_applied_watermark = server_applied.0,
                "server applied-push watermark behind local; regressing to re-push acked commits"
            );
            // Clear stale hub-order anchors BEFORE persisting the regressed
            // source frontier. A crash after the watermark write but before
            // this durable invalidation would otherwise let a later hub edit
            // lose to the restored hub's discarded order.
            self.db
                .with_authoritative_hub_reply(&authenticated_hub, |db| {
                    db.invalidate_accepted_local_ordering_after_hub_regression(server_applied)?;
                    db.persist_sync_push_watermark_while_authoritative(
                        &self.tenant_id,
                        server_applied,
                    )?;
                    self.push_watermark.store(server_applied, Ordering::SeqCst);
                    Ok(())
                })
                .map_err(|err| Error::SyncError(err.to_string()))?;
            local = server_applied;
        }

        let destination_reupload_epoch = self.destination_reupload_epoch()?;

        // A status watermark only says that this edge's request reached the
        // hub. It does not identify the row ordering that request received.
        // Recover that ordering with an ordinary pull before constructing any
        // next push changeset; otherwise a restart after a lost acknowledgement
        // could resend the same stampless LatestWins row as a fresh overwrite.
        // The marker is durable so this gate survives a new SyncClient/process.
        let durable_pending_confirmation = self
            .db
            .persisted_sync_pending_push_confirmation(&self.tenant_id)
            .map_err(|err| Error::SyncError(err.to_string()))?;
        let in_memory_pending = self.pending_push_confirmation.load(Ordering::SeqCst);
        let pending_confirmation = durable_pending_confirmation
            .or_else(|| (in_memory_pending.0 != 0).then_some(in_memory_pending));
        // A status frontier belongs to the edge identity the client presents,
        // not the remote hub identity. Identity-refusal test transports expose an
        // aggregate server value, so it must never be mistaken for this
        // client's pending work.
        let reconciliation_target = pending_confirmation.or_else(|| {
            // Moved-to/moved-back hub status-ahead is expected until one-time
            // rebuild completes, not evidence of this life's lost ack.
            if destination_reupload_epoch.is_none() && self.transport.has_stable_edge_identity() {
                pre_push_server_watermark.filter(|server_applied| *server_applied > local)
            } else {
                None
            }
        });
        if let Some(target) = reconciliation_target {
            let Some(server_applied) = pre_push_server_watermark else {
                return Err(Error::SyncPushUnconfirmed {
                    detail: "a prior push awaits exact hub-order reconciliation, but hub status is unavailable; no rows were resent".to_string(),
                });
            };
            let restored_before_pending = server_applied < target;
            // A restored status below the durable pending target proves that
            // this hub does not yet contain the re-send. Its ordinary history
            // must not clear Pending; only a status-confirmed hub can use the
            // explicit Pending-bypassing reconciliation mode.
            if restored_before_pending {
                self.db
                    .with_authoritative_hub_reply(&authenticated_hub, |db| {
                        db.mark_outbound_rows_pending(server_applied, None)
                    })?;
            } else {
                // The status covers this edge's pending batch. Protect only
                // that confirmed interval while the exact-order pull runs;
                // later, unconfirmed local work must remain ordinary local
                // work rather than being stamped by this confirmation.
                self.db
                    .with_authoritative_hub_reply(&authenticated_hub, |db| {
                        db.mark_outbound_rows_pending(local, Some(server_applied))
                    })?;
            }
            let reconciliation_pull = if restored_before_pending {
                self.pull_with_initial_adoption(SyncAdoption::Continuing)
                    .await
            } else {
                self.pull_default_confirmed_pending().await
            };
            if let Err(pull_err) = reconciliation_pull {
                return Err(Error::SyncPushUnconfirmed {
                    detail: format!(
                        "the prior push reconciliation pull failed ({pull_err}); no rows were resent"
                    ),
                });
            }
            if restored_before_pending {
                tracing::info!(
                    tenant_id = %self.tenant_id,
                    pending = target.0,
                    restored_frontier = server_applied.0,
                    "hub restored below pending push confirmation; adopted its history before re-delivery"
                );
            }
            // The pull supplied exact arrivals; status supplies this edge's
            // outbound frontier. Persist that frontier before clearing pending:
            // a crash can then only repeat the safe idempotent pull.
            self.db
                .with_authoritative_hub_reply(&authenticated_hub, |db| {
                    db.persist_sync_push_watermark_while_authoritative(
                        &self.tenant_id,
                        server_applied,
                    )?;
                    db.persist_sync_pending_push_confirmation_while_authoritative(
                        &self.tenant_id,
                        None,
                    )?;
                    self.push_watermark.store(server_applied, Ordering::SeqCst);
                    self.pending_push_confirmation
                        .store(Lsn(0), Ordering::SeqCst);
                    self.advance_engine_sync_watermark(server_applied);
                    Ok(())
                })
                .map_err(|err| Error::SyncError(err.to_string()))?;
            local = server_applied;
        }

        let since = local;
        let schema_read = self.db.enter_outbound_sync_schema_read();
        refuse_keyless_tables_with_no_identity_fallback(&self.db, &HashMap::new())?;
        let (changeset, _, ddl_provenance_source) =
            self.db.checked_changes_since_with_arrivals(since)?;
        let history = self.db.sync_direction_history();
        let changeset = changeset
            .filter_by_direction_history(&history, &[SyncDirection::Push, SyncDirection::Both]);
        let changeset = self.db.filter_outbound_received_ddl(
            changeset,
            &ddl_provenance_source,
            destination_reupload_epoch.map(|(through, _)| through),
        )?;
        let changeset = match destination_reupload_epoch {
            Some((through, _)) => self
                .db
                .append_destination_reupload_deletes(changeset, through)?,
            None => changeset,
        };
        let changeset = match destination_reupload_epoch {
            Some((through, _)) => {
                drop_rows_that_arrived_by_sync_after(&self.db, changeset, Some(through))
            }
            None => drop_rows_that_arrived_by_sync(&self.db, changeset),
        };

        // The greatest LSN this push actually TRANSMITS, taken PRE-send from the
        // changeset computed under the directions read above. A lost-ack
        // reconciliation must bound the hub's answer by this — recomputing it
        // after the await would let a concurrent direction change (a delivering
        // table switched to SYNC OFF) drop the bound below what already shipped
        // and reject a batch the hub genuinely holds.
        let units = self
            .db
            .dependency_complete_outbound_units(changeset, since)?;
        let transmitted_ceiling = units
            .iter()
            .filter_map(|unit| unit.changes.max_lsn())
            .max()
            .unwrap_or(Lsn(0));

        if units.iter().all(|unit| unit.changes.is_empty()) {
            if let Some((_, epoch_id)) = destination_reupload_epoch
                && let Some(hub_node_id) = self.hub_node_id()
            {
                self.db
                    .complete_destination_reupload(&self.tenant_id, &hub_node_id, epoch_id)?;
            }
            return Ok(ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: Vec::new(),
                new_lsn: self.db.current_lsn(),
            });
        }

        let mut total = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: Vec::new(),
            new_lsn: since,
        };
        let mut hub_reply_effects = Vec::new();

        let hub = Some(authenticated_hub.clone());
        let mut last_successful_lsn = since;
        // `ApplyResult::new_lsn` is one accepting commit position. Do not
        // stamp an entire size batch with it: a size batch may contain many
        // independently committed data-LSN groups and hub work can interleave
        // between them. Keep each request to exactly one group so the existing
        // The acknowledgement remains an exact provenance position.
        let mut batches = units
            .into_iter()
            .flat_map(|unit| {
                if unit.dependency_complete {
                    vec![(unit.changes, true)]
                } else {
                    acceptance_stamped_push_batches(unit.changes)
                        .into_iter()
                        .map(|changes| (changes, false))
                        .collect()
                }
            })
            .collect::<Vec<_>>();
        // Existing ordinary batches stay in source-LSN order. A connected
        // unit is ordered by the newest source member it consumes, while DDL
        // at that frontier stays ahead of data so a fresh receiver never sees
        // rows before its declaration.
        batches.sort_by_key(|(batch, dependency_complete)| {
            (
                batch.max_lsn().unwrap_or(Lsn(0)),
                batch.ddl.is_empty(),
                *dependency_complete,
            )
        });
        let mut prepared_batches = Vec::with_capacity(batches.len());
        for (batch, dependency_complete) in batches {
            let batch_max_lsn = batch.max_lsn().unwrap_or_else(|| {
                if batch.ddl.is_empty() {
                    since
                } else {
                    self.db.current_lsn()
                }
            });
            // Taken BEFORE the send, so the success path and the lost-ack
            // reconciliation below report the same transmitted set. Recomputing
            // them separately on each path is how the two would drift.
            let batch_items = batch.rows.len() as u64;
            let batch_payload_bytes = row_payload_bytes(&batch.rows);
            let arrivals = self.db.sync_arrivals_for_changes(&batch);
            let lineages = if batch.rows.is_empty() {
                HashMap::new()
            } else {
                let node_id = self.transport.local_node_id().ok_or_else(|| {
                    Error::SyncError(
                        "protocol v6 production push requires the stable authenticated edge identity"
                            .to_string(),
                    )
                })?;
                let signer = self.lineage_signer.as_ref().ok_or_else(|| {
                    Error::SyncError(
                        "protocol v6 production push requires the transport's creator signer"
                            .to_string(),
                    )
                })?;
                self.db.outbound_row_lineages(
                    &batch,
                    &self.tenant_id,
                    &node_id,
                    incarnation,
                    signer.as_ref(),
                )?
            };
            if batch.rows.iter().any(|row| row.deleted) {
                let author_node_id = self.transport.local_node_id().ok_or_else(|| {
                    Error::SyncError(
                        "a durable delete obligation requires the transport's stable fabric identity"
                            .to_string(),
                    )
                })?;
                self.db.stamp_pending_delete_author_tuple(
                    &batch.rows,
                    &author_node_id,
                    incarnation,
                )?;
            }
            let ddl_provenance = self
                .db
                .outbound_ddl_provenance(&batch, &ddl_provenance_source)?;
            prepared_batches.push(PreparedPushBatch {
                batch,
                dependency_complete,
                batch_max_lsn,
                batch_items,
                batch_payload_bytes,
                arrivals,
                lineages,
                ddl_provenance,
            });
        }
        // Every database-derived fact is now retained beside its raw batch;
        // release the schema read before encoding or waiting on transport.
        drop(schema_read);
        for PreparedPushBatch {
            batch,
            dependency_complete,
            batch_max_lsn,
            batch_items,
            batch_payload_bytes,
            arrivals,
            lineages,
            ddl_provenance,
        } in prepared_batches
        {
            let request = PushRequest {
                changeset:
                    crate::protocol::wire_changeset_with_arrivals_lineages_and_ddl_provenance(
                        batch.clone(),
                        &arrivals,
                        &lineages,
                        ddl_provenance,
                    ),
                incarnation,
            };
            let message_type = if dependency_complete {
                MessageType::DependencyCompletePushRequest
            } else {
                MessageType::PushRequest
            };
            let encoded =
                encode(message_type, &request).map_err(|e| Error::SyncError(e.to_string()))?;

            let result: ApplyResult = match self.request_push(encoded).await {
                Ok(result) => result,
                Err(PushRequestError::Terminal(err)) => return Err(err),
                Err(PushRequestError::Ambiguous(err)) => {
                    // This request may have left the edge, so the outcome is
                    // INDETERMINATE: the hub may have applied and committed the
                    // batch before the acknowledgement was lost. Reconcile once
                    // against the hub's applied-push watermark before reporting,
                    // so a push whose data actually landed is never announced as
                    // a definitive failure (usability job USR-19). Convergence is
                    // unchanged: the watermark advances only on a CONFIRMED
                    // batch, and an unconfirmed outcome leaves it untouched so a
                    // later push re-sends the same batch idempotently.
                    self.finish_interrupted_push(
                        err,
                        batch_max_lsn,
                        hub.as_deref(),
                        batch_items,
                        batch_payload_bytes,
                        transmitted_ceiling,
                        incarnation,
                        batch.rows.iter().any(|row| row.deleted),
                    )
                    .await?;
                    // The confirmed group is now durably retired, but this
                    // push can contain later independently committed LSN
                    // groups. Continue with them rather than returning a
                    // misleading success after only the first lost-ack group.
                    last_successful_lsn = batch_max_lsn;
                    continue;
                }
            };
            // `new_lsn` is the accepting hub's committed position for this
            // acknowledged batch. Record it on the author before advancing its
            // push watermark: a later pull can now distinguish this accepted
            // echo from a subsequent local mutation that is still unpushed.
            // This reuses the existing reply field; the acknowledgement's
            // meaning stays unchanged.
            // This request is exactly one data-LSN group, so `new_lsn` is the
            // exact hub ordering position for every mutation it addressed.
            // Every terminal diagnostic must cover exactly one transmitted
            // row. Ordinary arbitration carries both winner fields; permanent
            // purge and retired-generation boundaries carry neither because
            // no live winner can authorize an old lineage.
            let mut refused_indexes = BTreeSet::new();
            let mut ordinary_refused_rows = Vec::new();
            let mut purge_refused_rows = Vec::new();
            for conflict in &result.conflicts {
                let purged_lineage =
                    conflict.reason.as_deref() == Some(PURGED_LINEAGE_CONFLICT_REASON);
                let removed_generation =
                    conflict.reason.as_deref() == Some(REMOVED_GENERATION_CONFLICT_REASON);
                let winner_fields = (
                    conflict.winning_author_node_id.is_some(),
                    conflict.hub_acceptance_position.is_some(),
                );
                match (purged_lineage || removed_generation, winner_fields) {
                    (true, (false, false)) | (false, (true, true)) => {}
                    (false, (false, false)) => {
                        if conflict.table.is_some() || conflict.mutation_kind.is_some() {
                            return Err(Error::SyncError(
                                "ordinary accounting conflict claims a terminal row".to_string(),
                            ));
                        }
                        continue;
                    }
                    _ => {
                        return Err(Error::SyncError(
                            "terminal refusal has invalid winner diagnostics".to_string(),
                        ));
                    }
                }
                let matching_rows = batch
                    .rows
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| {
                        conflict.table.as_deref() == Some(row.table.as_str())
                            && conflict.natural_key == row.natural_key
                            && conflict.mutation_kind.as_deref()
                                == Some(if row.deleted { "delete" } else { "edit" })
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                if matching_rows.len() != 1 {
                    return Err(Error::SyncError(
                        "terminal refusal lacks unique exact row coverage".to_string(),
                    ));
                }
                let row_index = matching_rows[0];
                if !refused_indexes.insert(row_index) {
                    return Err(Error::SyncError(
                        "terminal refusal repeats a row diagnostic".to_string(),
                    ));
                }
                if purged_lineage {
                    purge_refused_rows.push(batch.rows[row_index].clone());
                } else {
                    ordinary_refused_rows.push(batch.rows[row_index].clone());
                }
            }
            if dependency_complete
                && !refused_indexes.is_empty()
                && refused_indexes.len() != batch.rows.len()
            {
                return Err(Error::SyncError(
                    "dependency-complete refusal covers only part of the unit".to_string(),
                ));
            }
            let accepted_rows = batch
                .rows
                .iter()
                .enumerate()
                .filter_map(|(index, row)| {
                    (!refused_indexes.contains(&index)).then_some(row.clone())
                })
                .collect::<Vec<_>>();
            hub_reply_effects.push((
                ordinary_refused_rows,
                purge_refused_rows,
                accepted_rows,
                result.new_lsn,
            ));
            // Every refusal this hub can still report advances the watermark:
            // now that arbitration compares each row against a single
            // accepting-node ordering position instead of two machines'
            // unrelated clocks, a `LatestWins` push is never refused at all
            // (it either wins outright or is a stale echo, both counted as
            // nothing on the receipt) — so the only refusals left
            // (`ServerWins`, `InsertIfNotExists`) are ones a later push could
            // never win either, and must not be retried forever. The dead
            // `has_retryable_legacy_lsn_conflict` guard this replaces never
            // matched its own producer's reason string (`"local_lsn_newer_or_equal"`
            // vs the engine's `"latest_wins_local_lsn_newer_or_equal"`) — it
            // has never fired, and there is no refusal class left that
            // landing it correctly would have helped: keeping it would
            // instead wedge every push against a hub legitimately refusing
            // under `ServerWins`.
            last_successful_lsn = batch_max_lsn;
            total.applied_rows += result.applied_rows;
            total.skipped_rows += result.skipped_rows;
            total.conflicts.extend(result.conflicts);
            total.new_lsn = result.new_lsn;
            self.receipts.record(
                hub.as_deref(),
                TransferPlane::Sync,
                TransferDirection::Sent,
                batch.rows.len() as u64,
                row_payload_bytes(&batch.rows),
            );
        }

        self.pause_after_push_response_for_test_if_armed();
        self.db
            .with_authoritative_hub_reply(&authenticated_hub, |db| {
                for (ordinary_refused_rows, purge_refused_rows, accepted_rows, hub_lsn) in
                    hub_reply_effects
                {
                    db.record_hub_push_reply_effects_while_authoritative(
                        &self.tenant_id,
                        &authenticated_hub,
                        &ordinary_refused_rows,
                        &purge_refused_rows,
                        &accepted_rows,
                        hub_lsn,
                    )?;
                }
                db.persist_sync_push_watermark_while_authoritative(
                    &self.tenant_id,
                    last_successful_lsn,
                )?;
                self.push_watermark
                    .store(last_successful_lsn, Ordering::SeqCst);
                self.advance_engine_sync_watermark(last_successful_lsn);
                if let Some((_, epoch_id)) = destination_reupload_epoch {
                    db.complete_destination_reupload_while_authoritative(
                        &self.tenant_id,
                        &authenticated_hub,
                        epoch_id,
                    )?;
                }
                Ok(())
            })
            .map_err(|err| Error::SyncError(err.to_string()))?;
        Ok(total)
    }

    /// Open the engine's `SYNC SAFE` deletion gate up to — and only up to —
    /// what the hub confirmed. The engine blocks pruning at
    /// `row.lsn >= sync_watermark`, so the watermark is the EXCLUSIVE frontier:
    /// the first LSN NOT yet confirmed. Confirmed through L therefore means
    /// L + 1; one more would open the gate on an unconfirmed row, one less
    /// would strand a confirmed one forever. No application code ever sets
    /// this — the real client does, on hub-confirmed push.
    fn advance_engine_sync_watermark(&self, confirmed_through: Lsn) {
        if confirmed_through.0 == 0 {
            return;
        }
        let frontier = Lsn(confirmed_through.0.saturating_add(1));
        if frontier > self.db.sync_watermark() {
            self.db.set_sync_watermark(frontier);
        }
    }

    /// Resolve a push whose batch was transmitted but whose transport failed
    /// before the acknowledgement returned. The batch may or may not have
    /// committed on the hub, so ask the hub what it actually applied:
    ///
    /// * If the hub's applied-push watermark covers this batch's max LSN, the
    ///   batch DID land, but the status watermark is not an exact row-arrival
    ///   position. Pull before retiring the batch: the accepted echo carries
    ///   the hub's precise arrival marker, and any later hub write follows it
    ///   in the same ordinary cursor order. Only that successful reconciliation
    ///   lets this method advance and persist the push watermark.
    /// * Otherwise the hub is unreachable or its watermark does not (yet) confirm
    ///   the batch. The outcome is genuinely UNKNOWN, so surface the distinct
    ///   [`Error::SyncPushUnconfirmed`] (never a definitive failure) and leave the
    ///   watermark untouched so a later push re-sends the batch idempotently.
    #[allow(clippy::too_many_arguments)]
    async fn finish_interrupted_push(
        &self,
        transport_err: Error,
        batch_max_lsn: Lsn,
        hub: Option<&str>,
        batch_items: u64,
        batch_payload_bytes: u64,
        transmitted_ceiling: Lsn,
        incarnation: Incarnation,
        contains_delete: bool,
    ) -> Result<(), Error> {
        // The hub answers this edge with the per-edge record it holds for THIS
        // life of the edge — keyed by (node_id, incarnation), stamped on the
        // status probe below. A lost acknowledgement is confirmed only when that
        // record both covers this batch AND is bounded by what this push actually
        // transmitted:
        //
        //  * The ceiling is the greatest LSN this push actually TRANSMITTED,
        //    captured from the changeset PRE-send. Recomputing it here would let a
        //    direction change racing this reconciliation (a delivering table
        //    switched to SYNC OFF) drop the ceiling below what already shipped and
        //    reject a batch the hub genuinely holds; and the whole-database
        //    `current_lsn` would overstate it (sync-off and pull-only writes never
        //    leave the edge), letting purely local work vouch for a stale hub
        //    watermark.
        //  * A wiped-and-recreated edge reusing its node id carries a FRESH
        //    incarnation, so the hub holds no record for it and answers Lsn(0) —
        //    below this batch's max — and the confirmation is refused by
        //    construction. The prior life's high watermark lives under the OLD
        //    incarnation's key and can never be read on this one.
        //
        // Failing the bound leaves the outcome unconfirmed and the edge
        // re-uploads — never opening the SYNC SAFE deletion gate on rows the hub
        // never received.
        let confirmed = self
            .fetch_sync_status(incarnation)
            .await
            .and_then(|status| status.applied_push_watermark)
            .is_some_and(|server_applied| {
                server_applied >= batch_max_lsn && server_applied <= transmitted_ceiling
            });

        if confirmed {
            let hub_node_id = hub.ok_or_else(|| {
                Error::SyncError(
                    "confirmed push did not retain its authenticated hub identity".to_string(),
                )
            })?;
            if contains_delete {
                // Status proves only that this source-LSN group was consumed;
                // it deliberately cannot distinguish an accepted delete from
                // a policy refusal.  Do not advance past a durable delete
                // obligation without the ordinary authenticated reply's
                // exact outcome.  A retry is idempotent and will obtain that
                // outcome; treating status as acceptance would strand Pending
                // behind an advanced watermark.
                return Err(Error::SyncPushUnconfirmed {
                    detail: "the hub status confirms a delete-bearing push was consumed but cannot distinguish acceptance from policy refusal; the durable delete remains pending and the push watermark was not advanced — retry the push for its authenticated outcome".to_string(),
                });
            }
            tracing::info!(
                tenant_id = %self.tenant_id,
                batch_max_lsn = batch_max_lsn.0,
                "push acknowledgement was lost but the hub confirms the batch landed; pulling exact hub ordering before retiring it"
            );
            // Mark the exact confirmed batch before recording its recovery
            // target or pulling. Its identical echo has no row mutation, so
            // without this durable Pending provenance a later restore could
            // classify the local row as Pulled and suppress re-upload.
            let previous_push_watermark = self.push_watermark.load(Ordering::SeqCst);
            self.db
                .with_authoritative_hub_reply(hub_node_id, |db| {
                    db.mark_outbound_rows_pending(previous_push_watermark, Some(batch_max_lsn))?;
                    // Record this before the pull. A crash or transport failure after
                    // status confirmation must survive restart as a no-send gate until
                    // the exact hub events are pulled.
                    db.persist_sync_pending_push_confirmation_while_authoritative(
                        &self.tenant_id,
                        Some(batch_max_lsn),
                    )?;
                    self.pending_push_confirmation
                        .store(batch_max_lsn, Ordering::SeqCst);
                    Ok(())
                })
                .map_err(|err| Error::SyncError(err.to_string()))?;
            // Never replay a stampless batch here. In particular, replaying a
            // previously accepted LatestWins write would look freshly authored
            // to the hub and could overwrite a newer hub edit. The original
            // accepted echo remains ahead of this pull cursor and carries its
            // exact arrival; pulling it also orders any later hub edit after
            // that echo. If the pull cannot complete, the push watermark stays
            // put and a later call can safely reconcile again.
            if let Err(pull_err) = self.pull_default_confirmed_pending().await {
                return Err(Error::SyncPushUnconfirmed {
                    detail: format!(
                        "the hub confirmed the push landed but the required pull reconciliation \
                         could not establish its exact ordering ({pull_err}); the push watermark \
                         remains unchanged and a later push will reconcile"
                    ),
                });
            }
            self.db
                .with_authoritative_hub_reply(hub_node_id, |db| {
                    db.persist_sync_push_watermark_while_authoritative(
                        &self.tenant_id,
                        batch_max_lsn,
                    )?;
                    db.persist_sync_pending_push_confirmation_while_authoritative(
                        &self.tenant_id,
                        None,
                    )?;
                    self.push_watermark.store(batch_max_lsn, Ordering::SeqCst);
                    self.pending_push_confirmation
                        .store(Lsn(0), Ordering::SeqCst);
                    self.advance_engine_sync_watermark(batch_max_lsn);
                    Ok(())
                })
                .map_err(|err| Error::SyncError(err.to_string()))?;
            // The batch is confirmed DELIVERED, so it is counted — recorded
            // here, beside the watermark advance, because this is the one place
            // that decides the push succeeded. Exactly once, with no
            // de-duplication state: the watermark just advanced past this batch,
            // so a later push cannot re-send it and cannot re-record it. The
            // unconfirmed branch below records nothing, and its batch stays in
            // the changeset to be sent — and counted — when a later push can
            // prove it landed.
            self.receipts.record(
                hub,
                TransferPlane::Sync,
                TransferDirection::Sent,
                batch_items,
                batch_payload_bytes,
            );
            return Ok(());
        }

        Err(Error::SyncPushUnconfirmed {
            detail: format!(
                "the hub did not acknowledge the push and its status could not confirm the batch \
                 landed ({transport_err}); the data may or may not have committed — run the push \
                 again to reconcile"
            ),
        })
    }

    /// Status-confirmed recovery is the sole pull mode allowed to resolve a
    /// restored-hub Pending marker. It is private to the lost-ack path.
    async fn pull_default_confirmed_pending(&self) -> Result<ApplyResult, Error> {
        // `finish_interrupted_push` just authenticated and bounded the hub's
        // per-edge watermark. Re-probing status here would add a second
        // independent status exchange after the lost reply without improving
        // the proof of this exact pending batch; the pull below supplies the
        // ordering evidence that is still required before retiring it.
        self.pull_with_initial_adoption_and_status_probe(
            SyncAdoption::ConfirmedPendingReconciliation,
            false,
        )
        .await
    }

    async fn pull_with_initial_adoption(
        &self,
        initial_adoption: SyncAdoption,
    ) -> Result<ApplyResult, Error> {
        self.pull_with_initial_adoption_and_status_probe(initial_adoption, true)
            .await
    }

    async fn pull_with_initial_adoption_and_status_probe(
        &self,
        initial_adoption: SyncAdoption,
        probe_status: bool,
    ) -> Result<ApplyResult, Error> {
        self.bind_authenticated_hub()?;
        self.ensure_connected().await.map_err(Error::SyncError)?;
        let authenticated_hub = self.hub_node_id().ok_or_else(|| {
            Error::SyncError(
                "protocol v6 production pull requires the authenticated authoritative hub identity"
                    .to_string(),
            )
        })?;
        // Pull-side regression safety (contract item 4): a server whose
        // LSN clock is behind our pull watermark was restored from a stale
        // artifact — the watermark refers to a lost server history, and any
        // post-restore commit may be stamped at or below it. The only resume
        // point the client can prove safe is the beginning; re-delivered rows
        // apply idempotently via the conflict policy, and genuinely new
        // server commits are never skipped.
        let local = self.pull_watermark.load(Ordering::SeqCst);
        let incarnation = self
            .db
            .sync_incarnation(&self.tenant_id)
            .map_err(|err| Error::SyncError(err.to_string()))?;
        if probe_status
            && let Some(status) = self.fetch_sync_status(incarnation).await
            && let Some(server_lsn) = status.server_current_lsn
            && server_lsn < local
        {
            tracing::info!(
                tenant_id = %self.tenant_id,
                local_watermark = local.0,
                server_current_lsn = server_lsn.0,
                "server LSN clock behind local pull watermark; resetting pull watermark to re-pull"
            );
            self.db
                .with_authoritative_hub_reply(&authenticated_hub, |db| {
                    db.persist_sync_pull_watermark_while_authoritative(&self.tenant_id, Lsn(0))?;
                    self.pull_watermark.store(Lsn(0), Ordering::SeqCst);
                    Ok(())
                })
                .map_err(|err| Error::SyncError(err.to_string()))?;
        }

        let hub = Some(authenticated_hub.clone());
        let mut terminal_refusal_context = match hub.as_deref() {
            Some(hub_node_id) => {
                let context = TerminalRefusalPullContext {
                    tenant_id: self.tenant_id.clone(),
                    hub_node_id: hub_node_id.to_string(),
                    generation: 0,
                };
                self.db
                    .has_terminal_refusal_markers(&context)?
                    .then_some(context)
            }
            None => None,
        };
        let terminal_scan_state = terminal_refusal_context
            .as_ref()
            .map(|context| self.db.terminal_refusal_scan_state(context))
            .transpose()?
            .flatten();
        if let (Some(context), Some(state)) = (&mut terminal_refusal_context, &terminal_scan_state)
        {
            context.generation = state.generation;
        }
        let terminal_scan_resume = terminal_scan_state
            .as_ref()
            .and_then(|state| state.source.map(|source| (source, state.next_lsn)));
        let terminal_scan_generation = terminal_scan_state
            .as_ref()
            .map(|state| state.generation)
            .unwrap_or(0);
        let durable_call_cursor = self.pull_watermark.load(Ordering::SeqCst);
        // A terminal refusal's winning row can be older than the ordinary
        // cursor. This is a private, one-call rewind only; the public cursor
        // remains monotonic when the serving source is unchanged.
        let mut since_lsn = if let Some((_, next_lsn)) = terminal_scan_resume {
            next_lsn
        } else if terminal_refusal_context.is_some() {
            Lsn(0)
        } else {
            durable_call_cursor
        };
        // Hidden pages still need a temporary request cursor so a paged pull
        // can traverse past them, but they are not externally consumable
        // progress. Preserve this call-start cursor unless an entry survives
        // direction and retention filtering below.
        let mut empty_call_baseline = durable_call_cursor;
        #[allow(unused_assignments)]
        let mut last_server_lsn = since_lsn;
        let mut saw_deliverable_changes = false;
        let mut total = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: vec![],
            new_lsn: since_lsn,
        };

        // The store this cursor addresses. A page whose source differs is
        // discarded unapplied rather than partially trusted: a cursor is
        // only ever compared against the history of the store that issued
        // it. `None` means this client has never bound to a source yet — the
        // first page of this call binds it fresh, with no mismatch possible.
        let mut expected_source = self
            .pull_source
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .to_owned();
        let mut terminal_cursor_floor = (terminal_refusal_context.is_some()
            && expected_source.is_some())
        .then_some(durable_call_cursor);
        let mut terminal_scan_source = terminal_scan_resume.map(|(source, _)| source);
        let mut first_page = true;
        // Set once this call detects its cursor's source changed, and never
        // cleared for the rest of THIS call: every page from that point on
        // is part of the same from-zero re-fetch of the newly adopted
        // source's full history (see `SyncAdoption`).
        let mut adoption = initial_adoption;

        loop {
            let request = PullRequest {
                since_lsn,
                max_entries: Some(PULL_PAGE_SIZE),
            };

            let PullPage {
                ordinary: mut response,
                dependency_units,
            } = self.request_pull(request).await?;
            self.pause_after_pull_response_for_test_if_armed();
            let served_source = response.source;

            if first_page
                && let Some(scan_source) = terminal_scan_source
                && served_source != Some(scan_source)
            {
                if let Some(context) = terminal_refusal_context.as_ref() {
                    self.db
                        .with_authoritative_hub_reply(&authenticated_hub, |db| {
                            db.clear_terminal_refusal_scan_state(context, terminal_scan_generation)
                        })?;
                }
                terminal_scan_source = None;
                since_lsn = Lsn(0);
                terminal_cursor_floor = None;
                continue;
            }

            // Check for source identity mismatches. Three cases:
            // 1. Both expected and served are Some: if they differ, reject the page
            // 2. Expected is Some but served is None: can't validate binding, refuse
            // 3. Expected is None: bind to served (on first page)
            if let Some(expected) = expected_source {
                if let Some(served) = served_source {
                    if served != expected {
                        if first_page {
                            // The serving store's identity changed since this cursor
                            // was last recorded (a replaced/rebuilt hub under the same
                            // transport identity). The old cursor addresses history
                            // that no longer exists at this address: forget it and
                            // re-address the new store from the beginning, in the
                            // SAME call — the new source's served content is
                            // authoritative for re-adoption, never arbitrated against
                            // whatever the old source's provenance recorded.
                            tracing::info!(
                                tenant_id = %self.tenant_id,
                                old_source = %expected,
                                new_source = %served,
                                "pull source changed; resetting cursor to re-pull the new store's full history"
                            );
                            since_lsn = Lsn(0);
                            // This source has no relationship to the old
                            // cursor. A hidden-only re-adoption may bind its
                            // source identity, but never to the old source's
                            // external/persisted LSN.
                            empty_call_baseline = Lsn(0);
                            expected_source = Some(served);
                            terminal_cursor_floor = None;
                            adoption = SyncAdoption::ReadoptingSource;
                            continue;
                        }
                        // The source changed BETWEEN two pages of this one paged
                        // pull. The page already addresses history this cursor no
                        // longer provably owns: discard it unapplied — never
                        // partially trusted — and stop. Everything already applied
                        // from earlier pages in this call, and their cursor advance,
                        // stands; the next `pull` call re-detects the change as a
                        // first-page mismatch and re-pulls the new store in full.
                        self.pages_discarded_for_source_mismatch
                            .fetch_add(1, Ordering::SeqCst);
                        tracing::info!(
                            tenant_id = %self.tenant_id,
                            old_source = %expected,
                            new_source = %served,
                            "served page's source changed mid-pull; discarding it unapplied"
                        );
                        break;
                    }
                } else {
                    // Cursor is bound to a source, but the response carries no
                    // source identity. Can't validate that the response addresses
                    // the same store, so refuse the pull request entirely.
                    return Err(Error::SyncError(format!(
                        "pull response missing source identity for tenant {}: \
                         cursor is bound to source {}, but response carries no source",
                        self.tenant_id, expected
                    )));
                }
            } else if let Some(served) = served_source {
                // First page: bind to the served source
                expected_source = Some(served);
            }

            let authoritative_purges = take_validated_ordinary_purges(
                &mut response.changeset,
                since_lsn,
                response.cursor,
            )?;
            let ddl_context = if !response.changeset.ddl.is_empty() {
                let source_incarnation = served_source.ok_or_else(|| {
                    Error::SyncError(
                        "protocol v6 pull DDL requires the serving source incarnation".to_string(),
                    )
                })?;
                crate::protocol::received_ddl_context(
                    &response.changeset,
                    &self.tenant_id,
                    hub.as_deref()
                        .expect("authenticated hub was required before pull"),
                    source_incarnation,
                )
                .map_err(|err| Error::SyncError(err.to_string()))?
            } else {
                None
            };
            let arrivals = crate::protocol::wire_row_arrivals(&response.changeset);
            let lineages = crate::protocol::wire_row_lineages(&response.changeset);
            let changes = ChangeSet::try_from(response.changeset)
                .map_err(|e| Error::SyncError(e.to_string()))?;
            let has_more = response.has_more;
            let cursor = response.cursor;

            // Extract server-side max LSN BEFORE filtering/applying
            let server_lsn = cursor.or_else(|| changes.max_lsn()).unwrap_or(since_lsn);

            let dependency_units = dependency_units
                .into_iter()
                .enumerate()
                .map(|(unit_index, wire)| {
                    let ddl_context = if !wire.ddl.is_empty() {
                        let source_incarnation = served_source.ok_or_else(|| {
                            Error::SyncError(
                                "protocol v6 pull DDL requires the serving source incarnation"
                                    .to_string(),
                            )
                        })?;
                        crate::protocol::received_ddl_context(
                            &wire,
                            &self.tenant_id,
                            hub.as_deref()
                                .expect("authenticated hub was required before pull"),
                            source_incarnation,
                        )
                        .map_err(|err| Error::SyncError(err.to_string()))?
                    } else {
                        None
                    };
                    let arrivals = crate::protocol::wire_row_arrivals(&wire);
                    let lineages = crate::protocol::wire_row_lineages(&wire);
                    ChangeSet::try_from(wire)
                        .map(|changes| (unit_index, changes, arrivals, lineages, ddl_context))
                        .map_err(|err| Error::SyncError(err.to_string()))
                })
                .collect::<contextdb_core::Result<Vec<_>>>()?;
            let applied_page = self
                .db
                .with_authoritative_hub_received_apply(&authenticated_hub, |db| {
            let mut directions = db.latest_declared_table_directions();
            let mut table_generations = HashMap::new();
            let mut prepared_dependency_units = Vec::new();
            for (unit_index, changes, arrivals, lineages, unit_ddl_context) in dependency_units {
                db.advance_authenticated_pull_schema_projection(
                    &mut table_generations,
                    &mut directions,
                    &changes,
                    unit_ddl_context.as_ref(),
                )?;
                let carries_ddl = !changes.ddl.is_empty();
                let row_count = changes.rows.len();
                db
                    .validate_received_row_lineages_against_generation_projection(
                        &self.tenant_id,
                        &changes,
                        &lineages,
                        &table_generations,
                    )
                    .map_err(|err| match err {
                        Error::SyncError(detail) => Error::SyncError(format!(
                            "indexed dependency pull unit lineage validation \
                             (unit={unit_index}, carries_ddl={carries_ddl}, row_count={row_count}): {detail}"
                        )),
                        other => other,
                    })?;
                let changes = changes
                    .filter_by_direction(&directions, &[SyncDirection::Pull, SyncDirection::Both]);
                let suppression = db
                    .reject_dependency_complete_accepted_lineage_replays_against_generation_projection(
                        &self.tenant_id,
                        changes,
                        &lineages,
                        &table_generations,
                    )?;
                if !suppression.changes.is_empty() {
                    prepared_dependency_units.push((
                        unit_index,
                        suppression.changes,
                        arrivals,
                        lineages,
                        unit_ddl_context,
                    ));
                }
            }
            db.advance_authenticated_pull_schema_projection(
                &mut table_generations,
                &mut directions,
                &changes,
                ddl_context.as_ref(),
            )?;
            let ordinary = changes
                .filter_by_direction(&directions, &[SyncDirection::Pull, SyncDirection::Both]);
            db
                .validate_received_row_lineages_against_generation_projection(
                    &self.tenant_id,
                    &ordinary,
                    &lineages,
                    &table_generations,
                )
                .map_err(|err| match err {
                    Error::SyncError(detail) => {
                        Error::SyncError(format!("ordinary pull lineage validation: {detail}"))
                    }
                    other => other,
                })?;
            let ordinary_suppression = db
                .drop_accepted_lineage_replays_against_generation_projection(
                    &self.tenant_id,
                    ordinary,
                    &lineages,
                    &table_generations,
                )?;
            let ordinary_suppressed_only = ordinary_suppression.suppressed_live_replay
                && ordinary_suppression.changes.is_empty();
            let ordinary = ordinary_suppression.changes;
            let ordinary_has_authoritative_purges = !authoritative_purges.is_empty();
            let ordinary_has_deliverable_changes = !ordinary.is_empty();
            let ordinary_received_items = ordinary.rows.len() as u64;
            let ordinary_payload_bytes = row_payload_bytes(&ordinary.rows);
            let ordinary_has_create_trigger = ordinary.has_create_trigger_ddl();
            let pending_refresh_rows = (adoption == SyncAdoption::ConfirmedPendingReconciliation)
                .then(|| ordinary.rows.clone());
            let dependency_units = prepared_dependency_units;
            let dependency_received_items = dependency_units
                .iter()
                .map(|(_, unit, _, _, _)| unit.rows.len() as u64)
                .sum::<u64>();
            let dependency_payload_bytes = dependency_units
                .iter()
                .map(|(_, unit, _, _, _)| row_payload_bytes(&unit.rows))
                .sum::<u64>();
            let dependency_has_create_trigger = dependency_units
                .iter()
                .any(|(_, unit, _, _, _)| unit.has_create_trigger_ddl());
            let dependency_has_deliverable_changes = !dependency_units.is_empty();
            db.preflight_incoming_authoritative_purge_batch_while_authoritative(
                &authoritative_purges,
            )?;
                    let mut result = ApplyResult {
                        applied_rows: 0,
                        skipped_rows: 0,
                        conflicts: Vec::new(),
                        new_lsn: db.current_lsn(),
                    };
                    for (unit_index, unit, unit_arrivals, unit_lineages, unit_ddl_context) in
                        dependency_units
                    {
                        let carries_ddl = !unit.ddl.is_empty();
                        let row_count = unit.rows.len();
                        let pending_unit_rows = (adoption
                            == SyncAdoption::ConfirmedPendingReconciliation)
                            .then(|| unit.rows.clone());
                        let unit_result = db
                            .apply_authenticated_received_changes_with_lineages_while_schema_publication_held(
                                unit,
                                &unit_arrivals,
                                adoption,
                                terminal_refusal_context.as_ref(),
                                &self.tenant_id,
                                &unit_lineages,
                                unit_ddl_context.as_ref(),
                                true,
                            )
                        .map_err(|err| match err {
                            Error::SyncError(detail) => Error::SyncError(format!(
                                "indexed dependency pull unit apply \
                                 (unit={unit_index}, carries_ddl={carries_ddl}, row_count={row_count}): {detail}"
                            )),
                            other => other,
                        })?;
                        result.applied_rows += unit_result.applied_rows;
                        result.skipped_rows += unit_result.skipped_rows;
                        result.conflicts.extend(unit_result.conflicts);
                        result.new_lsn = unit_result.new_lsn;
                        if let Some(rows) = pending_unit_rows {
                            db.refresh_confirmed_pending_rows(&rows, &unit_arrivals)?;
                        }
                    }
                    db.apply_incoming_authoritative_purge_batch_while_authoritative(
                        &authoritative_purges,
                    )?;
                    result.new_lsn = db.current_lsn();
                    if !ordinary.is_empty() {
                        let ordinary_result = db
                            .apply_authenticated_received_changes_with_lineages_while_schema_publication_held(
                                ordinary,
                                &arrivals,
                                adoption,
                                terminal_refusal_context.as_ref(),
                                &self.tenant_id,
                                &lineages,
                                ddl_context.as_ref(),
                                false,
                            )
                        .map_err(|err| match err {
                            Error::SyncError(detail) => {
                                Error::SyncError(format!("ordinary pull apply: {detail}"))
                            }
                            other => other,
                        })?;
                        result.applied_rows += ordinary_result.applied_rows;
                        result.skipped_rows += ordinary_result.skipped_rows;
                        result.conflicts.extend(ordinary_result.conflicts);
                        result.new_lsn = ordinary_result.new_lsn;
                    }
                    if let Some(rows) = pending_refresh_rows {
                        db.refresh_confirmed_pending_rows(&rows, &arrivals)?;
                    }
                    Ok(AppliedPullPage {
                        result,
                        suppressed_live_replay_only: ordinary_suppressed_only,
                        has_deliverable_changes: ordinary_has_authoritative_purges
                            || ordinary_has_deliverable_changes
                            || dependency_has_deliverable_changes,
                        has_create_trigger: ordinary_has_create_trigger
                            || dependency_has_create_trigger,
                        received_items: ordinary_received_items + dependency_received_items,
                        received_payload_bytes: ordinary_payload_bytes
                            + dependency_payload_bytes,
                    })
                })?;
            saw_deliverable_changes |=
                applied_page.suppressed_live_replay_only || applied_page.has_deliverable_changes;
            // Counted from the SAME row set the bytes are counted from: what
            // this end took off the wire. Using the applied count here instead
            // would pair an items figure with a payload figure drawn from two
            // different sets, so a pull with skipped rows would report bytes for
            // rows its own item count denied.
            let stop_for_trigger_bootstrap = has_more && applied_page.has_create_trigger;
            self.receipts.record(
                hub.as_deref(),
                TransferPlane::Sync,
                TransferDirection::Received,
                applied_page.received_items,
                applied_page.received_payload_bytes,
            );
            total.applied_rows += applied_page.result.applied_rows;
            total.skipped_rows += applied_page.result.skipped_rows;
            total.conflicts.extend(applied_page.result.conflicts);
            total.new_lsn = applied_page.result.new_lsn;
            last_server_lsn = server_lsn;
            first_page = false;

            if !has_more {
                if let Some(context) = terminal_refusal_context.as_ref() {
                    self.db
                        .with_authoritative_hub_reply(&authenticated_hub, |db| {
                            db.clear_terminal_refusal_scan_state(context, terminal_scan_generation)
                        })?;
                }
                break;
            }
            if stop_for_trigger_bootstrap {
                if let Some(context) = terminal_refusal_context.as_ref() {
                    let source = served_source.ok_or_else(|| {
                        Error::SyncError(
                            "terminal reconciliation trigger page omitted its source identity"
                                .to_string(),
                        )
                    })?;
                    self.db
                        .with_authoritative_hub_reply(&authenticated_hub, |db| {
                            db.persist_terminal_refusal_scan_state(
                                context,
                                terminal_scan_generation,
                                source,
                                cursor.unwrap_or(since_lsn),
                            )
                        })?;
                }
                break;
            }
            since_lsn = cursor.unwrap_or(since_lsn);
        }

        let final_cursor = if saw_deliverable_changes {
            last_server_lsn
        } else {
            empty_call_baseline
        };
        let final_cursor = terminal_cursor_floor
            .map(|floor| final_cursor.max(floor))
            .unwrap_or(final_cursor);
        self.db
            .with_authoritative_hub_reply(&authenticated_hub, |db| {
                db.persist_sync_pull_watermark_while_authoritative(&self.tenant_id, final_cursor)?;
                if let Some(source) = expected_source {
                    db.persist_sync_pull_cursor_while_authoritative(
                        &self.tenant_id,
                        source,
                        final_cursor,
                    )?;
                    *self
                        .pull_source
                        .write()
                        .unwrap_or_else(|err| err.into_inner()) = Some(source);
                }
                self.pull_watermark.store(final_cursor, Ordering::SeqCst);
                Ok(())
            })
            .map_err(|err| Error::SyncError(err.to_string()))?;
        Ok(total)
    }

    /// Pull using the durable table declarations.
    pub async fn pull_default(&self) -> Result<ApplyResult, Error> {
        // An offline local delete is owed to the hub.  Offer that durable
        // obligation before accepting a contradictory ordinary pull; otherwise
        // a close/open between delete and reconnect could resurrect the very
        // row the user removed.
        if self.db.has_durable_pending_delete_obligations()? {
            self.push().await?;
        }
        self.pull_with_initial_adoption(SyncAdoption::Continuing)
            .await
    }

    pub fn push_watermark(&self) -> Lsn {
        self.push_watermark.load(Ordering::SeqCst)
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn pending_push_confirmation_for_test(&self) -> Lsn {
        self.pending_push_confirmation.load(Ordering::SeqCst)
    }

    pub fn pull_watermark(&self) -> Lsn {
        self.pull_watermark.load(Ordering::SeqCst)
    }

    /// How many served pages this client has discarded unapplied, across
    /// every pull, because they reported an incarnation other than the one
    /// its cursor addresses. Zero until that detection exists.
    pub fn pages_discarded_for_source_mismatch(&self) -> usize {
        self.pages_discarded_for_source_mismatch
            .load(Ordering::SeqCst)
    }

    pub fn tenant_id(&self) -> &str {
        self.tenant_id.as_str()
    }

    /// The configured sync endpoint (an enrollment ticket or dial specification).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The hub this edge is provably connected to, taken from the
    /// transport-authenticated node id. `None` when the transport authenticates
    /// no peer (for example, while not connected). The operator surface reads
    /// this so a destination change adopts
    /// the hub the edge is actually talking to, rather than a hand-copied id.
    pub fn connected_hub_node_id(&self) -> Option<String> {
        self.hub_node_id()
    }

    /// Move this edge's retained-data destination to the hub identified by
    /// `new_node_id`, forgetting what the previous destination confirmed in the
    /// SAME operation (see [`Database::change_retention_sync_peer`]). The
    /// operator points the edge process at the new endpoint (configuration);
    /// this authorises the move at the sync layer, so the next push rebuilds the
    /// new destination from everything the edge holds — including rows older
    /// than the previous destination's last confirmation — and no local row is
    /// deleted until the new destination confirms receipt.
    pub fn change_destination(&self, new_node_id: &str) -> Result<(), Error> {
        let result = self
            .db
            .change_retention_sync_peer(&self.tenant_id, new_node_id);
        // A long-lived client caches the confirmation frontier in memory; drop it
        // in lockstep with the persisted record the engine clears. Reset it
        // regardless of the engine outcome: the engine clears the durable
        // watermarks on the safe side before it can fail, so leaving the cache at
        // the old high frontier would let the next push skip rows a new
        // destination never received. Resetting to zero only ever re-uploads,
        // which is idempotent and safe. The error, if any, still surfaces to the
        // caller.
        self.push_watermark.store(Lsn(0), Ordering::SeqCst);
        self.pending_push_confirmation
            .store(Lsn(0), Ordering::SeqCst);
        self.pull_watermark.store(Lsn(0), Ordering::SeqCst);
        // The pull cursor's bound source is explicitly stale the moment the
        // operator repoints the destination — the next pull binds fresh to
        // whatever the new destination reports, with no mismatch to detect.
        if let Ok(mut pull_source) = self.pull_source.write() {
            *pull_source = None;
        }
        result
    }
}

#[derive(Debug)]
enum PushReplyError {
    Malformed(Error),
    Terminal(Error),
}

#[derive(Debug)]
enum PushRequestError {
    Terminal(Error),
    Ambiguous(Error),
}

impl PushReplyError {
    fn into_request_error(self) -> PushRequestError {
        match self {
            PushReplyError::Malformed(err) => PushRequestError::Ambiguous(err),
            PushReplyError::Terminal(err) => PushRequestError::Terminal(err),
        }
    }
}

fn decode_push_response(reply: &[u8]) -> Result<ApplyResult, PushReplyError> {
    let envelope =
        decode(reply).map_err(|e| PushReplyError::Malformed(Error::SyncError(e.to_string())))?;
    if !matches!(envelope.message_type, MessageType::PushResponse) {
        return Err(PushReplyError::Malformed(Error::SyncError(
            "unexpected message type in push response".to_string(),
        )));
    }
    let response: PushResponse = rmp_serde::from_slice(&envelope.payload)
        .map_err(|e| PushReplyError::Malformed(Error::SyncError(e.to_string())))?;
    if let Some(application_error) = response.application_error {
        return Err(PushReplyError::Terminal(match application_error {
            WirePushError::PurgeRequiresAuthoritativeHub { hub_node_id } => {
                Error::PurgeRequiresAuthoritativeHub { hub_node_id }
            }
        }));
    }
    if let Some(err) = response.error {
        return Err(PushReplyError::Terminal(Error::SyncError(err)));
    }
    response
        .result
        .ok_or_else(|| {
            PushReplyError::Terminal(Error::SyncError("push response missing result".to_string()))
        })
        .map(Into::into)
}

/// Whether `direction` still delivers the table: the push filter includes
/// `Push` and `Both`, so any other setting keeps the table out of the outbound
/// changeset entirely.
fn direction_delivers(direction: SyncDirection) -> bool {
    direction.delivers()
}

/// Whether `table` has a sync identity a receiver can actually use to tell
/// one row from another. A declared identity — `natural_key_column`, a
/// table-level composite `PRIMARY KEY`, or a single-column `PRIMARY KEY` — is
/// auto-indexed by the engine at `CREATE`/`ALTER` time, so it is always
/// usable. The bare `id`-name fallback (`natural_key_columns_for_meta`'s
/// last resort) is NOT auto-indexed just by existing: the apply side's
/// exact-key probe (`required_indexed_visible_row_by_column`) hard-errors
/// against it with no covering index, so eligibility here requires that
/// SAME covering index — trusting the column's name alone would let push
/// declare a table eligible that the apply side then refuses.
fn table_has_usable_sync_identity(db: &Database, table: &str, meta: &TableMeta) -> bool {
    if meta.natural_key_column.is_some()
        || !meta.primary_key_columns.is_empty()
        || meta.columns.iter().any(|column| column.primary_key)
    {
        return true;
    }
    meta.columns.iter().any(|column| column.name == "id")
        && db.column_has_covering_index(table, "id")
}

/// Refuse a table that WOULD sync (any direction but `SYNC OFF`) but has no
/// usable sync identity (see [`table_has_usable_sync_identity`]) — the
/// engine's own changeset builders (`Database::persisted_state_since` /
/// `full_state_snapshot`) silently skip such a table's rows with a bare
/// `continue`, so `push()` would otherwise still report success with those
/// rows quietly dropped (the fixed silent-omission defect). A keyless table
/// declared `SYNC OFF` is unaffected: it was never eligible to leave this
/// machine either way, so there is nothing to refuse.
pub(crate) fn refuse_keyless_tables_with_no_identity_fallback(
    db: &Database,
    directions: &HashMap<String, SyncDirection>,
) -> Result<(), Error> {
    for table in db.table_names() {
        let Some(meta) = db.table_meta(&table) else {
            continue;
        };
        // A table's declaration wins outright; a runtime-registered
        // direction fills in only when the declaration named none; the
        // engine default (`Both`, i.e. it WOULD sync) applies when neither
        // said anything — matching `table_directions`' own precedence.
        let direction = meta.sync_direction.unwrap_or_else(|| {
            directions
                .get(&table)
                .copied()
                .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION)
        });
        if !direction_delivers(direction) {
            continue;
        }
        if table_has_usable_sync_identity(db, &table, &meta) {
            continue;
        }
        return Err(Error::SyncError(format!(
            "table '{table}' has no usable sync identity — no declared PRIMARY KEY and no \
             indexed `id`-column fallback — so its rows cannot be told apart across the wire. \
             Push refuses rather than silently omitting them while reporting success. Fix one \
             of: declare a PRIMARY KEY on '{table}'; add an indexed `id` column as the fallback \
             identity; or declare '{table}' SYNC OFF."
        )));
    }
    Ok(())
}

/// An edge pushes what it WROTE, not what it was given: a row whose current
/// local version or delete marker arrived over sync is dropped from the outbound changeset.
/// Pushing it back would hand the hub its own data — the echo that makes a
/// pull-then-push cycle re-deliver rows forever — and it would also make the
/// transfer receipt count rows that never needed to move. A row that was
/// pulled and then edited or deleted locally loses the marker, so local work
/// always propagates. Vectors follow their owning row's exact `(table, local
/// commit LSN)` group: a pulled vector must not echo, but a locally edited,
/// deleted, AcceptedLocal, or Pending owner remains outbound work.
pub(crate) fn drop_rows_that_arrived_by_sync(db: &Database, changes: ChangeSet) -> ChangeSet {
    drop_rows_that_arrived_by_sync_after(db, changes, None)
}

/// During an explicit destination move, retain only inherited mutations the
/// edge already held when it made the move. A pull from the new hub can land
/// before the rebuild push; those later arrivals remain ordinary self-echoes
/// and must not ride the one-time re-upload.
fn drop_rows_that_arrived_by_sync_after(
    db: &Database,
    changes: ChangeSet,
    reupload_through: Option<Lsn>,
) -> ChangeSet {
    let mut changes = changes;
    // A changeset can include the older local insert that preceded a pulled
    // tombstone. Suppressing only the tombstone would re-offer that stale
    // history as a live row, so suppress the complete natural-key history
    // while the current delete marker is known to have arrived by sync.
    let synced_delete_keys = changes
        .rows
        .iter()
        .filter(|row| {
            row.deleted
                && db.row_change_arrived_by_sync(row)
                && reupload_through.is_none_or(|through| row.lsn > through)
        })
        .map(|row| (row.table.clone(), format!("{:?}", row.natural_key.pairs())))
        .collect::<std::collections::HashSet<_>>();
    changes.rows.retain(|row| {
        !synced_delete_keys.contains(&(row.table.clone(), format!("{:?}", row.natural_key.pairs())))
            && (!db.row_change_arrived_by_sync(row)
                || reupload_through.is_some_and(|through| row.lsn <= through))
    });
    changes.vectors.retain(|vector| {
        !db.vector_change_arrived_by_sync(vector)
            || reupload_through.is_some_and(|through| vector.lsn <= through)
    });
    changes
}

/// Every table this database declares as `SYNC PUSH ONLY`. Read from the
/// persisted table meta, so the declaration survives a restart with no app
/// re-registration. Never-sent-back is a consequence of the declared direction
/// alone, independent of retention: a NON-retained push-only table is suppressed
/// here just like a retained one, so the hub never serves its rows back. A table
/// that declared `SYNC TWO WAY` is deliberately absent.
pub(crate) fn push_only_tables(db: &Database) -> BTreeSet<String> {
    db.table_names()
        .into_iter()
        .filter(|table| {
            db.table_meta(table).is_some_and(|meta| {
                meta.sync_direction == Some(contextdb_core::SyncDirection::Push)
            })
        })
        .collect()
}

/// Drop the ROWS (and vectors) of push-only tables from an inbound changeset.
/// Their DDL is kept: a table must still be able to ARRIVE by sync.
pub(crate) fn drop_push_only_retained_rows(db: &Database, changes: ChangeSet) -> ChangeSet {
    let one_way = push_only_tables(db);
    if one_way.is_empty() {
        return changes;
    }
    let mut changes = changes;
    changes.rows.retain(|row| !one_way.contains(&row.table));
    changes
        .vectors
        .retain(|vector| !one_way.contains(&vector.index.table));
    changes
}

pub(crate) fn split_changeset(changeset: ChangeSet) -> Vec<ChangeSet> {
    let bootstrap_batches = changeset.split_at_trigger_bootstrap_barriers();
    if bootstrap_batches.len() > 1 {
        return bootstrap_batches
            .into_iter()
            .flat_map(split_changeset_by_size)
            .collect();
    }
    split_changeset_by_size(bootstrap_batches.into_iter().next().unwrap_or_default())
}

/// The existing push acknowledgement carries one accepting commit position.
/// Keep every acknowledgement-stamped request to one data-LSN group, while
/// retaining the row/vector/edge entries that committed together.
fn acceptance_stamped_push_batches(changeset: ChangeSet) -> Vec<ChangeSet> {
    split_changeset(changeset)
        .into_iter()
        .flat_map(ChangeSet::split_by_data_lsn)
        .collect()
}

fn split_changeset_by_size(changeset: ChangeSet) -> Vec<ChangeSet> {
    let wire = WireChangeSet::from(changeset.clone());
    let estimated = rmp_serde::to_vec(&wire).map(|v| v.len()).unwrap_or(0);
    if estimated <= MAX_BATCH_BYTES && data_lsn_group_count(&changeset) <= MAX_BATCH_DATA_LSN_GROUPS
    {
        return vec![changeset];
    }

    let batches = fast_split_changeset(changeset.clone());
    if batches
        .iter()
        .all(|batch| batch_wire_size(batch) <= MAX_BATCH_BYTES)
    {
        return batches;
    }

    precise_split_changeset(changeset)
}

#[doc(hidden)]
pub fn split_changeset_for_test(changeset: ChangeSet) -> Vec<ChangeSet> {
    split_changeset(changeset)
}

/// The exact production request boundary for push acknowledgements. The
/// size/barrier splitter remains independently testable above; this second
/// step keeps every request to one atomic source-LSN group so `new_lsn` is
/// usable as provenance for every mutation in its reply.
#[doc(hidden)]
pub fn acceptance_stamped_push_batches_for_test(changeset: ChangeSet) -> Vec<ChangeSet> {
    acceptance_stamped_push_batches(changeset)
}

fn data_lsn_group_count(changeset: &ChangeSet) -> usize {
    let mut lsns = BTreeSet::new();
    lsns.extend(changeset.rows.iter().map(|row| row.lsn));
    lsns.extend(changeset.edges.iter().map(|edge| edge.lsn));
    lsns.extend(changeset.vectors.iter().map(|vector| vector.lsn));
    lsns.len()
}

fn batch_wire_size(changeset: &ChangeSet) -> usize {
    rmp_serde::to_vec(&WireChangeSet::from(changeset.clone()))
        .map(|v| v.len())
        .unwrap_or(usize::MAX)
}

fn fast_split_changeset(changeset: ChangeSet) -> Vec<ChangeSet> {
    split_complete_lsn_groups(changeset, TARGET_BATCH_BYTES, false)
}

fn precise_split_changeset(changeset: ChangeSet) -> Vec<ChangeSet> {
    split_complete_lsn_groups(changeset, MAX_BATCH_BYTES, true)
}

fn split_complete_lsn_groups(
    changeset: ChangeSet,
    target_bytes: usize,
    precise: bool,
) -> Vec<ChangeSet> {
    let groups = changeset.split_by_data_lsn();
    let mut batches = Vec::new();
    let mut current = ChangeSet::default();
    let mut current_groups = 0usize;

    for group in groups {
        let mut trial = current.clone();
        trial.rows.extend(group.rows.clone());
        trial.edges.extend(group.edges.clone());
        trial.vectors.extend(group.vectors.clone());
        trial.ddl.extend(group.ddl.clone());
        trial.ddl_lsn.extend(group.ddl_lsn.clone());
        let group_has_data = group.data_entry_count() > 0;
        let trial_size = if precise {
            batch_wire_size(&trial)
        } else {
            batch_wire_size(&current).saturating_add(batch_wire_size(&group))
        };
        let would_exceed_group_budget = group_has_data
            && current_groups > 0
            && current_groups.saturating_add(1) > MAX_BATCH_DATA_LSN_GROUPS;

        if current.data_entry_count() > 0
            && (trial_size > target_bytes || would_exceed_group_budget)
        {
            batches.push(std::mem::take(&mut current));
            current_groups = 0;
        }
        current.rows.extend(group.rows);
        current.edges.extend(group.edges);
        current.vectors.extend(group.vectors);
        current.ddl.extend(group.ddl);
        current.ddl_lsn.extend(group.ddl_lsn);
        if group_has_data {
            current_groups = current_groups.saturating_add(1);
        }
    }

    if current.data_entry_count() > 0 || !current.ddl.is_empty() {
        batches.push(current);
    }

    batches
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::{RowId, Value};
    use contextdb_engine::Database;
    use contextdb_engine::sync_types::{
        ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange, VectorChange,
    };
    use uuid::Uuid;

    // A14: Batch splitting respects byte size limits
    #[test]
    fn a14_batch_splitting_respects_byte_limits() {
        // Build a changeset with 10 rows, each ~100KB of data (total ~1MB)
        let large_text = "x".repeat(100 * 1024); // ~100KB per row
        let mut rows = Vec::new();
        for i in 0..10 {
            let id = Uuid::new_v4();
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Uuid(id));
            values.insert("data".to_string(), Value::Text(large_text.clone()));
            rows.push(RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
                values,
                deleted: false,
                lsn: Lsn(i + 1),
                created_at: None,
            });
        }

        let changeset = ChangeSet {
            rows,
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: vec![contextdb_engine::sync_types::DdlChange::CreateTable {
                name: "t".to_string(),
                columns: vec![
                    ("id".to_string(), "UUID".to_string()),
                    ("data".to_string(), "TEXT".to_string()),
                ],
                constraints: vec!["PRIMARY KEY (id)".to_string()],
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],

            ddl_lsn: vec![Lsn(1)],
        };

        let batches = split_changeset(changeset);

        // Must split into 2+ batches (10 rows * ~100KB > 800KB)
        // because each row is from a distinct sender LSN and can be split.
        assert!(
            batches.len() >= 2,
            "10 rows of ~100KB each (~1MB total) must split into at least 2 batches, got {}",
            batches.len()
        );

        // Each batch's serialized size must be under 800KB
        for (i, batch) in batches.iter().enumerate() {
            let wire = WireChangeSet::from(batch.clone());
            let size = rmp_serde::to_vec(&wire)
                .expect("a14 batch should serialize for byte-size accounting")
                .len();
            assert!(
                size <= 800 * 1024,
                "batch {} serialized to {} bytes, exceeds 800KB limit",
                i,
                size
            );
        }

        // DDL only in first batch
        assert!(!batches[0].ddl.is_empty(), "DDL must be in first batch");
        for batch in &batches[1..] {
            assert!(
                batch.ddl.is_empty(),
                "DDL must NOT be in subsequent batches"
            );
            assert!(
                batch.edges.is_empty(),
                "edges must NOT be in subsequent batches"
            );
        }
    }

    #[test]
    fn nv_snapshot_split_batches_emit_ddl_before_vector_batches_and_apply_cleanly() {
        let table = "snapshot_split_evidence";
        let ddl_only_marker = "ddl_order_marker_column";
        let vector_column = "vector_later_marker_text";
        let row_count = 10usize;
        let large_payload = "x".repeat(120 * 1024);
        let mut ids = Vec::new();
        let mut rows = Vec::new();
        let mut vectors = Vec::new();

        for i in 0..row_count {
            let id = Uuid::new_v4();
            ids.push(id);
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Uuid(id));
            values.insert("payload".to_string(), Value::Text(large_payload.clone()));
            rows.push(RowChange {
                table: table.to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
                values,
                deleted: false,
                lsn: Lsn((i + 1) as u64),
                created_at: None,
            });
            vectors.push(VectorChange {
                index: contextdb_core::VectorIndexRef::new(table, vector_column),
                row_id: RowId((i + 1) as u64),
                vector: if i == 0 {
                    vec![1.0, 0.0, 0.0, 0.0]
                } else {
                    vec![0.0, 1.0, 0.0, 0.0]
                },
                lsn: Lsn((i + 1) as u64),
            });
        }

        let changeset = ChangeSet {
            rows,
            edges: Vec::new(),
            vectors,
            ddl: vec![contextdb_engine::sync_types::DdlChange::CreateTable {
                name: table.to_string(),
                columns: vec![
                    ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                    ("payload".to_string(), "TEXT".to_string()),
                    (ddl_only_marker.to_string(), "TEXT".to_string()),
                    (vector_column.to_string(), "VECTOR(4)".to_string()),
                ],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],

            ddl_lsn: vec![Lsn(1)],
        };

        let batches = split_changeset(changeset);
        assert!(
            batches.len() >= 2,
            "snapshot-shaped changeset with large rows must exercise real split path; got {} batch(es)",
            batches.len()
        );
        let first_ddl_idx = batches
            .iter()
            .position(|batch| !batch.ddl.is_empty())
            .expect("split stream must include schema DDL");
        let first_vector_idx = batches
            .iter()
            .position(|batch| !batch.vectors.is_empty())
            .expect("split stream must include vector changes");
        assert!(
            first_ddl_idx <= first_vector_idx,
            "first vector batch must not be emitted before schema DDL; first_ddl_idx={first_ddl_idx}, first_vector_idx={first_vector_idx}"
        );
        for (idx, batch) in batches.iter().enumerate().skip(first_ddl_idx + 1) {
            assert!(
                batch.ddl.is_empty(),
                "schema DDL must appear once before vector replay, not again in batch {idx}"
            );
        }

        if first_ddl_idx == first_vector_idx {
            fn byte_pos(haystack: &[u8], needle: &str) -> usize {
                haystack
                    .windows(needle.len())
                    .position(|window| window == needle.as_bytes())
                    .unwrap_or_else(|| panic!("encoded batch must contain sentinel {needle:?}"))
            }

            let bytes = rmp_serde::to_vec(&WireChangeSet::from(batches[first_vector_idx].clone()))
                .expect("encode split vector-bearing batch");
            let ddl_marker_pos = byte_pos(&bytes, ddl_only_marker);
            let vector_marker_pos = byte_pos(&bytes, vector_column);
            assert!(
                ddl_marker_pos < vector_marker_pos,
                "vector-bearing split batch must serialize schema bytes before vector index bytes; \
                 ddl_marker_pos={ddl_marker_pos}, vector_marker_pos={vector_marker_pos}, encoded_len={}",
                bytes.len()
            );
        }

        let receiver = Database::open_memory();
        let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
        for (idx, batch) in batches.into_iter().enumerate() {
            receiver
                .apply_changes(batch, &policies)
                .unwrap_or_else(|err| panic!("receiver must apply split batch {idx}: {err}"));
        }

        let rows = receiver
            .execute(&format!("SELECT id FROM {table}"), &HashMap::new())
            .expect("receiver must expose replayed rows after split apply");
        assert_eq!(
            rows.rows.len(),
            row_count,
            "fresh receiver must contain every row after applying split snapshot batches"
        );

        let mut params = HashMap::new();
        params.insert("q".to_string(), Value::Vector(vec![1.0, 0.0, 0.0, 0.0]));
        let nearest = receiver
            .execute(
                &format!("SELECT id FROM {table} ORDER BY {vector_column} <=> $q LIMIT 1"),
                &params,
            )
            .expect("receiver must expose replayed vector index after split apply");
        let id_idx = nearest
            .columns
            .iter()
            .position(|column| column == "id")
            .expect("nearest query must project id");
        assert_eq!(
            nearest.rows[0][id_idx],
            Value::Uuid(ids[0]),
            "replayed vector index must route to the declared table+column after split apply"
        );
    }

    #[test]
    fn a14b_batch_splitting_accounts_for_vector_sizes() {
        let mut rows = Vec::new();
        let mut vectors = Vec::new();
        for i in 0..200 {
            let id = Uuid::new_v4();
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Uuid(id));
            values.insert("data".to_string(), Value::Text("x".repeat(3000)));
            rows.push(RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
                values,
                deleted: false,
                lsn: Lsn(i as u64 + 1),
                created_at: None,
            });
            vectors.push(VectorChange {
                index: contextdb_core::VectorIndexRef::default(),
                row_id: RowId(0),
                vector: (0..384).map(|j| j as f32).collect(),
                lsn: Lsn(i as u64 + 1),
            });
        }
        let changeset = ChangeSet {
            rows,
            edges: Vec::new(),
            vectors,
            ddl: vec![],

            ddl_lsn: Vec::new(),
        };
        let batches = split_changeset(changeset);
        assert!(
            batches.len() >= 2,
            "200 rows with 384-dim vectors must split into 2+ batches with correct accounting, got {}",
            batches.len()
        );
        for (i, batch) in batches.iter().enumerate() {
            let wire = WireChangeSet::from(batch.clone());
            let size = rmp_serde::to_vec(&wire)
                .expect("a14b batch should serialize for byte-size accounting")
                .len();
            assert!(
                size <= 800 * 1024,
                "batch {} serialized to {} bytes, exceeds 800KB limit",
                i,
                size
            );
        }
    }

    #[test]
    fn a14c_batch_splitting_caps_many_small_lsn_groups_below_byte_limit() {
        let row_count = MAX_BATCH_DATA_LSN_GROUPS + 1;
        let mut rows = Vec::new();
        for i in 0..row_count {
            let id = Uuid::new_v4();
            rows.push(RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("data".to_string(), Value::Text(format!("small-{i}"))),
                ]),
                deleted: false,
                lsn: Lsn(i as u64 + 1),
                created_at: None,
            });
        }
        let changeset = ChangeSet {
            rows,
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: Vec::new(),
            ddl_lsn: Vec::new(),
        };

        let encoded_size = batch_wire_size(&changeset);
        assert!(
            encoded_size <= MAX_BATCH_BYTES,
            "fixture must stay below byte split threshold to exercise the group-cap fast-path guard; encoded_size={encoded_size}"
        );

        let batches = split_changeset(changeset);
        assert_eq!(
            batches.len(),
            2,
            "{} small one-row LSN groups must split into two capped batches even under the byte limit",
            row_count
        );
        assert_eq!(
            batches.iter().map(|batch| batch.rows.len()).sum::<usize>(),
            row_count,
            "all rows must remain present across capped batches"
        );
        assert!(
            batches
                .iter()
                .all(|batch| data_lsn_group_count(batch) <= MAX_BATCH_DATA_LSN_GROUPS),
            "each split batch must obey the complete data-LSN-group cap; batches={batches:?}"
        );
    }

    // A15: split_changeset handles a single row that alone exceeds MAX_BATCH_BYTES
    #[test]
    fn a15_split_changeset_single_oversized_row() {
        let oversized_text = "x".repeat(600 * 1024);
        let id = Uuid::new_v4();
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Uuid(id));
        values.insert("data".to_string(), Value::Text(oversized_text));
        let row = RowChange {
            table: "observations".to_string(),
            natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
            values,
            deleted: false,
            lsn: Lsn(1),
            created_at: None,
        };
        let changeset = ChangeSet {
            rows: vec![row],
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: Vec::new(),

            ddl_lsn: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert!(
            !batches.is_empty(),
            "split_changeset must return at least one batch, got {}",
            batches.len()
        );
        let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();
        assert_eq!(
            total_rows, 1,
            "the single oversized row must appear in exactly one batch, got {}",
            total_rows
        );
    }

    // A16: split_changeset preserves row/vector pairing across batch boundaries
    #[test]
    fn a16_split_changeset_preserves_row_vector_pairing() {
        use contextdb_engine::sync_types::VectorChange;

        let mut rows = Vec::new();
        let mut vectors = Vec::new();
        for i in 0..10usize {
            let id = Uuid::new_v4();
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Uuid(id));
            values.insert("data".to_string(), Value::Text("x".repeat(100 * 1024)));
            rows.push(RowChange {
                table: "observations".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
                values,
                deleted: false,
                lsn: Lsn((i + 1) as u64),
                created_at: None,
            });
            vectors.push(VectorChange {
                index: contextdb_core::VectorIndexRef::default(),
                row_id: RowId((i + 1) as u64),
                vector: vec![i as f32; 3],
                lsn: Lsn((i + 1) as u64),
            });
        }
        let changeset = ChangeSet {
            rows,
            edges: Vec::new(),
            vectors,
            ddl: Vec::new(),

            ddl_lsn: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert!(
            batches.len() >= 2,
            "10 rows * ~100KB each must split into at least 2 batches, got {}",
            batches.len()
        );
        let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();
        let total_vecs: usize = batches.iter().map(|b| b.vectors.len()).sum();
        assert_eq!(total_rows, 10, "all 10 rows must be present across batches");
        assert_eq!(
            total_vecs, 10,
            "all 10 vectors must be present across batches"
        );
        for (i, batch) in batches.iter().enumerate() {
            assert_eq!(
                batch.rows.len(),
                batch.vectors.len(),
                "batch {} must have equal row and vector counts: rows={}, vectors={}",
                i,
                batch.rows.len(),
                batch.vectors.len()
            );
            for j in 0..batch.rows.len() {
                assert_eq!(
                    batch.rows[j].lsn, batch.vectors[j].lsn,
                    "batch {} position {}: row.lsn={} != vector.lsn={} — pairing is broken",
                    i, j, batch.rows[j].lsn, batch.vectors[j].lsn
                );
            }
        }
    }

    // A17: split_changeset on empty input returns exactly one empty batch
    #[test]
    fn a17_split_changeset_empty_input_returns_one_batch() {
        let changeset = ChangeSet {
            rows: Vec::new(),
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: Vec::new(),

            ddl_lsn: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert_eq!(
            batches.len(),
            1,
            "empty changeset must produce exactly 1 batch (not 0), got {}",
            batches.len()
        );
        assert!(
            batches[0].rows.is_empty(),
            "the single batch for an empty input must have no rows"
        );
    }

    // A18: split_changeset with edge-only changeset must not return vec![]
    #[test]
    fn a18_split_changeset_edge_only_not_dropped() {
        use contextdb_engine::sync_types::EdgeChange;

        let mut edges = Vec::new();
        for _ in 0..200 {
            edges.push(EdgeChange {
                source: Uuid::new_v4(),
                target: Uuid::new_v4(),
                edge_type: "x".repeat(5_000),
                properties: HashMap::new(),
                lsn: Lsn(1),
            });
        }
        let changeset = ChangeSet {
            rows: Vec::new(),
            edges,
            vectors: Vec::new(),
            ddl: Vec::new(),

            ddl_lsn: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert!(
            !batches.is_empty(),
            "edge-only changeset must produce at least 1 batch, got {} — edges silently dropped",
            batches.len()
        );
        let total_edges: usize = batches.iter().map(|b| b.edges.len()).sum();
        assert_eq!(
            total_edges, 200,
            "all 200 edges must be present across batches, got {}",
            total_edges
        );
    }

    // A19: split_changeset with DDL-only changeset must not return vec![]
    // Column names are padded to force estimated size > MAX_BATCH_BYTES
    #[test]
    fn a19_split_changeset_ddl_only_not_dropped() {
        use contextdb_engine::sync_types::DdlChange;

        let mut ddl = Vec::new();
        for i in 0..20 {
            ddl.push(DdlChange::CreateTable {
                name: format!("table_{}", i),
                columns: (0..100)
                    .map(|j| (format!("col_{}_{}", j, "x".repeat(500)), "TEXT".to_string()))
                    .collect(),
                constraints: vec![format!("PRIMARY KEY (col_{})", "x".repeat(500))],
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            });
        }
        let changeset = ChangeSet {
            rows: Vec::new(),
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl,

            ddl_lsn: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert!(
            !batches.is_empty(),
            "DDL-only changeset must produce at least 1 batch, got {} — DDL silently dropped",
            batches.len()
        );
        let total_ddl: usize = batches.iter().map(|b| b.ddl.len()).sum();
        assert_eq!(
            total_ddl, 20,
            "all 20 DDL entries must be present across batches, got {}",
            total_ddl
        );
    }

    #[test]
    fn a20_split_changeset_emits_trigger_bootstrap_barrier_even_when_small() {
        let id = Uuid::new_v4();
        let changeset = ChangeSet {
            rows: vec![RowChange {
                table: "host_writes".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    (
                        "content".to_string(),
                        Value::Text("after-trigger".to_string()),
                    ),
                ]),
                deleted: false,
                lsn: Lsn(3),
                created_at: None,
            }],
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: vec![
                DdlChange::CreateTable {
                    name: "host_writes".to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("content".to_string(), "TEXT".to_string()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTrigger {
                    name: "host_write_trigger".to_string(),
                    table: "host_writes".to_string(),
                    on_events: vec!["INSERT".to_string()],
                },
            ],
            ddl_lsn: vec![Lsn(2), Lsn(2)],
        };

        let batches = split_changeset(changeset);
        assert!(
            batches.len() >= 2
                && batches
                    .iter()
                    .any(|batch| batch.has_create_trigger_ddl() && batch.data_entry_count() == 0)
                && batches.iter().position(ChangeSet::has_create_trigger_ddl)
                    < batches.iter().position(|batch| !batch.rows.is_empty()),
            "small full-history batches must surface CREATE TRIGGER before any data so receivers can register callbacks; batches={batches:?}"
        );
        assert_eq!(
            batches
                .iter()
                .find(|batch| batch.has_create_trigger_ddl())
                .and_then(ChangeSet::max_lsn),
            Some(Lsn(2)),
            "trigger bootstrap batch cursor must advance through schema without skipping first data LSN; batches={batches:?}"
        );
        assert!(
            batches
                .iter()
                .filter(|batch| !batch.rows.is_empty())
                .all(|batch| batch.ddl.is_empty() && batch.max_lsn() == Some(Lsn(3))),
            "data after trigger bootstrap must remain available at its sender LSN without duplicated DDL; batches={batches:?}"
        );
    }

    #[test]
    fn a21_split_changeset_does_not_fabricate_cursor_for_same_lsn_trigger_data() {
        let id = Uuid::new_v4();
        let changeset = ChangeSet {
            rows: vec![RowChange {
                table: "host_writes".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    (
                        "content".to_string(),
                        Value::Text("same-lsn-trigger-data".to_string()),
                    ),
                ]),
                deleted: false,
                lsn: Lsn(3),
                created_at: None,
            }],
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: vec![DdlChange::CreateTrigger {
                name: "host_write_trigger".to_string(),
                table: "host_writes".to_string(),
                on_events: vec!["INSERT".to_string()],
            }],
            ddl_lsn: vec![Lsn(3)],
        };

        let batches = split_changeset(changeset);
        assert_eq!(
            batches.len(),
            1,
            "a same-LSN trigger DDL/data group cannot be split safely with an exclusive LSN cursor; batches={batches:?}"
        );
        assert_eq!(
            batches[0].max_lsn(),
            Some(Lsn(3)),
            "splitter must preserve the real sender LSN instead of fabricating LSN-1 progress"
        );
        assert!(
            batches[0].has_create_trigger_ddl() && batches[0].data_entry_count() == 1,
            "same-LSN trigger DDL/data remains atomic and will fail closed at apply if callbacks are missing; batches={batches:?}"
        );
    }

    #[test]
    fn acknowledgement_stamped_push_batches_keep_exact_data_lsn_groups() {
        let id = Uuid::new_v4();
        let changeset = ChangeSet {
            rows: vec![
                RowChange {
                    table: "notes".to_string(),
                    natural_key: NaturalKey::single("id".to_string(), Value::Uuid(id)),
                    values: HashMap::from([("id".to_string(), Value::Uuid(id))]),
                    deleted: false,
                    lsn: Lsn(7),
                    created_at: None,
                },
                RowChange {
                    table: "notes".to_string(),
                    natural_key: NaturalKey::single("id".to_string(), Value::Int64(2)),
                    values: HashMap::from([("id".to_string(), Value::Int64(2))]),
                    deleted: false,
                    lsn: Lsn(8),
                    created_at: None,
                },
            ],
            edges: Vec::new(),
            vectors: vec![VectorChange {
                index: contextdb_core::VectorIndexRef::new("notes", "embedding"),
                row_id: RowId(44),
                vector: vec![1.0, 0.0],
                lsn: Lsn(7),
            }],
            ddl: Vec::new(),
            ddl_lsn: Vec::new(),
        };

        let batches = acceptance_stamped_push_batches(changeset);
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| {
            let lsns = batch
                .rows
                .iter()
                .map(|row| row.lsn)
                .chain(batch.vectors.iter().map(|vector| vector.lsn))
                .collect::<std::collections::BTreeSet<_>>();
            lsns.len() == 1
        }));
        assert_eq!(batches[0].rows.len(), 1);
        assert_eq!(batches[0].vectors.len(), 1);
        assert_eq!(batches[0].max_lsn(), Some(Lsn(7)));
        assert_eq!(batches[1].max_lsn(), Some(Lsn(8)));
    }

    #[test]
    fn acknowledgement_stamped_push_never_splits_one_oversized_atomic_lsn_group() {
        let changeset = ChangeSet {
            rows: vec![
                RowChange {
                    table: "notes".to_string(),
                    natural_key: NaturalKey::single("id".to_string(), Value::Int64(1)),
                    values: HashMap::from([
                        ("id".to_string(), Value::Int64(1)),
                        ("body".to_string(), Value::Text("x".repeat(MAX_BATCH_BYTES))),
                    ]),
                    deleted: false,
                    lsn: Lsn(44),
                    created_at: None,
                },
                RowChange {
                    table: "notes".to_string(),
                    natural_key: NaturalKey::single("id".to_string(), Value::Int64(2)),
                    values: HashMap::from([
                        ("id".to_string(), Value::Int64(2)),
                        ("body".to_string(), Value::Text("y".repeat(MAX_BATCH_BYTES))),
                    ]),
                    deleted: false,
                    lsn: Lsn(44),
                    created_at: None,
                },
            ],
            ..ChangeSet::default()
        };
        assert!(batch_wire_size(&changeset) > MAX_BATCH_BYTES);
        let batches = acceptance_stamped_push_batches(changeset);
        assert_eq!(
            batches.len(),
            1,
            "one atomic source LSN may exceed the size target but cannot be split into inexact acknowledgement groups"
        );
        assert_eq!(batches[0].rows.len(), 2);
        assert_eq!(batches[0].max_lsn(), Some(Lsn(44)));
    }
}
