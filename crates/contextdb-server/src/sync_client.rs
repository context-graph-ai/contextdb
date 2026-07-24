use crate::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, SyncStatusRequest,
    SyncStatusResponse, WireChangeSet, decode, encode, row_payload_bytes,
};
use crate::subjects::{pull_subject, push_subject, status_subject};
use crate::transfer_receipts::{TransferDirection, TransferLedger, TransferPlane, TransferReceipt};
use crate::transport::{ClientTransport, TransportError};
use contextdb_core::{AtomicLsn, Error, Incarnation, Lsn, TableMeta, TenantId};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, ConflictPolicies, ConflictPolicy, SyncAdoption, SyncDirection,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const SYNC_TIMEOUT: Duration = Duration::from_secs(60);
const PUSH_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
/// Bound on the sync-status probe. No responder / timeout / malformed
/// reply all degrade to "no status" and the sync proceeds exactly as before
/// the probe existed (contract item 5: never hang against an old server).
const STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const PULL_PAGE_SIZE: u32 = 500;
const MAX_BATCH_BYTES: usize = 800 * 1024;
const BATCH_ESTIMATE_SAFETY_MARGIN: usize = 32 * 1024;
const TARGET_BATCH_BYTES: usize = MAX_BATCH_BYTES - BATCH_ESTIMATE_SAFETY_MARGIN;
const MAX_BATCH_DATA_LSN_GROUPS: usize = 100;

pub struct SyncClient {
    db: Arc<Database>,
    transport: Arc<dyn ClientTransport>,
    endpoint: String,
    tenant_id: TenantId,
    push_watermark: AtomicLsn,
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
    /// Directions set at runtime by the embedding application. These fill in
    /// for tables that DECLARED no direction; a table's own declaration is the
    /// source of truth and is never overridden from here, so what the
    /// application wrote in its schema is what happens.
    table_directions: std::sync::RwLock<HashMap<String, SyncDirection>>,
    conflict_policies: std::sync::RwLock<ConflictPolicies>,
    /// Per-peer transfer counters for the sync plane. In memory only.
    receipts: Arc<TransferLedger>,
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
            .field(
                "table_directions",
                &self.table_directions.read().map(|g| g.clone()).ok(),
            )
            .field(
                "conflict_policies",
                &self.conflict_policies.read().map(|g| g.clone()).ok(),
            )
            .finish()
    }
}

impl SyncClient {
    pub fn new(db: Arc<Database>, endpoint: &str, tenant_id: TenantId) -> Self {
        Self::build(
            db,
            crate::transport::client_transport(endpoint),
            endpoint.to_string(),
            tenant_id,
        )
    }

    fn build(
        db: Arc<Database>,
        transport: Arc<dyn ClientTransport>,
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
        Self {
            db,
            transport,
            endpoint,
            tenant_id,
            push_watermark: AtomicLsn::new(push_watermark),
            pull_watermark: AtomicLsn::new(pull_watermark),
            pages_discarded_for_source_mismatch: AtomicUsize::new(0),
            pull_source: std::sync::RwLock::new(pull_source),
            table_directions: std::sync::RwLock::new(HashMap::new()),
            conflict_policies: std::sync::RwLock::new(ConflictPolicies {
                per_table: HashMap::new(),
                default: ConflictPolicy::ServerWins,
            }),
            receipts: Arc::new(TransferLedger::new()),
        }
    }

    /// Construct a client that talks over `transport` instead of the default
    /// NATS transport. Used to drive sync with no broker.
    pub fn with_transport(
        db: Arc<Database>,
        transport: Arc<dyn crate::transport::ClientTransport>,
        tenant_id: TenantId,
    ) -> Self {
        Self::build(db, transport, "in-process".to_string(), tenant_id)
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

    /// An owned handle to this client's database, for a caller (the work
    /// ledger's worker loop) that needs a `'static`-safe handle to move onto
    /// a blocking-pool thread — [`Self::db`] borrows for this client's own
    /// lifetime, which is not `'static`-compatible.
    pub(crate) fn db_arc(&self) -> Arc<Database> {
        self.db.clone()
    }

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

    /// Before the FIRST retained-table push, bind this database to the hub it
    /// is delivering to. A retained table is delivered to exactly one hub — a
    /// second, different hub is refused here, by the push itself, with no
    /// cooperation from the embedding application and before a single retained
    /// row leaves the edge.
    fn bind_retention_hub(&self) -> Result<(), Error> {
        if !self.has_retained_table_that_delivers()? {
            return Ok(());
        }
        let Some(hub) = self.hub_node_id() else {
            return Ok(());
        };
        self.db.register_retention_sync_peer(&hub)
    }

    /// Whether any RETAINED table on this database actually delivers rows
    /// outbound. Any delivering direction arms the binding — not push-only
    /// alone — because a two-way retained table's rows are just as subject to
    /// delete-after-delivery, and a binding that failed to arm would let the
    /// gate open against a destination this edge was never bound to.
    fn has_retained_table_that_delivers(&self) -> Result<bool, Error> {
        let directions = self.table_directions()?;
        Ok(self.db.table_names().into_iter().any(|table| {
            self.db
                .table_meta(&table)
                .is_some_and(|meta| meta.default_ttl_seconds.is_some())
                && directions
                    .get(&table)
                    .copied()
                    .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION)
                    .delivers()
        }))
    }

    pub fn has_pending_push_changes(&self) -> Result<bool, Error> {
        let since = self.push_watermark.load(Ordering::SeqCst);
        let directions = self.table_directions()?;
        let changes = self
            .db
            .changes_since(since)
            .filter_by_direction(&directions, &[SyncDirection::Push, SyncDirection::Both]);
        let changes = drop_rows_that_arrived_by_sync(&self.db, changes);
        Ok(!changes.rows.is_empty()
            || !changes.edges.is_empty()
            || !changes.vectors.is_empty()
            || !changes.ddl.is_empty())
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

    async fn request_push_once(&self, encoded: Vec<u8>) -> Result<ApplyResult, Error> {
        let reply = self
            .transport
            .request(
                &push_subject(self.tenant_id.as_str()),
                encoded,
                SYNC_TIMEOUT,
            )
            .await
            .map_err(|err| Error::SyncError(format!("single-attempt push failed: {err}")))?;
        decode_push_response(&reply).map_err(PushReplyError::into_error)
    }

    async fn request_push(&self, encoded: Vec<u8>) -> Result<ApplyResult, Error> {
        match self.transport.ensure_single_reply_retry_safe(&encoded) {
            Ok(()) => {}
            Err(TransportError::RetryUnsafe(detail)) => {
                tracing::debug!(
                    %detail,
                    "push request requires a single-attempt transport send"
                );
                return self.request_push_once(encoded).await;
            }
            Err(err) => return Err(Error::SyncError(format!("push failed: {err}"))),
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
                    Err(PushReplyError::Malformed(err)) => return Err(err),
                    Err(PushReplyError::Terminal(err)) => return Err(err),
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
                    return Err(Error::SyncError(format!(
                        "push response incomplete: {detail}"
                    )));
                }
                Err(err) => return Err(Error::SyncError(format!("push failed: {err}"))),
            }
        }
        push_result.ok_or_else(|| {
            Error::SyncError("push failed after retries: no response from server".to_string())
        })
    }

    async fn request_pull(&self, request: PullRequest) -> Result<PullResponse, Error> {
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
        if !matches!(envelope.message_type, MessageType::PullResponse) {
            return Err(Error::SyncError(
                "unexpected message type in pull response".to_string(),
            ));
        }
        rmp_serde::from_slice(&envelope.payload).map_err(|e| Error::SyncError(e.to_string()))
    }

    pub async fn push(&self) -> Result<ApplyResult, Error> {
        // Verify connectivity early so users get a clear error even for empty pushes.
        self.ensure_connected().await.map_err(Error::SyncError)?;
        self.bind_retention_hub()?;
        self.refuse_directions_that_break_delivery()?;

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
        let local = self.push_watermark.load(Ordering::SeqCst);
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
            self.push_watermark.store(server_applied, Ordering::SeqCst);
            self.db
                .persist_sync_push_watermark(&self.tenant_id, server_applied)
                .map_err(|err| Error::SyncError(err.to_string()))?;
        }

        let since = self.push_watermark.load(Ordering::SeqCst);
        // Clone directions out of RwLock BEFORE any .await
        let directions = self.table_directions()?;
        refuse_keyless_tables_with_no_identity_fallback(&self.db, &directions)?;
        let (changeset, arrivals) = self.db.changes_since_with_arrivals(since);
        let changeset =
            changeset.filter_by_direction(&directions, &[SyncDirection::Push, SyncDirection::Both]);
        let changeset = drop_rows_that_arrived_by_sync(&self.db, changeset);

        // The greatest LSN this push actually TRANSMITS, taken PRE-send from the
        // changeset computed under the directions read above. A lost-ack
        // reconciliation must bound the hub's answer by this — recomputing it
        // after the await would let a concurrent direction change (a delivering
        // table switched to SYNC OFF) drop the bound below what already shipped
        // and reject a batch the hub genuinely holds.
        let transmitted_ceiling = changeset.max_lsn().unwrap_or(Lsn(0));

        if changeset.rows.is_empty()
            && changeset.edges.is_empty()
            && changeset.vectors.is_empty()
            && changeset.ddl.is_empty()
        {
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

        let hub = self.hub_node_id();
        let mut last_successful_lsn = since;
        for batch in split_changeset(changeset) {
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
            let request = PushRequest {
                changeset: crate::protocol::wire_changeset_with_arrivals(batch.clone(), &arrivals),
                incarnation,
            };
            let encoded = encode(MessageType::PushRequest, &request)
                .map_err(|e| Error::SyncError(e.to_string()))?;

            let result: ApplyResult = match self.request_push(encoded).await {
                Ok(result) => result,
                Err(err) => {
                    // The batch bytes already left the edge, so this failure is
                    // INDETERMINATE: the hub may have applied and committed the
                    // batch before the acknowledgement was lost. Reconcile once
                    // against the hub's applied-push watermark before reporting,
                    // so a push whose data actually landed is never announced as
                    // a definitive failure (usability job USR-19). Convergence is
                    // unchanged: the watermark advances only on a CONFIRMED
                    // batch, and an unconfirmed outcome leaves it untouched so a
                    // later push re-sends the same batch idempotently.
                    return self
                        .finish_interrupted_push(
                            err,
                            batch_max_lsn,
                            total,
                            hub.as_deref(),
                            batch_items,
                            batch_payload_bytes,
                            transmitted_ceiling,
                            incarnation,
                        )
                        .await;
                }
            };
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

        self.push_watermark
            .store(last_successful_lsn, Ordering::SeqCst);
        self.db
            .persist_sync_push_watermark(&self.tenant_id, last_successful_lsn)
            .map_err(|err| Error::SyncError(err.to_string()))?;
        self.advance_engine_sync_watermark(last_successful_lsn);
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
    ///   batch DID land: advance and persist the push watermark to it and report
    ///   the push as the success it was. Any later batches this run never sent
    ///   reconcile idempotently on the next push.
    /// * Otherwise the hub is unreachable or its watermark does not (yet) confirm
    ///   the batch. The outcome is genuinely UNKNOWN, so surface the distinct
    ///   [`Error::SyncPushUnconfirmed`] (never a definitive failure) and leave the
    ///   watermark untouched so a later push re-sends the batch idempotently.
    #[allow(clippy::too_many_arguments)]
    async fn finish_interrupted_push(
        &self,
        transport_err: Error,
        batch_max_lsn: Lsn,
        applied_before_interruption: ApplyResult,
        hub: Option<&str>,
        batch_items: u64,
        batch_payload_bytes: u64,
        transmitted_ceiling: Lsn,
        incarnation: Incarnation,
    ) -> Result<ApplyResult, Error> {
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
            tracing::info!(
                tenant_id = %self.tenant_id,
                batch_max_lsn = batch_max_lsn.0,
                "push acknowledgement was lost but the hub confirms the batch landed; reconciling to success"
            );
            self.push_watermark.store(batch_max_lsn, Ordering::SeqCst);
            self.db
                .persist_sync_push_watermark(&self.tenant_id, batch_max_lsn)
                .map_err(|err| Error::SyncError(err.to_string()))?;
            self.advance_engine_sync_watermark(batch_max_lsn);
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
            return Ok(applied_before_interruption);
        }

        Err(Error::SyncPushUnconfirmed {
            detail: format!(
                "the hub did not acknowledge the push and its status could not confirm the batch \
                 landed ({transport_err}); the data may or may not have committed — run the push \
                 again to reconcile"
            ),
        })
    }

    /// Pull with explicit policies (frozen test contract, library consumers).
    pub async fn pull(&self, policies: &ConflictPolicies) -> Result<ApplyResult, Error> {
        self.ensure_connected().await.map_err(Error::SyncError)?;
        // The work ledger's per-table policies are merged over whatever the
        // caller passes (they are the ledger's contract, not caller policy):
        // on pull, the ServerWins entries remap to EdgeWins below, which is
        // what reconciles a losing edge's claim/result row to the hub's row.
        let mut policies = policies.clone();
        contextdb_engine::work_ledger::apply_work_ledger_policy_overrides(&mut policies);
        contextdb_engine::peer_directory::apply_peer_directory_policy_overrides(&mut policies);
        let policies = &policies;
        let directions = self.table_directions()?;

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
        if let Some(status) = self.fetch_sync_status(incarnation).await
            && let Some(server_lsn) = status.server_current_lsn
            && server_lsn < local
        {
            tracing::info!(
                tenant_id = %self.tenant_id,
                local_watermark = local.0,
                server_current_lsn = server_lsn.0,
                "server LSN clock behind local pull watermark; resetting pull watermark to re-pull"
            );
            self.pull_watermark.store(Lsn(0), Ordering::SeqCst);
            self.db
                .persist_sync_pull_watermark(&self.tenant_id, Lsn(0))
                .map_err(|err| Error::SyncError(err.to_string()))?;
        }

        let hub = self.hub_node_id();
        let mut since_lsn = self.pull_watermark.load(Ordering::SeqCst);
        #[allow(unused_assignments)]
        let mut last_server_lsn = since_lsn;
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
        let mut first_page = true;
        // Set once this call detects its cursor's source changed, and never
        // cleared for the rest of THIS call: every page from that point on
        // is part of the same from-zero re-fetch of the newly adopted
        // source's full history (see `SyncAdoption`).
        let mut adoption = SyncAdoption::Continuing;

        loop {
            let request = PullRequest {
                since_lsn,
                max_entries: Some(PULL_PAGE_SIZE),
            };

            let response = self.request_pull(request).await?;
            let served_source = response.source;

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
                            expected_source = Some(served);
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

            let arrivals = crate::protocol::wire_row_arrivals(&response.changeset);
            let changes = ChangeSet::try_from(response.changeset)
                .map_err(|e| Error::SyncError(e.to_string()))?;
            let has_more = response.has_more;
            let cursor = response.cursor;

            // Extract server-side max LSN BEFORE filtering/applying
            let server_lsn = cursor.or_else(|| changes.max_lsn()).unwrap_or(since_lsn);

            let filtered = changes
                .filter_by_direction(&directions, &[SyncDirection::Pull, SyncDirection::Both]);
            // The declared one-way policy, read off THIS database's table meta
            // — not off runtime registration, which starts empty on every
            // construction. So it holds after a restart with no app call.
            let filtered = drop_push_only_retained_rows(&self.db, filtered);
            // Counted from the SAME row set the bytes are counted from: what
            // this end took off the wire. Using the applied count here instead
            // would pair an items figure with a payload figure drawn from two
            // different sets, so a pull with skipped rows would report bytes for
            // rows its own item count denied.
            let received_items = filtered.rows.len() as u64;
            let received_payload_bytes = row_payload_bytes(&filtered.rows);
            let stop_for_trigger_bootstrap = filtered.has_create_trigger_ddl() && has_more;
            let result = self.db.apply_synced_changes(
                filtered,
                &remap_pull_policies(policies),
                &arrivals,
                adoption,
            )?;
            self.receipts.record(
                hub.as_deref(),
                TransferPlane::Sync,
                TransferDirection::Received,
                received_items,
                received_payload_bytes,
            );
            total.applied_rows += result.applied_rows;
            total.skipped_rows += result.skipped_rows;
            total.conflicts.extend(result.conflicts);
            total.new_lsn = result.new_lsn;
            last_server_lsn = server_lsn;
            first_page = false;

            if !has_more {
                break;
            }
            if stop_for_trigger_bootstrap {
                break;
            }
            since_lsn = cursor.unwrap_or(since_lsn);
        }

        self.pull_watermark.store(last_server_lsn, Ordering::SeqCst);
        self.db
            .persist_sync_pull_watermark(&self.tenant_id, last_server_lsn)
            .map_err(|err| Error::SyncError(err.to_string()))?;
        if let Some(source) = expected_source {
            *self
                .pull_source
                .write()
                .unwrap_or_else(|err| err.into_inner()) = Some(source);
            self.db
                .persist_sync_pull_cursor(&self.tenant_id, source, last_server_lsn)
                .map_err(|err| Error::SyncError(err.to_string()))?;
        }
        Ok(total)
    }

    /// Pull using internally configured conflict policies (used by CLI).
    pub async fn pull_default(&self) -> Result<ApplyResult, Error> {
        let policies = self.conflict_policies()?;
        self.pull(&policies).await
    }

    /// Initial sync using explicit policies (frozen test contract).
    pub async fn initial_sync(&self, policies: &ConflictPolicies) -> Result<ApplyResult, Error> {
        self.pull(policies).await
    }

    pub fn push_watermark(&self) -> Lsn {
        self.push_watermark.load(Ordering::SeqCst)
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

    /// The configured sync endpoint (a ticket, or a deprecated broker URL).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The hub this edge is provably connected to, taken from the
    /// transport-authenticated node id. `None` when the transport authenticates
    /// no peer (e.g. not connected, or a broker transport that has no dialed
    /// endpoint). The operator surface reads this so a destination change adopts
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
        self.pull_watermark.store(Lsn(0), Ordering::SeqCst);
        // The pull cursor's bound source is explicitly stale the moment the
        // operator repoints the destination — the next pull binds fresh to
        // whatever the new destination reports, with no mismatch to detect.
        if let Ok(mut pull_source) = self.pull_source.write() {
            *pull_source = None;
        }
        result
    }

    /// Set the sync direction for `table`.
    ///
    /// Refused when the table is declared `SYNC SAFE` and the direction would
    /// stop delivering it: that declaration promises deletion only AFTER
    /// delivery, so a direction that keeps the table out of the outbound
    /// changeset would let retention delete rows the hub never received. A
    /// table whose meta is not known yet cannot be judged here — nothing is
    /// knowable about a table that does not exist — so the same contradiction
    /// is caught at push, once the table has arrived.
    pub fn set_table_direction(&self, table: &str, direction: SyncDirection) -> Result<(), Error> {
        refuse_direction_that_breaks_delivery(&self.db, table, direction)?;
        match self.table_directions.write() {
            Ok(mut directions) => {
                directions.insert(table.to_string(), direction);
            }
            Err(_) => tracing::warn!("sync table_directions lock poisoned; ignoring update"),
        }
        Ok(())
    }

    /// Re-check every RUNTIME-configured direction against the tables as they
    /// stand now. A direction set BEFORE its table existed could not be judged
    /// then; this is where that contradiction becomes visible, and the push
    /// refuses rather than shipping a changeset that silently omits a
    /// delivery-promising table while the watermark advances over everything
    /// else in it.
    ///
    /// This reads the RAW runtime settings, never the persisted declaration
    /// merged on top: the declaration is the source of truth for what actually
    /// crosses the wire, but a runtime setting that contradicts a `SYNC SAFE`
    /// table's delivery promise is a conflict to REFUSE loudly, not to resolve
    /// by silently letting the declaration win — the operator's explicit
    /// setting would then be ignored with no error. Merging the declaration
    /// here would mask exactly the contradiction this check exists to catch.
    fn refuse_directions_that_break_delivery(&self) -> Result<(), Error> {
        let runtime_directions = self
            .table_directions
            .read()
            .map(|directions| directions.clone())
            .map_err(|_| Error::SyncError("sync table directions lock poisoned".to_string()))?;
        for (table, direction) in runtime_directions {
            refuse_direction_that_breaks_delivery(&self.db, &table, direction)?;
        }
        Ok(())
    }

    pub fn set_conflict_policy(&self, table: &str, policy: ConflictPolicy) {
        match self.conflict_policies.write() {
            Ok(mut policies) => {
                policies.per_table.insert(table.to_string(), policy);
            }
            Err(_) => tracing::warn!("sync conflict_policies lock poisoned; ignoring update"),
        }
    }

    pub fn set_default_conflict_policy(&self, policy: ConflictPolicy) {
        match self.conflict_policies.write() {
            Ok(mut policies) => {
                policies.default = policy;
            }
            Err(_) => tracing::warn!("sync conflict_policies lock poisoned; ignoring update"),
        }
    }

    /// The direction every table actually syncs by. Runtime settings first,
    /// then the PERSISTED declarations on top of them: a table that declared
    /// `SYNC OFF` stays off whatever the application registered, so no second
    /// setting quietly wins. A table that declared nothing keeps whatever the
    /// application configured, and failing that the engine default.
    fn table_directions(&self) -> Result<HashMap<String, SyncDirection>, Error> {
        let mut directions = self
            .table_directions
            .read()
            .map(|directions| directions.clone())
            .map_err(|_| Error::SyncError("sync table directions lock poisoned".to_string()))?;
        for table in self.db.table_names() {
            if let Some(declared) = self
                .db
                .table_meta(&table)
                .and_then(|meta| meta.sync_direction)
            {
                directions.insert(table, declared);
            }
        }
        Ok(directions)
    }

    fn conflict_policies(&self) -> Result<ConflictPolicies, Error> {
        self.conflict_policies
            .read()
            .map(|policies| policies.clone())
            .map_err(|_| Error::SyncError("sync conflict policies lock poisoned".to_string()))
    }
}

#[derive(Debug)]
enum PushReplyError {
    Malformed(Error),
    Terminal(Error),
}

impl PushReplyError {
    fn into_error(self) -> Error {
        match self {
            PushReplyError::Malformed(err) | PushReplyError::Terminal(err) => err,
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

/// Refuse a direction that would stop a `SYNC SAFE` table being delivered.
/// A table with no such declaration promises nothing about delivery and may be
/// configured however the application likes, including out of the changeset.
fn refuse_direction_that_breaks_delivery(
    db: &Database,
    table: &str,
    direction: SyncDirection,
) -> Result<(), Error> {
    if direction_delivers(direction) {
        return Ok(());
    }
    let Some(meta) = db.table_meta(table) else {
        return Ok(());
    };
    if !meta.sync_safe {
        return Ok(());
    }
    Err(Error::SyncError(format!(
        "table '{table}' is declared SYNC SAFE, so its rows are deleted only after they are \
         DELIVERED to the hub. Sync direction {direction:?} would stop delivering '{table}', \
         and retention would then delete rows the hub never received. Leave it on Push."
    )))
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
/// local version arrived over sync is dropped from the outbound changeset.
/// Pushing it back would hand the hub its own data — the echo that makes a
/// pull-then-push cycle re-deliver rows forever — and it would also make the
/// transfer receipt count rows that never needed to move. A row that was
/// pulled and then EDITED here has lost the marker, so local work always
/// propagates. Edges, vectors and DDL are untouched.
pub(crate) fn drop_rows_that_arrived_by_sync(db: &Database, changes: ChangeSet) -> ChangeSet {
    let mut changes = changes;
    changes
        .rows
        .retain(|row| !db.row_version_arrived_by_sync(&row.table, &row.natural_key));
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

fn remap_pull_policies(policies: &ConflictPolicies) -> ConflictPolicies {
    let remap = |policy: ConflictPolicy| match policy {
        ConflictPolicy::ServerWins => ConflictPolicy::EdgeWins,
        ConflictPolicy::EdgeWins => ConflictPolicy::ServerWins,
        other => other,
    };

    ConflictPolicies {
        per_table: policies
            .per_table
            .iter()
            .map(|(table, policy)| (table.clone(), remap(*policy)))
            .collect(),
        default: remap(policies.default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::{RowId, Value};
    use contextdb_engine::Database;
    use contextdb_engine::sync_types::{DdlChange, NaturalKey, RowChange, VectorChange};
    #[cfg(feature = "nats")]
    use std::sync::Arc;
    #[cfg(feature = "nats")]
    use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
    #[cfg(feature = "nats")]
    use testcontainers::runners::AsyncRunner;
    #[cfg(feature = "nats")]
    use testcontainers::{ContainerAsync, GenericImage, ImageExt};
    use uuid::Uuid;

    #[cfg(feature = "nats")]
    struct NatsFixture {
        _container: ContainerAsync<GenericImage>,
        nats_url: String,
    }

    #[cfg(feature = "nats")]
    async fn start_nats() -> NatsFixture {
        let nats_conf = format!("{}/tests/nats.conf", env!("CARGO_MANIFEST_DIR"));

        let image = GenericImage::new("nats", "latest")
            .with_exposed_port(4222.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"));

        let request = image
            .with_mount(Mount::bind_mount(&nats_conf, "/etc/nats/nats.conf"))
            .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);

        let container: ContainerAsync<GenericImage> = request.start().await.unwrap();
        let nats_port = container.get_host_port_ipv4(4222.tcp()).await.unwrap();

        NatsFixture {
            _container: container,
            nats_url: format!("nats://127.0.0.1:{nats_port}"),
        }
    }

    #[cfg(feature = "nats")]
    #[tokio::test]
    async fn sync_01_client_push_survives_poisoned_direction_lock() {
        let nats = start_nats().await;
        let client = Arc::new(SyncClient::new(
            Arc::new(Database::open_memory()),
            &nats.nats_url,
            contextdb_core::TenantId::from("sync-01"),
        ));

        client.ensure_connected().await.expect("connect NATS");
        let poison_client = client.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison_client.table_directions.write().unwrap();
            panic!("poison sync_client directions lock");
        })
        .join();

        let join = tokio::spawn({
            let client = client.clone();
            async move { client.push().await }
        })
        .await;

        assert!(
            matches!(join, Ok(Err(Error::SyncError(_)))),
            "push should return a sync error instead of panicking on poisoned table_directions, got {join:?}"
        );
    }

    #[cfg(feature = "nats")]
    #[tokio::test]
    async fn sync_02_client_pull_default_survives_poisoned_policy_lock() {
        let nats = start_nats().await;
        let client = Arc::new(SyncClient::new(
            Arc::new(Database::open_memory()),
            &nats.nats_url,
            contextdb_core::TenantId::from("sync-02"),
        ));

        client.ensure_connected().await.expect("connect NATS");
        let poison_client = client.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison_client.conflict_policies.write().unwrap();
            panic!("poison sync_client policies lock");
        })
        .join();

        let join = tokio::spawn({
            let client = client.clone();
            async move { client.pull_default().await }
        })
        .await;

        assert!(
            matches!(join, Ok(Err(Error::SyncError(_)))),
            "pull_default should return a sync error instead of panicking on poisoned conflict_policies, got {join:?}"
        );
    }

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
}
