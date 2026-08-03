//! The media data plane: content-address + serve local blobs (holder side)
//! and resolve blob_ref references node-to-node (consumer side). Moves OPAQUE
//! bytes + a hash — no work-class meaning (Rule 5). The fetch backend lives
//! behind the adapter boundary (Rule 2); this module names only contextdb
//! types and the adapter's opaque facade.
//!
//! A consumer may only resolve a blob while it holds a LIVE work-ledger
//! claim on a job whose inputs reference that blob's hash; see
//! [`BlobStore::resolve_blob_ref`] for the entitlement precondition and
//! error conditions.

use crate::Database;
use crate::transfer_receipts::{TransferDirection, TransferLedger, TransferPlane, TransferReceipt};
use crate::transport::adapter::{self, BlobStoreHandle, FetchFailure, FetchVerdict};
use crate::transport::iroh::IrohServer;
use crate::work_ledger::{
    BlobHash, MovementPolicy, any_node_holds_claim_for_blob, blob_ref_retention_eligible,
    node_claim_permits_movement, node_holds_claim_for_blob,
};
use contextdb_core::{Error, Result, Wallclock};
#[cfg(any(test, feature = "test-seams"))]
use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// A holder-side hook that refreshes THIS node's own ledger mirror (e.g. a
/// `SyncClient::pull_default()`) before a second, final entitlement check.
/// [`BlobStore::serve_on`]'s verdict handler invokes it AT MOST ONCE per
/// fetch request, and only on an initial claim-lookup miss — never polled,
/// never retried. `None` (the default every existing caller gets) preserves
/// today's behavior exactly: a miss is a miss, checked once against
/// whatever this node's local mirror already holds.
pub type ClaimRefreshHook =
    Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// The owned reclaim cadence (see [`BlobStore::spawn_reclaim_driver`]).
const RECLAIM_INTERVAL: Duration = Duration::from_secs(300);

/// Operator-declared bound on one [`BlobStore::resolve_blob_ref`] fetch
/// attempt, covering the whole dial-and-transfer path — not just the
/// per-progress-item idle bound already applied inside the transfer loop.
/// Declared like [`crate::work_ledger::MovementPolicy`] (a plain
/// struct an installation supplies) rather than a bare `Duration` constant
/// buried in the resolver, so an installation can widen or narrow the bound
/// without a code change. `resolve_blob_ref` enforces it by spawning the
/// fetch attempt and timing out the JOIN; an installation sets a
/// non-default bound via [`BlobStore::set_fetch_policy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobFetchPolicy {
    /// The whole-attempt deadline, in milliseconds.
    pub fetch_deadline_ms: u64,
}

impl Default for BlobFetchPolicy {
    fn default() -> Self {
        // Documented default: generous enough for a legitimately large
        // resume/transfer over a slow link, bounded enough that a wedge
        // surfaces as a typed error instead of an un-cancellable hang.
        Self {
            fetch_deadline_ms: 120_000,
        }
    }
}

impl BlobFetchPolicy {
    /// The declared bound as a [`Duration`], for `resolve_blob_ref`'s
    /// deadline wrap. Kept off that function's own body deliberately: the
    /// no-bare-duration-literal guard (`resolve_blob_ref_body_has_no_bare_duration_literal`)
    /// exists so the bound is ALWAYS read from this declared policy value,
    /// never inlined ad hoc inside the resolver.
    fn as_duration(&self) -> Duration {
        Duration::from_millis(self.fetch_deadline_ms)
    }
}

/// Ties a spawned fetch task's lifetime to the calling future's own: on
/// every exit from `resolve_blob_ref`'s deadline wrap — the declared
/// timeout firing, the join completing normally, or `resolve_blob_ref`'s
/// OWN future being dropped by a caller that lost interest (its own outer
/// `select!`/timeout) — the spawned task is aborted rather than left
/// running unbounded in the background. `abort()` on an already-finished
/// task is a harmless no-op, so the normal-completion path pays nothing
/// extra for this.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> AbortOnDrop<T> {
    async fn join(&mut self) -> std::result::Result<T, tokio::task::JoinError> {
        (&mut self.0).await
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A node's media data-plane host: the holder side (ingest, serve, reclaim)
/// and the consumer side (resolve) of the blob_ref reference kind.
pub struct BlobStore {
    db: Arc<Database>,
    policy: MovementPolicy,
    identity_path: PathBuf,
    /// Lazily opened content-addressed store (under the identity's
    /// directory); an open failure surfaces typed at the first use.
    store: OnceLock<std::result::Result<Arc<BlobStoreHandle>, String>>,
    /// Per-instance resolve-time clock: injected by tests, else the fabric
    /// Wallclock. Per-INSTANCE deliberately — concurrent services carry
    /// independent clocks.
    test_clock: Arc<Mutex<Option<i64>>>,
    /// Optional holder-side ledger-refresh hook; see [`ClaimRefreshHook`].
    /// `None` until [`Self::set_claim_refresh`] is called.
    claim_refresh: Arc<Mutex<Option<ClaimRefreshHook>>>,
    /// Per-peer transfer counters for the blob plane, both directions. In
    /// memory only.
    receipts: Arc<TransferLedger>,
    /// The declared whole-fetch-attempt bound; see [`BlobFetchPolicy`].
    /// Read by `resolve_blob_ref` at the top of each call to size its
    /// deadline wrap.
    fetch_policy: Mutex<BlobFetchPolicy>,
    /// Test-only wedge, armed at most once per use: when set, the NEXT
    /// `resolve_blob_ref` call on THIS instance that would otherwise dial
    /// the holder instead blocks forever (an ordinary,
    /// cooperatively-cancellable pending future — never a synchronous
    /// OS-level block) right at the point the fetch attempt begins.
    /// Standing in for a stuck internal await (e.g. the local store's tag
    /// bookkeeping never returning) without depending on the fetch backend's
    /// own internal failure modes: it proves whether `resolve_blob_ref` itself
    /// carries a bound on the fetch attempt, independent of what actually
    /// causes a real-world stall. Per-INSTANCE deliberately, like
    /// `test_clock` above — a process-global flag would race across the
    /// concurrently-running tests in one test binary. Off (the default)
    /// changes nothing for every existing caller. The arming method below is
    /// compiled only under `cfg(test)` / the `test-seams` feature, so no
    /// ordinary consumer of this crate can reach it — production is never
    /// armable.
    wedge_next_fetch_for_test: AtomicBool,
}

#[cfg(any(test, feature = "test-seams"))]
pub struct PausedProviderGenerationForTest {
    repository: Arc<crate::blob_repository::BlobRepository>,
    hash: [u8; 32],
    generation: u64,
    session: Mutex<Option<crate::blob_repository::BlobReadSession>>,
    payload_bytes_emitted: std::sync::atomic::AtomicU64,
}

#[cfg(any(test, feature = "test-seams"))]
impl PausedProviderGenerationForTest {
    pub fn wait_until_shared_hash_guarded(&self, _timeout: Duration) -> bool {
        self.session
            .lock()
            .expect("provider read session")
            .is_some()
    }

    pub fn abort_retaining_stale_generation_for_test(&self) {
        self.session.lock().expect("provider read session").take();
    }

    pub fn payload_bytes_emitted_for_test(&self) -> u64 {
        self.payload_bytes_emitted.load(Ordering::SeqCst)
    }

    pub fn resume_from_stale_generation_for_test(&self) -> Result<()> {
        let session = self.repository.open_read(self.hash)?.ok_or_else(|| {
            Error::Other("blob provider generation was retired by authoritative purge".to_string())
        })?;
        if session.generation() != self.generation {
            return Err(Error::Other("blob provider generation changed".to_string()));
        }
        let bytes = session.read_payload_at(0, 1)?;
        self.payload_bytes_emitted
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(any(test, feature = "test-seams"))]
pub struct PausedWriterGenerationForTest {
    repository: Arc<crate::blob_repository::BlobRepository>,
    hash: [u8; 32],
    generation: u64,
    guard: Mutex<Option<crate::blob_repository::BlobSharedGenerationGuard>>,
    object_id: [u8; 16],
    total_size: u64,
    payload_chunk_count: u64,
}

#[cfg(any(test, feature = "test-seams"))]
impl PausedWriterGenerationForTest {
    pub fn wait_until_shared_hash_guarded(&self, _timeout: Duration) -> bool {
        self.guard
            .lock()
            .expect("writer generation guard")
            .is_some()
    }

    pub fn abort_retaining_stale_generation_for_test(&self) {
        self.guard.lock().expect("writer generation guard").take();
    }

    pub fn complete_from_stale_generation_for_test(&self) -> Result<()> {
        let guard = self.repository.begin_write_generation(self.hash)?;
        if guard.generation() != self.generation {
            return Err(Error::Other(format!(
                "blob writer generation {} was retired by authoritative purge; current generation is {}",
                self.generation,
                guard.generation()
            )));
        }
        let partial = crate::blob_repository::BlobPartialState::from_exact_indices(
            0..self.payload_chunk_count,
            std::iter::empty(),
            Vec::new(),
        )?;
        self.repository.bind_staging(
            &guard,
            crate::blob_repository::BlobStagingBind {
                hash: self.hash,
                object_id: self.object_id,
                format: 0,
                total_size: self.total_size,
                outboard_size: 0,
                validated_size: self.total_size,
                state: crate::blob_repository::BlobManifestState::Partial,
                partial_state: Some(&partial),
                tags: &[],
                replace_active: true,
            },
        )?;
        Ok(())
    }
}

/// Why a resolve failed. Seven outcomes the caller must tell apart: five
/// pre-transfer refusals/misses, a mid-stream abort, and a local sink
/// failure.
#[derive(Debug)]
pub enum ResolveError {
    /// The holder found no ledger claim entitling the caller to this blob.
    Unentitled,
    /// The holder's movement policy forbids this input from leaving.
    PolicyForbidden,
    /// The fetched bytes did not hash to the reference. `got` is the digest
    /// of what was committed as verified before the mismatch — never the
    /// unverified payload, which is discarded unseen.
    HashMismatch { expected: BlobHash, got: BlobHash },
    /// The holder could not be dialed (down / never served).
    HolderUnreachable,
    /// The consumer's OWN local content-addressed store could not be opened
    /// (e.g. its `blobs.db` is held exclusively by another store handle in
    /// this process, or a slow/failed disk). Bounded and typed rather than an
    /// un-cancellable hang; the transfer never started.
    LocalStoreUnavailable,
    /// The holder authorized the fetch but does not hold this blob.
    BlobNotFound,
    /// The connection dropped mid-transfer; no verified blob was produced.
    TransferAborted,
    /// The local materialization side failed to accept the bytes (e.g.
    /// ENOSPC); the transfer never completed and no verified blob was
    /// produced.
    SinkWrite(std::io::Error),
    /// The fetch attempt did not complete within the declared
    /// [`BlobFetchPolicy::fetch_deadline_ms`] bound. Distinct from
    /// [`Self::HolderUnreachable`] (a definitive dial failure) and
    /// [`Self::TransferAborted`] (a definitive mid-stream drop): this is
    /// "still not known," bounded so the caller is never left waiting
    /// un-cancellably.
    FetchTimedOut { after_ms: u64 },
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::Unentitled => {
                write!(f, "the holder's ledger does not entitle this node")
            }
            ResolveError::PolicyForbidden => {
                write!(
                    f,
                    "the holder's movement policy forbids this input from leaving"
                )
            }
            ResolveError::HashMismatch { expected, .. } => {
                write!(f, "received bytes did not verify against {expected}")
            }
            ResolveError::HolderUnreachable => write!(f, "the holder could not be dialed"),
            ResolveError::LocalStoreUnavailable => {
                write!(f, "the local content-addressed store could not be opened")
            }
            ResolveError::BlobNotFound => write!(f, "the holder does not hold this blob"),
            ResolveError::TransferAborted => write!(f, "the transfer aborted mid-stream"),
            ResolveError::SinkWrite(err) => write!(f, "local sink write failed: {err}"),
            ResolveError::FetchTimedOut { after_ms } => write!(
                f,
                "the fetch attempt did not complete within the declared {after_ms}ms bound"
            ),
        }
    }
}

impl std::error::Error for ResolveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ResolveError::SinkWrite(err) => Some(err),
            _ => None,
        }
    }
}

impl BlobStore {
    pub fn new(db: Arc<Database>, policy: MovementPolicy, identity_path: PathBuf) -> Self {
        Self {
            db,
            policy,
            identity_path,
            store: OnceLock::new(),
            test_clock: Arc::new(Mutex::new(None)),
            claim_refresh: Arc::new(Mutex::new(None)),
            receipts: Arc::new(TransferLedger::new()),
            fetch_policy: Mutex::new(BlobFetchPolicy::default()),
            wedge_next_fetch_for_test: AtomicBool::new(false),
        }
    }

    /// Declare the whole-fetch-attempt bound this instance's
    /// [`Self::resolve_blob_ref`] enforces; see [`BlobFetchPolicy`].
    pub fn set_fetch_policy(&self, policy: BlobFetchPolicy) {
        *self.fetch_policy.lock().expect("fetch policy lock") = policy;
    }

    /// The currently declared fetch policy.
    pub fn fetch_policy(&self) -> BlobFetchPolicy {
        *self.fetch_policy.lock().expect("fetch policy lock")
    }

    /// Test-only: arm the wedge for the next `resolve_blob_ref` call on THIS
    /// instance; see the `wedge_next_fetch_for_test` field doc. Compiled
    /// only under `cfg(test)` (this crate's own unit tests) or the
    /// `test-seams` feature (this crate's own integration test targets, via
    /// the self-dependency in `Cargo.toml`) — production builds and every
    /// downstream consumer never see this method at all, so a fetch can
    /// never be armed outside a test binary.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn arm_wedge_next_fetch_for_test(&self) {
        self.wedge_next_fetch_for_test.store(true, Ordering::SeqCst);
    }

    /// Install (or clear, with `None`) the holder-side ledger-refresh hook —
    /// see [`ClaimRefreshHook`]. Typically wired to this node's own
    /// `SyncClient::pull_default()` so [`Self::serve_on`]'s entitlement
    /// check can catch a claim that landed on the hub after this holder's
    /// local mirror was last refreshed, without polling. Unset (the
    /// default) preserves today's behavior exactly.
    pub fn set_claim_refresh(&self, hook: Option<ClaimRefreshHook>) {
        *self.claim_refresh.lock().expect("claim refresh lock") = hook;
    }

    fn store(&self) -> Result<Arc<BlobStoreHandle>> {
        self.store
            .get_or_init(|| {
                BlobStoreHandle::open(self.db.blob_repository())
                    .map(Arc::new)
                    .map_err(|e| e.to_string())
            })
            .clone()
            .map_err(Error::Other)
    }

    /// The resolve-time "now": the per-instance injected cell, else the
    /// fabric Wallclock — NEVER a 0/MAX default (a 0 default reads every
    /// lease as never-expiring: liveness-blind in production).
    fn now_ms(&self) -> i64 {
        self.test_clock
            .lock()
            .expect("clock lock")
            .unwrap_or_else(|| Wallclock::now().0 as i64)
    }

    /// Content-address `bytes` into the local store and make them servable;
    /// the returned name IS `BlobHash::of(bytes)`. Idempotent.
    pub fn ingest_bytes(&self, bytes: &[u8]) -> Result<BlobHash> {
        self.store()?.ingest_bytes(bytes).map(BlobHash::from_bytes)
    }

    /// Content-address the file at `path` (streamed, never buffered whole)
    /// and make it servable.
    pub fn ingest_file(&self, path: &Path) -> Result<BlobHash> {
        self.store()?.ingest_file(path).map(BlobHash::from_bytes)
    }

    /// Holder side: register the authorizing serving handler on a bound peer
    /// endpoint — the blob door, answering from the ledger against the
    /// authenticated caller's peer identity BEFORE any payload bytes move —
    /// and enroll this node's ticket in the fabric peer directory so
    /// consumers can discover it.
    pub fn serve_on(&self, endpoint: &IrohServer) {
        let Ok(store) = self.store() else {
            // A store that cannot open cannot serve. The local error
            // re-surfaces, typed, on this holder's next ingest/reclaim call;
            // remote consumers see the protocol as absent.
            return;
        };
        let db = self.db.clone();
        let policy = self.policy;
        let clock = self.test_clock.clone();
        let claim_refresh = self.claim_refresh.clone();
        let receipts = self.receipts.clone();
        // One completed serve, attributed to the peer that asked for it.
        let served: adapter::ServedObserver = Arc::new(move |node_id: &str, moved: u64| {
            receipts.record(
                Some(node_id),
                TransferPlane::Blob,
                TransferDirection::Sent,
                1,
                moved,
            );
        });
        // The ledger's verdict for one authenticated fetch: entitlement
        // first (the claim), then the movement-policy re-check — two
        // DISTINCT gates with two distinct refusals. A ledger read failure
        // fails CLOSED.
        let verdict: adapter::VerdictFn = Arc::new(move |node_id, hash| {
            let hash = BlobHash::from_bytes(*hash);
            let node_id = node_id.to_string();
            let db = db.clone();
            let clock = clock.clone();
            let claim_refresh = claim_refresh.clone();
            Box::pin(async move {
                let now = clock
                    .lock()
                    .expect("clock lock")
                    .unwrap_or_else(|| Wallclock::now().0 as i64);
                let mut entitled = matches!(
                    node_holds_claim_for_blob(&db, &node_id, &hash, now),
                    Ok(true)
                );
                if !entitled {
                    // A miss may just mean this holder's own ledger mirror
                    // hasn't yet caught the claimant's claim landing on the
                    // hub. Give the installed refresh hook ONE chance to
                    // catch it up, then re-check ONCE more — never polled,
                    // never retried beyond this single extra look.
                    let hook = claim_refresh.lock().expect("claim refresh lock").clone();
                    if let Some(hook) = hook {
                        hook().await;
                        entitled = matches!(
                            node_holds_claim_for_blob(&db, &node_id, &hash, now),
                            Ok(true)
                        );
                    }
                }
                if !entitled {
                    return FetchVerdict::Unentitled;
                }
                // Movement-policy re-check: the submitter always reads its
                // own input; a non-submitter needs propagation on AND every
                // input in the job in the movable allowlist — a mixed
                // blob_ref/local_path job never auto-propagates, even to an
                // otherwise entitled claimant.
                match node_claim_permits_movement(&db, &node_id, &hash, policy, now) {
                    Ok(true) => FetchVerdict::Allow,
                    Ok(false) | Err(_) => FetchVerdict::PolicyForbidden,
                }
            })
        });
        store.serve_on(endpoint, verdict, served);
        // v1 holder discovery: the serving node registers its OWN ticket;
        // the row syncs through the hub like any coordination row.
        if let Ok(identity) = crate::FabricIdentity::load_or_generate(&self.identity_path) {
            let _ = crate::peer_directory::install_peer_directory_schema(&self.db);
            let _ = crate::peer_directory::register_peer_ticket(
                &self.db,
                &identity.node_id(),
                &endpoint.ticket(),
                self.now_ms(),
            );
        }
    }

    /// Test-only convenience: binds a real local peer endpoint for
    /// `identity_path` and serves this BlobStore on it in one step (the
    /// same `bind` + `serve_on` pair every two-node blob test performs), so
    /// an integration test file never has to name the network adapter or
    /// spell out its config-surface spec string itself — that stays
    /// confined to this already-sanctioned adapter/config surface (Rule 2 /
    /// the adapter-purity guard). Returns the same endpoint handle
    /// `serve_on` takes; callers only need its existing public methods
    /// (`ticket()`, `close()`, ...).
    #[cfg(any(test, feature = "test-seams"))]
    pub async fn bind_and_serve_for_test(&self, identity_path: &Path) -> Result<IrohServer> {
        let spec = format!("iroh:?identity={}", identity_path.display());
        let endpoint = IrohServer::bind(&spec)
            .await
            .map_err(|err| Error::Other(err.to_string()))?;
        self.serve_on(&endpoint);
        Ok(endpoint)
    }

    /// Consumer side: materialize `hash`, hash-verified, and stream the
    /// VERIFIED bytes into `sink`; returns the bytes written. The sink never
    /// receives unverified bytes. If this service's OWN store already holds
    /// the COMPLETE blob (content-addressed correctness — e.g. this node is
    /// both the job's submitter and its own claimant), it is read straight
    /// from disk and `holder_ticket` is never dialed; otherwise this fetches
    /// `hash` from `holder_ticket` node-to-node exactly as before.
    ///
    /// # Entitlement precondition
    ///
    /// A caller may fetch a blob only while it holds a LIVE work-ledger
    /// claim — the lease is still ahead of `now` and that attempt has not
    /// failed — on a job whose inputs reference this blob's hash. The
    /// check is enforced HOLDER-SIDE, at serve time, against the
    /// authenticated peer identity (see [`Self::serve_on`]): the local
    /// pre-check this method runs first, against this consumer's own
    /// ledger mirror, is a dial-avoidance optimization only — it is
    /// identity-blind and can be lied to by a forged local row, so it is
    /// never the entitlement boundary itself. This precondition is checked
    /// UNCONDITIONALLY before the local-store short-circuit below — local
    /// presence never waives entitlement.
    ///
    /// # Errors
    ///
    /// - [`ResolveError::Unentitled`] — no ledger claim (local mirror or
    ///   the holder's own check) entitles this node to the blob.
    /// - [`ResolveError::PolicyForbidden`] — the holder's movement policy
    ///   forbids this input from leaving (e.g. a mixed `blob_ref`/
    ///   `local_path` job that never auto-propagates).
    /// - [`ResolveError::HashMismatch`] — the fetched bytes did not hash to
    ///   the reference; the unverified payload is discarded unseen.
    /// - [`ResolveError::HolderUnreachable`] — the holder could not be
    ///   dialed.
    /// - [`ResolveError::LocalStoreUnavailable`] — this consumer's own
    ///   content-addressed store could not be opened.
    /// - [`ResolveError::BlobNotFound`] — the holder authorized the fetch
    ///   but does not hold this blob.
    /// - [`ResolveError::TransferAborted`] — the connection dropped
    ///   mid-transfer; no verified blob was produced.
    /// - [`ResolveError::SinkWrite`] — the local materialization side
    ///   failed to accept the bytes (e.g. `ENOSPC`); no verified blob was
    ///   produced.
    /// - [`ResolveError::FetchTimedOut`] — the fetch did not complete
    ///   within the declared deadline; on a multi-thread runtime the call
    ///   returns within the timeout and the abandoned fetch is aborted at
    ///   its next yield point (see the runtime-shape note below for the
    ///   current-thread limitation).
    pub async fn resolve_blob_ref(
        &self,
        hash: &BlobHash,
        holder_ticket: &str,
        sink: &mut (dyn Write + Send),
    ) -> std::result::Result<u64, ResolveError> {
        // Client-side pre-check on the consumer's OWN local ledger mirror: a
        // pure optimization to skip a wasted dial when this node has never
        // even mirrored a live claim for this hash. It does NOT check
        // identity (that would require trusting the local mirror's
        // claimant field, which a forged local row could lie about); the
        // holder's own `node_holds_claim_for_blob` check against the
        // authenticated peer identity is the actual security boundary.
        match any_node_holds_claim_for_blob(&self.db, hash, self.now_ms()) {
            Ok(true) => {}
            Ok(false) | Err(_) => return Err(ResolveError::Unentitled),
        }
        let store = self
            .store()
            .map_err(|_| ResolveError::LocalStoreUnavailable)?;
        // Content-addressed correctness: entitlement is checked
        // UNCONDITIONALLY above and is never waived by local presence. Once
        // entitled, if this service's OWN store already holds the
        // COMPLETE, verified blob for `hash` — e.g. this node is both the
        // job's submitter and its own claimant, or it has already fetched
        // this exact content for another job — materialize it from disk
        // and never dial the holder for bytes already sitting locally. The
        // dial path below is unchanged for content this store does not hold.
        if adapter::local_complete_size(&store, &hash.as_bytes())
            .await
            .is_some()
        {
            let outcome = adapter::export_into_sink(&store, &hash.as_bytes(), sink, 0)
                .await
                .map_err(|failure| map_failure(failure, hash))?;
            adapter::release_complete_cache_ownership(&store, &hash.as_bytes())
                .await
                .map_err(|failure| map_failure(failure, hash))?;
            return Ok(outcome.bytes_written);
        }
        // The declared whole-fetch-attempt bound (see `BlobFetchPolicy`):
        // the network dial, the holder's tag bookkeeping, and the verified
        // transfer loop all run inside a SPAWNED task, and only the JOIN is
        // timed — never the future being awaited directly. On a MULTI-THREAD
        // tokio runtime this is a genuine bound regardless of the spawned
        // task's behavior: a non-yielding poll can only occupy the worker
        // thread it landed on, leaving the timer free to fire on another
        // thread. On a CURRENT-THREAD runtime this call does NOT return
        // within the bound if the spawned task never yields — there is only
        // one OS thread, so a non-cooperating poll starves that thread
        // entirely, including the timer that would otherwise fire this
        // deadline. The caller's own timeout is never what reclaims a
        // wedged fetch on either runtime shape.
        let policy = self.fetch_policy();
        let wedged = self.wedge_next_fetch_for_test.swap(false, Ordering::SeqCst);
        let fetch_store = store.clone();
        let fetch_identity = self.identity_path.clone();
        let fetch_ticket = holder_ticket.to_string();
        let fetch_hash = hash.as_bytes();
        let handle = tokio::spawn(async move {
            if wedged {
                // Harness seam: stand in for an unbounded internal wedge
                // (e.g. the holder's own tag bookkeeping never returning)
                // without depending on the fetch backend's own internal
                // failure modes.
                std::future::pending::<()>().await;
            }
            adapter::fetch_verified_into_local_store(
                fetch_store,
                fetch_identity,
                fetch_ticket,
                fetch_hash,
            )
            .await
        });
        let mut guard = AbortOnDrop(handle);
        let local_outcome = match tokio::time::timeout(policy.as_duration(), guard.join()).await {
            Ok(Ok(Ok(local_outcome))) => local_outcome,
            Ok(Ok(Err(failure))) => {
                if matches!(failure, adapter::FetchFailure::Aborted) {
                    // The local store already committed whatever it
                    // verified before the abort; export that verified
                    // partial prefix into the caller's sink so a retry can
                    // resume from it, then still report the abort.
                    let _ = adapter::export_verified_prefix_into_sink(
                        &store,
                        &hash.as_bytes(),
                        sink,
                        0,
                    )
                    .await;
                }
                return Err(map_failure(failure, hash));
            }
            Ok(Err(_join_error)) => return Err(ResolveError::TransferAborted),
            Err(_elapsed) => {
                // Timeout fired: abort the spawned task explicitly so it stops
                // at the next yield point and does not occupy its worker thread.
                // The guard will also abort on drop, but we abort here explicitly
                // to be clear about the intent.
                guard.0.abort();
                return Err(ResolveError::FetchTimedOut {
                    after_ms: policy.fetch_deadline_ms,
                });
            }
        };
        let outcome = adapter::export_into_sink(&store, &hash.as_bytes(), sink, 0)
            .await
            .map_err(|failure| map_failure(failure, hash))?;
        adapter::release_fetch_protection(&store, &hash.as_bytes())
            .await
            .map_err(|failure| map_failure(failure, hash))?;
        // One blob, its own bytes, against the holder the ticket actually
        // dialed. Framing and encryption overhead are not counted.
        self.receipts.record(
            (!local_outcome.holder_node_id.is_empty())
                .then_some(local_outcome.holder_node_id.as_str()),
            TransferPlane::Blob,
            TransferDirection::Received,
            1,
            outcome.bytes_written,
        );
        Ok(outcome.bytes_written)
    }

    /// Harness seam (precedent: the shipped `split_changeset_for_test`):
    /// make the holder SERVE `bytes` under `claimed` even though they do not
    /// content-address to it — the consumer's verification is what must
    /// catch the lie.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn serve_wrong_bytes_for_test(&self, claimed: &BlobHash, bytes: &[u8]) -> Result<()> {
        self.store()?.arm_tamper(&claimed.as_bytes(), bytes)
    }

    /// What this node has moved on the blob plane, per authenticated peer and
    /// direction, since the service was constructed. Monotonic, payload bytes
    /// only, in memory only.
    pub fn transfer_receipts(&self) -> Vec<TransferReceipt> {
        self.receipts.receipts()
    }

    /// Test seam: the exact PAYLOAD bytes (not wire/framing bytes) this
    /// holder has served across every successfully completed transfer.
    /// Zero when the local store never opened.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn payload_bytes_emitted_for_test(&self) -> u64 {
        self.store().map(|s| s.payload_bytes_emitted()).unwrap_or(0)
    }

    /// Test seam: count of blob-door requests whose `GetRequest` this holder
    /// read and authorized, INCLUDING refused ones — a reset still counts.
    /// Zero when the local store never opened.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn fetch_requests_received_for_test(&self) -> u64 {
        self.store()
            .map(|s| s.fetch_requests_received())
            .unwrap_or(0)
    }

    /// Harness seam: dial the holder's blob door via the STOCK fetch path,
    /// SKIPPING the consumer-side client pre-check (`node_holds_claim_for_blob`)
    /// that `resolve_blob_ref` runs first — proving the HOLDER itself refuses
    /// a caller that bypassed all client-side logic, not just the client
    /// convenience check.
    #[cfg(any(test, feature = "test-seams"))]
    pub async fn attempt_authorization_bypass_for_test(
        &self,
        hash: &BlobHash,
        holder_ticket: &str,
        sink: &mut (dyn Write + Send),
    ) -> std::result::Result<u64, ResolveError> {
        let store = self
            .store()
            .map_err(|_| ResolveError::LocalStoreUnavailable)?;
        let outcome = adapter::fetch_into_sink(
            &store,
            &self.identity_path,
            holder_ticket,
            &hash.as_bytes(),
            sink,
            None,
            None,
            0,
        )
        .await
        .map_err(|failure| map_failure(failure, hash))?;
        Ok(outcome.bytes_written)
    }

    /// Test seam: shorten the local content-store open fail-fast bound so a
    /// contention test need not pay the full production wait. Production is
    /// unaffected; the bounded-and-typed store-open guarantee is unchanged.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn set_store_open_timeout_ms_for_test(&self, ms: u64) {
        adapter::set_store_open_timeout_ms_for_test(ms);
    }

    /// Harness seam: how many blobs this node currently serves — lets a test
    /// observe the reclaim driver's effect without performing the reclaim.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn servable_count_for_test(&self) -> usize {
        self.store().map(|s| s.servable_count()).unwrap_or(0)
    }

    /// Read-only exact-hash snapshot over the production blob-store adapter.
    /// This exists only in ContextDB's own test builds and deliberately cannot
    /// mutate the store, inspect payload bytes, or stand in for a purge.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn exact_hash_state_for_test(&self) -> BTreeSet<[u8; 32]> {
        self.store()
            .map(|store| store.servable_hashes_for_test())
            .unwrap_or_default()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn pause_authorized_provider_stream_after_shared_hash_guard_for_test(
        &self,
        hash: &BlobHash,
    ) -> Result<PausedProviderGenerationForTest> {
        let repository = self.db.blob_repository();
        let session = repository
            .open_read(hash.as_bytes())?
            .ok_or_else(|| Error::Other("blob provider has no canonical object".to_string()))?;
        Ok(PausedProviderGenerationForTest {
            repository,
            hash: hash.as_bytes(),
            generation: session.generation(),
            session: Mutex::new(Some(session)),
            payload_bytes_emitted: std::sync::atomic::AtomicU64::new(0),
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn pause_partial_blob_writer_after_shared_hash_guard_for_test(
        &self,
        hash: &BlobHash,
        bytes: &[u8],
    ) -> Result<PausedWriterGenerationForTest> {
        let repository = self.db.blob_repository();
        let guard = repository.begin_write_generation(hash.as_bytes())?;
        let mut staging = repository.begin_import_staging_for_generation(
            &guard,
            Some(bytes.len() as u64),
            Vec::new(),
        )?;
        for (index, block) in
            crate::blob_repository::BlobRepository::logical_blocks(bytes).enumerate()
        {
            staging = repository.append_staging_payload(staging.object_id, index as u64, block)?;
        }
        Ok(PausedWriterGenerationForTest {
            repository,
            hash: hash.as_bytes(),
            generation: guard.generation(),
            guard: Mutex::new(Some(guard)),
            object_id: staging.object_id,
            total_size: bytes.len() as u64,
            payload_chunk_count: staging.payload_chunk_count,
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn authoritative_engine_blob_roles_for_test(&self, hash: &BlobHash) -> BTreeSet<String> {
        self.store()
            .map(|store| store.authoritative_roles_for_test(&hash.as_bytes()))
            .unwrap_or_default()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn bytes_hydrated_while_opening_for_test(&self) -> u64 {
        self.store()
            .map(|store| store.bytes_hydrated_while_opening_for_test())
            .expect("open ContextDB blob store for hydration measurement")
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn verified_partial_bytes_for_test(&self, hash: &BlobHash) -> u64 {
        self.store()
            .map(|store| store.verified_partial_bytes_for_test(&hash.as_bytes()))
            .unwrap_or(0)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn bounded_raw_blob_bytes_for_test(
        &self,
        hash: &BlobHash,
        max_bytes: usize,
    ) -> Option<Vec<u8>> {
        self.store()
            .ok()
            .and_then(|store| store.bounded_raw_blob_bytes_for_test(&hash.as_bytes(), max_bytes))
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn durable_next_missing_range_for_test(
        &self,
        hash: &BlobHash,
    ) -> Option<std::ops::Range<u64>> {
        self.store()
            .ok()
            .and_then(|store| store.durable_next_missing_range_for_test(&hash.as_bytes()))
    }

    /// Harness seam: how many consumer-side fetch-protection tags remain — a
    /// fully-delivered resolve must leave ZERO, proving no permanent per-fetch
    /// disk leak.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn fetch_tag_count_for_test(&self) -> usize {
        self.store().map(|s| s.fetch_tag_count()).unwrap_or(0)
    }

    /// Harness seam: the holder drops the next authorized, servable GET once
    /// after attempting `n` payload bytes. Later GETs run normally, allowing
    /// retry from whatever prefix the consumer durably verified.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn drop_after_bytes_for_test(&self, n: u64) {
        if let Ok(store) = self.store() {
            store.arm_drop_after(n);
        }
    }

    /// Test seam (precedent: `Wallclock::set_test_clock`): inject the
    /// resolve-time "now" this instance's holder-side liveness check reads.
    /// Per-instance interior-mutable cell; unset falls back to the fabric
    /// `Wallclock::now()`.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn set_test_clock(&self, now_ms: i64) {
        *self.test_clock.lock().expect("clock lock") = Some(now_ms);
    }

    /// Reclaim local blob copies no live job references any more: a blob
    /// whose EVERY referencing job is terminal past `grace_ms` (and any blob
    /// no job references at all) becomes unservable immediately — a later
    /// entitled resolve gets BlobNotFound — and its bytes free at the
    /// store's GC interval. Returns the count reclaimed.
    pub fn reclaim_unreferenced(&self, now_ms: i64, grace_ms: i64) -> Result<usize> {
        let db = self.db.clone();
        self.store()?.reclaim(|hash| {
            blob_ref_retention_eligible(&db, &BlobHash::from_bytes(*hash), now_ms, grace_ms)
        })
    }

    /// Spawn the owned reclaim cadence: every [`RECLAIM_INTERVAL`] this calls
    /// [`Self::reclaim_unreferenced`] at the real fabric wall-clock with
    /// `grace_ms`, so a holder's blob store cannot fill without bound — the
    /// production complement to the store's GC (which only frees bytes of
    /// blobs this driver has already made unservable). `reclaim_unreferenced`
    /// stays public for a host that prefers to drive reclaim opportunistically
    /// (e.g. after each import). The returned handle owns the loop; drop or
    /// abort it to stop. The unit suites drive reclaim explicitly and do not
    /// spawn this; hosts (the server embedder, the live smoke) do.
    pub fn spawn_reclaim_driver(self: Arc<Self>, grace_ms: i64) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(RECLAIM_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let now = Wallclock::now().0 as i64;
                if let Err(err) = self.reclaim_unreferenced(now, grace_ms) {
                    tracing::warn!("blob reclaim sweep failed: {err}");
                }
            }
        })
    }

    /// Harness seam: run the RETRY leg of a resume. FIRST verifies `sink`'s
    /// existing bytes against this consumer's own locally verified partial
    /// for `hash` (a diverging prefix fails closed with `HashMismatch`
    /// WITHOUT dialing); only then dials the holder, reauthorizes, and
    /// fetches + appends the still-missing verified tail into `sink`. The
    /// already-verified local prefix is never re-fetched. Returns the
    /// payload bytes that actually moved over the wire on this leg.
    #[cfg(any(test, feature = "test-seams"))]
    pub async fn resume_bytes_moved_for_test(
        &self,
        hash: &BlobHash,
        holder_ticket: &str,
        sink: &mut Vec<u8>,
    ) -> std::result::Result<u64, ResolveError> {
        let store = self
            .store()
            .map_err(|e| ResolveError::SinkWrite(std::io::Error::other(e.to_string())))?;
        adapter::resume_into_sink(
            &store,
            &self.identity_path,
            holder_ticket,
            &hash.as_bytes(),
            sink,
        )
        .await
        .map_err(|failure| map_failure(failure, hash))
    }
}

fn map_failure(failure: FetchFailure, expected: &BlobHash) -> ResolveError {
    match failure {
        FetchFailure::Unentitled => ResolveError::Unentitled,
        FetchFailure::PolicyForbidden => ResolveError::PolicyForbidden,
        FetchFailure::NotFound => ResolveError::BlobNotFound,
        FetchFailure::Unreachable => ResolveError::HolderUnreachable,
        FetchFailure::HashMismatch { got } => ResolveError::HashMismatch {
            expected: expected.clone(),
            got: BlobHash::from_bytes(got),
        },
        FetchFailure::Aborted => ResolveError::TransferAborted,
        FetchFailure::SinkWrite(err) => ResolveError::SinkWrite(err),
    }
}
