//! The media-plane fetch backend, contained here per Rule 2: this module is
//! the ONLY place iroh-blobs is named. It wraps the upstream content store
//! (BLAKE3-addressed, Bao-verified streaming, partial-state resume) and the
//! upstream provider path, composing both UNDER contextdb's ledger
//! authorization: the serving side evaluates the caller's verdict — supplied
//! by the resolver as an opaque callback — against the transport-
//! authenticated node id BEFORE any payload bytes are emitted. This module
//! never names an engine type (Rule 2's transport-purity companion): the
//! content hash crosses this boundary as a raw 32-byte digest; the resolver
//! converts to/from the engine's `BlobHash` at the call site.

use super::iroh::{IrohServer, PeerConnection, peer_connect};
use contextdb_core::{Error, Result};
use iroh::endpoint::VarInt;
use iroh_blobs::api::blobs::BlobStatus;
use iroh_blobs::api::remote::GetProgressItem;
use iroh_blobs::protocol::{ChunkRangesSeq, GetRequest, Request};
use iroh_blobs::provider::events::EventSender;
use iroh_blobs::provider::{StreamPair, handle_get};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::util::{RecvStream, SendStream};
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Interval at which the upstream store's garbage collector frees the bytes
/// of blobs whose protection tag `reclaim` removed. Reclaim's CONTRACT
/// (unservable + BlobNotFound) is synchronous; the disk bytes free at this
/// cadence.
const GC_INTERVAL: Duration = Duration::from_secs(30);

/// Payload chunk size for exporting a verified blob into the caller's sink.
const EXPORT_CHUNK: usize = 1024 * 1024;

/// Abort a fetch when no progress arrives for this long (a holder that keeps
/// the connection open but stops sending must not hang the worker; the dial
/// timeout bounds only the dial).
const FETCH_IDLE: Duration = Duration::from_secs(20);

/// Fail-fast bound on opening the local content-addressed store. Opening a
/// fresh or existing store is a sub-second local operation; a slow disk gives
/// this generous headroom. The bound exists to convert a PATHOLOGICAL wedge
/// — the underlying redb file is already held exclusively by another store
/// handle in this process (e.g. two `BlobService` instances whose identity
/// files share a parent directory collide on the same `<dir>/blob-store`) —
/// into a typed, bounded error instead of an un-cancellable hang. The store
/// open runs on a dedicated thread and is awaited synchronously by `open`, so
/// an unbounded wait there blocks the calling worker in a way NO caller-side
/// `tokio::time::timeout` can reclaim. `load_with_opts` yields while it waits
/// on the lock, so this bound cancels it cleanly (no leaked thread).
const DEFAULT_STORE_OPEN_TIMEOUT_MS: u64 = 10_000;

/// The live store-open bound in milliseconds. A constant in production; a
/// test seam (`set_store_open_timeout_ms_for_test`) lowers it so the
/// shared-store-root contention test does not pay the full production wait.
static STORE_OPEN_TIMEOUT_MS: AtomicU64 = AtomicU64::new(DEFAULT_STORE_OPEN_TIMEOUT_MS);

/// Test seam: shorten the store-open fail-fast bound. Never called in
/// production, so the production guarantee (a bounded, typed store-open
/// outcome) is unchanged; this only trims the wait a test observes.
pub(crate) fn set_store_open_timeout_ms_for_test(ms: u64) {
    STORE_OPEN_TIMEOUT_MS.store(ms, Ordering::Relaxed);
}

/// How many blob serves one holder runs concurrently; excess claimants queue.
const SERVE_CONCURRENCY: usize = 8;

/// A holder-side serve stream must present its request within this window; a
/// peer that opens a stream and stalls is dropped so it cannot hold a
/// concurrency permit.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(15);

/// The ledger's answer for one authenticated fetch request, produced by the
/// resolver (the adapter never reads the ledger itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchVerdict {
    Allow,
    Unentitled,
    PolicyForbidden,
    /// Reserved for a future ledger-derived "does not exist" verdict; today
    /// non-existence is decided independently of the ledger callback (the
    /// blob door's own servable-set check), so this arm is never produced,
    /// only matched (defense in depth if a caller starts producing it).
    #[allow(dead_code)]
    NotFound,
}

/// Why a fetch failed, in adapter-neutral terms the resolver maps onto its
/// public error contract.
#[derive(Debug)]
pub(crate) enum FetchFailure {
    Unentitled,
    PolicyForbidden,
    NotFound,
    Unreachable,
    /// Received bytes did not verify against the requested hash; `got` is the
    /// digest of the raw payload received before verification failed.
    HashMismatch {
        got: [u8; 32],
    },
    /// The transfer started and did not complete (drop, reset, stall).
    Aborted,
    /// Writing the verified bytes into the caller's sink failed.
    SinkWrite(std::io::Error),
}

/// The verdict callback the resolver installs: authenticated caller node id
/// plus requested hash (opaque 32-byte digest), evaluated at serve time.
/// Async so the resolver's entitlement check may run a bounded, at-most-once
/// ledger-refresh hook on an initial miss before returning a final verdict
/// (see [`crate::blob_resolver::BlobService::set_claim_refresh`]).
/// Called after a served fetch completes, with the authenticated peer that
/// asked and the PAYLOAD bytes it received. The holder's transfer receipts are
/// built from this — one call per completed serve, per peer.
pub(crate) type ServedObserver = Arc<dyn Fn(&str, u64) + Send + Sync>;

pub(crate) type VerdictFn = Arc<
    dyn Fn(
            &str,
            &[u8; 32],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = FetchVerdict> + Send>>
        + Send
        + Sync,
>;

/// Stream-reset codes for the blob-ALPN serving side (defense for callers
/// that skip the preflight). The honest consumer never parses these; it
/// learns refusals from the preflight.
const RESET_UNENTITLED: u32 = 7001;
const RESET_POLICY: u32 = 7002;
const RESET_NOT_FOUND: u32 = 7003;
/// The drop-after-N harness seam resets with this code mid-payload.
const RESET_DROPPED: u32 = 7100;

struct StoreRuntime {
    /// Dedicated runtime owning the store actor and GC task, so the SYNC
    /// ingest surface works from inside any caller runtime (including a
    /// current-thread test runtime) without nested block_on.
    rt: Option<tokio::runtime::Runtime>,
    store: FsStore,
}

impl Drop for StoreRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            // A plain Runtime drop panics inside an async context; background
            // shutdown is legal everywhere.
            rt.shutdown_background();
        }
    }
}

/// The holder/consumer-side blob store plus the serve seams. One per
/// BlobService.
pub(crate) struct BlobStoreHandle {
    inner: Arc<StoreRuntime>,
    /// Retained for diagnostics/future use (e.g. operator-facing store-path
    /// reporting); the open path itself is captured in `inner`.
    #[allow(dead_code)]
    root: PathBuf,
    /// Hashes this node serves (mirrors the protection tags; rebuilt from
    /// tags at open so reclaim survives restart).
    servable: Arc<Mutex<HashSet<[u8; 32]>>>,
    /// Harness seam: requested hash -> substitute content hash to serve.
    tamper: Arc<Mutex<HashMap<[u8; 32], Hash>>>,
    /// Harness seam: reset the payload stream after N bytes.
    drop_after: Arc<Mutex<Option<u64>>>,
    serve_permits: Arc<tokio::sync::Semaphore>,
    /// Holder-side: count of blob-door requests whose `GetRequest` was read
    /// and authorized, INCLUDING refused ones (a reset still counts).
    fetch_requests_received: Arc<AtomicU64>,
    /// Holder-side: the exact PAYLOAD bytes (not wire/framing bytes) this
    /// holder has served across every successful `handle_get`.
    payload_bytes_emitted: Arc<AtomicU64>,
}

fn other(context: &str, err: impl std::fmt::Display) -> Error {
    Error::Other(format!("media adapter: {context}: {err}"))
}

fn to_backend_hash(hash: &[u8; 32]) -> Hash {
    Hash::from(*hash)
}

fn from_backend_hash(hash: &Hash) -> [u8; 32] {
    *hash.as_bytes()
}

/// Decode a lowercase 64-char hex digest back to raw bytes; `None` on any
/// malformed input (used only to rebuild the servable set from on-disk tag
/// names at open).
fn parse_hex32(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = (hi * 16 + lo) as u8;
    }
    Some(out)
}

impl BlobStoreHandle {
    /// Open (or create) the content-addressed store under `root`, with GC
    /// configured so untagged blobs free at [`GC_INTERVAL`].
    pub(crate) fn open(root: &Path) -> Result<BlobStoreHandle> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("contextdb-blob-store")
            .build()
            .map_err(|e| other("store runtime", e))?;
        let root_buf = root.to_path_buf();
        let joined = {
            let root = root_buf.clone();
            let handle = rt.handle().clone();
            std::thread::spawn(move || {
                handle.block_on(async move {
                    std::fs::create_dir_all(&root).map_err(|e| other("create store dir", e))?;
                    let mut options = iroh_blobs::store::fs::options::Options::new(&root);
                    options.gc = Some(iroh_blobs::store::GcConfig {
                        interval: GC_INTERVAL,
                        add_protected: None,
                    });
                    let db_path = root.join("blobs.db");
                    // Bound the open: `open` awaits this thread synchronously,
                    // so an unbounded wait here (the redb file already held
                    // exclusively elsewhere in-process) becomes an
                    // un-cancellable hang. `load_with_opts` yields while it
                    // waits on the lock, so this timeout cancels it cleanly.
                    let bound =
                        Duration::from_millis(STORE_OPEN_TIMEOUT_MS.load(Ordering::Relaxed));
                    match tokio::time::timeout(bound, FsStore::load_with_opts(db_path, options))
                        .await
                    {
                        Ok(result) => result.map_err(|e| other("open store", e)),
                        Err(_) => Err(other(
                            "open store",
                            format!(
                                "timed out after {}ms opening {:?}; the content store may be \
                                 held by another handle (identity files sharing a parent \
                                 directory collide on one blob-store)",
                                bound.as_millis(),
                                root.join("blobs.db"),
                            ),
                        )),
                    }
                })
            })
            .join()
        };
        // On ANY open failure, shut the store runtime down in the BACKGROUND
        // rather than letting this local `rt` drop: a plain multi-thread
        // Runtime drop blocks, and `open` can run inside a caller's async
        // context (a resolve driven by `block_on`), where a blocking drop
        // panics. The success path hands `rt` to `StoreRuntime`, whose own
        // Drop backgrounds it for the same reason.
        let store = match joined {
            Ok(Ok(store)) => store,
            Ok(Err(open_err)) => {
                rt.shutdown_background();
                return Err(open_err);
            }
            Err(_) => {
                rt.shutdown_background();
                return Err(Error::Other(
                    "media adapter: store open thread panicked".into(),
                ));
            }
        };
        let handle = BlobStoreHandle {
            inner: Arc::new(StoreRuntime {
                rt: Some(rt),
                store,
            }),
            root: root_buf,
            servable: Arc::new(Mutex::new(HashSet::new())),
            tamper: Arc::new(Mutex::new(HashMap::new())),
            drop_after: Arc::new(Mutex::new(None)),
            serve_permits: Arc::new(tokio::sync::Semaphore::new(SERVE_CONCURRENCY)),
            fetch_requests_received: Arc::new(AtomicU64::new(0)),
            payload_bytes_emitted: Arc::new(AtomicU64::new(0)),
        };
        let names = handle.block_on_store(|store| async move {
            let mut names = Vec::new();
            let mut tags = store
                .tags()
                .list()
                .await
                .map_err(|e| other("list tags", e))?;
            use futures_util::StreamExt;
            while let Some(tag) = tags.next().await {
                let tag = tag.map_err(|e| other("read tag", e))?;
                if let Some(hex) = tag_hex(tag.name.0.as_ref())
                    && let Some(bytes) = parse_hex32(&hex)
                {
                    names.push(bytes);
                }
            }
            Ok(names)
        })?;
        handle.servable.lock().expect("servable lock").extend(names);
        Ok(handle)
    }

    /// Run a store future to completion from a SYNC caller on the dedicated
    /// runtime (never the caller's), so this works under a current-thread
    /// caller runtime.
    fn block_on_store<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(FsStore) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let store = self.inner.store.clone();
        let handle = self
            .inner
            .rt
            .as_ref()
            .expect("store runtime lives as long as the handle")
            .handle()
            .clone();
        let (tx, rx) = std::sync::mpsc::channel();
        handle.spawn(async move {
            let _ = tx.send(f(store).await);
        });
        rx.recv()
            .map_err(|_| Error::Other("media adapter: store task dropped".into()))?
    }

    fn mark_servable(&self, hash: &[u8; 32]) {
        self.servable.lock().expect("servable lock").insert(*hash);
    }

    /// Retained as the single-source servable-set predicate (mirrors the
    /// inline check in `serve_stream`); a future caller (e.g. an operator
    /// `has()` surface) can reuse it instead of re-deriving the check.
    #[allow(dead_code)]
    fn is_servable(&self, hash: &Hash) -> bool {
        self.servable
            .lock()
            .expect("servable lock")
            .contains(hash.as_bytes())
    }

    /// Content-address `bytes` into the store, tag them servable, return the
    /// BLAKE3 name. Idempotent: identical bytes converge on one blob.
    pub(crate) fn ingest_bytes(&self, bytes: &[u8]) -> Result<[u8; 32]> {
        let data = bytes.to_vec();
        let hash = self.block_on_store(move |store| async move {
            let tag = store
                .add_bytes(data)
                .await
                .map_err(|e| other("add bytes", e))?;
            let hash = tag.hash;
            store
                .tags()
                .set(servable_tag(&hash), hash)
                .await
                .map_err(|e| other("tag blob", e))?;
            Ok(from_backend_hash(&hash))
        })?;
        self.mark_servable(&hash);
        Ok(hash)
    }

    /// Content-address the FILE at `path` (streamed by the store, never a
    /// whole-file buffer here), tag it servable, return the BLAKE3 name.
    pub(crate) fn ingest_file(&self, path: &Path) -> Result<[u8; 32]> {
        // The backing store requires an ABSOLUTE path to import; canonicalize
        // so a caller may pass a relative path, and a missing file yields a
        // clear error naming the path instead of an opaque store IO failure.
        let path = std::fs::canonicalize(path).map_err(|e| {
            Error::Other(format!(
                "media adapter: cannot read file to ingest ({}): {e}",
                path.display()
            ))
        })?;
        let hash = self.block_on_store(move |store| async move {
            let tag = store
                .add_path(path)
                .await
                .map_err(|e| other("add path", e))?;
            let hash = tag.hash;
            store
                .tags()
                .set(servable_tag(&hash), hash)
                .await
                .map_err(|e| other("tag blob", e))?;
            Ok(from_backend_hash(&hash))
        })?;
        self.mark_servable(&hash);
        Ok(hash)
    }

    /// Harness seam: serve `bytes` (their OWN valid Bao encoding) whenever
    /// `claimed` is requested — the consumer's verification against `claimed`
    /// then fails, which is the point.
    pub(crate) fn arm_tamper(&self, claimed: &[u8; 32], bytes: &[u8]) -> Result<()> {
        let data = bytes.to_vec();
        let substitute = self.block_on_store(move |store| async move {
            let tag = store
                .add_bytes(data)
                .await
                .map_err(|e| other("add tamper bytes", e))?;
            let hash = tag.hash;
            store
                .tags()
                .set(servable_tag(&hash), hash)
                .await
                .map_err(|e| other("tag tamper blob", e))?;
            Ok(hash)
        })?;
        self.tamper
            .lock()
            .expect("tamper lock")
            .insert(*claimed, substitute);
        // The claimed hash must read as servable even though the store
        // never received its real bytes — that is the whole point of the
        // harness (a holder that claims to hold `claimed` but serves
        // something else). Without this, `serve_stream`'s servable check
        // (keyed on the REQUESTED hash, before tamper substitution) would
        // refuse the stream as BlobNotFound before the substitute content
        // ever reaches the consumer's verification.
        self.mark_servable(claimed);
        Ok(())
    }

    /// Harness seam: reset the payload stream after `n` bytes on every
    /// subsequent serve.
    pub(crate) fn arm_drop_after(&self, n: u64) {
        *self.drop_after.lock().expect("drop lock") = Some(n);
    }

    /// Test seam: the exact PAYLOAD bytes this holder has served across
    /// every successfully completed `handle_get`. Never counts refused or
    /// aborted transfers.
    pub(crate) fn payload_bytes_emitted(&self) -> u64 {
        self.payload_bytes_emitted.load(Ordering::SeqCst)
    }

    /// Test seam: count of blob-door requests whose `GetRequest` was read
    /// and authorized, INCLUDING refused ones (a reset still counts).
    pub(crate) fn fetch_requests_received(&self) -> u64 {
        self.fetch_requests_received.load(Ordering::SeqCst)
    }

    /// Test seam: how many blobs this node currently serves (mirrors the
    /// protection tags). Lets a test observe reclaim without doing it.
    pub(crate) fn servable_count(&self) -> usize {
        self.servable.lock().expect("servable lock").len()
    }

    /// Test seam: how many consumer-side `fetch/` protection tags currently
    /// exist in the store. A fully-delivered fetch must leave ZERO (the leak
    /// this guards against); an aborted-not-yet-resumed fetch leaves one.
    pub(crate) fn fetch_tag_count(&self) -> usize {
        self.block_on_store(|store| async move {
            let mut count = 0usize;
            let mut tags = store
                .tags()
                .list()
                .await
                .map_err(|e| other("list tags", e))?;
            use futures_util::StreamExt;
            while let Some(tag) = tags.next().await {
                let tag = tag.map_err(|e| other("read tag", e))?;
                if fetch_tag_hex(tag.name.0.as_ref()).is_some() {
                    count += 1;
                }
            }
            Ok(count)
        })
        .unwrap_or(0)
    }

    /// Drop the protection tags of every blob `eligible` says may go, across
    /// BOTH retention roles: the `mt/` tags of locally-served (ingested)
    /// blobs, and the `fetch/` tags of blobs this node fetched as a consumer
    /// whose transfer aborted and was never resumed (a completed fetch
    /// releases its own tag — see [`fetch_into_sink`] — so only abandoned
    /// partials linger here). Returns how many were reclaimed; the bytes free
    /// at the GC interval, but the served contract (unservable, BlobNotFound
    /// on resolve) is immediate.
    pub(crate) fn reclaim<F>(&self, eligible: F) -> Result<usize>
    where
        F: Fn(&[u8; 32]) -> Result<bool>,
    {
        let mut reclaimed = 0usize;
        // Served blobs: tracked in the in-memory servable mirror.
        let served: Vec<[u8; 32]> = self
            .servable
            .lock()
            .expect("servable lock")
            .iter()
            .cloned()
            .collect();
        for hash in served {
            if !eligible(&hash)? {
                continue;
            }
            let backend = to_backend_hash(&hash);
            self.block_on_store(move |store| async move {
                store
                    .tags()
                    .delete(servable_tag(&backend))
                    .await
                    .map_err(|e| other("delete tag", e))?;
                Ok(())
            })?;
            self.servable.lock().expect("servable lock").remove(&hash);
            reclaimed += 1;
        }
        // Abandoned consumer partials: read the store's live `fetch/` tags
        // (they carry no in-memory mirror — a fetch is transient), and sweep
        // the ones whose referencing jobs are all terminal-past-grace. A fetch
        // whose job stays live for the transfer's duration is never eligible,
        // so the steady state cannot race a running transfer. The one narrow
        // window is a fetch whose OWN job is cancelled mid-transfer with grace
        // already elapsed: a concurrent sweep may drop its tag while bytes
        // still move. That degrades safely — verification still gates the
        // sink, so the transfer only surfaces as a typed Aborted/SinkWrite,
        // never wrong bytes (tracked follow-up: close this reclaim/fetch race).
        let fetched: Vec<[u8; 32]> = self.block_on_store(|store| async move {
            let mut out = Vec::new();
            let mut tags = store
                .tags()
                .list()
                .await
                .map_err(|e| other("list tags", e))?;
            use futures_util::StreamExt;
            while let Some(tag) = tags.next().await {
                let tag = tag.map_err(|e| other("read tag", e))?;
                if let Some(hex) = fetch_tag_hex(tag.name.0.as_ref())
                    && let Some(bytes) = parse_hex32(&hex)
                {
                    out.push(bytes);
                }
            }
            Ok(out)
        })?;
        for hash in fetched {
            if !eligible(&hash)? {
                continue;
            }
            let backend = to_backend_hash(&hash);
            self.block_on_store(move |store| async move {
                store
                    .tags()
                    .delete(fetch_protect_tag(&backend))
                    .await
                    .map_err(|e| other("delete fetch tag", e))?;
                Ok(())
            })?;
            reclaimed += 1;
        }
        Ok(reclaimed)
    }

    /// Release the consumer-side fetch-protection tag once a fetch has fully
    /// delivered its verified bytes to the caller's sink: the store copy is
    /// then a transient cache and must not outlive delivery, or every blob a
    /// node ever fetches would pin disk forever.
    async fn release_fetch_tag(&self, backend: Hash) {
        let _ = self
            .inner
            .store
            .tags()
            .delete(fetch_protect_tag(&backend))
            .await;
    }

    /// Register the serving side on `endpoint`: the blob protocol door,
    /// enforcing `verdict` against the transport-authenticated caller BEFORE
    /// any payload bytes move. There is no separate preflight door — an
    /// honest consumer dials the blob door directly, and a refusal arrives
    /// as a typed stream-reset code (see [`classify_get_error`]). A
    /// preflight would double-count requests against the two test counters
    /// below, since every dial is exactly one authorization decision.
    pub(crate) fn serve_on(
        &self,
        endpoint: &IrohServer,
        verdict: VerdictFn,
        served: ServedObserver,
    ) {
        let store = self.inner.store.clone();
        let servable = self.servable.clone();
        let tamper = self.tamper.clone();
        let drop_after = self.drop_after.clone();
        let permits = self.serve_permits.clone();
        let fetch_requests = self.fetch_requests_received.clone();
        let payload_bytes = self.payload_bytes_emitted.clone();
        endpoint.register_connection_protocol(
            iroh_blobs::protocol::ALPN.to_vec(),
            Arc::new(move |peer: PeerConnection| {
                let store = store.clone();
                let servable = servable.clone();
                let tamper = tamper.clone();
                let drop_after = drop_after.clone();
                let verdict = verdict.clone();
                let served = served.clone();
                let permits = permits.clone();
                let fetch_requests = fetch_requests.clone();
                let payload_bytes = payload_bytes.clone();
                Box::pin(async move {
                    let remote = peer.remote_node_id.clone();
                    loop {
                        let Ok((send, recv)) = peer.connection.accept_bi().await else {
                            break;
                        };
                        let _permit = permits.acquire().await;
                        let outcome = serve_stream(
                            &store,
                            &servable,
                            &tamper,
                            &drop_after,
                            &verdict,
                            &fetch_requests,
                            &payload_bytes,
                            &served,
                            &remote,
                            peer.connection.stable_id() as u64,
                            send,
                            recv,
                        )
                        .await;
                        if outcome.is_err() {
                            // The stream carried its refusal/reset; keep
                            // accepting further streams on this connection.
                            continue;
                        }
                    }
                    Ok(())
                })
            }),
        );
    }
}

/// The protection-tag name for a served blob.
fn servable_tag(hash: &Hash) -> String {
    format!("mt/{}", hash.to_hex())
}

/// The protection-tag name for a blob this node is FETCHING (consumer
/// side), distinct from `servable_tag` (this node never intends to SERVE
/// it, just to avoid the store's GC sweeping a mid-transfer or resumable
/// partial before the caller comes back for the tail — the exact hazard
/// `iroh_blobs::provider` warns a temp tag exists to prevent). Fetched
/// content is never marked servable via this tag alone (that requires the
/// separate in-memory `servable` set, populated only by `ingest_*`).
fn fetch_protect_tag(hash: &Hash) -> String {
    format!("fetch/{}", hash.to_hex())
}

/// The hex of a servable tag name, when it is one of ours.
fn tag_hex(name: &[u8]) -> Option<String> {
    let name = std::str::from_utf8(name).ok()?;
    name.strip_prefix("mt/").map(|hex| hex.to_string())
}

/// The hash of a consumer-side fetch-protection tag, when the name is one.
fn fetch_tag_hex(name: &[u8]) -> Option<String> {
    let name = std::str::from_utf8(name).ok()?;
    name.strip_prefix("fetch/").map(|hex| hex.to_string())
}

/// A SendStream wrapper that counts payload bytes and force-resets after the
/// armed threshold (the drop-after harness seam).
struct CountingSend<W> {
    inner: W,
    sent: u64,
    reset_after: Option<u64>,
}

impl<W: SendStream> SendStream for CountingSend<W> {
    async fn send_bytes(&mut self, bytes: bytes::Bytes) -> std::io::Result<()> {
        self.check(bytes.len() as u64)?;
        self.inner.send_bytes(bytes).await
    }

    async fn send(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.check(buf.len() as u64)?;
        self.inner.send(buf).await
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        self.inner.sync().await
    }

    fn reset(&mut self, code: VarInt) -> std::io::Result<()> {
        self.inner.reset(code)
    }

    async fn stopped(&mut self) -> std::io::Result<Option<VarInt>> {
        self.inner.stopped().await
    }

    fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl<W: SendStream> CountingSend<W> {
    fn check(&mut self, adding: u64) -> std::io::Result<()> {
        if let Some(limit) = self.reset_after
            && self.sent + adding > limit
        {
            let _ = self.inner.reset(RESET_DROPPED.into());
            return Err(std::io::Error::other("drop-after harness reset"));
        }
        self.sent += adding;
        Ok(())
    }
}

/// Authorize one accepted stream against the ledger verdict, then delegate
/// the byte-serving to the unchanged upstream provider.
#[allow(clippy::too_many_arguments)]
async fn serve_stream<W, R>(
    store: &FsStore,
    servable: &Arc<Mutex<HashSet<[u8; 32]>>>,
    tamper: &Arc<Mutex<HashMap<[u8; 32], Hash>>>,
    drop_after: &Arc<Mutex<Option<u64>>>,
    verdict: &VerdictFn,
    fetch_requests: &Arc<AtomicU64>,
    payload_bytes: &Arc<AtomicU64>,
    served: &ServedObserver,
    remote_node_id: &str,
    connection_id: u64,
    send: W,
    mut recv: R,
) -> std::result::Result<(), ()>
where
    W: SendStream,
    R: RecvStream,
{
    // Bound the read: a peer that opens a stream and then stalls without
    // sending a full request must not pin this serve task (and its
    // concurrency permit). The QUIC max_idle_timeout reaps the connection
    // eventually; this frees the permit sooner. The value is generous — a
    // legitimate request header arrives in milliseconds.
    let request =
        match tokio::time::timeout(REQUEST_READ_TIMEOUT, Request::read_async(&mut recv)).await {
            Ok(Ok((request, _len))) => request,
            Ok(Err(_)) | Err(_) => return Err(()),
        };
    let Request::Get(get) = request else {
        // The media plane serves single-blob gets only.
        let mut send = send;
        let _ = send.reset(RESET_NOT_FOUND.into());
        return Err(());
    };
    // Every dial that reaches this handler and presents a GET counts once,
    // authorized or refused — counted BEFORE the authorization decision.
    fetch_requests.fetch_add(1, Ordering::SeqCst);
    let requested = from_backend_hash(&get.hash);
    // The ledger verdict, keyed on the transport-authenticated caller. This
    // runs BEFORE any payload byte is read from the store or written to the
    // wire.
    let answer = verdict(remote_node_id, &requested).await;
    let mut send = send;
    match answer {
        FetchVerdict::Allow => {}
        FetchVerdict::Unentitled => {
            let _ = send.reset(RESET_UNENTITLED.into());
            return Err(());
        }
        FetchVerdict::PolicyForbidden => {
            let _ = send.reset(RESET_POLICY.into());
            return Err(());
        }
        FetchVerdict::NotFound => {
            let _ = send.reset(RESET_NOT_FOUND.into());
            return Err(());
        }
    }
    if !servable.lock().expect("servable lock").contains(&requested) {
        let _ = send.reset(RESET_NOT_FOUND.into());
        return Err(());
    }
    // Harness seams. Tamper serves the substitute content's (valid) encoding
    // under the requested stream — the consumer's verification against the
    // hash it asked for is what must catch it.
    let effective = tamper.lock().expect("tamper lock").get(&requested).copied();
    let get = match effective {
        Some(substitute) => GetRequest {
            hash: substitute,
            ranges: get.ranges,
        },
        None => get,
    };
    let served_hash = get.hash;
    let served_ranges = get.ranges.clone();
    let reset_after = *drop_after.lock().expect("drop lock");
    let send = CountingSend {
        inner: send,
        sent: 0,
        reset_after,
    };
    let pair = StreamPair::new(connection_id, recv, send, EventSender::DEFAULT);
    let deref_store = store.deref_store();
    let result = handle_get(pair, deref_store.clone(), get).await;
    if result.is_ok() {
        // Event-free payload counting: never read from the handler's own
        // internal write stats — derive the served byte length from the
        // REQUEST that just completed, against the served hash's known
        // size. A full GET serves the whole blob; a ranges (resume-tail)
        // GET serves from its lowest requested chunk boundary to the end.
        if let Ok(BlobStatus::Complete { size }) = deref_store.blobs().status(served_hash).await {
            let moved = served_bytes_for_request(&served_ranges, size);
            payload_bytes.fetch_add(moved, Ordering::SeqCst);
            // Same derived figure, now attributed to the authenticated peer
            // that asked for it — the per-peer receipt the aggregate counter
            // above cannot produce.
            served(remote_node_id, moved);
        }
    }
    Ok(())
}

/// The PAYLOAD byte length a completed `GetRequest` with these `ranges`
/// served out of a blob of `size` bytes. A full request (`ranges.is_all()`)
/// serves the whole blob; any other (single, contiguous-tail) request
/// serves from its lowest requested chunk boundary (1024 bytes/chunk) to
/// the end of the blob, clamped to the blob size.
fn served_bytes_for_request(ranges: &ChunkRangesSeq, size: u64) -> u64 {
    if ranges.is_all() {
        return size;
    }
    let Some((_, chunk_ranges)) = ranges.as_single() else {
        return 0;
    };
    match chunk_ranges.boundaries().first() {
        Some(first_missing_chunk) => size.saturating_sub(first_missing_chunk.to_bytes()),
        None => 0,
    }
}

/// FsStore derefs to the api Store handle; spelled out so the call site
/// stays readable.
trait DerefStore {
    fn deref_store(&self) -> iroh_blobs::api::Store;
}

impl DerefStore for FsStore {
    fn deref_store(&self) -> iroh_blobs::api::Store {
        use std::ops::Deref;
        self.deref().clone()
    }
}

/// The consumer half: preflight, fetch (verified, resumable), export.
pub(crate) struct FetchOutcome {
    pub bytes_written: u64,
}

/// Preflight + fetch + export `hash` from the holder at `ticket` into
/// `sink`. `payload_moved` (when given) receives the wire payload bytes this
/// call actually transferred (the resume-leg counter); `holder_node_id` (when
/// given) receives the holder's transport-authenticated identity, so a caller
/// can key a transfer receipt on who actually served it rather than on a name
/// it supplied itself.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_into_sink(
    store: &BlobStoreHandle,
    identity_path: &Path,
    ticket: &str,
    hash: &[u8; 32],
    sink: &mut (dyn Write + Send),
    payload_moved: Option<&mut u64>,
    holder_node_id: Option<&mut String>,
    export_offset: u64,
) -> std::result::Result<FetchOutcome, FetchFailure> {
    // 1. Fetch over the blob door, verified into the local store. There is
    // no separate preflight: the consumer dials the blob door directly, and
    // a refusal arrives as a typed stream-reset code the holder sends BEFORE
    // any payload byte (see `classify_get_error`). Upstream fetches only
    // what the local partial state is missing, so a retry after an abort
    // moves the tail, not the blob.
    let backend = to_backend_hash(hash);
    let peer = peer_connect(identity_path, ticket, iroh_blobs::protocol::ALPN)
        .await
        .map_err(|_| FetchFailure::Unreachable)?;
    if let Some(slot) = holder_node_id {
        *slot = peer.remote_node_id.clone();
    }
    let moved = run_verified_fetch(store, &peer, backend, hash).await;
    let moved = match moved {
        Ok(moved) => moved,
        Err(failure) => {
            peer.close().await;
            if matches!(failure, FetchFailure::Aborted) {
                // The local store already committed whatever it verified
                // before the abort (bao writes verified chunks
                // incrementally). Export that verified partial prefix into
                // the caller's sink so a retry can resume from it, then
                // still report the abort. Refusals (Unentitled/Policy/
                // NotFound) never reach here — they leave the sink empty,
                // since no fetch ever ran.
                let _ = export_into_sink(store, hash, sink, export_offset).await;
            }
            return Err(failure);
        }
    };
    peer.close().await;
    if let Some(counter) = payload_moved {
        *counter = moved;
    }
    // 2. Export the verified blob from the local store into the caller's
    // sink, streaming in bounded chunks.
    let result = export_into_sink(store, hash, sink, export_offset).await;
    if result.is_ok() {
        // Fully delivered: the caller now holds the verified bytes, so the
        // store copy is transient — release the fetch-protection tag so the
        // blob is GC-eligible like any other untagged content. On an abort
        // (handled above) the tag stays, so a later resume can reuse the
        // verified partial; an abandoned partial is swept by `reclaim`.
        store.release_fetch_tag(backend).await;
    }
    result
}

async fn run_verified_fetch(
    store: &BlobStoreHandle,
    peer: &PeerConnection,
    backend: Hash,
    hash: &[u8; 32],
) -> std::result::Result<u64, FetchFailure> {
    // Protect this hash from the store's GC sweep for as long as this node
    // might still be mid-transfer OR resuming later: an unprotected partial
    // (or even complete) fetched blob is exactly the hazard
    // `iroh_blobs::provider::Blobs::batch` warns a temp tag exists to
    // prevent — "without a temp tag, GC may collect the partially written
    // data file before the operation completes." A resume can legitimately
    // straddle more than one `GC_INTERVAL`, so a scoped batch/temp-tag
    // (released when this call returns) is not enough; this tag is a
    // permanent protection, mirroring how ingested/servable content is
    // tagged.
    let _ = store
        .inner
        .store
        .tags()
        .set(fetch_protect_tag(&backend), backend)
        .await;
    let remote = store.inner.store.remote();
    let content = HashAndFormat::new(backend, BlobFormat::Raw);
    let progress = remote.fetch(peer.connection.clone(), content);
    let mut progress = Box::pin(progress.stream());
    let mut last_payload: u64 = 0;
    loop {
        let next = tokio::time::timeout(FETCH_IDLE, futures_next(&mut progress)).await;
        let item = match next {
            Ok(Some(item)) => item,
            Ok(None) => break,
            Err(_) => return Err(FetchFailure::Aborted),
        };
        match item {
            GetProgressItem::Progress(payload) => last_payload = payload,
            GetProgressItem::Done(stats) => {
                return Ok(stats.payload_bytes_read.max(last_payload));
            }
            GetProgressItem::Error(err) => {
                return Err(classify_get_error(err, hash));
            }
        }
    }
    Err(FetchFailure::Aborted)
}

async fn futures_next<S>(progress: &mut std::pin::Pin<Box<S>>) -> Option<GetProgressItem>
where
    S: futures_util::Stream<Item = GetProgressItem> + ?Sized,
{
    use futures_util::StreamExt;
    progress.next().await
}

/// Map an upstream fetch error onto the adapter-neutral failure kinds, by
/// VARIANT: a Bao hash-verification failure is a HashMismatch (wrong bytes
/// were caught before any verified handoff); an adapter reset code carries
/// the serving side's refusal for a caller that skipped the preflight; every
/// other mid-transfer failure is an Aborted (retryable).
fn classify_get_error(err: iroh_blobs::get::GetError, expected: &[u8; 32]) -> FetchFailure {
    use iroh_blobs::get::GetError;
    if let Some(code) = err.iroh_error_code() {
        let code = u64::from(code);
        if code == u64::from(RESET_UNENTITLED) {
            return FetchFailure::Unentitled;
        }
        if code == u64::from(RESET_POLICY) {
            return FetchFailure::PolicyForbidden;
        }
        if code == u64::from(RESET_NOT_FOUND) {
            return FetchFailure::NotFound;
        }
        if code == u64::from(RESET_DROPPED) {
            return FetchFailure::Aborted;
        }
    }
    if let GetError::Decode { source, .. } = &err {
        use iroh_blobs::get::fsm::DecodeError;
        if matches!(
            source,
            DecodeError::ParentHashMismatch { .. } | DecodeError::LeafHashMismatch { .. }
        ) {
            // No unverified payload is ever committed, so the only honest
            // "got" digest is of what was verified and handed over: nothing.
            let _ = expected;
            return FetchFailure::HashMismatch {
                got: *blake3::hash(&[]).as_bytes(),
            };
        }
    }
    FetchFailure::Aborted
}

/// Whether `store` already holds the COMPLETE, verified blob for `hash` —
/// checked via iroh-blobs' own status query (never inferred from a partial
/// byte count, which a still-resuming fetch could satisfy misleadingly).
/// `Some(size)` only on `BlobStatus::Complete`; a partial, absent, or
/// unqueryable blob returns `None`.
pub(crate) async fn local_complete_size(store: &BlobStoreHandle, hash: &[u8; 32]) -> Option<u64> {
    let backend = to_backend_hash(hash);
    match store.inner.store.blobs().status(backend).await {
        Ok(BlobStatus::Complete { size }) => Some(size),
        _ => None,
    }
}

pub(crate) async fn export_into_sink(
    store: &BlobStoreHandle,
    hash: &[u8; 32],
    sink: &mut (dyn Write + Send),
    offset: u64,
) -> std::result::Result<FetchOutcome, FetchFailure> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let backend = to_backend_hash(hash);
    let mut reader = store.inner.store.blobs().reader(backend);
    if offset > 0 {
        reader
            .seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(FetchFailure::SinkWrite)?;
    }
    // Stream the verified local blob in bounded chunks — never a whole-blob
    // buffer (the bounded-memory invariant, both sides).
    let mut buf = vec![0u8; EXPORT_CHUNK];
    let mut written: u64 = 0;
    loop {
        let n = reader
            .read(&mut buf)
            .await
            .map_err(|_| FetchFailure::Aborted)?;
        if n == 0 {
            break;
        }
        sink.write_all(&buf[..n]).map_err(FetchFailure::SinkWrite)?;
        written += n as u64;
    }
    Ok(FetchOutcome {
        bytes_written: written,
    })
}

/// The size of the verified LOCAL state for `hash` (the resume prefix), in
/// bytes: what a retry does not need to move again.
pub(crate) async fn verified_local_bytes(store: &BlobStoreHandle, hash: &[u8; 32]) -> Result<u64> {
    let backend = to_backend_hash(hash);
    let remote = store.inner.store.remote();
    let request = GetRequest {
        hash: backend,
        ranges: ChunkRangesSeq::all(),
    };
    let info = remote
        .local_for_request(request)
        .await
        .map_err(|e| other("local_for_request", e))?;
    Ok(info.local_bytes())
}

/// Resume leg: VERIFY the caller's existing `sink` prefix against this
/// consumer's own locally verified partial for `hash` FIRST, before any
/// dial — a diverging prefix (the harness corrupts `sink` directly) must
/// never get laundered into a verified blob by a tail fetch. Only when the
/// local store holds no verified partial at all (a fresh consumer that
/// never ran a prior leg) is the prefix check skipped, since there is
/// nothing of this consumer's own to compare against; the holder's own
/// authorization is the real security boundary in that case regardless.
/// Once the prefix checks out, dial the holder, reauthorize (a rogue
/// caller is reset exactly like any other unauthorized request, sink
/// unchanged), and fetch + export ONLY the still-missing tail, APPENDED to
/// `sink`. Returns the wire payload bytes moved on this leg.
pub(crate) async fn resume_into_sink(
    store: &BlobStoreHandle,
    identity_path: &Path,
    ticket: &str,
    hash: &[u8; 32],
    sink: &mut Vec<u8>,
) -> std::result::Result<u64, FetchFailure> {
    let local_verified = verified_local_bytes(store, hash)
        .await
        .map_err(|_| FetchFailure::Aborted)?;
    if local_verified > 0 && !sink.is_empty() {
        let check_len = (sink.len() as u64).min(local_verified) as usize;
        if check_len > 0 {
            let stored_prefix = read_store_prefix(store, hash, check_len as u64).await;
            if stored_prefix.len() != check_len || stored_prefix.as_slice() != &sink[..check_len] {
                return Err(FetchFailure::HashMismatch {
                    got: *blake3::hash(sink.as_slice()).as_bytes(),
                });
            }
        }
    }
    let backend = to_backend_hash(hash);
    let peer = peer_connect(identity_path, ticket, iroh_blobs::protocol::ALPN)
        .await
        .map_err(|_| FetchFailure::Unreachable)?;
    let moved = run_verified_fetch(store, &peer, backend, hash).await;
    let moved = match moved {
        Ok(moved) => moved,
        Err(failure) => {
            peer.close().await;
            return Err(failure);
        }
    };
    peer.close().await;
    let export_offset = sink.len() as u64;
    export_into_sink(store, hash, sink, export_offset).await?;
    // The resume completed and the full verified blob is in the caller's
    // sink; release the fetch-protection tag so the store copy is reclaimable.
    store.release_fetch_tag(backend).await;
    Ok(moved)
}

/// Read up to `len` bytes (fewer, on short or absent local state) of the
/// LOCAL verified state for `hash`, from offset 0 — used only to compare
/// against a caller-supplied resume-sink prefix, never handed to a caller.
async fn read_store_prefix(store: &BlobStoreHandle, hash: &[u8; 32], len: u64) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let backend = to_backend_hash(hash);
    let mut reader = store.inner.store.blobs().reader(backend);
    let mut buf = vec![0u8; len as usize];
    let mut read_total = 0usize;
    while read_total < buf.len() {
        let n = match reader.read(&mut buf[read_total..]).await {
            Ok(n) => n,
            Err(_) => break,
        };
        if n == 0 {
            break;
        }
        read_total += n;
    }
    buf.truncate(read_total);
    buf
}
