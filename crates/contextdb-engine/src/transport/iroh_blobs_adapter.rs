//! The media-plane fetch backend, contained here by the transport adapter
//! purity guard: this module is the ONLY place iroh-blobs is named. It
//! wraps the upstream content store
//! (BLAKE3-addressed, Bao-verified streaming, partial-state resume) and the
//! upstream provider path, composing both UNDER contextdb's ledger
//! authorization: the serving side evaluates the caller's verdict — supplied
//! by the resolver as an opaque callback — against the transport-
//! authenticated node id BEFORE any payload bytes are emitted. This module
//! deliberately names only the engine-owned raw blob repository plus transport
//! types: persistence, generation fencing, partial-state resume, and PURGE all
//! share the same redb owner, while work-ledger and application media types stay
//! outside this adapter. The content hash crosses the resolver boundary as a
//! raw 32-byte digest and is converted to/from `BlobHash` at the call site.

use super::iroh::{IrohServer, PeerConnection, peer_connect};
use crate::blob_repository::{
    BLOB_LOGICAL_BLOCK_BYTES, BlobImportStaging, BlobManifestState, BlobPartialState,
    BlobRepository, BlobSharedGenerationGuard, BlobStagingBind, BlobTagRecord, BlobTagRole,
};
use bao_tree::io::mixed::{
    EncodedItem, ReadBytesAt, Sender as BaoSender, traverse_ranges_validated,
};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::{CreateOutboard, ReadAt, WriteAt};
use bao_tree::io::{BaoContentItem, EncodeError, Leaf};
use bao_tree::{BaoTree, ChunkNum, ChunkRanges};
use contextdb_core::{Error, Result};
use iroh::endpoint::VarInt;
use iroh_blobs::api::Store;
use iroh_blobs::api::blobs::BlobStatus;
use iroh_blobs::api::proto::*;
use iroh_blobs::api::remote::GetProgressItem;
use iroh_blobs::api::{self, TempTag};
use iroh_blobs::protocol::{ChunkRangesExt, ChunkRangesSeq, GetRequest, Request};
use iroh_blobs::provider::events::EventSender;
use iroh_blobs::provider::{StreamPair, handle_get};
use iroh_blobs::store::IROH_BLOCK_SIZE;
use iroh_blobs::util::{RecvStream, SendStream};
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};
use range_collections::range_set::RangeSetRange;
#[cfg(any(test, feature = "test-seams"))]
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Payload chunk size for exporting a verified blob into the caller's sink.
const EXPORT_CHUNK: usize = 1024 * 1024;
const COMPLETE_STAGING_STATE: &[u8] = b"contextdb-complete-import-v1";

/// Abort a fetch when no progress arrives for this long (a holder that keeps
/// the connection open but stops sending must not hang the worker; the dial
/// timeout bounds only the dial).
const FETCH_IDLE: Duration = Duration::from_secs(20);

/// Compatibility seam retained for older media tests. The ContextDB-backed
/// actor attaches to the already-open Database repository and therefore has
/// no second store-open wait to tune.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn set_store_open_timeout_ms_for_test(_ms: u64) {}

/// How many blob serves one holder runs concurrently; excess claimants queue.
const SERVE_CONCURRENCY: usize = 8;

/// A holder-side serve stream must present its request within this window; a
/// peer that opens a stream and stalls is dropped so it cannot hold a
/// concurrency permit.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(15);

/// The ledger's answer for one authenticated fetch request, produced by the
/// resolver (the adapter never reads the ledger itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FetchVerdict {
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
/// (see [`crate::blob_store::BlobStore::set_claim_refresh`]).
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

/// Adapts Iroh's public actor channel to Bao's public validated traversal.
/// Keeping this one-line wire bridge here avoids requiring a workspace-only
/// visibility patch in downstream ContextDB builds.
struct BaoTreeSender<'a>(&'a mut irpc::channel::mpsc::Sender<EncodedItem>);

impl BaoSender for BaoTreeSender<'_> {
    type Error = irpc::channel::SendError;

    async fn send(&mut self, item: EncodedItem) -> std::result::Result<(), Self::Error> {
        self.0.send(item).await
    }
}

struct StoreRuntime {
    /// Dedicated runtime owning the repository-backed store actor, so the SYNC
    /// ingest surface works from inside any caller runtime (including a
    /// current-thread test runtime) without nested block_on.
    rt: Option<tokio::runtime::Runtime>,
    store: Store,
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

fn api_error(message: impl Into<String>) -> api::Error {
    api::Error::io(std::io::ErrorKind::Other, message.into())
}

fn command_io_error(name: &str, err: impl std::fmt::Display) -> api::Error {
    api_error(format!("ContextDB store {name}: {err}"))
}

async fn run_contextdb_store_actor(
    repository: Arc<BlobRepository>,
    mut commands: tokio::sync::mpsc::Receiver<Command>,
) {
    let mut tasks = tokio::task::JoinSet::new();
    let mut idle_waiters = Vec::new();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    Command::ListBlobs(cmd) => {
                        tasks.spawn(contextdb_list_blobs(repository.clone(), cmd));
                    }
                    Command::Batch(BatchMsg { tx, mut rx, .. }) => {
                        tasks.spawn(async move {
                            let _ = tx.send(Scope::GLOBAL).await;
                            while rx.recv().await.ok().flatten().is_some() {}
                        });
                    }
                    Command::DeleteBlobs(DeleteBlobsMsg { inner, tx, .. }) => {
                        let repository = repository.clone();
                        tasks.spawn(async move {
                            let force = inner.force;
                            let hashes = inner.hashes;
                            let result = tokio::task::spawn_blocking(move || {
                                hashes.into_iter().try_for_each(|hash| {
                                    repository
                                        .delete_ordinary(from_backend_hash(&hash), force)
                                        .map(|_| ())
                                })
                            })
                            .await
                            .map_err(|err| command_io_error("DeleteBlobs", err))
                            .and_then(|result| {
                                result.map_err(|err| command_io_error("DeleteBlobs", err))
                            });
                            let _ = tx.send(result).await;
                        });
                    }
                    Command::ImportBao(cmd) => {
                        tasks.spawn(contextdb_import_bao(repository.clone(), cmd));
                    }
                    Command::ExportBao(cmd) => {
                        tasks.spawn(contextdb_export_bao(repository.clone(), cmd));
                    }
                    Command::ExportRanges(cmd) => {
                        tasks.spawn(contextdb_export_ranges(repository.clone(), cmd));
                    }
                    Command::Observe(cmd) => {
                        tasks.spawn(contextdb_observe(repository.clone(), cmd));
                    }
                    Command::BlobStatus(cmd) => {
                        tasks.spawn(contextdb_blob_status(repository.clone(), cmd));
                    }
                    Command::ImportBytes(cmd) => {
                        tasks.spawn(contextdb_import_bytes(repository.clone(), cmd));
                    }
                    Command::ImportByteStream(ImportByteStreamMsg { tx, .. }) => {
                        tasks.spawn(async move {
                            let _ = tx.send(AddProgressItem::Error(std::io::Error::new(
                                std::io::ErrorKind::Unsupported,
                                "ContextDB store does not expose an unbounded byte-stream import; use bounded bytes or path import",
                            ))).await;
                        });
                    }
                    Command::ImportPath(cmd) => {
                        tasks.spawn(contextdb_import_path(repository.clone(), cmd));
                    }
                    Command::ExportPath(cmd) => {
                        tasks.spawn(contextdb_export_path(repository.clone(), cmd));
                    }
                    Command::ListTags(ListTagsMsg { tx, .. }) => {
                        tasks.spawn(async move {
                            let _ = tx.send(vec![Err(api_error(
                                "unbounded ListTags is unsupported by the ContextDB store; adapter-owned callers use repository paging",
                            ))]).await;
                        });
                    }
                    Command::SetTag(cmd) => {
                        tasks.spawn(contextdb_set_tag(repository.clone(), cmd));
                    }
                    Command::DeleteTags(cmd) => {
                        tasks.spawn(contextdb_delete_tags(repository.clone(), cmd));
                    }
                    Command::RenameTag(cmd) => {
                        tasks.spawn(contextdb_rename_tag(repository.clone(), cmd));
                    }
                    Command::CreateTag(cmd) => {
                        tasks.spawn(contextdb_create_tag(repository.clone(), cmd));
                    }
                    Command::ListTempTags(cmd) => {
                        tasks.spawn(async move { let _ = cmd.tx.send(Vec::new()).await; });
                    }
                    Command::CreateTempTag(cmd) => {
                        tasks.spawn(async move {
                            let _ = cmd.tx.send(TempTag::new(cmd.inner.value, None)).await;
                        });
                    }
                    Command::SyncDb(cmd) => {
                        tasks.spawn(async move { let _ = cmd.tx.send(Ok(())).await; });
                    }
                    Command::WaitIdle(cmd) => {
                        if tasks.is_empty() {
                            let _ = cmd.tx.send(()).await;
                        } else {
                            idle_waiters.push(cmd.tx);
                        }
                    }
                    Command::Shutdown(cmd) => {
                        while tasks.join_next().await.is_some() {}
                        let _ = cmd.tx.send(()).await;
                        break;
                    }
                    Command::ClearProtected(cmd) => {
                        tasks.spawn(async move {
                            let _ = cmd.tx.send(Err(api_error(
                                "ClearProtected has no ContextDB-wide meaning; delete explicit fetch-protection tags",
                            ))).await;
                        });
                    }
                }
            }
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {
                if tasks.is_empty() {
                    for waiter in idle_waiters.drain(..) {
                        let _ = waiter.send(()).await;
                    }
                }
            }
        }
    }
}

fn format_code(format: BlobFormat) -> u8 {
    if format.is_raw() { 0 } else { 1 }
}

fn role_for_tag(name: &[u8]) -> BlobTagRole {
    if name.starts_with(b"mt/") {
        BlobTagRole::Servable
    } else {
        BlobTagRole::FetchProtection
    }
}

async fn contextdb_blob_status(repository: Arc<BlobRepository>, cmd: BlobStatusMsg) {
    let raw_hash = from_backend_hash(&cmd.inner.hash);
    let status = match repository.open_read(raw_hash) {
        Ok(Some(session)) => match session.manifest().state {
            contextdb_engine::blob_repository::BlobManifestState::Complete => {
                BlobStatus::Complete {
                    size: session.manifest().total_size,
                }
            }
            contextdb_engine::blob_repository::BlobManifestState::Partial => {
                let mut validated = Some(session.manifest().validated_size);
                if let Some(partial) = session.partial()
                    && let Ok(mut active) =
                        serde_json::from_slice::<DurableBaoState>(&partial.adapter_bitfield)
                    && active.version == 1
                {
                    if let Ok(Some(staged)) = provisional_bao_payload(
                        &repository,
                        &raw_hash,
                        session.manifest().total_size,
                    ) {
                        active.payload.update(&staged);
                    }
                    validated = active.payload.validated_size();
                }
                BlobStatus::Partial { size: validated }
            }
        },
        Ok(None) => match provisional_bao_payload(&repository, &raw_hash, u64::MAX) {
            Ok(Some(payload)) => BlobStatus::Partial {
                size: payload.validated_size(),
            },
            Ok(None) | Err(_) => BlobStatus::NotFound,
        },
        Err(_) => BlobStatus::NotFound,
    };
    let _ = cmd.tx.send(status).await;
}

async fn contextdb_list_blobs(repository: Arc<BlobRepository>, cmd: ListBlobsMsg) {
    let mut cursor = None;
    loop {
        let page = match repository.list_active_hashes_page(cursor.as_ref(), 256) {
            Ok(page) => page,
            Err(err) => {
                let _ = cmd.tx.send(Err(command_io_error("ListBlobs", err))).await;
                return;
            }
        };
        if page.is_empty() {
            return;
        }
        for hash in &page {
            if cmd.tx.send(Ok(to_backend_hash(hash))).await.is_err() {
                return;
            }
        }
        cursor = page.last().copied();
    }
}

async fn contextdb_set_tag(repository: Arc<BlobRepository>, cmd: SetTagMsg) {
    let SetTagRequest { name, value } = cmd.inner;
    let result = repository
        .set_tag(
            name.0.to_vec(),
            from_backend_hash(&value.hash),
            format_code(value.format),
            role_for_tag(name.0.as_ref()),
        )
        .map_err(|err| command_io_error("SetTag", err));
    let _ = cmd.tx.send(result).await;
}

async fn contextdb_delete_tags(repository: Arc<BlobRepository>, cmd: DeleteTagsMsg) {
    let result = repository
        .delete_tags(
            cmd.inner.from.as_ref().map(|tag| tag.0.as_ref()),
            cmd.inner.to.as_ref().map(|tag| tag.0.as_ref()),
        )
        .map_err(|err| command_io_error("DeleteTags", err));
    let _ = cmd.tx.send(result).await;
}

async fn contextdb_rename_tag(repository: Arc<BlobRepository>, cmd: RenameTagMsg) {
    let result = repository
        .rename_tag(cmd.inner.from.0.as_ref(), cmd.inner.to.0.to_vec())
        .map_err(|err| command_io_error("RenameTag", err));
    let _ = cmd.tx.send(result).await;
}

async fn contextdb_create_tag(repository: Arc<BlobRepository>, cmd: CreateTagMsg) {
    let value = cmd.inner.value;
    let hash = from_backend_hash(&value.hash);
    let mut name = b"auto/".to_vec();
    name.extend_from_slice(value.hash.to_hex().as_bytes());
    let result = repository
        .set_tag(
            name.clone(),
            hash,
            format_code(value.format),
            BlobTagRole::FetchProtection,
        )
        .map(|_| iroh_blobs::api::Tag::from(name.as_slice()))
        .map_err(|err| command_io_error("CreateTag", err));
    let _ = cmd.tx.send(result).await;
}

fn stage_reader(
    repository: &BlobRepository,
    guard: &BlobSharedGenerationGuard,
    object_id: [u8; 16],
    mut reader: impl std::io::Read,
    outboard: bool,
    logical_size: u64,
    adapter_state: &[u8],
) -> Result<(u64, Option<[u8; 32]>)> {
    let mut index = 0u64;
    let mut total = 0u64;
    let mut hasher = (!outboard).then(blake3::Hasher::new);
    let mut block = vec![0u8; BLOB_LOGICAL_BLOCK_BYTES];
    loop {
        let mut used = 0usize;
        while used < block.len() {
            let read = reader
                .read(&mut block[used..])
                .map_err(|err| other("read staged blob", err))?;
            if read == 0 {
                break;
            }
            used += read;
        }
        if used == 0 {
            break;
        }
        repository.checkpoint_staging_fragment(
            guard,
            object_id,
            index,
            0,
            &block[..used],
            logical_size,
            outboard,
            adapter_state,
            adapter_state.to_vec(),
        )?;
        if let Some(hasher) = &mut hasher {
            hasher.update(&block[..used]);
        }
        total = total.saturating_add(used as u64);
        index = index.saturating_add(1);
        if used < block.len() {
            break;
        }
    }
    Ok((total, hasher.map(|hasher| *hasher.finalize().as_bytes())))
}

#[allow(
    clippy::too_many_arguments,
    reason = "a complete content-addressed import must bind its lease, hash, payload, outboard, and replacement decision atomically"
)]
fn commit_complete_readers(
    repository: &Arc<BlobRepository>,
    guard: &BlobSharedGenerationGuard,
    hash: [u8; 32],
    size: u64,
    format: BlobFormat,
    payload: impl std::io::Read,
    outboard: impl std::io::Read,
    outboard_size: u64,
    replace_active: bool,
) -> Result<()> {
    let candidates = repository.list_import_staging_for_hash(&hash, 256)?;
    let mut complete_candidates = candidates
        .into_iter()
        .filter(|staging| staging.adapter_state == COMPLETE_STAGING_STATE);
    let (staging, reused) = match complete_candidates.next() {
        Some(staging)
            if staging.expected_size == Some(size)
                && staging.provisional_generation == Some((hash, guard.generation())) =>
        {
            if complete_candidates.next().is_some() {
                return Err(Error::Other(
                    "media adapter: multiple complete staging objects claim one blob hash"
                        .to_string(),
                ));
            }
            (staging, true)
        }
        Some(_) => {
            return Err(Error::Other(
                "media adapter: prior complete staging disagrees with current import".to_string(),
            ));
        }
        None => (
            repository.begin_import_staging_for_generation(
                guard,
                Some(size),
                COMPLETE_STAGING_STATE.to_vec(),
            )?,
            false,
        ),
    };
    let adapter_state = staging.adapter_state.clone();
    let outcome = (|| {
        let (payload_size, staged_hash) = stage_reader(
            repository,
            guard,
            staging.object_id,
            payload,
            false,
            size,
            &adapter_state,
        )?;
        let (staged_outboard, _) = stage_reader(
            repository,
            guard,
            staging.object_id,
            outboard,
            true,
            outboard_size,
            &adapter_state,
        )?;
        if payload_size != size || staged_outboard != outboard_size || staged_hash != Some(hash) {
            return Err(Error::Other(
                "media adapter: staged complete blob disagrees with its content address"
                    .to_string(),
            ));
        }
        repository.bind_staging(
            guard,
            BlobStagingBind {
                hash,
                object_id: staging.object_id,
                format: format_code(format),
                total_size: size,
                outboard_size,
                validated_size: size,
                state: BlobManifestState::Complete,
                partial_state: None,
                tags: &[],
                replace_active,
            },
        )?;
        Ok(())
    })();
    if outcome.is_err() && !reused {
        let _ = repository.discard_staging(staging.object_id);
    }
    outcome
}

fn import_complete_bytes(
    repository: &Arc<BlobRepository>,
    data: bytes::Bytes,
    format: BlobFormat,
) -> Result<Hash> {
    let outboard =
        PreOrderOutboard::<Vec<u8>>::create(std::io::Cursor::new(data.as_ref()), IROH_BLOCK_SIZE)
            .map_err(|err| other("build Bao outboard", err))?;
    let hash = Hash::from(*outboard.root.as_bytes());
    let raw_hash = from_backend_hash(&hash);
    let existing = repository.open_read(raw_hash)?;
    let existing_partial = existing
        .as_ref()
        .is_some_and(|read| read.manifest().state == BlobManifestState::Partial);
    let existing_missing = existing.is_none();
    drop(existing);
    let guard = repository.begin_write_generation(raw_hash)?;
    if existing_missing || existing_partial {
        commit_complete_readers(
            repository,
            &guard,
            raw_hash,
            data.len() as u64,
            format,
            std::io::Cursor::new(data.as_ref()),
            std::io::Cursor::new(outboard.data.as_slice()),
            outboard.tree.outboard_size(),
            existing_partial,
        )?;
    } else {
        for previous in repository.list_import_staging_for_hash(&raw_hash, 256)? {
            repository.discard_staging_for_writer(&guard, previous.object_id)?;
        }
    }
    Ok(hash)
}

async fn contextdb_import_bytes(repository: Arc<BlobRepository>, cmd: ImportBytesMsg) {
    let data = cmd.inner.data;
    let format = cmd.inner.format;
    let size = data.len() as u64;
    let _ = cmd.tx.send(AddProgressItem::Size(size)).await;
    let _ = cmd.tx.send(AddProgressItem::CopyDone).await;
    let import_repository = repository.clone();
    match tokio::task::spawn_blocking(move || {
        import_complete_bytes(&import_repository, data, format)
    })
    .await
    {
        Ok(Ok(hash)) => {
            let tag = TempTag::new(HashAndFormat::new(hash, format), None);
            let _ = cmd.tx.send(AddProgressItem::Done(tag)).await;
        }
        Ok(Err(err)) => {
            let _ = cmd
                .tx
                .send(AddProgressItem::Error(std::io::Error::other(
                    err.to_string(),
                )))
                .await;
        }
        Err(err) => {
            let _ = cmd
                .tx
                .send(AddProgressItem::Error(std::io::Error::other(format!(
                    "media adapter: import bytes task failed: {err}"
                ))))
                .await;
        }
    }
}

#[derive(Clone)]
struct RepositoryReadAt {
    session: Arc<contextdb_engine::blob_repository::BlobReadSession>,
    outboard: bool,
}

impl RepositoryReadAt {
    fn read(&self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let result = if self.outboard {
            self.session.read_outboard_at(offset, len)
        } else {
            self.session.read_payload_at(offset, len)
        };
        result.map_err(|err| std::io::Error::other(err.to_string()))
    }
}

impl ReadAt for RepositoryReadAt {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes = self.read(pos, buf.len())?;
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }
}

impl ReadBytesAt for RepositoryReadAt {
    fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<bytes::Bytes> {
        self.read(offset, size).map(Into::into)
    }
}

fn repository_outboard(
    hash: Hash,
    session: Arc<contextdb_engine::blob_repository::BlobReadSession>,
) -> PreOrderOutboard<RepositoryReadAt> {
    PreOrderOutboard {
        root: blake3::Hash::from_bytes(from_backend_hash(&hash)),
        tree: BaoTree::new(session.manifest().total_size, IROH_BLOCK_SIZE),
        data: RepositoryReadAt {
            session,
            outboard: true,
        },
    }
}

async fn contextdb_export_bao(repository: Arc<BlobRepository>, mut cmd: ExportBaoMsg) {
    let hash = cmd.inner.hash;
    let Some(session) = repository
        .open_read(from_backend_hash(&hash))
        .ok()
        .flatten()
    else {
        let _ = cmd
            .tx
            .send(EncodedItem::Error(EncodeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "blob not found",
            ))))
            .await;
        return;
    };
    let session = Arc::new(session);
    let data = RepositoryReadAt {
        session: session.clone(),
        outboard: false,
    };
    let outboard = repository_outboard(hash, session);
    let mut sender = BaoTreeSender(&mut cmd.tx);
    let _ = traverse_ranges_validated(data, outboard, &cmd.inner.ranges, &mut sender).await;
}

async fn contextdb_export_ranges(repository: Arc<BlobRepository>, cmd: ExportRangesMsg) {
    let Some(session) = repository
        .open_read(from_backend_hash(&cmd.inner.hash))
        .ok()
        .flatten()
    else {
        let _ = cmd
            .tx
            .send(ExportRangesItem::Error(api_error("blob not found")))
            .await;
        return;
    };
    let size = session.manifest().total_size;
    let _ = cmd.tx.send(ExportRangesItem::Size(size)).await;
    for range in cmd.inner.ranges.iter() {
        let (mut offset, end) = match range {
            RangeSetRange::Range(range) => ((*range.start).min(size), (*range.end).min(size)),
            RangeSetRange::RangeFrom(range) => ((*range.start).min(size), size),
        };
        while offset < end {
            let take = (end - offset).min(EXPORT_CHUNK as u64) as usize;
            let data = match session.read_payload_at(offset, take) {
                Ok(data) if !data.is_empty() => data,
                Ok(_) => return,
                Err(err) => {
                    let _ = cmd
                        .tx
                        .send(ExportRangesItem::Error(command_io_error(
                            "ExportRanges",
                            err,
                        )))
                        .await;
                    return;
                }
            };
            let len = data.len() as u64;
            if cmd
                .tx
                .send(ExportRangesItem::Data(Leaf {
                    offset,
                    data: data.into(),
                }))
                .await
                .is_err()
            {
                return;
            }
            offset = offset.saturating_add(len);
        }
    }
}

async fn contextdb_observe(repository: Arc<BlobRepository>, cmd: ObserveMsg) {
    let raw_hash = from_backend_hash(&cmd.inner.hash);
    let bitfield = match repository.open_read(raw_hash) {
        Ok(Some(session)) if session.manifest().state == BlobManifestState::Complete => {
            iroh_blobs::api::proto::Bitfield::complete(session.manifest().total_size)
        }
        Ok(Some(session)) => {
            let Some(partial) = session.partial() else {
                return;
            };
            let Ok(state) = serde_json::from_slice::<DurableBaoState>(&partial.adapter_bitfield)
            else {
                return;
            };
            if state.version != 1 {
                return;
            }
            let mut payload = state.payload;
            if let Ok(Some(staged)) =
                provisional_bao_payload(&repository, &raw_hash, session.manifest().total_size)
            {
                payload.update(&staged);
            }
            payload
        }
        Ok(None) => match provisional_bao_payload(&repository, &raw_hash, u64::MAX) {
            Ok(Some(payload)) => payload,
            Ok(None) | Err(_) => iroh_blobs::api::proto::Bitfield::empty(),
        },
        Err(_) => iroh_blobs::api::proto::Bitfield::empty(),
    };
    let _ = cmd.tx.send(bitfield).await;
}

struct RepositoryStagingWriteAt<'a> {
    repository: &'a BlobRepository,
    guard: &'a BlobSharedGenerationGuard,
    object_id: [u8; 16],
    logical_size: u64,
    adapter_state: &'a [u8],
}

impl WriteAt for RepositoryStagingWriteAt<'_> {
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<usize> {
        let mut consumed = 0usize;
        while consumed < buf.len() {
            let offset = pos.saturating_add(consumed as u64);
            let index = offset / BLOB_LOGICAL_BLOCK_BYTES as u64;
            let within = (offset % BLOB_LOGICAL_BLOCK_BYTES as u64) as usize;
            let take = (BLOB_LOGICAL_BLOCK_BYTES - within).min(buf.len() - consumed);
            self.repository
                .checkpoint_staging_fragment(
                    self.guard,
                    self.object_id,
                    index,
                    within,
                    &buf[consumed..consumed + take],
                    self.logical_size,
                    true,
                    self.adapter_state,
                    self.adapter_state.to_vec(),
                )
                .map_err(|err| std::io::Error::other(err.to_string()))?;
            consumed += take;
        }
        Ok(consumed)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct RepositoryStagingReader<'a, R> {
    inner: R,
    repository: &'a BlobRepository,
    guard: &'a BlobSharedGenerationGuard,
    object_id: [u8; 16],
    size: u64,
    offset: u64,
    adapter_state: &'a [u8],
}

impl<R: std::io::Read> std::io::Read for RepositoryStagingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.size.saturating_sub(self.offset);
        if remaining == 0 {
            return Ok(0);
        }
        let limit = usize::try_from(remaining.min(buf.len() as u64)).unwrap_or(buf.len());
        let read = self.inner.read(&mut buf[..limit])?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "import path shortened while constructing its Bao tree",
            ));
        }
        let mut consumed = 0usize;
        while consumed < read {
            let offset = self.offset.saturating_add(consumed as u64);
            let index = offset / BLOB_LOGICAL_BLOCK_BYTES as u64;
            let within = (offset % BLOB_LOGICAL_BLOCK_BYTES as u64) as usize;
            let take = (BLOB_LOGICAL_BLOCK_BYTES - within).min(read - consumed);
            self.repository
                .checkpoint_staging_fragment(
                    self.guard,
                    self.object_id,
                    index,
                    within,
                    &buf[consumed..consumed + take],
                    self.size,
                    false,
                    self.adapter_state,
                    self.adapter_state.to_vec(),
                )
                .map_err(|err| std::io::Error::other(err.to_string()))?;
            consumed += take;
        }
        self.offset = self.offset.saturating_add(read as u64);
        Ok(read)
    }
}

fn hash_open_file(file: &mut std::fs::File, size: u64) -> Result<[u8; 32]> {
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))
        .map_err(|err| other("rewind import path", err))?;
    let mut remaining = size;
    let mut hasher = blake3::Hasher::new();
    let mut block = vec![0u8; EXPORT_CHUNK];
    while remaining != 0 {
        let take = usize::try_from(remaining.min(block.len() as u64)).unwrap_or(block.len());
        let read = file
            .read(&mut block[..take])
            .map_err(|err| other("hash import path", err))?;
        if read == 0 {
            return Err(Error::Other(
                "media adapter: import path shortened while hashing".to_string(),
            ));
        }
        hasher.update(&block[..read]);
        remaining = remaining.saturating_sub(read as u64);
    }
    Ok(*hasher.finalize().as_bytes())
}

async fn contextdb_import_path(repository: Arc<BlobRepository>, cmd: ImportPathMsg) {
    let path = cmd.inner.path;
    let format = cmd.inner.format;
    let result = tokio::task::spawn_blocking(move || -> Result<(Hash, u64)> {
        use std::io::{Seek, SeekFrom};
        let mut file = std::fs::File::open(&path).map_err(|err| other("open import path", err))?;
        let size = file
            .metadata()
            .map_err(|err| other("stat open import path", err))?
            .len();
        let raw_hash = hash_open_file(&mut file, size)?;
        let hash = Hash::from(raw_hash);
        let guard = repository.begin_write_generation(raw_hash)?;
        if repository
            .open_read(raw_hash)?
            .is_some_and(|read| read.manifest().state == BlobManifestState::Complete)
        {
            for previous in repository.list_import_staging_for_hash(&raw_hash, 256)? {
                repository.discard_staging_for_writer(&guard, previous.object_id)?;
            }
            return Ok((hash, size));
        }
        let replace_active = repository.open_read(raw_hash)?.is_some();
        let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
        let candidates = repository.list_import_staging_for_hash(&raw_hash, 256)?;
        let mut path_candidates = candidates
            .into_iter()
            .filter(|staging| staging.adapter_state == COMPLETE_STAGING_STATE);
        let staging = match path_candidates.next() {
            Some(staging)
                if staging.expected_size == Some(size)
                    && staging.provisional_generation == Some((raw_hash, guard.generation())) =>
            {
                if path_candidates.next().is_some() {
                    return Err(Error::Other(
                        "media adapter: multiple path staging objects claim one blob hash"
                            .to_string(),
                    ));
                }
                staging
            }
            Some(_) => {
                return Err(Error::Other(
                    "media adapter: prior path staging disagrees with the current import"
                        .to_string(),
                ));
            }
            None => repository.begin_import_staging_for_generation(
                &guard,
                Some(size),
                COMPLETE_STAGING_STATE.to_vec(),
            )?,
        };
        let adapter_state = staging.adapter_state.clone();
        file.seek(SeekFrom::Start(0))
            .map_err(|err| other("rewind import path for staging", err))?;
        let mut outboard = PreOrderOutboard {
            root: blake3::hash(&[]),
            tree,
            data: RepositoryStagingWriteAt {
                repository: &repository,
                guard: &guard,
                object_id: staging.object_id,
                logical_size: tree.outboard_size(),
                adapter_state: &adapter_state,
            },
        };
        let outcome = (|| -> Result<()> {
            outboard
                .init_from(RepositoryStagingReader {
                    inner: file,
                    repository: &repository,
                    guard: &guard,
                    object_id: staging.object_id,
                    size,
                    offset: 0,
                    adapter_state: &adapter_state,
                })
                .map_err(|err| other("build and stage path Bao object", err))?;
            if outboard.root.as_bytes() != &raw_hash {
                return Err(Error::Other(
                    "media adapter: import path changed between hashing and durable staging"
                        .to_string(),
                ));
            }
            repository.bind_staging(
                &guard,
                BlobStagingBind {
                    hash: raw_hash,
                    object_id: staging.object_id,
                    format: format_code(format),
                    total_size: size,
                    outboard_size: tree.outboard_size(),
                    validated_size: size,
                    state: BlobManifestState::Complete,
                    partial_state: None,
                    tags: &[],
                    replace_active,
                },
            )?;
            Ok(())
        })();
        if outcome.is_err() {
            let _ = repository.discard_staging(staging.object_id);
        }
        outcome?;
        Ok((hash, size))
    })
    .await
    .map_err(|err| Error::Other(format!("media adapter: import path task failed: {err}")))
    .and_then(|result| result);
    match result {
        Ok((hash, size)) => {
            let _ = cmd.tx.send(AddProgressItem::Size(size)).await;
            let _ = cmd.tx.send(AddProgressItem::CopyDone).await;
            let tag = TempTag::new(HashAndFormat::new(hash, format), None);
            let _ = cmd.tx.send(AddProgressItem::Done(tag)).await;
        }
        Err(err) => {
            let _ = cmd
                .tx
                .send(AddProgressItem::Error(std::io::Error::other(
                    err.to_string(),
                )))
                .await;
        }
    }
}

async fn contextdb_export_path(repository: Arc<BlobRepository>, cmd: ExportPathMsg) {
    let result = (|| -> Result<u64> {
        let session = repository
            .open_read(from_backend_hash(&cmd.inner.hash))?
            .ok_or_else(|| Error::Other("blob is not present".to_string()))?;
        if session.manifest().state != BlobManifestState::Complete {
            return Err(Error::Other(
                "partial blob cannot be exported as a complete path".to_string(),
            ));
        }
        if let Some(parent) = cmd.inner.target.parent() {
            std::fs::create_dir_all(parent).map_err(|err| other("create export directory", err))?;
        }
        let mut output = std::fs::File::create(&cmd.inner.target)
            .map_err(|err| other("create export path", err))?;
        let mut offset = 0u64;
        while offset < session.manifest().total_size {
            let data = session.read_payload_at(offset, EXPORT_CHUNK)?;
            if data.is_empty() {
                return Err(Error::Other("canonical blob ended early".to_string()));
            }
            output
                .write_all(&data)
                .map_err(|err| other("write export path", err))?;
            offset = offset.saturating_add(data.len() as u64);
        }
        Ok(offset)
    })();
    match result {
        Ok(size) => {
            let _ = cmd.tx.send(ExportProgressItem::Size(size)).await;
            let _ = cmd.tx.send(ExportProgressItem::Done).await;
        }
        Err(err) => {
            let _ = cmd
                .tx
                .send(ExportProgressItem::Error(command_io_error(
                    "ExportPath",
                    err,
                )))
                .await;
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DurableBaoState {
    version: u8,
    payload: iroh_blobs::api::proto::Bitfield,
    outboard_pairs: ChunkRanges,
}

fn provisional_bao_payload(
    repository: &BlobRepository,
    hash: &[u8; 32],
    expected_size: u64,
) -> Result<Option<iroh_blobs::api::proto::Bitfield>> {
    let mut union: Option<iroh_blobs::api::proto::Bitfield> = None;
    for staging in repository.list_import_staging_for_hash(hash, 256)? {
        if expected_size != u64::MAX && staging.expected_size != Some(expected_size) {
            continue;
        }
        let Ok(state) = serde_json::from_slice::<DurableBaoState>(&staging.adapter_state) else {
            continue;
        };
        if state.version != 1 {
            continue;
        }
        match &mut union {
            Some(payload) => {
                payload.update(&state.payload);
            }
            None => union = Some(state.payload),
        }
    }
    Ok(union)
}

struct PartialBaoImport {
    guard: BlobSharedGenerationGuard,
    replace_active: bool,
    tree: BaoTree,
    staging: BlobImportStaging,
    bitfield: iroh_blobs::api::proto::Bitfield,
    outboard_pairs: ChunkRanges,
}

fn encode_durable_bao_state(
    payload: &iroh_blobs::api::proto::Bitfield,
    outboard_pairs: &ChunkRanges,
) -> Result<Vec<u8>> {
    serde_json::to_vec(&DurableBaoState {
        version: 1,
        payload: payload.clone(),
        outboard_pairs: outboard_pairs.clone(),
    })
    .map_err(|err| other("encode durable Bao state", err))
}

fn merge_canonical_partial_into_staging(
    repository: &BlobRepository,
    guard: &BlobSharedGenerationGuard,
    session: &contextdb_engine::blob_repository::BlobReadSession,
    staging: &mut BlobImportStaging,
    payload: &mut iroh_blobs::api::proto::Bitfield,
    outboard_pairs: &mut ChunkRanges,
) -> Result<()> {
    let Some(partial) = session.partial() else {
        return Ok(());
    };
    let canonical: DurableBaoState = serde_json::from_slice(&partial.adapter_bitfield)
        .map_err(|err| other("decode canonical Bao resume state", err))?;
    if canonical.version != 1 {
        return Err(Error::Other(
            "media adapter: unsupported canonical Bao resume state version".to_string(),
        ));
    }
    let size = session.manifest().total_size;
    let payload_blocks =
        size.saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64 - 1) / BLOB_LOGICAL_BLOCK_BYTES as u64;
    for index in 0..payload_blocks {
        let offset = index.saturating_mul(BLOB_LOGICAL_BLOCK_BYTES as u64);
        let end = offset
            .saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64)
            .min(size);
        let extent = ChunkRanges::bytes(offset..end);
        if !canonical.payload.ranges.is_superset(&extent) || payload.ranges.is_superset(&extent) {
            continue;
        }
        let bytes = session.read_payload_at(offset, (end - offset) as usize)?;
        if bytes.len() != (end - offset) as usize {
            return Err(Error::Other(
                "canonical Bao state names a missing payload extent".to_string(),
            ));
        }
        let expected_state = encode_durable_bao_state(payload, outboard_pairs)?;
        payload.update(&iroh_blobs::api::proto::Bitfield::new(extent, size));
        let next_state = encode_durable_bao_state(payload, outboard_pairs)?;
        *staging = repository.checkpoint_staging_fragment(
            guard,
            staging.object_id,
            index,
            0,
            &bytes,
            size,
            false,
            &expected_state,
            next_state,
        )?;
    }
    for pair_range in canonical.outboard_pairs.iter() {
        let (start, end) = match pair_range {
            RangeSetRange::Range(range) => (range.start.0, range.end.0),
            RangeSetRange::RangeFrom(_) => {
                return Err(Error::Other(
                    "canonical Bao state contains an unbounded outboard range".to_string(),
                ));
            }
        };
        for pair_number in start..end {
            let pair_index = ChunkNum(pair_number);
            let pair_range =
                ChunkRanges::from(pair_index..ChunkNum(pair_index.0.saturating_add(1)));
            if !(&*outboard_pairs & &pair_range).is_empty() {
                continue;
            }
            let pair_offset = pair_index.0.saturating_mul(64);
            let bytes = session.read_outboard_at(pair_offset, 64)?;
            if bytes.len() != 64 {
                return Err(Error::Other(
                    "canonical Bao state names a missing outboard pair".to_string(),
                ));
            }
            let expected_state = encode_durable_bao_state(payload, outboard_pairs)?;
            *outboard_pairs |= pair_range;
            let next_state = encode_durable_bao_state(payload, outboard_pairs)?;
            *staging = repository.checkpoint_staging_fragment(
                guard,
                staging.object_id,
                pair_offset / BLOB_LOGICAL_BLOCK_BYTES as u64,
                (pair_offset % BLOB_LOGICAL_BLOCK_BYTES as u64) as usize,
                &bytes,
                session.manifest().outboard_size,
                true,
                &expected_state,
                next_state,
            )?;
        }
    }
    Ok(())
}

fn prepare_partial_bao_import(
    repository: &Arc<BlobRepository>,
    hash: [u8; 32],
    size: u64,
) -> Result<Option<PartialBaoImport>> {
    let guard = repository.begin_write_generation(hash)?;
    let existing = repository.open_read(hash)?;
    if existing
        .as_ref()
        .is_some_and(|session| session.manifest().state == BlobManifestState::Complete)
    {
        return Ok(None);
    }
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let mut bitfield = if size == 0 {
        iroh_blobs::api::proto::Bitfield::complete(0)
    } else {
        iroh_blobs::api::proto::Bitfield::empty()
    };
    let mut outboard_pairs = ChunkRanges::empty();
    let candidates = repository.list_import_staging_for_hash(&hash, 256)?;
    let mut bao_candidates = candidates.into_iter().filter(|staging| {
        serde_json::from_slice::<DurableBaoState>(&staging.adapter_state)
            .is_ok_and(|state| state.version == 1)
    });
    if let Some(mut staging) = bao_candidates.next() {
        if bao_candidates.next().is_some() {
            return Err(Error::Other(
                "multiple durable Bao staging objects claim one blob hash".to_string(),
            ));
        }
        if staging.expected_size != Some(size)
            || staging.provisional_generation != Some((hash, guard.generation()))
        {
            return Err(Error::Other(
                "durable Bao staging disagrees with its resumed generation".to_string(),
            ));
        }
        let durable: DurableBaoState = serde_json::from_slice(&staging.adapter_state)
            .map_err(|err| other("decode durable staged Bao state", err))?;
        if durable.version != 1 {
            return Err(Error::Other(
                "media adapter: unsupported durable staged Bao state version".to_string(),
            ));
        }
        bitfield = durable.payload;
        outboard_pairs = durable.outboard_pairs;
        if let Some(existing) = existing.as_ref() {
            merge_canonical_partial_into_staging(
                repository,
                &guard,
                existing,
                &mut staging,
                &mut bitfield,
                &mut outboard_pairs,
            )?;
        }
        return Ok(Some(PartialBaoImport {
            guard,
            replace_active: existing.is_some(),
            tree,
            staging,
            bitfield,
            outboard_pairs,
        }));
    }
    let empty_payload = bitfield.clone();
    let empty_outboard = ChunkRanges::empty();
    let initial_state = encode_durable_bao_state(&empty_payload, &empty_outboard)?;
    let mut staging =
        repository.begin_import_staging_for_generation(&guard, Some(size), initial_state)?;
    if let Some(existing) = existing.as_ref() {
        let mut copied_payload = empty_payload;
        let mut copied_outboard = empty_outboard;
        if let Some(partial) = existing.partial() {
            let durable: DurableBaoState = serde_json::from_slice(&partial.adapter_bitfield)
                .map_err(|err| other("decode durable Bao state", err))?;
            if durable.version != 1 {
                return Err(Error::Other(
                    "media adapter: unsupported durable Bao state version".to_string(),
                ));
            }
            bitfield = durable.payload;
            outboard_pairs = durable.outboard_pairs;
        }
        let payload_blocks = size.saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64 - 1)
            / BLOB_LOGICAL_BLOCK_BYTES as u64;
        for index in 0..payload_blocks {
            let offset = index.saturating_mul(BLOB_LOGICAL_BLOCK_BYTES as u64);
            let end = offset
                .saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64)
                .min(size);
            if bitfield
                .ranges
                .is_superset(&ChunkRanges::bytes(offset..end))
            {
                let bytes = existing.read_payload_at(offset, (end - offset) as usize)?;
                if bytes.len() != (end - offset) as usize {
                    return Err(Error::Other(
                        "durable Bao bitfield names a missing payload extent".to_string(),
                    ));
                }
                let expected_state = encode_durable_bao_state(&copied_payload, &copied_outboard)?;
                copied_payload.update(&iroh_blobs::api::proto::Bitfield::new(
                    ChunkRanges::bytes(offset..end),
                    size,
                ));
                let next_state = encode_durable_bao_state(&copied_payload, &copied_outboard)?;
                staging = repository.checkpoint_staging_fragment(
                    &guard,
                    staging.object_id,
                    index,
                    0,
                    &bytes,
                    size,
                    false,
                    &expected_state,
                    next_state,
                )?;
            }
        }
        for pair_range in outboard_pairs.iter() {
            let (start, end) = match pair_range {
                RangeSetRange::Range(range) => (range.start.0, range.end.0),
                RangeSetRange::RangeFrom(_) => {
                    return Err(Error::Other(
                        "durable Bao state contains an unbounded outboard range".to_string(),
                    ));
                }
            };
            for pair_number in start..end {
                let pair_index = ChunkNum(pair_number);
                let pair_offset = pair_index.0.saturating_mul(64);
                let bytes = existing.read_outboard_at(pair_offset, 64)?;
                if bytes.len() != 64 {
                    return Err(Error::Other(
                        "durable Bao state names a missing outboard pair".to_string(),
                    ));
                }
                let expected_state = encode_durable_bao_state(&copied_payload, &copied_outboard)?;
                copied_outboard |=
                    ChunkRanges::from(pair_index..ChunkNum(pair_index.0.saturating_add(1)));
                let next_state = encode_durable_bao_state(&copied_payload, &copied_outboard)?;
                let block_index = pair_offset / BLOB_LOGICAL_BLOCK_BYTES as u64;
                let within = (pair_offset % BLOB_LOGICAL_BLOCK_BYTES as u64) as usize;
                staging = repository.checkpoint_staging_fragment(
                    &guard,
                    staging.object_id,
                    block_index,
                    within,
                    &bytes,
                    existing.manifest().outboard_size,
                    true,
                    &expected_state,
                    next_state,
                )?;
            }
        }
    }
    Ok(Some(PartialBaoImport {
        guard,
        replace_active: existing.is_some(),
        tree,
        staging,
        bitfield,
        outboard_pairs,
    }))
}

fn finish_partial_bao_import(
    repository: &Arc<BlobRepository>,
    backend_hash: Hash,
    size: u64,
    import: PartialBaoImport,
) -> Result<()> {
    let hash = from_backend_hash(&backend_hash);
    let staged = (|| -> Result<()> {
        let payload_blocks = size.saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64 - 1)
            / BLOB_LOGICAL_BLOCK_BYTES as u64;
        let outboard_count = import
            .tree
            .outboard_size()
            .saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64 - 1)
            / BLOB_LOGICAL_BLOCK_BYTES as u64;
        let mut verified_payload_bytes = 0u64;
        for index in 0..payload_blocks {
            let offset = index.saturating_mul(BLOB_LOGICAL_BLOCK_BYTES as u64);
            let end = offset
                .saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64)
                .min(size);
            if import
                .bitfield
                .ranges
                .is_superset(&ChunkRanges::bytes(offset..end))
            {
                verified_payload_bytes = verified_payload_bytes.saturating_add(end - offset);
            }
        }
        let encoded_bitfield = encode_durable_bao_state(&import.bitfield, &import.outboard_pairs)?;
        let partial = BlobPartialState::from_exact_indices(
            (0..payload_blocks).filter(|index| {
                let offset = index.saturating_mul(BLOB_LOGICAL_BLOCK_BYTES as u64);
                let end = offset
                    .saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64)
                    .min(size);
                import
                    .bitfield
                    .ranges
                    .is_superset(&ChunkRanges::bytes(offset..end))
            }),
            (0..outboard_count).filter(|index| {
                let offset = index.saturating_mul(BLOB_LOGICAL_BLOCK_BYTES as u64);
                let end = offset
                    .saturating_add(BLOB_LOGICAL_BLOCK_BYTES as u64)
                    .min(import.tree.outboard_size());
                let block_pairs = ChunkRanges::from(ChunkNum(offset / 64)..ChunkNum(end / 64));
                !(&import.outboard_pairs & &block_pairs).is_empty()
            }),
            encoded_bitfield,
        )?;
        let complete = import.bitfield.is_complete();
        let tag = BlobTagRecord::new(
            fetch_protect_tag(&backend_hash).into_bytes(),
            hash,
            import.guard.generation(),
            format_code(BlobFormat::Raw),
            BlobTagRole::FetchProtection,
        );
        repository.bind_staging(
            &import.guard,
            BlobStagingBind {
                hash,
                object_id: import.staging.object_id,
                format: format_code(BlobFormat::Raw),
                total_size: size,
                outboard_size: import.tree.outboard_size(),
                validated_size: verified_payload_bytes,
                state: if complete {
                    BlobManifestState::Complete
                } else {
                    BlobManifestState::Partial
                },
                partial_state: (!complete).then_some(&partial),
                tags: &[tag],
                replace_active: import.replace_active,
            },
        )?;
        Ok(())
    })();
    if staged.is_err() {
        let _ = repository.discard_staging(import.staging.object_id);
    }
    staged
}

async fn contextdb_import_bao(repository: Arc<BlobRepository>, mut cmd: ImportBaoMsg) {
    let backend_hash = cmd.inner.hash;
    let raw_hash = from_backend_hash(&backend_hash);
    let size = cmd.inner.size.get();
    let prepare_repository = repository.clone();
    let mut import = match tokio::task::spawn_blocking(move || {
        prepare_partial_bao_import(&prepare_repository, raw_hash, size)
    })
    .await
    {
        Ok(Ok(Some(import))) => import,
        Ok(Ok(None)) => {
            let _ = cmd.tx.send(Ok(())).await;
            return;
        }
        Ok(Err(err)) => {
            let _ = cmd.tx.send(Err(command_io_error("ImportBao", err))).await;
            return;
        }
        Err(err) => {
            let _ = cmd
                .tx
                .send(Err(command_io_error(
                    "ImportBao",
                    format!("writer preparation task failed: {err}"),
                )))
                .await;
            return;
        }
    };
    let mut receive_error = None;
    loop {
        match cmd.rx.recv().await {
            Ok(Some(BaoContentItem::Parent(parent))) => {
                if let Some(offset) = import.tree.pre_order_offset(parent.node) {
                    let mut pair = [0u8; 64];
                    pair[..32].copy_from_slice(parent.pair.0.as_bytes());
                    pair[32..].copy_from_slice(parent.pair.1.as_bytes());
                    let expected_state =
                        match encode_durable_bao_state(&import.bitfield, &import.outboard_pairs) {
                            Ok(state) => state,
                            Err(err) => {
                                receive_error = Some(err);
                                break;
                            }
                        };
                    let mut next_pairs = import.outboard_pairs.clone();
                    next_pairs |=
                        ChunkRanges::from(ChunkNum(offset)..ChunkNum(offset.saturating_add(1)));
                    let next_state = match encode_durable_bao_state(&import.bitfield, &next_pairs) {
                        Ok(state) => state,
                        Err(err) => {
                            receive_error = Some(err);
                            break;
                        }
                    };
                    let byte_offset = offset.saturating_mul(64);
                    let result = repository.checkpoint_staging_fragment(
                        &import.guard,
                        import.staging.object_id,
                        byte_offset / BLOB_LOGICAL_BLOCK_BYTES as u64,
                        (byte_offset % BLOB_LOGICAL_BLOCK_BYTES as u64) as usize,
                        &pair,
                        import.tree.outboard_size(),
                        true,
                        &expected_state,
                        next_state,
                    );
                    match result {
                        Ok(staging) => {
                            import.staging = staging;
                            import.outboard_pairs = next_pairs;
                        }
                        Err(err) => {
                            receive_error = Some(err);
                            break;
                        }
                    }
                }
            }
            Ok(Some(BaoContentItem::Leaf(leaf))) => {
                let expected_state =
                    match encode_durable_bao_state(&import.bitfield, &import.outboard_pairs) {
                        Ok(state) => state,
                        Err(err) => {
                            receive_error = Some(err);
                            break;
                        }
                    };
                let mut next_bitfield = import.bitfield.clone();
                next_bitfield.update(&iroh_blobs::api::proto::Bitfield::new(
                    ChunkRanges::bytes(
                        leaf.offset..leaf.offset.saturating_add(leaf.data.len() as u64),
                    ),
                    size,
                ));
                let next_state =
                    match encode_durable_bao_state(&next_bitfield, &import.outboard_pairs) {
                        Ok(state) => state,
                        Err(err) => {
                            receive_error = Some(err);
                            break;
                        }
                    };
                let mut consumed = 0usize;
                while consumed < leaf.data.len() {
                    let byte_offset = leaf.offset.saturating_add(consumed as u64);
                    let block_index = byte_offset / BLOB_LOGICAL_BLOCK_BYTES as u64;
                    let within = (byte_offset % BLOB_LOGICAL_BLOCK_BYTES as u64) as usize;
                    let take = (BLOB_LOGICAL_BLOCK_BYTES - within)
                        .min(leaf.data.len().saturating_sub(consumed));
                    let final_fragment = consumed.saturating_add(take) == leaf.data.len();
                    let result = repository.checkpoint_staging_fragment(
                        &import.guard,
                        import.staging.object_id,
                        block_index,
                        within,
                        &leaf.data[consumed..consumed + take],
                        size,
                        false,
                        &expected_state,
                        if final_fragment {
                            next_state.clone()
                        } else {
                            expected_state.clone()
                        },
                    );
                    match result {
                        Ok(staging) => import.staging = staging,
                        Err(err) => {
                            receive_error = Some(err);
                            break;
                        }
                    }
                    consumed += take;
                }
                if receive_error.is_some() {
                    break;
                }
                import.bitfield = next_bitfield;
            }
            Ok(None) => break,
            Err(err) => {
                receive_error = Some(Error::Other(format!(
                    "media adapter: receive Bao import: {err}"
                )));
                break;
            }
        }
    }
    let result = match receive_error {
        None => finish_partial_bao_import(&repository, backend_hash, size, import),
        Some(receive_error) => {
            match finish_partial_bao_import(&repository, backend_hash, size, import) {
                Ok(()) => Err(receive_error),
                Err(commit_error) => Err(commit_error),
            }
        }
    }
    .map_err(|err| command_io_error("ImportBao", err));
    let _ = cmd.tx.send(result).await;
}

/// The holder/consumer-side blob store plus the serve seams. One per
/// BlobStore.
pub(crate) struct BlobStoreHandle {
    inner: Arc<StoreRuntime>,
    repository: Arc<BlobRepository>,
    bytes_hydrated_while_opening: u64,
    /// Harness seam: requested hash -> substitute content hash to serve.
    tamper: Arc<Mutex<HashMap<[u8; 32], Hash>>>,
    /// Harness seam: reset the next authorized, servable GET once after N
    /// payload bytes.
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

impl BlobStoreHandle {
    /// Read-only test support for purge proofs.  This intentionally exposes
    /// only the exact content addresses protected by the existing production
    /// adapter; it cannot add, remove, or inspect blob payload bytes.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn servable_hashes_for_test(&self) -> BTreeSet<[u8; 32]> {
        let mut result = BTreeSet::new();
        let mut cursor = None;
        loop {
            let page = self
                .repository
                .list_tags_page(cursor.as_deref(), 256)
                .unwrap_or_default();
            if page.is_empty() {
                break;
            }
            for tag in &page {
                if tag.role == BlobTagRole::Servable {
                    result.insert(tag.hash);
                }
            }
            cursor = page.last().map(|tag| tag.name.clone());
        }
        result
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn authoritative_roles_for_test(&self, hash: &[u8; 32]) -> BTreeSet<String> {
        self.repository
            .authoritative_purge_roles_for_test(hash)
            .expect("authoritative blob roles")
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn verified_partial_bytes_for_test(&self, hash: &[u8; 32]) -> u64 {
        self.repository
            .open_read(*hash)
            .expect("open partial blob")
            .map(|session| {
                session
                    .contiguous_payload_prefix()
                    .expect("read partial prefix")
            })
            .unwrap_or(0)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn bounded_raw_blob_bytes_for_test(
        &self,
        hash: &[u8; 32],
        max_bytes: usize,
    ) -> Option<Vec<u8>> {
        self.repository
            .bounded_payload_for_test(hash, max_bytes)
            .expect("bounded canonical payload")
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn durable_next_missing_range_for_test(
        &self,
        hash: &[u8; 32],
    ) -> Option<std::ops::Range<u64>> {
        self.repository
            .next_missing_range_for_test(hash)
            .expect("durable next missing range")
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn bytes_hydrated_while_opening_for_test(&self) -> u64 {
        self.bytes_hydrated_while_opening
    }

    /// Open (or create) the content-addressed store under `root`, with GC
    /// configured so untagged blobs free at [`GC_INTERVAL`].
    pub(crate) fn open(repository: Arc<BlobRepository>) -> Result<BlobStoreHandle> {
        let bytes_before_open = repository.bounded_blob_bytes_read();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("contextdb-blob-store")
            .build()
            .map_err(|e| other("store runtime", e))?;
        // `MemStore::from_sender` is Iroh's public constructor for a Store API
        // backed by an external command actor. Only the client wrapper is used;
        // ContextDB owns and services the receiver below from its Redb actor.
        let (commands_tx, commands) = tokio::sync::mpsc::channel(64);
        let store = Store::from(iroh_blobs::store::mem::MemStore::from_sender(
            commands_tx.into(),
        ));
        rt.handle()
            .spawn(run_contextdb_store_actor(repository.clone(), commands));
        let handle = BlobStoreHandle {
            inner: Arc::new(StoreRuntime {
                rt: Some(rt),
                store,
            }),
            repository,
            bytes_hydrated_while_opening: 0,
            tamper: Arc::new(Mutex::new(HashMap::new())),
            drop_after: Arc::new(Mutex::new(None)),
            serve_permits: Arc::new(tokio::sync::Semaphore::new(SERVE_CONCURRENCY)),
            fetch_requests_received: Arc::new(AtomicU64::new(0)),
            payload_bytes_emitted: Arc::new(AtomicU64::new(0)),
        };
        let bytes_after_open = handle.repository.bounded_blob_bytes_read();
        let mut handle = handle;
        handle.bytes_hydrated_while_opening = bytes_after_open.saturating_sub(bytes_before_open);
        Ok(handle)
    }

    /// Run a store future to completion from a SYNC caller on the dedicated
    /// runtime (never the caller's), so this works under a current-thread
    /// caller runtime.
    fn block_on_store<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Store) -> Fut + Send + 'static,
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

    /// Retained as the single-source servable-set predicate (mirrors the
    /// inline check in `serve_stream`); a future caller (e.g. an operator
    /// `has()` surface) can reuse it instead of re-deriving the check.
    #[allow(dead_code)]
    fn is_servable(&self, hash: &Hash) -> bool {
        self.repository
            .has_tag_role(hash.as_bytes(), BlobTagRole::Servable)
            .unwrap_or(false)
    }

    /// Content-address `bytes` into the store, tag them servable, return the
    /// BLAKE3 name. Idempotent: identical bytes converge on one blob.
    pub(crate) fn ingest_bytes(&self, bytes: &[u8]) -> Result<[u8; 32]> {
        let data = bytes.to_vec();
        let hash = self.block_on_store(move |store| async move {
            let tag = store
                .add_bytes(data)
                .temp_tag()
                .await
                .map_err(|e| other("add bytes", e))?;
            let hash = tag.hash();
            store
                .tags()
                .set(servable_tag(&hash), hash)
                .await
                .map_err(|e| other("tag blob", e))?;
            Ok(from_backend_hash(&hash))
        })?;
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
                .temp_tag()
                .await
                .map_err(|e| other("add path", e))?;
            let hash = tag.hash();
            store
                .tags()
                .set(servable_tag(&hash), hash)
                .await
                .map_err(|e| other("tag blob", e))?;
            Ok(from_backend_hash(&hash))
        })?;
        Ok(hash)
    }

    /// Harness seam: serve `bytes` (their OWN valid Bao encoding) whenever
    /// `claimed` is requested — the consumer's verification against `claimed`
    /// then fails, which is the point.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn arm_tamper(&self, claimed: &[u8; 32], bytes: &[u8]) -> Result<()> {
        let data = bytes.to_vec();
        let substitute = self.block_on_store(move |store| async move {
            let tag = store
                .add_bytes(data)
                .temp_tag()
                .await
                .map_err(|e| other("add tamper bytes", e))?;
            let hash = tag.hash();
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
        Ok(())
    }

    /// Harness seam: reset the next authorized, servable GET once after `n`
    /// payload bytes.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn arm_drop_after(&self, n: u64) {
        *self.drop_after.lock().expect("drop lock") = Some(n);
    }

    /// Test seam: the exact PAYLOAD bytes this holder has served across
    /// every successfully completed `handle_get`. Never counts refused or
    /// aborted transfers.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn payload_bytes_emitted(&self) -> u64 {
        self.payload_bytes_emitted.load(Ordering::SeqCst)
    }

    /// Test seam: count of blob-door requests whose `GetRequest` was read
    /// and authorized, INCLUDING refused ones (a reset still counts).
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn fetch_requests_received(&self) -> u64 {
        self.fetch_requests_received.load(Ordering::SeqCst)
    }

    /// Test seam: how many blobs this node currently serves (mirrors the
    /// protection tags). Lets a test observe reclaim without doing it.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn servable_count(&self) -> usize {
        self.repository
            .count_tag_role(BlobTagRole::Servable)
            .expect("canonical servable tag count") as usize
    }

    /// Test seam: how many consumer-side `fetch/` protection tags currently
    /// exist in the store. A fully-delivered fetch must leave ZERO (the leak
    /// this guards against); an aborted-not-yet-resumed fetch leaves one.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn fetch_tag_count(&self) -> usize {
        self.repository
            .count_tag_role(BlobTagRole::FetchProtection)
            .expect("canonical fetch-protection tag count") as usize
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
        let mut cursor = None;
        loop {
            let page = self.repository.list_tags_page(cursor.as_deref(), 256)?;
            if page.is_empty() {
                break;
            }
            for tag in &page {
                if !eligible(&tag.hash)? {
                    continue;
                }
                let mut end = tag.name.clone();
                end.push(0);
                self.repository.delete_tags(Some(&tag.name), Some(&end))?;
                if self.repository.delete_ordinary(tag.hash, false)? {
                    reclaimed = reclaimed.saturating_add(1);
                }
            }
            cursor = page.last().map(|tag| tag.name.clone());
        }
        let mut staging_cursor = None;
        loop {
            let page = self
                .repository
                .list_provisional_staging_page(staging_cursor.as_ref(), 256)?;
            if page.is_empty() {
                break;
            }
            for (object_id, hash) in &page {
                if eligible(hash)?
                    && self
                        .repository
                        .delete_provisional_staging(*hash, *object_id)?
                {
                    reclaimed = reclaimed.saturating_add(1);
                }
            }
            staging_cursor = page.last().map(|(object_id, _)| *object_id);
        }
        Ok(reclaimed)
    }

    /// Release the consumer-side fetch-protection tag once a fetch has fully
    /// delivered its verified bytes to the caller's sink: the store copy is
    /// then a transient cache and must not outlive delivery, or every blob a
    /// node ever fetches would pin disk forever.
    async fn release_fetch_tag(&self, backend: Hash) -> Result<()> {
        let complete = self
            .repository
            .open_read(from_backend_hash(&backend))?
            .is_some_and(|session| session.manifest().state == BlobManifestState::Complete);
        if !complete {
            return Err(Error::Other(
                "media adapter: fetch protection cannot be released before canonical completion"
                    .to_string(),
            ));
        }
        let deleted = self
            .inner
            .store
            .tags()
            .delete(fetch_protect_tag(&backend))
            .await
            .map_err(|err| other("delete fetch-protection tag", err))?;
        if deleted == 0 {
            return Err(Error::Other(
                "media adapter: completed fetch lost its durable protection tag".to_string(),
            ));
        }
        self.repository
            .delete_ordinary(from_backend_hash(&backend), false)?;
        Ok(())
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
        let repository = self.repository.clone();
        let tamper = self.tamper.clone();
        let drop_after = self.drop_after.clone();
        let permits = self.serve_permits.clone();
        let fetch_requests = self.fetch_requests_received.clone();
        let payload_bytes = self.payload_bytes_emitted.clone();
        endpoint.register_connection_protocol(
            iroh_blobs::protocol::ALPN.to_vec(),
            Arc::new(move |peer: PeerConnection| {
                let store = store.clone();
                let repository = repository.clone();
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
                            &repository,
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

/// A SendStream wrapper that counts payload bytes and force-resets after one
/// armed request's threshold (the transient drop-after harness seam).
struct CountingSend<W> {
    inner: W,
    payload_sent: u64,
    reset_after: Option<u64>,
}

impl<W: SendStream> SendStream for CountingSend<W> {
    async fn send_bytes(&mut self, bytes: bytes::Bytes) -> std::io::Result<()> {
        if let Some(limit) = self.reset_after {
            let remaining = limit.saturating_sub(self.payload_sent);
            if remaining < bytes.len() as u64 {
                if remaining != 0 {
                    self.inner
                        .send_bytes(bytes.slice(..remaining as usize))
                        .await?;
                    self.payload_sent = self.payload_sent.saturating_add(remaining);
                }
                let _ = self.inner.reset(RESET_DROPPED.into());
                return Err(std::io::Error::other("drop-after harness reset"));
            }
        }
        let len = bytes.len() as u64;
        self.inner.send_bytes(bytes).await?;
        self.payload_sent = self.payload_sent.saturating_add(len);
        Ok(())
    }

    async fn send(&mut self, buf: &[u8]) -> std::io::Result<()> {
        // Upstream's Bao writer uses `send` only for the encoded size and
        // parent hashes; leaf payload is sent through `send_bytes`.  The
        // harness contract is a PAYLOAD-byte threshold, so framing and
        // outboard bytes must neither consume the budget nor trigger a reset.
        self.inner.send(buf).await?;
        Ok(())
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

/// Authorize one accepted stream against the ledger verdict, then delegate
/// the byte-serving to the unchanged upstream provider.
#[allow(clippy::too_many_arguments)]
async fn serve_stream<W, R>(
    store: &Store,
    repository: &Arc<BlobRepository>,
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
    let provider_guard = repository.open_read(requested).ok().flatten();
    let tampered = tamper.lock().expect("tamper lock").contains_key(&requested);
    let servable = repository
        .has_tag_role(&requested, BlobTagRole::Servable)
        .unwrap_or(false);
    if !tampered && (provider_guard.is_none() || !servable) {
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
    // Consume the transient fault only after both authorization and servable
    // existence succeed. A refused or missing request must not disarm the
    // next real payload-serving attempt.
    let reset_after = drop_after.lock().expect("drop lock").take();
    let send = CountingSend {
        inner: send,
        payload_sent: 0,
        reset_after,
    };
    let pair = StreamPair::new(connection_id, recv, send, EventSender::DEFAULT);
    let result = handle_get(pair, store.clone(), get).await;
    if result.is_ok() {
        // Event-free payload counting: never read from the handler's own
        // internal write stats — derive the served byte length from the
        // REQUEST that just completed, against the served hash's known
        // size. A full GET serves the whole blob; a ranges (resume-tail)
        // GET serves from its lowest requested chunk boundary to the end.
        if let Ok(BlobStatus::Complete { size }) = store.blobs().status(served_hash).await {
            let moved = served_bytes_for_request(&served_ranges, size);
            payload_bytes.fetch_add(moved, Ordering::SeqCst);
            // Same derived figure, now attributed to the authenticated peer
            // that asked for it — the per-peer receipt the aggregate counter
            // above cannot produce.
            served(remote_node_id, moved);
        }
    }
    drop(provider_guard);
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

/// The consumer half: preflight, fetch (verified, resumable), export.
pub(crate) struct FetchOutcome {
    pub bytes_written: u64,
}

/// The network-and-tag-bookkeeping phase of a fetch (dial + verify into the
/// LOCAL store), isolated from the caller's sink: `[fetch_verified_into_local_store]`
/// and `fetch_into_sink` below both drive this same body, so the two stay
/// identical apart from what happens to the bytes once they are locally
/// verified.
async fn fetch_into_local_store(
    store: &BlobStoreHandle,
    identity_path: &Path,
    ticket: &str,
    hash: &[u8; 32],
) -> std::result::Result<LocalFetchOutcome, FetchFailure> {
    // Fetch over the blob door, verified into the local store. There is no
    // separate preflight: the consumer dials the blob door directly, and a
    // refusal arrives as a typed stream-reset code the holder sends BEFORE
    // any payload byte (see `classify_get_error`). Upstream fetches only
    // what the local partial state is missing, so a retry after an abort
    // moves the tail, not the blob.
    let backend = to_backend_hash(hash);
    let peer = peer_connect(identity_path, ticket, iroh_blobs::protocol::ALPN)
        .await
        .map_err(|_| FetchFailure::Unreachable)?;
    let holder_node_id = peer.remote_node_id.clone();
    let moved = run_verified_fetch(store, &peer, backend, hash).await;
    if matches!(
        &moved,
        Err(FetchFailure::Aborted | FetchFailure::HashMismatch { .. })
    ) {
        // Upstream returns a decode/transport error as soon as its writer
        // half fails, while dropping that half is what tells our ImportBao
        // actor to bind its already verified extents. Wait for the real actor
        // task to finish before the resolver exports the durable prefix.
        let _ = store.inner.store.wait_idle().await;
    }
    peer.close().await;
    let moved = moved?;
    #[cfg(not(any(test, feature = "test-seams")))]
    let _ = moved;
    Ok(LocalFetchOutcome {
        #[cfg(any(test, feature = "test-seams"))]
        moved,
        holder_node_id,
    })
}

/// The result of [`fetch_into_local_store`]: the wire payload bytes moved
/// and the holder's transport-authenticated identity.
pub(crate) struct LocalFetchOutcome {
    #[cfg(any(test, feature = "test-seams"))]
    pub moved: u64,
    pub holder_node_id: String,
}

/// Owned/'static wrapper over [`fetch_into_local_store`]: every argument here
/// is owned rather than borrowed, so a caller (see `resolve_blob_ref`'s
/// declared-deadline wrap) can `tokio::spawn` this exact phase and bound the
/// JOIN with a timeout — a `&mut dyn Write` sink cannot itself cross a spawn
/// boundary, which is why the sink-touching second half (export) stays a
/// separate, unspawned step in every caller.
pub(crate) async fn fetch_verified_into_local_store(
    store: Arc<BlobStoreHandle>,
    identity_path: PathBuf,
    ticket: String,
    hash: [u8; 32],
) -> std::result::Result<LocalFetchOutcome, FetchFailure> {
    fetch_into_local_store(&store, &identity_path, &ticket, &hash).await
}

/// Release the consumer-side fetch-protection tag for `hash` once its bytes
/// have been fully, successfully exported to the caller. Split out of
/// `fetch_into_sink`'s tail so `resolve_blob_ref`'s spawned-and-joined path
/// (which performs its own, separately-awaited export) can call it too.
pub(crate) async fn release_fetch_protection(
    store: &BlobStoreHandle,
    hash: &[u8; 32],
) -> std::result::Result<(), FetchFailure> {
    store
        .release_fetch_tag(to_backend_hash(hash))
        .await
        .map_err(|_| FetchFailure::Aborted)
}

/// Complete-cache fast-path cleanup. A crash may leave FetchProtection on an
/// already complete object; roleless complete cache objects are transient and
/// are removed after export, while Servable ownership is preserved.
pub(crate) async fn release_complete_cache_ownership(
    store: &BlobStoreHandle,
    hash: &[u8; 32],
) -> std::result::Result<(), FetchFailure> {
    if store
        .repository
        .has_tag_role(hash, BlobTagRole::FetchProtection)
        .map_err(|_| FetchFailure::Aborted)?
    {
        return release_fetch_protection(store, hash).await;
    }
    if !store
        .repository
        .has_tag_role(hash, BlobTagRole::Servable)
        .map_err(|_| FetchFailure::Aborted)?
    {
        store
            .repository
            .delete_ordinary(*hash, false)
            .map_err(|_| FetchFailure::Aborted)?;
    }
    Ok(())
}

/// Preflight + fetch + export `hash` from the holder at `ticket` into
/// `sink`. `payload_moved` (when given) receives the wire payload bytes this
/// call actually transferred (the resume-leg counter); `holder_node_id` (when
/// given) receives the holder's transport-authenticated identity, so a caller
/// can key a transfer receipt on who actually served it rather than on a name
/// it supplied itself.
#[allow(clippy::too_many_arguments)]
#[cfg(any(test, feature = "test-seams"))]
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
    let outcome = match fetch_into_local_store(store, identity_path, ticket, hash).await {
        Ok(outcome) => outcome,
        Err(failure) => {
            if matches!(failure, FetchFailure::Aborted) {
                // The local store already committed whatever it verified
                // before the abort (bao writes verified chunks
                // incrementally). Export that verified partial prefix into
                // the caller's sink so a retry can resume from it, then
                // still report the abort. Refusals (Unentitled/Policy/
                // NotFound) never reach here — they leave the sink empty,
                // since no fetch ever ran.
                let _ = export_local_into_sink(store, hash, sink, export_offset, false).await;
            }
            return Err(failure);
        }
    };
    if let Some(slot) = holder_node_id {
        *slot = outcome.holder_node_id;
    }
    if let Some(counter) = payload_moved {
        *counter = outcome.moved;
    }
    // Export the verified blob from the local store into the caller's sink,
    // streaming in bounded chunks.
    let result = export_into_sink(store, hash, sink, export_offset).await?;
    // Fully delivered: cleanup is part of success. A caller must not observe
    // success while durable fetch protection (and therefore cache ownership)
    // remains because a storage mutation failed.
    release_fetch_protection(store, hash).await?;
    Ok(result)
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
    export_local_into_sink(store, hash, sink, offset, true).await
}

/// Export only the canonically verified contiguous prefix after an aborted
/// transfer. Success paths must use `export_into_sink`, which requires the
/// full canonical object to be Complete.
pub(crate) async fn export_verified_prefix_into_sink(
    store: &BlobStoreHandle,
    hash: &[u8; 32],
    sink: &mut (dyn Write + Send),
    offset: u64,
) -> std::result::Result<FetchOutcome, FetchFailure> {
    export_local_into_sink(store, hash, sink, offset, false).await
}

async fn export_local_into_sink(
    store: &BlobStoreHandle,
    hash: &[u8; 32],
    sink: &mut (dyn Write + Send),
    offset: u64,
    require_complete: bool,
) -> std::result::Result<FetchOutcome, FetchFailure> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let backend = to_backend_hash(hash);
    let expected_size = if require_complete {
        local_complete_size(store, hash)
            .await
            .ok_or(FetchFailure::Aborted)?
    } else {
        verified_local_bytes(store, hash)
            .await
            .map_err(|_| FetchFailure::Aborted)?
    };
    if offset > expected_size {
        return Err(FetchFailure::Aborted);
    }
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
    while offset.saturating_add(written) < expected_size {
        let remaining = expected_size.saturating_sub(offset.saturating_add(written));
        let take = usize::try_from(remaining.min(buf.len() as u64)).unwrap_or(buf.len());
        let n = reader
            .read(&mut buf[..take])
            .await
            .map_err(|_| FetchFailure::Aborted)?;
        if n == 0 {
            break;
        }
        sink.write_all(&buf[..n]).map_err(FetchFailure::SinkWrite)?;
        written += n as u64;
    }
    if offset.saturating_add(written) != expected_size {
        return Err(FetchFailure::Aborted);
    }
    Ok(FetchOutcome {
        bytes_written: written,
    })
}

/// The size of the verified LOCAL state for `hash` (the resume prefix), in
/// bytes: what a retry does not need to move again.
pub(crate) async fn verified_local_bytes(store: &BlobStoreHandle, hash: &[u8; 32]) -> Result<u64> {
    store
        .repository
        .open_read(*hash)?
        .map(|session| session.contiguous_payload_prefix())
        .transpose()
        .map(|prefix| prefix.unwrap_or(0))
}

/// Resume leg: VERIFY the caller's existing `sink` prefix against this
/// consumer's own locally verified partial for `hash` FIRST, before any
/// dial — a diverging prefix (the harness corrupts `sink` directly) must
/// never get laundered into a verified blob by a tail fetch. Only when the
/// local store has no repository session at all (a fresh consumer that never
/// ran a prior leg) is the prefix check skipped. An existing sparse partial
/// with a zero-byte contiguous prefix still rejects a supplied prefix before
/// dial; the holder's authorization is the boundary only for true absence.
/// Once the prefix checks out, dial the holder, reauthorize (a rogue
/// caller is reset exactly like any other unauthorized request, sink
/// unchanged), and fetch + export ONLY the still-missing tail, APPENDED to
/// `sink`. Returns the wire payload bytes moved on this leg.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) async fn resume_into_sink(
    store: &BlobStoreHandle,
    identity_path: &Path,
    ticket: &str,
    hash: &[u8; 32],
    sink: &mut Vec<u8>,
) -> std::result::Result<u64, FetchFailure> {
    let local_session = store
        .repository
        .open_read(*hash)
        .map_err(|_| FetchFailure::Aborted)?;
    if let Some(session) = local_session {
        let local_verified = session
            .contiguous_payload_prefix()
            .map_err(|_| FetchFailure::Aborted)?;
        if sink.len() as u64 > local_verified {
            return Err(FetchFailure::HashMismatch {
                got: *blake3::hash(sink.as_slice()).as_bytes(),
            });
        }
        let mut offset = 0usize;
        while offset < sink.len() {
            let take = (sink.len() - offset).min(EXPORT_CHUNK);
            let stored = session
                .read_payload_at(offset as u64, take)
                .map_err(|_| FetchFailure::Aborted)?;
            if stored.len() != take || stored.as_slice() != &sink[offset..offset + take] {
                return Err(FetchFailure::HashMismatch {
                    got: *blake3::hash(sink.as_slice()).as_bytes(),
                });
            }
            offset += take;
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
            if matches!(
                &failure,
                FetchFailure::Aborted | FetchFailure::HashMismatch { .. }
            ) {
                let _ = store.inner.store.wait_idle().await;
            }
            peer.close().await;
            return Err(failure);
        }
    };
    peer.close().await;
    let export_offset = sink.len() as u64;
    export_into_sink(store, hash, sink, export_offset).await?;
    // The resume completed and the full verified blob is in the caller's
    // sink; release the fetch-protection tag so the store copy is reclaimable.
    store
        .release_fetch_tag(backend)
        .await
        .map_err(|_| FetchFailure::Aborted)?;
    Ok(moved)
}
