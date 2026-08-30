//! Engine-owned content-addressed blob persistence.
//!
//! The repository deliberately contains no concrete transport types. The
//! server adapter translates transport commands into these bounded,
//! transport-neutral records.

use crate::persistence::RedbPersistence;
use contextdb_core::{Error, Lsn, Result};
use parking_lot::{Condvar, Mutex};
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(test)]
thread_local! {
    static AUTHORITATIVE_PURGE_BLOB_COMMIT_FAULT: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Payload and outboard identity follow a fixed verification block grain, so
/// an interrupted transfer never has to overwrite a partially occupied index.
pub const BLOB_LOGICAL_BLOCK_BYTES: usize = 16 * 1024;

/// No repository value may exceed one MiB. Outboard segments may use the
/// full bound; payload values remain at the verified block grain above.
pub const BLOB_VALUE_BYTES: usize = 1024 * 1024;

/// One repository write batch is bounded independently from item count.
pub const BLOB_WRITE_BATCH_BYTES: usize = 8 * BLOB_VALUE_BYTES;

const HASH_GENERATIONS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__blob_hash_generations");
const MANIFESTS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__blob_manifests");
const PAYLOAD_CHUNKS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__blob_payload_chunks");
const OUTBOARD_CHUNKS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__blob_outboard_chunks");
const PARTIAL_BITFIELDS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__blob_partial_bitfields");
const TAGS_BY_NAME_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__blob_tags_by_name");
const TAGS_BY_HASH_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__blob_tags_by_hash");
const IMPORT_STAGING_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__blob_import_staging");

const RECORD_VERSION: u8 = 1;

/// The durable identity of one active content generation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveBlobGeneration {
    pub generation: u64,
    pub object_id: [u8; 16],
}

/// The durable anti-resurrection fence for one content hash.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobHashGenerationState {
    pub version: u8,
    pub next_generation: u64,
    pub active: Option<ActiveBlobGeneration>,
    pub last_retired_generation: u64,
    pub last_purge_lsn: u64,
}

impl Default for BlobHashGenerationState {
    fn default() -> Self {
        Self {
            version: RECORD_VERSION,
            next_generation: 1,
            active: None,
            last_retired_generation: 0,
            last_purge_lsn: 0,
        }
    }
}

/// Whether the canonical generation is resumable or fully verified.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobManifestState {
    Partial,
    Complete,
}

/// The one canonical manifest for an active hash generation.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobManifest {
    pub version: u8,
    pub object_id: [u8; 16],
    pub format: u8,
    pub total_size: u64,
    pub outboard_size: u64,
    pub validated_size: u64,
    pub payload_chunk_count: u64,
    pub outboard_chunk_count: u64,
    pub state: BlobManifestState,
}

/// Durable partial-transfer state. The engine owns exact present-chunk sets;
/// the adapter owns only the opaque transport-resume bitfield codec.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobPartialState {
    pub version: u8,
    pub payload_indices: Vec<u8>,
    pub outboard_indices: Vec<u8>,
    pub adapter_bitfield: Vec<u8>,
}

/// A durable tag's addressability role.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobTagRole {
    Servable,
    FetchProtection,
}

/// The value stored in both tag indexes.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobTagRecord {
    pub version: u8,
    pub name: Vec<u8>,
    pub hash: [u8; 32],
    pub generation: u64,
    pub format: u8,
    pub role: BlobTagRole,
}

/// Unaddressable import state. Export never copies this record or its chunks.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobImportStaging {
    pub version: u8,
    pub object_id: [u8; 16],
    pub start_lsn: u64,
    pub expected_size: Option<u64>,
    pub received_size: u64,
    pub payload_chunk_count: u64,
    pub outboard_chunk_count: u64,
    pub payload_indices: Vec<u8>,
    pub outboard_indices: Vec<u8>,
    pub provisional_generation: Option<([u8; 32], u64)>,
    #[serde(default)]
    pub adapter_state: Vec<u8>,
}

/// Canonical metadata supplied when an unbound import learns its hash.
#[doc(hidden)]
pub struct BlobStagingBind<'a> {
    pub hash: [u8; 32],
    pub object_id: [u8; 16],
    pub format: u8,
    pub total_size: u64,
    pub outboard_size: u64,
    pub validated_size: u64,
    pub state: BlobManifestState,
    pub partial_state: Option<&'a BlobPartialState>,
    pub tags: &'a [BlobTagRecord],
    /// Replace the guarded active generation from staging (resume/import
    /// completion) instead of treating an existing generation as an
    /// idempotent no-op.
    pub replace_active: bool,
}

#[derive(Debug, Clone)]
enum BlobChunkSetWitness {
    Dense(u64),
    Exact(roaring::RoaringTreemap),
}

#[derive(Debug, Clone)]
struct BlobCanonicalObjectPurgeWitness {
    object_id: [u8; 16],
    payload: BlobChunkSetWitness,
    outboard: BlobChunkSetWitness,
    payload_size: u64,
    outboard_size: u64,
    state: BlobManifestState,
}

#[derive(Debug, Clone)]
struct BlobTagPurgeWitness {
    name: Vec<u8>,
    reverse_key: Vec<u8>,
    encoded: Vec<u8>,
}

#[derive(Debug, Clone)]
struct BlobStagingPurgeWitness {
    object_id: [u8; 16],
    encoded: Vec<u8>,
}

/// Fully validated, bounded metadata for the blob half of one authoritative
/// purge. Payload bytes and per-chunk keys are never projected: complete sets
/// stay scalar, partial sets stay compressed, and the shared Redb transaction
/// validates expected blocks before deleting each object prefix in bounded
/// key batches.
#[derive(Debug, Clone)]
pub(crate) struct BlobAuthoritativePurgeHashProjection {
    hash: [u8; 32],
    expected_generation: Option<Vec<u8>>,
    next_generation: Vec<u8>,
    manifest: Option<(Vec<u8>, Vec<u8>)>,
    partials: Vec<(Vec<u8>, Vec<u8>)>,
    canonical_object: Option<BlobCanonicalObjectPurgeWitness>,
    tags: Vec<BlobTagPurgeWitness>,
    staging: Vec<BlobStagingPurgeWitness>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BlobAuthoritativePurgeProjection {
    hashes: Vec<BlobAuthoritativePurgeHashProjection>,
}

#[derive(Default)]
struct HashFenceState {
    active_shared: usize,
    writer_active: bool,
    quiescing: bool,
    purge_waiters: usize,
}

#[derive(Default)]
struct BlobRepositoryOperationState {
    active: usize,
    closing: bool,
    closed: bool,
}

#[derive(Default)]
struct BlobRepositoryOperationGate {
    state: Mutex<BlobRepositoryOperationState>,
    changed: Condvar,
}

impl BlobRepositoryOperationGate {
    fn begin(self: &Arc<Self>) -> Result<BlobRepositoryOperationGuard> {
        let mut state = self.state.lock();
        if state.closing || state.closed {
            return Err(Error::Other("blob repository is closed".to_string()));
        }
        state.active = state.active.saturating_add(1);
        Ok(BlobRepositoryOperationGuard {
            gate: self.clone(),
            released: false,
        })
    }

    fn begin_close(&self) -> bool {
        let mut state = self.state.lock();
        loop {
            if state.closed {
                return false;
            }
            if !state.closing {
                state.closing = true;
                while state.active != 0 {
                    self.changed.wait(&mut state);
                }
                return true;
            }
            self.changed.wait(&mut state);
        }
    }

    fn finish_close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        state.closing = false;
        self.changed.notify_all();
    }

    fn release(&self) {
        let mut state = self.state.lock();
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.changed.notify_all();
        }
    }
}

struct BlobRepositoryOperationGuard {
    gate: Arc<BlobRepositoryOperationGate>,
    released: bool,
}

impl Drop for BlobRepositoryOperationGuard {
    fn drop(&mut self) {
        if !self.released {
            self.gate.release();
            self.released = true;
        }
    }
}

/// Process-local coordination for bounded readers, writers, and purge.
#[doc(hidden)]
#[derive(Default)]
pub struct BlobHashFenceRegistry {
    states: Mutex<HashMap<[u8; 32], HashFenceState>>,
    changed: Condvar,
    closed: AtomicBool,
}

impl BlobHashFenceRegistry {
    fn acquire_shared(
        self: &Arc<Self>,
        hash: [u8; 32],
        generation: u64,
        operation: BlobRepositoryOperationGuard,
    ) -> Result<BlobSharedGenerationGuard> {
        let mut states = self.states.lock();
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Other("blob repository is closed".to_string()));
            }
            let state = states.entry(hash).or_default();
            if !state.quiescing {
                state.active_shared = state.active_shared.saturating_add(1);
                return Ok(BlobSharedGenerationGuard {
                    registry: self.clone(),
                    hash,
                    generation,
                    writer: false,
                    _operation: operation,
                    released: false,
                });
            }
            self.changed.wait(&mut states);
        }
    }

    fn acquire_writer(
        self: &Arc<Self>,
        hash: [u8; 32],
        generation: u64,
        operation: BlobRepositoryOperationGuard,
    ) -> Result<BlobSharedGenerationGuard> {
        let mut states = self.states.lock();
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Other("blob repository is closed".to_string()));
            }
            let state = states.entry(hash).or_default();
            if !state.quiescing && !state.writer_active {
                state.writer_active = true;
                state.active_shared = state.active_shared.saturating_add(1);
                return Ok(BlobSharedGenerationGuard {
                    registry: self.clone(),
                    hash,
                    generation,
                    writer: true,
                    _operation: operation,
                    released: false,
                });
            }
            self.changed.wait(&mut states);
        }
    }

    fn acquire_shared_sorted(
        self: &Arc<Self>,
        generations: &BTreeMap<[u8; 32], u64>,
        operation: BlobRepositoryOperationGuard,
    ) -> Result<BlobSharedHashSetGuard> {
        let mut states = self.states.lock();
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Other("blob repository is closed".to_string()));
            }
            if generations
                .keys()
                .all(|hash| !states.get(hash).is_some_and(|state| state.quiescing))
            {
                for hash in generations.keys() {
                    let state = states.entry(*hash).or_default();
                    state.active_shared = state.active_shared.saturating_add(1);
                }
                return Ok(BlobSharedHashSetGuard {
                    registry: self.clone(),
                    generations: generations.clone(),
                    _operation: operation,
                    released: false,
                });
            }
            self.changed.wait(&mut states);
        }
    }

    /// Atomically mark the complete sorted set quiescing, then wait for every
    /// existing shared operation. A caller retry drops this entire guard and
    /// reacquires the full expanded union from the beginning.
    fn acquire_exclusive_sorted(
        self: &Arc<Self>,
        hashes: &BTreeSet<[u8; 32]>,
        operation: BlobRepositoryOperationGuard,
    ) -> Result<BlobExclusiveHashSetGuard> {
        let hashes = hashes.iter().copied().collect::<Vec<_>>();
        let mut states = self.states.lock();
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Other("blob repository is closed".to_string()));
            }
            if hashes
                .iter()
                .all(|hash| !states.get(hash).is_some_and(|state| state.quiescing))
            {
                for hash in &hashes {
                    states.entry(*hash).or_default().quiescing = true;
                }
                break;
            }
            self.changed.wait(&mut states);
        }

        for hash in &hashes {
            states.entry(*hash).or_default().purge_waiters = 1;
        }
        self.changed.notify_all();
        while hashes.iter().any(|hash| {
            states
                .get(hash)
                .is_some_and(|state| state.active_shared != 0)
        }) {
            self.changed.wait(&mut states);
        }
        for hash in &hashes {
            states.entry(*hash).or_default().purge_waiters = 0;
        }
        Ok(BlobExclusiveHashSetGuard {
            registry: self.clone(),
            hashes,
            _operation: operation,
            released: false,
        })
    }

    #[cfg(feature = "test-seams")]
    fn wait_until_purge_waits_for_hash_for_test(
        &self,
        hash: &[u8; 32],
        timeout: std::time::Duration,
    ) -> bool {
        let started = std::time::Instant::now();
        let mut states = self.states.lock();
        loop {
            if states
                .get(hash)
                .is_some_and(|state| state.purge_waiters != 0)
            {
                return true;
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return false;
            }
            self.changed.wait_for(&mut states, remaining);
        }
    }

    fn release_shared(&self, hash: [u8; 32], writer: bool) {
        let mut states = self.states.lock();
        if let Some(state) = states.get_mut(&hash) {
            state.active_shared = state.active_shared.saturating_sub(1);
            if writer {
                state.writer_active = false;
            }
            self.changed.notify_all();
        }
    }

    fn release_shared_set(&self, hashes: impl Iterator<Item = [u8; 32]>) {
        let mut states = self.states.lock();
        for hash in hashes {
            if let Some(state) = states.get_mut(&hash) {
                state.active_shared = state.active_shared.saturating_sub(1);
            }
        }
        states.retain(|_, state| {
            state.quiescing || state.active_shared != 0 || state.purge_waiters != 0
        });
        self.changed.notify_all();
    }

    fn release_exclusive(&self, hashes: &[[u8; 32]]) {
        let mut states = self.states.lock();
        for hash in hashes {
            if let Some(state) = states.get_mut(hash) {
                state.quiescing = false;
                state.purge_waiters = 0;
            }
        }
        states.retain(|_, state| {
            state.quiescing || state.active_shared != 0 || state.purge_waiters != 0
        });
        self.changed.notify_all();
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_all();
    }
}

/// A shared provider/writer lease carrying the generation it must revalidate.
#[doc(hidden)]
pub struct BlobSharedGenerationGuard {
    registry: Arc<BlobHashFenceRegistry>,
    hash: [u8; 32],
    generation: u64,
    writer: bool,
    _operation: BlobRepositoryOperationGuard,
    released: bool,
}

/// An atomically acquired sorted set of shared generation guards. Tag
/// writers use this instead of acquiring hashes one-by-one, which would
/// deadlock against purge after it quiesces the same complete set.
#[doc(hidden)]
pub struct BlobSharedHashSetGuard {
    registry: Arc<BlobHashFenceRegistry>,
    generations: BTreeMap<[u8; 32], u64>,
    _operation: BlobRepositoryOperationGuard,
    released: bool,
}

impl BlobSharedHashSetGuard {
    pub fn generation(&self, hash: &[u8; 32]) -> Option<u64> {
        self.generations.get(hash).copied()
    }
}

impl Drop for BlobSharedHashSetGuard {
    fn drop(&mut self) {
        if !self.released {
            self.registry
                .release_shared_set(self.generations.keys().copied());
            self.released = true;
        }
    }
}

impl BlobSharedGenerationGuard {
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

impl Drop for BlobSharedGenerationGuard {
    fn drop(&mut self) {
        if !self.released {
            self.registry.release_shared(self.hash, self.writer);
            self.released = true;
        }
    }
}

/// The full exclusive hash set held through durable commit and publication.
#[doc(hidden)]
pub struct BlobExclusiveHashSetGuard {
    registry: Arc<BlobHashFenceRegistry>,
    hashes: Vec<[u8; 32]>,
    _operation: BlobRepositoryOperationGuard,
    released: bool,
}

impl BlobExclusiveHashSetGuard {
    pub fn hashes(&self) -> &[[u8; 32]] {
        &self.hashes
    }
}

impl Drop for BlobExclusiveHashSetGuard {
    fn drop(&mut self) {
        if !self.released {
            self.registry.release_exclusive(&self.hashes);
            self.released = true;
        }
    }
}

/// A stable Redb MVCC view captured with the database snapshot tuple.
#[doc(hidden)]
pub struct BlobExportSnapshot {
    read: redb::ReadTransaction,
    _operation: BlobRepositoryOperationGuard,
}

/// A canonical blob view whose shared generation guard remains held for the
/// complete provider/export operation. Every bounded read revalidates the
/// manifest inside a fresh Redb snapshot; the guard prevents authoritative
/// purge from retiring that generation between those reads.
#[doc(hidden)]
pub struct BlobReadSession {
    repository: Arc<BlobRepository>,
    guard: BlobSharedGenerationGuard,
    manifest: BlobManifest,
    partial: Option<BlobPartialState>,
}

impl BlobReadSession {
    pub fn generation(&self) -> u64 {
        self.guard.generation()
    }

    pub fn manifest(&self) -> &BlobManifest {
        &self.manifest
    }

    pub fn partial(&self) -> Option<&BlobPartialState> {
        self.partial.as_ref()
    }

    /// Read at most one repository value from the payload. Large exports
    /// iterate this method and therefore never hydrate a whole blob.
    pub fn read_payload_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.repository.read_session_at(self, offset, len, false)
    }

    /// Read at most one repository value from the Bao outboard.
    pub fn read_outboard_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.repository.read_session_at(self, offset, len, true)
    }

    pub fn contiguous_payload_prefix(&self) -> Result<u64> {
        if self.manifest.state == BlobManifestState::Complete {
            return Ok(self.manifest.total_size);
        }
        let partial = self
            .partial
            .as_ref()
            .ok_or_else(|| Error::Other("partial blob lost its exact state".to_string()))?;
        let indices = decode_chunk_indices(&partial.payload_indices)?;
        let block_bytes = BLOB_LOGICAL_BLOCK_BYTES as u64;
        let block_count = logical_block_count(self.manifest.total_size);
        let mut prefix = 0u64;
        for index in 0..block_count {
            if !indices.contains(index) {
                break;
            }
            let offset = index.saturating_mul(block_bytes);
            let expected = self
                .manifest
                .total_size
                .saturating_sub(offset)
                .min(block_bytes);
            let bytes = self.read_payload_at(offset, expected as usize)?;
            if bytes.len() as u64 != expected {
                return Err(Error::Other(
                    "partial blob exact state names a missing payload block".to_string(),
                ));
            }
            prefix = prefix.saturating_add(expected);
        }
        Ok(prefix)
    }
}

impl BlobTagRecord {
    pub fn new(
        name: Vec<u8>,
        hash: [u8; 32],
        generation: u64,
        format: u8,
        role: BlobTagRole,
    ) -> Self {
        Self {
            version: RECORD_VERSION,
            name,
            hash,
            generation,
            format,
            role,
        }
    }
}

impl BlobPartialState {
    pub fn from_exact_indices(
        payload_indices: impl IntoIterator<Item = u64>,
        outboard_indices: impl IntoIterator<Item = u64>,
        adapter_bitfield: Vec<u8>,
    ) -> Result<Self> {
        new_partial_state(payload_indices, outboard_indices, adapter_bitfield)
    }
}

/// One engine-owned blob repository.
#[doc(hidden)]
pub struct BlobRepository {
    /// Absent when the owning handle reads a committed image whose source
    /// file is already closed. Media lives in the durable store, so an image
    /// that outlives its file carries none, and every media operation says so
    /// rather than inventing a store to hold it.
    persistence: Option<Arc<RedbPersistence>>,
    ephemeral_dir: Option<Arc<tempfile::TempDir>>,
    fences: Arc<BlobHashFenceRegistry>,
    operation_gate: Arc<BlobRepositoryOperationGate>,
    bounded_blob_bytes_read: AtomicU64,
}

impl BlobRepository {
    /// A repository for a handle that has no durable store to hold media.
    pub fn absent() -> Arc<Self> {
        Arc::new(Self {
            persistence: None,
            ephemeral_dir: None,
            fences: Arc::new(BlobHashFenceRegistry::default()),
            operation_gate: Arc::new(BlobRepositoryOperationGate::default()),
            bounded_blob_bytes_read: AtomicU64::new(0),
        })
    }

    /// The durable store behind this repository, or the refusal that says
    /// this handle never had one.
    fn store(&self) -> Result<&RedbPersistence> {
        self.persistence.as_deref().ok_or_else(|| {
            Error::Other(
                "this handle reads a committed image whose source is released, so it holds no media repository"
                    .to_string(),
            )
        })
    }

    /// Attach blob tables to an existing file-backed database.
    pub fn shared(persistence: Arc<RedbPersistence>) -> Arc<Self> {
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_blob_repository_open();
        Arc::new(Self {
            persistence: Some(persistence),
            ephemeral_dir: None,
            fences: Arc::new(BlobHashFenceRegistry::default()),
            operation_gate: Arc::new(BlobRepositoryOperationGate::default()),
            bounded_blob_bytes_read: AtomicU64::new(0),
        })
    }

    /// Create a disk-bounded ephemeral repository for `Database::open_memory`.
    pub fn ephemeral() -> Result<Arc<Self>> {
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_blob_repository_open();
        let dir = Arc::new(
            tempfile::tempdir()
                .map_err(|err| Error::Other(format!("create ephemeral blob repository: {err}")))?,
        );
        let persistence = Arc::new(RedbPersistence::create(
            &dir.path().join("contextdb-blobs.redb"),
        )?);
        Ok(Arc::new(Self {
            persistence: Some(persistence),
            ephemeral_dir: Some(dir),
            fences: Arc::new(BlobHashFenceRegistry::default()),
            operation_gate: Arc::new(BlobRepositoryOperationGate::default()),
            bounded_blob_bytes_read: AtomicU64::new(0),
        }))
    }

    fn begin_operation(&self) -> Result<BlobRepositoryOperationGuard> {
        self.operation_gate.begin()
    }

    pub fn bounded_blob_bytes_read(&self) -> u64 {
        self.bounded_blob_bytes_read.load(Ordering::Relaxed)
    }

    #[cfg(feature = "test-seams")]
    pub fn wait_until_authoritative_purge_waits_for_hash_for_test(
        &self,
        hash: &[u8; 32],
        timeout: std::time::Duration,
    ) -> bool {
        self.fences
            .wait_until_purge_waits_for_hash_for_test(hash, timeout)
    }

    /// Load the durable generation fence without hydrating blob data.
    pub fn generation_state(&self, hash: &[u8; 32]) -> Result<BlobHashGenerationState> {
        let _operation = self.begin_operation()?;
        self.generation_state_under_operation(hash)
    }

    fn generation_state_under_operation(&self, hash: &[u8; 32]) -> Result<BlobHashGenerationState> {
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(HASH_GENERATIONS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok(BlobHashGenerationState::default());
                }
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            let Some(value) = table
                .get(hash.as_slice())
                .map_err(RedbPersistence::storage_error)?
            else {
                return Ok(BlobHashGenerationState::default());
            };
            decode(value.value())
        })
    }

    /// Acquire a real shared generation and revalidate after waiting for any
    /// quiescing purge to finish.
    pub fn begin_shared_generation(
        self: &Arc<Self>,
        hash: [u8; 32],
    ) -> Result<BlobSharedGenerationGuard> {
        let operation = self.begin_operation()?;
        let initial = self
            .generation_state_under_operation(&hash)?
            .active
            .ok_or_else(|| Error::Other("blob generation is not active".to_string()))?;
        let guard = self
            .fences
            .acquire_shared(hash, initial.generation, operation)?;
        let current = self.generation_state_under_operation(&hash)?.active;
        if current != Some(initial) {
            return Err(Error::Other(
                "blob generation changed while acquiring its shared guard".to_string(),
            ));
        }
        Ok(guard)
    }

    /// Acquire the generation a writer must use. A hash without an active
    /// manifest leases `next_generation`; an idempotent existing hash leases
    /// its current active generation.
    pub fn begin_write_generation(
        self: &Arc<Self>,
        hash: [u8; 32],
    ) -> Result<BlobSharedGenerationGuard> {
        let operation = self.begin_operation()?;
        let initial = self.generation_state_under_operation(&hash)?;
        let generation = initial
            .active
            .map(|active| active.generation)
            .unwrap_or(initial.next_generation);
        let guard = self.fences.acquire_writer(hash, generation, operation)?;
        let current = self.generation_state_under_operation(&hash)?;
        let current_generation = current
            .active
            .map(|active| active.generation)
            .unwrap_or(current.next_generation);
        if generation != current_generation {
            return Err(Error::Other(
                "blob write generation changed while acquiring its shared guard".to_string(),
            ));
        }
        Ok(guard)
    }

    fn begin_shared_generation_set(
        self: &Arc<Self>,
        hashes: &BTreeSet<[u8; 32]>,
    ) -> Result<BlobSharedHashSetGuard> {
        loop {
            let operation = self.begin_operation()?;
            let mut generations = BTreeMap::new();
            for hash in hashes {
                let active = self
                    .generation_state_under_operation(hash)?
                    .active
                    .ok_or_else(|| Error::Other("blob tag owner is not active".to_string()))?;
                generations.insert(*hash, active.generation);
            }
            let guard = self.fences.acquire_shared_sorted(&generations, operation)?;
            let stable = generations.iter().all(|(hash, generation)| {
                self.generation_state_under_operation(hash)
                    .ok()
                    .and_then(|state| state.active)
                    .is_some_and(|active| active.generation == *generation)
            });
            if stable {
                return Ok(guard);
            }
            drop(guard);
        }
    }

    /// Acquire the complete sorted candidate set before the database commit
    /// mutex. A retry drops this guard and reacquires the expanded union.
    pub fn acquire_exclusive_sorted(
        self: &Arc<Self>,
        hashes: &BTreeSet<[u8; 32]>,
    ) -> Result<BlobExclusiveHashSetGuard> {
        let operation = self.begin_operation()?;
        self.fences.acquire_exclusive_sorted(hashes, operation)
    }

    /// Ordinary cache/reclaim deletion. This retires the active generation
    /// monotonically but deliberately does not advance the authoritative
    /// PURGE frontier. `force == false` preserves any canonically tagged
    /// generation; `force == true` removes both tag indexes as well.
    pub fn delete_ordinary(self: &Arc<Self>, hash: [u8; 32], force: bool) -> Result<bool> {
        let hashes = BTreeSet::from([hash]);
        let guard = self.acquire_exclusive_sorted(&hashes)?;
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let mut state = {
                let table = write
                    .open_table(HASH_GENERATIONS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .get(hash.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| decode::<BlobHashGenerationState>(value.value()))
                    .transpose()?
                    .unwrap_or_default()
            };
            let Some(active) = state.active else {
                write.commit().map_err(RedbPersistence::storage_error)?;
                return Ok(false);
            };
            if guard.hashes() != [hash] {
                return Err(Error::Other(
                    "ordinary blob delete lost its exclusive hash fence".to_string(),
                ));
            }
            let manifest_key = Self::manifest_key(&hash, active.generation);
            let manifest = {
                let manifests = write
                    .open_table(MANIFESTS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                manifests
                    .get(manifest_key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| decode::<BlobManifest>(value.value()))
                    .transpose()?
                    .ok_or_else(|| {
                        Error::Other("ordinary blob delete lost its manifest".to_string())
                    })?
            };
            let tags = {
                let by_hash = write
                    .open_table(TAGS_BY_HASH_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                let mut tags = Vec::new();
                for entry in by_hash
                    .range(manifest_key.as_slice()..)
                    .map_err(RedbPersistence::storage_error)?
                    .take(257)
                {
                    let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                    if !key.value().starts_with(manifest_key.as_slice()) {
                        break;
                    }
                    tags.push(decode::<BlobTagRecord>(value.value())?);
                }
                tags
            };
            if !force && !tags.is_empty() {
                write.commit().map_err(RedbPersistence::storage_error)?;
                return Ok(false);
            }
            if tags.len() > 256 {
                return Err(Error::Other(
                    "ordinary blob deletion requires bounded prior tag cleanup".to_string(),
                ));
            }
            for tag in tags {
                let mut reverse = manifest_key.to_vec();
                reverse.extend_from_slice(&tag.name);
                {
                    let mut by_name = write
                        .open_table(TAGS_BY_NAME_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    by_name
                        .remove(tag.name.as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                }
                {
                    let mut by_hash = write
                        .open_table(TAGS_BY_HASH_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    by_hash
                        .remove(reverse.as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                }
            }
            remove_object_chunks_in_write(&write, PAYLOAD_CHUNKS_TABLE, &manifest.object_id)?;
            remove_object_chunks_in_write(&write, OUTBOARD_CHUNKS_TABLE, &manifest.object_id)?;
            {
                let mut manifests = write
                    .open_table(MANIFESTS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                manifests
                    .remove(manifest_key.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            {
                let mut partials = write
                    .open_table(PARTIAL_BITFIELDS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                partials
                    .remove(manifest_key.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            state.active = None;
            state.last_retired_generation = state.last_retired_generation.max(active.generation);
            state.next_generation = state
                .next_generation
                .max(active.generation.saturating_add(1));
            let encoded = encode(&state)?;
            {
                let mut generations = write
                    .open_table(HASH_GENERATIONS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                generations
                    .insert(hash.as_slice(), encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)?;
            Ok(true)
        })
    }

    /// Create staging already owned by a known hash generation. The durable
    /// association and its current purge frontier are committed before any
    /// payload or outboard byte may be written.
    pub fn begin_import_staging_for_generation(
        &self,
        guard: &BlobSharedGenerationGuard,
        expected_size: Option<u64>,
        adapter_state: Vec<u8>,
    ) -> Result<BlobImportStaging> {
        if !Arc::ptr_eq(&guard.registry, &self.fences) || !guard.writer {
            return Err(Error::Other(
                "blob staging creation requires this repository's exclusive writer lease"
                    .to_string(),
            ));
        }
        if adapter_state.len() > BLOB_VALUE_BYTES {
            return Err(Error::Other(
                "blob adapter staging state exceeds its bounded value".to_string(),
            ));
        }
        let _operation = self.begin_operation()?;
        let state = self.generation_state_under_operation(&guard.hash)?;
        let current_generation = state
            .active
            .map(|active| active.generation)
            .unwrap_or(state.next_generation);
        if current_generation != guard.generation {
            return Err(Error::Other(
                "blob write generation changed before staging creation".to_string(),
            ));
        }
        let mut staging = new_import_staging(Lsn(state.last_purge_lsn), expected_size)?;
        staging.provisional_generation = Some((guard.hash, guard.generation));
        staging.adapter_state = adapter_state;
        let bytes = encode(&staging)?;
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(staging.object_id.as_slice(), bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })?;
        Ok(staging)
    }

    pub fn list_import_staging_for_hash(
        &self,
        hash: &[u8; 32],
        limit: usize,
    ) -> Result<Vec<BlobImportStaging>> {
        let _operation = self.begin_operation()?;
        let limit = limit.min(256);
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(IMPORT_STAGING_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            let mut found = Vec::new();
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (_, value) = entry.map_err(RedbPersistence::storage_error)?;
                let staging: BlobImportStaging = decode(value.value())?;
                if staging
                    .provisional_generation
                    .is_some_and(|(candidate, _)| candidate == *hash)
                {
                    found.push(staging);
                    if found.len() == limit {
                        break;
                    }
                }
            }
            Ok(found)
        })
    }

    /// Page hash-associated provisional staging by durable object id without
    /// hydrating any payload or outboard value.
    pub fn list_provisional_staging_page(
        &self,
        after: Option<&[u8; 16]>,
        limit: usize,
    ) -> Result<Vec<([u8; 16], [u8; 32])>> {
        let _operation = self.begin_operation()?;
        let limit = limit.min(256);
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(IMPORT_STAGING_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            let mut result = Vec::new();
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                let key = key.value();
                if after.is_some_and(|after| key <= after.as_slice()) {
                    continue;
                }
                let staging: BlobImportStaging = decode(value.value())?;
                let Some((hash, _)) = staging.provisional_generation else {
                    continue;
                };
                let object_id: [u8; 16] = key.try_into().map_err(|_| {
                    Error::Other("blob staging object id has invalid length".to_string())
                })?;
                result.push((object_id, hash));
                if result.len() == limit {
                    break;
                }
            }
            Ok(result)
        })
    }

    /// Reclaim one still-provisional object under the same exclusive hash
    /// fence used by purge. A live writer finishes or aborts before removal.
    pub fn delete_provisional_staging(
        self: &Arc<Self>,
        hash: [u8; 32],
        object_id: [u8; 16],
    ) -> Result<bool> {
        let guard = self.acquire_exclusive_sorted(&BTreeSet::from([hash]))?;
        if guard.hashes() != [hash] {
            return Err(Error::Other(
                "provisional staging reclaim lost its exclusive hash fence".to_string(),
            ));
        }
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let staging = {
                let table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .get(object_id.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| decode::<BlobImportStaging>(value.value()))
                    .transpose()?
            };
            let Some(staging) = staging else {
                write.commit().map_err(RedbPersistence::storage_error)?;
                return Ok(false);
            };
            if staging
                .provisional_generation
                .is_none_or(|(candidate, _)| candidate != hash)
            {
                write.commit().map_err(RedbPersistence::storage_error)?;
                return Ok(false);
            }
            remove_staging_in_write(&write, &staging)?;
            write.commit().map_err(RedbPersistence::storage_error)?;
            Ok(true)
        })
    }

    /// Discard this writer's prior provisional object before rebuilding the
    /// same hash from a complete trusted local source.
    pub fn discard_staging_for_writer(
        &self,
        guard: &BlobSharedGenerationGuard,
        object_id: [u8; 16],
    ) -> Result<bool> {
        if !Arc::ptr_eq(&guard.registry, &self.fences) || !guard.writer {
            return Err(Error::Other(
                "blob staging discard requires this repository's exclusive writer lease"
                    .to_string(),
            ));
        }
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let staging = {
                let table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .get(object_id.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| decode::<BlobImportStaging>(value.value()))
                    .transpose()?
            };
            let Some(staging) = staging else {
                write.commit().map_err(RedbPersistence::storage_error)?;
                return Ok(false);
            };
            if staging.provisional_generation != Some((guard.hash, guard.generation)) {
                return Err(Error::Other(
                    "blob writer cannot discard another generation's staging".to_string(),
                ));
            }
            remove_staging_in_write(&write, &staging)?;
            write.commit().map_err(RedbPersistence::storage_error)?;
            Ok(true)
        })
    }

    /// Append one bounded, already-associated payload chunk.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn append_staging_payload(
        &self,
        object_id: [u8; 16],
        index: u64,
        bytes: &[u8],
    ) -> Result<BlobImportStaging> {
        self.append_staging_chunk(object_id, index, bytes, false)
    }

    /// Persist one verified fragment into a bounded staging block. This is
    /// used for sparse Bao parent pairs: the physical block is durable after
    /// every decoder item, while the adapter's exact pair-validity mask stays
    /// in `BlobPartialState` and never treats zero fill as verified.
    #[allow(
        clippy::too_many_arguments,
        reason = "one fragment checkpoint must validate its writer lease, byte range, and before/after adapter state in the same durable operation"
    )]
    pub fn checkpoint_staging_fragment(
        &self,
        guard: &BlobSharedGenerationGuard,
        object_id: [u8; 16],
        index: u64,
        within: usize,
        bytes: &[u8],
        logical_size: u64,
        outboard: bool,
        expected_adapter_state: &[u8],
        adapter_state: Vec<u8>,
    ) -> Result<BlobImportStaging> {
        if !Arc::ptr_eq(&guard.registry, &self.fences) || !guard.writer {
            return Err(Error::Other(
                "blob staging checkpoint requires this repository's exclusive writer lease"
                    .to_string(),
            ));
        }
        if adapter_state.len() > BLOB_VALUE_BYTES {
            return Err(Error::Other(
                "blob adapter staging state exceeds its bounded value".to_string(),
            ));
        }
        let _operation = self.begin_operation()?;
        if bytes.is_empty() || bytes.len() > BLOB_LOGICAL_BLOCK_BYTES {
            return Err(Error::Other(
                "blob staging fragment violates the logical block bound".to_string(),
            ));
        }
        let block_offset = index.saturating_mul(BLOB_LOGICAL_BLOCK_BYTES as u64);
        let expected = logical_size
            .saturating_sub(block_offset)
            .min(BLOB_LOGICAL_BLOCK_BYTES as u64) as usize;
        if expected == 0 || within.saturating_add(bytes.len()) > expected {
            return Err(Error::Other(
                "blob staging fragment is outside its declared logical block".to_string(),
            ));
        }
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let mut staging = {
                let table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                decode::<BlobImportStaging>(
                    table
                        .get(object_id.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .ok_or_else(|| Error::Other("blob staging object is missing".to_string()))?
                        .value(),
                )?
            };
            if staging.provisional_generation != Some((guard.hash, guard.generation)) {
                return Err(Error::Other(
                    "blob staging fragment lost its durable hash-generation association"
                        .to_string(),
                ));
            }
            if staging.adapter_state != expected_adapter_state {
                return Err(Error::Other(
                    "blob staging adapter state changed before its atomic fragment checkpoint"
                        .to_string(),
                ));
            }
            let encoded_indices = if outboard {
                &staging.outboard_indices
            } else {
                &staging.payload_indices
            };
            let mut indices = decode_chunk_indices(encoded_indices)?;
            let first_write = indices.insert(index);
            let key = Self::chunk_key(&object_id, index);
            let definition = if outboard {
                OUTBOARD_CHUNKS_TABLE
            } else {
                PAYLOAD_CHUNKS_TABLE
            };
            let mut value = {
                let table = write
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| value.value().to_vec())
                    .unwrap_or_else(|| vec![0; expected])
            };
            if value.len() != expected {
                return Err(Error::Other(
                    "blob staging fragment found a malformed physical block".to_string(),
                ));
            }
            value[within..within + bytes.len()].copy_from_slice(bytes);
            {
                let mut table = write
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            let encoded_indices = encode_chunk_indices(&indices)?;
            if outboard {
                staging.outboard_indices = encoded_indices;
                staging.outboard_chunk_count = indices.len();
            } else {
                staging.payload_indices = encoded_indices;
                staging.payload_chunk_count = indices.len();
                if first_write {
                    staging.received_size = staging.received_size.saturating_add(expected as u64);
                }
            }
            staging.adapter_state = adapter_state;
            let encoded = encode(&staging)?;
            {
                let mut table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(object_id.as_slice(), encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)?;
            Ok(staging)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn append_staging_chunk(
        &self,
        object_id: [u8; 16],
        index: u64,
        bytes: &[u8],
        outboard: bool,
    ) -> Result<BlobImportStaging> {
        let _operation = self.begin_operation()?;
        let value_bound = BLOB_LOGICAL_BLOCK_BYTES;
        if bytes.len() > value_bound {
            return Err(Error::Other(
                "blob staging value exceeds its logical block bound".to_string(),
            ));
        }
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let mut staging = {
                let table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                let value = table
                    .get(object_id.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::Other("blob staging object is missing".to_string()))?;
                decode::<BlobImportStaging>(value.value())?
            };
            if staging.provisional_generation.is_none() {
                return Err(Error::Other(
                    "blob bytes cannot be written before durable hash-generation association"
                        .to_string(),
                ));
            }
            let encoded_indices = if outboard {
                &staging.outboard_indices
            } else {
                &staging.payload_indices
            };
            let mut indices = decode_chunk_indices(encoded_indices)?;
            if !indices.insert(index) {
                return Err(Error::Other(format!(
                    "blob staging chunk index {index} was already occupied"
                )));
            }
            let key = Self::chunk_key(&object_id, index);
            {
                let definition = if outboard {
                    OUTBOARD_CHUNKS_TABLE
                } else {
                    PAYLOAD_CHUNKS_TABLE
                };
                let mut table = write
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                let previous = table
                    .insert(key.as_slice(), bytes)
                    .map_err(RedbPersistence::storage_error)?;
                if previous.is_some() {
                    return Err(Error::Other(
                        "blob staging chunk key already exists".to_string(),
                    ));
                }
            }
            let encoded_indices = encode_chunk_indices(&indices)?;
            if outboard {
                staging.outboard_chunk_count = indices.len();
                staging.outboard_indices = encoded_indices;
            } else {
                staging.payload_chunk_count = indices.len();
                staging.payload_indices = encoded_indices;
                staging.received_size = staging
                    .received_size
                    .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
            }
            let encoded = encode(&staging)?;
            {
                let mut table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(object_id.as_slice(), encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)?;
            Ok(staging)
        })
    }

    /// Compare the staging frontier, allocate/reuse its generation, and bind
    /// the canonical manifest and tags in one Redb transaction. The caller
    /// holds this hash's shared writer guard and the database commit mutex.
    /// A pre-frontier staging refusal commits synchronous cleanup before the
    /// purge/generation error is returned.
    pub fn bind_staging(
        &self,
        guard: &BlobSharedGenerationGuard,
        bind: BlobStagingBind<'_>,
    ) -> Result<ActiveBlobGeneration> {
        // The writer guard retains the repository operation lease through
        // this complete transaction, including a close racing this call.
        if !Arc::ptr_eq(&guard.registry, &self.fences) || guard.hash() != bind.hash || !guard.writer
        {
            return Err(Error::Other(
                "blob staging bind requires this repository's exclusive hash writer".to_string(),
            ));
        }
        enum BindOutcome {
            Bound(ActiveBlobGeneration),
            RefusedByPurge,
            StaleGeneration,
        }
        let outcome = self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let staging = {
                let table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                let value = table
                    .get(bind.object_id.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::Other("blob staging object is missing".to_string()))?;
                decode::<BlobImportStaging>(value.value())?
            };
            let mut generation_state = {
                let table = write
                    .open_table(HASH_GENERATIONS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                match table
                    .get(bind.hash.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                {
                    Some(value) => decode::<BlobHashGenerationState>(value.value())?,
                    None => BlobHashGenerationState::default(),
                }
            };
            let outcome = if generation_state.last_purge_lsn > staging.start_lsn {
                remove_staging_in_write(&write, &staging)?;
                BindOutcome::RefusedByPurge
            } else {
                let staging_payload_indices = decode_chunk_indices(&staging.payload_indices)?;
                let staging_outboard_indices = decode_chunk_indices(&staging.outboard_indices)?;
                let verified_payload_bytes = validate_staged_blocks_in_write(
                    &write,
                    PAYLOAD_CHUNKS_TABLE,
                    &staging.object_id,
                    &staging_payload_indices,
                    bind.total_size,
                    "payload",
                )?;
                validate_staged_blocks_in_write(
                    &write,
                    OUTBOARD_CHUNKS_TABLE,
                    &staging.object_id,
                    &staging_outboard_indices,
                    bind.outboard_size,
                    "outboard",
                )?;
                validate_staging_bind_metadata(
                    &staging,
                    &bind,
                    guard.generation(),
                    &staging_payload_indices,
                    &staging_outboard_indices,
                    verified_payload_bytes,
                )?;
                match (bind.state, bind.partial_state) {
                    (BlobManifestState::Partial, Some(partial)) => {
                        let (payload_indices, outboard_indices) = validate_partial_state(
                            partial,
                            staging.payload_chunk_count,
                            staging.outboard_chunk_count,
                        )?;
                        if payload_indices != staging_payload_indices
                            || outboard_indices != staging_outboard_indices
                        {
                            return Err(Error::Other(
                                "canonical partial state does not match the staged exact chunk sets"
                                    .to_string(),
                            ));
                        }
                    }
                    (BlobManifestState::Partial, None) => {
                        return Err(Error::Other(
                            "partial blob bind is missing its durable exact chunk sets and adapter bitfield"
                                .to_string(),
                        ));
                    }
                    (BlobManifestState::Complete, Some(_)) => {
                        return Err(Error::Other(
                            "complete blob bind unexpectedly carries partial transfer state"
                                .to_string(),
                        ));
                    }
                    (BlobManifestState::Complete, None) => {
                        validate_complete_block_count(
                            bind.total_size,
                            staging.payload_chunk_count,
                            "payload",
                        )?;
                        validate_complete_block_count(
                            bind.outboard_size,
                            staging.outboard_chunk_count,
                            "outboard",
                        )?;
                        validate_dense_indices(
                            &staging_payload_indices,
                            staging.payload_chunk_count,
                            "payload",
                        )?;
                        validate_dense_indices(
                            &staging_outboard_indices,
                            staging.outboard_chunk_count,
                            "outboard",
                        )?;
                    }
                }

                if let Some(active) = generation_state.active
                    && !bind.replace_active
                {
                    remove_staging_in_write(&write, &staging)?;
                    if active.generation == guard.generation() {
                        BindOutcome::Bound(active)
                    } else {
                        BindOutcome::StaleGeneration
                    }
                } else if generation_state.active.is_some_and(|active| {
                    active.generation != guard.generation()
                }) || generation_state.active.is_none()
                    && generation_state.next_generation != guard.generation()
                {
                    remove_staging_in_write(&write, &staging)?;
                    BindOutcome::StaleGeneration
                } else {
                    let replaced = generation_state.active;
                    let active = ActiveBlobGeneration {
                        generation: guard.generation(),
                        object_id: bind.object_id,
                    };
                    let mut tag_names = HashSet::new();
                    for tag in bind.tags {
                        if tag.version != RECORD_VERSION
                            || tag.hash != bind.hash
                            || tag.generation != active.generation
                        {
                            return Err(Error::Other(
                                "blob staging tag does not match its bound generation".to_string(),
                            ));
                        }
                        if !tag_names.insert(tag.name.clone()) {
                            return Err(Error::Other(
                                "blob staging bind contains duplicate tag ownership".to_string(),
                            ));
                        }
                        if replaced.is_none() {
                            let by_name = write
                                .open_table(TAGS_BY_NAME_TABLE)
                                .map_err(RedbPersistence::storage_error)?;
                            if by_name
                                .get(tag.name.as_slice())
                                .map_err(RedbPersistence::storage_error)?
                                .is_some()
                            {
                                return Err(Error::Other(
                                    "blob staging bind conflicts with existing authoritative tag ownership"
                                        .to_string(),
                                ));
                            }
                        }
                        let mut reverse_key =
                            Self::manifest_key(&bind.hash, active.generation).to_vec();
                        reverse_key.extend_from_slice(&tag.name);
                        if replaced.is_none() {
                            let by_hash = write
                                .open_table(TAGS_BY_HASH_TABLE)
                                .map_err(RedbPersistence::storage_error)?;
                            if by_hash
                                .get(reverse_key.as_slice())
                                .map_err(RedbPersistence::storage_error)?
                                .is_some()
                            {
                                return Err(Error::Other(
                                    "blob staging bind conflicts with existing reverse tag ownership"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    if let Some(replaced) = replaced {
                        let old_key = Self::manifest_key(&bind.hash, replaced.generation);
                        let old_manifest = {
                            let manifests = write
                                .open_table(MANIFESTS_TABLE)
                                .map_err(RedbPersistence::storage_error)?;
                            manifests
                                .get(old_key.as_slice())
                                .map_err(RedbPersistence::storage_error)?
                                .map(|value| decode::<BlobManifest>(value.value()))
                                .transpose()?
                                .ok_or_else(|| {
                                    Error::Other(
                                        "blob replacement lost its prior canonical manifest"
                                            .to_string(),
                                    )
                                })?
                        };
                        if old_manifest.state == BlobManifestState::Complete
                            && bind.state == BlobManifestState::Partial
                        {
                            return Err(Error::Other(
                                "a complete canonical blob cannot regress to partial".to_string(),
                            ));
                        }
                        remove_object_chunks_in_write(
                            &write,
                            PAYLOAD_CHUNKS_TABLE,
                            &old_manifest.object_id,
                        )?;
                        remove_object_chunks_in_write(
                            &write,
                            OUTBOARD_CHUNKS_TABLE,
                            &old_manifest.object_id,
                        )?;
                        let mut partials = write
                            .open_table(PARTIAL_BITFIELDS_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        partials
                            .remove(old_key.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                let manifest = BlobManifest {
                    version: RECORD_VERSION,
                    object_id: bind.object_id,
                    format: bind.format,
                    total_size: bind.total_size,
                    outboard_size: bind.outboard_size,
                    validated_size: bind.validated_size,
                    payload_chunk_count: staging.payload_chunk_count,
                    outboard_chunk_count: staging.outboard_chunk_count,
                    state: bind.state,
                };
                let manifest_key = Self::manifest_key(&bind.hash, active.generation);
                let manifest_bytes = encode(&manifest)?;
                {
                    let mut manifests = write
                        .open_table(MANIFESTS_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    manifests
                        .insert(manifest_key.as_slice(), manifest_bytes.as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                }
                if let Some(partial) = bind.partial_state {
                    let partial_bytes = encode(partial)?;
                    let mut bitfields = write
                        .open_table(PARTIAL_BITFIELDS_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    bitfields
                        .insert(manifest_key.as_slice(), partial_bytes.as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                }
                for tag in bind.tags {
                    let tag_bytes = encode(tag)?;
                    let mut reverse_key = manifest_key.to_vec();
                    reverse_key.extend_from_slice(&tag.name);
                    {
                        let mut by_name = write
                            .open_table(TAGS_BY_NAME_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        by_name
                            .insert(tag.name.as_slice(), tag_bytes.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                    {
                        let mut by_hash = write
                            .open_table(TAGS_BY_HASH_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        by_hash
                            .insert(reverse_key.as_slice(), tag_bytes.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                }
                generation_state.active = Some(active);
                generation_state.next_generation = active.generation.saturating_add(1);
                let generation_bytes = encode(&generation_state)?;
                {
                    let mut generations = write
                        .open_table(HASH_GENERATIONS_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    generations
                        .insert(bind.hash.as_slice(), generation_bytes.as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                }
                if bind.state == BlobManifestState::Complete {
                    let siblings = {
                        let staging_table = write
                            .open_table(IMPORT_STAGING_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        let mut siblings = Vec::new();
                        for entry in staging_table
                            .iter()
                            .map_err(RedbPersistence::storage_error)?
                        {
                            let (_, value) = entry.map_err(RedbPersistence::storage_error)?;
                            let candidate: BlobImportStaging = decode(value.value())?;
                            if candidate.object_id != bind.object_id
                                && candidate.provisional_generation
                                    == Some((bind.hash, guard.generation()))
                            {
                                siblings.push(candidate);
                                if siblings.len() > 256 {
                                    return Err(Error::Other(
                                        "complete blob bind found more than 256 provisional siblings"
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                        siblings
                    };
                    for sibling in siblings {
                        remove_staging_in_write(&write, &sibling)?;
                    }
                }
                {
                    let mut staging_table = write
                        .open_table(IMPORT_STAGING_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    staging_table
                        .remove(bind.object_id.as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                }
                    BindOutcome::Bound(active)
                }
            };
            write.commit().map_err(RedbPersistence::storage_error)?;
            Ok(outcome)
        })?;
        match outcome {
            BindOutcome::Bound(active) => Ok(active),
            BindOutcome::RefusedByPurge => Err(Error::Other(
                "blob staging began before the hash's authoritative purge frontier".to_string(),
            )),
            BindOutcome::StaleGeneration => Err(Error::Other(
                "blob staging retained a stale hash generation".to_string(),
            )),
        }
    }

    /// Synchronously remove every unbound staging role.
    pub fn discard_staging(&self, object_id: [u8; 16]) -> Result<()> {
        let _operation = self.begin_operation()?;
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let staging = {
                let table = write
                    .open_table(IMPORT_STAGING_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .get(object_id.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| decode::<BlobImportStaging>(value.value()))
                    .transpose()?
            };
            if let Some(staging) = staging {
                remove_staging_in_write(&write, &staging)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
    }

    /// Validate every durable blob witness while the caller owns the full
    /// exclusive hash set. The returned projection contains bounded metadata
    /// only and is applied by `RedbPersistence` inside the row purge's one
    /// write transaction.
    pub(crate) fn prepare_authoritative_purge(
        &self,
        guard: &BlobExclusiveHashSetGuard,
        hashes: &BTreeSet<[u8; 32]>,
        frontier: Lsn,
    ) -> Result<BlobAuthoritativePurgeProjection> {
        if !Arc::ptr_eq(&guard.registry, &self.fences)
            || hashes
                .iter()
                .any(|hash| guard.hashes.binary_search(hash).is_err())
        {
            return Err(Error::Other(
                "blob authoritative purge does not hold its complete exclusive hash set"
                    .to_string(),
            ));
        }

        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table_names = read
                .list_tables()
                .map_err(RedbPersistence::storage_error)?
                .map(|handle| handle.name().to_string())
                .collect::<HashSet<_>>();
            let mut projected = Vec::with_capacity(hashes.len());

            for hash in hashes {
                let expected_generation = if table_names.contains(HASH_GENERATIONS_TABLE.name()) {
                    let generations = read
                        .open_table(HASH_GENERATIONS_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    generations
                        .get(hash.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .map(|value| value.value().to_vec())
                } else {
                    None
                };
                let mut generation = expected_generation
                    .as_deref()
                    .map(decode::<BlobHashGenerationState>)
                    .transpose()?
                    .unwrap_or_default();
                if generation.version != RECORD_VERSION {
                    return Err(Error::Other(format!(
                        "blob authoritative purge found unsupported generation version {}",
                        generation.version
                    )));
                }

                let mut manifest = None;
                let mut partials = BTreeMap::<Vec<u8>, Vec<u8>>::new();
                let mut canonical_object = None;
                let mut tags = Vec::new();
                let mut staging = Vec::new();

                if let Some(active) = generation.active {
                    if !table_names.contains(MANIFESTS_TABLE.name()) {
                        return Err(Error::Other(
                            "blob active generation has no canonical manifest table".to_string(),
                        ));
                    }
                    let manifest_key = Self::manifest_key(hash, active.generation).to_vec();
                    let manifest_bytes = {
                        let manifests = read
                            .open_table(MANIFESTS_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        manifests
                            .get(manifest_key.as_slice())
                            .map_err(RedbPersistence::storage_error)?
                            .ok_or_else(|| {
                                Error::Other(
                                    "blob active generation lost its canonical manifest"
                                        .to_string(),
                                )
                            })?
                            .value()
                            .to_vec()
                    };
                    let decoded_manifest: BlobManifest = decode(&manifest_bytes)?;
                    if decoded_manifest.version != RECORD_VERSION
                        || decoded_manifest.object_id != active.object_id
                    {
                        return Err(Error::Other(
                            "blob active generation and canonical manifest witnesses disagree"
                                .to_string(),
                        ));
                    }

                    let (payload_indices, outboard_indices) = match decoded_manifest.state {
                        BlobManifestState::Complete => {
                            validate_complete_block_count(
                                decoded_manifest.total_size,
                                decoded_manifest.payload_chunk_count,
                                "payload",
                            )?;
                            validate_complete_block_count(
                                decoded_manifest.outboard_size,
                                decoded_manifest.outboard_chunk_count,
                                "outboard",
                            )?;
                            (
                                BlobChunkSetWitness::Dense(decoded_manifest.payload_chunk_count),
                                BlobChunkSetWitness::Dense(decoded_manifest.outboard_chunk_count),
                            )
                        }
                        BlobManifestState::Partial => {
                            if !table_names.contains(PARTIAL_BITFIELDS_TABLE.name()) {
                                return Err(Error::Other(
                                    "blob partial generation has no durable partial-state table"
                                        .to_string(),
                                ));
                            }
                            let partial_bytes = {
                                let table = read
                                    .open_table(PARTIAL_BITFIELDS_TABLE)
                                    .map_err(RedbPersistence::storage_error)?;
                                table
                                    .get(manifest_key.as_slice())
                                    .map_err(RedbPersistence::storage_error)?
                                    .ok_or_else(|| {
                                        Error::Other(
                                            "blob partial generation lost its exact chunk sets"
                                                .to_string(),
                                        )
                                    })?
                                    .value()
                                    .to_vec()
                            };
                            let partial: BlobPartialState = decode(&partial_bytes)?;
                            let indices = validate_partial_state(
                                &partial,
                                decoded_manifest.payload_chunk_count,
                                decoded_manifest.outboard_chunk_count,
                            )?;
                            partials.insert(manifest_key.clone(), partial_bytes);
                            (
                                BlobChunkSetWitness::Exact(indices.0),
                                BlobChunkSetWitness::Exact(indices.1),
                            )
                        }
                    };

                    validate_canonical_chunk_witnesses_in_read(
                        &read,
                        PAYLOAD_CHUNKS_TABLE,
                        &decoded_manifest.object_id,
                        &payload_indices,
                        decoded_manifest.total_size,
                        decoded_manifest.state,
                        "payload",
                    )?;
                    validate_canonical_chunk_witnesses_in_read(
                        &read,
                        OUTBOARD_CHUNKS_TABLE,
                        &decoded_manifest.object_id,
                        &outboard_indices,
                        decoded_manifest.outboard_size,
                        decoded_manifest.state,
                        "outboard",
                    )?;
                    tags = collect_tag_purge_witnesses_in_read(
                        &read,
                        hash,
                        active.generation,
                        decoded_manifest.format,
                        &table_names,
                    )?;
                    canonical_object = Some(BlobCanonicalObjectPurgeWitness {
                        object_id: decoded_manifest.object_id,
                        payload: payload_indices,
                        outboard: outboard_indices,
                        payload_size: decoded_manifest.total_size,
                        outboard_size: decoded_manifest.outboard_size,
                        state: decoded_manifest.state,
                    });
                    manifest = Some((manifest_key, manifest_bytes));
                    generation.last_retired_generation =
                        generation.last_retired_generation.max(active.generation);
                    generation.next_generation = generation
                        .next_generation
                        .max(active.generation.saturating_add(1));
                    generation.active = None;
                }

                if table_names.contains(IMPORT_STAGING_TABLE.name()) {
                    let staging_table = read
                        .open_table(IMPORT_STAGING_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    for entry in staging_table
                        .iter()
                        .map_err(RedbPersistence::storage_error)?
                    {
                        let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                        let encoded = value.value().to_vec();
                        let decoded: BlobImportStaging = decode(&encoded)?;
                        if decoded.version != RECORD_VERSION
                            || key.value() != decoded.object_id.as_slice()
                        {
                            return Err(Error::Other(
                                "blob provisional staging witness is malformed".to_string(),
                            ));
                        }
                        let Some((staged_hash, staged_generation)) = decoded.provisional_generation
                        else {
                            continue;
                        };
                        if staged_hash != *hash {
                            continue;
                        }
                        if table_names.contains(PARTIAL_BITFIELDS_TABLE.name()) {
                            let partial_key = Self::manifest_key(hash, staged_generation).to_vec();
                            let partial_table = read
                                .open_table(PARTIAL_BITFIELDS_TABLE)
                                .map_err(RedbPersistence::storage_error)?;
                            if let Some(value) = partial_table
                                .get(partial_key.as_slice())
                                .map_err(RedbPersistence::storage_error)?
                            {
                                let encoded_partial = value.value().to_vec();
                                if let Some(existing) =
                                    partials.insert(partial_key, encoded_partial.clone())
                                    && existing != encoded_partial
                                {
                                    return Err(Error::Other(
                                        "blob provisional partial witnesses disagree".to_string(),
                                    ));
                                }
                            }
                        }
                        staging.push(BlobStagingPurgeWitness {
                            object_id: decoded.object_id,
                            encoded,
                        });
                    }
                }

                generation.next_generation = generation
                    .next_generation
                    .max(generation.last_retired_generation.saturating_add(1))
                    .max(1);
                generation.last_purge_lsn = frontier.0;
                projected.push(BlobAuthoritativePurgeHashProjection {
                    hash: *hash,
                    expected_generation,
                    next_generation: encode(&generation)?,
                    manifest,
                    partials: partials.into_iter().collect(),
                    canonical_object,
                    tags,
                    staging,
                });
            }

            Ok(BlobAuthoritativePurgeProjection { hashes: projected })
        })
    }

    #[cfg(test)]
    pub(crate) fn install_complete_fixture_for_test(
        self: &Arc<Self>,
        hash: [u8; 32],
        bytes: &[u8],
        start_lsn: u64,
    ) -> Result<()> {
        self.install_fixture_for_test(hash, bytes, bytes.len() as u64, start_lsn, false)
    }

    #[cfg(test)]
    pub(crate) fn install_interrupted_fixture_for_test(
        self: &Arc<Self>,
        hash: [u8; 32],
        verified_prefix: &[u8],
        total_size: u64,
        start_lsn: u64,
    ) -> Result<()> {
        if verified_prefix.len() as u64 >= total_size {
            return Err(Error::Other(
                "blob interrupted fixture must retain a missing suffix".to_string(),
            ));
        }
        self.install_fixture_for_test(hash, verified_prefix, total_size, start_lsn, true)
    }

    #[cfg(test)]
    fn install_fixture_for_test(
        self: &Arc<Self>,
        hash: [u8; 32],
        payload: &[u8],
        total_size: u64,
        start_lsn: u64,
        partial: bool,
    ) -> Result<()> {
        let guard = self.begin_write_generation(hash)?;
        let mut object_id = [0u8; 16];
        object_id.copy_from_slice(&hash[..16]);
        for (byte, salt) in object_id[8..].iter_mut().zip(start_lsn.to_be_bytes()) {
            *byte ^= salt;
        }
        let generation = guard.generation();
        let payload_count = logical_block_count(payload.len() as u64);
        let outboard = hash.as_slice();
        let outboard_count = logical_block_count(outboard.len() as u64);
        let state = if partial {
            BlobManifestState::Partial
        } else {
            BlobManifestState::Complete
        };
        let manifest = BlobManifest {
            version: RECORD_VERSION,
            object_id,
            format: 0,
            total_size,
            outboard_size: outboard.len() as u64,
            validated_size: payload.len() as u64,
            payload_chunk_count: payload_count,
            outboard_chunk_count: outboard_count,
            state,
        };
        let manifest_key = Self::manifest_key(&hash, generation);
        let manifest_bytes = encode(&manifest)?;
        let partial_state = partial
            .then(|| {
                new_partial_state(
                    0..payload_count,
                    0..outboard_count,
                    payload.len().to_be_bytes().to_vec(),
                )
            })
            .transpose()?;
        let tag_role = if partial {
            BlobTagRole::FetchProtection
        } else {
            BlobTagRole::Servable
        };
        let mut tag_name = if partial {
            b"test-fetch-protection:".to_vec()
        } else {
            b"test-servable:".to_vec()
        };
        tag_name.extend_from_slice(&hash);
        let tag = BlobTagRecord {
            version: RECORD_VERSION,
            name: tag_name,
            hash,
            generation,
            format: 0,
            role: tag_role,
        };
        let tag_bytes = encode(&tag)?;
        let mut reverse_key = manifest_key.to_vec();
        reverse_key.extend_from_slice(&tag.name);

        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let mut generation_state = {
                let table = write
                    .open_table(HASH_GENERATIONS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .get(hash.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| decode::<BlobHashGenerationState>(value.value()))
                    .transpose()?
                    .unwrap_or_default()
            };
            if generation_state.active.is_some() || generation_state.next_generation != generation {
                return Err(Error::Other(
                    "blob fixture conflicts with an existing active generation".to_string(),
                ));
            }
            insert_fixture_chunks_in_write(&write, PAYLOAD_CHUNKS_TABLE, &object_id, payload)?;
            insert_fixture_chunks_in_write(&write, OUTBOARD_CHUNKS_TABLE, &object_id, outboard)?;
            {
                let mut manifests = write
                    .open_table(MANIFESTS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                if manifests
                    .insert(manifest_key.as_slice(), manifest_bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
                {
                    return Err(Error::Other(
                        "blob fixture would overwrite a canonical manifest".to_string(),
                    ));
                }
            }
            if let Some(partial_state) = partial_state.as_ref() {
                let bytes = encode(partial_state)?;
                let mut table = write
                    .open_table(PARTIAL_BITFIELDS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(manifest_key.as_slice(), bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            {
                let mut by_name = write
                    .open_table(TAGS_BY_NAME_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                if by_name
                    .insert(tag.name.as_slice(), tag_bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
                {
                    return Err(Error::Other(
                        "blob fixture would overwrite authoritative tag ownership".to_string(),
                    ));
                }
            }
            {
                let mut by_hash = write
                    .open_table(TAGS_BY_HASH_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                by_hash
                    .insert(reverse_key.as_slice(), tag_bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            generation_state.active = Some(ActiveBlobGeneration {
                generation,
                object_id,
            });
            generation_state.next_generation = generation.saturating_add(1);
            let encoded_generation = encode(&generation_state)?;
            {
                let mut generations = write
                    .open_table(HASH_GENERATIONS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                generations
                    .insert(hash.as_slice(), encoded_generation.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn authoritative_purge_roles_for_test(&self, hash: &[u8; 32]) -> Result<BTreeSet<String>> {
        let _operation = self.begin_operation()?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let mut roles = BTreeSet::new();
            if let Ok(generations) = read.open_table(HASH_GENERATIONS_TABLE)
                && let Some(generation_bytes) = generations
                    .get(hash.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                && let Some(active) =
                    decode::<BlobHashGenerationState>(generation_bytes.value())?.active
            {
                roles.insert("hash_generation_fence".to_string());
                let manifest_key = Self::manifest_key(hash, active.generation);
                let manifests = read
                    .open_table(MANIFESTS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                if let Some(manifest_bytes) = manifests
                    .get(manifest_key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                {
                    roles.insert("canonical_manifest".to_string());
                    let manifest: BlobManifest = decode(manifest_bytes.value())?;
                    if object_has_chunks_in_read(&read, PAYLOAD_CHUNKS_TABLE, &manifest.object_id)?
                    {
                        roles.insert("payload_chunks".to_string());
                    }
                    if object_has_chunks_in_read(&read, OUTBOARD_CHUNKS_TABLE, &manifest.object_id)?
                    {
                        roles.insert("outboard_chunks".to_string());
                    }
                    if let Ok(partials) = read.open_table(PARTIAL_BITFIELDS_TABLE)
                        && partials
                            .get(manifest_key.as_slice())
                            .map_err(RedbPersistence::storage_error)?
                            .is_some()
                    {
                        roles.insert("partial_bitfield".to_string());
                    }
                    if let Ok(tags) = read.open_table(TAGS_BY_HASH_TABLE) {
                        for entry in tags
                            .range(manifest_key.as_slice()..)
                            .map_err(RedbPersistence::storage_error)?
                        {
                            let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                            if !key.value().starts_with(manifest_key.as_slice()) {
                                break;
                            }
                            let tag: BlobTagRecord = decode(value.value())?;
                            roles.insert(match tag.role {
                                BlobTagRole::Servable => "servable_tag".to_string(),
                                BlobTagRole::FetchProtection => "fetch_protection".to_string(),
                            });
                        }
                    }
                }
            }
            if let Ok(staging_table) = read.open_table(IMPORT_STAGING_TABLE) {
                for entry in staging_table
                    .iter()
                    .map_err(RedbPersistence::storage_error)?
                {
                    let (_, value) = entry.map_err(RedbPersistence::storage_error)?;
                    let staging: BlobImportStaging = decode(value.value())?;
                    if staging
                        .provisional_generation
                        .is_some_and(|(candidate, _)| candidate == *hash)
                    {
                        roles.insert("import_staging".to_string());
                        if !staging.adapter_state.is_empty() {
                            roles.insert("provisional_partial_state".to_string());
                        }
                        if object_has_chunks_in_read(
                            &read,
                            PAYLOAD_CHUNKS_TABLE,
                            &staging.object_id,
                        )? {
                            roles.insert("staging_payload_chunks".to_string());
                        }
                        if object_has_chunks_in_read(
                            &read,
                            OUTBOARD_CHUNKS_TABLE,
                            &staging.object_id,
                        )? {
                            roles.insert("staging_outboard_chunks".to_string());
                        }
                    }
                }
            }
            Ok(roles)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn bounded_payload_for_test(
        self: &Arc<Self>,
        hash: &[u8; 32],
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>> {
        if self.generation_state(hash)?.active.is_none() {
            return Ok(None);
        }
        let guard = self.begin_shared_generation(*hash)?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let generations = read
                .open_table(HASH_GENERATIONS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let generation: BlobHashGenerationState = decode(
                generations
                    .get(hash.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| {
                        Error::Other("blob bounded read lost its generation fence".to_string())
                    })?
                    .value(),
            )?;
            let active = generation
                .active
                .filter(|active| active.generation == guard.generation())
                .ok_or_else(|| Error::Other("blob bounded read lost its generation".to_string()))?;
            let key = Self::manifest_key(hash, active.generation);
            let manifests = read
                .open_table(MANIFESTS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let manifest: BlobManifest = decode(
                manifests
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::Other("blob bounded read lost its manifest".to_string()))?
                    .value(),
            )?;
            let payload = read
                .open_table(PAYLOAD_CHUNKS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let mut bytes = Vec::with_capacity(max_bytes.min(manifest.total_size as usize));
            for index in 0..manifest.payload_chunk_count {
                if bytes.len() == max_bytes {
                    break;
                }
                let chunk_key = Self::chunk_key(&manifest.object_id, index);
                let chunk = payload
                    .get(chunk_key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| {
                        Error::Other("blob bounded read lost a payload block".to_string())
                    })?;
                let remaining = max_bytes.saturating_sub(bytes.len());
                bytes.extend_from_slice(&chunk.value()[..chunk.value().len().min(remaining)]);
            }
            Ok(Some(bytes))
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn next_missing_range_for_test(
        self: &Arc<Self>,
        hash: &[u8; 32],
    ) -> Result<Option<std::ops::Range<u64>>> {
        if self.generation_state(hash)?.active.is_none() {
            return Ok(None);
        }
        let guard = self.begin_shared_generation(*hash)?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let key = Self::manifest_key(hash, guard.generation());
            let manifests = read
                .open_table(MANIFESTS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let manifest: BlobManifest = decode(
                manifests
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::Other("blob resume read lost its manifest".to_string()))?
                    .value(),
            )?;
            if manifest.state != BlobManifestState::Partial {
                return Ok(None);
            }
            let partials = read
                .open_table(PARTIAL_BITFIELDS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let partial: BlobPartialState = decode(
                partials
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::Other("blob resume state is missing".to_string()))?
                    .value(),
            )?;
            let (payload_indices, _) = validate_partial_state(
                &partial,
                manifest.payload_chunk_count,
                manifest.outboard_chunk_count,
            )?;
            let payload = read
                .open_table(PAYLOAD_CHUNKS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let block_bytes = BLOB_LOGICAL_BLOCK_BYTES as u64;
            let block_count = logical_block_count(manifest.total_size);
            for index in 0..block_count {
                let offset = index.saturating_mul(block_bytes);
                let next_present_boundary = || {
                    payload_indices
                        .iter()
                        .find(|present| *present > index)
                        .map(|present| present.saturating_mul(block_bytes))
                        .unwrap_or(manifest.total_size)
                        .min(manifest.total_size)
                };
                if !payload_indices.contains(index) {
                    return Ok(Some(offset..next_present_boundary()));
                }
                let chunk_key = Self::chunk_key(&manifest.object_id, index);
                let chunk_len = payload
                    .get(chunk_key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| {
                        Error::Other("blob durable partial index has no payload block".to_string())
                    })?
                    .value()
                    .len() as u64;
                let expected = manifest.total_size.saturating_sub(offset).min(block_bytes);
                if chunk_len == 0 || chunk_len > expected {
                    return Err(Error::Other(
                        "blob durable partial block length is malformed".to_string(),
                    ));
                }
                if chunk_len < expected {
                    return Ok(Some(
                        offset.saturating_add(chunk_len)..next_present_boundary(),
                    ));
                }
            }
            Ok(None)
        })
    }

    #[cfg(test)]
    pub(crate) fn arm_authoritative_purge_commit_failure_for_test(&self) {
        AUTHORITATIVE_PURGE_BLOB_COMMIT_FAULT.with(|fault| fault.set(true));
    }

    /// Open the active canonical generation without hydrating payload or
    /// outboard bytes. The returned session owns the shared generation guard
    /// until it is dropped.
    pub fn open_read(self: &Arc<Self>, hash: [u8; 32]) -> Result<Option<BlobReadSession>> {
        if self.generation_state(&hash)?.active.is_none() {
            return Ok(None);
        }
        let guard = self.begin_shared_generation(hash)?;
        let (manifest, partial) = self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let key = Self::manifest_key(&hash, guard.generation());
            let manifests = read
                .open_table(MANIFESTS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let manifest: BlobManifest = decode(
                manifests
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| {
                        Error::Other("blob read session lost its canonical manifest".to_string())
                    })?
                    .value(),
            )?;
            let partial = if manifest.state == BlobManifestState::Partial {
                let partials = read
                    .open_table(PARTIAL_BITFIELDS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                Some(decode(
                    partials
                        .get(key.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .ok_or_else(|| {
                            Error::Other("blob read session lost its partial state".to_string())
                        })?
                        .value(),
                )?)
            } else {
                None
            };
            Ok((manifest, partial))
        })?;
        Ok(Some(BlobReadSession {
            repository: self.clone(),
            guard,
            manifest,
            partial,
        }))
    }

    fn read_session_at(
        &self,
        session: &BlobReadSession,
        offset: u64,
        len: usize,
        outboard: bool,
    ) -> Result<Vec<u8>> {
        if !Arc::ptr_eq(&session.guard.registry, &self.fences) {
            return Err(Error::Other(
                "blob read session belongs to a different repository".to_string(),
            ));
        }
        if len > BLOB_VALUE_BYTES {
            return Err(Error::Other(
                "blob bounded read exceeds the repository value bound".to_string(),
            ));
        }
        let total_size = if outboard {
            session.manifest.outboard_size
        } else {
            session.manifest.total_size
        };
        if offset >= total_size || len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(len as u64).min(total_size);
        let definition = if outboard {
            OUTBOARD_CHUNKS_TABLE
        } else {
            PAYLOAD_CHUNKS_TABLE
        };
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let generations = read
                .open_table(HASH_GENERATIONS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let generation: BlobHashGenerationState = decode(
                generations
                    .get(session.guard.hash.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::Other("blob read lost its generation fence".to_string()))?
                    .value(),
            )?;
            if generation.active.map(|active| active.generation) != Some(session.guard.generation())
            {
                return Err(Error::Other(
                    "blob read generation changed while its guard was held".to_string(),
                ));
            }
            let table = read
                .open_table(definition)
                .map_err(RedbPersistence::storage_error)?;
            let mut result = Vec::with_capacity((end - offset) as usize);
            let block_bytes = BLOB_LOGICAL_BLOCK_BYTES as u64;
            let mut cursor = offset;
            while cursor < end {
                let index = cursor / block_bytes;
                let within = (cursor % block_bytes) as usize;
                let key = Self::chunk_key(&session.manifest.object_id, index);
                let Some(value) = table
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                else {
                    break;
                };
                let bytes = value.value();
                if within >= bytes.len() {
                    break;
                }
                let take = bytes
                    .len()
                    .saturating_sub(within)
                    .min((end - cursor) as usize);
                result.extend_from_slice(&bytes[within..within + take]);
                cursor = cursor.saturating_add(take as u64);
                if take == 0 {
                    break;
                }
            }
            self.bounded_blob_bytes_read
                .fetch_add(result.len() as u64, Ordering::Relaxed);
            Ok(result)
        })
    }

    /// List canonical tags directly from Redb. Callers use these records as
    /// the sole serving/protection authority; no process-local mirror is
    /// involved.
    pub fn list_tags_page(&self, after: Option<&[u8]>, limit: usize) -> Result<Vec<BlobTagRecord>> {
        const MAX_TAG_PAGE: usize = 256;
        if limit == 0 || limit > MAX_TAG_PAGE {
            return Err(Error::Other(format!(
                "blob tag page must contain 1..={MAX_TAG_PAGE} records"
            )));
        }
        let _operation = self.begin_operation()?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(TAGS_BY_NAME_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            let mut records = Vec::with_capacity(limit);
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                if after.is_some_and(|after| key.value() <= after) {
                    continue;
                }
                records.push(decode(value.value())?);
                if records.len() == limit {
                    break;
                }
            }
            Ok(records)
        })
    }

    pub fn list_active_hashes_page(
        &self,
        after: Option<&[u8; 32]>,
        limit: usize,
    ) -> Result<Vec<[u8; 32]>> {
        const MAX_HASH_PAGE: usize = 256;
        if limit == 0 || limit > MAX_HASH_PAGE {
            return Err(Error::Other(format!(
                "blob hash page must contain 1..={MAX_HASH_PAGE} records"
            )));
        }
        let _operation = self.begin_operation()?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(HASH_GENERATIONS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            let mut hashes = Vec::with_capacity(limit);
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                if key.value().len() != 32
                    || after.is_some_and(|after| key.value() <= after.as_slice())
                {
                    continue;
                }
                let state: BlobHashGenerationState = decode(value.value())?;
                if state.active.is_none() {
                    continue;
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(key.value());
                hashes.push(hash);
                if hashes.len() == limit {
                    break;
                }
            }
            Ok(hashes)
        })
    }

    /// Canonical addressability predicate used immediately before serving.
    pub fn has_tag_role(&self, hash: &[u8; 32], role: BlobTagRole) -> Result<bool> {
        let _operation = self.begin_operation()?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let generations = match read.open_table(HASH_GENERATIONS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            let Some(generation) = generations
                .get(hash.as_slice())
                .map_err(RedbPersistence::storage_error)?
            else {
                return Ok(false);
            };
            let generation: BlobHashGenerationState = decode(generation.value())?;
            let Some(active) = generation.active else {
                return Ok(false);
            };
            let prefix = Self::manifest_key(hash, active.generation);
            let tags = match read.open_table(TAGS_BY_HASH_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            for entry in tags
                .range(prefix.as_slice()..)
                .map_err(RedbPersistence::storage_error)?
            {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                if !key.value().starts_with(prefix.as_slice()) {
                    break;
                }
                if decode::<BlobTagRecord>(value.value())?.role == role {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn count_tag_role(&self, role: BlobTagRole) -> Result<u64> {
        let _operation = self.begin_operation()?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(TAGS_BY_NAME_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            let mut count = 0u64;
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (_, value) = entry.map_err(RedbPersistence::storage_error)?;
                if decode::<BlobTagRecord>(value.value())?.role == role {
                    count = count.saturating_add(1);
                }
            }
            Ok(count)
        })
    }

    /// Atomically install or replace one canonical tag on an active
    /// generation. The shared guard is retained through both tag indexes.
    pub fn set_tag(
        self: &Arc<Self>,
        name: Vec<u8>,
        hash: [u8; 32],
        format: u8,
        role: BlobTagRole,
    ) -> Result<()> {
        let previous = self.tag_by_name(&name)?;
        let mut hashes = BTreeSet::from([hash]);
        if let Some(previous) = &previous {
            hashes.insert(previous.hash);
        }
        let guard = self.begin_shared_generation_set(&hashes)?;
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let current_previous = {
                let by_name = write
                    .open_table(TAGS_BY_NAME_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                by_name
                    .get(name.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| decode::<BlobTagRecord>(value.value()))
                    .transpose()?
            };
            if current_previous != previous {
                return Err(Error::Other(
                    "blob tag ownership changed while acquiring its complete guard set".to_string(),
                ));
            }
            let generation = guard
                .generation(&hash)
                .ok_or_else(|| Error::Other("blob tag destination guard is missing".to_string()))?;
            let record = BlobTagRecord::new(name.clone(), hash, generation, format, role);
            let encoded = encode(&record)?;
            {
                let mut by_name = write
                    .open_table(TAGS_BY_NAME_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                by_name
                    .insert(name.as_slice(), encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            if let Some(previous) = &previous {
                let mut previous_key =
                    Self::manifest_key(&previous.hash, previous.generation).to_vec();
                previous_key.extend_from_slice(&previous.name);
                let mut by_hash = write
                    .open_table(TAGS_BY_HASH_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                by_hash
                    .remove(previous_key.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            let mut reverse_key = Self::manifest_key(&hash, generation).to_vec();
            reverse_key.extend_from_slice(&name);
            {
                let mut by_hash = write
                    .open_table(TAGS_BY_HASH_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                by_hash
                    .insert(reverse_key.as_slice(), encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
    }

    /// Delete a half-open tag range in bounded pages. Each page atomically
    /// acquires the complete sorted owner set and revalidates every name in
    /// the same Redb transaction that removes both indexes.
    pub fn delete_tags(self: &Arc<Self>, from: Option<&[u8]>, to: Option<&[u8]>) -> Result<u64> {
        const DELETE_PAGE: usize = 256;
        let mut cursor = from.map(ToOwned::to_owned);
        let mut deleted = 0u64;
        loop {
            let victims = self.store()?.with_db(|db| {
                let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
                let table = match read.open_table(TAGS_BY_NAME_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                    Err(err) => return Err(RedbPersistence::storage_error(err)),
                };
                let mut victims = Vec::with_capacity(DELETE_PAGE);
                for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                    let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                    if cursor.as_deref().is_some_and(|cursor| key.value() < cursor)
                        || to.is_some_and(|to| key.value() >= to)
                    {
                        continue;
                    }
                    victims.push(decode::<BlobTagRecord>(value.value())?);
                    if victims.len() == DELETE_PAGE {
                        break;
                    }
                }
                Ok(victims)
            })?;
            if victims.is_empty() {
                return Ok(deleted);
            }
            let hashes = victims.iter().map(|record| record.hash).collect();
            let guard = self.begin_shared_generation_set(&hashes)?;
            self.store()?.with_db(|db| {
                let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
                for record in &victims {
                    if guard.generation(&record.hash) != Some(record.generation) {
                        return Err(Error::Other(
                            "blob tag owner generation changed before bounded deletion".to_string(),
                        ));
                    }
                    let current = {
                        let by_name = write
                            .open_table(TAGS_BY_NAME_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        by_name
                            .get(record.name.as_slice())
                            .map_err(RedbPersistence::storage_error)?
                            .map(|value| decode::<BlobTagRecord>(value.value()))
                            .transpose()?
                    };
                    if current.as_ref() != Some(record) {
                        return Err(Error::Other(
                            "blob tag ownership changed before bounded deletion".to_string(),
                        ));
                    }
                    let mut reverse_key =
                        Self::manifest_key(&record.hash, record.generation).to_vec();
                    reverse_key.extend_from_slice(&record.name);
                    {
                        let mut by_name = write
                            .open_table(TAGS_BY_NAME_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        by_name
                            .remove(record.name.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                    {
                        let mut by_hash = write
                            .open_table(TAGS_BY_HASH_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        by_hash
                            .remove(reverse_key.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                }
                write.commit().map_err(RedbPersistence::storage_error)
            })?;
            deleted = deleted.saturating_add(victims.len() as u64);
            cursor = victims.last().map(|record| record.name.clone());
        }
    }

    pub fn rename_tag(self: &Arc<Self>, from: &[u8], to: Vec<u8>) -> Result<()> {
        let record = self
            .tag_by_name(from)?
            .ok_or_else(|| Error::Other("blob tag does not exist".to_string()))?;
        let guard = self.begin_shared_generation_set(&BTreeSet::from([record.hash]))?;
        self.store()?.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            let current = {
                let by_name = write
                    .open_table(TAGS_BY_NAME_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                by_name
                    .get(from)
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| decode::<BlobTagRecord>(value.value()))
                    .transpose()?
            };
            if current.as_ref() != Some(&record)
                || guard.generation(&record.hash) != Some(record.generation)
            {
                return Err(Error::Other(
                    "blob tag ownership changed before rename".to_string(),
                ));
            }
            {
                let by_name = write
                    .open_table(TAGS_BY_NAME_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                if by_name
                    .get(to.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
                {
                    return Err(Error::Other(
                        "blob destination tag already exists".to_string(),
                    ));
                }
            }
            let mut updated = record.clone();
            updated.name = to.clone();
            let encoded = encode(&updated)?;
            let mut old_reverse = Self::manifest_key(&record.hash, record.generation).to_vec();
            old_reverse.extend_from_slice(&record.name);
            let mut new_reverse = Self::manifest_key(&record.hash, record.generation).to_vec();
            new_reverse.extend_from_slice(&to);
            {
                let mut by_name = write
                    .open_table(TAGS_BY_NAME_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                by_name
                    .remove(from)
                    .map_err(RedbPersistence::storage_error)?;
                by_name
                    .insert(to.as_slice(), encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            {
                let mut by_hash = write
                    .open_table(TAGS_BY_HASH_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                by_hash
                    .remove(old_reverse.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
                by_hash
                    .insert(new_reverse.as_slice(), encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
    }

    fn tag_by_name(&self, name: &[u8]) -> Result<Option<BlobTagRecord>> {
        let _operation = self.begin_operation()?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(TAGS_BY_NAME_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(err) => return Err(RedbPersistence::storage_error(err)),
            };
            table
                .get(name)
                .map_err(RedbPersistence::storage_error)?
                .map(|value| decode(value.value()))
                .transpose()
        })
    }

    /// Capture an owning Redb read transaction. It remains readable while a
    /// later purge commits, and does not hold the database commit mutex.
    pub fn begin_export_snapshot(&self) -> Result<BlobExportSnapshot> {
        let operation = self.begin_operation()?;
        self.store()?.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            Ok(BlobExportSnapshot {
                read,
                _operation: operation,
            })
        })
    }

    /// Mark this shared repository closed. Only the ephemeral form owns and
    /// closes its persistence; file-backed persistence remains Database-owned.
    pub fn close(&self) {
        if !self.operation_gate.begin_close() {
            return;
        }
        self.fences.close();
        if self.ephemeral_dir.is_some()
            && let Some(persistence) = self.persistence.as_ref()
        {
            persistence.close();
        }
        self.operation_gate.finish_close();
    }

    /// Encode the ordered key for one canonical hash generation.
    pub fn manifest_key(hash: &[u8; 32], generation: u64) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[..32].copy_from_slice(hash);
        key[32..].copy_from_slice(&generation.to_be_bytes());
        key
    }

    /// Encode the ordered key for one payload or outboard chunk.
    pub fn chunk_key(object_id: &[u8; 16], index: u64) -> [u8; 24] {
        let mut key = [0u8; 24];
        key[..16].copy_from_slice(object_id);
        key[16..].copy_from_slice(&index.to_be_bytes());
        key
    }

    /// Borrow bounded slices without allocating another blob-sized buffer.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn logical_blocks(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
        bytes.chunks(BLOB_LOGICAL_BLOCK_BYTES)
    }
}

impl BlobExportSnapshot {
    /// Copy only canonical manifest-owned roles plus durable generation
    /// fences. Unbound staging and orphan chunks are deliberately excluded.
    pub(crate) fn copy_canonical_into(&self, artifact: &RedbPersistence) -> Result<()> {
        let table_names = self
            .read
            .list_tables()
            .map_err(RedbPersistence::storage_error)?
            .map(|handle| handle.name().to_string())
            .collect::<HashSet<_>>();
        if !table_names.contains(HASH_GENERATIONS_TABLE.name()) {
            return Ok(());
        }

        let generations = self
            .read
            .open_table(HASH_GENERATIONS_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        for entry in generations.iter().map_err(RedbPersistence::storage_error)? {
            let (hash, state_bytes) = entry.map_err(RedbPersistence::storage_error)?;
            let hash = hash.value().to_vec();
            let state_bytes = state_bytes.value().to_vec();
            if hash.len() != 32 {
                return Err(Error::Other(
                    "blob generation table contains a malformed hash key".to_string(),
                ));
            }
            let state: BlobHashGenerationState = decode(&state_bytes)?;
            if state.version != RECORD_VERSION {
                return Err(Error::Other(format!(
                    "unsupported blob generation-state version {}",
                    state.version
                )));
            }
            insert_raw_entries(
                artifact,
                HASH_GENERATIONS_TABLE,
                &[(hash.clone(), state_bytes)],
            )?;

            let Some(active) = state.active else {
                continue;
            };
            let mut hash_array = [0u8; 32];
            hash_array.copy_from_slice(&hash);
            let manifest_key = BlobRepository::manifest_key(&hash_array, active.generation);
            let manifests = self
                .read
                .open_table(MANIFESTS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let manifest_bytes = manifests
                .get(manifest_key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    Error::Other(
                        "active blob generation has no canonical manifest in export snapshot"
                            .to_string(),
                    )
                })?
                .value()
                .to_vec();
            let manifest: BlobManifest = decode(&manifest_bytes)?;
            if manifest.version != RECORD_VERSION {
                return Err(Error::Other(format!(
                    "unsupported blob manifest version {}",
                    manifest.version
                )));
            }
            if manifest.object_id != active.object_id {
                return Err(Error::Other(
                    "active blob generation and manifest object witnesses disagree".to_string(),
                ));
            }
            insert_raw_entries(
                artifact,
                MANIFESTS_TABLE,
                &[(manifest_key.to_vec(), manifest_bytes)],
            )?;
            match manifest.state {
                BlobManifestState::Complete => {
                    validate_complete_block_count(
                        manifest.total_size,
                        manifest.payload_chunk_count,
                        "payload",
                    )?;
                    validate_complete_block_count(
                        manifest.outboard_size,
                        manifest.outboard_chunk_count,
                        "outboard",
                    )?;
                    self.copy_chunks_at_indices(
                        artifact,
                        PAYLOAD_CHUNKS_TABLE,
                        &manifest.object_id,
                        0..manifest.payload_chunk_count,
                        manifest.payload_chunk_count,
                        manifest.total_size,
                        BlobManifestState::Complete,
                        &table_names,
                    )?;
                    self.copy_chunks_at_indices(
                        artifact,
                        OUTBOARD_CHUNKS_TABLE,
                        &manifest.object_id,
                        0..manifest.outboard_chunk_count,
                        manifest.outboard_chunk_count,
                        manifest.outboard_size,
                        BlobManifestState::Complete,
                        &table_names,
                    )?;
                }
                BlobManifestState::Partial => {
                    if !table_names.contains(PARTIAL_BITFIELDS_TABLE.name()) {
                        return Err(Error::Other(
                            "partial blob export snapshot is missing durable partial state"
                                .to_string(),
                        ));
                    }
                    let bitfields = self
                        .read
                        .open_table(PARTIAL_BITFIELDS_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    let partial_bytes = bitfields
                        .get(manifest_key.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .ok_or_else(|| {
                            Error::Other(
                                "partial blob export snapshot is missing its exact chunk sets"
                                    .to_string(),
                            )
                        })?
                        .value()
                        .to_vec();
                    let partial: BlobPartialState = decode(&partial_bytes)?;
                    let (payload_indices, outboard_indices) = validate_partial_state(
                        &partial,
                        manifest.payload_chunk_count,
                        manifest.outboard_chunk_count,
                    )?;
                    insert_raw_entries(
                        artifact,
                        PARTIAL_BITFIELDS_TABLE,
                        &[(manifest_key.to_vec(), partial_bytes)],
                    )?;
                    self.copy_chunks_at_indices(
                        artifact,
                        PAYLOAD_CHUNKS_TABLE,
                        &manifest.object_id,
                        payload_indices.iter(),
                        manifest.payload_chunk_count,
                        manifest.total_size,
                        BlobManifestState::Partial,
                        &table_names,
                    )?;
                    self.copy_chunks_at_indices(
                        artifact,
                        OUTBOARD_CHUNKS_TABLE,
                        &manifest.object_id,
                        outboard_indices.iter(),
                        manifest.outboard_chunk_count,
                        manifest.outboard_size,
                        BlobManifestState::Partial,
                        &table_names,
                    )?;
                }
            }
            self.copy_tags(artifact, &hash_array, active.generation, &table_names)?;
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the export kernel must carry the manifest's exact chunk projection and validation bounds together"
    )]
    fn copy_chunks_at_indices<I>(
        &self,
        artifact: &RedbPersistence,
        definition: TableDefinition<&[u8], &[u8]>,
        object_id: &[u8; 16],
        indices: I,
        expected_count: u64,
        logical_size: u64,
        state: BlobManifestState,
        table_names: &HashSet<String>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = u64>,
    {
        if expected_count == 0 {
            return Ok(());
        }
        if !table_names.contains(definition.name()) {
            return Err(Error::Other(format!(
                "canonical blob manifest owns missing table {}",
                definition.name()
            )));
        }
        let source = self
            .read
            .open_table(definition)
            .map_err(RedbPersistence::storage_error)?;
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        let mut copied = 0u64;
        let block_bytes = BLOB_LOGICAL_BLOCK_BYTES as u64;
        for index in indices {
            let key = BlobRepository::chunk_key(object_id, index);
            let value = source
                .get(key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    Error::Other(format!(
                        "canonical blob manifest owns missing chunk {index} in {}",
                        definition.name()
                    ))
                })?
                .value()
                .to_vec();
            let offset = index.saturating_mul(block_bytes);
            let expected_value_bytes = logical_size.saturating_sub(offset).min(block_bytes);
            let valid_length = match state {
                BlobManifestState::Complete => value.len() as u64 == expected_value_bytes,
                BlobManifestState::Partial => {
                    !value.is_empty() && value.len() as u64 <= expected_value_bytes
                }
            };
            if expected_value_bytes == 0 || !valid_length {
                return Err(Error::Other(format!(
                    "blob chunk {index} in {} disagrees with its deterministic logical offset",
                    definition.name()
                )));
            }
            if !batch.is_empty() && batch_bytes.saturating_add(value.len()) > BLOB_WRITE_BATCH_BYTES
            {
                insert_raw_entries(artifact, definition, &batch)?;
                batch.clear();
                batch_bytes = 0;
            }
            batch_bytes = batch_bytes.saturating_add(value.len());
            batch.push((key.to_vec(), value));
            copied = copied.saturating_add(1);
        }
        if !batch.is_empty() {
            insert_raw_entries(artifact, definition, &batch)?;
        }
        if copied != expected_count {
            return Err(Error::Other(format!(
                "blob exact chunk set cardinality {copied} disagrees with manifest count {expected_count} in {}",
                definition.name()
            )));
        }
        Ok(())
    }

    fn copy_tags(
        &self,
        artifact: &RedbPersistence,
        hash: &[u8; 32],
        generation: u64,
        table_names: &HashSet<String>,
    ) -> Result<()> {
        if !table_names.contains(TAGS_BY_HASH_TABLE.name()) {
            return Ok(());
        }
        if !table_names.contains(TAGS_BY_NAME_TABLE.name()) {
            return Err(Error::Other(
                "blob reverse tag index exists without its authoritative by-name index".to_string(),
            ));
        }
        let reverse = self
            .read
            .open_table(TAGS_BY_HASH_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let authoritative = self
            .read
            .open_table(TAGS_BY_NAME_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let prefix = BlobRepository::manifest_key(hash, generation);
        let mut reverse_entries = Vec::new();
        let mut name_entries = Vec::new();
        let mut owned_names = HashSet::new();
        for entry in reverse
            .range(prefix.as_slice()..)
            .map_err(RedbPersistence::storage_error)?
        {
            let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
            if !key.value().starts_with(prefix.as_slice()) {
                break;
            }
            let value = value.value().to_vec();
            let tag: BlobTagRecord = decode(&value)?;
            if tag.version != RECORD_VERSION || tag.hash != *hash || tag.generation != generation {
                return Err(Error::Other(
                    "blob reverse tag index disagrees with its canonical generation".to_string(),
                ));
            }
            let mut expected_reverse_key = prefix.to_vec();
            expected_reverse_key.extend_from_slice(&tag.name);
            if key.value() != expected_reverse_key.as_slice() {
                return Err(Error::Other(
                    "blob reverse tag key disagrees with its encoded owner".to_string(),
                ));
            }
            if !owned_names.insert(tag.name.clone()) {
                return Err(Error::Other(
                    "blob export snapshot contains duplicate reverse tag ownership".to_string(),
                ));
            }
            let authoritative_value = authoritative
                .get(tag.name.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    Error::Other(
                        "blob reverse tag has no authoritative by-name ownership".to_string(),
                    )
                })?;
            if authoritative_value.value() != value.as_slice() {
                return Err(Error::Other(
                    "blob reverse tag disagrees with authoritative by-name ownership".to_string(),
                ));
            }
            reverse_entries.push((expected_reverse_key, value.clone()));
            name_entries.push((tag.name, value));
        }
        if !reverse_entries.is_empty() {
            insert_raw_entries(artifact, TAGS_BY_HASH_TABLE, &reverse_entries)?;
            insert_raw_entries(artifact, TAGS_BY_NAME_TABLE, &name_entries)?;
        }
        Ok(())
    }
}

#[cfg(test)]
fn insert_fixture_chunks_in_write(
    write: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    object_id: &[u8; 16],
    bytes: &[u8],
) -> Result<()> {
    let mut table = write
        .open_table(definition)
        .map_err(RedbPersistence::storage_error)?;
    for (index, block) in BlobRepository::logical_blocks(bytes).enumerate() {
        let index = u64::try_from(index)
            .map_err(|_| Error::Other("blob fixture has too many bounded blocks".to_string()))?;
        let key = BlobRepository::chunk_key(object_id, index);
        if table
            .insert(key.as_slice(), block)
            .map_err(RedbPersistence::storage_error)?
            .is_some()
        {
            return Err(Error::Other(
                "blob fixture would overwrite a bounded chunk".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "test-seams"))]
fn object_has_chunks_in_read(
    read: &redb::ReadTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    object_id: &[u8; 16],
) -> Result<bool> {
    let table = match read.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
        Err(err) => return Err(RedbPersistence::storage_error(err)),
    };
    let start = BlobRepository::chunk_key(object_id, 0);
    let end = BlobRepository::chunk_key(object_id, u64::MAX);
    Ok(table
        .range(start.as_slice()..=end.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .next()
        .transpose()
        .map_err(RedbPersistence::storage_error)?
        .is_some())
}

fn validate_canonical_chunk_witnesses_in_read(
    read: &redb::ReadTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    object_id: &[u8; 16],
    indices: &BlobChunkSetWitness,
    logical_size: u64,
    state: BlobManifestState,
    role: &str,
) -> Result<()> {
    let is_empty = match indices {
        BlobChunkSetWitness::Dense(count) => *count == 0,
        BlobChunkSetWitness::Exact(indices) => indices.is_empty(),
    };
    if is_empty {
        return Ok(());
    }
    let table = read
        .open_table(definition)
        .map_err(RedbPersistence::storage_error)?;
    let indices: Box<dyn Iterator<Item = u64> + '_> = match indices {
        BlobChunkSetWitness::Dense(count) => Box::new(0..*count),
        BlobChunkSetWitness::Exact(indices) => Box::new(indices.iter()),
    };
    let block_bytes = BLOB_LOGICAL_BLOCK_BYTES as u64;
    let block_count = logical_block_count(logical_size);
    for index in indices {
        if index >= block_count {
            return Err(Error::Other(format!(
                "blob canonical {role} witness is outside its declared size"
            )));
        }
        let key = BlobRepository::chunk_key(object_id, index);
        let actual = table
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| {
                Error::Other(format!(
                    "blob canonical {role} witness is missing bounded block {index}"
                ))
            })?
            .value()
            .len();
        let offset = index.saturating_mul(block_bytes);
        let expected = logical_size.saturating_sub(offset).min(block_bytes) as usize;
        let valid = match state {
            BlobManifestState::Complete => actual == expected,
            BlobManifestState::Partial => actual != 0 && actual <= expected,
        };
        if !valid {
            return Err(Error::Other(format!(
                "blob canonical {role} block {index} length disagrees with its manifest"
            )));
        }
    }
    Ok(())
}

fn collect_tag_purge_witnesses_in_read(
    read: &redb::ReadTransaction,
    hash: &[u8; 32],
    generation: u64,
    format: u8,
    table_names: &HashSet<String>,
) -> Result<Vec<BlobTagPurgeWitness>> {
    let has_reverse = table_names.contains(TAGS_BY_HASH_TABLE.name());
    let has_names = table_names.contains(TAGS_BY_NAME_TABLE.name());
    if !has_reverse && !has_names {
        return Ok(Vec::new());
    }
    if !has_reverse || !has_names {
        return Err(Error::Other(
            "blob tag indexes are not both present for authoritative purge".to_string(),
        ));
    }

    let reverse = read
        .open_table(TAGS_BY_HASH_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let by_name = read
        .open_table(TAGS_BY_NAME_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let prefix = BlobRepository::manifest_key(hash, generation);
    let mut reverse_by_name = BTreeMap::<Vec<u8>, BlobTagPurgeWitness>::new();
    for entry in reverse
        .range(prefix.as_slice()..)
        .map_err(RedbPersistence::storage_error)?
    {
        let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
        if !key.value().starts_with(prefix.as_slice()) {
            break;
        }
        let encoded = value.value().to_vec();
        let tag: BlobTagRecord = decode(&encoded)?;
        let mut expected_key = prefix.to_vec();
        expected_key.extend_from_slice(&tag.name);
        if tag.version != RECORD_VERSION
            || tag.hash != *hash
            || tag.generation != generation
            || tag.format != format
            || key.value() != expected_key.as_slice()
        {
            return Err(Error::Other(
                "blob reverse tag witness disagrees with its canonical owner".to_string(),
            ));
        }
        let authoritative = by_name
            .get(tag.name.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| {
                Error::Other("blob reverse tag lost its authoritative name witness".to_string())
            })?;
        if authoritative.value() != encoded.as_slice() {
            return Err(Error::Other(
                "blob reverse tag disagrees with authoritative tag ownership".to_string(),
            ));
        }
        if reverse_by_name
            .insert(
                tag.name.clone(),
                BlobTagPurgeWitness {
                    name: tag.name,
                    reverse_key: expected_key,
                    encoded,
                },
            )
            .is_some()
        {
            return Err(Error::Other(
                "blob canonical generation has duplicate tag ownership".to_string(),
            ));
        }
    }

    for entry in by_name.iter().map_err(RedbPersistence::storage_error)? {
        let (name, value) = entry.map_err(RedbPersistence::storage_error)?;
        let tag: BlobTagRecord = decode(value.value())?;
        if tag.hash != *hash || tag.generation != generation {
            continue;
        }
        let Some(reverse_witness) = reverse_by_name.get(name.value()) else {
            return Err(Error::Other(
                "blob authoritative tag has no reverse ownership witness".to_string(),
            ));
        };
        if tag.version != RECORD_VERSION
            || tag.name.as_slice() != name.value()
            || reverse_witness.encoded.as_slice() != value.value()
        {
            return Err(Error::Other(
                "blob authoritative tag witness is malformed".to_string(),
            ));
        }
    }
    Ok(reverse_by_name.into_values().collect())
}

/// Apply the preflighted blob projection inside the authoritative row purge's
/// existing Redb write transaction. Every witness mismatch names the blob
/// boundary and aborts the whole transaction.
pub(crate) fn apply_authoritative_purge_in_write(
    write: &redb::WriteTransaction,
    projection: &BlobAuthoritativePurgeProjection,
) -> Result<()> {
    for hash_projection in &projection.hashes {
        {
            let mut table = write
                .open_table(HASH_GENERATIONS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let current = table
                .get(hash_projection.hash.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .map(|value| value.value().to_vec());
            if current != hash_projection.expected_generation {
                return Err(Error::Other(
                    "blob generation witness changed before authoritative purge commit".to_string(),
                ));
            }
            table
                .insert(
                    hash_projection.hash.as_slice(),
                    hash_projection.next_generation.as_slice(),
                )
                .map_err(RedbPersistence::storage_error)?;
        }

        if let Some((key, expected)) = &hash_projection.manifest {
            remove_exact_blob_value_in_write(write, MANIFESTS_TABLE, key, expected)?;
        }
        for (key, expected) in &hash_projection.partials {
            remove_exact_blob_value_in_write(write, PARTIAL_BITFIELDS_TABLE, key, expected)?;
        }
        if let Some(canonical) = &hash_projection.canonical_object {
            validate_canonical_chunk_witnesses_in_write(
                write,
                PAYLOAD_CHUNKS_TABLE,
                &canonical.object_id,
                &canonical.payload,
                canonical.payload_size,
                canonical.state,
                "payload",
            )?;
            validate_canonical_chunk_witnesses_in_write(
                write,
                OUTBOARD_CHUNKS_TABLE,
                &canonical.object_id,
                &canonical.outboard,
                canonical.outboard_size,
                canonical.state,
                "outboard",
            )?;
        }
        for tag in &hash_projection.tags {
            remove_exact_blob_value_in_write(
                write,
                TAGS_BY_HASH_TABLE,
                &tag.reverse_key,
                &tag.encoded,
            )?;
            remove_exact_blob_value_in_write(write, TAGS_BY_NAME_TABLE, &tag.name, &tag.encoded)?;
        }
        for staging in &hash_projection.staging {
            remove_exact_blob_value_in_write(
                write,
                IMPORT_STAGING_TABLE,
                &staging.object_id,
                &staging.encoded,
            )?;
        }
        let mut object_ids = hash_projection
            .staging
            .iter()
            .map(|staging| staging.object_id)
            .collect::<BTreeSet<_>>();
        if let Some(canonical) = &hash_projection.canonical_object {
            object_ids.insert(canonical.object_id);
        }
        for object_id in object_ids {
            remove_object_chunks_in_write(write, PAYLOAD_CHUNKS_TABLE, &object_id)?;
            remove_object_chunks_in_write(write, OUTBOARD_CHUNKS_TABLE, &object_id)?;
        }
    }

    #[cfg(test)]
    if !projection.hashes.is_empty()
        && AUTHORITATIVE_PURGE_BLOB_COMMIT_FAULT.with(|fault| fault.replace(false))
    {
        return Err(Error::Other(
            "blob authoritative purge commit failure injected".to_string(),
        ));
    }
    Ok(())
}

fn remove_exact_blob_value_in_write(
    write: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    key: &[u8],
    expected: &[u8],
) -> Result<()> {
    let mut table = write
        .open_table(definition)
        .map_err(RedbPersistence::storage_error)?;
    let current = table
        .get(key)
        .map_err(RedbPersistence::storage_error)?
        .map(|value| value.value().to_vec());
    if current.as_deref() != Some(expected) {
        return Err(Error::Other(format!(
            "blob durable witness changed in {} before authoritative purge commit",
            definition.name()
        )));
    }
    table.remove(key).map_err(RedbPersistence::storage_error)?;
    Ok(())
}

fn validate_canonical_chunk_witnesses_in_write(
    write: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    object_id: &[u8; 16],
    indices: &BlobChunkSetWitness,
    logical_size: u64,
    state: BlobManifestState,
    role: &str,
) -> Result<()> {
    let is_empty = match indices {
        BlobChunkSetWitness::Dense(count) => *count == 0,
        BlobChunkSetWitness::Exact(indices) => indices.is_empty(),
    };
    if is_empty {
        return Ok(());
    }
    let table = write
        .open_table(definition)
        .map_err(RedbPersistence::storage_error)?;
    let indices: Box<dyn Iterator<Item = u64> + '_> = match indices {
        BlobChunkSetWitness::Dense(count) => Box::new(0..*count),
        BlobChunkSetWitness::Exact(indices) => Box::new(indices.iter()),
    };
    let block_bytes = BLOB_LOGICAL_BLOCK_BYTES as u64;
    let block_count = logical_block_count(logical_size);
    for index in indices {
        if index >= block_count {
            return Err(Error::Other(format!(
                "blob canonical {role} witness is outside its declared size"
            )));
        }
        let key = BlobRepository::chunk_key(object_id, index);
        let actual = table
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| {
                Error::Other(format!(
                    "blob canonical {role} witness disappeared before authoritative purge commit"
                ))
            })?
            .value()
            .len();
        let offset = index.saturating_mul(block_bytes);
        let expected = logical_size.saturating_sub(offset).min(block_bytes) as usize;
        let valid = match state {
            BlobManifestState::Complete => actual == expected,
            BlobManifestState::Partial => actual != 0 && actual <= expected,
        };
        if !valid {
            return Err(Error::Other(format!(
                "blob canonical {role} block changed before authoritative purge commit"
            )));
        }
    }
    Ok(())
}

fn insert_raw_entries(
    persistence: &RedbPersistence,
    definition: TableDefinition<&[u8], &[u8]>,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    persistence.with_db(|db| {
        let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
        {
            let mut table = write
                .open_table(definition)
                .map_err(RedbPersistence::storage_error)?;
            for (key, value) in entries {
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
        }
        write.commit().map_err(RedbPersistence::storage_error)
    })
}

fn remove_staging_in_write(
    write: &redb::WriteTransaction,
    staging: &BlobImportStaging,
) -> Result<()> {
    remove_object_chunks_in_write(write, PAYLOAD_CHUNKS_TABLE, &staging.object_id)?;
    remove_object_chunks_in_write(write, OUTBOARD_CHUNKS_TABLE, &staging.object_id)?;
    if let Some((hash, generation)) = staging.provisional_generation {
        let key = BlobRepository::manifest_key(&hash, generation);
        let mut partial = write
            .open_table(PARTIAL_BITFIELDS_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        partial
            .remove(key.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    {
        let mut staging_table = write
            .open_table(IMPORT_STAGING_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        staging_table
            .remove(staging.object_id.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}

fn remove_object_chunks_in_write(
    write: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    object_id: &[u8; 16],
) -> Result<()> {
    let start = BlobRepository::chunk_key(object_id, 0);
    let end = BlobRepository::chunk_key(object_id, u64::MAX);
    let mut table = write
        .open_table(definition)
        .map_err(RedbPersistence::storage_error)?;
    loop {
        let keys = table
            .range(start.as_slice()..=end.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .take(1024)
            .map(|entry| {
                entry
                    .map(|(key, _)| key.value().to_vec())
                    .map_err(RedbPersistence::storage_error)
            })
            .collect::<Result<Vec<_>>>()?;
        if keys.is_empty() {
            break;
        }
        for key in keys {
            table
                .remove(key.as_slice())
                .map_err(RedbPersistence::storage_error)?;
        }
    }
    Ok(())
}

fn encode_chunk_indices(indices: &roaring::RoaringTreemap) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    indices
        .serialize_into(&mut bytes)
        .map_err(|err| Error::Other(format!("encode exact blob chunk set: {err}")))?;
    Ok(bytes)
}

fn decode_chunk_indices(bytes: &[u8]) -> Result<roaring::RoaringTreemap> {
    roaring::RoaringTreemap::deserialize_from(bytes)
        .map_err(|err| Error::Other(format!("decode exact blob chunk set: {err}")))
}

fn validate_partial_state(
    partial: &BlobPartialState,
    payload_count: u64,
    outboard_count: u64,
) -> Result<(roaring::RoaringTreemap, roaring::RoaringTreemap)> {
    if partial.version != RECORD_VERSION {
        return Err(Error::Other(format!(
            "unsupported blob partial-state version {}",
            partial.version
        )));
    }
    let payload = decode_chunk_indices(&partial.payload_indices)?;
    let outboard = decode_chunk_indices(&partial.outboard_indices)?;
    if payload.len() != payload_count || outboard.len() != outboard_count {
        return Err(Error::Other(
            "blob partial exact chunk sets disagree with manifest cardinalities".to_string(),
        ));
    }
    Ok((payload, outboard))
}

fn logical_block_count(size: u64) -> u64 {
    size.div_ceil(BLOB_LOGICAL_BLOCK_BYTES as u64)
}

fn validate_complete_block_count(size: u64, count: u64, role: &str) -> Result<()> {
    let expected = logical_block_count(size);
    if count != expected {
        return Err(Error::Other(format!(
            "complete blob {role} count {count} disagrees with size-implied count {expected}"
        )));
    }
    Ok(())
}

fn validate_staging_bind_metadata(
    staging: &BlobImportStaging,
    bind: &BlobStagingBind<'_>,
    generation: u64,
    payload_indices: &roaring::RoaringTreemap,
    outboard_indices: &roaring::RoaringTreemap,
    verified_payload_bytes: u64,
) -> Result<()> {
    if staging.provisional_generation != Some((bind.hash, generation)) {
        return Err(Error::Other(
            "blob staging bind lacks its exact durable hash-generation association".to_string(),
        ));
    }
    if staging
        .expected_size
        .is_some_and(|size| size != bind.total_size)
    {
        return Err(Error::Other(
            "blob staging declared size disagrees with its canonical manifest".to_string(),
        ));
    }
    if bind.validated_size > bind.total_size
        || bind.validated_size != verified_payload_bytes
        || staging.received_size != verified_payload_bytes
    {
        return Err(Error::Other(
            "blob validated size disagrees with its verified payload blocks".to_string(),
        ));
    }
    if payload_indices.len() != staging.payload_chunk_count
        || outboard_indices.len() != staging.outboard_chunk_count
    {
        return Err(Error::Other(
            "blob staging exact block sets disagree with their cardinalities".to_string(),
        ));
    }
    let payload_bound = logical_block_count(bind.total_size);
    let outboard_bound = logical_block_count(bind.outboard_size);
    if payload_indices.iter().any(|index| index >= payload_bound)
        || outboard_indices.iter().any(|index| index >= outboard_bound)
    {
        return Err(Error::Other(
            "blob staging contains a logical block outside its declared size".to_string(),
        ));
    }
    Ok(())
}

fn validate_staged_blocks_in_write(
    write: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    object_id: &[u8; 16],
    indices: &roaring::RoaringTreemap,
    logical_size: u64,
    role: &str,
) -> Result<u64> {
    if indices.is_empty() {
        return Ok(0);
    }
    let table = write
        .open_table(definition)
        .map_err(RedbPersistence::storage_error)?;
    let block_bytes = BLOB_LOGICAL_BLOCK_BYTES as u64;
    let block_count = logical_block_count(logical_size);
    let mut verified_bytes = 0u64;
    for index in indices {
        if index >= block_count {
            return Err(Error::Other(format!(
                "blob {role} block {index} is outside its declared size"
            )));
        }
        let key = BlobRepository::chunk_key(object_id, index);
        let value = table
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| Error::Other(format!("blob {role} block {index} is missing")))?;
        let offset = index.saturating_mul(block_bytes);
        let expected = logical_size.saturating_sub(offset).min(block_bytes);
        if value.value().len() as u64 != expected {
            return Err(Error::Other(format!(
                "blob {role} block {index} length disagrees with its deterministic offset"
            )));
        }
        verified_bytes = verified_bytes.saturating_add(expected);
    }
    Ok(verified_bytes)
}

fn validate_dense_indices(indices: &roaring::RoaringTreemap, count: u64, role: &str) -> Result<()> {
    if indices.len() != count || !indices.iter().eq(0..count) {
        return Err(Error::Other(format!(
            "complete blob {role} staging indices are not the dense range implied by its count"
        )));
    }
    Ok(())
}

/// Build versioned partial state from exact logical chunk indices.
#[doc(hidden)]
pub fn new_partial_state(
    payload_indices: impl IntoIterator<Item = u64>,
    outboard_indices: impl IntoIterator<Item = u64>,
    adapter_bitfield: Vec<u8>,
) -> Result<BlobPartialState> {
    let mut payload = roaring::RoaringTreemap::new();
    for index in payload_indices {
        payload.insert(index);
    }
    let mut outboard = roaring::RoaringTreemap::new();
    for index in outboard_indices {
        outboard.insert(index);
    }
    Ok(BlobPartialState {
        version: RECORD_VERSION,
        payload_indices: encode_chunk_indices(&payload)?,
        outboard_indices: encode_chunk_indices(&outboard)?,
        adapter_bitfield,
    })
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(value)
        .map_err(|err| Error::Other(format!("encode blob repository record: {err}")))
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes)
        .map_err(|err| Error::Other(format!("decode blob repository record: {err}")))
}

/// Construct an unbound staging record at a database frontier.
#[doc(hidden)]
pub fn new_import_staging(start_lsn: Lsn, expected_size: Option<u64>) -> Result<BlobImportStaging> {
    let empty = roaring::RoaringTreemap::new();
    Ok(BlobImportStaging {
        version: RECORD_VERSION,
        object_id: *uuid::Uuid::new_v4().as_bytes(),
        start_lsn: start_lsn.0,
        expected_size,
        received_size: 0,
        payload_chunk_count: 0,
        outboard_chunk_count: 0,
        payload_indices: encode_chunk_indices(&empty)?,
        outboard_indices: encode_chunk_indices(&empty)?,
        provisional_generation: None,
        adapter_state: Vec::new(),
    })
}
