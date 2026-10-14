//! Native storage for finite, restartable paged vector generations.
//!
//! A build owns one durable restart record per partition (`BUILDS`). Every
//! construction batch runs in ONE write transaction that compares the complete
//! previous record, applies the builder's point/identity/directory writes and
//! installs the successor record, so neighbour links never become durable
//! ahead of the progress that names them. Points, identities and the row and
//! exact-key directories live under a storage identity in `PAGES`; a published
//! generation is served from those same pages through a reader that holds one
//! captured writer root. Nothing here keeps a population-sized collection.
pub(crate) mod import_tail;

use super::vector_membership_pages;
use super::*;
use contextdb_vector::hnsw::paged_builder::{
    PagedEdge, PagedGenerationPersistence, PagedGenerationTransaction, PagedGraphState, PagedPoint,
    PagedVectorBatchPlan, PagedVectorBuildEntry, PagedVectorGraphSource, PagedVectorIdentity,
    PagedVectorPhase, PagedVectorPoint, PagedVectorProgress, PagedVectorReader,
    PagedVectorReclamation, PagedVectorTransaction, PagedVectorValues, PointId, decode_paged_point,
    encode_paged_point_into,
};
use contextdb_vector::hnsw::paged_builder::{PagedVectorReadRequest, PagedVectorReadValue};
use contextdb_vector::{ScopedVectorCopy, VectorCopyAdmission};
use std::cell::RefCell;
use std::ops::Bound;

pub(super) const PAGES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_paged_generation_pages");
pub(super) const BUILDS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_paged_builds");
pub(super) const CLEANUP: TableDefinition<u64, &[u8]> =
    TableDefinition::new("vector_paged_cleanup");
const SCRATCH: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_paged_search_scratch");
const COUNTERS: TableDefinition<&str, u64> = TableDefinition::new("vector_paged_counters");
/// Frontier of an in-flight whole (resident) build, per partition route.
const FLOORS: TableDefinition<&[u8], u64> = TableDefinition::new("vector_build_floors");
/// A source cursor cannot have this length/value. Incomplete resident exports
/// are discarded on restart; their pages remain owned by BUILDS until queued
/// for cleanup. Complete exports clear it before native publication.
pub(crate) const RESIDENT_EXPORT_MARKER: &[u8] = b"resident-export";

const NEXT_STORAGE: &str = "next_storage_id";

// A native import tail owns its byte table. Its catalog authenticates the
// physical root, so publishing a successor examines no preceding point pages.
// Reserve a namespace that still fits the existing SQL INTEGER inspection contract.
pub(crate) const MUTABLE_STORAGE: u64 = 1 << 62;
pub(crate) const MUTABLE_PAGE_SET: u64 = u64::MAX;

pub(super) fn page_table_name(storage_id: u64) -> String {
    if storage_id & MUTABLE_STORAGE != 0 {
        format!("vector_paged_tail_{storage_id:016x}")
    } else {
        PAGES.name().to_owned()
    }
}

const KIND_POINT: u8 = 1;
const KIND_IDENTITY: u8 = 2;
const KIND_ROW: u8 = 3;
const KIND_EXACT: u8 = 4;
const SCRATCH_VISITED: u8 = 1;
const SCRATCH_CANDIDATE: u8 = 2;
const CHECK_BYTES: usize = 16;
pub(crate) const META_MAGIC: [u8; 8] = *b"CDBVPG01";
/// Distinct from the whole-envelope graph format. A base generation record
/// carrying this version names native paged storage through its meta bytes.
pub(crate) const VECTOR_GRAPH_PAGED_FORMAT_VERSION: u16 = 0x0100;
/// Canonical source key width (`canonical_source_key`).
const CANONICAL_BYTES: usize = 32;
/// Physical source companion suffix width (`vector_membership_pages::source_key`).
const SOURCE_SUFFIX_BYTES: usize = 24;

pub(crate) fn corruption(reason: impl Into<String>) -> Error {
    Error::StoreCorrupted {
        path: "vector_paged_generation_pages".into(),
        reason: reason.into(),
    }
}

/// Column declaration a paged generation was built under. Its digest is the
/// restart record's `declaration_digest`; a changed declaration abandons the
/// build instead of mixing graphs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PagedDeclaration {
    pub(crate) dimension: u32,
    pub(crate) quantization: VectorQuantization,
    pub(crate) hnsw_m: u32,
    pub(crate) hnsw_ef_construction: u32,
    pub(crate) hnsw_ef_search: u32,
    pub(crate) policy_revision: u64,
}

impl PagedDeclaration {
    pub(crate) fn digest(&self) -> Result<u64> {
        let bytes = RedbPersistence::encode(self)?;
        let hash = blake3::hash(&bytes);
        Ok(u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap()))
    }
}

/// Why a build was started; restored verbatim into the reported progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PagedBuildReason {
    InitialBuild,
    Tombstones,
    NewChanges,
    PolicyChange,
}

impl PagedBuildReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InitialBuild => "initial_build",
            Self::Tombstones => "tombstones",
            Self::NewChanges => "new_changes",
            Self::PolicyChange => "policy_change",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProgressRecord {
    pub(super) generation_id: u64,
    pub(super) declaration_digest: u64,
    pub(super) frontier_tx: u64,
    pub(super) frontier_lsn: u64,
    pub(super) graph: PagedGraphState,
    pub(super) last_source_key: Option<Vec<u8>>,
    pub(super) total_points: u64,
    pub(super) phase: u8,
    pub(super) encoding_cursor: Option<(u8, i32)>,
    pub(super) encoding_bytes: u64,
    pub(super) encoded_points: u64,
    pub(super) encoding_digest: u64,
    pub(super) cleanup_cursor: Option<Vec<u8>>,
}

/// The graph state contains only scalars, a fixed layer-count array and an
/// optional fixed PointId. The two progress cursors are the only variable
/// fields. Inspect the complete positional record without allocating either.
#[allow(dead_code)]
#[derive(Deserialize)]
struct InspectedProgressRecord {
    generation_id: u64,
    declaration_digest: u64,
    frontier_tx: u64,
    frontier_lsn: u64,
    graph: PagedGraphState,
    last_source_key: Option<InspectedSequence<u8>>,
    total_points: u64,
    phase: u8,
    encoding_cursor: Option<(u8, i32)>,
    encoding_bytes: u64,
    encoded_points: u64,
    encoding_digest: u64,
    cleanup_cursor: Option<InspectedSequence<u8>>,
}

fn decode_image_build_record(payload: &[u8]) -> Result<PagedBuildRecord> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    type Record = (
        u64,
        u64,
        PagedBuildReason,
        PagedDeclaration,
        InspectedProgressRecord,
        VectorSchemaBinding,
    );
    let (_, consumed) =
        bincode::serde::borrow_decode_from_slice::<Record, _>(payload, bincode::config::standard())
            .map_err(|_| corruption("paged build record cannot be decoded"))?;
    if consumed != payload.len() {
        return Err(corruption("paged build record has trailing bytes"));
    }
    // Callers hold the actual operation's encoded-byte reservation through all
    // uses of the owned record. Complete inspection proves each allocation hint
    // has its elements in that already admitted input before serde can reserve.
    let record: PagedBuildRecord = RedbPersistence::decode_exact(payload)
        .map_err(|_| corruption("paged build record cannot be decoded"))?;
    if record.storage_id == 0 {
        return Err(corruption("paged build storage identity is reserved"));
    }
    Ok(record)
}

fn phase_code(phase: PagedVectorPhase) -> u8 {
    match phase {
        PagedVectorPhase::Inserting => 0,
        PagedVectorPhase::Encoding => 1,
        PagedVectorPhase::Complete => 2,
        PagedVectorPhase::Published => 3,
        PagedVectorPhase::Abandoned => 4,
        PagedVectorPhase::Reclaiming => 5,
        PagedVectorPhase::Reclaimed => 6,
    }
}

fn phase_from_code(code: u8) -> Result<PagedVectorPhase> {
    Ok(match code {
        0 => PagedVectorPhase::Inserting,
        1 => PagedVectorPhase::Encoding,
        2 => PagedVectorPhase::Complete,
        3 => PagedVectorPhase::Published,
        4 => PagedVectorPhase::Abandoned,
        5 => PagedVectorPhase::Reclaiming,
        6 => PagedVectorPhase::Reclaimed,
        _ => return Err(corruption("paged build record has an unknown phase")),
    })
}

impl ProgressRecord {
    pub(crate) fn from_progress(progress: &PagedVectorProgress) -> Self {
        Self {
            generation_id: progress.generation_id,
            declaration_digest: progress.declaration_digest,
            frontier_tx: progress.frontier_tx.0,
            frontier_lsn: progress.frontier_lsn.0,
            graph: progress.graph.clone(),
            last_source_key: progress.last_source_key.clone(),
            total_points: progress.total_points as u64,
            phase: phase_code(progress.phase),
            encoding_cursor: progress.encoding_cursor.map(|id| (id.0, id.1)),
            encoding_bytes: progress.encoding_bytes,
            encoded_points: progress.encoded_points as u64,
            encoding_digest: progress.encoding_digest,
            cleanup_cursor: progress.cleanup_cursor.clone(),
        }
    }

    fn to_progress(&self) -> Result<PagedVectorProgress> {
        let total_points = usize::try_from(self.total_points)
            .map_err(|_| corruption("paged build point total exceeds usize"))?;
        let encoded_points = usize::try_from(self.encoded_points)
            .map_err(|_| corruption("paged build encoded total exceeds usize"))?;
        Ok(PagedVectorProgress {
            generation_id: self.generation_id,
            declaration_digest: self.declaration_digest,
            frontier_tx: TxId(self.frontier_tx),
            frontier_lsn: Lsn(self.frontier_lsn),
            graph: self.graph.clone(),
            last_source_key: self.last_source_key.clone(),
            total_points,
            phase: phase_from_code(self.phase)?,
            encoding_cursor: self
                .encoding_cursor
                .map(|(layer, rank)| PointId(layer, rank)),
            encoding_bytes: self.encoding_bytes,
            encoded_points,
            encoding_digest: self.encoding_digest,
            cleanup_cursor: self.cleanup_cursor.clone(),
        })
    }
}

/// The durable restart record of one partition's unpublished build.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PagedBuildRecord {
    pub(crate) storage_id: u64,
    /// The catalog generation this build replaces; zero when none existed.
    pub(crate) previous_generation_id: u64,
    pub(crate) reason: PagedBuildReason,
    pub(crate) declaration: PagedDeclaration,
    pub(crate) progress: ProgressRecord,
    pub(crate) schema_binding: VectorSchemaBinding,
}

impl PagedBuildRecord {
    pub(crate) fn progress(&self) -> Result<PagedVectorProgress> {
        if self.storage_id == 0 {
            return Err(corruption("paged build storage identity is reserved"));
        }
        self.progress.to_progress()
    }
}

/// Graph bytes of a base generation record in the paged format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PagedGenerationMeta {
    pub(super) magic: [u8; 8],
    pub(crate) storage_id: u64,
    pub(crate) declaration: PagedDeclaration,
    pub(crate) progress: ProgressRecord,
    pub(crate) saved: Option<SavedGenerationBinding>,
    pub(crate) schema_binding: VectorSchemaBinding,
    pub(crate) page_set: PageSetRoot,
}

/// The final root over one published generation's whole page range, taken
/// after its last page write. A load verifies it once, so damage to any page
/// of this route refuses this route and no other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PageSetRoot {
    pub(crate) pages: u64,
    pub(crate) bytes: u64,
    pub(crate) digest: [u8; 32],
}

/// Incremental root over one storage range in key order. Saved exports and
/// native publication both use it, so one verifier covers both.
pub(crate) struct PageSetHasher {
    hasher: blake3::Hasher,
    pages: u64,
    bytes: u64,
}
impl PageSetHasher {
    pub(crate) fn new() -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"contextdb.paged.page-set.v1");
        Self {
            hasher,
            pages: 0,
            bytes: 0,
        }
    }
    pub(crate) fn page(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.hasher.update(&(key.len() as u64).to_be_bytes());
        self.hasher.update(key);
        self.hasher.update(&(value.len() as u64).to_be_bytes());
        self.hasher.update(value);
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| corruption("page set count overflow"))?;
        self.bytes = self
            .bytes
            .checked_add((key.len() + value.len()) as u64)
            .ok_or_else(|| corruption("page set byte count overflow"))?;
        Ok(())
    }
    pub(crate) fn finish(self) -> PageSetRoot {
        PageSetRoot {
            pages: self.pages,
            bytes: self.bytes,
            digest: *self.hasher.finalize().as_bytes(),
        }
    }
}

/// Proofs that a committed page set verified against an unchanged page table.
/// Pages under a storage id never change once published, so a proof holds
/// until the page table's own root moves. Fixed size: the oldest proof is
/// dropped first.
const VERIFIED_PAGE_SETS: usize = 256;
static VERIFIED_PAGE_SET_PROOFS: parking_lot::Mutex<
    std::collections::VecDeque<(u64, [u8; 32], [u8; 32])>,
> = parking_lot::Mutex::new(std::collections::VecDeque::new());

fn page_set_proven(storage_id: u64, root: &PageSetRoot, table: [u8; 32]) -> bool {
    VERIFIED_PAGE_SET_PROOFS
        .lock()
        .iter()
        .any(|proof| *proof == (storage_id, root.digest, table))
}

fn record_page_set_proof(storage_id: u64, root: &PageSetRoot, table: [u8; 32]) {
    let mut proofs = VERIFIED_PAGE_SET_PROOFS.lock();
    if proofs.len() == VERIFIED_PAGE_SETS {
        proofs.pop_front();
    }
    proofs.push_back((storage_id, root.digest, table));
}

fn route_page_corrupt() -> Error {
    Error::StoreCorrupted {
        path: "vector_paged_generation_pages".into(),
        reason: "vector partition graph physical page is corrupt".into(),
    }
}

/// Root over a published storage's final page range, read in the caller's
/// transaction after the build's last page write.
pub(crate) fn page_set_root_in<T: redb::ReadableTable<&'static [u8], &'static [u8]>>(
    pages: &T,
    storage_id: u64,
) -> Result<PageSetRoot> {
    let storage = storage_prefix(storage_id);
    let end = writer_source::prefix_end(&storage)
        .ok_or_else(|| corruption("paged storage has no successor"))?;
    let mut root = PageSetHasher::new();
    for entry in pages
        .range::<&[u8]>(storage.as_slice()..end.as_slice())
        .map_err(RedbPersistence::storage_error)?
    {
        let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
        root.page(key.value(), value.value())?;
    }
    Ok(root.finish())
}

/// Walk an untrusted storage range against its committed root. The page
/// count may never pass the committed count, and every page must fit the
/// window admitted for it before it is hashed. Returns (pages, bytes) walked.
fn verify_page_set<T: redb::ReadableTable<&'static [u8], &'static [u8]>>(
    pages: &T,
    storage: &[u8; 8],
    committed: &PageSetRoot,
    page_window: usize,
    admit: &mut dyn FnMut(usize) -> Result<Option<VectorCopyAdmission>>,
) -> Result<(u64, u64)> {
    let end = writer_source::prefix_end(storage)
        .ok_or_else(|| corruption("paged storage has no successor"))?;
    let mut root = PageSetHasher::new();
    for entry in pages
        .range::<&[u8]>(storage.as_slice()..end.as_slice())
        .map_err(RedbPersistence::storage_error)?
    {
        if root.pages >= committed.pages {
            return Err(route_page_corrupt());
        }
        let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
        let len = key.value().len().saturating_add(value.value().len());
        if len > page_window {
            return Err(route_page_corrupt());
        }
        let _charge = admit(len)?;
        root.page(key.value(), value.value())?;
    }
    let walked = (root.pages, root.bytes);
    #[cfg(any(test, feature = "test-seams"))]
    note_route_verification(walked.0, walked.1);
    contextdb_core::find_attribution::note_hash_pass(
        "graph_route_page_set_hashed_bytes",
        usize::try_from(walked.1).unwrap_or(usize::MAX),
    );
    let finished = root.finish();
    if finished != *committed {
        return Err(route_page_corrupt());
    }
    Ok(walked)
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static ROUTE_VERIFICATIONS: std::cell::Cell<(u64, u64, u64)> =
        const { std::cell::Cell::new((0, 0, 0)) };
}

#[cfg(any(test, feature = "test-seams"))]
fn note_route_verification(pages: u64, bytes: u64) {
    ROUTE_VERIFICATIONS.with(|counters| {
        let (walks, walked, hashed) = counters.get();
        counters.set((walks + 1, walked + pages, hashed + bytes));
    });
}

/// (verifications, pages walked, bytes hashed) by route page-set checks on
/// this thread.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn route_verifications_for_test() -> (u64, u64, u64) {
    ROUTE_VERIFICATIONS.with(std::cell::Cell::get)
}

/// Immutable format-1 publication binding; every field has fixed width.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SavedGenerationBinding {
    pub(crate) route: [u8; 32],
    pub(crate) is_base: bool,
    pub(crate) base_generation_id: u64,
    pub(crate) base_tx: TxId,
    pub(crate) base_lsn: Lsn,
    pub(crate) source_digest: [u8; 32],
    pub(crate) distinct_rows: u64,
    pub(crate) record_bytes: u64,
    pub(crate) record_digest: [u8; 32],
    pub(crate) node_limit: u64,
    pub(crate) extend_candidates: bool,
    pub(crate) keep_pruned: bool,
    pub(crate) max_layer: u8,
    pub(crate) scale_bits: u64,
}

impl PagedGenerationMeta {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        #[cfg(any(test, feature = "test-seams"))]
        contextdb_vector::hnsw::observe_paged_storage_work(
            contextdb_vector::hnsw::PagedVectorStorageWork::DescriptorDecode,
            bytes.len(),
        );
        type Meta = (
            [u8; 8],
            u64,
            PagedDeclaration,
            InspectedProgressRecord,
            Option<SavedGenerationBinding>,
            VectorSchemaBinding,
            PageSetRoot,
        );
        let ((_, _, _, inspected, _, _, _), consumed) =
            bincode::serde::borrow_decode_from_slice::<Meta, _>(bytes, bincode::config::standard())
                .map_err(|_| corruption("paged generation metadata cannot be decoded"))?;
        if consumed != bytes.len()
            || inspected.last_source_key.is_some()
            || inspected.cleanup_cursor.is_some()
        {
            return Err(corruption(
                "paged generation metadata has trailing bytes or construction cursors",
            ));
        }
        let meta: Self = RedbPersistence::decode_exact(bytes)
            .map_err(|_| corruption("paged generation metadata cannot be decoded"))?;
        if meta.storage_id == 0 {
            return Err(corruption("paged generation storage identity is reserved"));
        }
        if meta.magic != META_MAGIC {
            return Err(corruption("paged generation metadata has the wrong magic"));
        }
        Ok(meta)
    }

    pub(crate) fn progress(&self) -> Result<PagedVectorProgress> {
        if self.storage_id == 0 {
            return Err(corruption("paged build storage identity is reserved"));
        }
        self.progress.to_progress()
    }
}

/// A saved graph's point pages are its physical graph pages. A page that
/// fails its keyed check is reported as such, never as a missing route.
fn open_graph_page<'v>(key: &[u8], value: &'v [u8]) -> Result<&'v [u8]> {
    open_value(key, value).map_err(|_| Error::StoreCorrupted {
        path: "vector_paged_generation_pages".into(),
        reason: "vector partition graph physical page is corrupt".into(),
    })
}

/// Storage of a paged generation record's graph bytes, or None when the
/// graph is stored inline.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn paged_storage_of_graph_for_test(
    graph_format_version: u16,
    graph_bytes: &[u8],
) -> Result<Option<u64>> {
    if graph_format_version != VECTOR_GRAPH_PAGED_FORMAT_VERSION
        && !graph_bytes.starts_with(&META_MAGIC)
    {
        return Ok(None);
    }
    Ok(Some(PagedGenerationMeta::decode(graph_bytes)?.storage_id))
}

#[cfg(any(test, feature = "test-seams"))]
fn first_page_in_write(
    pages: &redb::Table<&'static [u8], &'static [u8]>,
    storage: &[u8; 8],
    kind: Option<u8>,
    last: bool,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut start = storage.to_vec();
    let mut end = storage.to_vec();
    match kind {
        Some(kind) => {
            start.push(kind);
            end.push(kind + 1);
        }
        None => {
            end = writer_source::prefix_end(storage)
                .ok_or_else(|| Error::Other("paged storage has no successor".into()))?;
        }
    }
    let mut range = pages
        .range(start.as_slice()..end.as_slice())
        .map_err(RedbPersistence::storage_error)?;
    let entry = if last {
        range.next_back()
    } else {
        range.next()
    };
    let (key, value) = entry
        .ok_or_else(|| Error::Other("paged saved graph has no such page".into()))?
        .map_err(RedbPersistence::storage_error)?;
    Ok((key.value().to_vec(), value.value().to_vec()))
}

/// Physical damage to one route's committed page set, made without
/// rebinding any root.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn damage_route_pages_in_write_for_test(
    write: &redb::WriteTransaction,
    storage_id: u64,
    other_storage_id: Option<u64>,
    fault: crate::persistence::PagedRouteFaultForTest,
) -> Result<()> {
    use crate::persistence::PagedRouteFaultForTest as Fault;
    let storage = storage_prefix(storage_id);
    let name = page_table_name(storage_id);
    let mut pages = write
        .open_table(TableDefinition::<&[u8], &[u8]>::new(&name))
        .map_err(RedbPersistence::storage_error)?;
    let (key, value) = match fault {
        Fault::FlipFirstPointPage => {
            let (key, mut value) = first_page_in_write(&pages, &storage, Some(KIND_POINT), false)?;
            *value
                .first_mut()
                .ok_or_else(|| Error::Other("paged point page is empty".into()))? ^= 0xff;
            (key, value)
        }
        Fault::FlipLastPage => {
            let (key, mut value) = first_page_in_write(&pages, &storage, None, true)?;
            let at = value.len().saturating_sub(1);
            *value
                .get_mut(at)
                .ok_or_else(|| Error::Other("paged page is empty".into()))? ^= 0xff;
            (key, value)
        }
        Fault::ResealFirstPointPage => {
            // A well-formed page under the same key: the keyed check passes.
            let (key, value) = first_page_in_write(&pages, &storage, Some(KIND_POINT), false)?;
            let mut payload = open_value(&key, &value)?.to_vec();
            *payload
                .last_mut()
                .ok_or_else(|| Error::Other("paged point payload is empty".into()))? ^= 0x01;
            let sealed = seal_value(&key, &payload);
            (key, sealed)
        }
        Fault::CrossRouteFirstPointPage => {
            let other =
                storage_prefix(other_storage_id.ok_or_else(|| {
                    Error::Other("cross-route damage names no other route".into())
                })?);
            let (key, _) = first_page_in_write(&pages, &storage, Some(KIND_POINT), false)?;
            let (other_key, other_value) =
                first_page_in_write(&pages, &other, Some(KIND_POINT), false)?;
            let payload = open_value(&other_key, &other_value)?.to_vec();
            let sealed = seal_value(&key, &payload);
            (key, sealed)
        }
        Fault::ExtraPage => {
            let (mut key, value) = first_page_in_write(&pages, &storage, None, true)?;
            key.push(0);
            let payload = open_value(&key[..key.len() - 1], &value)?.to_vec();
            let sealed = seal_value(&key, &payload);
            (key, sealed)
        }
        Fault::OversizedPage => {
            let (key, _) = first_page_in_write(&pages, &storage, Some(KIND_POINT), false)?;
            let sealed = seal_value(&key, &vec![0u8; 4 * 1024 * 1024]);
            (key, sealed)
        }
    };
    pages
        .insert(key.as_slice(), value.as_slice())
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}

pub(super) fn storage_prefix(storage_id: u64) -> [u8; 8] {
    storage_id.to_be_bytes()
}

pub(super) fn point_key(storage: &[u8; 8], id: PointId) -> [u8; 14] {
    let mut key = [0u8; 14];
    key[..8].copy_from_slice(storage);
    key[8] = KIND_POINT;
    key[9] = id.0;
    key[10..14].copy_from_slice(&((id.1 as u32) ^ 0x8000_0000).to_be_bytes());
    key
}

pub(super) fn identity_key(storage: &[u8; 8], data_id: usize) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[..8].copy_from_slice(storage);
    key[8] = KIND_IDENTITY;
    key[9..17].copy_from_slice(&(data_id as u64).to_be_bytes());
    key
}

pub(super) fn row_key(storage: &[u8; 8], row: RowId, data_id: usize) -> [u8; 25] {
    let mut key = [0u8; 25];
    key[..8].copy_from_slice(storage);
    key[8] = KIND_ROW;
    key[9..17].copy_from_slice(&row.0.to_be_bytes());
    key[17..25].copy_from_slice(&(data_id as u64).to_be_bytes());
    key
}

pub(super) fn kind_prefix(storage: &[u8; 8], kind: u8) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[..8].copy_from_slice(storage);
    key[8] = kind;
    key
}

fn exact_prefix(storage: &[u8; 8], exact: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(13 + exact.len() + 16);
    key.extend_from_slice(storage);
    key.push(KIND_EXACT);
    key.extend_from_slice(&(exact.len() as u32).to_be_bytes());
    key.extend_from_slice(exact);
    key
}

pub(super) fn exact_key(storage: &[u8; 8], exact: &[u8], row: RowId, data_id: usize) -> Vec<u8> {
    let mut key = exact_prefix(storage, exact);
    key.extend_from_slice(&row.0.to_be_bytes());
    key.extend_from_slice(&(data_id as u64).to_be_bytes());
    key
}

/// Every page value carries a keyed check: a page moved under another key, a
/// truncated body or a flipped byte is a typed corruption, never a graph.
pub(super) fn seal_value(key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(payload);
    let mut value = Vec::with_capacity(payload.len() + CHECK_BYTES);
    value.extend_from_slice(payload);
    value.extend_from_slice(&hash.finalize().as_bytes()[..CHECK_BYTES]);
    value
}

pub(super) fn open_value<'v>(key: &[u8], value: &'v [u8]) -> Result<&'v [u8]> {
    #[cfg(any(test, feature = "test-seams"))]
    contextdb_vector::hnsw::observe_paged_storage_work(
        contextdb_vector::hnsw::PagedVectorStorageWork::Authentication,
        value.len(),
    );
    if value.len() < CHECK_BYTES {
        return Err(corruption("paged vector page is truncated"));
    }
    let (payload, check) = value.split_at(value.len() - CHECK_BYTES);
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(payload);
    if &hash.finalize().as_bytes()[..CHECK_BYTES] != check {
        return Err(corruption("paged vector page check does not match its key"));
    }
    Ok(payload)
}

fn encode_identity(identity: &PagedVectorIdentity) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(54 + identity.exact_key.len());
    bytes.push(identity.point.0);
    bytes.extend_from_slice(&identity.point.1.to_be_bytes());
    bytes.extend_from_slice(&(identity.data_id as u64).to_be_bytes());
    bytes.extend_from_slice(&identity.row_id.0.to_be_bytes());
    bytes.extend_from_slice(&identity.lsn.0.to_be_bytes());
    bytes.extend_from_slice(&identity.created_tx.0.to_be_bytes());
    match identity.deleted_tx {
        Some(tx) => {
            bytes.push(1);
            bytes.extend_from_slice(&tx.0.to_be_bytes());
        }
        None => {
            bytes.push(0);
            bytes.extend_from_slice(&0u64.to_be_bytes());
        }
    }
    bytes.extend_from_slice(&(identity.exact_key.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&identity.exact_key);
    bytes
}

fn decode_identity(bytes: &[u8]) -> Result<PagedVectorIdentity> {
    let invalid = || corruption("paged vector identity record is invalid");
    if bytes.len() < 50 {
        return Err(invalid());
    }
    let u64_at = |at: usize| u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap());
    let point = PointId(
        bytes[0],
        i32::from_be_bytes(bytes[1..5].try_into().unwrap()),
    );
    let data_id = usize::try_from(u64_at(5)).map_err(|_| invalid())?;
    let deleted_tx = match bytes[37] {
        0 => None,
        1 => Some(TxId(u64_at(38))),
        _ => return Err(invalid()),
    };
    let len = u32::from_be_bytes(bytes[46..50].try_into().unwrap()) as usize;
    if bytes.len() != 50 + len || point.0 >= 16 || point.1 < 0 {
        return Err(invalid());
    }
    Ok(PagedVectorIdentity {
        point,
        data_id,
        row_id: RowId(u64_at(13)),
        lsn: Lsn(u64_at(21)),
        created_tx: TxId(u64_at(29)),
        deleted_tx,
        exact_key: bytes[50..].to_vec(),
    })
}

fn encode_point_id(id: PointId) -> [u8; 5] {
    let mut bytes = [0u8; 5];
    bytes[0] = id.0;
    bytes[1..5].copy_from_slice(&id.1.to_be_bytes());
    bytes
}

fn decode_point_id(bytes: &[u8]) -> Result<PointId> {
    if bytes.len() != 5 || bytes[0] >= 16 {
        return Err(corruption("paged vector row directory entry is invalid"));
    }
    let rank = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
    if rank < 0 {
        return Err(corruption("paged vector row directory entry is invalid"));
    }
    Ok(PointId(bytes[0], rank))
}

/// Order-preserving candidate key: `PagedEdge`'s total order (distance by
/// `total_cmp`, then point, then origin), then an insertion sequence that keeps
/// duplicate heap entries distinct. The largest key is the heap maximum.
fn candidate_key(edge: &PagedEdge, sequence: u64) -> [u8; 30] {
    let bits = edge.distance.to_bits();
    let ordered = if bits & 0x8000_0000 != 0 {
        !bits
    } else {
        bits | 0x8000_0000
    };
    let mut key = [0u8; 30];
    key[0] = SCRATCH_CANDIDATE;
    key[1..5].copy_from_slice(&ordered.to_be_bytes());
    key[5] = edge.point_id.0;
    key[6..10].copy_from_slice(&((edge.point_id.1 as u32) ^ 0x8000_0000).to_be_bytes());
    key[10..18].copy_from_slice(&(edge.origin_id as u64).to_be_bytes());
    key[18..26].copy_from_slice(&sequence.to_be_bytes());
    key[26..30].copy_from_slice(&[0; 4]);
    key
}

fn edge_from_candidate_key(key: &[u8]) -> Result<PagedEdge> {
    if key.len() != 30 || key[0] != SCRATCH_CANDIDATE {
        return Err(corruption("paged search scratch candidate key is invalid"));
    }
    let ordered = u32::from_be_bytes(key[1..5].try_into().unwrap());
    let bits = if ordered & 0x8000_0000 != 0 {
        ordered & 0x7fff_ffff
    } else {
        !ordered
    };
    let rank = (u32::from_be_bytes(key[6..10].try_into().unwrap()) ^ 0x8000_0000) as i32;
    Ok(PagedEdge {
        point_id: PointId(key[5], rank),
        origin_id: usize::try_from(u64::from_be_bytes(key[10..18].try_into().unwrap()))
            .map_err(|_| corruption("paged search scratch origin exceeds usize"))?,
        distance: f32::from_bits(bits),
    })
}

fn visited_key(id: PointId) -> [u8; 6] {
    let mut key = [0u8; 6];
    key[0] = SCRATCH_VISITED;
    key[1] = id.0;
    key[2..6].copy_from_slice(&((id.1 as u32) ^ 0x8000_0000).to_be_bytes());
    key
}

fn identity_visible(identity: &PagedVectorIdentity, snapshot: SnapshotId) -> bool {
    let tx = TxId::from_snapshot(snapshot);
    identity.created_tx <= tx && identity.deleted_tx.is_none_or(|deleted| deleted > tx)
}

/// One storage-guard-scoped admission before any native owned page copy.
pub(super) trait PageSource {
    fn admitted_page(
        &self,
        key: &[u8],
        before_copy: &mut dyn FnMut(&[u8]) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<Option<ScopedVectorCopy<Vec<u8>>>>;
    fn first_after(
        &self,
        lower: Bound<&[u8]>,
        upper: &[u8],
        before_copy: &mut dyn FnMut(&[u8], &[u8]) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<Option<ScopedVectorCopy<(Vec<u8>, Vec<u8>)>>>;
    fn last_before(
        &self,
        lower: &[u8],
        upper: &[u8],
        before_copy: &mut dyn FnMut(&[u8], &[u8]) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<Option<ScopedVectorCopy<(Vec<u8>, Vec<u8>)>>>;
}
impl<T: ReadableTable<&'static [u8], &'static [u8]>> PageSource for T {
    fn admitted_page(
        &self,
        key: &[u8],
        before_copy: &mut dyn FnMut(&[u8]) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<Option<ScopedVectorCopy<Vec<u8>>>> {
        let value = self.get(key).map_err(RedbPersistence::storage_error)?;
        match value {
            Some(value) => {
                #[cfg(any(test, feature = "test-seams"))]
                contextdb_vector::hnsw::observe_paged_storage_work(
                    contextdb_vector::hnsw::PagedVectorStorageWork::SourceRead,
                    value.value().len(),
                );
                let admission = before_copy(value.value())?;
                Ok(Some(ScopedVectorCopy::from_admitted(
                    value.value().to_vec(),
                    admission,
                )))
            }
            None => Ok(None),
        }
    }
    fn first_after(
        &self,
        lower: Bound<&[u8]>,
        upper: &[u8],
        before_copy: &mut dyn FnMut(&[u8], &[u8]) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<Option<ScopedVectorCopy<(Vec<u8>, Vec<u8>)>>> {
        let mut range = self
            .range::<&[u8]>((lower, Bound::Excluded(upper)))
            .map_err(RedbPersistence::storage_error)?;
        Ok(match range.next() {
            Some(entry) => {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                #[cfg(any(test, feature = "test-seams"))]
                contextdb_vector::hnsw::observe_paged_storage_work(
                    contextdb_vector::hnsw::PagedVectorStorageWork::SourceRead,
                    value.value().len(),
                );
                let admission = before_copy(key.value(), value.value())?;
                Some(ScopedVectorCopy::from_admitted(
                    (key.value().to_vec(), value.value().to_vec()),
                    admission,
                ))
            }
            None => None,
        })
    }
    fn last_before(
        &self,
        lower: &[u8],
        upper: &[u8],
        before_copy: &mut dyn FnMut(&[u8], &[u8]) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<Option<ScopedVectorCopy<(Vec<u8>, Vec<u8>)>>> {
        let mut range = self
            .range::<&[u8]>((Bound::Included(lower), Bound::Excluded(upper)))
            .map_err(RedbPersistence::storage_error)?;
        Ok(match range.next_back() {
            Some(entry) => {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                #[cfg(any(test, feature = "test-seams"))]
                contextdb_vector::hnsw::observe_paged_storage_work(
                    contextdb_vector::hnsw::PagedVectorStorageWork::SourceRead,
                    value.value().len(),
                );
                let admission = before_copy(key.value(), value.value())?;
                Some(ScopedVectorCopy::from_admitted(
                    (key.value().to_vec(), value.value().to_vec()),
                    admission,
                ))
            }
            None => None,
        })
    }
}

/// Point, identity and directory reads common to construction and serving.
pub(super) struct PagedLookup<'s, 'a, S: PageSource> {
    pub(super) admission:
        Option<&'s RefCell<&'a mut dyn FnMut(usize) -> Result<Option<VectorCopyAdmission>>>>,
    pub(super) pages: &'s S,
    pub(super) storage: [u8; 8],
    pub(super) dimension: usize,
    pub(super) quantization: VectorQuantization,
    pub(super) node_limit: usize,
}

// This layout mirrors the fork's private decoded neighbour fields. Both
// old decoded neighbours and new PagedEdge representations are funded.
#[allow(dead_code)]
struct DecodedNeighbourLayout {
    origin: usize,
    point: (u8, i32),
    distance: f32,
}

/// Authenticate before this structural borrowed-frame walk. Every fixed-int
/// length must fit its exact bytes before serde sees a length or allocates.
/// Three times the greater of a nonempty sequence's real length and Vec's
/// minimum capacity bounds final capacity and overlapping reallocations.
fn point_decode_window(payload: &[u8], value_bytes: usize) -> Result<usize> {
    let invalid = || corruption("paged point encoded lengths or window size are invalid");
    if payload.len() < 13
        || payload[8] >= 16
        || i32::from_le_bytes(payload[9..13].try_into().unwrap()) < 0
    {
        return Err(invalid());
    }
    let mut at = 13usize;
    let count = |at: &mut usize| -> Result<usize> {
        let end = at.checked_add(8).ok_or_else(invalid)?;
        let bytes = payload.get(*at..end).ok_or_else(invalid)?;
        *at = end;
        usize::try_from(u64::from_le_bytes(bytes.try_into().unwrap())).map_err(|_| invalid())
    };
    let growth = |n: usize, bytes: usize, minimum: usize| -> Result<usize> {
        if n == 0 {
            return Ok(0);
        }
        n.max(minimum)
            .checked_mul(bytes)
            .and_then(|v| v.checked_mul(3))
            .ok_or_else(invalid)
    };
    let values = count(&mut at)?;
    at = at
        .checked_add(values.checked_mul(value_bytes).ok_or_else(invalid)?)
        .ok_or_else(invalid)?;
    if at > payload.len() {
        return Err(invalid());
    }
    let layers = count(&mut at)?;
    if layers > 16 {
        return Err(invalid());
    }
    let header = std::mem::size_of::<Vec<PagedEdge>>();
    let old_edge = std::mem::size_of::<DecodedNeighbourLayout>();
    let new_edge = std::mem::size_of::<PagedEdge>();
    let mut decoded = growth(values, value_bytes, if value_bytes == 1 { 8 } else { 4 })?
        .checked_add(growth(layers, header, 4)?)
        .ok_or_else(invalid)?;
    let mut conversion = layers.checked_mul(header).ok_or_else(invalid)?;
    for _ in 0..layers {
        let edges = count(&mut at)?;
        at = at
            .checked_add(edges.checked_mul(17).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        if at > payload.len() {
            return Err(invalid());
        }
        decoded = decoded
            .checked_add(growth(edges, old_edge, 4)?)
            .ok_or_else(invalid)?;
        conversion = conversion
            .checked_add(edges.checked_mul(new_edge).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
    }
    if at != payload.len() {
        return Err(invalid());
    }
    decoded
        .checked_add(conversion)
        .and_then(|n| n.checked_add(std::mem::size_of::<PagedVectorPoint>()))
        .ok_or_else(invalid)
}

impl<S: PageSource> PagedLookup<'_, '_, S> {
    fn admit(&self, bytes: usize) -> Result<Option<VectorCopyAdmission>> {
        match self.admission {
            Some(admit) => (admit.borrow_mut())(bytes),
            None => Ok(None),
        }
    }
    fn directory_admission(&self, key: &[u8], frame: &[u8]) -> Result<Option<VectorCopyAdmission>> {
        let payload = open_value(key, frame)?;
        if key.len() < 9 || key[..8] != self.storage {
            return Err(corruption("paged directory key is invalid"));
        }
        match key[8] {
            KIND_ROW => {
                if key.len() != 25 {
                    return Err(corruption("paged row directory key is invalid"));
                }
                decode_point_id(payload)?;
            }
            KIND_EXACT => {
                if key.len() < 29 {
                    return Err(corruption("paged exact directory key is invalid"));
                }
                let exact_len = u32::from_be_bytes(key[9..13].try_into().unwrap()) as usize;
                if exact_len.checked_add(29) != Some(key.len()) || !payload.is_empty() {
                    return Err(corruption("paged exact directory record is invalid"));
                }
            }
            _ => return Err(corruption("paged directory kind is invalid")),
        }
        let bytes = key
            .len()
            .checked_add(frame.len())
            .and_then(|n| n.checked_add(2 * std::mem::size_of::<Vec<u8>>()))
            .ok_or_else(|| corruption("paged directory window size overflow"))?;
        self.admit(bytes)
    }
    pub(super) fn load_point(&self, id: PointId) -> Result<ScopedVectorCopy<PagedVectorPoint>> {
        let key = point_key(&self.storage, id);
        let value = self
            .pages
            .admitted_page(&key, &mut |frame| {
                let payload = open_graph_page(&key, frame)?;
                if payload.len() > self.node_limit {
                    return Err(corruption("HNSW point exceeds its admitted decode window"));
                }
                let decoded = point_decode_window(
                    payload,
                    if self.quantization == VectorQuantization::F32 {
                        4
                    } else {
                        1
                    },
                )?;
                let window = frame
                    .len()
                    .checked_add(decoded)
                    .and_then(|n| n.checked_add(std::mem::size_of::<Vec<u8>>()))
                    .ok_or_else(|| corruption("paged point window size overflow"))?;
                self.admit(window)
            })?
            .ok_or_else(|| corruption("paged vector graph names a missing point"))?;
        value.try_map(|value| {
            let payload = open_graph_page(&key, value)?;
            #[cfg(any(test, feature = "test-seams"))]
            contextdb_vector::hnsw::observe_paged_storage_work(
                contextdb_vector::hnsw::PagedVectorStorageWork::PageRecordDecode,
                payload.len(),
            );
            let point = match self.quantization {
                VectorQuantization::F32 => {
                    let point: PagedPoint<f32> = decode_paged_point(payload, self.node_limit)
                        .map_err(|error| {
                            corruption(format!("paged point decode failed: {error}"))
                        })?;
                    if point.values.len() != self.dimension {
                        return Err(corruption(
                            "paged point dimension disagrees with its column",
                        ));
                    }
                    PagedVectorPoint::F32(point)
                }
                VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
                    let point: PagedPoint<u8> = decode_paged_point(payload, self.node_limit)
                        .map_err(|error| {
                            corruption(format!("paged point decode failed: {error}"))
                        })?;
                    let expected = 12usize
                        .checked_add(if self.quantization == VectorQuantization::SQ8 {
                            self.dimension
                        } else {
                            self.dimension.div_ceil(2)
                        })
                        .ok_or_else(|| corruption("paged quantized dimension overflow"))?;
                    if point.values.len() != expected
                        || point.values.len() < 12
                        || u32::from_le_bytes(point.values[..4].try_into().unwrap()) as usize
                            != self.dimension
                    {
                        return Err(corruption(
                            "paged quantized payload disagrees with its column",
                        ));
                    }
                    let min =
                        f32::from_bits(u32::from_le_bytes(point.values[4..8].try_into().unwrap()));
                    let max =
                        f32::from_bits(u32::from_le_bytes(point.values[8..12].try_into().unwrap()));
                    if !min.is_finite() || !max.is_finite() || min > max {
                        return Err(corruption("paged quantized bounds are invalid"));
                    }
                    PagedVectorPoint::Quantized(point)
                }
            };
            let decoded = match &point {
                PagedVectorPoint::F32(point) => point.id,
                PagedVectorPoint::Quantized(point) => point.id,
            };
            if decoded != id {
                return Err(corruption("paged point identity disagrees with its key"));
            }
            Ok(point)
        })
    }

    pub(super) fn identity(&self, data_id: usize) -> Result<ScopedVectorCopy<PagedVectorIdentity>> {
        let key = identity_key(&self.storage, data_id);
        let value = self
            .pages
            .admitted_page(&key, &mut |frame| {
                let payload = open_value(&key, frame)?;
                if payload.len() < 50 {
                    return Err(corruption("paged vector identity record is invalid"));
                }
                let key_len = u32::from_be_bytes(payload[46..50].try_into().unwrap()) as usize;
                if payload.len()
                    != 50usize
                        .checked_add(key_len)
                        .ok_or_else(|| corruption("paged identity window size overflow"))?
                {
                    return Err(corruption("paged vector identity record is invalid"));
                }
                let window = frame
                    .len()
                    .checked_add(key_len)
                    .and_then(|n| n.checked_add(std::mem::size_of::<Vec<u8>>()))
                    .and_then(|n| n.checked_add(std::mem::size_of::<PagedVectorIdentity>()))
                    .ok_or_else(|| corruption("paged identity window size overflow"))?;
                self.admit(window)
            })?
            .ok_or_else(|| corruption("paged vector graph names a missing identity"))?;
        value.try_map(|value| {
            let identity = decode_identity(open_value(&key, value)?)?;
            if identity.data_id != data_id {
                return Err(corruption("paged identity disagrees with its key"));
            }
            Ok(identity)
        })
    }

    fn row_entry(&self, key: &[u8], value: &[u8]) -> Result<(RowId, usize, PointId)> {
        if key.len() != 25 || key[..8] != self.storage || key[8] != KIND_ROW {
            return Err(corruption("paged row directory key is invalid"));
        }
        let row = RowId(u64::from_be_bytes(key[9..17].try_into().unwrap()));
        let data_id = usize::try_from(u64::from_be_bytes(key[17..25].try_into().unwrap()))
            .map_err(|_| corruption("paged row directory identity exceeds usize"))?;
        let point = decode_point_id(open_value(key, value)?)?;
        Ok((row, data_id, point))
    }

    fn rows_end(&self) -> [u8; 9] {
        kind_prefix(&self.storage, KIND_ROW + 1)
    }

    /// The first row directory entry at or after `row`.
    pub(super) fn row_at_or_after(&self, row: RowId) -> Result<Option<(RowId, usize, PointId)>> {
        let lower = row_key(&self.storage, row, 0);
        let upper = self.rows_end();
        self.pages
            .first_after(Bound::Included(&lower), &upper, &mut |key, value| {
                self.directory_admission(key, value)
            })?
            .map(|entry| self.row_entry(&entry.0, &entry.1))
            .transpose()
    }

    /// The first row directory entry strictly after `(row, data_id)`.
    pub(super) fn row_after(
        &self,
        row: RowId,
        data_id: usize,
    ) -> Result<Option<(RowId, usize, PointId)>> {
        let lower = row_key(&self.storage, row, data_id);
        let upper = self.rows_end();
        self.pages
            .first_after(Bound::Excluded(&lower), &upper, &mut |key, value| {
                self.directory_admission(key, value)
            })?
            .map(|entry| self.row_entry(&entry.0, &entry.1))
            .transpose()
    }

    fn graph_row_at_or_after(&self, row: RowId) -> Result<Option<(RowId, usize, PointId)>> {
        let Some(mut selected) = self.row_at_or_after(row)? else {
            return Ok(None);
        };
        let mut cursor = self.row_after(selected.0, selected.1)?;
        while let Some(entry) = cursor {
            if entry.0 != selected.0 {
                break;
            }
            if entry.2 > selected.2 {
                selected = entry;
            }
            cursor = self.row_after(entry.0, entry.1)?;
        }
        Ok(Some(selected))
    }

    fn visible_point_for_row(&self, row: RowId, snapshot: SnapshotId) -> Result<Option<PointId>> {
        let mut cursor = self.row_at_or_after(row)?;
        while let Some((found, data_id, point)) = cursor {
            if found != row {
                return Ok(None);
            }
            let identity = self.identity(data_id)?;
            if identity.row_id != row || identity.point != point {
                return Err(corruption(
                    "paged row directory disagrees with its identity",
                ));
            }
            if identity_visible(&identity, snapshot) {
                return Ok(Some(point));
            }
            cursor = self.row_after(found, data_id)?;
        }
        Ok(None)
    }

    pub(super) fn exact_after(
        &self,
        exact: &[u8],
        after: Option<RowId>,
    ) -> Result<Option<(RowId, usize)>> {
        let prefix_len = exact
            .len()
            .checked_add(13)
            .ok_or_else(|| corruption("paged exact prefix size overflow"))?;
        let cursor_bytes = prefix_len
            .checked_add(16)
            .and_then(|n| n.checked_mul(5))
            .and_then(|n| n.checked_add(3 * std::mem::size_of::<Vec<u8>>()))
            .ok_or_else(|| corruption("paged exact cursor window size overflow"))?;
        let prefix_admission = self.admit(cursor_bytes)?;
        let prefix =
            ScopedVectorCopy::from_admitted(exact_prefix(&self.storage, exact), prefix_admission);
        let upper = writer_source::prefix_end(&prefix)
            .ok_or_else(|| corruption("paged exact directory prefix has no successor"))?;
        let lower = match after {
            Some(row) => {
                let mut key = Vec::with_capacity(prefix_len + 16);
                key.extend_from_slice(&prefix);
                key.extend_from_slice(&row.0.to_be_bytes());
                key.extend_from_slice(&u64::MAX.to_be_bytes());
                key
            }
            None => prefix.to_vec(),
        };
        let bound = if after.is_some() {
            Bound::Excluded(lower.as_slice())
        } else {
            Bound::Included(lower.as_slice())
        };
        let Some(entry) = self.pages.first_after(bound, &upper, &mut |key, value| {
            self.directory_admission(key, value)
        })?
        else {
            return Ok(None);
        };
        let (key, value) = &*entry;
        open_value(key, value)?;
        if key.len() != prefix.len() + 16 || !key.starts_with(&prefix) {
            return Err(corruption("paged exact directory key is invalid"));
        }
        let row = RowId(u64::from_be_bytes(
            key[prefix.len()..prefix.len() + 8].try_into().unwrap(),
        ));
        let data_id = usize::try_from(u64::from_be_bytes(
            key[prefix.len() + 8..].try_into().unwrap(),
        ))
        .map_err(|_| corruption("paged exact directory identity exceeds usize"))?;
        Ok(Some((row, data_id)))
    }

    fn exact_rows(
        &self,
        exact: &[u8],
        snapshot: SnapshotId,
        mut after: Option<RowId>,
        limit: usize,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<ScopedVectorCopy<Vec<RowId>>> {
        let credit = self.admit(
            limit
                .checked_mul(std::mem::size_of::<RowId>())
                .and_then(|n| n.checked_add(std::mem::size_of::<Vec<RowId>>()))
                .ok_or_else(|| corruption("paged row window overflow"))?,
        )?;
        let mut rows = ScopedVectorCopy::from_admitted(Vec::with_capacity(limit), credit);
        while rows.len() < limit {
            checkpoint()?;
            let Some((row, data_id)) = self.exact_after(exact, after)? else {
                break;
            };
            after = Some(row);
            let identity = self.identity(data_id)?;
            if identity.row_id != row || identity.exact_key != exact {
                return Err(corruption(
                    "paged exact directory disagrees with its identity",
                ));
            }
            if identity_visible(&identity, snapshot) && rows.last() != Some(&row) {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn row_window(
        &self,
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<ScopedVectorCopy<Vec<RowId>>> {
        let credit = self.admit(
            limit
                .checked_mul(std::mem::size_of::<RowId>())
                .and_then(|n| n.checked_add(std::mem::size_of::<Vec<RowId>>()))
                .ok_or_else(|| corruption("paged row window overflow"))?,
        )?;
        let mut rows = ScopedVectorCopy::from_admitted(Vec::with_capacity(limit), credit);
        let mut cursor = match after {
            Some(row) => self.row_after(row, usize::MAX)?,
            None => self.row_at_or_after(RowId(0))?,
        };
        while rows.len() < limit {
            checkpoint()?;
            let Some((row, data_id, point)) = cursor else {
                break;
            };
            let identity = self.identity(data_id)?;
            if identity.row_id != row || identity.point != point {
                return Err(corruption(
                    "paged row directory disagrees with its identity",
                ));
            }
            if identity_visible(&identity, snapshot) && rows.last() != Some(&row) {
                rows.push(row);
            }
            cursor = self.row_after(row, data_id)?;
        }
        Ok(rows)
    }

    fn row_bounds(&self, points: usize) -> Result<Option<(RowId, RowId, usize)>> {
        let lower = kind_prefix(&self.storage, KIND_ROW);
        let upper = self.rows_end();
        let Some(first_entry) =
            self.pages
                .first_after(Bound::Included(&lower), &upper, &mut |key, value| {
                    self.directory_admission(key, value)
                })?
        else {
            return Ok(None);
        };
        let Some(last_entry) = self.pages.last_before(&lower, &upper, &mut |key, value| {
            self.directory_admission(key, value)
        })?
        else {
            return Err(corruption("paged row directory lost its last entry"));
        };
        let (first, _, _) = self.row_entry(&first_entry.0, &first_entry.1)?;
        let (last, _, _) = self.row_entry(&last_entry.0, &last_entry.1)?;
        Ok(Some((first, last, points)))
    }

    /// Visible points at `snapshot`. Every published identity is current at
    /// the build frontier, so a snapshot at or after it sees all of them; an
    /// older snapshot walks the identity directory with its checkpoint.
    fn visible_count(
        &self,
        snapshot: SnapshotId,
        frontier: TxId,
        points: usize,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<usize> {
        if TxId::from_snapshot(snapshot) >= frontier {
            return Ok(points);
        }
        let mut visible = 0usize;
        let mut after = None;
        let prefix = kind_prefix(&self.storage, KIND_IDENTITY);
        let upper = writer_source::prefix_end(&prefix)
            .ok_or_else(|| corruption("paged identity prefix has no successor"))?;
        loop {
            checkpoint()?;
            let lower = after
                .as_ref()
                .map_or(Bound::Included(prefix.as_slice()), |key: &[u8; 17]| {
                    Bound::Excluded(key.as_slice())
                });
            let Some(entry) = self.pages.first_after(lower, &upper, &mut |key, frame| {
                if key.len() != 17 {
                    return Err(corruption("paged identity key has invalid width"));
                }
                let payload = open_value(key, frame)?;
                self.admit(
                    key.len()
                        .saturating_add(frame.len())
                        .saturating_add(payload.len())
                        .saturating_add(512),
                )
            })?
            else {
                break;
            };
            let origin = usize::try_from(u64::from_be_bytes(entry.0[9..17].try_into().unwrap()))
                .map_err(|_| corruption("paged identity exceeds usize"))?;
            let identity = self.identity(origin)?;
            if identity_visible(&identity, snapshot) {
                visible += 1;
            }
            after = Some(
                entry
                    .0
                    .as_slice()
                    .try_into()
                    .map_err(|_| corruption("paged identity key width is invalid"))?,
            );
        }
        Ok(visible)
    }
}

/// Engine hooks a build batch calls back into: maintenance cancellation and
/// the in-flight inserted-point report.
pub(crate) struct PagedBuildHooks {
    pub(crate) checkpoint: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    pub(crate) inserted: Arc<dyn Fn(usize) + Send + Sync>,
    /// Serializes every build/abandon transaction with ordinary commits, DDL
    /// and purge. Acquired before the storage write transaction.
    pub(crate) gate: Arc<dyn Fn(&mut dyn FnMut() -> Result<()>) -> Result<()> + Send + Sync>,
}

/// What a publishing batch writes: the immutable base record and catalog, as
/// the existing generation publication encodes them, plus the catalog
/// generation the build must still be replacing when it commits.
pub(crate) struct PagedPublication {
    pub(crate) prepared: PreencodedVectorPartitionGenerationCandidate,
    pub(crate) previous_generation_id: u64,
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) published_pending: Arc<parking_lot::Mutex<Option<(usize, usize)>>>,
}

/// One captured storage root plus the owner that keeps it readable: the
/// writer's published snapshot, or a committed image's retained source.
pub(crate) type PagedRootCapture = Arc<
    dyn Fn() -> Result<(
            Arc<redb::ReadTransaction>,
            Arc<dyn std::any::Any + Send + Sync>,
        )> + Send
        + Sync,
>;

/// Only an active serving operation supplies this root. Maintenance may read
/// immutable candidate records before the next serving root is published.
pub(crate) type ScopedWriterRootCapture =
    Arc<dyn Fn() -> Result<Option<Arc<WriterSnapshot>>> + Send + Sync>;

/// Envelope format of a sealed change generation (`HnswIndex` durable codec).
pub(crate) const VECTOR_GRAPH_ENVELOPE_FORMAT_VERSION: u16 = 1;

/// A sealed change generation keeps the envelope codec. It may extend an
/// envelope base or a native paged base; nothing else is a valid chain.
pub(crate) fn change_format_compatible(change: u16, base: u16) -> bool {
    change == base
        || (change == VECTOR_GRAPH_ENVELOPE_FORMAT_VERSION
            && base == VECTOR_GRAPH_PAGED_FORMAT_VERSION)
}

/// The engine's persistence handle for one partition's paged build.
pub(crate) struct NativePagedGeneration {
    persistence: Arc<RedbPersistence>,
    partition: VectorPartitionRef,
    record_key: Vec<u8>,
    storage_id: u64,
    declaration: PagedDeclaration,
    schema_binding: VectorSchemaBinding,
    hooks: PagedBuildHooks,
}

impl NativePagedGeneration {
    pub(crate) fn new(
        persistence: Arc<RedbPersistence>,
        partition: VectorPartitionRef,
        record: &PagedBuildRecord,
        hooks: PagedBuildHooks,
    ) -> Self {
        let record_key =
            RedbPersistence::vector_partition_prefix(&partition.index, &partition.partition_key);
        Self {
            persistence,
            partition,
            record_key,
            storage_id: record.storage_id,
            declaration: record.declaration.clone(),
            schema_binding: record.schema_binding,
            hooks,
        }
    }

    pub(crate) fn storage_id(&self) -> u64 {
        self.storage_id
    }

    pub(crate) fn declaration(&self) -> &PagedDeclaration {
        &self.declaration
    }

    fn read_record(
        &self,
        builds: &redb::Table<'_, &'static [u8], &'static [u8]>,
    ) -> Result<PagedBuildRecord> {
        let value = builds
            .get(self.record_key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| {
                Error::Other("paged vector build record is no longer registered".into())
            })?;
        let record: PagedBuildRecord =
            RedbPersistence::decode_exact(open_value(&self.record_key, value.value())?)
                .map_err(|_| corruption("paged build record cannot be decoded"))?;
        if record.storage_id == 0 {
            return Err(corruption("paged build storage identity is reserved"));
        }
        if record.storage_id != self.storage_id
            || record.declaration != self.declaration
            || record.schema_binding != self.schema_binding
        {
            return Err(Error::Other(
                "paged vector build record changed outside this builder".into(),
            ));
        }
        Ok(record)
    }
}

impl PagedGenerationPersistence for NativePagedGeneration {
    fn abandon_generation(
        &self,
        previous: &PagedVectorProgress,
        next: &PagedVectorProgress,
        _plan: PagedVectorBatchPlan,
    ) -> Result<PagedVectorProgress> {
        let mut committed = None;
        (self.hooks.gate)(&mut || {
            self.persistence.with_db(|db| {
                let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
                {
                    let mut builds = write
                        .open_table(BUILDS)
                        .map_err(RedbPersistence::storage_error)?;
                    let record = self.read_record(&builds)?;
                    if record.progress.to_progress()? != *previous
                        || next.phase != PagedVectorPhase::Abandoned
                    {
                        return Err(Error::Other(
                            "paged abandon does not match the durable build record".into(),
                        ));
                    }
                    builds
                        .remove(self.record_key.as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                    write
                        .open_table(CLEANUP)
                        .map_err(RedbPersistence::storage_error)?
                        .insert(self.storage_id, [].as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                }
                // The pages-root binding rewrites the identity roots that the
                // committed read bounds authenticate, so it precedes their refresh.
                bind_pages_root(&write)?;
                RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                write.commit().map_err(RedbPersistence::storage_error)?;
                committed = Some(next.clone());
                Ok(())
            })
        })?;
        committed.ok_or_else(|| Error::Other("paged abandon did not commit".into()))
    }

    fn commit_generation_batch(
        &self,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        batch: &mut dyn FnMut(&mut dyn PagedGenerationTransaction) -> Result<PagedVectorProgress>,
    ) -> Result<PagedVectorProgress> {
        let mut committed = None;
        (self.hooks.gate)(&mut || {
            (self.hooks.checkpoint)()?;
            self.persistence.with_db(|db| {
                let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
                let record = {
                    let builds = write
                        .open_table(BUILDS)
                        .map_err(RedbPersistence::storage_error)?;
                    self.read_record(&builds)?
                };
                RedbPersistence::validate_vector_schema_binding_in_write(
                    &write,
                    &self.partition.index,
                    self.schema_binding,
                )?;
                if record.progress.to_progress()? != *previous {
                    return Err(Error::Other(
                        "paged vector batch is stale against its durable build record".into(),
                    ));
                }
                if previous.declaration_digest != self.declaration.digest()? {
                    return Err(Error::Other(
                        "paged vector build declaration changed; abandon the frontier".into(),
                    ));
                }
                let publication = self.persistence.take_paged_publication(&self.record_key);
                let (next, published) = {
                    let mut transaction = NativeBatch::open(
                        &self.persistence,
                        &write,
                        &self.partition,
                        self.storage_id,
                        &self.declaration,
                        previous,
                        plan,
                        &self.hooks,
                        publication.as_ref(),
                    )?;
                    let next = batch(&mut transaction)?;
                    let published = transaction.published;
                    #[cfg(any(test, feature = "test-seams"))]
                    let pending = transaction.published_pending;
                    transaction.finish()?;
                    #[cfg(any(test, feature = "test-seams"))]
                    let published = (published, pending);
                    (next, published)
                };
                #[cfg(any(test, feature = "test-seams"))]
                let (published, pending) = published;
                if published != (next.phase == PagedVectorPhase::Published) {
                    return Err(Error::Other(
                        "paged publication and its successor record disagree".into(),
                    ));
                }
                {
                    let mut builds = write
                        .open_table(BUILDS)
                        .map_err(RedbPersistence::storage_error)?;
                    if matches!(
                        next.phase,
                        PagedVectorPhase::Published | PagedVectorPhase::Abandoned
                    ) {
                        builds
                            .remove(self.record_key.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    } else {
                        let successor = PagedBuildRecord {
                            progress: ProgressRecord::from_progress(&next),
                            ..record
                        };
                        let payload = RedbPersistence::encode(&successor)?;
                        builds
                            .insert(
                                self.record_key.as_slice(),
                                seal_value(&self.record_key, &payload).as_slice(),
                            )
                            .map_err(RedbPersistence::storage_error)?;
                    }
                }
                bind_pages_root(&write)?;
                RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                (self.hooks.checkpoint)()?;
                #[cfg(any(test, feature = "test-seams"))]
                if published {
                    crate::writer_commit_test_seams::reached(
                        crate::writer_commit_test_seams::CommitBoundary::GenerationBeforeRedb,
                    );
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    crate::writer_commit_test_seams::note_build_successor(&next);
                    crate::writer_commit_test_seams::reached(
                        crate::writer_commit_test_seams::CommitBoundary::BuildBatchBeforeRedb,
                    );
                }
                write.commit().map_err(RedbPersistence::storage_error)?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_commit_test_seams::reached(
                    crate::writer_commit_test_seams::CommitBoundary::BuildBatchAfterRedb,
                );
                #[cfg(any(test, feature = "test-seams"))]
                if let Some(publication) = &publication {
                    *publication.published_pending.lock() = pending;
                }
                committed = Some(next);
                Ok(())
            })
        })?;
        committed.ok_or_else(|| Error::Other("paged vector batch did not commit".into()))
    }
}

/// One build batch's view of its write transaction.
pub(super) struct NativeBatch<'t> {
    persistence: &'t RedbPersistence,
    write: &'t redb::WriteTransaction,
    pages: redb::Table<'t, &'static [u8], &'static [u8]>,
    scratch: redb::Table<'t, &'static [u8], &'static [u8]>,
    partition: &'t VectorPartitionRef,
    source_prefix: Vec<u8>,
    storage: [u8; 8],
    declaration: &'t PagedDeclaration,
    previous: &'t PagedVectorProgress,
    node_limit: usize,
    window_limit: usize,
    window_exhausted: bool,
    window_position: Option<Vec<u8>>,
    scratch_keys: usize,
    candidate_sequence: u64,
    hooks: &'t PagedBuildHooks,
    publication: Option<&'t PagedPublication>,
    published: bool,
    #[cfg(any(test, feature = "test-seams"))]
    published_pending: Option<(usize, usize)>,
    inserted: usize,
}

impl<'t> NativeBatch<'t> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn open(
        persistence: &'t RedbPersistence,
        write: &'t redb::WriteTransaction,
        partition: &'t VectorPartitionRef,
        storage_id: u64,
        declaration: &'t PagedDeclaration,
        previous: &'t PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        hooks: &'t PagedBuildHooks,
        publication: Option<&'t PagedPublication>,
    ) -> Result<Self> {
        let page_name = page_table_name(storage_id);
        let pages = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new(&page_name))
            .map_err(RedbPersistence::storage_error)?;
        let scratch = write
            .open_table(SCRATCH)
            .map_err(RedbPersistence::storage_error)?;
        if scratch.len().map_err(RedbPersistence::storage_error)? != 0 {
            return Err(corruption("paged search scratch survived its transaction"));
        }
        Ok(Self {
            persistence,
            write,
            pages,
            scratch,
            partition,
            source_prefix: RedbPersistence::vector_partition_prefix(
                &partition.index,
                &partition.partition_key,
            ),
            storage: storage_prefix(storage_id),
            declaration,
            previous,
            node_limit: plan.max_node_bytes,
            // Rejected history and accepted bodies share one finite window
            // per source call; four rejected keys per admitted point.
            window_limit: plan.max_points.saturating_mul(4).max(1),
            window_exhausted: true,
            window_position: None,
            scratch_keys: 0,
            candidate_sequence: 0,
            hooks,
            publication,
            published: false,
            #[cfg(any(test, feature = "test-seams"))]
            published_pending: None,
            inserted: 0,
        })
    }

    fn lookup(&self) -> PagedLookup<'_, '_, redb::Table<'t, &'static [u8], &'static [u8]>> {
        PagedLookup {
            admission: None,
            pages: &self.pages,
            storage: self.storage,
            dimension: self.declaration.dimension as usize,
            quantization: self.declaration.quantization,
            node_limit: self.node_limit,
        }
    }

    fn clear_scratch(&mut self) -> Result<()> {
        // Exactly the keys this search inserted; never a table-wide walk.
        while self.scratch_keys != 0 {
            let removed = self
                .scratch
                .pop_last()
                .map_err(RedbPersistence::storage_error)?;
            if removed.is_none() {
                return Err(corruption("paged search scratch lost a key"));
            }
            self.scratch_keys -= 1;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<()> {
        self.clear_scratch()?;
        if self.scratch.len().map_err(RedbPersistence::storage_error)? != 0 {
            return Err(corruption(
                "paged search scratch would survive its transaction",
            ));
        }
        Ok(())
    }

    fn check(&mut self) -> Result<()> {
        (self.hooks.checkpoint)()
    }

    fn note_visit(&self) {
        #[cfg(any(test, feature = "test-seams"))]
        crate::database::resident_history_test_seams::note(|work| {
            work.maintenance_source_visits += 1
        });
    }

    /// Load the original body named by one owning membership interval.
    fn source_values(&self, record: &VectorPartitionMembershipRecord) -> Result<PagedVectorValues> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let vectors = self
            .write
            .open_table(VECTORS_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let key = RedbPersistence::vector_identity_key(
            &record.index,
            record.row_id,
            record.vector_created_tx,
            record.vector_lsn,
        );
        let stored = vectors
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| corruption("a paged build source names a missing vector body"))?;
        let dimension = self.declaration.dimension as usize;
        self.persistence.validate_raw_vector_entry_encoded_width(
            stored.value(),
            &record.index,
            dimension,
        )?;
        // Exact-length bodies: the f32 values become the source values with
        // no growth slack and no copy.
        let persisted = RedbPersistence::decode_persisted_vector_entry_exact(stored.value())
            .map_err(|error| match error {
                Error::MemoryBudgetExceeded { .. } | Error::ReadFailure(_) => error,
                _ => corruption("a paged build source body cannot be decoded"),
            })?;
        if persisted.index != record.index
            || persisted.row_id != record.row_id
            || persisted.created_tx != record.vector_created_tx
            || persisted.lsn != record.vector_lsn
        {
            return Err(corruption(
                "a paged build source body disagrees with its membership",
            ));
        }
        self.note_visit();
        match (persisted.vector, self.declaration.quantization) {
            (PersistedVector::F32(values), VectorQuantization::F32) => {
                if values.len() != dimension {
                    return Err(corruption("a paged build source has the wrong dimension"));
                }
                Ok(PagedVectorValues::F32(values))
            }
            (
                PersistedVector::SQ8 {
                    min,
                    max,
                    len,
                    payload,
                },
                VectorQuantization::SQ8,
            )
            | (
                PersistedVector::SQ4 {
                    min,
                    max,
                    len,
                    payload,
                },
                VectorQuantization::SQ4,
            ) => PagedVectorValues::from_quantized_payload(
                self.declaration.quantization,
                len as usize,
                min,
                max,
                &payload,
            ),
            _ => Err(corruption(
                "a paged build source body disagrees with its column representation",
            )),
        }
    }
}

impl PagedVectorReader for NativeBatch<'_> {
    fn load_point(&mut self, id: PointId) -> Result<PagedVectorPoint> {
        self.lookup().load_point(id)?.into_already_funded()
    }
    fn identity(&mut self, data_id: usize) -> Result<PagedVectorIdentity> {
        self.lookup().identity(data_id)?.into_already_funded()
    }
    fn point_for_row(&mut self, row: RowId, snapshot: SnapshotId) -> Result<Option<PointId>> {
        self.lookup().visible_point_for_row(row, snapshot)
    }
    fn exact_rows(
        &mut self,
        key: &[u8],
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
    ) -> Result<Vec<RowId>> {
        let checkpoint = Arc::clone(&self.hooks.checkpoint);
        self.lookup()
            .exact_rows(key, snapshot, after, limit, &mut || checkpoint())?
            .into_already_funded()
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.check()
    }
    fn before_distance(&mut self) -> Result<()> {
        self.check()
    }
    fn before_visit(&mut self) -> Result<()> {
        self.check()
    }
    fn visible_count(&mut self, snapshot: SnapshotId) -> Result<usize> {
        let checkpoint = Arc::clone(&self.hooks.checkpoint);
        let points = self.previous.graph.points + self.inserted;
        self.lookup()
            .visible_count(snapshot, self.previous.frontier_tx, points, &mut || {
                checkpoint()
            })
    }
    fn row_window(
        &mut self,
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
    ) -> Result<Vec<RowId>> {
        let checkpoint = Arc::clone(&self.hooks.checkpoint);
        self.lookup()
            .row_window(snapshot, after, limit, &mut || checkpoint())?
            .into_already_funded()
    }
    fn row_bounds(&mut self) -> Result<Option<(RowId, RowId, usize)>> {
        let points = self.previous.graph.points + self.inserted;
        self.lookup().row_bounds(points)
    }
    fn point_at_or_after(&mut self, row: RowId) -> Result<Option<(RowId, PointId)>> {
        Ok(self
            .lookup()
            .graph_row_at_or_after(row)?
            .map(|(row, _, point)| (row, point)))
    }
    fn exact_graph_row_after(&mut self, key: &[u8], after: Option<RowId>) -> Result<Option<RowId>> {
        Ok(self.lookup().exact_after(key, after)?.map(|(row, _)| row))
    }
}

impl PagedVectorTransaction for NativeBatch<'_> {
    /// Unique navigation visits and queued candidates can grow with the graph.
    /// Spill them into this transaction's admitted pages; only the bounded
    /// neighbor selection and pruning windows remain in the heap workspace.
    fn spills_search_scratch(&self) -> bool {
        true
    }

    fn store_point(&mut self, point: &PagedVectorPoint) -> Result<()> {
        let (id, mut bytes) = match point {
            PagedVectorPoint::F32(point) => (point.id, Vec::new()),
            PagedVectorPoint::Quantized(point) => (point.id, Vec::new()),
        };
        match point {
            PagedVectorPoint::F32(point) => encode_paged_point_into(point, &mut bytes),
            PagedVectorPoint::Quantized(point) => encode_paged_point_into(point, &mut bytes),
        }
        .map_err(|error| Error::Other(format!("paged point encoding failed: {error}")))?;
        if bytes.len() > self.node_limit {
            return Err(Error::Other(
                "paged point exceeds its admitted page window".into(),
            ));
        }
        let key = point_key(&self.storage, id);
        self.write_graph_page(key.as_slice(), &seal_value(&key, &bytes))?;
        Ok(())
    }

    fn store_identity(&mut self, identity: &PagedVectorIdentity) -> Result<()> {
        let key = identity_key(&self.storage, identity.data_id);
        if self
            .pages
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .is_some()
        {
            return Err(corruption("paged identity would overwrite a stored point"));
        }
        self.write_graph_page(
            key.as_slice(),
            &seal_value(&key, &encode_identity(identity)),
        )?;
        if u64::from_be_bytes(self.storage) & MUTABLE_STORAGE != 0 {
            let key = import_tail::source_identity_key(
                u64::from_be_bytes(self.storage),
                identity.row_id,
                identity.created_tx,
                identity.lsn,
            );
            let payload = (identity.data_id as u64).to_be_bytes();
            self.write_graph_page(key.as_slice(), &seal_value(&key, &payload))?;
        }
        if u64::from_be_bytes(self.storage) & MUTABLE_STORAGE != 0 {
            let mut prefix = self.storage.to_vec();
            prefix.push(KIND_ROW);
            prefix.extend_from_slice(&identity.row_id.0.to_be_bytes());
            let upper = writer_source::prefix_end(&prefix)
                .ok_or_else(|| corruption("native row prefix has no successor"))?;
            let exists = self
                .pages
                .range::<&[u8]>(prefix.as_slice()..upper.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .next()
                .transpose()
                .map_err(RedbPersistence::storage_error)?
                .is_some();
            if !exists {
                let mut key = self.storage.to_vec();
                key.push(6);
                let count = self
                    .pages
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| {
                        let bytes = open_value(&key, value.value())?;
                        let bytes: [u8; 8] = bytes
                            .try_into()
                            .map_err(|_| corruption("native row counter has the wrong width"))?;
                        Ok::<_, Error>(u64::from_be_bytes(bytes))
                    })
                    .transpose()?
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| corruption("native row counter overflow"))?;
                self.write_graph_page(key.as_slice(), &seal_value(&key, &count.to_be_bytes()))?;
            }
        }
        let row = row_key(&self.storage, identity.row_id, identity.data_id);
        self.write_graph_page(
            row.as_slice(),
            &seal_value(&row, &encode_point_id(identity.point)),
        )?;
        let exact = exact_key(
            &self.storage,
            &identity.exact_key,
            identity.row_id,
            identity.data_id,
        );
        self.write_graph_page(exact.as_slice(), &seal_value(&exact, &[]))?;
        self.inserted += 1;
        (self.hooks.inserted)(self.previous.graph.points + self.inserted);
        Ok(())
    }

    fn begin_search_scratch(&mut self) -> Result<()> {
        self.clear_scratch()?;
        self.candidate_sequence = 0;
        Ok(())
    }

    fn visit_search_scratch(&mut self, id: PointId) -> Result<bool> {
        let key = visited_key(id);
        if self
            .scratch
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .is_some()
        {
            return Ok(false);
        }
        self.scratch
            .insert(key.as_slice(), [].as_slice())
            .map_err(RedbPersistence::storage_error)?;
        self.scratch_keys += 1;
        Ok(true)
    }

    fn push_search_candidate(&mut self, edge: PagedEdge) -> Result<()> {
        self.candidate_sequence += 1;
        let key = candidate_key(&edge, self.candidate_sequence);
        self.scratch
            .insert(key.as_slice(), [].as_slice())
            .map_err(RedbPersistence::storage_error)?;
        self.scratch_keys += 1;
        Ok(())
    }

    fn pop_search_candidate(&mut self) -> Result<Option<PagedEdge>> {
        let lower = [SCRATCH_CANDIDATE];
        let upper = [SCRATCH_CANDIDATE + 1];
        let last = {
            let mut range = self
                .scratch
                .range::<&[u8]>(lower.as_slice()..upper.as_slice())
                .map_err(RedbPersistence::storage_error)?;
            match range.next_back() {
                Some(entry) => Some(
                    entry
                        .map_err(RedbPersistence::storage_error)?
                        .0
                        .value()
                        .to_vec(),
                ),
                None => None,
            }
        };
        let Some(key) = last else {
            return Ok(None);
        };
        self.scratch
            .remove(key.as_slice())
            .map_err(RedbPersistence::storage_error)?;
        self.scratch_keys -= 1;
        edge_from_candidate_key(&key).map(Some)
    }
}

impl PagedGenerationTransaction for NativeBatch<'_> {
    fn source_next(
        &mut self,
        progress: &PagedVectorProgress,
        after: Option<&[u8]>,
    ) -> Result<Option<PagedVectorBuildEntry>> {
        self.window_exhausted = true;
        self.window_position = None;
        let source = self
            .write
            .open_table(vector_membership_pages::SOURCE)
            .map_err(RedbPersistence::storage_error)?;
        let upper = writer_source::prefix_end(&self.source_prefix)
            .ok_or_else(|| corruption("paged source prefix has no successor"))?;
        let mut lower = self.source_prefix.clone();
        let lower_bound = match after {
            None => Bound::Included(lower.as_slice()),
            Some(after) if after.len() == CANONICAL_BYTES => {
                // Exclusive after every physical companion of that canonical key.
                lower.extend_from_slice(after);
                lower.extend_from_slice(&[0xff; SOURCE_SUFFIX_BYTES]);
                Bound::Excluded(lower.as_slice())
            }
            Some(after) if after.len() == CANONICAL_BYTES + SOURCE_SUFFIX_BYTES => {
                lower.extend_from_slice(after);
                Bound::Excluded(lower.as_slice())
            }
            Some(_) => return Err(corruption("paged source cursor has an invalid width")),
        };
        let mut rejected = 0usize;
        let range = source
            .range::<&[u8]>((lower_bound, Bound::Excluded(upper.as_slice())))
            .map_err(RedbPersistence::storage_error)?;
        for entry in range {
            (self.hooks.checkpoint)()?;
            let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
            self.note_visit();
            let key = key.value();
            if key.len() != self.source_prefix.len() + CANONICAL_BYTES + SOURCE_SUFFIX_BYTES
                || !key.starts_with(&self.source_prefix)
            {
                return Err(corruption("paged source companion key has the wrong width"));
            }
            let tail = &key[self.source_prefix.len()..];
            let record = vector_membership_pages::decode_source_member(key, value.value())?;
            if record.index != self.partition.index
                || record.partition_key != self.partition.partition_key
            {
                return Err(corruption("paged source companion names another route"));
            }
            if record.row_lsn > progress.frontier_lsn {
                // Canonical order is LSN-major: nothing later is inside the
                // pinned frontier.
                return Ok(None);
            }
            let visible = record.visible_from <= progress.frontier_tx
                && record
                    .deleted_tx
                    .is_none_or(|deleted| deleted > progress.frontier_tx);
            if !visible {
                rejected += 1;
                if rejected >= self.window_limit {
                    self.window_exhausted = false;
                    self.window_position = Some(tail.to_vec());
                    return Ok(None);
                }
                continue;
            }
            let values = self.source_values(&record)?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::database::resident_history_test_seams::note(|work| {
                if work.maintenance_source_keys < work.maintenance_source_identities.len() {
                    work.maintenance_source_identities[work.maintenance_source_keys] =
                        Some((record.row_id.0, record.row_lsn.0));
                }
                work.maintenance_source_keys += 1;
            });
            return Ok(Some(PagedVectorBuildEntry {
                source_key: tail[..CANONICAL_BYTES].to_vec(),
                row_id: record.row_id,
                lsn: record.row_lsn,
                created_tx: record.visible_from,
                // The generation is the frontier's image; later deletes are
                // authoritative in raw membership and the journal suffix.
                deleted_tx: None,
                values,
            }));
        }
        Ok(None)
    }

    fn source_window_exhausted(&self) -> bool {
        self.window_exhausted
    }

    fn source_window_position(&self) -> Option<&[u8]> {
        self.window_position.as_deref()
    }

    fn source_entry_after_position(&self, entry: &[u8], position: &[u8]) -> bool {
        // A physical rejection position carries its canonical key plus the
        // companion suffix; the next accepted canonical key may equal that
        // prefix only for a later physical companion of the same key.
        if position.len() == CANONICAL_BYTES + SOURCE_SUFFIX_BYTES {
            entry >= &position[..CANONICAL_BYTES]
        } else {
            entry > position
        }
    }

    fn seal_point(&mut self, point: PointId, encoded: &[u8]) -> Result<()> {
        let key = point_key(&self.storage, point);
        let value = self
            .pages
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| corruption("paged encoding names a missing point"))?;
        if open_value(&key, value.value())? != encoded {
            return Err(corruption(
                "paged sealed point differs from its stored page",
            ));
        }
        Ok(())
    }

    fn publish(
        &mut self,
        progress: &PagedVectorProgress,
        through_lsn: Lsn,
    ) -> Result<PagedVectorPhase> {
        let publication = self
            .publication
            .ok_or_else(|| Error::Other("paged publication was not staged by the engine".into()))?;
        if progress.phase != PagedVectorPhase::Complete
            || through_lsn < progress.frontier_lsn
            || progress.encoded_points != progress.graph.points
            || progress.graph.points != progress.total_points
        {
            return Err(Error::Other(
                "paged generation cannot publish incomplete coverage".into(),
            ));
        }
        // Empty routes cannot acquire a base record. Keep the existing page
        // storage discoverable through cleanup before retiring its restart
        // record in this same transaction.
        if vector_membership_pages::is_empty_partitioned_route_in_write(
            self.write,
            &publication.prepared.catalog_route,
        )? {
            self.write
                .open_table(CLEANUP)
                .map_err(RedbPersistence::storage_error)?
                .insert(u64::from_be_bytes(self.storage), [].as_slice())
                .map_err(RedbPersistence::storage_error)?;
            return Ok(PagedVectorPhase::Abandoned);
        }
        // The catalog must still name the generation this build replaces;
        // otherwise another publication won and this frontier is stale.
        let current = {
            let catalogs = self
                .write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let key = RedbPersistence::vector_partition_catalog_key_from_parts(
                &self.partition.index,
                &self.partition.partition_key,
            );
            let value = catalogs
                .get(key.as_slice())
                .map_err(RedbPersistence::storage_error)?;
            match value {
                Some(value) => {
                    let catalog: VectorPartitionGenerationCatalogRecord =
                        self.persistence.decode_vector_partition_record(
                            VectorPartitionRecordKind::GenerationCatalog,
                            value.value(),
                        )?;
                    catalog
                        .change
                        .map(|change| change.generation_id)
                        .unwrap_or(catalog.base.generation_id)
                        .max(catalog.base.generation_id)
                }
                None => 0,
            }
        };
        if current != publication.previous_generation_id {
            return Err(Error::Other(
                "paged vector build was superseded by another publication".into(),
            ));
        }
        self.persistence
            .insert_preencoded_vector_partition_generation_objects_in_write(
                self.write,
                &publication.prepared,
            )?;
        let pending = self
            .persistence
            .publish_preencoded_vector_partition_generation_catalog_in_write(
                self.write,
                &publication.prepared,
            )?;
        #[cfg(any(test, feature = "test-seams"))]
        {
            self.published_pending = pending;
        }
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = pending;
        self.published = true;
        Ok(PagedVectorPhase::Published)
    }

    fn reclaim(
        &mut self,
        generation: u64,
        after: Option<&[u8]>,
        max_keys: usize,
    ) -> Result<PagedVectorReclamation> {
        if generation != self.previous.generation_id {
            return Err(Error::Other(
                "paged reclamation names another generation".into(),
            ));
        }
        reclaim_storage_keys(&mut self.pages, &self.storage, after, max_keys)
    }
}

/// Remove at most `max_keys` pages of one storage identity after `after`.
fn reclaim_storage_keys(
    pages: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    storage: &[u8; 8],
    after: Option<&[u8]>,
    max_keys: usize,
) -> Result<PagedVectorReclamation> {
    reclaim_storage_keys_with_admission(pages, storage, after, max_keys, &mut |_| Ok(()))
}

/// Admit the cursor and the old/new seek keys before copying them. The page
/// cache separately owns the storage pages retained by each short seek.
fn reclaim_storage_keys_with_admission(
    pages: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    storage: &[u8; 8],
    after: Option<&[u8]>,
    max_keys: usize,
    admit: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<PagedVectorReclamation> {
    admit(
        after
            .map_or(0, <[u8]>::len)
            .saturating_add(storage.len())
            .saturating_add(3 * std::mem::size_of::<Vec<u8>>()),
    )?;
    let upper = writer_source::prefix_end(storage)
        .ok_or_else(|| corruption("paged storage identity has no successor"))?;
    let mut removed = 0usize;
    let mut last = None;
    while removed < max_keys {
        let lower = match last.as_deref().or(after) {
            Some(key) => Bound::Excluded(key),
            None => Bound::Included(storage.as_slice()),
        };
        let next = {
            let mut range = pages
                .range::<&[u8]>((lower, Bound::Excluded(upper.as_slice())))
                .map_err(RedbPersistence::storage_error)?;
            match range.next() {
                Some(entry) => {
                    let (key, _) = entry.map_err(RedbPersistence::storage_error)?;
                    admit(
                        after
                            .map_or(0, <[u8]>::len)
                            .saturating_add(last.as_ref().map_or(0, Vec::capacity))
                            .saturating_add(key.value().len())
                            .saturating_add(upper.capacity())
                            .saturating_add(3 * std::mem::size_of::<Vec<u8>>()),
                    )?;
                    Some(key.value().to_vec())
                }
                None => None,
            }
        };
        let Some(key) = next else {
            return Ok(PagedVectorReclamation {
                keys_removed: removed,
                last_key: last,
                complete: true,
            });
        };
        pages
            .remove(key.as_slice())
            .map_err(RedbPersistence::storage_error)?;
        removed += 1;
        last = Some(key);
    }
    Ok(PagedVectorReclamation {
        keys_removed: removed,
        last_key: last,
        complete: false,
    })
}

/// A published generation's serving source: each reader captures the
/// operation's captured writer root and reads only that root's pages.
pub(crate) struct NativePagedGraphSource {
    pub(crate) capture: PagedRootCapture,
    pub(crate) index: VectorIndexRef,
    pub(crate) schema_binding: VectorSchemaBinding,
    pub(crate) storage_id: u64,
    pub(crate) declaration: PagedDeclaration,
    pub(crate) frontier: TxId,
    pub(crate) points: usize,
    pub(crate) rows: usize,
    pub(crate) node_limit: usize,
    pub(crate) checkpoint: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    pub(crate) page_set: PageSetRoot,
    /// Set once this load has verified the route's whole page set.
    pub(crate) verified: Arc<parking_lot::Mutex<bool>>,
}

impl PagedVectorGraphSource for NativePagedGraphSource {
    fn reader(&self) -> Result<Box<dyn PagedVectorReader>> {
        let (read, root) = (self.capture)()?;
        RedbPersistence::validate_vector_schema_binding_in_read(
            &read,
            &self.index,
            self.schema_binding,
        )?;
        validate_pages_root(&read)?;
        let page_name = page_table_name(self.storage_id);
        let table = TableDefinition::<&[u8], &[u8]>::new(&page_name);
        let pages_table_root = read
            .table_root_fingerprint(table)
            .map_err(RedbPersistence::storage_error)?
            .unwrap_or([0; 32]);
        let mutable = self.storage_id & MUTABLE_STORAGE != 0;
        if mutable != (self.page_set.pages == MUTABLE_PAGE_SET)
            || (mutable && pages_table_root != self.page_set.digest)
        {
            return Err(route_page_corrupt());
        }
        let pages = match read.open_table(table) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(corruption("published paged generation has no page table"));
            }
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        };
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_physical_source(
            crate::writer_query_test_seams::PhysicalFamily::Graph,
            &read,
        );
        Ok(Box::new(NativePagedReader {
            _root: root,
            pages,
            storage: storage_prefix(self.storage_id),
            dimension: self.declaration.dimension as usize,
            quantization: self.declaration.quantization,
            frontier: self.frontier,
            points: self.points,
            rows: self.rows,
            node_limit: self.node_limit,
            checkpoint: Arc::clone(&self.checkpoint),
            page_set: self.page_set,
            verified: Arc::clone(&self.verified),
            storage_id: self.storage_id,
            pages_table_root,
        }))
    }

    fn retained_bytes(&self) -> usize {
        0
    }
}

struct NativePagedReader {
    pages: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    storage: [u8; 8],
    dimension: usize,
    quantization: VectorQuantization,
    frontier: TxId,
    points: usize,
    rows: usize,
    node_limit: usize,
    checkpoint: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    page_set: PageSetRoot,
    verified: Arc<parking_lot::Mutex<bool>>,
    storage_id: u64,
    pages_table_root: [u8; 32],
    // Table and page handles die before the root that authenticates them.
    _root: Arc<dyn std::any::Any + Send + Sync>,
}

impl NativePagedReader {
    fn lookup(&self) -> PagedLookup<'_, '_, redb::ReadOnlyTable<&'static [u8], &'static [u8]>> {
        PagedLookup {
            admission: None,
            pages: &self.pages,
            storage: self.storage,
            dimension: self.dimension,
            quantization: self.quantization,
            node_limit: self.node_limit,
        }
    }
}

impl PagedVectorReader for NativePagedReader {
    fn read_with_copy_admission(
        &mut self,
        request: PagedVectorReadRequest<'_>,
        admit: &mut dyn FnMut(usize) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<PagedVectorReadValue> {
        (self.checkpoint)()?;
        {
            // The first read of this load answers nothing until the route's
            // whole committed page set verifies; later reads reuse that proof.
            let mut verified = self.verified.lock();
            if self.page_set.pages == MUTABLE_PAGE_SET {
                // The selected root authenticated the table's Merkle root
                // before any record was admitted to this reader.
                *verified = true;
            }
            if !*verified && page_set_proven(self.storage_id, &self.page_set, self.pages_table_root)
            {
                *verified = true;
            }
            if !*verified {
                // The widest page a generation writes: a point page, or an
                // identity or exact-directory page carrying one exact vector
                // identity in its key and value.
                let exact = self.dimension.saturating_mul(std::mem::size_of::<f32>());
                let window = self
                    .node_limit
                    .max(exact.saturating_mul(2).saturating_add(128))
                    .saturating_add(CHECK_BYTES)
                    .saturating_add(64);
                verify_page_set(&self.pages, &self.storage, &self.page_set, window, admit)?;
                record_page_set_proof(self.storage_id, &self.page_set, self.pages_table_root);
                *verified = true;
            }
        }
        let admission = RefCell::new(admit);
        let lookup = PagedLookup {
            admission: Some(&admission),
            pages: &self.pages,
            storage: self.storage,
            dimension: self.dimension,
            quantization: self.quantization,
            node_limit: self.node_limit,
        };
        Ok(match request {
            PagedVectorReadRequest::Point { id } => {
                PagedVectorReadValue::Point(lookup.load_point(id)?)
            }
            PagedVectorReadRequest::Identity { data_id } => {
                PagedVectorReadValue::Identity(lookup.identity(data_id)?)
            }
            PagedVectorReadRequest::PointForRow { row, snapshot } => {
                PagedVectorReadValue::PointForRow(ScopedVectorCopy::from_admitted(
                    lookup.visible_point_for_row(row, snapshot)?,
                    None,
                ))
            }
            PagedVectorReadRequest::ExactRows {
                key,
                snapshot,
                after,
                limit,
            } => PagedVectorReadValue::ExactRows(lookup.exact_rows(
                key,
                snapshot,
                after,
                limit,
                &mut || (self.checkpoint)(),
            )?),
            PagedVectorReadRequest::VisibleCount { snapshot } => {
                PagedVectorReadValue::VisibleCount(ScopedVectorCopy::from_admitted(
                    lookup.visible_count(snapshot, self.frontier, self.points, &mut || {
                        (self.checkpoint)()
                    })?,
                    None,
                ))
            }
            PagedVectorReadRequest::RowWindow {
                snapshot,
                after,
                limit,
            } => PagedVectorReadValue::RowWindow(lookup.row_window(
                snapshot,
                after,
                limit,
                &mut || (self.checkpoint)(),
            )?),
            PagedVectorReadRequest::RowBounds => PagedVectorReadValue::RowBounds(
                ScopedVectorCopy::from_admitted(lookup.row_bounds(self.rows)?, None),
            ),
            PagedVectorReadRequest::PointAtOrAfter { row } => {
                PagedVectorReadValue::PointAtOrAfter(ScopedVectorCopy::from_admitted(
                    lookup
                        .graph_row_at_or_after(row)?
                        .map(|(row, _, point)| (row, point)),
                    None,
                ))
            }
            PagedVectorReadRequest::GraphPointForRow { row } => {
                PagedVectorReadValue::GraphPointForRow(ScopedVectorCopy::from_admitted(
                    lookup
                        .graph_row_at_or_after(row)?
                        .filter(|(found, _, _)| *found == row)
                        .map(|(_, _, point)| point),
                    None,
                ))
            }
            PagedVectorReadRequest::ExactGraphRowAfter { key, after } => {
                PagedVectorReadValue::ExactGraphRowAfter(ScopedVectorCopy::from_admitted(
                    lookup.exact_after(key, after)?.map(|(row, _)| row),
                    None,
                ))
            }
        })
    }

    fn load_point(&mut self, id: PointId) -> Result<PagedVectorPoint> {
        self.lookup().load_point(id)?.into_already_funded()
    }
    fn identity(&mut self, data_id: usize) -> Result<PagedVectorIdentity> {
        self.lookup().identity(data_id)?.into_already_funded()
    }
    fn point_for_row(&mut self, row: RowId, snapshot: SnapshotId) -> Result<Option<PointId>> {
        self.lookup().visible_point_for_row(row, snapshot)
    }
    fn exact_rows(
        &mut self,
        key: &[u8],
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
    ) -> Result<Vec<RowId>> {
        let checkpoint = Arc::clone(&self.checkpoint);
        self.lookup()
            .exact_rows(key, snapshot, after, limit, &mut || checkpoint())?
            .into_already_funded()
    }
    fn checkpoint(&mut self) -> Result<()> {
        (self.checkpoint)()
    }
    fn before_distance(&mut self) -> Result<()> {
        (self.checkpoint)()
    }
    fn before_visit(&mut self) -> Result<()> {
        (self.checkpoint)()
    }
    fn visible_count(&mut self, snapshot: SnapshotId) -> Result<usize> {
        let checkpoint = Arc::clone(&self.checkpoint);
        self.lookup()
            .visible_count(snapshot, self.frontier, self.points, &mut || checkpoint())
    }
    fn row_window(
        &mut self,
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
    ) -> Result<Vec<RowId>> {
        let checkpoint = Arc::clone(&self.checkpoint);
        self.lookup()
            .row_window(snapshot, after, limit, &mut || checkpoint())?
            .into_already_funded()
    }
    fn row_bounds(&mut self) -> Result<Option<(RowId, RowId, usize)>> {
        self.lookup().row_bounds(self.rows)
    }
    fn point_at_or_after(&mut self, row: RowId) -> Result<Option<(RowId, PointId)>> {
        Ok(self
            .lookup()
            .graph_row_at_or_after(row)?
            .map(|(row, _, point)| (row, point)))
    }
    fn exact_graph_row_after(&mut self, key: &[u8], after: Option<RowId>) -> Result<Option<RowId>> {
        Ok(self.lookup().exact_after(key, after)?.map(|(row, _)| row))
    }
}

/// Persistence entry points used by the engine's open and maintenance paths.
impl RedbPersistence {
    /// Writer serving readers capture the published writer snapshot.
    pub(crate) fn set_paged_root_capture(
        &self,
        capture: Arc<dyn Fn() -> Result<Arc<WriterSnapshot>> + Send + Sync>,
        scoped_capture: ScopedWriterRootCapture,
    ) {
        let _ = self.scoped_vector_root_capture.set(scoped_capture);
        let capture: PagedRootCapture = Arc::new(move || {
            let root = capture()?;
            let read = root.owner_read();
            Ok((read?, root as Arc<dyn std::any::Any + Send + Sync>))
        });
        let _ = self.paged_root_capture.set(capture);
    }

    pub(crate) fn paged_root_capture(&self) -> Result<PagedRootCapture> {
        self.paged_root_capture
            .get()
            .cloned()
            .ok_or_else(|| Error::Other("paged vector serving has no captured writer root".into()))
    }

    pub(crate) fn stage_paged_publication(
        &self,
        partition: &VectorPartitionRef,
        publication: PagedPublication,
    ) {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.paged_publications.lock().insert(key, publication);
    }

    pub(crate) fn clear_paged_publication(&self, partition: &VectorPartitionRef) {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.paged_publications.lock().remove(&key);
    }

    fn take_paged_publication(&self, record_key: &[u8]) -> Option<PagedPublication> {
        self.paged_publications.lock().remove(record_key)
    }

    /// Rebind the descriptor without rewriting its immutable storage pages.
    pub(super) fn rebind_paged_generation_in_write(
        &self,
        write: &redb::WriteTransaction,
        index: &VectorIndexRef,
        partition: &VectorPartitionKey,
        graph: &mut VectorPartitionGraphBytes,
    ) -> Result<()> {
        if !graph.bytes.starts_with(&META_MAGIC) {
            return Ok(());
        }
        let _memory = self
            .proof_accountant()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    graph
                        .bytes
                        .len()
                        .saturating_mul(8)
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len())
                        .saturating_add(4096),
                    "vector",
                    "rename_paged_generation",
                    "Raise MEMORY_LIMIT to rename this paged generation.",
                )
            })
            .transpose()?;
        let mut meta = PagedGenerationMeta::decode(&graph.bytes)?;
        meta.schema_binding = Self::vector_schema_binding_in_write(write, index)?;
        if let Some(saved) = &mut meta.saved {
            let key = Self::vector_partition_prefix(index, partition);
            saved.route = *blake3::hash(&key).as_bytes();
        }
        let route = Self::vector_partition_prefix(index, partition);
        import_tail::rebind_native_change_backing_in_write(write, &graph.bytes, &meta, &route)?;
        graph.bytes = Self::encode(&meta)?;
        graph.checksum = *blake3::hash(&graph.bytes).as_bytes();
        Ok(())
    }

    pub(super) fn rename_paged_build_bindings_in_write(
        &self,
        write: &redb::WriteTransaction,
        from: &VectorIndexRef,
        to: &VectorIndexRef,
    ) -> Result<()> {
        let _prefix_memory = self
            .proof_accountant()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    from.table
                        .len()
                        .saturating_add(from.column.len())
                        .saturating_add(to.table.len())
                        .saturating_add(to.column.len())
                        .saturating_mul(32)
                        .saturating_add(4096),
                    "vector",
                    "rename_paged_build",
                    "Raise MEMORY_LIMIT to rename this paged build.",
                )
            })
            .transpose()?;
        let binding = Self::vector_schema_binding_in_write(write, to)?;
        let mut prefix = Vec::new();
        Self::append_partition_key_component(&mut prefix, from.table.as_bytes());
        Self::append_partition_key_component(&mut prefix, from.column.as_bytes());
        let mut builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
        loop {
            let next = {
                let mut entries = builds
                    .prefix_bounded(prefix.as_slice())
                    .map_err(Self::storage_error)?;
                entries
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .map(|(key, value)| -> Result<_> {
                        let _memory = Self::admit_vector_image_build_route_with_owner(
                            self.proof_accountant(),
                            key.value(),
                            value.value().len(),
                        )?;
                        let route = vector_membership_pages::partition_from_prefix(key.value())?;
                        let mut record =
                            decode_image_build_record(open_value(key.value(), value.value())?)?;
                        record.schema_binding = binding;
                        let target = Self::vector_partition_prefix(to, &route.partition_key);
                        let encoded = Self::encode(&record)?;
                        Ok((key.value().to_vec(), target, encoded, _memory))
                    })
                    .transpose()?
            };
            let Some((old, target, encoded, _memory)) = next else {
                break;
            };
            builds
                .insert(target.as_slice(), seal_value(&target, &encoded).as_slice())
                .map_err(Self::storage_error)?;
            builds.remove(old.as_slice()).map_err(Self::storage_error)?;
        }
        let mut floors = write.open_table(FLOORS).map_err(Self::storage_error)?;
        loop {
            let next = {
                let end = writer_source::prefix_end(&prefix)
                    .ok_or_else(|| corruption("rename prefix has no successor"))?;
                let mut entries = floors
                    .range(prefix.as_slice()..end.as_slice())
                    .map_err(Self::storage_error)?;
                entries
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .map(|(key, value)| -> Result<_> {
                        let memory = Self::admit_vector_image_build_route_with_owner(
                            self.proof_accountant(),
                            key.value(),
                            0,
                        )?;
                        let route = vector_membership_pages::partition_from_prefix(key.value())?;
                        Ok((
                            key.value().to_vec(),
                            Self::vector_partition_prefix(to, &route.partition_key),
                            value.value(),
                            memory,
                        ))
                    })
                    .transpose()?
            };
            let Some((old, target, floor, _memory)) = next else {
                break;
            };
            floors
                .insert(target.as_slice(), floor)
                .map_err(Self::storage_error)?;
            floors.remove(old.as_slice()).map_err(Self::storage_error)?;
        }
        Ok(())
    }

    /// A build record's decode failure. A memory refusal, a read failure or
    /// a cancellation is the caller's typed outcome and travels unchanged;
    /// only a record that cannot be decoded is store corruption.
    fn build_record_decode_error(error: Error) -> Error {
        match error {
            Error::MemoryBudgetExceeded { .. } | Error::ReadFailure(_) | Error::ReadCancelled => {
                error
            }
            _ => corruption("paged build record cannot be decoded"),
        }
    }

    /// Every durable build record, for restart registration and floor restore.
    /// The inventory is one small control record per unpublished partition.
    /// The first durable build record after `after` (all records when None),
    /// in key order, from its own read: reopen streams build records one at a
    /// time instead of materializing the whole inventory.
    pub(crate) fn next_paged_build_record_after(
        &self,
        after: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, VectorPartitionRef, PagedBuildRecord)>> {
        self.saved_export_recovery_cutoff()?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let builds = match read.open_table(BUILDS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut range = match after {
                Some(after) => builds
                    .range::<&[u8]>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded)),
                None => builds.range::<&[u8]>(..),
            }
            .map_err(Self::storage_error)?;
            let Some(entry) = range.next() else {
                return Ok(None);
            };
            let (key, value) = entry.map_err(Self::storage_error)?;
            let record: PagedBuildRecord =
                Self::decode_exact(open_value(key.value(), value.value())?)
                    .map_err(Self::build_record_decode_error)?;
            let partition = vector_membership_pages::partition_from_prefix(key.value())?;
            Ok(Some((key.value().to_vec(), partition, record)))
        })
    }

    /// The first durable construction floor after `after`, in key order,
    /// from its own read.
    pub(crate) fn next_construction_floor_after(
        &self,
        after: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, VectorPartitionRef, TxId)>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let floors = match read.open_table(FLOORS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut range = match after {
                Some(after) => floors
                    .range::<&[u8]>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded)),
                None => floors.range::<&[u8]>(..),
            }
            .map_err(Self::storage_error)?;
            let Some(entry) = range.next() else {
                return Ok(None);
            };
            let (key, value) = entry.map_err(Self::storage_error)?;
            Ok(Some((
                key.value().to_vec(),
                vector_membership_pages::partition_from_prefix(key.value())?,
                TxId(value.value()),
            )))
        })
    }

    pub(crate) fn load_paged_build_records(
        &self,
    ) -> Result<Vec<(VectorPartitionRef, PagedBuildRecord)>> {
        self.saved_export_recovery_cutoff()?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let builds = match read.open_table(BUILDS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut records = Vec::new();
            for entry in builds.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let record: PagedBuildRecord =
                    Self::decode_exact(open_value(key.value(), value.value())?)
                        .map_err(|_| corruption("paged build record cannot be decoded"))?;
                let partition = vector_membership_pages::partition_from_prefix(key.value())?;
                records.push((partition, record));
            }
            Ok(records)
        })
    }

    pub(crate) fn load_paged_build_record(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<Option<PagedBuildRecord>> {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let builds = match read.open_table(BUILDS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            builds
                .get(key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| {
                    Self::decode_exact(open_value(&key, value.value())?)
                        .map_err(Self::build_record_decode_error)
                })
                .transpose()
        })
    }

    /// Reconcile fixed construction inputs before replacing a received image.
    /// The admitted image is already owned by this writer; each durable route
    /// owns only one control record and one authenticated source lookup at a time.
    pub(super) fn reconcile_paged_builds_for_image_in_write(
        &self,
        write: &redb::WriteTransaction,
        image: &CompleteVectorPartitionImage,
        previous: &redb::ReadTransaction,
        stage: &ReceivedSchemaPersistenceProjection,
    ) -> Result<()> {
        let affected_tables = &stage.affected_tables;
        let table_meta = &stage.table_meta;
        let retired = &stage.retired_vector_construction;
        let raw = match previous.open_byte_table_bounded(VECTORS_TABLE) {
            Ok(table) => Some(table),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(error) => return Err(Self::storage_error(error)),
        };
        let verifier = content_identity::ContentIdentityReadVerifier::open(
            previous,
            &self.path,
            Some(stage.vector_image_memory.accountant.clone()),
        )?;
        let mut proof = content_identity::ReadAuthenticationNodes::new(
            RAW_VECTOR_BODY_SCAN_PROOF_BYTES,
            Some(stage.vector_image_memory.accountant.clone()),
        );
        for table in affected_tables {
            let _prefix_memory = Self::admit_vector_image_build_route_with_owner(
                Some(stage.vector_image_memory.accountant.clone()),
                table.as_bytes(),
                0,
            )?;
            let mut prefix = Vec::new();
            Self::append_partition_key_component(&mut prefix, table.as_bytes());
            let mut builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
            let source = write
                .open_table(vector_membership_pages::SOURCE)
                .map_err(vector_membership_pages::metadata_error)?;
            let catalogs = write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(Self::storage_error)?;
            let mut cursor = None::<(
                Vec<u8>,
                Option<crate::memory_accounting::OwnedMemoryReservation>,
            )>;
            loop {
                let next = {
                    let start = cursor
                        .as_ref()
                        .map_or(Bound::Included(prefix.as_slice()), |(key, _)| {
                            Bound::Excluded(key.as_slice())
                        });
                    let mut entries = builds
                        .range::<&[u8]>((start, Bound::Unbounded))
                        .map_err(Self::storage_error)?;
                    entries
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .filter(|(key, _)| key.value().starts_with(&prefix))
                        .map(|(key, value)| -> Result<_> {
                            let memory = Self::admit_vector_image_build_route_with_owner(
                                Some(stage.vector_image_memory.accountant.clone()),
                                key.value(),
                                value.value().len(),
                            )?;
                            let record: PagedBuildRecord =
                                decode_image_build_record(open_value(key.value(), value.value())?)?;
                            Ok((key.value().to_vec(), record, memory))
                        })
                        .transpose()?
                };
                let Some((key, record, memory)) = next else {
                    break;
                };
                let partition = vector_membership_pages::partition_from_prefix(&key)?;
                let progress = record.progress()?;
                let mut surviving = 0usize;
                let declaration_survives = table_meta
                    .get(&partition.index.table)
                    .and_then(|meta| {
                        meta.columns
                            .iter()
                            .find(|column| column.name == partition.index.column)
                    })
                    .is_some_and(|column| {
                        column.column_type
                            == ColumnType::Vector(record.declaration.dimension as usize)
                            && column.quantization == record.declaration.quantization
                    });
                let binding_survives = declaration_survives
                    && Self::vector_schema_binding_in_write(write, &partition.index)?
                        == record.schema_binding;
                let mut valid = binding_survives
                    && record.previous_generation_id == 0
                    && catalogs
                        .get_bounded(key.as_slice())
                        .map_err(Self::storage_error)?
                        .is_none();
                if valid {
                    for member in image.memberships.iter().filter(|member| {
                        member.index == partition.index
                            && member.partition_key == partition.partition_key
                            && member.visible_from <= progress.frontier_tx
                            && member.deleted_tx.is_none_or(|tx| tx > progress.frontier_tx)
                    }) {
                        let source_key = vector_membership_pages::source_key(member);
                        let previous = source
                            .get_bounded(source_key.as_slice())
                            .map_err(vector_membership_pages::metadata_error)?
                            .map(|value| {
                                let memory = Self::admit_vector_image_build_route_with_owner(
                                    Some(stage.vector_image_memory.accountant.clone()),
                                    &source_key,
                                    value.value().len(),
                                )?;
                                Ok::<_, Error>((
                                    vector_membership_pages::decode_source_member(
                                        &source_key,
                                        value.value(),
                                    )?,
                                    memory,
                                ))
                            })
                            .transpose()?;
                        let Some((previous, _old_member_memory)) = previous else {
                            valid = false;
                            break;
                        };
                        if previous.visible_from != member.visible_from
                            || previous.deleted_tx != member.deleted_tx
                            || previous.visible_from > progress.frontier_tx
                            || previous
                                .deleted_tx
                                .is_some_and(|tx| tx <= progress.frontier_tx)
                        {
                            valid = false;
                            break;
                        }
                        let Some(next_body) = stage.vectors.iter().find(|entry| {
                            entry.index == member.index
                                && entry.row_id == member.row_id
                                && entry.created_tx == member.vector_created_tx
                                && entry.lsn == member.vector_lsn
                        }) else {
                            valid = false;
                            break;
                        };
                        // One exact old body, authenticated against the committed
                        // version commitment, and one accepted-image commitment.
                        // Refusal aborts; only a proven changed input cancels.
                        let body_memory = crate::read_image_memory::ImageMemory::charging_for(
                            stage.vector_image_memory.accountant.clone(),
                            "sync",
                            "compare_vector_image_input",
                            "Raise MEMORY_LIMIT to authenticate this fixed vector input.",
                        );
                        body_memory.reserve(
                            next_body
                                .index
                                .table
                                .len()
                                .saturating_add(next_body.index.column.len())
                                .saturating_add(64),
                        )?;
                        let body_key = Self::vector_key(next_body);
                        let Some(raw) = raw.as_ref() else {
                            valid = false;
                            break;
                        };
                        let Some(stored) = raw
                            .get_bounded(body_key.as_slice())
                            .map_err(Self::storage_error)?
                        else {
                            valid = false;
                            break;
                        };
                        body_memory.reserve(
                            stored
                                .value()
                                .len()
                                .saturating_mul(64)
                                .saturating_add(next_body.vector.len().saturating_mul(32))
                                .saturating_add(8192),
                        )?;
                        self.validate_raw_vector_entry_encoded_width(
                            stored.value(),
                            &member.index,
                            record.declaration.dimension as usize,
                        )?;
                        let old = decode_image_vector_entry(
                            stored.value(),
                            record.declaration.dimension as usize,
                        )?;
                        if old.index != member.index
                            || old.row_id != member.row_id
                            || old.created_tx != member.vector_created_tx
                            || old.lsn != member.vector_lsn
                            || old.vector.quantization() != record.declaration.quantization
                        {
                            return Err(corruption(
                                "fixed vector body differs from its source identity",
                            ));
                        }
                        let old_values = old.vector.to_f32();
                        let old_commitment =
                            content_identity::decoded_vector_version_value_mutation(
                                &old.index,
                                old.row_id,
                                old.created_tx,
                                old.lsn,
                                &old_values,
                                record.declaration.quantization,
                            );
                        verifier.verify_with_nodes(&mut proof, &old_commitment)?;
                        let next_commitment = content_identity::vector_version_mutation(
                            next_body,
                            record.declaration.quantization,
                        );
                        if old_commitment.logical_value != next_commitment.logical_value {
                            valid = false;
                            break;
                        }
                        surviving = surviving
                            .checked_add(1)
                            .ok_or_else(|| corruption("paged image input count overflow"))?;
                    }
                    valid &= surviving == progress.total_points;
                }
                let cursor_memory = Self::admit_vector_image_build_route_with_owner(
                    Some(stage.vector_image_memory.accountant.clone()),
                    &key,
                    0,
                )?;
                if !valid {
                    // Removing the predecessor catalog invalidates replacement
                    // builds. Lost fixed inputs invalidate initial builds too.
                    // Use the same finite cleanup queue as ordinary abandonment.
                    builds.remove(key.as_slice()).map_err(Self::storage_error)?;
                    write
                        .open_table(CLEANUP)
                        .map_err(Self::storage_error)?
                        .insert(record.storage_id, [].as_slice())
                        .map_err(Self::storage_error)?;
                    write
                        .open_table(FLOORS)
                        .map_err(Self::storage_error)?
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                    retired.lock().push((
                        partition,
                        memory
                            .map(crate::memory_accounting::ContainedMemoryReservation::from_owned),
                    ));
                }
                cursor = Some((key, cursor_memory));
            }
            // Whole-build candidates belong to the replaced registry image.
            let mut floors = write.open_table(FLOORS).map_err(Self::storage_error)?;
            loop {
                let next = floors
                    .range(prefix.as_slice()..)
                    .map_err(Self::storage_error)?
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .filter(|(key, _)| key.value().starts_with(&prefix))
                    .map(|(key, _)| -> Result<_> {
                        let memory = Self::admit_vector_image_build_route_with_owner(
                            Some(stage.vector_image_memory.accountant.clone()),
                            key.value(),
                            0,
                        )?;
                        Ok((key.value().to_vec(), memory))
                    })
                    .transpose()?;
                let Some((key, memory)) = next else {
                    break;
                };
                let partition = vector_membership_pages::partition_from_prefix(&key)?;
                floors.remove(key.as_slice()).map_err(Self::storage_error)?;
                if builds
                    .get_bounded(key.as_slice())
                    .map_err(Self::storage_error)?
                    .is_none()
                {
                    retired.lock().push((
                        partition,
                        memory
                            .map(crate::memory_accounting::ContainedMemoryReservation::from_owned),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn admit_vector_image_build_route(
        &self,
        key: &[u8],
        stored_bytes: usize,
    ) -> Result<Option<crate::memory_accounting::OwnedMemoryReservation>> {
        self.prepare_proof_owner()?;
        Self::admit_vector_image_build_route_with_owner(self.proof_accountant(), key, stored_bytes)
    }

    fn admit_vector_image_build_route_with_owner(
        accountant: Option<crate::memory_accounting::AccountantHandle>,
        key: &[u8],
        stored_bytes: usize,
    ) -> Result<Option<crate::memory_accounting::OwnedMemoryReservation>> {
        if stored_bytes != 0 && accountant.is_none() {
            return Err(corruption(
                "vector image record has no operation memory owner",
            ));
        }
        accountant
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    key.len()
                        .saturating_add(stored_bytes)
                        .saturating_mul(64)
                        .saturating_add(8192),
                    "vector",
                    "replace_vector_image_build",
                    "Raise MEMORY_LIMIT to reconcile vector construction with this image.",
                )
            })
            .transpose()
    }

    pub(super) fn seed_paged_image_frontiers_in_write(
        &self,
        write: &redb::WriteTransaction,
        affected_tables: &HashSet<String>,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<()> {
        let builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
        for table in affected_tables {
            let _prefix_memory = Self::admit_vector_image_build_route_with_owner(
                Some(accountant.clone()),
                table.as_bytes(),
                0,
            )?;
            let mut prefix = Vec::new();
            Self::append_partition_key_component(&mut prefix, table.as_bytes());
            for entry in builds
                .prefix_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let _memory = Self::admit_vector_image_build_route_with_owner(
                    Some(accountant.clone()),
                    key.value(),
                    value.value().len(),
                )?;
                let record: PagedBuildRecord =
                    decode_image_build_record(open_value(key.value(), value.value())?)?;
                let partition = vector_membership_pages::partition_from_prefix(key.value())?;
                vector_membership_pages::begin_pending_frontier(
                    write,
                    &partition,
                    record.progress()?.frontier_tx,
                )?;
            }
        }
        Ok(())
    }

    /// The import cohort after `covered`, never past `target`: whole committed
    /// LSNs, oldest first, while `fits` accepts their journal record count.
    /// Grouping reads only the ordered journal keys (prefix, LSN, ordinal),
    /// never a payload, so planning decodes nothing. A tombstone's key is
    /// counted too: the count is an upper bound, like the tail admission's.
    /// Returns `None` when no record lies in (covered, target], and
    /// `Some((0, first))` when the first LSN alone does not fit.
    pub(crate) fn vector_partition_journal_cohort_by_key(
        &self,
        partition: &VectorPartitionRef,
        covered: Lsn,
        target: Lsn,
        fits: impl Fn(usize) -> bool,
    ) -> Result<Option<(usize, Lsn)>> {
        let Some((lower, upper)) = Self::vector_partition_journal_suffix_bounds(
            &partition.index,
            &partition.partition_key,
            covered,
            Some(target),
        )?
        else {
            return Ok(None);
        };
        let lsn_at = lower.len() - 8;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let journal = match read.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut taken = 0usize;
            let mut through: Option<Lsn> = None;
            let mut group: Option<(Lsn, usize)> = None;
            for entry in journal
                .range(lower.as_slice()..upper.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, _) = entry.map_err(Self::storage_error)?;
                let lsn = key
                    .value()
                    .get(lsn_at..lsn_at + 8)
                    .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                    .map(|bytes| Lsn(u64::from_be_bytes(bytes)))
                    .ok_or_else(|| self.vector_partition_corruption("journal key has no LSN"))?;
                group = match group {
                    Some((current, records)) if current == lsn => Some((current, records + 1)),
                    Some((current, records)) => {
                        if !fits(taken.saturating_add(records)) {
                            return Ok(Some((taken, through.unwrap_or(current))));
                        }
                        taken += records;
                        through = Some(current);
                        Some((lsn, 1))
                    }
                    None => Some((lsn, 1)),
                };
            }
            Ok(match group {
                None => None,
                Some((current, records)) if fits(taken.saturating_add(records)) => {
                    Some((taken + records, current))
                }
                Some((current, _)) => Some((taken, through.unwrap_or(current))),
            })
        })
    }

    /// Live membership count of one partition at a past frontier: the
    /// memberships a build pinned at `(frontier_tx, frontier_lsn)` sources,
    /// by the same visibility rule its source scan applies. One record is
    /// decoded at a time under the caller's decode owner.
    pub(crate) fn vector_partition_live_membership_count_at(
        &self,
        partition: &VectorPartitionRef,
        frontier_tx: TxId,
        frontier_lsn: Lsn,
    ) -> Result<usize> {
        let prefix = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let memberships = match read.open_byte_table_bounded(VECTOR_PARTITION_MEMBERSHIP_TABLE)
            {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut live = 0usize;
            for entry in memberships
                .prefix_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
            {
                let (_, value) = entry.map_err(Self::storage_error)?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::vector_observations::note_membership_count_decode_for_test();
                let record: VectorPartitionMembershipRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::Membership,
                        value.value(),
                        &partition.index,
                        &partition.partition_key,
                    )?;
                if record.row_lsn <= frontier_lsn
                    && record.visible_from <= frontier_tx
                    && record
                        .deleted_tx
                        .is_none_or(|deleted| deleted > frontier_tx)
                {
                    live = live
                        .checked_add(1)
                        .ok_or_else(|| corruption("live membership count overflow"))?;
                }
            }
            Ok(live)
        })
    }

    /// Live membership count of one partition, from its maintained summary.
    /// Called under the engine commit gate, so it is the frontier's count.
    pub(crate) fn vector_partition_live_membership_count(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<usize> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            vector_membership_pages::partition_live_entries(&read, partition)
        })
    }

    /// Persist a new build's frontier record before any scratch page exists.
    /// The record is also the restored maintenance floor after a restart.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_paged_build(
        &self,
        partition: &VectorPartitionRef,
        declaration: PagedDeclaration,
        reason: PagedBuildReason,
        generation_id: u64,
        previous_generation_id: u64,
        frontier_tx: TxId,
        frontier_lsn: Lsn,
        total_points: usize,
        schema_binding: VectorSchemaBinding,
    ) -> Result<PagedBuildRecord> {
        self.begin_paged_build_inner(
            partition,
            declaration,
            reason,
            generation_id,
            previous_generation_id,
            frontier_tx,
            frontier_lsn,
            total_points,
            false,
            schema_binding,
        )
    }

    pub(crate) fn begin_resident_paged_export(
        &self,
        partition: &VectorPartitionRef,
        declaration: PagedDeclaration,
        reason: PagedBuildReason,
        generation_id: u64,
        previous_generation_id: u64,
        frontier_tx: TxId,
        frontier_lsn: Lsn,
        total_points: usize,
        schema_binding: VectorSchemaBinding,
    ) -> Result<PagedBuildRecord> {
        self.begin_paged_build_inner(
            partition,
            declaration,
            reason,
            generation_id,
            previous_generation_id,
            frontier_tx,
            frontier_lsn,
            total_points,
            true,
            schema_binding,
        )
    }

    fn begin_paged_build_inner(
        &self,
        partition: &VectorPartitionRef,
        declaration: PagedDeclaration,
        reason: PagedBuildReason,
        generation_id: u64,
        previous_generation_id: u64,
        frontier_tx: TxId,
        frontier_lsn: Lsn,
        total_points: usize,
        resident_export: bool,
        expected_binding: VectorSchemaBinding,
    ) -> Result<PagedBuildRecord> {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        let mut progress = PagedVectorProgress::new(
            generation_id,
            declaration.digest()?,
            frontier_tx,
            frontier_lsn,
            total_points,
        );
        if resident_export {
            progress.phase = PagedVectorPhase::Encoding;
            progress.last_source_key = Some(RESIDENT_EXPORT_MARKER.to_vec());
        }
        let selected_native = self
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )?
            .map(|catalog| self.catalog_has_native_import_backing(&catalog))
            .transpose()?
            .unwrap_or(false);
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            let schema_binding = Self::vector_schema_binding_in_write(&write, &partition.index)?;
            if expected_binding != schema_binding {
                return Err(corruption(
                    "saved build selection is stale after schema change",
                ));
            }
            // Purge exports and repair can start a new catalog without a
            // predecessor while retaining a native generation high-water ID.
            let native_route =
                selected_native || (previous_generation_id | generation_id) & MUTABLE_STORAGE != 0;
            if previous_generation_id > i64::MAX as u64 || generation_id > i64::MAX as u64 {
                return Err(Error::Other(
                    "paged vector generation identities are exhausted".into(),
                ));
            }
            let storage_id = {
                let mut counters = write.open_table(COUNTERS).map_err(Self::storage_error)?;
                let next = counters
                    .get(NEXT_STORAGE)
                    .map_err(Self::storage_error)?
                    .map(|value| value.value())
                    .unwrap_or(1);
                if next == 0 || next >= MUTABLE_STORAGE {
                    return Err(corruption("paged storage allocation counter is reserved"));
                }
                // Saved exports use this same monotone namespace for their
                // immutable artifact root. Never reuse the captured source ID.
                let next = if resident_export {
                    next.max(
                        (generation_id & !MUTABLE_STORAGE)
                            .checked_add(1)
                            .ok_or_else(|| {
                                Error::Other(
                                    "paged vector generation identities are exhausted".into(),
                                )
                            })?,
                    )
                } else {
                    next
                };
                // Recheck after the resident-export high-water adjustment.
                // Legacy storage must never enter the reserved native namespace.
                if next == 0 || next >= MUTABLE_STORAGE {
                    return Err(Error::Other(
                        "paged vector storage identities are exhausted".into(),
                    ));
                }
                counters
                    .insert(NEXT_STORAGE, next + 1)
                    .map_err(Self::storage_error)?;
                next
            };
            let storage_id = if native_route {
                storage_id | MUTABLE_STORAGE
            } else {
                storage_id
            };
            if resident_export || native_route {
                progress.generation_id = storage_id;
            }
            if native_route {
                let mut marker = storage_id.to_be_bytes().to_vec();
                marker.push(0);
                let mut owner = (key.len() as u32).to_be_bytes().to_vec();
                owner.extend_from_slice(&key);
                owner.extend_from_slice(&[0; 32]);
                write
                    .open_table(PAGES)
                    .map_err(Self::storage_error)?
                    .insert(marker.as_slice(), seal_value(&marker, &owner).as_slice())
                    .map_err(Self::storage_error)?;
            }
            vector_membership_pages::begin_pending_frontier(&write, partition, frontier_tx)?;
            let record = PagedBuildRecord {
                storage_id,
                previous_generation_id,
                reason,
                declaration,
                progress: ProgressRecord::from_progress(&progress),
                schema_binding,
            };
            {
                let mut builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
                if builds
                    .get(key.as_slice())
                    .map_err(Self::storage_error)?
                    .is_some()
                {
                    return Err(Error::Other(
                        "a paged vector build is already registered for this partition".into(),
                    ));
                }
                let payload = Self::encode(&record)?;
                builds
                    .insert(key.as_slice(), seal_value(&key, &payload).as_slice())
                    .map_err(Self::storage_error)?;
            }
            // The pages-root binding rewrites the identity roots that the
            // committed read bounds authenticate, so it precedes their refresh.
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)?;
            Ok(record)
        })
    }

    /// Drop an unregistrable build (its route or declaration is gone) and
    /// queue its storage for finite cleanup.
    pub(crate) fn discard_paged_build(&self, partition: &VectorPartitionRef) -> Result<()> {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
                let Some(value) = builds.remove(key.as_slice()).map_err(Self::storage_error)?
                else {
                    return Ok(());
                };
                let record: PagedBuildRecord = Self::decode_exact(open_value(&key, value.value())?)
                    .map_err(|_| corruption("paged build record cannot be decoded"))?;
                if record.storage_id == 0 {
                    return Err(corruption("paged build storage identity is reserved"));
                }
                drop(value);
                write
                    .open_table(CLEANUP)
                    .map_err(Self::storage_error)?
                    .insert(record.storage_id, [].as_slice())
                    .map_err(Self::storage_error)?;
            }
            // The pages-root binding rewrites the identity roots that the
            // committed read bounds authenticate, so it precedes their refresh.
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Queue a retired published generation's pages for finite cleanup, in
    /// the transaction that removes its base record.
    pub(crate) fn queue_paged_generation_cleanup_in_write(
        write: &redb::WriteTransaction,
        meta_bytes: &[u8],
    ) -> Result<()> {
        let storage_id = authenticated_native_storage_id(meta_bytes)?;
        if super::vector_saved::saved_export_owned_in_write(write, storage_id)? {
            return Ok(());
        }
        write
            .open_table(CLEANUP)
            .map_err(Self::storage_error)?
            .insert(storage_id, [].as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    /// One finite cleanup transaction: remove at most `max_keys` pages of the
    /// first queued storage identity. Returns the number of pages removed.
    pub(crate) fn paged_cleanup_step(
        &self,
        max_keys: usize,
        mut admit: impl FnMut(usize) -> Result<()>,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<usize> {
        let cutoff = self.saved_export_recovery_cutoff()?;
        self.with_db(|db| {
            checkpoint()?;
            let write = db.begin_write().map_err(Self::storage_error)?;
            super::vector_saved::recover_saved_exports_in_write(
                &write,
                cutoff,
                &mut admit,
                &mut checkpoint,
            )?;
            let removed = {
                let mut cleanup = match write.open_table(CLEANUP) {
                    Ok(table) => table,
                    Err(error) => return Err(Self::storage_error(error)),
                };
                let Some((storage_id, cursor)) = cleanup
                    .range(1..)
                    .map_err(Self::storage_error)?
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .map(|(key, value)| {
                        admit(
                            value
                                .value()
                                .len()
                                .saturating_add(3 * std::mem::size_of::<Vec<u8>>()),
                        )?;
                        Ok::<_, Error>((key.value(), value.value().to_vec()))
                    })
                    .transpose()?
                else {
                    drop(cleanup);
                    bind_pages_root(&write)?;
                    Self::write_read_bounds_roots_in_write(&write)?;
                    write.commit().map_err(Self::storage_error)?;
                    return Ok(0);
                };
                if super::vector_saved::saved_export_owned_in_write(&write, storage_id)? {
                    cleanup.remove(storage_id).map_err(Self::storage_error)?;
                    drop(cleanup);
                    bind_pages_root(&write)?;
                    Self::write_read_bounds_roots_in_write(&write)?;
                    write.commit().map_err(Self::storage_error)?;
                    return Ok(0);
                }
                if storage_id & MUTABLE_STORAGE != 0
                    && import_tail::native_storage_is_selected_in_write(self, &write, storage_id)?
                {
                    cleanup.remove(storage_id).map_err(Self::storage_error)?;
                    drop(cleanup);
                    bind_pages_root(&write)?;
                    Self::write_writer_recovery_bounds_in_write(&write, None)?;
                    write.commit().map_err(Self::storage_error)?;
                    return Ok(0);
                }
                let page_name = page_table_name(storage_id);
                let definition = TableDefinition::<&[u8], &[u8]>::new(&page_name);
                let mut pages = write.open_table(definition).map_err(Self::storage_error)?;
                let storage = storage_prefix(storage_id);
                let result = reclaim_storage_keys_with_admission(
                    &mut pages,
                    &storage,
                    (!cursor.is_empty()).then_some(cursor.as_slice()),
                    max_keys,
                    &mut |bytes| {
                        checkpoint()?;
                        admit(bytes)
                    },
                )?;
                if result.complete {
                    drop(pages);
                    if storage_id & MUTABLE_STORAGE != 0 {
                        write
                            .delete_table(definition)
                            .map_err(Self::storage_error)?;
                        let mut marker = storage.to_vec();
                        marker.push(0);
                        write
                            .open_table(PAGES)
                            .map_err(Self::storage_error)?
                            .remove(marker.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                    cleanup.remove(storage_id).map_err(Self::storage_error)?;
                } else if let Some(last) = &result.last_key {
                    cleanup
                        .insert(storage_id, last.as_slice())
                        .map_err(Self::storage_error)?;
                }
                result.keys_removed
            };
            // The pages-root binding rewrites the identity roots that the
            // committed read bounds authenticate, so it precedes their refresh.
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            checkpoint()?;
            write.commit().map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    pub(crate) fn has_paged_cleanup(&self) -> Result<bool> {
        if self.has_recoverable_saved_exports()? {
            return Ok(true);
        }
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            match read.open_table(CLEANUP) {
                Ok(table) => Ok(table
                    .range(1..)
                    .map_err(Self::storage_error)?
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .is_some()),
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
                Err(error) => Err(Self::storage_error(error)),
            }
        })
    }

    /// Authoritative erasure of one partition route's paged storage, inside the
    /// purge transaction: every paged base generation's pages, any unpublished
    /// build's scratch pages and record, and its whole-build floor record.
    pub(crate) fn erase_partition_paged_storage_in_write(
        &self,
        write: &redb::WriteTransaction,
        prefix: &[u8],
        accountant: Arc<dyn contextdb_vector::MemoryBudget>,
        operation: &str,
        own_storage: &[u64],
    ) -> Result<()> {
        Self::refuse_unresolved_vector_erasure_in_write(write, own_storage)?;
        let mut pages = write.open_table(PAGES).map_err(Self::storage_error)?;
        let mut cleanup = write.open_table(CLEANUP).map_err(Self::storage_error)?;
        let mut erase_storage = |storage_id: u64| -> Result<()> {
            let backing = {
                let mut table = write
                    .open_table(import_tail::CHANGE_BACKINGS)
                    .map_err(Self::storage_error)?;
                let value = table
                    .remove(storage_id.to_be_bytes().as_slice())
                    .map_err(Self::storage_error)?;
                value
                    .map(|value| {
                        import_tail::decode_native_change_backing(storage_id, value.value())
                    })
                    .transpose()?
            };
            for storage_id in [Some(storage_id), backing.map(|b| b.physical.storage_id)]
                .into_iter()
                .flatten()
            {
                let storage = storage_prefix(storage_id);
                let name = page_table_name(storage_id);
                let definition = TableDefinition::<&[u8], &[u8]>::new(&name);
                let mut private_pages = if storage_id & MUTABLE_STORAGE != 0 {
                    Some(write.open_table(definition).map_err(Self::storage_error)?)
                } else {
                    None
                };
                loop {
                    // A batch holds only its high-water seek-key working set. Its
                    // owner releases after the returned cursor has been dropped.
                    let mut owner = contextdb_vector::VectorCopyAdmission::new(accountant.clone());
                    let mut admitted = 0usize;
                    let step = reclaim_storage_keys_with_admission(
                        private_pages.as_mut().unwrap_or(&mut pages),
                        &storage,
                        None,
                        4096,
                        &mut |bytes| {
                            if bytes > admitted {
                                owner.reserve_for(
                                    bytes - admitted,
                                    "vector",
                                    operation,
                                    "Raise MEMORY_LIMIT before erasing vector storage.",
                                )?;
                                admitted = bytes;
                            }
                            Ok(())
                        },
                    )?;
                    if step.complete {
                        break;
                    }
                }
                drop(private_pages);
                if storage_id & MUTABLE_STORAGE != 0 {
                    write
                        .delete_table(definition)
                        .map_err(Self::storage_error)?;
                    let mut marker = storage.to_vec();
                    marker.push(0);
                    pages
                        .remove(marker.as_slice())
                        .map_err(Self::storage_error)?;
                }
                cleanup.remove(storage_id).map_err(Self::storage_error)?;
            }
            Ok(())
        };
        {
            let bases = match write.open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE) {
                Ok(table) => Some(table),
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(error) => return Err(Self::storage_error(error)),
            };
            if let Some(bases) = bases {
                for entry in bases.range(prefix..).map_err(Self::storage_error)? {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if !key.value().starts_with(prefix) {
                        break;
                    }
                    // Only one decoded route is held at a time; do not retain
                    // a partition-population-sized list of storage identities.
                    let mut owner = contextdb_vector::VectorCopyAdmission::new(accountant.clone());
                    owner.reserve_for(
                        value.value().len().saturating_add(std::mem::size_of::<
                            VectorPartitionBaseGenerationRecord,
                        >()),
                        "vector",
                        operation,
                        "Raise MEMORY_LIMIT before erasing vector storage.",
                    )?;
                    let record: VectorPartitionBaseGenerationRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::BaseGeneration,
                            value.value(),
                        )?;
                    if record.graph_format_version == VECTOR_GRAPH_PAGED_FORMAT_VERSION
                        || record.graph.bytes.starts_with(&META_MAGIC)
                    {
                        // The frozen decoder owns two progress cursors. Fund
                        // those copies while the decoded base is still held.
                        owner.reserve_for(
                            record
                                .graph
                                .bytes
                                .len()
                                .saturating_add(std::mem::size_of::<PagedGenerationMeta>()),
                            "vector",
                            operation,
                            "Raise MEMORY_LIMIT before erasing vector storage.",
                        )?;
                        let meta = PagedGenerationMeta::decode(&record.graph.bytes)?;
                        // The erasure's own replacement keeps its pages.
                        if !own_storage.contains(&meta.storage_id) {
                            erase_storage(meta.storage_id)?;
                        }
                    }
                }
            }
        }
        {
            let changes = write
                .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(Self::storage_error)?;
            for entry in changes.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if !key.value().starts_with(prefix) {
                    break;
                }
                let mut owner = contextdb_vector::VectorCopyAdmission::new(accountant.clone());
                owner.reserve_for(
                    value
                        .value()
                        .len()
                        .saturating_mul(4)
                        .saturating_add(128 * 1024),
                    "vector",
                    operation,
                    "Raise MEMORY_LIMIT before erasing vector storage.",
                )?;
                if let Some(storage) =
                    Self::authenticated_generation_storage(value.value(), key.value(), false)?
                    && !own_storage.contains(&storage)
                {
                    erase_storage(storage)?;
                }
            }
        }
        {
            let mut builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
            let removed = builds.remove(prefix).map_err(Self::storage_error)?;
            if let Some(value) = removed {
                let mut owner = contextdb_vector::VectorCopyAdmission::new(accountant.clone());
                owner.reserve_for(
                    value
                        .value()
                        .len()
                        .saturating_add(std::mem::size_of::<PagedBuildRecord>()),
                    "vector",
                    operation,
                    "Raise MEMORY_LIMIT before erasing vector storage.",
                )?;
                let record = decode_image_build_record(open_value(prefix, value.value())?)?;
                erase_storage(record.storage_id)?;
            }
        }
        write
            .open_table(FLOORS)
            .map_err(Self::storage_error)?
            .remove(prefix)
            .map_err(Self::storage_error)?;
        Ok(())
    }

    /// Durably record a whole build's frontier before it builds.
    pub(crate) fn record_construction_floor(
        &self,
        partition: &VectorPartitionRef,
        frontier: TxId,
    ) -> Result<()> {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            write
                .open_table(FLOORS)
                .map_err(Self::storage_error)?
                .insert(key.as_slice(), frontier.0)
                .map_err(Self::storage_error)?;
            vector_membership_pages::begin_pending_frontier(&write, partition, frontier)?;
            // The pages-root binding rewrites the identity roots that the
            // committed read bounds authenticate, so it precedes their refresh.
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }

    pub(crate) fn clear_construction_floor(&self, partition: &VectorPartitionRef) -> Result<()> {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let present = {
                let read = db.begin_read().map_err(Self::storage_error)?;
                match read.open_table(FLOORS) {
                    Ok(table) => table
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .is_some(),
                    Err(redb::TableError::TableDoesNotExist(_)) => false,
                    Err(error) => return Err(Self::storage_error(error)),
                }
            };
            if !present {
                return Ok(());
            }
            let write = db.begin_write().map_err(Self::storage_error)?;
            write
                .open_table(FLOORS)
                .map_err(Self::storage_error)?
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
            // The pages-root binding rewrites the identity roots that the
            // committed read bounds authenticate, so it precedes their refresh.
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }

    pub(crate) fn load_construction_floors(&self) -> Result<Vec<(VectorPartitionRef, TxId)>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let floors = match read.open_table(FLOORS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut out = Vec::new();
            for entry in floors.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                out.push((
                    vector_membership_pages::partition_from_prefix(key.value())?,
                    TxId(value.value()),
                ));
            }
            Ok(out)
        })
    }

    /// Final page-set root of a completed build's storage.
    pub(crate) fn paged_storage_page_set_root(&self, storage_id: u64) -> Result<PageSetRoot> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            if storage_id & MUTABLE_STORAGE != 0 {
                let name = page_table_name(storage_id);
                let table = TableDefinition::<&[u8], &[u8]>::new(&name);
                let digest = read
                    .table_root_fingerprint(table)
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| corruption("native replacement lost its page table"))?;
                let pages = read.open_table(table).map_err(Self::storage_error)?;
                let bytes = import_tail::native_bytes_in(&pages, storage_id)?;
                return Ok(PageSetRoot {
                    pages: MUTABLE_PAGE_SET,
                    bytes,
                    digest,
                });
            }
            let pages = read.open_table(PAGES).map_err(Self::storage_error)?;
            page_set_root_in(&pages, storage_id)
        })
    }

    pub(crate) fn encode_paged_generation_meta(
        storage_id: u64,
        declaration: &PagedDeclaration,
        progress: &PagedVectorProgress,
        schema_binding: VectorSchemaBinding,
        page_set: PageSetRoot,
    ) -> Result<Vec<u8>> {
        let mut serving = progress.clone();
        serving.last_source_key = None;
        serving.cleanup_cursor = None;
        serving.encoding_cursor = None;
        Self::encode(&PagedGenerationMeta {
            magic: META_MAGIC,
            storage_id,
            declaration: declaration.clone(),
            progress: ProgressRecord::from_progress(&serving),
            saved: None,
            schema_binding,
            page_set,
        })
    }
}

/// Zero is never allocated as native storage. It is the authenticated fence
/// for storage whose damaged ownership reference has been retired.
const REPAIR_FENCE: u64 = 0;

impl RedbPersistence {
    pub(super) fn repair_fence_in_write(write: &redb::WriteTransaction) -> Result<bool> {
        match write.open_table(CLEANUP) {
            Ok(table) => Ok(table
                .get(REPAIR_FENCE)
                .map_err(Self::storage_error)?
                .is_some()),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
            Err(error) => Err(Self::storage_error(error)),
        }
    }

    pub(crate) fn has_vector_repair_fence(&self) -> Result<bool> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            match read.open_table(CLEANUP) {
                Ok(table) => Ok(table
                    .get(REPAIR_FENCE)
                    .map_err(Self::storage_error)?
                    .is_some()),
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
                Err(error) => Err(Self::storage_error(error)),
            }
        })
    }

    /// The decoded cutoff is proof input, never evidence that the fence is absent.
    /// Malformed fence bytes force a new proof over the current allocation range.
    fn repair_fence_cutoff_in_read(read: &redb::ReadTransaction) -> Result<Option<(u64, bool)>> {
        let cleanup = match read.open_table(CLEANUP) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let Some(value) = cleanup.get(REPAIR_FENCE).map_err(Self::storage_error)? else {
            return Ok(None);
        };
        let decoded = (value.value().len() <= 1024)
            .then(|| open_value(&REPAIR_FENCE.to_be_bytes(), value.value()).ok())
            .flatten()
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
            .map(u64::from_be_bytes);
        if let Some(cutoff) = decoded.filter(|cutoff| *cutoff != 0) {
            return Ok(Some((cutoff, true)));
        }
        let counters = read.open_table(COUNTERS).map_err(Self::storage_error)?;
        let cutoff = counters
            .get(NEXT_STORAGE)
            .map_err(Self::storage_error)?
            .map(|value| value.value())
            .unwrap_or(1);
        if cutoff == 0 {
            return Err(corruption("repair fence allocation cutoff is reserved"));
        }
        Ok(Some((cutoff, false)))
    }

    pub(super) fn raise_repair_fence_in_write(write: &redb::WriteTransaction) -> Result<u64> {
        let mut counters = write.open_table(COUNTERS).map_err(Self::storage_error)?;
        let next = counters
            .get(NEXT_STORAGE)
            .map_err(Self::storage_error)?
            .map(|value| value.value())
            .unwrap_or(1);
        if next == 0 || next >= MUTABLE_STORAGE {
            return Err(corruption("repair fence allocation counter is reserved"));
        }
        // Reserve an unused identity so even consecutive retirements without
        // construction have distinct, strictly increasing proof epochs.
        let cutoff = next
            .checked_add(1)
            .ok_or_else(|| Error::Other("paged vector storage identities are exhausted".into()))?;
        counters
            .insert(NEXT_STORAGE, cutoff)
            .map_err(Self::storage_error)?;
        drop(counters);
        let mut cleanup = write.open_table(CLEANUP).map_err(Self::storage_error)?;
        let previous = cleanup
            .get(REPAIR_FENCE)
            .map_err(Self::storage_error)?
            .and_then(|value| {
                (value.value().len() <= 1024)
                    .then(|| open_value(&REPAIR_FENCE.to_be_bytes(), value.value()).ok())
                    .flatten()
                    .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                    .map(u64::from_be_bytes)
            });
        if previous.is_some_and(|previous| previous >= cutoff) {
            // A valid fence cannot refer to an unallocated future identity.
            return Err(corruption(
                "repair fence exceeds the storage allocation counter",
            ));
        }
        let encoded = seal_value(&REPAIR_FENCE.to_be_bytes(), &cutoff.to_be_bytes());
        cleanup
            .insert(REPAIR_FENCE, encoded.as_slice())
            .map_err(Self::storage_error)?;
        Ok(cutoff)
    }

    /// Atomic logical retirement. No body is decoded and no storage id is
    /// guessed. Healthy retained generation records and authoritative vectors
    /// remain; bounded cleanup removes eligible records separately.
    pub(crate) fn retire_broken_vector_catalog(
        &self,
        partition: &VectorPartitionRef,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<()> {
        let key = Self::vector_partition_catalog_key_from_parts(
            &partition.index,
            &partition.partition_key,
        );
        self.with_db(|db| {
            checkpoint()?;
            let write = db.begin_write().map_err(Self::storage_error)?;
            Self::raise_repair_fence_in_write(&write)?;
            write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(Self::storage_error)?
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
            vector_membership_pages::reset_pending_frontier(&write, partition)?;
            // An old construction record cannot install into the reset route.
            // Only an authenticated record is permitted to name cleanup storage.
            let mut builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
            let existing = builds.get(key.as_slice()).map_err(Self::storage_error)?;
            let storage = existing
                .as_ref()
                .and_then(|value| Self::authenticated_build_storage_id(&key, value.value()).ok());
            drop(existing);
            builds.remove(key.as_slice()).map_err(Self::storage_error)?;
            drop(builds);
            if let Some(storage) = storage.filter(|id| *id != REPAIR_FENCE) {
                write
                    .open_table(CLEANUP)
                    .map_err(Self::storage_error)?
                    .insert(storage, [].as_slice())
                    .map_err(Self::storage_error)?;
            }
            write
                .open_table(FLOORS)
                .map_err(Self::storage_error)?
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
            // The pages-root binding rewrites the identity roots that the
            // committed read bounds authenticate, so it precedes their refresh.
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            checkpoint()?;
            write.commit().map_err(Self::storage_error)
        })
    }

    /// A restart learns the retired initial-build route from existing durable
    /// facts. No volatile quarantine or new marker directory is required.
    pub(crate) fn retired_vector_rebuild_pending(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<bool> {
        self.prepare_proof_owner()?;
        if !self.has_vector_repair_fence()? {
            return Ok(false);
        }
        let key_bound = Self::vector_partition_physical_key_bound(
            &partition.index,
            &partition.partition_key,
            0,
        );
        // canonical_bytes and the physical lookup key coexist. Both are
        // admitted before either is constructed; no catalog is owned/decoded.
        let _keys = self
            .proof_accountant()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    key_bound.saturating_mul(4),
                    "vector_index",
                    "prepare_vector_generation",
                    "Free writer memory before inspecting retired vector routes.",
                )
            })
            .transpose()?;
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        let catalog_present = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
                Err(error) => return Err(Self::storage_error(error)),
            };
            Ok(table
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
                .is_some())
        })?;
        // Present but corrupt catalogs are handled by this route's existing
        // quarantine/repair path. They cannot abort another route's sample.
        if catalog_present {
            return Ok(false);
        }
        drop(key);
        drop(_keys);
        Ok(self.highest_retired_vector_generation_key(partition)? != 0)
    }

    /// Physical generation identities remain ordered even if their payload is
    /// unusable. The caller has already retired the catalog under the fence.
    /// Last-key seeks inspect only two B-tree paths, not every retained body.
    pub(crate) fn highest_retired_vector_generation_key(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<u64> {
        self.prepare_proof_owner()?;
        let key_bound = Self::vector_partition_physical_key_bound(
            &partition.index,
            &partition.partition_key,
            16,
        );
        let _keys = self
            .proof_accountant()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    key_bound.saturating_mul(4),
                    "vector_index",
                    "prepare_vector_generation",
                    "Free writer memory before inspecting retired generation identities.",
                )
            })
            .transpose()?;
        let prefix = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        let end = writer_source::prefix_end(&prefix)
            .ok_or_else(|| corruption("vector generation prefix has no upper bound"))?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let mut highest = 0;
            for (definition, suffix) in [
                (VECTOR_PARTITION_BASE_GENERATION_TABLE, 8usize),
                (VECTOR_PARTITION_CHANGE_GENERATION_TABLE, 16usize),
            ] {
                let table = match read.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => continue,
                    Err(error) => return Err(Self::storage_error(error)),
                };
                let last = table
                    .range(prefix.as_slice()..end.as_slice())
                    .map_err(Self::storage_error)?
                    .next_back()
                    .transpose()
                    .map_err(Self::storage_error)?;
                if let Some((key, _)) = last {
                    if !key.value().starts_with(&prefix)
                        || key.value().len() != prefix.len() + suffix
                    {
                        return Err(corruption("vector generation physical key is malformed"));
                    }
                    for bytes in key.value()[prefix.len()..].chunks_exact(8) {
                        highest = highest.max(u64::from_be_bytes(bytes.try_into().unwrap()));
                    }
                }
            }
            Ok(highest)
        })
    }

    /// `own_storage` names the replacement exports this erasure itself prepared:
    /// they are its staging, published in the same transaction, not unresolved
    /// ownership.
    pub(super) fn refuse_unresolved_vector_erasure_in_write(
        write: &redb::WriteTransaction,
        own_storage: &[u64],
    ) -> Result<()> {
        let cleanup = match write.open_table(CLEANUP) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        if !cleanup.is_empty().map_err(Self::storage_error)? {
            return Err(corruption(
                "vector page ownership or cleanup is incomplete; run maintenance before authoritative erasure",
            ));
        }
        let foreign_export = {
            let exports = write
                .open_table(super::vector_saved::SAVED_EXPORTS)
                .map_err(Self::storage_error)?;
            let mut foreign = false;
            for entry in exports.iter().map_err(Self::storage_error)? {
                let (storage, _) = entry.map_err(Self::storage_error)?;
                if !own_storage.contains(&storage.value()) {
                    foreign = true;
                    break;
                }
            }
            foreign
        };
        if foreign_export {
            return Err(corruption(
                "unpublished saved graph ownership is incomplete; run maintenance before authoritative erasure",
            ));
        }
        Ok(())
    }
}

/// Only an authenticated native metadata body may supply deletion identity.
pub(super) fn authenticated_native_storage_id(bytes: &[u8]) -> Result<u64> {
    // Inspect positionally, never materialize ProgressRecord's cursor Vecs.
    // Over-width authenticated metadata is UNKNOWN to the orphan proof.
    if bytes.len() > 64 * 1024 {
        return Err(corruption(
            "native ownership metadata exceeds its bounded window",
        ));
    }
    type Meta = (
        [u8; 8],
        u64,
        PagedDeclaration,
        InspectedProgressRecord,
        Option<SavedGenerationBinding>,
        VectorSchemaBinding,
        PageSetRoot,
    );
    let ((magic, id, _, progress, _, _, _), consumed) =
        bincode::serde::borrow_decode_from_slice::<Meta, _>(bytes, bincode::config::standard())
            .map_err(|_| corruption("native ownership metadata cannot be inspected"))?;
    if consumed != bytes.len()
        || magic != META_MAGIC
        || id == REPAIR_FENCE
        || progress
            .last_source_key
            .as_ref()
            .is_some_and(|key| key.count > 64 * 1024)
        || progress
            .cleanup_cursor
            .as_ref()
            .is_some_and(|key| key.count > 64 * 1024)
    {
        return Err(corruption(
            "native ownership metadata is invalid or oversized",
        ));
    }
    Ok(id)
}

impl RedbPersistence {
    pub(super) fn authenticated_build_storage_id(key: &[u8], encoded: &[u8]) -> Result<u64> {
        if encoded.len() > 64 * 1024 {
            return Err(corruption(
                "build ownership metadata exceeds its bounded window",
            ));
        }
        let payload = open_value(key, encoded)?;
        type Record = (
            u64,
            u64,
            PagedBuildReason,
            PagedDeclaration,
            InspectedProgressRecord,
            VectorSchemaBinding,
        );
        let ((id, _, _, _, progress, _), consumed) = bincode::serde::borrow_decode_from_slice::<
            Record,
            _,
        >(payload, bincode::config::standard())
        .map_err(|_| corruption("build ownership metadata cannot be inspected"))?;
        if consumed != payload.len()
            || id == REPAIR_FENCE
            || progress
                .last_source_key
                .as_ref()
                .is_some_and(|key| key.count > 64 * 1024)
            || progress
                .cleanup_cursor
                .as_ref()
                .is_some_and(|key| key.count > 64 * 1024)
        {
            return Err(corruption(
                "build ownership metadata is invalid or oversized",
            ));
        }
        Ok(id)
    }

    pub(super) fn queue_authenticated_native_storage_in_write(
        write: &redb::WriteTransaction,
        id: u64,
    ) -> Result<()> {
        if id == REPAIR_FENCE {
            return Err(corruption("native storage identity is reserved"));
        }
        if super::vector_saved::saved_export_owned_in_write(write, id)? {
            return Ok(());
        }
        let backing = {
            let mut table = write
                .open_table(import_tail::CHANGE_BACKINGS)
                .map_err(Self::storage_error)?;
            let value = table
                .remove(id.to_be_bytes().as_slice())
                .map_err(Self::storage_error)?;
            value
                .map(|value| import_tail::decode_native_change_backing(id, value.value()))
                .transpose()?
        };
        let mut cleanup = write.open_table(CLEANUP).map_err(Self::storage_error)?;
        if let Some(backing) = backing {
            cleanup
                .insert(backing.physical.storage_id, [].as_slice())
                .map_err(Self::storage_error)?;
        }
        cleanup
            .insert(id, [].as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    /// Borrow graph bytes, authenticate their envelope and checksum, and
    /// bound owned route metadata independently of the graph population.
    pub(super) fn authenticated_generation_storage(
        encoded: &[u8],
        physical: &[u8],
        is_base: bool,
    ) -> Result<Option<u64>> {
        let kind = if is_base {
            VectorPartitionRecordKind::BaseGeneration
        } else {
            VectorPartitionRecordKind::ChangeGeneration
        };
        let payload = vector_partition_record_payload(kind, encoded).map_err(corruption)?;
        let route_bytes = inspect_vector_partition_metadata_route(payload).map_err(corruption)?;
        let route_decode_bound = Self::vector_partition_record_decode_bound(
            route_bytes,
            std::mem::size_of::<BorrowedVectorPartitionBaseGenerationRecord<'_>>().max(
                std::mem::size_of::<BorrowedVectorPartitionChangeGenerationRecord<'_>>(),
            ),
        );
        if route_decode_bound > 64 * 1024 || physical.len() > 64 * 1024 {
            return Err(corruption(
                "generation route exceeds bounded ownership inspection",
            ));
        }
        prevalidate_vector_partition_metadata_routes(kind, payload).map_err(corruption)?;
        let (format, graph) = if is_base {
            let (record, consumed): (BorrowedVectorPartitionBaseGenerationRecord<'_>, usize) =
                bincode::serde::borrow_decode_from_slice(payload, bincode::config::standard())
                    .map_err(|_| corruption("base ownership reference cannot be decoded"))?;
            if consumed != payload.len()
                || Self::vector_partition_base_generation_key_from_parts(
                    &record.index,
                    &record.partition_key,
                    record.generation_id,
                )
                .as_slice()
                    != physical
            {
                return Err(corruption(
                    "base ownership reference disagrees with its physical key",
                ));
            }
            (record.graph_format_version, record.graph)
        } else {
            let (record, consumed): (BorrowedVectorPartitionChangeGenerationRecord<'_>, usize) =
                bincode::serde::borrow_decode_from_slice(payload, bincode::config::standard())
                    .map_err(|_| corruption("change ownership reference cannot be decoded"))?;
            if consumed != payload.len()
                || Self::vector_partition_change_generation_key_from_parts(
                    &record.index,
                    &record.partition_key,
                    record.base_generation_id,
                    record.change_generation_id,
                )
                .as_slice()
                    != physical
            {
                return Err(corruption(
                    "change ownership reference disagrees with its physical key",
                ));
            }
            (record.graph_format_version, record.graph)
        };
        if *blake3::hash(graph.bytes).as_bytes() != graph.checksum {
            return Err(corruption("ownership reference graph checksum mismatch"));
        }
        if format == VECTOR_GRAPH_PAGED_FORMAT_VERSION
            || (format == 1 && graph.bytes.starts_with(&META_MAGIC))
        {
            Ok(Some(authenticated_native_storage_id(graph.bytes)?))
        } else if format == 1 {
            Ok(None)
        } else {
            Err(corruption("ownership reference format is unsupported"))
        }
    }
}

const AUDIT_KEYS: usize = 64;
const AUDIT_METADATA_BYTES: usize = 256 * 1024;
const AUDIT_REFERENCE_BYTES: usize = 32 * 1024 * 1024;

/// One volatile proof cursor, never a set indexed by the population. A restart
/// repeats proof; the durable fence is the sole authority to refuse erasure.
pub(super) struct RepairAudit {
    cutoff: u64,
    epoch: [Option<[u8; 32]>; 6],
    after_storage: u64,
    candidate: Option<u64>,
    phase: usize,
    after_key: Vec<u8>,
    referenced: bool,
    unknown: bool,
    finished: bool,
    pending_orphan: Option<u64>,
    validated_references: bool,
    pub(super) _memory: crate::memory_accounting::OwnedMemoryReservation,
}

#[derive(Clone, Copy)]
pub(crate) enum RepairAuditStatus {
    Absent,
    Progressing,
    Blocked,
}

impl RedbPersistence {
    fn repair_reference_epoch(read: &redb::ReadTransaction) -> Result<[Option<[u8; 32]>; 6]> {
        Ok([
            read.table_root_fingerprint(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(Self::storage_error)?,
            read.table_root_fingerprint(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(Self::storage_error)?,
            read.table_root_fingerprint(BUILDS)
                .map_err(Self::storage_error)?,
            read.table_root_fingerprint(VECTOR_PARTITION_JOURNAL_TABLE)
                .map_err(Self::storage_error)?,
            read.table_root_fingerprint(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(Self::storage_error)?,
            read.table_root_fingerprint(import_tail::CHANGE_BACKINGS)
                .map_err(Self::storage_error)?,
        ])
    }

    fn repair_audit_cursor(
        &self,
        cutoff: u64,
        epoch: [Option<[u8; 32]>; 6],
    ) -> Result<RepairAudit> {
        self.prepare_proof_owner()?;
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("vector ownership audit requires the owning accountant".into())
        })?;
        let memory = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            accountant,
            AUDIT_METADATA_BYTES,
            "vector",
            "audit_vector_page_ownership",
            "Free writer memory before auditing retired vector pages.",
        )?;
        Ok(RepairAudit {
            cutoff,
            epoch,
            after_storage: 0,
            candidate: None,
            phase: 0,
            after_key: Vec::new(),
            referenced: false,
            unknown: false,
            finished: false,
            pending_orphan: None,
            validated_references: false,
            _memory: memory,
        })
    }

    /// The caller holds the engine's existing maintenance publication guard
    /// and source-page admission. No root is retained across calls.
    pub(crate) fn repair_orphan_audit_step(
        &self,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<RepairAuditStatus> {
        let mut cursor = self.vector_repair_audit.lock();
        let result = self.with_db(|db| {
            checkpoint()?;
            let read = db.begin_read().map_err(Self::storage_error)?;
            let Some((cutoff, authenticated)) = Self::repair_fence_cutoff_in_read(&read)? else {
                *cursor = None;
                return Ok(RepairAuditStatus::Absent);
            };
            let epoch = Self::repair_reference_epoch(&read)?;
            if cursor
                .as_ref()
                .is_none_or(|c| c.cutoff != cutoff || c.epoch != epoch)
            {
                *cursor = Some(self.repair_audit_cursor(cutoff, epoch)?);
            }
            let c = cursor.as_mut().unwrap();
            if c.finished && c.unknown {
                return Ok(RepairAuditStatus::Blocked);
            }
            let mut scanned = 0;
            let mut metadata = 0usize;
            let mut inspected = 0usize;
            let mut queue = None;
            while scanned < AUDIT_KEYS && metadata < AUDIT_METADATA_BYTES {
                checkpoint()?;
                // Independently validate all retired references even when no
                // PAGES exist. A corrupt whole record keeps restart's fence
                // until bounded generation cleanup retires that record.
                if !c.validated_references && c.phase == 5 {
                    c.validated_references = true;
                    c.phase = 0;
                    c.after_key.clear();
                    if c.unknown {
                        c.finished = true;
                        break;
                    }
                }
                if c.validated_references && c.candidate.is_none() {
                    let pages = match read.open_table(PAGES) {
                        Ok(table) => Some(table),
                        Err(redb::TableError::TableDoesNotExist(_)) => None,
                        Err(error) => return Err(Self::storage_error(error)),
                    };
                    let lower = c.after_storage.saturating_add(1).to_be_bytes();
                    let next = if let Some(pages) = pages.as_ref() {
                        pages
                            .range(lower.as_slice()..)
                            .map_err(Self::storage_error)?
                            .next()
                            .transpose()
                            .map_err(Self::storage_error)?
                    } else {
                        None
                    };
                    let id = next
                        .as_ref()
                        .and_then(|(key, _)| key.value().get(..8))
                        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                        .map(u64::from_be_bytes);
                    if next.is_some() && id.is_none() {
                        c.unknown = true;
                        c.finished = true;
                        break;
                    }
                    let Some(id) = id else {
                        c.finished = true;
                        break;
                    };
                    if id < MUTABLE_STORAGE && id >= cutoff {
                        // New ordinary storage is outside restart's cutoff;
                        // seek directly to the native namespace instead.
                        c.after_storage = MUTABLE_STORAGE - 1;
                        continue;
                    }
                    if id & !MUTABLE_STORAGE >= cutoff {
                        c.finished = true;
                        break;
                    }
                    if id == REPAIR_FENCE {
                        c.unknown = true;
                        c.finished = true;
                        break;
                    }
                    c.candidate = Some(id);
                    c.phase = 0;
                    c.after_key.clear();
                    c.referenced = false;
                    scanned += 1;
                    // Existing positive rows already own the deletion cursor.
                    let cleanup = read.open_table(CLEANUP).map_err(Self::storage_error)?;
                    if cleanup.get(id).map_err(Self::storage_error)?.is_some() {
                        c.after_storage = id;
                        c.candidate = None;
                        continue;
                    }
                }
                let id = c.candidate;
                if c.validated_references && (c.phase == 5 || c.referenced) {
                    let id = id.unwrap();
                    c.after_storage = id;
                    c.candidate = None;
                    if !c.referenced && !c.unknown {
                        queue = c.pending_orphan.replace(id);
                        if queue.is_some() {
                            break;
                        }
                    }
                    continue;
                }
                let definition = match c.phase {
                    0 => VECTOR_PARTITION_BASE_GENERATION_TABLE,
                    1 => VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
                    2 => BUILDS,
                    3 => VECTOR_PARTITION_JOURNAL_TABLE,
                    _ => {
                        // Backings carry one bounded physical reference under their old storage key.
                        let table = match read.open_table(import_tail::CHANGE_BACKINGS) {
                            Ok(table) => table,
                            Err(redb::TableError::TableDoesNotExist(_)) => {
                                c.phase += 1;
                                c.after_key.clear();
                                continue;
                            }
                            Err(error) => return Err(Self::storage_error(error)),
                        };
                        let lower = if c.after_key.is_empty() {
                            0
                        } else {
                            u64::from_be_bytes(
                                c.after_key
                                    .as_slice()
                                    .try_into()
                                    .map_err(|_| corruption("backing audit cursor width"))?,
                            )
                            .saturating_add(1)
                        };
                        let next = table
                            .range(lower.to_be_bytes().as_slice()..)
                            .map_err(Self::storage_error)?
                            .next()
                            .transpose()
                            .map_err(Self::storage_error)?;
                        if let Some((key, value)) = next {
                            scanned += 1;
                            metadata = metadata.saturating_add(value.value().len().min(64 * 1024));
                            match import_tail::decode_native_change_backing(
                                u64::from_be_bytes(
                                    key.value()
                                        .try_into()
                                        .map_err(|_| corruption("backing storage key width"))?,
                                ),
                                value.value(),
                            ) {
                                Ok(binding) => {
                                    if id == Some(binding.physical.storage_id) {
                                        c.referenced = true;
                                    }
                                }
                                Err(_) => c.unknown = true,
                            }
                            c.after_key = key.value().to_vec();
                        } else {
                            c.phase += 1;
                            c.after_key.clear();
                        }
                        continue;
                    }
                };
                let table = match read.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => {
                        c.phase += 1;
                        c.after_key.clear();
                        continue;
                    }
                    Err(error) => return Err(Self::storage_error(error)),
                };
                let lower = if c.after_key.is_empty() {
                    std::ops::Bound::Unbounded
                } else {
                    std::ops::Bound::Excluded(c.after_key.as_slice())
                };
                let next = table
                    .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?;
                let Some((key, value)) = next else {
                    c.phase += 1;
                    c.after_key.clear();
                    continue;
                };
                scanned += 1;
                if key.value().len() > 64 * 1024 {
                    c.unknown = true;
                    c.finished = true;
                    break;
                }
                if value.value().len() <= AUDIT_REFERENCE_BYTES
                    && inspected != 0
                    && inspected.saturating_add(value.value().len()) > AUDIT_REFERENCE_BYTES
                {
                    break;
                }
                inspected =
                    inspected.saturating_add(value.value().len().min(AUDIT_REFERENCE_BYTES));
                metadata = metadata.saturating_add(key.value().len());
                // Accounted source-page admission precedes allocation. The
                // proof never clones an entire graph or decodes an owned one.
                let mut reference = if value.value().len() > AUDIT_REFERENCE_BYTES {
                    Err(corruption("ownership reference exceeds bounded inspection"))
                } else if c.phase < 2 {
                    Self::authenticated_generation_storage(value.value(), key.value(), c.phase == 0)
                } else if c.phase == 2 {
                    Self::authenticated_build_storage_id(key.value(), value.value()).map(Some)
                } else {
                    Self::authenticated_journal_ownership_reference(value.value(), key.value())
                };
                if reference.is_ok() && c.phase == 0 {
                    let prefix_len = key.value().len().checked_sub(8);
                    let catalogs = match read.open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    {
                        Ok(table) => Some(table),
                        Err(redb::TableError::TableDoesNotExist(_)) => None,
                        Err(error) => return Err(Self::storage_error(error)),
                    };
                    let catalog_present =
                        if let (Some(table), Some(end)) = (catalogs.as_ref(), prefix_len) {
                            table
                                .get(&key.value()[..end])
                                .map_err(Self::storage_error)?
                                .is_some()
                        } else {
                            false
                        };
                    if !catalog_present {
                        reference = Err(corruption(
                            "retired route has not published its replacement catalog",
                        ));
                    }
                }
                match reference {
                    Ok(Some(storage)) if id == Some(storage) => c.referenced = true,
                    Ok(_) => {}
                    Err(_) => c.unknown = true,
                }
                c.after_key.clear();
                c.after_key.extend_from_slice(key.value());
            }
            let finished = c.finished;
            let unknown = c.unknown;
            if finished && !unknown {
                queue = c.pending_orphan.take();
            }
            drop(read);
            // Reference publication cannot interleave: the caller owns the
            // commit gate. Recheck the fence inside the cleanup transaction.
            if queue.is_some() || (finished && !unknown) || !authenticated {
                checkpoint()?;
                let write = db.begin_write().map_err(Self::storage_error)?;
                let mut cleanup = write.open_table(CLEANUP).map_err(Self::storage_error)?;
                let current = cleanup.get(REPAIR_FENCE).map_err(Self::storage_error)?;
                let current_cutoff = current
                    .as_ref()
                    .filter(|v| v.value().len() <= 1024)
                    .and_then(|v| open_value(&REPAIR_FENCE.to_be_bytes(), v.value()).ok())
                    .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                    .map(u64::from_be_bytes);
                if authenticated && current_cutoff != Some(cutoff) {
                    *cursor = None;
                    return Ok(RepairAuditStatus::Progressing);
                }
                drop(current);
                if !authenticated {
                    let encoded = seal_value(&REPAIR_FENCE.to_be_bytes(), &cutoff.to_be_bytes());
                    cleanup
                        .insert(REPAIR_FENCE, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
                if let Some(id) = queue {
                    cleanup
                        .insert(id, [].as_slice())
                        .map_err(Self::storage_error)?;
                }
                if finished && !unknown {
                    cleanup.remove(REPAIR_FENCE).map_err(Self::storage_error)?;
                }
                drop(cleanup);
                checkpoint()?;
                bind_pages_root(&write)?;
                Self::write_read_bounds_roots_in_write(&write)?;
                checkpoint()?;
                write.commit().map_err(Self::storage_error)?;
                if finished && !unknown {
                    *cursor = None;
                }
            }
            Ok(if unknown {
                RepairAuditStatus::Blocked
            } else {
                RepairAuditStatus::Progressing
            })
        });
        // The volatile cursor must never commit speculative advancement after
        // admission, IO, or commit failure. Previously queued durable rows
        // remain valid and are rediscovered by the restarted proof.
        if result.is_err() {
            *cursor = None;
        }
        result
    }
}

impl RedbPersistence {
    pub(crate) fn repair_orphan_audit_blocked(&self) -> bool {
        self.vector_repair_audit
            .lock()
            .as_ref()
            .is_some_and(|cursor| cursor.unknown)
    }
}

impl RedbPersistence {
    pub(crate) fn abandon_vector_repair_audit_cursor(&self) {
        *self.vector_repair_audit.lock() = None;
    }
}

impl RedbPersistence {
    fn authenticated_journal_ownership_reference(
        encoded: &[u8],
        physical: &[u8],
    ) -> Result<Option<u64>> {
        // Journal metadata has no graph/body Vec. Width and both route
        // allocation shapes are checked before the admitted owned decode.
        // The cursor's 256 KiB owner covers these at-most-64 KiB temporaries.
        if encoded.len() > 64 * 1024 || physical.len() > 64 * 1024 {
            return Err(corruption(
                "journal ownership reference exceeds bounded metadata",
            ));
        }
        let payload = vector_partition_record_payload(VectorPartitionRecordKind::Journal, encoded)
            .map_err(corruption)?;
        prevalidate_vector_partition_metadata_routes(VectorPartitionRecordKind::Journal, payload)
            .map_err(corruption)?;
        let decode_bound = Self::vector_partition_record_decode_bound(
            payload.len(),
            std::mem::size_of::<VectorPartitionJournalRecord>(),
        );
        if decode_bound > 64 * 1024 {
            return Err(corruption(
                "journal ownership route exceeds bounded metadata",
            ));
        }
        let record: VectorPartitionJournalRecord = Self::decode_exact(payload)
            .map_err(|_| corruption("journal ownership reference cannot be decoded"))?;
        if Self::vector_partition_journal_key(&record).as_slice() != physical {
            return Err(corruption(
                "journal ownership reference disagrees with its physical key",
            ));
        }
        let membership = match &record.change {
            VectorPartitionJournalChange::Upsert(membership) => membership,
            VectorPartitionJournalChange::Tombstone(tombstone) => &tombstone.membership,
        };
        if membership.index != record.index || membership.partition_key != record.partition_key {
            return Err(corruption(
                "journal ownership reference has conflicting routes",
            ));
        }
        Ok(None)
    }
}

// Every native page mutation binds its physical root in the protected
// Configuration root, alongside the same transaction's generation metadata.
pub(super) fn bind_pages_root(write: &redb::WriteTransaction) -> Result<()> {
    content_identity::bind_vector_pages_root_in_write(write)
}
/// The schema half of the paged binding, checked once when a read opens. A
/// store that never bound a paged generation has no schema half to check.
pub(crate) fn validate_schema_meta_root(read: &redb::ReadTransaction) -> Result<()> {
    if content_identity::vector_schema_meta_root_bound_in_read(read)?
        && !content_identity::vector_schema_meta_root_matches_in_read(read)?
    {
        return Err(corruption(
            "schema metadata physical root differs from its published binding",
        ));
    }
    Ok(())
}

pub(crate) fn validate_pages_root(read: &redb::ReadTransaction) -> Result<()> {
    let actual = content_identity::vector_schema_pages_root_in_read(read)?;
    let expected = content_identity::vector_pages_root_in_read(read)
        .map_err(|error| corruption(format!("paged graph root binding is invalid: {error}")))?;
    // Only the page table may disagree without refusing: every served route
    // verifies its own committed page set before answering, so damage there
    // refuses that route alone. Schema metadata still refuses outright.
    if actual != expected && !content_identity::vector_schema_meta_root_matches_in_read(read)? {
        return Err(corruption(
            "paged graph or schema physical root differs from its published binding",
        ));
    }
    Ok(())
}

impl RedbPersistence {
    pub(super) fn saved_export_recovery_cutoff(&self) -> Result<u64> {
        if let Some(cutoff) = self.saved_export_recovery_cutoff.get() {
            return Ok(*cutoff);
        }
        let cutoff = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            match read.open_table(COUNTERS) {
                Ok(counters) => Ok(counters
                    .get(NEXT_STORAGE)
                    .map_err(Self::storage_error)?
                    .map_or(1, |value| value.value())),
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(1),
                Err(error) => Err(Self::storage_error(error)),
            }
        })?;
        let _ = self.saved_export_recovery_cutoff.set(cutoff);
        Ok(*self.saved_export_recovery_cutoff.get().unwrap())
    }
}

#[cfg(test)]
mod native_namespace_tests;
