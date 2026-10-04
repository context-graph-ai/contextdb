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
use super::vector_membership_pages;
use super::*;
use contextdb_vector::hnsw::paged_builder::{
    PagedEdge, PagedGenerationPersistence, PagedGenerationTransaction, PagedGraphState, PagedPoint,
    PagedVectorBatchPlan, PagedVectorBuildEntry, PagedVectorGraphSource, PagedVectorIdentity,
    PagedVectorPhase, PagedVectorPoint, PagedVectorProgress, PagedVectorReader,
    PagedVectorReclamation, PagedVectorTransaction, PagedVectorValues, PointId, decode_paged_point,
    encode_paged_point_into,
};
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
const NEXT_STORAGE: &str = "next_storage_id";

const KIND_POINT: u8 = 1;
const KIND_IDENTITY: u8 = 2;
const KIND_ROW: u8 = 3;
const KIND_EXACT: u8 = 4;
const SCRATCH_VISITED: u8 = 1;
const SCRATCH_CANDIDATE: u8 = 2;
const CHECK_BYTES: usize = 16;
const META_MAGIC: [u8; 8] = *b"CDBVPG01";
/// Distinct from the whole-envelope graph format. A base generation record
/// carrying this version names native paged storage through its meta bytes.
pub(crate) const VECTOR_GRAPH_PAGED_FORMAT_VERSION: u16 = 0x0100;
/// Canonical source key width (`canonical_source_key`).
const CANONICAL_BYTES: usize = 32;
/// Physical source companion suffix width (`vector_membership_pages::source_key`).
const SOURCE_SUFFIX_BYTES: usize = 24;

fn corruption(reason: impl Into<String>) -> Error {
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
struct ProgressRecord {
    generation_id: u64,
    declaration_digest: u64,
    frontier_tx: u64,
    frontier_lsn: u64,
    graph: PagedGraphState,
    last_source_key: Option<Vec<u8>>,
    total_points: u64,
    phase: u8,
    encoding_cursor: Option<(u8, i32)>,
    encoding_bytes: u64,
    encoded_points: u64,
    encoding_digest: u64,
    cleanup_cursor: Option<Vec<u8>>,
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
    fn from_progress(progress: &PagedVectorProgress) -> Self {
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
    progress: ProgressRecord,
}

impl PagedBuildRecord {
    pub(crate) fn progress(&self) -> Result<PagedVectorProgress> {
        self.progress.to_progress()
    }
}

/// Graph bytes of a base generation record in the paged format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PagedGenerationMeta {
    magic: [u8; 8],
    pub(crate) storage_id: u64,
    pub(crate) declaration: PagedDeclaration,
    progress: ProgressRecord,
}

impl PagedGenerationMeta {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let meta: Self = RedbPersistence::decode_exact(bytes)
            .map_err(|_| corruption("paged generation metadata cannot be decoded"))?;
        if meta.magic != META_MAGIC {
            return Err(corruption("paged generation metadata has the wrong magic"));
        }
        Ok(meta)
    }

    pub(crate) fn progress(&self) -> Result<PagedVectorProgress> {
        self.progress.to_progress()
    }
}

fn storage_prefix(storage_id: u64) -> [u8; 8] {
    storage_id.to_be_bytes()
}

fn point_key(storage: &[u8; 8], id: PointId) -> [u8; 14] {
    let mut key = [0u8; 14];
    key[..8].copy_from_slice(storage);
    key[8] = KIND_POINT;
    key[9] = id.0;
    key[10..14].copy_from_slice(&((id.1 as u32) ^ 0x8000_0000).to_be_bytes());
    key
}

fn identity_key(storage: &[u8; 8], data_id: usize) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[..8].copy_from_slice(storage);
    key[8] = KIND_IDENTITY;
    key[9..17].copy_from_slice(&(data_id as u64).to_be_bytes());
    key
}

fn row_key(storage: &[u8; 8], row: RowId, data_id: usize) -> [u8; 25] {
    let mut key = [0u8; 25];
    key[..8].copy_from_slice(storage);
    key[8] = KIND_ROW;
    key[9..17].copy_from_slice(&row.0.to_be_bytes());
    key[17..25].copy_from_slice(&(data_id as u64).to_be_bytes());
    key
}

fn kind_prefix(storage: &[u8; 8], kind: u8) -> [u8; 9] {
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

fn exact_key(storage: &[u8; 8], exact: &[u8], row: RowId, data_id: usize) -> Vec<u8> {
    let mut key = exact_prefix(storage, exact);
    key.extend_from_slice(&row.0.to_be_bytes());
    key.extend_from_slice(&(data_id as u64).to_be_bytes());
    key
}

/// Every page value carries a keyed check: a page moved under another key, a
/// truncated body or a flipped byte is a typed corruption, never a graph.
fn seal_value(key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(payload);
    let mut value = Vec::with_capacity(payload.len() + CHECK_BYTES);
    value.extend_from_slice(payload);
    value.extend_from_slice(&hash.finalize().as_bytes()[..CHECK_BYTES]);
    value
}

fn open_value<'v>(key: &[u8], value: &'v [u8]) -> Result<&'v [u8]> {
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

/// Shared page lookups over either the build's write handle or a captured
/// root. Each call returns at most one owned record window.
trait PageSource {
    fn page(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn first_after(&self, lower: Bound<&[u8]>, upper: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>>;
    fn last_before(&self, lower: &[u8], upper: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>>;
}

impl<T: ReadableTable<&'static [u8], &'static [u8]>> PageSource for T {
    fn page(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .get(key)
            .map_err(RedbPersistence::storage_error)?
            .map(|value| value.value().to_vec()))
    }
    fn first_after(&self, lower: Bound<&[u8]>, upper: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut range = self
            .range::<&[u8]>((lower, Bound::Excluded(upper)))
            .map_err(RedbPersistence::storage_error)?;
        Ok(match range.next() {
            Some(entry) => {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                Some((key.value().to_vec(), value.value().to_vec()))
            }
            None => None,
        })
    }
    fn last_before(&self, lower: &[u8], upper: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut range = self
            .range::<&[u8]>((Bound::Included(lower), Bound::Excluded(upper)))
            .map_err(RedbPersistence::storage_error)?;
        Ok(match range.next_back() {
            Some(entry) => {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                Some((key.value().to_vec(), value.value().to_vec()))
            }
            None => None,
        })
    }
}

/// Point, identity and directory reads common to construction and serving.
struct PagedLookup<'s, S: PageSource> {
    pages: &'s S,
    storage: [u8; 8],
    dimension: usize,
    quantization: VectorQuantization,
    node_limit: usize,
}

impl<S: PageSource> PagedLookup<'_, S> {
    fn load_point(&self, id: PointId) -> Result<PagedVectorPoint> {
        let key = point_key(&self.storage, id);
        let value = self
            .pages
            .page(&key)?
            .ok_or_else(|| corruption("paged vector graph names a missing point"))?;
        let payload = open_value(&key, &value)?;
        let point = match self.quantization {
            VectorQuantization::F32 => {
                let point: PagedPoint<f32> = decode_paged_point(payload, self.node_limit)
                    .map_err(|error| corruption(format!("paged point decode failed: {error}")))?;
                if point.values.len() != self.dimension {
                    return Err(corruption(
                        "paged point dimension disagrees with its column",
                    ));
                }
                PagedVectorPoint::F32(point)
            }
            VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
                let point: PagedPoint<u8> = decode_paged_point(payload, self.node_limit)
                    .map_err(|error| corruption(format!("paged point decode failed: {error}")))?;
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
    }

    fn identity(&self, data_id: usize) -> Result<PagedVectorIdentity> {
        let key = identity_key(&self.storage, data_id);
        let value = self
            .pages
            .page(&key)?
            .ok_or_else(|| corruption("paged vector graph names a missing identity"))?;
        let identity = decode_identity(open_value(&key, &value)?)?;
        if identity.data_id != data_id {
            return Err(corruption("paged identity disagrees with its key"));
        }
        Ok(identity)
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
    fn row_at_or_after(&self, row: RowId) -> Result<Option<(RowId, usize, PointId)>> {
        let lower = row_key(&self.storage, row, 0);
        let upper = self.rows_end();
        self.pages
            .first_after(Bound::Included(&lower), &upper)?
            .map(|(key, value)| self.row_entry(&key, &value))
            .transpose()
    }

    /// The first row directory entry strictly after `(row, data_id)`.
    fn row_after(&self, row: RowId, data_id: usize) -> Result<Option<(RowId, usize, PointId)>> {
        let lower = row_key(&self.storage, row, data_id);
        let upper = self.rows_end();
        self.pages
            .first_after(Bound::Excluded(&lower), &upper)?
            .map(|(key, value)| self.row_entry(&key, &value))
            .transpose()
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

    fn exact_after(&self, exact: &[u8], after: Option<RowId>) -> Result<Option<(RowId, usize)>> {
        let prefix = exact_prefix(&self.storage, exact);
        let upper = writer_source::prefix_end(&prefix)
            .ok_or_else(|| corruption("paged exact directory prefix has no successor"))?;
        let lower = match after {
            Some(row) => {
                let mut key = prefix.clone();
                key.extend_from_slice(&row.0.to_be_bytes());
                key.extend_from_slice(&u64::MAX.to_be_bytes());
                key
            }
            None => prefix.clone(),
        };
        let bound = if after.is_some() {
            Bound::Excluded(lower.as_slice())
        } else {
            Bound::Included(lower.as_slice())
        };
        let Some((key, value)) = self.pages.first_after(bound, &upper)? else {
            return Ok(None);
        };
        open_value(&key, &value)?;
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
    ) -> Result<Vec<RowId>> {
        let mut rows = Vec::new();
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
    ) -> Result<Vec<RowId>> {
        let mut rows: Vec<RowId> = Vec::new();
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
        let Some((first_key, first_value)) =
            self.pages.first_after(Bound::Included(&lower), &upper)?
        else {
            return Ok(None);
        };
        let Some((last_key, last_value)) = self.pages.last_before(&lower, &upper)? else {
            return Err(corruption("paged row directory lost its last entry"));
        };
        let (first, _, _) = self.row_entry(&first_key, &first_value)?;
        let (last, _, _) = self.row_entry(&last_key, &last_value)?;
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
        for data_id in 0..points {
            checkpoint()?;
            if identity_visible(&self.identity(data_id)?, snapshot) {
                visible += 1;
            }
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
        if record.storage_id != self.storage_id || record.declaration != self.declaration {
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
                    transaction.finish()?;
                    (next, published)
                };
                if published != (next.phase == PagedVectorPhase::Published) {
                    return Err(Error::Other(
                        "paged publication and its successor record disagree".into(),
                    ));
                }
                {
                    let mut builds = write
                        .open_table(BUILDS)
                        .map_err(RedbPersistence::storage_error)?;
                    if published {
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
                RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                (self.hooks.checkpoint)()?;
                #[cfg(feature = "test-seams")]
                if published {
                    crate::writer_commit_test_seams::reached(
                        crate::writer_commit_test_seams::CommitBoundary::GenerationBeforeRedb,
                    );
                }
                write.commit().map_err(RedbPersistence::storage_error)?;
                committed = Some(next);
                Ok(())
            })
        })?;
        committed.ok_or_else(|| Error::Other("paged vector batch did not commit".into()))
    }
}

/// One build batch's view of its write transaction.
struct NativeBatch<'t> {
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
    inserted: usize,
}

impl<'t> NativeBatch<'t> {
    #[allow(clippy::too_many_arguments)]
    fn open(
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
        let pages = write
            .open_table(PAGES)
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
            inserted: 0,
        })
    }

    fn lookup(&self) -> PagedLookup<'_, redb::Table<'t, &'static [u8], &'static [u8]>> {
        PagedLookup {
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

    fn finish(mut self) -> Result<()> {
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
        #[cfg(feature = "test-seams")]
        crate::database::resident_history_test_seams::note(|work| {
            work.maintenance_source_visits += 1
        });
    }

    /// Load the original body named by one owning membership interval.
    fn source_values(&self, record: &VectorPartitionMembershipRecord) -> Result<PagedVectorValues> {
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
        let persisted: PersistedVectorEntry = RedbPersistence::decode(stored.value())
            .map_err(|_| corruption("a paged build source body cannot be decoded"))?;
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
        self.lookup().load_point(id)
    }
    fn identity(&mut self, data_id: usize) -> Result<PagedVectorIdentity> {
        self.lookup().identity(data_id)
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
            .exact_rows(key, snapshot, after, limit, &mut || checkpoint())
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
            .row_window(snapshot, after, limit, &mut || checkpoint())
    }
    fn row_bounds(&mut self) -> Result<Option<(RowId, RowId, usize)>> {
        let points = self.previous.graph.points + self.inserted;
        self.lookup().row_bounds(points)
    }
    fn point_at_or_after(&mut self, row: RowId) -> Result<Option<(RowId, PointId)>> {
        Ok(self
            .lookup()
            .row_at_or_after(row)?
            .map(|(row, _, point)| (row, point)))
    }
    fn exact_graph_row_after(&mut self, key: &[u8], after: Option<RowId>) -> Result<Option<RowId>> {
        Ok(self.lookup().exact_after(key, after)?.map(|(row, _)| row))
    }
}

impl PagedVectorTransaction for NativeBatch<'_> {
    /// Search scratch stays in the batch's fixed-capacity workspace. Spilling
    /// it into storage pages made every visit and queue pop rewrite a leaf
    /// page, multiplying the batch's dirty pages (measured, decisions.md).
    fn spills_search_scratch(&self) -> bool {
        false
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
        self.pages
            .insert(key.as_slice(), seal_value(&key, &bytes).as_slice())
            .map_err(RedbPersistence::storage_error)?;
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
        self.pages
            .insert(
                key.as_slice(),
                seal_value(&key, &encode_identity(identity)).as_slice(),
            )
            .map_err(RedbPersistence::storage_error)?;
        let row = row_key(&self.storage, identity.row_id, identity.data_id);
        self.pages
            .insert(
                row.as_slice(),
                seal_value(&row, &encode_point_id(identity.point)).as_slice(),
            )
            .map_err(RedbPersistence::storage_error)?;
        let exact = exact_key(
            &self.storage,
            &identity.exact_key,
            identity.row_id,
            identity.data_id,
        );
        self.pages
            .insert(exact.as_slice(), seal_value(&exact, &[]).as_slice())
            .map_err(RedbPersistence::storage_error)?;
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
            #[cfg(feature = "test-seams")]
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

    fn publish(&mut self, progress: &PagedVectorProgress, through_lsn: Lsn) -> Result<()> {
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
        self.persistence
            .publish_preencoded_vector_partition_generation_catalog_in_write(
                self.write,
                &publication.prepared,
            )?;
        self.published = true;
        Ok(())
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
    pub(crate) storage_id: u64,
    pub(crate) declaration: PagedDeclaration,
    pub(crate) frontier: TxId,
    pub(crate) points: usize,
    pub(crate) node_limit: usize,
    pub(crate) checkpoint: Arc<dyn Fn() -> Result<()> + Send + Sync>,
}

impl PagedVectorGraphSource for NativePagedGraphSource {
    fn reader(&self) -> Result<Box<dyn PagedVectorReader>> {
        let (read, root) = (self.capture)()?;
        let pages = match read.open_table(PAGES) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(corruption("published paged generation has no page table"));
            }
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        };
        #[cfg(feature = "test-seams")]
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
            node_limit: self.node_limit,
            checkpoint: Arc::clone(&self.checkpoint),
        }))
    }

    fn retained_bytes(&self) -> usize {
        0
    }
}

struct NativePagedReader {
    _root: Arc<dyn std::any::Any + Send + Sync>,
    pages: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    storage: [u8; 8],
    dimension: usize,
    quantization: VectorQuantization,
    frontier: TxId,
    points: usize,
    node_limit: usize,
    checkpoint: Arc<dyn Fn() -> Result<()> + Send + Sync>,
}

impl NativePagedReader {
    fn lookup(&self) -> PagedLookup<'_, redb::ReadOnlyTable<&'static [u8], &'static [u8]>> {
        PagedLookup {
            pages: &self.pages,
            storage: self.storage,
            dimension: self.dimension,
            quantization: self.quantization,
            node_limit: self.node_limit,
        }
    }
}

impl PagedVectorReader for NativePagedReader {
    fn load_point(&mut self, id: PointId) -> Result<PagedVectorPoint> {
        self.lookup().load_point(id)
    }
    fn identity(&mut self, data_id: usize) -> Result<PagedVectorIdentity> {
        if data_id >= self.points {
            return Err(corruption("paged graph names an identity beyond its count"));
        }
        self.lookup().identity(data_id)
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
            .exact_rows(key, snapshot, after, limit, &mut || checkpoint())
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
            .row_window(snapshot, after, limit, &mut || checkpoint())
    }
    fn row_bounds(&mut self) -> Result<Option<(RowId, RowId, usize)>> {
        self.lookup().row_bounds(self.points)
    }
    fn point_at_or_after(&mut self, row: RowId) -> Result<Option<(RowId, PointId)>> {
        Ok(self
            .lookup()
            .row_at_or_after(row)?
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

    /// Every durable build record, for restart registration and floor restore.
    /// The inventory is one small control record per unpublished partition.
    pub(crate) fn load_paged_build_records(
        &self,
    ) -> Result<Vec<(VectorPartitionRef, PagedBuildRecord)>> {
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
                        .map_err(|_| corruption("paged build record cannot be decoded"))
                })
                .transpose()
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
    ) -> Result<PagedBuildRecord> {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        let progress = PagedVectorProgress::new(
            generation_id,
            declaration.digest()?,
            frontier_tx,
            frontier_lsn,
            total_points,
        );
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            let storage_id = {
                let mut counters = write.open_table(COUNTERS).map_err(Self::storage_error)?;
                let next = counters
                    .get(NEXT_STORAGE)
                    .map_err(Self::storage_error)?
                    .map(|value| value.value())
                    .unwrap_or(1);
                counters
                    .insert(
                        NEXT_STORAGE,
                        next.checked_add(1).ok_or_else(|| {
                            Error::Other("paged vector storage identities are exhausted".into())
                        })?,
                    )
                    .map_err(Self::storage_error)?;
                next
            };
            let record = PagedBuildRecord {
                storage_id,
                previous_generation_id,
                reason,
                declaration,
                progress: ProgressRecord::from_progress(&progress),
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
                drop(value);
                write
                    .open_table(CLEANUP)
                    .map_err(Self::storage_error)?
                    .insert(record.storage_id, [].as_slice())
                    .map_err(Self::storage_error)?;
            }
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
        let meta = PagedGenerationMeta::decode(meta_bytes)?;
        write
            .open_table(CLEANUP)
            .map_err(Self::storage_error)?
            .insert(meta.storage_id, [].as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    /// One finite cleanup transaction: remove at most `max_keys` pages of the
    /// first queued storage identity. Returns the number of pages removed.
    pub(crate) fn paged_cleanup_step(&self, max_keys: usize) -> Result<usize> {
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            let removed = {
                let mut cleanup = match write.open_table(CLEANUP) {
                    Ok(table) => table,
                    Err(error) => return Err(Self::storage_error(error)),
                };
                let Some((storage_id, cursor)) = cleanup
                    .first()
                    .map_err(Self::storage_error)?
                    .map(|(key, value)| (key.value(), value.value().to_vec()))
                else {
                    return Ok(0);
                };
                let mut pages = write.open_table(PAGES).map_err(Self::storage_error)?;
                let storage = storage_prefix(storage_id);
                let result = reclaim_storage_keys(
                    &mut pages,
                    &storage,
                    (!cursor.is_empty()).then_some(cursor.as_slice()),
                    max_keys,
                )?;
                if result.complete {
                    cleanup.remove(storage_id).map_err(Self::storage_error)?;
                } else if let Some(last) = &result.last_key {
                    cleanup
                        .insert(storage_id, last.as_slice())
                        .map_err(Self::storage_error)?;
                }
                result.keys_removed
            };
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    pub(crate) fn has_paged_cleanup(&self) -> Result<bool> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            match read.open_table(CLEANUP) {
                Ok(table) => Ok(!table.is_empty().map_err(Self::storage_error)?),
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
    ) -> Result<()> {
        let mut storage_ids = Vec::new();
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
                    let record: VectorPartitionBaseGenerationRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::BaseGeneration,
                            value.value(),
                        )?;
                    if record.graph_format_version == VECTOR_GRAPH_PAGED_FORMAT_VERSION {
                        storage_ids
                            .push(PagedGenerationMeta::decode(&record.graph.bytes)?.storage_id);
                    }
                }
            }
        }
        {
            let mut builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
            let removed = builds.remove(prefix).map_err(Self::storage_error)?;
            if let Some(value) = removed {
                let record: PagedBuildRecord =
                    Self::decode_exact(open_value(prefix, value.value())?)
                        .map_err(|_| corruption("paged build record cannot be decoded"))?;
                storage_ids.push(record.storage_id);
            }
        }
        write
            .open_table(FLOORS)
            .map_err(Self::storage_error)?
            .remove(prefix)
            .map_err(Self::storage_error)?;
        let mut pages = write.open_table(PAGES).map_err(Self::storage_error)?;
        let mut cleanup = write.open_table(CLEANUP).map_err(Self::storage_error)?;
        for storage_id in storage_ids {
            let storage = storage_prefix(storage_id);
            loop {
                let step = reclaim_storage_keys(&mut pages, &storage, None, 4096)?;
                if step.complete {
                    break;
                }
            }
            cleanup.remove(storage_id).map_err(Self::storage_error)?;
        }
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

    pub(crate) fn encode_paged_generation_meta(
        storage_id: u64,
        declaration: &PagedDeclaration,
        progress: &PagedVectorProgress,
    ) -> Result<Vec<u8>> {
        Self::encode(&PagedGenerationMeta {
            magic: META_MAGIC,
            storage_id,
            declaration: declaration.clone(),
            progress: ProgressRecord::from_progress(progress),
        })
    }
}
