//! Transactional paged vector graphs. The engine supplies authenticated point
//! pages and durable lookup directories; this module never retains an all-node
//! graph or row/exact-vector map.
use super::{DistQuantizedCosine, HNSW_BUILD_SEED, exact_key_for_stored_vector, hnsw_f32_distance};
use crate::memory_budget::{MemoryBudget, VectorWorkspaceReservation, VectorCopyAdmission, ScopedVectorCopy};
use crate::quantized::StoredVector;
use crate::store::{ResolvedVectorPolicy, VectorPartitionRef};
use contextdb_core::{
    Error, Lsn, Result, RowId, SnapshotId, TxId, VectorEntry, VectorQuantization,
};
#[doc(hidden)]
pub use hnsw_rs::hnsw::PointId;
#[doc(hidden)]
pub use hnsw_rs::hnswio::{decode_paged_point, encode_paged_point_into};
#[doc(hidden)]
pub use hnsw_rs::paged::{PagedEdge, PagedGraphState, PagedPoint};
use hnsw_rs::paged::{
    PagedGraphError, PagedGraphPolicy, PagedGraphWorkspace, PagedPointStorage, insert_paged,
    search_paged_allowed,
};
use std::sync::{Arc, atomic::AtomicUsize};

/// One point page's decoded value. Adjacency uses the same point/edge fields
/// and ordering as the sealed owned-graph format. Engines authenticate the
/// page's generation and point identity before handing it to the algorithm.
#[derive(Clone, Debug)]
pub enum PagedVectorPoint {
    F32(PagedPoint<f32>),
    Quantized(PagedPoint<u8>),
}

/// Durable point/row/exact lookup record, committed with its graph links.
#[derive(Clone, Debug)]
pub struct PagedVectorIdentity {
    pub point: PointId,
    pub data_id: usize,
    pub row_id: RowId,
    pub lsn: Lsn,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub exact_key: Vec<u8>,
}

/// Small restart record. The engine persists and reinstalls the frontier floor
/// before any cleanup, verifies the declaration digest, and owns the captured
/// durable root. It must compare the complete record at transaction admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PagedVectorProgress {
    pub generation_id: u64,
    pub declaration_digest: u64,
    pub frontier_tx: TxId,
    pub frontier_lsn: Lsn,
    pub graph: PagedGraphState,
    pub last_source_key: Option<Vec<u8>>,
    pub total_points: usize,
    pub phase: PagedVectorPhase,
    pub encoding_cursor: Option<PointId>,
    pub encoding_bytes: u64,
    pub encoded_points: usize,
    pub encoding_digest: u64,
    pub cleanup_cursor: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PagedVectorPhase {
    Inserting,
    Encoding,
    Complete,
    Published,
    Abandoned,
    Reclaiming,
    Reclaimed,
}

impl PagedVectorProgress {
    pub fn new(
        generation_id: u64,
        declaration_digest: u64,
        frontier_tx: TxId,
        frontier_lsn: Lsn,
        total_points: usize,
    ) -> Self {
        Self {
            generation_id,
            declaration_digest,
            frontier_tx,
            frontier_lsn,
            graph: PagedGraphState::new(HNSW_BUILD_SEED),
            last_source_key: None,
            total_points,
            phase: PagedVectorPhase::Inserting,
            encoding_cursor: None,
            encoding_bytes: 0,
            encoded_points: 0,
            encoding_digest: 0,
            cleanup_cursor: None,
        }
    }
}

/// Prices one finite transaction, including graph scratch and worst-case live
/// point decodes, encoded dirty pages and progress/source keys. The engine
/// admits disk space separately and supplies bounds matching its page codec.
/// Neither graph count nor partition population multiplies this workspace.
#[derive(Clone, Copy, Debug)]
pub struct PagedVectorBatchPlan {
    pub max_points: usize,
    pub scratch_points: usize,
    pub max_node_bytes: usize,
    pub max_node_writes: usize,
    pub dirty_page_bytes: usize,
    pub source_key_bytes: usize,
    pub progress_bytes: usize,
}
impl PagedVectorBatchPlan {
    pub fn workspace_bytes(&self) -> Result<usize> {
        let scratch = PagedGraphWorkspace::allocation_bound(self.scratch_points)
            .ok_or_else(workspace_overflow)?;
        scratch
            .checked_add(
                self.max_node_bytes
                    .checked_mul(6)
                    .ok_or_else(workspace_overflow)?,
            )
            .and_then(|n| n.checked_add(self.dirty_page_bytes))
            .and_then(|n| n.checked_add(self.source_key_bytes.checked_mul(3)?))
            .and_then(|n| n.checked_add(self.progress_bytes))
            .ok_or_else(workspace_overflow)
    }
    /// Construction spills its search visited set and priority queue into
    /// the engine transaction (`PagedVectorTransaction`), so its in-memory
    /// graph scratch is only the result/selection/pruning/extension window.
    pub fn construction_workspace_bytes(&self) -> Result<usize> {
        // Covers both a spilling provider and one whose search scratch stays
        // in this fixed-capacity workspace.
        let scratch = PagedGraphWorkspace::exact_allocation_bound(self.scratch_points)
            .ok_or_else(workspace_overflow)?;
        scratch
            .checked_add(
                self.max_node_bytes
                    .checked_mul(6)
                    .ok_or_else(workspace_overflow)?,
            )
            .and_then(|n| n.checked_add(self.dirty_page_bytes))
            .and_then(|n| n.checked_add(self.source_key_bytes.checked_mul(3)?))
            .and_then(|n| n.checked_add(self.progress_bytes))
            .ok_or_else(workspace_overflow)
    }
    fn validate(
        &self,
        policy: PagedGraphPolicy,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<()> {
        let value_bytes = match quantization {
            VectorQuantization::F32 => dimension.checked_mul(4),
            VectorQuantization::SQ8 => dimension.checked_add(32),
            VectorQuantization::SQ4 => dimension.div_ceil(2).checked_add(32),
        }
        .ok_or_else(workspace_overflow)?;
        // Layer zero keeps 2M neighbours and every upper layer M, exactly the
        // degrees the paged algorithm enforces when it loads a point.
        let minimum_node = policy
            .max_layer
            .checked_sub(1)
            .and_then(|upper| upper.checked_mul(policy.m))
            .and_then(|n| n.checked_add(2 * policy.m))
            .and_then(|n| n.checked_mul(std::mem::size_of::<hnsw_rs::paged::PagedEdge>()))
            .and_then(|n| {
                n.checked_add(
                    policy.max_layer * std::mem::size_of::<Vec<hnsw_rs::paged::PagedEdge>>(),
                )
            })
            .and_then(|n| n.checked_add(value_bytes))
            .and_then(|n| n.checked_add(std::mem::size_of::<PagedVectorPoint>()))
            .ok_or_else(workspace_overflow)?;
        if self.max_points == 0
            || self.max_node_writes == 0
            || !policy.validate()
            || self.scratch_points
                <= policy
                    .ef_construction
                    .max((2 * policy.m + 1) * (2 * policy.m + 1))
            || self.max_node_writes
                < self
                    .max_points
                    .saturating_mul(1 + 2 * policy.m * policy.max_layer)
            || self.max_node_bytes < minimum_node
            || self.progress_bytes < std::mem::size_of::<PagedVectorProgress>()
        {
            return Err(Error::Other(
                "invalid paged vector batch workspace plan".into(),
            ));
        }
        self.workspace_bytes()?;
        Ok(())
    }
}
fn workspace_overflow() -> Error {
    Error::Other("paged vector workspace size overflow".into())
}

/// Native allocation request at one captured root. The admitted capability
/// covers point, identity and directory copies, without changing traversal.
#[doc(hidden)]
pub enum PagedVectorReadRequest<'q> {
    Point { id: PointId },
    Identity { data_id: usize },
    PointForRow { row: RowId, snapshot: SnapshotId },
    ExactRows { key: &'q [u8], snapshot: SnapshotId, after: Option<RowId>, limit: usize },
    VisibleCount { snapshot: SnapshotId },
    RowWindow { snapshot: SnapshotId, after: Option<RowId>, limit: usize },
    RowBounds,
    PointAtOrAfter { row: RowId },
    GraphPointForRow { row: RowId },
    ExactGraphRowAfter { key: &'q [u8], after: Option<RowId> },
}
#[doc(hidden)]
pub enum PagedVectorReadValue {
    Point(ScopedVectorCopy<PagedVectorPoint>),
    Identity(ScopedVectorCopy<PagedVectorIdentity>),
    PointForRow(ScopedVectorCopy<Option<PointId>>),
    ExactRows(ScopedVectorCopy<Vec<RowId>>),
    VisibleCount(ScopedVectorCopy<usize>),
    RowWindow(ScopedVectorCopy<Vec<RowId>>),
    RowBounds(ScopedVectorCopy<Option<(RowId, RowId, usize)>>),
    PointAtOrAfter(ScopedVectorCopy<Option<(RowId, PointId)>>),
    GraphPointForRow(ScopedVectorCopy<Option<PointId>>),
    ExactGraphRowAfter(ScopedVectorCopy<Option<RowId>>),
}

/// A captured-root serving reader. Every load validates checksum, generation,
/// point identity, dimensions and all referenced identities. A reader may keep
/// only admitted, evictable serving pages, never a directory of every point.
pub trait PagedVectorReader: Send {
    /// Explicit native admission seam. Native providers authenticate and
    /// validate borrowed lengths, then invoke `admit` before any owned copy.
    /// Each returned value retains its per-value token through value drop.
    /// The default introduces no admission: it is only valid when every
    /// returned allocation is already funded by the provider's workspace.
    fn read_with_copy_admission(
        &mut self,
        request: PagedVectorReadRequest<'_>,
        _admit: &mut dyn FnMut(usize) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<PagedVectorReadValue> {
        Ok(match request {
            PagedVectorReadRequest::Point { id } => PagedVectorReadValue::Point(ScopedVectorCopy::from_admitted(self.load_point(id)?, None)),
            PagedVectorReadRequest::Identity { data_id } => PagedVectorReadValue::Identity(ScopedVectorCopy::from_admitted(self.identity(data_id)?, None)),
            PagedVectorReadRequest::PointForRow { row, snapshot } => PagedVectorReadValue::PointForRow(ScopedVectorCopy::from_admitted(self.point_for_row(row, snapshot)?, None)),
            PagedVectorReadRequest::ExactRows { key, snapshot, after, limit } => PagedVectorReadValue::ExactRows(ScopedVectorCopy::from_admitted(self.exact_rows(key, snapshot, after, limit)?, None)),
            PagedVectorReadRequest::VisibleCount { snapshot } => PagedVectorReadValue::VisibleCount(ScopedVectorCopy::from_admitted(self.visible_count(snapshot)?, None)),
            PagedVectorReadRequest::RowWindow { snapshot, after, limit } => PagedVectorReadValue::RowWindow(ScopedVectorCopy::from_admitted(self.row_window(snapshot, after, limit)?, None)),
            PagedVectorReadRequest::RowBounds => PagedVectorReadValue::RowBounds(ScopedVectorCopy::from_admitted(self.row_bounds()?, None)),
            PagedVectorReadRequest::PointAtOrAfter { row } => PagedVectorReadValue::PointAtOrAfter(ScopedVectorCopy::from_admitted(self.point_at_or_after(row)?, None)),
            PagedVectorReadRequest::GraphPointForRow { row } => PagedVectorReadValue::GraphPointForRow(ScopedVectorCopy::from_admitted(self.graph_point_for_row(row)?, None)),
            PagedVectorReadRequest::ExactGraphRowAfter { key, after } => PagedVectorReadValue::ExactGraphRowAfter(ScopedVectorCopy::from_admitted(self.exact_graph_row_after(key, after)?, None)),
        })
    }

    fn load_point(&mut self, id: PointId) -> Result<PagedVectorPoint>;
    fn identity(&mut self, data_id: usize) -> Result<PagedVectorIdentity>;
    /// Ordered point probe into the durable row directory, at this root.
    fn point_for_row(&mut self, row: RowId, snapshot: SnapshotId) -> Result<Option<PointId>>;
    /// Return a finite ordered window, exclusive of `after`. Authenticate each
    /// exact-key membership and its visibility; absence comes from this root.
    fn exact_rows(
        &mut self,
        key: &[u8],
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
    ) -> Result<Vec<RowId>>;
    fn checkpoint(&mut self) -> Result<()>;
    fn before_distance(&mut self) -> Result<()>;
    fn before_visit(&mut self) -> Result<()>;
    /// Authenticated count of visible graph points at this captured root.
    fn visible_count(&mut self, snapshot: SnapshotId) -> Result<usize>;
    /// Finite ordered visible row directory window at this root. Used when
    /// every vector has the same mathematical score (a zero-norm query).
    fn row_window(
        &mut self,
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
    ) -> Result<Vec<RowId>>;
    /// Unique graph row directory bounds, independent of visibility. These
    /// preserve the existing dispersed-seed ordering without an owned map.
    fn row_bounds(&mut self) -> Result<Option<(RowId, RowId, usize)>>;
    fn point_at_or_after(&mut self, row: RowId) -> Result<Option<(RowId, PointId)>>;
    /// Graph directory membership independent of MVCC visibility. The shared
    /// union caller admits rows with its authoritative snapshot predicate.
    fn graph_point_for_row(&mut self, row: RowId) -> Result<Option<PointId>> {
        Ok(self
            .point_at_or_after(row)?
            .filter(|(found, _)| *found == row)
            .map(|(_, point)| point))
    }
    /// Finite ordered exact-key graph membership window, including retained
    /// intervals. Statement allowed membership determines which rows may win.
    fn exact_graph_row_after(&mut self, key: &[u8], after: Option<RowId>) -> Result<Option<RowId>>;
}

/// Stack adapter carrying the executing query funder, never TLS state.
pub(crate) struct AdmittedPagedReader<'a> {
    reader: &'a mut dyn PagedVectorReader,
    owner: Option<Arc<dyn MemoryBudget>>,
    reclaim: Option<&'a dyn Fn() -> bool>,
}
impl<'a> AdmittedPagedReader<'a> {
    pub(crate) fn with_reclamation(reader: &'a mut dyn PagedVectorReader, owner: Option<Arc<dyn MemoryBudget>>, reclaim: &'a dyn Fn() -> bool) -> Self { Self { reader, owner, reclaim: Some(reclaim) } }
    fn read(&mut self, request: PagedVectorReadRequest<'_>) -> Result<PagedVectorReadValue> {
        let owner = self.owner.as_ref();
        let reclaim = self.reclaim;
        self.reader.read_with_copy_admission(request, &mut |bytes| {
            let Some(owner) = owner else { return Ok(None); };
            let mut token = VectorCopyAdmission::new(Arc::clone(owner));
            loop {
                match token.reserve_for(bytes, "vector_index", "read_paged_vector_window", "Reduce the selected reader working set or raise its declared memory limit.") {
                    Ok(()) => break,
                    Err(error) if super::union::navigation_memory_refusal(&error) && reclaim.is_some_and(|reclaim| reclaim()) => {},
                    Err(error) => return Err(error),
                }
            }
            Ok(Some(token))
        })
    }
    pub(crate) fn load_point(&mut self, id: PointId) -> Result<ScopedVectorCopy<PagedVectorPoint>> {
        match self.read(PagedVectorReadRequest::Point { id })? {
            PagedVectorReadValue::Point(value) => Ok(value),
            _ => Err(Error::Other("paged admission returned the wrong request kind".into())),
        }
    }
    pub(crate) fn identity(&mut self, data_id: usize) -> Result<ScopedVectorCopy<PagedVectorIdentity>> {
        match self.read(PagedVectorReadRequest::Identity { data_id })? {
            PagedVectorReadValue::Identity(value) => Ok(value),
            _ => Err(Error::Other("paged admission returned the wrong request kind".into())),
        }
    }
    pub(crate) fn row_bounds(&mut self) -> Result<Option<(RowId, RowId, usize)>> {
        match self.read(PagedVectorReadRequest::RowBounds)? {
            PagedVectorReadValue::RowBounds(value) => Ok(*value),
            _ => Err(Error::Other("paged admission returned the wrong request kind".into())),
        }
    }
    pub(crate) fn point_at_or_after(&mut self, row: RowId) -> Result<Option<(RowId, PointId)>> {
        match self.read(PagedVectorReadRequest::PointAtOrAfter { row })? {
            PagedVectorReadValue::PointAtOrAfter(value) => Ok(*value),
            _ => Err(Error::Other("paged admission returned the wrong request kind".into())),
        }
    }
    pub(crate) fn graph_point_for_row(&mut self, row: RowId) -> Result<Option<PointId>> {
        match self.read(PagedVectorReadRequest::GraphPointForRow { row })? {
            PagedVectorReadValue::GraphPointForRow(value) => Ok(*value),
            _ => Err(Error::Other("paged admission returned the wrong request kind".into())),
        }
    }
    pub(crate) fn exact_graph_row_after(&mut self, key: &[u8], after: Option<RowId>) -> Result<Option<RowId>> {
        match self.read(PagedVectorReadRequest::ExactGraphRowAfter { key, after })? {
            PagedVectorReadValue::ExactGraphRowAfter(value) => Ok(*value),
            _ => Err(Error::Other("paged admission returned the wrong request kind".into())),
        }
    }
}

/// All mutations go directly into the engine's one admitted transaction. Its
/// bounded dirty-page owner belongs to the batch plan; failures discard every
/// scratch point, directory update and progress change in this transaction.
pub trait PagedVectorTransaction: PagedVectorReader {
    fn store_point(&mut self, point: &PagedVectorPoint) -> Result<()>;
    fn store_identity(&mut self, identity: &PagedVectorIdentity) -> Result<()>;
    /// Clear transaction-local search scratch. Visited identities and pending
    /// candidates use finite admitted page buffers, and are never accumulated
    /// in a resident population-sized collection. The queue orders PagedEdge
    /// exactly as the fork BinaryHeap (maximum edge first).
    fn begin_search_scratch(&mut self) -> Result<()>;
    fn visit_search_scratch(&mut self, id: PointId) -> Result<bool>;
    fn push_search_candidate(&mut self, edge: hnsw_rs::paged::PagedEdge) -> Result<()>;
    fn pop_search_candidate(&mut self) -> Result<Option<hnsw_rs::paged::PagedEdge>>;
    /// False keeps the search visited set and priority queue inside the
    /// batch's fixed-capacity workspace instead of the four calls above.
    fn spills_search_scratch(&self) -> bool {
        true
    }
}

/// Exactly one persistence entry per insertion batch. The callback runs inside
/// one storage transaction; returning an error rolls it back. Returning progress
/// must atomically commit *both* all callback writes and that exact progress.
/// The complete previous record is compared before work, rejecting stale or
/// cancelled builders. Source floors/root and declaration validity are checked
/// under the same gate. A successful return carries the durable successor.
/// One immutable published root. Readers clone this handle, never begin a
/// fresh writer read for a prior descriptor. The engine shares its captured
/// durable root/buffer pool and admits every page/record window it serves.
pub trait PagedVectorGraphSource: Send + Sync {
    /// Create a captured reader exposing its native admission capability.
    /// Already-funded sources use the ordinary reader by default; wrappers
    /// must forward `read_with_copy_admission` to the actual native provider.
    fn reader_with_copy_admission(&self) -> Result<Box<dyn PagedVectorReader>> {
        self.reader()
    }

    fn reader(&self) -> Result<Box<dyn PagedVectorReader>>;
    /// Exclusive resident source controls transferred to each fresh tail root.
    /// Return a fresh exclusive Arc for these controls; captured pool/root
    /// handles shared elsewhere retain their own existing charge. Every reader
    /// owns its finite decode/page reservations and captures the reader root.
    fn retained_bytes(&self) -> usize;
}
pub struct PagedVectorCommittedBatch {
    pub progress: PagedVectorProgress,
    pub source: Arc<dyn PagedVectorGraphSource>,
}

pub trait PagedVectorPersistence: Send + Sync {
    fn commit_batch(
        &self,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        batch: &mut dyn FnMut(&mut dyn PagedVectorTransaction) -> Result<PagedVectorProgress>,
    ) -> Result<PagedVectorCommittedBatch>;
}

struct PointAdapter<'a> {
    transaction: &'a mut dyn PagedVectorTransaction,
    writes: usize,
    max_writes: usize,
    max_node_bytes: usize,
    dimension: usize,
    quantization: VectorQuantization,
    accountant: &'a Arc<dyn MemoryBudget>,
    live_workspace: &'a Arc<AtomicUsize>,
}
fn point_bytes<T>(point: &PagedPoint<T>) -> Option<usize> {
    point
        .values
        .capacity()
        .checked_mul(std::mem::size_of::<T>())?
        .checked_add(
            point
                .neighbours
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<hnsw_rs::paged::PagedEdge>>())?,
        )?
        .checked_add(point.neighbours.iter().try_fold(0usize, |n, l| {
            n.checked_add(
                l.capacity()
                    .checked_mul(std::mem::size_of::<hnsw_rs::paged::PagedEdge>())?,
            )
        })?)?
        .checked_add(std::mem::size_of::<PagedVectorPoint>())
}
impl PointAdapter<'_> {
    fn reserve_selection_window<T>(&self, points: usize) -> Result<VectorWorkspaceReservation> {
        let bytes = self
            .max_node_bytes
            .checked_add(std::mem::size_of::<PagedPoint<T>>())
            .and_then(|node| node.checked_mul(points))
            .ok_or_else(workspace_overflow)?;
        VectorWorkspaceReservation::try_new(
            Arc::clone(self.accountant),
            Arc::clone(self.live_workspace),
            bytes,
            "paged_vector_selection",
            "release idle serving pages",
        )
    }
    fn before_write(&mut self) -> Result<()> {
        if self.writes >= self.max_writes {
            return Err(Error::Other(
                "paged vector batch node-write limit exhausted".into(),
            ));
        }
        self.writes += 1;
        self.transaction.checkpoint()
    }
    fn validate<T>(&self, point: &PagedPoint<T>) -> Result<()> {
        if point_bytes(point).is_none_or(|bytes| bytes > self.max_node_bytes) {
            return Err(Error::Other(
                "paged vector point exceeds admitted decode workspace".into(),
            ));
        }
        Ok(())
    }
}
impl PagedPointStorage<f32> for PointAdapter<'_> {
    type Error = Error;
    fn with_selection_window<R>(
        &mut self,
        points: usize,
        select: impl FnOnce(&mut Self) -> std::result::Result<R, PagedGraphError<Error>>,
    ) -> std::result::Result<R, PagedGraphError<Error>> {
        let _reservation = self.reserve_selection_window::<f32>(points)?;
        select(self)
    }
    fn load_point(&mut self, id: PointId) -> Result<PagedPoint<f32>> {
        let PagedVectorPoint::F32(point) = self.transaction.load_point(id)? else {
            return Err(Error::Other(
                "paged vector payload representation mismatch".into(),
            ));
        };
        self.validate(&point)?;
        if point.values.len() != self.dimension || point.values.iter().any(|v| !v.is_finite()) {
            return Err(Error::Other(
                "paged vector point dimension or values mismatch".into(),
            ));
        }
        Ok(point)
    }
    fn store_point(&mut self, point: &PagedPoint<f32>) -> Result<()> {
        self.validate(point)?;
        self.before_write()?;
        self.transaction
            .store_point(&PagedVectorPoint::F32(point.clone()))
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.transaction.checkpoint()
    }
    fn before_distance(&mut self) -> Result<()> {
        self.transaction.before_distance()
    }
    fn before_visit(&mut self) -> Result<()> {
        self.transaction.before_visit()
    }
    fn begin_search_scratch(&mut self) -> Result<()> {
        if !self.transaction.spills_search_scratch() {
            return Ok(());
        }
        self.transaction.begin_search_scratch()
    }
    fn visit_search_scratch(&mut self, id: PointId) -> Result<Option<bool>> {
        if !self.transaction.spills_search_scratch() {
            return Ok(None);
        }
        self.transaction.visit_search_scratch(id).map(Some)
    }
    fn push_search_candidate(&mut self, edge: hnsw_rs::paged::PagedEdge) -> Result<bool> {
        if !self.transaction.spills_search_scratch() {
            return Ok(false);
        }
        self.transaction.push_search_candidate(edge)?;
        Ok(true)
    }
    fn pop_search_candidate(&mut self) -> Result<Option<Option<hnsw_rs::paged::PagedEdge>>> {
        if !self.transaction.spills_search_scratch() {
            return Ok(None);
        }
        self.transaction.pop_search_candidate().map(Some)
    }
}
impl PagedPointStorage<u8> for PointAdapter<'_> {
    type Error = Error;
    fn with_selection_window<R>(
        &mut self,
        points: usize,
        select: impl FnOnce(&mut Self) -> std::result::Result<R, PagedGraphError<Error>>,
    ) -> std::result::Result<R, PagedGraphError<Error>> {
        let _reservation = self.reserve_selection_window::<u8>(points)?;
        select(self)
    }
    fn load_point(&mut self, id: PointId) -> Result<PagedPoint<u8>> {
        let PagedVectorPoint::Quantized(point) = self.transaction.load_point(id)? else {
            return Err(Error::Other(
                "paged vector payload representation mismatch".into(),
            ));
        };
        self.validate(&point)?;
        let header = crate::quantized::decode_hnsw_quantized(&point.values)
            .ok_or_else(|| Error::Other("invalid paged quantized point".into()))?;
        if header.len != self.dimension
            || header.payload.len()
                != match self.quantization {
                    VectorQuantization::SQ8 => self.dimension,
                    VectorQuantization::SQ4 => self.dimension.div_ceil(2),
                    VectorQuantization::F32 => 0,
                }
        {
            return Err(Error::Other(
                "paged quantized point definition mismatch".into(),
            ));
        }
        Ok(point)
    }
    fn store_point(&mut self, point: &PagedPoint<u8>) -> Result<()> {
        self.validate(point)?;
        self.before_write()?;
        self.transaction
            .store_point(&PagedVectorPoint::Quantized(point.clone()))
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.transaction.checkpoint()
    }
    fn before_distance(&mut self) -> Result<()> {
        self.transaction.before_distance()
    }
    fn before_visit(&mut self) -> Result<()> {
        self.transaction.before_visit()
    }
    fn begin_search_scratch(&mut self) -> Result<()> {
        if !self.transaction.spills_search_scratch() {
            return Ok(());
        }
        self.transaction.begin_search_scratch()
    }
    fn visit_search_scratch(&mut self, id: PointId) -> Result<Option<bool>> {
        if !self.transaction.spills_search_scratch() {
            return Ok(None);
        }
        self.transaction.visit_search_scratch(id).map(Some)
    }
    fn push_search_candidate(&mut self, edge: hnsw_rs::paged::PagedEdge) -> Result<bool> {
        if !self.transaction.spills_search_scratch() {
            return Ok(false);
        }
        self.transaction.push_search_candidate(edge)?;
        Ok(true)
    }
    fn pop_search_candidate(&mut self) -> Result<Option<Option<hnsw_rs::paged::PagedEdge>>> {
        if !self.transaction.spills_search_scratch() {
            return Ok(None);
        }
        self.transaction.pop_search_candidate().map(Some)
    }
}
fn graph_error(error: PagedGraphError<Error>) -> Error {
    match error {
        PagedGraphError::Storage(error) => error,
        other => Error::Other(format!("paged HNSW operation refused: {other:?}")),
    }
}
// Construction scratch is an algorithm window, not the writer accountant.
// Non-spilling providers can exhaust navigation visits; spilling providers
// still require sufficient result and pruning windows for their policy.
fn build_graph_error(error: PagedGraphError<Error>) -> Error {
    match error {
        PagedGraphError::ScratchExhausted => Error::Other(
            "HNSW build scratch window exhausted; inspect the construction provider and policy"
                .into(),
        ),
        other => graph_error(other),
    }
}
fn graph_policy(
    policy: ResolvedVectorPolicy,
    quantization: VectorQuantization,
) -> PagedGraphPolicy {
    let quantized = quantization != VectorQuantization::F32;
    PagedGraphPolicy {
        m: policy.hnsw_m,
        ef_construction: policy.hnsw_ef_construction,
        max_layer: 16,
        extend_candidates: quantized,
        keep_pruned: quantized,
    }
}

struct PagedRootCharge {
    accountant: Arc<dyn MemoryBudget>,
    bytes: usize,
}
impl PagedRootCharge {
    fn new(accountant: Arc<dyn MemoryBudget>, bytes: usize) -> Result<Self> {
        accountant.try_allocate_for(
            bytes,
            "vector_index",
            "retain_paged_vector_root",
            "release idle serving roots",
        )?;
        Ok(Self { accountant, bytes })
    }
    fn shrink(&mut self, bytes: usize) {
        self.accountant.release(self.bytes - bytes);
        self.bytes = bytes;
    }
}
impl Drop for PagedRootCharge {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}
// The source's exclusive controls and descriptor are paid for until the last
// tail/root/reader drops them, independently of the store's graph wrapper.
struct RetainedPagedPartition {
    partition: VectorPartitionRef,
    charge: PagedRootCharge,
}
struct RetainedPagedReader {
    reader: Box<dyn PagedVectorReader>,
    _charge: Arc<PagedRootCharge>,
}
impl PagedVectorReader for RetainedPagedReader {
    fn read_with_copy_admission(
        &mut self, request: PagedVectorReadRequest<'_>,
        admit: &mut dyn FnMut(usize) -> Result<Option<VectorCopyAdmission>>,
    ) -> Result<PagedVectorReadValue> {
        self.reader.read_with_copy_admission(request, admit)
    }

    fn load_point(&mut self, id: PointId) -> Result<PagedVectorPoint> {
        self.reader.load_point(id)
    }
    fn identity(&mut self, id: usize) -> Result<PagedVectorIdentity> {
        self.reader.identity(id)
    }
    fn point_for_row(&mut self, row: RowId, snapshot: SnapshotId) -> Result<Option<PointId>> {
        self.reader.point_for_row(row, snapshot)
    }
    fn exact_rows(
        &mut self,
        key: &[u8],
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
    ) -> Result<Vec<RowId>> {
        self.reader.exact_rows(key, snapshot, after, limit)
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.reader.checkpoint()
    }
    fn before_distance(&mut self) -> Result<()> {
        self.reader.before_distance()
    }
    fn before_visit(&mut self) -> Result<()> {
        self.reader.before_visit()
    }
    fn visible_count(&mut self, snapshot: SnapshotId) -> Result<usize> {
        self.reader.visible_count(snapshot)
    }
    fn row_window(
        &mut self,
        snapshot: SnapshotId,
        after: Option<RowId>,
        limit: usize,
    ) -> Result<Vec<RowId>> {
        self.reader.row_window(snapshot, after, limit)
    }
    fn row_bounds(&mut self) -> Result<Option<(RowId, RowId, usize)>> {
        self.reader.row_bounds()
    }
    fn point_at_or_after(&mut self, row: RowId) -> Result<Option<(RowId, PointId)>> {
        self.reader.point_at_or_after(row)
    }
    fn graph_point_for_row(&mut self, row: RowId) -> Result<Option<PointId>> {
        self.reader.graph_point_for_row(row)
    }
    fn exact_graph_row_after(&mut self, key: &[u8], after: Option<RowId>) -> Result<Option<RowId>> {
        self.reader.exact_graph_row_after(key, after)
    }
}
fn root_control_head() -> usize {
    std::mem::size_of::<PagedVectorTail>()
        + std::mem::size_of::<PagedRootCharge>()
        + 6 * std::mem::size_of::<usize>()
}
fn root_control_bytes(
    progress: &PagedVectorProgress,
    source: &dyn PagedVectorGraphSource,
) -> Result<usize> {
    root_control_head()
        .checked_add(std::mem::size_of::<PagedVectorProgress>())
        .and_then(|n| n.checked_add(progress.last_source_key.as_ref().map_or(0, Vec::capacity)))
        .and_then(|n| n.checked_add(progress.cleanup_cursor.as_ref().map_or(0, Vec::capacity)))
        .and_then(|n| n.checked_add(source.retained_bytes()))
        .ok_or_else(workspace_overflow)
}

/// A mutable tail's constant-size control handle. Nodes, row and exact
/// directories are engine-owned pages. Each append is searchable immediately
/// after the row transaction atomically publishes its successor descriptor.
/// Readers opened earlier retain the preceding root through `reader`.
#[derive(Clone)]
pub struct PagedVectorTail {
    partition: Arc<RetainedPagedPartition>,
    progress: Arc<PagedVectorProgress>,
    policy: ResolvedVectorPolicy,
    dimension: usize,
    quantization: VectorQuantization,
    persistence: Arc<dyn PagedVectorPersistence>,
    source: Arc<dyn PagedVectorGraphSource>,
    accountant: Arc<dyn MemoryBudget>,
    live_workspace: Arc<AtomicUsize>,
    root_charge: Arc<PagedRootCharge>,
}
impl PagedVectorTail {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition: VectorPartitionRef,
        progress: PagedVectorProgress,
        policy: ResolvedVectorPolicy,
        dimension: usize,
        quantization: VectorQuantization,
        persistence: Arc<dyn PagedVectorPersistence>,
        source: Arc<dyn PagedVectorGraphSource>,
        accountant: Arc<dyn MemoryBudget>,
        live_workspace: Arc<AtomicUsize>,
    ) -> Result<Self> {
        if dimension == 0 || !graph_policy(policy, quantization).validate() {
            return Err(Error::Other("invalid paged vector tail definition".into()));
        }
        // Exclusive source controls transfer into this root. Shared pool
        // ownership remains separately charged by the engine source.
        if source.retained_bytes() != 0 && Arc::strong_count(&source) != 1 {
            return Err(Error::Other(
                "paged root requires exclusive uncharged source controls".into(),
            ));
        }
        let partition_bytes = std::mem::size_of::<RetainedPagedPartition>()
            .checked_add(2 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(partition.index.table.capacity()))
            .and_then(|n| n.checked_add(partition.index.column.capacity()))
            .and_then(|n| {
                n.checked_add(
                    partition.partition_key.estimated_bytes()
                        - std::mem::size_of::<contextdb_core::VectorPartitionKey>(),
                )
            })
            .ok_or_else(workspace_overflow)?;
        let partition_charge = PagedRootCharge::new(Arc::clone(&accountant), partition_bytes)?;
        let root_charge = Arc::new(PagedRootCharge::new(
            Arc::clone(&accountant),
            root_control_bytes(&progress, source.as_ref())?,
        )?);
        Ok(Self {
            partition: Arc::new(RetainedPagedPartition {
                partition,
                charge: partition_charge,
            }),
            progress: Arc::new(progress),
            policy,
            dimension,
            quantization,
            persistence,
            source,
            accountant,
            live_workspace,
            root_charge,
        })
    }
    pub fn partition(&self) -> &VectorPartitionRef {
        &self.partition.partition
    }
    pub fn progress(&self) -> &PagedVectorProgress {
        &self.progress
    }
    pub(super) fn graph_reader(&self) -> Result<Box<dyn PagedVectorReader>> {
        Ok(Box::new(RetainedPagedReader {
            reader: self.source.reader_with_copy_admission()?,
            _charge: Arc::clone(&self.root_charge),
        }))
    }

    pub(crate) fn definition(&self) -> (usize, VectorQuantization, ResolvedVectorPolicy) {
        (self.dimension, self.quantization, self.policy)
    }
    /// Retained root/source charge plus the shared immutable partition charge.
    /// Clones share both receipts; successor roots retain the partition once.
    pub fn retained_control_bytes(&self) -> usize {
        self.root_charge.bytes + self.partition.charge.bytes
    }

    pub fn len(&self) -> usize {
        self.progress.graph.points
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn reserve(
        &self,
        plan: PagedVectorBatchPlan,
        operation: &str,
    ) -> Result<VectorWorkspaceReservation> {
        plan.validate(
            graph_policy(self.policy, self.quantization),
            self.dimension,
            self.quantization,
        )?;
        VectorWorkspaceReservation::try_new(
            Arc::clone(&self.accountant),
            Arc::clone(&self.live_workspace),
            plan.workspace_bytes()?,
            operation,
            "reduce the insertion batch or release idle serving pages",
        )
    }
    fn reserve_construction(
        &self,
        plan: PagedVectorBatchPlan,
        operation: &str,
    ) -> Result<VectorWorkspaceReservation> {
        plan.validate(
            graph_policy(self.policy, self.quantization),
            self.dimension,
            self.quantization,
        )?;
        VectorWorkspaceReservation::try_new(
            Arc::clone(&self.accountant),
            Arc::clone(&self.live_workspace),
            plan.construction_workspace_bytes()?,
            operation,
            "reduce the insertion batch or release idle serving pages",
        )
    }
    /// Inserts the supplied finite row batch. The engine's persistence callback
    /// must include its row publication in this same transaction. On failure,
    /// both the durable state and this handle retain the previous progress.
    pub fn append(&mut self, entries: &[VectorEntry], plan: PagedVectorBatchPlan) -> Result<()> {
        if self.progress.phase != PagedVectorPhase::Inserting {
            return Err(Error::Other(
                "sealed paged generation cannot accept tail inserts".into(),
            ));
        }
        if entries.len() > plan.max_points {
            return Err(Error::Other(
                "paged vector append exceeds its admitted batch".into(),
            ));
        }
        let _reservation = self.reserve_construction(plan, "append_paged_vector_tail")?;
        let previous = self.progress.as_ref().clone();
        let retained_bound = root_control_head()
            .checked_add(plan.progress_bytes)
            .ok_or_else(workspace_overflow)?;
        let mut next_root_charge =
            PagedRootCharge::new(Arc::clone(&self.accountant), retained_bound)?;
        let mut invoked = false;
        let mut produced = None;
        let mut batch = |transaction: &mut dyn PagedVectorTransaction| {
            if invoked {
                return Err(Error::Other(
                    "paged vector persistence invoked a batch twice".into(),
                ));
            }
            invoked = true;
            let mut next = previous.clone();
            let mut workspace = PagedGraphWorkspace::new_lazy(plan.scratch_points);
            let mut adapter = PointAdapter {
                transaction,
                writes: 0,
                max_writes: plan.max_node_writes,
                max_node_bytes: plan.max_node_bytes,
                dimension: self.dimension,
                quantization: self.quantization,
                accountant: &self.accountant,
                live_workspace: &self.live_workspace,
            };
            for entry in entries {
                self.insert_entry(&mut adapter, &mut workspace, &mut next, entry)?;
            }
            next.total_points = next.graph.points;
            for entry in entries {
                next.frontier_lsn = next.frontier_lsn.max(entry.lsn);
                next.frontier_tx = next.frontier_tx.max(entry.created_tx);
            }
            produced = Some(next.clone());
            Ok(next)
        };
        let committed = self.persistence.commit_batch(&previous, plan, &mut batch)?;
        let next = committed.progress;
        if !invoked
            || produced.as_ref() != Some(&next)
            || next.generation_id != previous.generation_id
            || next.graph.points != previous.graph.points + entries.len()
        {
            return Err(Error::Other(
                "paged vector persistence returned an invalid successor".into(),
            ));
        }
        let retained_bytes = root_control_bytes(&next, committed.source.as_ref())?;
        if retained_bytes > retained_bound
            || committed.source.retained_bytes() != 0 && Arc::strong_count(&committed.source) != 1
        {
            return Err(Error::Other(
                "committed paged source controls exceed their admitted exclusive root owner".into(),
            ));
        }
        next_root_charge.shrink(retained_bytes);
        self.progress = Arc::new(next);
        self.source = committed.source;
        self.root_charge = Arc::new(next_root_charge);
        Ok(())
    }
    fn insert_entry(
        &self,
        adapter: &mut PointAdapter<'_>,
        workspace: &mut PagedGraphWorkspace,
        progress: &mut PagedVectorProgress,
        entry: &VectorEntry,
    ) -> Result<()> {
        if entry.vector.len() != self.dimension || entry.vector.iter().any(|v| !v.is_finite()) {
            return Err(Error::Other(
                "paged vector insertion dimension or values mismatch".into(),
            ));
        }
        let stored = StoredVector::from_f32(&entry.vector, self.quantization);
        let data_id = progress.graph.points;
        let policy = graph_policy(self.policy, self.quantization);
        let point = match self.quantization {
            VectorQuantization::F32 => insert_paged(
                adapter,
                policy,
                &mut progress.graph,
                workspace,
                entry.vector.clone(),
                data_id,
                &hnsw_f32_distance(),
            ),
            VectorQuantization::SQ8 | VectorQuantization::SQ4 => insert_paged(
                adapter,
                policy,
                &mut progress.graph,
                workspace,
                stored.to_hnsw_u8(),
                data_id,
                &DistQuantizedCosine {
                    quantization: self.quantization,
                },
            ),
        }
        .map_err(build_graph_error)?;
        adapter.transaction.store_identity(&PagedVectorIdentity {
            point,
            data_id,
            row_id: entry.row_id,
            lsn: entry.lsn,
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            exact_key: exact_key_for_stored_vector(&stored).unwrap_or_default(),
        })
    }
}

/// Returned rows keep their actual allocation charged through final drop. The
/// engine may move both parts together when transferring statement ownership.
pub struct PagedVectorSearchResult {
    rows: Vec<(RowId, f32)>,
    reservation: Option<VectorWorkspaceReservation>,
}
impl PagedVectorSearchResult {
    pub fn rows(&self) -> &[(RowId, f32)] {
        &self.rows
    }
    pub fn into_parts(self) -> (Vec<(RowId, f32)>, Option<VectorWorkspaceReservation>) {
        (self.rows, self.reservation)
    }
}
struct ReadAdapter<'a> {
    reader: &'a mut dyn PagedVectorReader,
    snapshot: SnapshotId,
    predicate: &'a mut dyn FnMut(RowId) -> Result<bool>,
}
impl ReadAdapter<'_> {
    fn allowed(&mut self, origin: usize) -> Result<bool> {
        let identity = self.reader.identity(origin)?;
        if identity.data_id != origin {
            return Err(Error::Other("paged graph row identity mismatch".into()));
        }
        if identity.created_tx > TxId::from_snapshot(self.snapshot)
            || identity
                .deleted_tx
                .is_some_and(|tx| tx <= TxId::from_snapshot(self.snapshot))
        {
            return Ok(false);
        }
        (self.predicate)(identity.row_id)
    }
}
impl PagedPointStorage<f32> for ReadAdapter<'_> {
    type Error = Error;
    fn load_point(&mut self, id: PointId) -> Result<PagedPoint<f32>> {
        match self.reader.load_point(id)? {
            PagedVectorPoint::F32(point) => Ok(point),
            _ => Err(Error::Other("paged graph representation mismatch".into())),
        }
    }
    fn store_point(&mut self, _: &PagedPoint<f32>) -> Result<()> {
        Err(Error::Other(
            "cannot mutate a published paged graph reader".into(),
        ))
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.reader.checkpoint()
    }
    fn before_distance(&mut self) -> Result<()> {
        self.reader.before_distance()
    }
    fn before_visit(&mut self) -> Result<()> {
        self.reader.before_visit()
    }
    fn is_allowed(&mut self, origin: usize) -> Result<bool> {
        self.allowed(origin)
    }
}
impl PagedPointStorage<u8> for ReadAdapter<'_> {
    type Error = Error;
    fn load_point(&mut self, id: PointId) -> Result<PagedPoint<u8>> {
        match self.reader.load_point(id)? {
            PagedVectorPoint::Quantized(point) => Ok(point),
            _ => Err(Error::Other("paged graph representation mismatch".into())),
        }
    }
    fn store_point(&mut self, _: &PagedPoint<u8>) -> Result<()> {
        Err(Error::Other(
            "cannot mutate a published paged graph reader".into(),
        ))
    }
    fn checkpoint(&mut self) -> Result<()> {
        self.reader.checkpoint()
    }
    fn before_distance(&mut self) -> Result<()> {
        self.reader.before_distance()
    }
    fn before_visit(&mut self) -> Result<()> {
        self.reader.before_visit()
    }
    fn is_allowed(&mut self, origin: usize) -> Result<bool> {
        self.allowed(origin)
    }
}
impl PagedVectorTail {
    /// Serving performs no build work. Visibility enters graph admission,
    /// rather than filtering the final nearest candidates after the walk.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        snapshot: SnapshotId,
        plan: PagedVectorBatchPlan,
    ) -> Result<PagedVectorSearchResult> {
        self.search_with_predicate(query, k, snapshot, plan, None, &mut |_| Ok(true))
    }
    /// The caller supplies its authoritative visible allowed count and shared
    /// predicate. Physical read, distance and visit work use the captured
    /// reader's statement hooks; no independent query budget is reset here.
    pub fn search_allowed(
        &self,
        query: &[f32],
        k: usize,
        snapshot: SnapshotId,
        plan: PagedVectorBatchPlan,
        allowed_count: usize,
        predicate: &mut dyn FnMut(RowId) -> Result<bool>,
    ) -> Result<PagedVectorSearchResult> {
        self.search_with_predicate(query, k, snapshot, plan, Some(allowed_count), predicate)
    }
    #[allow(clippy::too_many_arguments)]
    fn search_with_predicate(
        &self,
        query: &[f32],
        k: usize,
        snapshot: SnapshotId,
        plan: PagedVectorBatchPlan,
        allowed_count: Option<usize>,
        predicate: &mut dyn FnMut(RowId) -> Result<bool>,
    ) -> Result<PagedVectorSearchResult> {
        if query.len() != self.dimension || query.iter().any(|v| !v.is_finite()) {
            return Err(Error::Other(
                "paged vector query dimension or values mismatch".into(),
            ));
        }
        if k == 0 {
            return Ok(PagedVectorSearchResult {
                rows: Vec::new(),
                reservation: None,
            });
        }
        let mut reservation = self.reserve(plan, "search_paged_vector_tail")?;
        let ef = self.policy.hnsw_ef_search.max(k);
        if ef >= plan.scratch_points {
            return Err(Error::Other(
                "paged vector search exceeds its admitted breadth".into(),
            ));
        }
        let output = {
            let mut reader = self.graph_reader()?;
            let visible_count = reader.visible_count(snapshot)?;
            let count = allowed_count.unwrap_or(visible_count);
            if count > visible_count || visible_count > self.progress.graph.points {
                return Err(Error::Other(
                    "paged graph visible count disagrees with allowed membership".into(),
                ));
            }
            let required = k.min(count);
            if query.iter().all(|value| *value == 0.) {
                let mut output = Vec::with_capacity(required);
                let mut after = None;
                while output.len() < required {
                    reader.checkpoint()?;
                    let window = reader.row_window(snapshot, after, k)?;
                    if window.len() > k
                        || window.windows(2).any(|w| w[0] >= w[1])
                        || window
                            .first()
                            .is_some_and(|row| after.is_some_and(|old| *row <= old))
                    {
                        return Err(Error::Other(
                            "paged vector row directory returned an invalid window".into(),
                        ));
                    }
                    if window.is_empty() {
                        break;
                    }
                    let full = window.len() == k;
                    for row in window {
                        after = Some(row);
                        if !predicate(row)? {
                            continue;
                        }
                        reader.before_distance()?;
                        output.push((row, 0.));
                        if output.len() == required {
                            break;
                        }
                    }
                    if !full {
                        break;
                    }
                }
                if output.len() < required {
                    return Err(Error::Other(
                        "paged vector row directory disagrees with its visible count".into(),
                    ));
                }
                drop(reader);
                let bytes = output
                    .capacity()
                    .checked_mul(std::mem::size_of::<(RowId, f32)>())
                    .ok_or_else(workspace_overflow)?;
                reservation.resize(
                    bytes,
                    "retain_paged_vector_search_result",
                    "retain the result reservation with its rows",
                )?;
                return Ok(PagedVectorSearchResult {
                    rows: output,
                    reservation: Some(reservation),
                });
            }
            let mut workspace = PagedGraphWorkspace::new(plan.scratch_points);
            let stored = StoredVector::from_f32(query, self.quantization);
            let mut adapter = ReadAdapter {
                reader: reader.as_mut(),
                snapshot,
                predicate,
            };
            // The exact-key directory participates in allowed refill below.
            // Require completeness only after merging both candidate sources;
            // a short graph walk alone is not a refusal for an exact tie set.
            let neighbours = match self.quantization {
                VectorQuantization::F32 => search_paged_allowed(
                    &mut adapter,
                    graph_policy(self.policy, self.quantization),
                    &self.progress.graph,
                    &mut workspace,
                    query,
                    ef,
                    0,
                    count,
                    &hnsw_f32_distance(),
                ),
                VectorQuantization::SQ8 | VectorQuantization::SQ4 => search_paged_allowed(
                    &mut adapter,
                    graph_policy(self.policy, self.quantization),
                    &self.progress.graph,
                    &mut workspace,
                    &stored.to_hnsw_u8(),
                    ef,
                    0,
                    count,
                    &DistQuantizedCosine {
                        quantization: self.quantization,
                    },
                ),
            }
            .map_err(graph_error)?;
            let mut rows = Vec::with_capacity(ef + k);
            for neighbour in neighbours {
                let identity = adapter.reader.identity(neighbour.origin_id)?;
                if identity.point != neighbour.point_id || identity.data_id != neighbour.origin_id {
                    return Err(Error::Other(
                        "paged vector point and row directory disagree".into(),
                    ));
                }
                let score = match self.quantization {
                    VectorQuantization::F32 => {
                        let PagedVectorPoint::F32(point) =
                            adapter.reader.load_point(neighbour.point_id)?
                        else {
                            return Err(Error::Other("paged graph representation mismatch".into()));
                        };
                        adapter.reader.before_distance()?;
                        crate::cosine_similarity(&point.values, query)
                    }
                    VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
                        let PagedVectorPoint::Quantized(point) =
                            adapter.reader.load_point(neighbour.point_id)?
                        else {
                            return Err(Error::Other("paged graph representation mismatch".into()));
                        };
                        adapter.reader.before_distance()?;
                        crate::quantized::quantized_hnsw_query_score(
                            &point.values,
                            query,
                            self.quantization,
                        )
                        .ok_or_else(|| {
                            Error::Other("invalid paged quantized query payload".into())
                        })?
                    }
                };
                rows.push((identity.row_id, score));
            }
            // Reuse the established self-score contract, including zero norms
            // and quantization loss. Ordered windows refill through excluded
            // ties instead of materializing the entire exact-key bucket.
            let key = exact_key_for_stored_vector(&stored).unwrap_or_default();
            let mut after = None;
            let mut exact_kept = 0;
            let mut exact_score = None;
            while exact_kept < required {
                adapter.reader.checkpoint()?;
                let exact = adapter.reader.exact_rows(&key, snapshot, after, k)?;
                if exact.len() > k
                    || exact.windows(2).any(|w| w[0] >= w[1])
                    || exact
                        .first()
                        .is_some_and(|row| after.is_some_and(|old| *row <= old))
                {
                    return Err(Error::Other(
                        "paged vector exact directory returned an invalid window".into(),
                    ));
                }
                if exact.is_empty() {
                    break;
                }
                let full = exact.len() == k;
                for row in exact {
                    after = Some(row);
                    if !(adapter.predicate)(row)? {
                        continue;
                    }
                    let score = match exact_score {
                        Some(score) => score,
                        None => {
                            adapter.reader.before_distance()?;
                            let score = stored.cosine_similarity(query);
                            exact_score = Some(score);
                            score
                        }
                    };
                    rows.push((row, score));
                    exact_kept += 1;
                    if exact_kept == required {
                        break;
                    }
                }
                if !full {
                    break;
                }
            }
            rows.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let mut output = Vec::with_capacity(required);
            for (row, score) in rows {
                if output.iter().any(|(id, _)| *id == row) {
                    continue;
                }
                output.push((row, score));
                if output.len() == required {
                    break;
                }
            }
            if output.len() < required {
                return Err(Error::Other(
                    "paged graph did not reach the required visible allowed rows".into(),
                ));
            }
            output
        };
        // Every page/heap/decode scratch owner above is gone before shrinking
        // the token. The returned row allocation keeps its actual charge.
        let bytes = output
            .capacity()
            .checked_mul(std::mem::size_of::<(RowId, f32)>())
            .ok_or_else(workspace_overflow)?;
        reservation.resize(
            bytes,
            "retain_paged_vector_search_result",
            "retain the result reservation with its rows",
        )?;
        Ok(PagedVectorSearchResult {
            rows: output,
            reservation: Some(reservation),
        })
    }
}

/// A retained-frontier source point, already stored in the column's declared
/// representation. Quantized bytes use the unchanged HNSW payload header;
/// rebuilding never dequantizes and re-quantizes retained payloads.
#[derive(Clone, Debug)]
pub enum PagedVectorValues {
    F32(Vec<f32>),
    Quantized(Vec<u8>),
}
impl PagedVectorValues {
    /// Preserve an authenticated durable SQ payload in the existing graph
    /// representation. The caller admits the returned header/body allocation;
    /// no F32 reconstruction or second quantization occurs on this path.
    #[doc(hidden)]
    pub fn from_quantized_payload(
        quantization: VectorQuantization,
        len: usize,
        min: f32,
        max: f32,
        payload: &[u8],
    ) -> Result<Self> {
        let expected = match quantization {
            VectorQuantization::SQ8 => len,
            VectorQuantization::SQ4 => len.div_ceil(2),
            VectorQuantization::F32 => {
                return Err(Error::Other("F32 has no quantized payload".into()));
            }
        };
        if len == 0
            || len > u32::MAX as usize
            || !min.is_finite()
            || !max.is_finite()
            || min > max
            || payload.len() != expected
        {
            return Err(Error::Other(
                "invalid durable quantized vector payload".into(),
            ));
        }
        Ok(Self::Quantized(
            crate::quantized::encode_hnsw_quantized_payload(len, min, max, payload),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct PagedVectorBuildEntry {
    /// Full key in canonical insertion order: source LSN, creation tx,
    /// representation insertion key, then row id. Resumption is exclusive.
    pub source_key: Vec<u8>,
    pub row_id: RowId,
    pub lsn: Lsn,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub values: PagedVectorValues,
}

/// Actual work performed by one finite cleanup transaction.
#[derive(Clone, Debug)]
pub struct PagedVectorReclamation {
    pub keys_removed: usize,
    pub last_key: Option<Vec<u8>>,
    pub complete: bool,
}

/// Engine-owned finite work that must participate in the same scratch/progress
/// transaction as point writes. Source reads retain the persisted frontier
/// floor, authenticate each used interval/body and seek after the full key.
/// The engine checks source/declaration/erasure validity before each batch.
pub trait PagedGenerationTransaction: PagedVectorTransaction {
    fn source_next(
        &mut self,
        progress: &PagedVectorProgress,
        after: Option<&[u8]>,
    ) -> Result<Option<PagedVectorBuildEntry>>;
    /// A provider may end one finite physical scan window before reaching EOF.
    /// Existing providers return true; a bounded engine provider returns false
    /// only after authenticating and skipping at least one complete source key.
    fn source_window_exhausted(&self) -> bool {
        true
    }
    /// Exclusive authenticated position reached by that finite scan window.
    /// Rejected intervals advance progress without inserting a graph point.
    fn source_window_position(&self) -> Option<&[u8]> {
        None
    }
    /// Compare a canonical insertion key with a physical rejection position.
    /// A provider using version buckets may resume inside an identity bucket;
    /// ordinary providers retain the existing strict canonical comparison.
    fn source_entry_after_position(&self, entry: &[u8], position: &[u8]) -> bool {
        entry > position
    }
    /// Persist one encoded point in the sealed change page layout, including
    /// its authenticated identity. Prior sealed pages stay durable, not resident.
    fn seal_point(&mut self, point: PointId, encoded: &[u8]) -> Result<()>;
    /// Verify the complete encoded base plus searchable suffix through this
    /// LSN, and atomically publish descriptor/coverage. Retain old reader roots.
    /// An emptied route instead returns Abandoned with its storage queued
    /// for cleanup in the same transaction.
    fn publish(
        &mut self,
        progress: &PagedVectorProgress,
        through_lsn: Lsn,
    ) -> Result<PagedVectorPhase>;
    /// Remove at most max_keys eligible keys after the exclusive cursor. The
    /// engine rechecks reader/external floors and erasure fences transactionally.
    fn reclaim(
        &mut self,
        generation: u64,
        after: Option<&[u8]>,
        max_keys: usize,
    ) -> Result<PagedVectorReclamation>;
}

/// One persistence invocation for each build, encoding, publication or cleanup
/// batch. Atomically commit callback mutations with its returned progress;
/// on cancellation/error discard the current transaction. `previous` is a
/// complete compare-and-swap record, including RNG and all source cursors.
pub trait PagedGenerationPersistence: Send + Sync {
    /// Fenced metadata-only invalidation. CAS the complete previous record,
    /// reject any serving/external-reader exposure, and atomically install
    /// next (Abandoned) plus cleanup inventory. A stale declaration/frontier
    /// may be abandoned even though it cannot admit another construction batch.
    /// Preserve its cleanup identity and floors until this marker is durable.
    fn abandon_generation(
        &self,
        previous: &PagedVectorProgress,
        next: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
    ) -> Result<PagedVectorProgress>;
    fn commit_generation_batch(
        &self,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        batch: &mut dyn FnMut(&mut dyn PagedGenerationTransaction) -> Result<PagedVectorProgress>,
    ) -> Result<PagedVectorProgress>;
}

pub(crate) struct PagedBuilderProgress {
    value: PagedVectorProgress,
    _charge: PagedRootCharge,
}
impl std::ops::Deref for PagedBuilderProgress {
    type Target = PagedVectorProgress;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
fn builder_progress_bytes(progress: &PagedVectorProgress) -> Result<usize> {
    std::mem::size_of::<PagedBuilderProgress>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .and_then(|n| n.checked_add(progress.last_source_key.as_ref().map_or(0, Vec::capacity)))
        .and_then(|n| n.checked_add(progress.cleanup_cursor.as_ref().map_or(0, Vec::capacity)))
        .ok_or_else(workspace_overflow)
}
fn partition_heap_bytes(partition: &VectorPartitionRef) -> Result<usize> {
    partition
        .index
        .table
        .capacity()
        .checked_add(partition.index.column.capacity())
        .and_then(|n| {
            n.checked_add(
                partition.partition_key.estimated_bytes()
                    - std::mem::size_of::<contextdb_core::VectorPartitionKey>(),
            )
        })
        .ok_or_else(workspace_overflow)
}

/// A restartable builder holds only control metadata, never the accumulated
/// graph. Every successful step commits real source points, sealed point pages
/// or eligible deletions before advancing its durable cursor.
pub struct PagedGenerationBuilder {
    pub partition: VectorPartitionRef,
    progress: Arc<PagedBuilderProgress>,
    policy: ResolvedVectorPolicy,
    dimension: usize,
    quantization: VectorQuantization,
    persistence: Arc<dyn PagedGenerationPersistence>,
    accountant: Arc<dyn MemoryBudget>,
    live_workspace: Arc<AtomicUsize>,
    _partition_charge: PagedRootCharge,
}
impl PagedVectorBuildEntry {
    #[doc(hidden)]
    pub fn from_vector_entry(entry: VectorEntry, quantization: VectorQuantization) -> Self {
        let source_key = canonical_source_key(entry.row_id, entry.lsn, entry.created_tx);
        let values = if quantization == VectorQuantization::F32 {
            PagedVectorValues::F32(entry.vector)
        } else {
            PagedVectorValues::Quantized(
                StoredVector::from_f32(&entry.vector, quantization).to_hnsw_u8(),
            )
        };
        Self {
            source_key,
            row_id: entry.row_id,
            lsn: entry.lsn,
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            values,
        }
    }
}

impl PagedGenerationBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn resume(
        partition: VectorPartitionRef,
        progress: PagedVectorProgress,
        policy: ResolvedVectorPolicy,
        dimension: usize,
        quantization: VectorQuantization,
        persistence: Arc<dyn PagedGenerationPersistence>,
        accountant: Arc<dyn MemoryBudget>,
        live_workspace: Arc<AtomicUsize>,
    ) -> Result<Self> {
        let partition_charge =
            PagedRootCharge::new(Arc::clone(&accountant), partition_heap_bytes(&partition)?)?;
        Self::resume_admitted(
            partition,
            progress,
            policy,
            dimension,
            quantization,
            persistence,
            accountant,
            live_workspace,
            partition_charge,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resume_for_partition(
        partition: &VectorPartitionRef,
        progress: PagedVectorProgress,
        policy: ResolvedVectorPolicy,
        dimension: usize,
        quantization: VectorQuantization,
        persistence: Arc<dyn PagedGenerationPersistence>,
        accountant: Arc<dyn MemoryBudget>,
        live_workspace: Arc<AtomicUsize>,
    ) -> Result<Self> {
        // Admit the catalog-key copy before creating it. Registry key ownership
        // is independent of this restartable builder's retained copy.
        let mut partition_charge =
            PagedRootCharge::new(Arc::clone(&accountant), partition_heap_bytes(partition)?)?;
        let partition = partition.clone();
        partition_charge.shrink(partition_heap_bytes(&partition)?);
        Self::resume_admitted(
            partition,
            progress,
            policy,
            dimension,
            quantization,
            persistence,
            accountant,
            live_workspace,
            partition_charge,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn resume_admitted(
        partition: VectorPartitionRef,
        progress: PagedVectorProgress,
        policy: ResolvedVectorPolicy,
        dimension: usize,
        quantization: VectorQuantization,
        persistence: Arc<dyn PagedGenerationPersistence>,
        accountant: Arc<dyn MemoryBudget>,
        live_workspace: Arc<AtomicUsize>,
        partition_charge: PagedRootCharge,
    ) -> Result<Self> {
        if dimension == 0
            || !graph_policy(policy, quantization).validate()
            || progress.graph.points > progress.total_points
            || progress.graph.points != progress.graph.layer_counts.iter().sum::<usize>()
            || (progress.graph.points == 0) != progress.graph.entry_point.is_none()
            || progress.encoded_points > progress.graph.points
            || (matches!(
                progress.phase,
                PagedVectorPhase::Complete | PagedVectorPhase::Published
            ) && progress.encoded_points != progress.graph.points)
            || progress.encoding_cursor.is_some_and(|id| {
                id.1 < 0
                    || id.0 as usize >= 16
                    || id.1 as usize >= progress.graph.layer_counts[id.0 as usize]
            })
            || progress.graph.seed != HNSW_BUILD_SEED
        {
            return Err(Error::Other(
                "invalid paged generation restart record".into(),
            ));
        }
        let progress_charge =
            PagedRootCharge::new(Arc::clone(&accountant), builder_progress_bytes(&progress)?)?;
        Ok(Self {
            partition,
            progress: Arc::new(PagedBuilderProgress {
                value: progress,
                _charge: progress_charge,
            }),
            policy,
            dimension,
            quantization,
            persistence,
            accountant,
            live_workspace,
            _partition_charge: partition_charge,
        })
    }
    pub fn progress(&self) -> &PagedVectorProgress {
        &self.progress
    }
    pub(crate) fn progress_snapshot(&self) -> Arc<PagedBuilderProgress> {
        Arc::clone(&self.progress)
    }
    /// Final-holder charge for the builder's partition and progress controls.
    pub fn retained_control_bytes(&self) -> usize {
        self._partition_charge.bytes + self.progress._charge.bytes
    }

    /// Perform one admitted insertion/encoding batch. Publishing and reclaiming
    /// have separate explicit calls because only the engine knows their floors.
    pub fn step(
        &mut self,
        plan: PagedVectorBatchPlan,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        match self.progress.phase {
            PagedVectorPhase::Inserting => {
                self.transact(plan, checkpoint, |builder, tx, previous, checkpoint| {
                    builder.insert_batch(tx, previous, plan, checkpoint)
                })
            }
            PagedVectorPhase::Encoding => {
                self.transact(plan, checkpoint, |builder, tx, previous, checkpoint| {
                    builder.encode_batch(tx, previous, plan, checkpoint)
                })
            }
            PagedVectorPhase::Complete
            | PagedVectorPhase::Published
            | PagedVectorPhase::Reclaimed => Ok(()),
            PagedVectorPhase::Abandoned | PagedVectorPhase::Reclaiming => Err(Error::Other(
                "paged generation cleanup requires explicit floor validation".into(),
            )),
        }
    }
    fn transact(
        &mut self,
        plan: PagedVectorBatchPlan,
        checkpoint: &mut dyn FnMut() -> Result<()>,
        work: impl FnOnce(
            &Self,
            &mut dyn PagedGenerationTransaction,
            &PagedVectorProgress,
            &mut dyn FnMut() -> Result<()>,
        ) -> Result<PagedVectorProgress>,
    ) -> Result<()> {
        plan.validate(
            graph_policy(self.policy, self.quantization),
            self.dimension,
            self.quantization,
        )?;
        for key in [
            &self.progress.last_source_key,
            &self.progress.cleanup_cursor,
        ]
        .into_iter()
        .flatten()
        {
            if key.len() > plan.source_key_bytes {
                return Err(Error::Other(
                    "paged generation cursor exceeds its admitted key window".into(),
                ));
            }
        }
        let _reservation = VectorWorkspaceReservation::try_new(
            Arc::clone(&self.accountant),
            Arc::clone(&self.live_workspace),
            plan.construction_workspace_bytes()?,
            "advance_paged_vector_generation",
            "reduce batch size or release idle serving pages",
        )?;
        let retained_bound = std::mem::size_of::<PagedBuilderProgress>()
            .checked_add(2 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(plan.progress_bytes))
            .ok_or_else(workspace_overflow)?;
        let mut next_charge = PagedRootCharge::new(Arc::clone(&self.accountant), retained_bound)?;
        checkpoint()?;
        let previous = self.progress.value.clone();
        let mut work = Some(work);
        let mut produced = None;
        let mut callback = |transaction: &mut dyn PagedGenerationTransaction| {
            let work = work
                .take()
                .ok_or_else(|| Error::Other("paged generation callback invoked twice".into()))?;
            checkpoint()?;
            let next = work(self, transaction, &previous, checkpoint)?;
            if builder_progress_bytes(&next)? > retained_bound {
                return Err(Error::Other(
                    "paged generation successor exceeds admitted controls".into(),
                ));
            }
            produced = Some(next.clone());
            Ok(next)
        };
        let next = self
            .persistence
            .commit_generation_batch(&previous, plan, &mut callback)?;
        if produced.as_ref() != Some(&next) {
            return Err(Error::Other(
                "paged generation persistence returned another progress record".into(),
            ));
        }
        next_charge.shrink(builder_progress_bytes(&next)?);
        self.progress = Arc::new(PagedBuilderProgress {
            value: next,
            _charge: next_charge,
        });
        Ok(())
    }
    fn insert_batch(
        &self,
        transaction: &mut dyn PagedGenerationTransaction,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<PagedVectorProgress> {
        let mut next = previous.clone();
        let mut workspace = PagedGraphWorkspace::new_lazy(plan.scratch_points);
        let mut writes = 0;
        for _ in 0..plan.max_points {
            checkpoint()?;
            transaction.checkpoint()?;
            let Some(entry) = transaction.source_next(previous, next.last_source_key.as_deref())?
            else {
                if !transaction.source_window_exhausted() {
                    let position = transaction.source_window_position().ok_or_else(|| {
                        Error::Other(
                            "paged generation finite source window made no progress".into(),
                        )
                    })?;
                    if position.len() > plan.source_key_bytes
                        || next
                            .last_source_key
                            .as_deref()
                            .is_some_and(|last| position <= last)
                    {
                        return Err(Error::Other(
                            "paged generation finite source position is invalid".into(),
                        ));
                    }
                    next.last_source_key = Some(position.to_vec());
                    return Ok(next);
                }
                if next.graph.points != next.total_points {
                    return Err(Error::Other(
                        "paged generation source count disagrees with its frontier".into(),
                    ));
                }
                next.phase = PagedVectorPhase::Encoding;
                return Ok(next);
            };
            if entry.source_key.len() > plan.source_key_bytes
                || entry.source_key
                    != canonical_source_key(entry.row_id, entry.lsn, entry.created_tx)
                || next.last_source_key.as_ref().is_some_and(|key| {
                    !transaction.source_entry_after_position(&entry.source_key, key)
                })
                || entry.created_tx > next.frontier_tx
                || entry.lsn > next.frontier_lsn
                || entry.deleted_tx.is_some_and(|tx| tx <= next.frontier_tx)
                || next.graph.points >= next.total_points
            {
                return Err(Error::Other(
                    "paged generation source violates the pinned frontier or key order".into(),
                ));
            }
            let mut adapter = PointAdapter {
                transaction,
                writes,
                max_writes: plan.max_node_writes,
                max_node_bytes: plan.max_node_bytes,
                dimension: self.dimension,
                quantization: self.quantization,
                accountant: &self.accountant,
                live_workspace: &self.live_workspace,
            };
            let data_id = next.graph.points;
            let policy = graph_policy(self.policy, self.quantization);
            let (point, exact_key) = match entry.values {
                PagedVectorValues::F32(values) if self.quantization == VectorQuantization::F32 => {
                    if values.len() != self.dimension || values.iter().any(|v| !v.is_finite()) {
                        return Err(Error::Other(
                            "paged generation source dimension or values mismatch".into(),
                        ));
                    }
                    let exact_key = super::f32_exact_key(&values);
                    let id = insert_paged(
                        &mut adapter,
                        policy,
                        &mut next.graph,
                        &mut workspace,
                        values,
                        data_id,
                        &hnsw_f32_distance(),
                    )
                    .map_err(build_graph_error)?;
                    (id, exact_key)
                }
                PagedVectorValues::Quantized(values)
                    if self.quantization != VectorQuantization::F32 =>
                {
                    let header =
                        crate::quantized::decode_hnsw_quantized(&values).ok_or_else(|| {
                            Error::Other("invalid paged generation source payload".into())
                        })?;
                    let payload_len = if self.quantization == VectorQuantization::SQ8 {
                        self.dimension
                    } else {
                        self.dimension.div_ceil(2)
                    };
                    if header.len != self.dimension
                        || header.payload.len() != payload_len
                        || !header.min.is_finite()
                        || !header.max.is_finite()
                        || header.min > header.max
                    {
                        return Err(Error::Other(
                            "paged generation source payload definition mismatch".into(),
                        ));
                    }
                    let exact_key = values.clone();
                    let id = insert_paged(
                        &mut adapter,
                        policy,
                        &mut next.graph,
                        &mut workspace,
                        values,
                        data_id,
                        &DistQuantizedCosine {
                            quantization: self.quantization,
                        },
                    )
                    .map_err(build_graph_error)?;
                    (id, exact_key)
                }
                _ => {
                    return Err(Error::Other(
                        "paged generation source representation mismatch".into(),
                    ));
                }
            };
            writes = adapter.writes;
            adapter.transaction.store_identity(&PagedVectorIdentity {
                point,
                data_id,
                row_id: entry.row_id,
                lsn: entry.lsn,
                created_tx: entry.created_tx,
                deleted_tx: entry.deleted_tx,
                exact_key,
            })?;
            next.last_source_key = Some(entry.source_key);
        }
        // The next cycle verifies terminal source absence before encoding;
        // completing exactly max_points never fabricates an exhausted cursor.
        Ok(next)
    }
    fn encode_batch(
        &self,
        transaction: &mut dyn PagedGenerationTransaction,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<PagedVectorProgress> {
        let mut next = previous.clone();
        let mut point = next_point(&next.graph, previous.encoding_cursor);
        for _ in 0..plan.max_points {
            let Some(id) = point else {
                next.phase = PagedVectorPhase::Complete;
                return Ok(next);
            };
            checkpoint()?;
            transaction.checkpoint()?;
            let mut adapter = PointAdapter {
                transaction,
                writes: 0,
                max_writes: plan.max_node_writes,
                max_node_bytes: plan.max_node_bytes,
                dimension: self.dimension,
                quantization: self.quantization,
                accountant: &self.accountant,
                live_workspace: &self.live_workspace,
            };
            let mut bytes = Vec::with_capacity(plan.max_node_bytes);
            match self.quantization {
                VectorQuantization::F32 => {
                    let node =
                        <PointAdapter<'_> as PagedPointStorage<f32>>::load_point(&mut adapter, id)?;
                    if node.id != id {
                        return Err(Error::Other(
                            "paged generation encoding point identity mismatch".into(),
                        ));
                    }
                    hnsw_rs::hnswio::encode_paged_point_into(
                        &node,
                        &mut FinitePointWriter {
                            bytes: &mut bytes,
                            limit: plan.max_node_bytes,
                        },
                    )
                    .map_err(|e| {
                        Error::Other(format!("paged generation point encoding failed: {e}"))
                    })?;
                }
                VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
                    let node =
                        <PointAdapter<'_> as PagedPointStorage<u8>>::load_point(&mut adapter, id)?;
                    if node.id != id {
                        return Err(Error::Other(
                            "paged generation encoding point identity mismatch".into(),
                        ));
                    }
                    hnsw_rs::hnswio::encode_paged_point_into(
                        &node,
                        &mut FinitePointWriter {
                            bytes: &mut bytes,
                            limit: plan.max_node_bytes,
                        },
                    )
                    .map_err(|e| {
                        Error::Other(format!("paged generation point encoding failed: {e}"))
                    })?;
                }
            }
            for byte in &bytes {
                next.encoding_digest =
                    (next.encoding_digest ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
            }
            next.encoding_bytes = next
                .encoding_bytes
                .checked_add(bytes.len() as u64)
                .ok_or_else(workspace_overflow)?;
            adapter.transaction.checkpoint()?;
            transaction.seal_point(id, &bytes)?;
            next.encoded_points = next
                .encoded_points
                .checked_add(1)
                .ok_or_else(workspace_overflow)?;
            next.encoding_cursor = Some(id);
            point = next_point(&next.graph, Some(id));
        }
        if point.is_none() {
            next.phase = PagedVectorPhase::Complete;
        }
        Ok(next)
    }
    /// Only a complete generation may publish. The transaction verifies the
    /// durable sealed pages and later suffix before moving the catalog pointer.
    pub fn publish(
        &mut self,
        through_lsn: Lsn,
        plan: PagedVectorBatchPlan,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if self.progress.phase != PagedVectorPhase::Complete
            || through_lsn < self.progress.frontier_lsn
        {
            return Err(Error::Other(
                "paged generation cannot publish incomplete coverage".into(),
            ));
        }
        self.transact(plan, checkpoint, |_, transaction, previous, checkpoint| {
            checkpoint()?;
            let phase = transaction.publish(previous, through_lsn)?;
            if !matches!(
                phase,
                PagedVectorPhase::Published | PagedVectorPhase::Abandoned
            ) {
                return Err(Error::Other(
                    "paged publication returned a nonterminal phase".into(),
                ));
            }
            let mut next = previous.clone();
            next.phase = phase;
            Ok(next)
        })
    }
    /// Persist an explicit abandon fence before releasing the pending slot.
    /// This path does not require a still-valid construction frontier, and
    /// admission includes only the finite progress/marker workspace.
    pub fn abandon(
        &mut self,
        plan: PagedVectorBatchPlan,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if self.progress.phase == PagedVectorPhase::Abandoned {
            return Ok(());
        }
        if !matches!(
            self.progress.phase,
            PagedVectorPhase::Inserting | PagedVectorPhase::Encoding | PagedVectorPhase::Complete
        ) {
            return Err(Error::Other(
                "only an unpublished paged build can be abandoned".into(),
            ));
        }
        let metadata = std::mem::size_of::<PagedVectorProgress>()
            .checked_add(
                self.progress
                    .last_source_key
                    .as_ref()
                    .map_or(0, Vec::capacity),
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    self.progress
                        .cleanup_cursor
                        .as_ref()
                        .map_or(0, Vec::capacity),
                )
            })
            .ok_or_else(workspace_overflow)?;
        if plan.progress_bytes < metadata {
            return Err(Error::Other(
                "abandon marker exceeds admitted progress workspace".into(),
            ));
        }
        let _reservation = VectorWorkspaceReservation::try_new(
            Arc::clone(&self.accountant),
            Arc::clone(&self.live_workspace),
            plan.progress_bytes,
            "abandon_paged_vector_generation",
            "release idle serving pages",
        )?;
        let retained_bound = std::mem::size_of::<PagedBuilderProgress>()
            .checked_add(2 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(plan.progress_bytes))
            .ok_or_else(workspace_overflow)?;
        let mut next_charge = PagedRootCharge::new(Arc::clone(&self.accountant), retained_bound)?;
        checkpoint()?;
        let previous = self.progress.value.clone();
        let mut next = previous.clone();
        next.phase = PagedVectorPhase::Abandoned;
        let committed = self
            .persistence
            .abandon_generation(&previous, &next, plan)?;
        if committed != next {
            return Err(Error::Other(
                "paged abandon returned another progress record".into(),
            ));
        }
        next_charge.shrink(builder_progress_bytes(&committed)?);
        self.progress = Arc::new(PagedBuilderProgress {
            value: committed,
            _charge: next_charge,
        });
        Ok(())
    }

    /// Reclaim a superseded or cancelled scratch generation in finite batches.
    /// A published active generation must first be retired by the engine's
    /// transaction; the engine refuses removal while any reader floor needs it.
    pub fn reclaim(
        &mut self,
        plan: PagedVectorBatchPlan,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<usize> {
        if self.progress.phase == PagedVectorPhase::Reclaimed {
            return Ok(0);
        }
        if !matches!(
            self.progress.phase,
            PagedVectorPhase::Published
                | PagedVectorPhase::Abandoned
                | PagedVectorPhase::Reclaiming
        ) {
            return Err(Error::Other(
                "unpublished paged build requires a durable abandon fence before cleanup".into(),
            ));
        }
        let mut removed = 0;
        self.transact(plan, checkpoint, |_, transaction, previous, checkpoint| {
            checkpoint()?;
            let result = transaction.reclaim(
                previous.generation_id,
                previous.cleanup_cursor.as_deref(),
                plan.max_points,
            )?;
            if result.keys_removed > plan.max_points
                || result.last_key.as_ref().is_some_and(|key| {
                    key.len() > plan.source_key_bytes
                        || previous
                            .cleanup_cursor
                            .as_ref()
                            .is_some_and(|old| key <= old)
                })
                || (!result.complete && (result.keys_removed == 0 || result.last_key.is_none()))
            {
                return Err(Error::Other(
                    "paged generation reclamation returned invalid finite progress".into(),
                ));
            }
            removed = result.keys_removed;
            let mut next = previous.clone();
            next.cleanup_cursor = result.last_key.or_else(|| previous.cleanup_cursor.clone());
            next.phase = if result.complete {
                PagedVectorPhase::Reclaimed
            } else {
                PagedVectorPhase::Reclaiming
            };
            Ok(next)
        })?;
        Ok(removed)
    }
}
fn next_point(state: &PagedGraphState, after: Option<PointId>) -> Option<PointId> {
    let (mut layer, mut rank) = after.map_or((0, 0), |id| (id.0 as usize, id.1 as usize + 1));
    while layer < state.layer_counts.len() {
        if rank < state.layer_counts[layer] {
            return i32::try_from(rank)
                .ok()
                .map(|rank| PointId(layer as u8, rank));
        }
        layer += 1;
        rank = 0;
    }
    None
}
struct FinitePointWriter<'a> {
    bytes: &'a mut Vec<u8>,
    limit: usize,
}
impl std::io::Write for FinitePointWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "point encoding exceeds its admitted page window",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Canonical fixed-width ordering used by resident seeded construction.
/// Engines may build an ordered durable source index using this exact key.
pub fn canonical_source_key(row: RowId, lsn: Lsn, created: TxId) -> Vec<u8> {
    let mut x = row.0 ^ lsn.0 ^ created.0;
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    let mut key = Vec::with_capacity(32);
    for component in [lsn.0, created.0, x, row.0] {
        key.extend_from_slice(&component.to_be_bytes());
    }
    key
}
