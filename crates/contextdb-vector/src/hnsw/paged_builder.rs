//! Transactional paged vector graphs. The engine supplies authenticated point
//! pages and durable lookup directories; this module never retains an all-node
//! graph or row/exact-vector map.
use super::{DistQuantizedCosine, HNSW_BUILD_SEED, exact_key_for_stored_vector, hnsw_f32_distance};
use crate::memory_budget::{MemoryBudget, VectorWorkspaceReservation};
use crate::quantized::StoredVector;
use crate::store::{ResolvedVectorPolicy, VectorPartitionRef};
use contextdb_core::{
    Error, Lsn, Result, RowId, SnapshotId, TxId, VectorEntry, VectorQuantization,
};
use hnsw_rs::hnsw::PointId;
use hnsw_rs::paged::{
    PagedGraphError, PagedGraphPolicy, PagedGraphState, PagedGraphWorkspace, PagedPoint,
    PagedPointStorage, insert_paged, search_paged,
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
        let minimum_node = policy
            .max_layer
            .checked_mul(2 * policy.m)
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
            || self.scratch_points <= policy.ef_construction.max(2 * policy.m)
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

/// A captured-root serving reader. Every load validates checksum, generation,
/// point identity, dimensions and all referenced identities. A reader may keep
/// only admitted, evictable serving pages, never a directory of every point.
pub trait PagedVectorReader: Send {
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
}

/// All mutations go directly into the engine's one admitted transaction. Its
/// bounded dirty-page owner belongs to the batch plan; failures discard every
/// scratch point, directory update and progress change in this transaction.
pub trait PagedVectorTransaction: PagedVectorReader {
    fn store_point(&mut self, point: &PagedVectorPoint) -> Result<()>;
    fn store_identity(&mut self, identity: &PagedVectorIdentity) -> Result<()>;
}

/// Exactly one persistence entry per insertion batch. The callback runs inside
/// one storage transaction; returning an error rolls it back. Returning progress
/// must atomically commit *both* all callback writes and that exact progress.
/// The complete previous record is compared before work, rejecting stale or
/// cancelled builders. Source floors/root and declaration validity are checked
/// under the same gate. A successful return carries the durable successor.
pub trait PagedVectorPersistence: Send + Sync {
    fn commit_batch(
        &self,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        batch: &mut dyn FnMut(&mut dyn PagedVectorTransaction) -> Result<PagedVectorProgress>,
    ) -> Result<PagedVectorProgress>;
    fn reader(&self, progress: &PagedVectorProgress) -> Result<Box<dyn PagedVectorReader>>;
}

struct PointAdapter<'a> {
    transaction: &'a mut dyn PagedVectorTransaction,
    writes: usize,
    max_writes: usize,
    max_node_bytes: usize,
    dimension: usize,
    quantization: VectorQuantization,
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
}
impl PagedPointStorage<u8> for PointAdapter<'_> {
    type Error = Error;
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
}
fn graph_error(error: PagedGraphError<Error>) -> Error {
    match error {
        PagedGraphError::Storage(error) => error,
        other => Error::Other(format!("paged HNSW operation refused: {other:?}")),
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

/// A mutable tail's constant-size control handle. Nodes, row and exact
/// directories are engine-owned pages. Each append is searchable immediately
/// after the row transaction atomically publishes its successor descriptor.
/// Readers opened earlier retain the preceding root through `reader`.
pub struct PagedVectorTail {
    pub partition: VectorPartitionRef,
    progress: PagedVectorProgress,
    policy: ResolvedVectorPolicy,
    dimension: usize,
    quantization: VectorQuantization,
    persistence: Arc<dyn PagedVectorPersistence>,
    accountant: Arc<dyn MemoryBudget>,
    live_workspace: Arc<AtomicUsize>,
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
        accountant: Arc<dyn MemoryBudget>,
        live_workspace: Arc<AtomicUsize>,
    ) -> Result<Self> {
        if dimension == 0 || !graph_policy(policy, quantization).validate() {
            return Err(Error::Other("invalid paged vector tail definition".into()));
        }
        Ok(Self {
            partition,
            progress,
            policy,
            dimension,
            quantization,
            persistence,
            accountant,
            live_workspace,
        })
    }
    pub fn progress(&self) -> &PagedVectorProgress {
        &self.progress
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
    /// Inserts the supplied finite row batch. The engine's persistence callback
    /// must include its row publication in this same transaction. On failure,
    /// both the durable state and this handle retain the previous progress.
    pub fn append(&mut self, entries: &[VectorEntry], plan: PagedVectorBatchPlan) -> Result<()> {
        if entries.len() > plan.max_points {
            return Err(Error::Other(
                "paged vector append exceeds its admitted batch".into(),
            ));
        }
        let _reservation = self.reserve(plan, "append_paged_vector_tail")?;
        let previous = self.progress.clone();
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
            let mut workspace = PagedGraphWorkspace::new(plan.scratch_points);
            let mut adapter = PointAdapter {
                transaction,
                writes: 0,
                max_writes: plan.max_node_writes,
                max_node_bytes: plan.max_node_bytes,
                dimension: self.dimension,
                quantization: self.quantization,
            };
            for entry in entries {
                self.insert_entry(&mut adapter, &mut workspace, &mut next, entry)?;
            }
            produced = Some(next.clone());
            Ok(next)
        };
        let next = self.persistence.commit_batch(&previous, plan, &mut batch)?;
        if !invoked
            || next.generation_id != previous.generation_id
            || next.graph.points != previous.graph.points + entries.len()
        {
            return Err(Error::Other(
                "paged vector persistence returned an invalid successor".into(),
            ));
        }
        self.progress = next;
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
        .map_err(graph_error)?;
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

struct ReadAdapter<'a>(&'a mut dyn PagedVectorReader);
impl PagedPointStorage<f32> for ReadAdapter<'_> {
    type Error = Error;
    fn load_point(&mut self, id: PointId) -> Result<PagedPoint<f32>> {
        match self.0.load_point(id)? {
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
        self.0.checkpoint()
    }
}
impl PagedPointStorage<u8> for ReadAdapter<'_> {
    type Error = Error;
    fn load_point(&mut self, id: PointId) -> Result<PagedPoint<u8>> {
        match self.0.load_point(id)? {
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
        self.0.checkpoint()
    }
}
impl PagedVectorTail {
    /// Serving never builds, seals or loads a whole graph. The reader's page
    /// cache is engine-admitted and evictable; this call owns finite scratch.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        snapshot: SnapshotId,
        plan: PagedVectorBatchPlan,
    ) -> Result<Vec<(RowId, f32)>> {
        if query.len() != self.dimension || query.iter().any(|v| !v.is_finite()) {
            return Err(Error::Other(
                "paged vector query dimension or values mismatch".into(),
            ));
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        let _reservation = self.reserve(plan, "search_paged_vector_tail")?;
        let ef = self.policy.hnsw_ef_search.max(k);
        if ef >= plan.scratch_points {
            return Err(Error::Other(
                "paged vector search exceeds its admitted breadth".into(),
            ));
        }
        let mut reader = self.persistence.reader(&self.progress)?;
        let mut workspace = PagedGraphWorkspace::new(plan.scratch_points);
        let stored = StoredVector::from_f32(query, self.quantization);
        let neighbours = match self.quantization {
            VectorQuantization::F32 => search_paged(
                &mut ReadAdapter(reader.as_mut()),
                graph_policy(self.policy, self.quantization),
                &self.progress.graph,
                &mut workspace,
                query,
                ef,
                &hnsw_f32_distance(),
            ),
            VectorQuantization::SQ8 | VectorQuantization::SQ4 => search_paged(
                &mut ReadAdapter(reader.as_mut()),
                graph_policy(self.policy, self.quantization),
                &self.progress.graph,
                &mut workspace,
                &stored.to_hnsw_u8(),
                ef,
                &DistQuantizedCosine {
                    quantization: self.quantization,
                },
            ),
        }
        .map_err(graph_error)?;
        let mut rows = Vec::with_capacity(ef + k);
        for neighbour in neighbours {
            let identity = reader.identity(neighbour.origin_id)?;
            if identity.point != neighbour.point_id || identity.data_id != neighbour.origin_id {
                return Err(Error::Other(
                    "paged vector point and row directory disagree".into(),
                ));
            }
            if identity.created_tx <= TxId::from_snapshot(snapshot)
                && identity
                    .deleted_tx
                    .is_none_or(|tx| TxId::from_snapshot(snapshot) < tx)
            {
                let score = match self.quantization {
                    VectorQuantization::F32 => 1. - neighbour.distance,
                    VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
                        let PagedVectorPoint::Quantized(point) =
                            reader.load_point(neighbour.point_id)?
                        else {
                            return Err(Error::Other("paged graph representation mismatch".into()));
                        };
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
        }
        // Exact-vector ties use an ordered durable directory. At most k rows
        // need to be read; no population-sized exact bucket is materialized.
        if let Some((key, exact_score)) = super::exact_key_for_query(query, self.quantization) {
            let exact = reader.exact_rows(&key, snapshot, None, k)?;
            if exact.len() > k || exact.windows(2).any(|w| w[0] >= w[1]) {
                return Err(Error::Other(
                    "paged vector exact directory returned an invalid window".into(),
                ));
            }
            for row in exact {
                rows.push((row, exact_score));
            }
        }
        rows.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let mut output = Vec::with_capacity(k);
        for (row, score) in rows {
            if output.iter().any(|(id, _)| *id == row) {
                continue;
            }
            output.push((row, score));
            if output.len() == k {
                break;
            }
        }
        Ok(output)
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
    /// Persist one encoded point in the sealed change page layout, including
    /// its authenticated identity. Prior sealed pages stay durable, not resident.
    fn seal_point(&mut self, point: PointId, encoded: &[u8]) -> Result<()>;
    /// Verify the complete encoded base plus searchable suffix through this
    /// LSN, and atomically publish descriptor/coverage. Retain old reader roots.
    fn publish(&mut self, progress: &PagedVectorProgress, through_lsn: Lsn) -> Result<()>;
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
    fn commit_generation_batch(
        &self,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        batch: &mut dyn FnMut(&mut dyn PagedGenerationTransaction) -> Result<PagedVectorProgress>,
    ) -> Result<PagedVectorProgress>;
}

/// A restartable builder holds only control metadata, never the accumulated
/// graph. Every successful step commits real source points, sealed point pages
/// or eligible deletions before advancing its durable cursor.
pub struct PagedGenerationBuilder {
    pub partition: VectorPartitionRef,
    progress: PagedVectorProgress,
    policy: ResolvedVectorPolicy,
    dimension: usize,
    quantization: VectorQuantization,
    persistence: Arc<dyn PagedGenerationPersistence>,
    accountant: Arc<dyn MemoryBudget>,
    live_workspace: Arc<AtomicUsize>,
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
        Ok(Self {
            partition,
            progress,
            policy,
            dimension,
            quantization,
            persistence,
            accountant,
            live_workspace,
        })
    }
    pub fn progress(&self) -> &PagedVectorProgress {
        &self.progress
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
            PagedVectorPhase::Reclaiming => Err(Error::Other(
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
            plan.workspace_bytes()?,
            "advance_paged_vector_generation",
            "reduce batch size or release idle serving pages",
        )?;
        checkpoint()?;
        let previous = self.progress.clone();
        let mut work = Some(work);
        let mut produced = None;
        let mut callback = |transaction: &mut dyn PagedGenerationTransaction| {
            let work = work
                .take()
                .ok_or_else(|| Error::Other("paged generation callback invoked twice".into()))?;
            checkpoint()?;
            let next = work(self, transaction, &previous, checkpoint)?;
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
        self.progress = next;
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
        let mut workspace = PagedGraphWorkspace::new(plan.scratch_points);
        let mut writes = 0;
        for _ in 0..plan.max_points {
            checkpoint()?;
            transaction.checkpoint()?;
            let Some(entry) = transaction.source_next(previous, next.last_source_key.as_deref())?
            else {
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
                || next
                    .last_source_key
                    .as_ref()
                    .is_some_and(|key| *key >= entry.source_key)
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
                    .map_err(graph_error)?;
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
                    .map_err(graph_error)?;
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
            transaction.publish(previous, through_lsn)?;
            let mut next = previous.clone();
            next.phase = PagedVectorPhase::Published;
            Ok(next)
        })
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
