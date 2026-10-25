//! What these guards read from the clustered vector index.
//!
//! Every guard in this target states a user outcome and checks it through
//! public SQL plus the small observation surface below. Each observation says
//! exactly what it must report. The engine binds each one to its own
//! observation seam; a body that still panics with [`UNBOUND`] means the
//! engine does not yet expose that observation. The guard bodies in the
//! sibling modules do not change when a binding is made.
#![allow(dead_code)]

use contextdb_core::Value;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub const UNBOUND: &str = "the engine does not expose this clustered vector index observation";

/// Maintenance is bounded work; every fixture here reaches a serving index in
/// far fewer cycles than this.
pub const MAX_MAINTENANCE_CYCLES: usize = 64;

/// The clustered index declaration with every member explicit.
pub fn ivf(lists: u32, probes: u32, rerank: u32) -> String {
    format!("IVF (LISTS = {lists}, PROBES = {probes}, RERANK = {rerank})")
}

pub fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

pub fn params<const N: usize>(pairs: [(&str, Value); N]) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// A store whose maintenance runs only when the guard drives it, so every
/// build, replacement and reclamation step happens at a point the guard chose.
pub fn open_caller_driven(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the caller-driven store")
}

/// Drives finite maintenance cycles until no vector partition has build or
/// replacement work left.
pub fn drive_until_serving(db: &Database) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let report = db
            .run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle returns");
        if report.vector.remaining_partitions == 0 {
            return;
        }
    }
    panic!("vector maintenance still had work after {MAX_MAINTENANCE_CYCLES} finite cycles");
}

/// Deterministic, well-separated unit vectors: a vector's own row is its
/// unique nearest row.
pub fn unit_vector(seed: u64, dimensions: usize) -> Vec<f32> {
    let mut state = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    let mut values = Vec::with_capacity(dimensions);
    for _ in 0..dimensions {
        state ^= state >> 33;
        state = state.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        state ^= state >> 33;
        let unit = (state >> 11) as f64 / (1_u64 << 53) as f64;
        values.push((unit * 2.0 - 1.0) as f32);
    }
    let norm = values
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    values
        .into_iter()
        .map(|value| (f64::from(value) / norm) as f32)
        .collect()
}

/// Cosine distance computed in f64 from the stored f32 values: the order a
/// full-precision comparison of the stored vectors produces.
pub fn cosine_distance(left: &[f32], right: &[f32]) -> f64 {
    let dot = left
        .iter()
        .zip(right)
        .map(|(l, r)| f64::from(*l) * f64::from(*r))
        .sum::<f64>();
    let left_norm = left
        .iter()
        .map(|v| f64::from(*v) * f64::from(*v))
        .sum::<f64>()
        .sqrt();
    let right_norm = right
        .iter()
        .map(|v| f64::from(*v) * f64::from(*v))
        .sum::<f64>()
        .sqrt();
    1.0 - dot / (left_norm * right_norm)
}

pub fn int_ids(result: &QueryResult) -> Vec<i64> {
    let column = result
        .columns()
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("the search projects id");
    result
        .rows()
        .iter()
        .map(|row| match &row[column] {
            Value::Int64(id) => *id,
            other => panic!("id is an integer, got {other:?}"),
        })
        .collect()
}

pub fn count(db: &Database, table: &str) -> i64 {
    let result = db
        .execute(&format!("SELECT COUNT(*) FROM {table}"), &empty())
        .expect("count rows");
    match &result.rows()[0][0] {
        Value::Int64(count) => *count,
        other => panic!("COUNT(*) is an integer, got {other:?}"),
    }
}

pub fn little_endian_bytes(vector: &[f32]) -> Vec<u8> {
    vector
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

pub fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// The work one search did, observed at the index's own work boundaries.
///
/// Every distance computation the search performs is counted in exactly one
/// of the four evaluation counters, so their sum is its complete distance
/// work.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchWork {
    /// The search ran on the clustered list layout (centroids, then list
    /// pages), not on another route.
    pub clustered_route: bool,
    /// Query-to-centroid distance evaluations, including codebook selection.
    pub centroid_evaluations: u64,
    /// Distance evaluations on quantized (int8) list records: the int8
    /// candidates scored.
    pub quantized_evaluations: u64,
    /// Distance evaluations on the canonical stored representation read from
    /// list-generation pages (re-rank, or EXACT's comparisons).
    pub full_precision_evaluations: u64,
    /// Exact comparisons against vectors in the capped, unsealed write buffer.
    pub buffer_evaluations: u64,
    /// List pages (base lists and sealed delta runs) the search read, each
    /// read counted.
    pub list_pages_read: u64,
    /// Visibility bitmap and tombstone-journal pages the search read.
    pub visibility_pages_read: u64,
    /// Canonical-vector pages the search read to re-rank or compare.
    pub rerank_pages_read: u64,
    /// Context directory objects (catalog entries, directory chunks and
    /// manifests) the search loaded or consulted, each counted.
    pub directory_objects_read: u64,
    /// The partition-key tuple of every Context whose list-segment pages the
    /// search read, each tuple once. An unpartitioned column reports one empty
    /// tuple.
    pub contexts_read: Vec<Vec<Value>>,
    /// The terminal outcome the search's route record reports.
    pub outcome: Option<SearchOutcome>,
    /// Lists the route record reports as actually probed, summed over the
    /// searched Contexts.
    pub lists_probed: u64,
    /// The published generation the search served from, for a one-Context
    /// search on the clustered route.
    pub served_generation: Option<u64>,
    /// The approximation-quality status the route record reports.
    pub objective_status: Option<ObjectiveStatus>,
    /// The route the search's route record says it took.
    pub route: Option<SearchRoute>,
}

/// The route a vector search took.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchRoute {
    Exact,
    Indexed,
}

/// The terminal outcome of an indexed search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchOutcome {
    /// The selected route finished its declared work.
    Complete,
    /// An existing request deadline or work limit stopped it.
    RanOut,
    /// A typed admission or resource refusal.
    Refused,
}

/// Approximation-quality metadata on a COMPLETE answer, from the serving
/// generation's calibration receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectiveStatus {
    Met,
    Unmet,
    Unqualified,
    Pending,
}

impl SearchWork {
    /// The complete distance work: every centroid, int8, canonical and
    /// buffer comparison.
    pub fn distance_evaluations(&self) -> u64 {
        self.centroid_evaluations
            + self.quantized_evaluations
            + self.full_precision_evaluations
            + self.buffer_evaluations
    }

    /// Comparisons made at full precision, from list generations or from the
    /// buffer.
    pub fn full_precision_comparisons(&self) -> u64 {
        self.full_precision_evaluations + self.buffer_evaluations
    }
}

/// Observes the clustered index work done by the searches `operation` runs on
/// the calling thread.
pub fn observe_search<T>(_db: &Database, _operation: impl FnOnce() -> T) -> (T, SearchWork) {
    unimplemented!("{UNBOUND}: per-search work")
}

/// The bounded in-memory write buffer of one indexed column.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteBuffer {
    /// The declared byte threshold at which the buffer seals a list run.
    pub cap_bytes: u64,
    /// The most bytes the buffer has held since this store was opened.
    pub high_water_bytes: u64,
    /// Bounded list runs the buffer has sealed since this store was opened.
    pub sealed_runs: u64,
}

/// Opens a caller-driven store whose clustered write buffers seal at
/// `cap_bytes` instead of the product threshold.
pub fn open_with_write_buffer_cap(_path: &Path, _cap_bytes: u64) -> Database {
    unimplemented!("{UNBOUND}: a declared write-buffer threshold")
}

pub fn write_buffer(_db: &Database, _table: &str, _column: &str) -> WriteBuffer {
    unimplemented!("{UNBOUND}: write-buffer state")
}

/// The published generation that searches of one Context read. Each Context
/// (one partition-key tuple) has its own generations; an unpartitioned column
/// has one, named by the empty tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Serving {
    pub generation: u64,
    pub lists: u64,
    /// Identity of the generation's trained centroid codebook.
    pub codebook: u64,
}

pub fn serving(_db: &Database, _table: &str, _column: &str, _context: &[Value]) -> Option<Serving> {
    unimplemented!("{UNBOUND}: the serving generation")
}

/// Every generation of one Context whose pages are still physically held:
/// the serving one, plus any a reader or a pending reclamation keeps.
pub fn retained_generations(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> Vec<u64> {
    unimplemented!("{UNBOUND}: retained generations")
}

/// Opens a caller-driven store whose maintenance advances a clustered build
/// or replacement by at most `rows_per_cycle` source rows per cycle, so a
/// build spans many cycles.
pub fn open_with_build_batch_rows(_path: &Path, _rows_per_cycle: u64) -> Database {
    unimplemented!("{UNBOUND}: a bounded build batch")
}

/// The build or replacement work of one Context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildProgress {
    /// A build or replacement has started and not yet published.
    pub in_flight: bool,
    /// Builds abandoned before publication since this store was opened.
    pub aborted: u64,
    /// The class of persisted build object the build's next maintenance step
    /// writes, when a build is due or in flight.
    pub next_write: Option<BuildWriteClass>,
    /// A replacement fell due while another switch was still remapping and
    /// waits to start; inspection shows it with its trigger.
    pub replacement_pending: bool,
    /// What made the pending replacement due.
    pub pending_trigger: Option<ReplacementTrigger>,
}

/// What made a replacement due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplacementTrigger {
    ChangePercent,
    TombstonePercent,
    PolicyRevision,
    Requalification,
}

/// The persisted object classes a build or replacement writes before it
/// publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildWriteClass {
    TrainingReservoir,
    BuildAccumulator,
    UnpublishedRun,
    PreparedReplacement,
}

/// One persisted build write, observed at the engine's own disk admission and
/// write boundaries.
#[derive(Clone, Debug, PartialEq)]
pub enum BuildWriteEvent {
    /// The disk admission decision for one write operation of `class`, taken
    /// before any byte of that operation may reach the store.
    Admission {
        /// Identifies this write operation; its executions carry the same id.
        operation: u64,
        class: BuildWriteClass,
        context: Vec<Value>,
        /// Bytes the write asks the store for.
        requested_bytes: u64,
        /// The smallest store growth with which any valid layout, including a
        /// smaller retry, could satisfy this write after reusing every free
        /// block it is allowed to reuse.
        minimum_growth_bytes: u64,
        /// The store's size when the decision was taken.
        current_store_bytes: u64,
        /// The store's size if the write is admitted with the layout chosen.
        projected_store_bytes: u64,
        /// The declared disk ceiling at the decision, if any.
        ceiling: Option<u64>,
        admitted: bool,
    },
    /// Bytes of a persisted build write of `class` actually reached the
    /// store. Every execution is reported, whether or not an admission
    /// admitted it.
    Written {
        /// The admission operation this execution ran under; `None` when it
        /// ran under no admission decision at all.
        operation: Option<u64>,
        class: BuildWriteClass,
        context: Vec<Value>,
        bytes: u64,
    },
}

/// Calls `observer` for every persisted build write admission decision and
/// every actual build write execution that `operation` performs on the
/// calling thread, in the order they happen.
pub fn observe_build_writes<T>(
    _observer: Arc<dyn Fn(&BuildWriteEvent) + Send + Sync>,
    _operation: impl FnOnce() -> T,
) -> T {
    unimplemented!("{UNBOUND}: persisted build write admission and execution")
}

pub fn build_progress(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> BuildProgress {
    unimplemented!("{UNBOUND}: build progress")
}

/// Durability boundaries of publishing a newly built generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationCut {
    /// The new generation's pages are durable; the manifest still names the
    /// old generation.
    BeforeGenerationPublication,
    /// The manifest naming the new generation is durable; nothing after it
    /// (reclamation of the old generation) has run.
    AfterGenerationPublication,
    /// Inside one remap batch of the legacy entries, after its work is written
    /// and before it commits.
    DuringRemapBatch,
}

/// Calls `observer` at each publication boundary that `operation` reaches on
/// the calling thread, before continuing past it.
pub fn observe_publication_cuts<T>(
    _observer: Arc<dyn Fn(PublicationCut) + Send + Sync>,
    _operation: impl FnOnce() -> T,
) -> T {
    unimplemented!("{UNBOUND}: generation publication boundaries")
}

/// Where a physical index page or object lives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PhysicalPlace {
    ListPage {
        generation: u64,
    },
    SealedRun {
        generation: u64,
    },
    WriteBuffer,
    PreparedReplacement,
    /// Vectors sampled to train an in-flight build's centroids.
    TrainingReservoir,
    /// Partial sums or assignments an in-flight build has persisted.
    BuildAccumulator,
    /// List runs an in-flight build has written but not published.
    UnpublishedRun,
    Other(String),
}

impl PhysicalPlace {
    pub fn belongs_to_an_unpublished_build(&self) -> bool {
        matches!(
            self,
            Self::TrainingReservoir | Self::BuildAccumulator | Self::UnpublishedRun
        )
    }

    /// Any persisted state of a build that has not published, including a
    /// prepared replacement.
    pub fn is_unpublished_build_state(&self) -> bool {
        self.belongs_to_an_unpublished_build() || matches!(self, Self::PreparedReplacement)
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalPage {
    pub place: PhysicalPlace,
    pub bytes: Vec<u8>,
}

/// The raw bytes of every page and object that holds index records or build
/// state for one column, across every Context: base list pages and sealed
/// runs of every retained generation, the in-memory write buffer, any
/// prepared replacement, and every in-flight build's persisted training
/// reservoir, accumulators and unpublished list runs.
pub fn physical_pages(_db: &Database, _table: &str, _column: &str) -> Vec<PhysicalPage> {
    unimplemented!("{UNBOUND}: a physical page scan")
}

/// The exact byte sequence a list record stores for `vector`'s quantized
/// representation on an unpartitioned column: under the serving generation's
/// quantizer, or under an in-flight build's trained quantizer when nothing is
/// serving yet. Empty while no quantizer has been trained.
pub fn quantized_record(_db: &Database, _table: &str, _column: &str, _vector: &[f32]) -> Vec<u8> {
    unimplemented!("{UNBOUND}: the quantized record encoding")
}

/// The size of one list page of one indexed column.
pub fn list_page_bytes(_db: &Database, _table: &str, _column: &str) -> u64 {
    unimplemented!("{UNBOUND}: the list page size")
}

/// A kind of index metadata object a search loads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataObjectKind {
    /// One Context's centroid codebook (with its active bitmap and quantizer).
    Codebook,
    /// One bounded chunk of a Context directory.
    DirectoryChunk,
    /// One generation manifest.
    Manifest,
}

/// One index metadata object an operation loaded, with every reader charge
/// taken for it.
#[derive(Clone, Debug)]
pub struct MetadataObject {
    pub kind: MetadataObjectKind,
    /// The Context (partition-key tuple) the object belongs to.
    pub context: Vec<Value>,
    /// The object's complete decoded size.
    pub object_bytes: u64,
    /// Every reader charge taken for this object, in order.
    pub charges: Vec<u64>,
    /// Bytes still charged for this object once the operation returned.
    pub outstanding_after: u64,
}

/// What one operation charged to the reader budget for index metadata.
#[derive(Clone, Debug, Default)]
pub struct ReaderCharges {
    /// Every metadata object the operation loaded, in load order.
    pub objects: Vec<MetadataObject>,
    /// The directory window bound K the operation was admitted with.
    pub directory_window: u64,
    /// The most directory chunks the operation held resident at once.
    pub peak_resident_directory_chunks: u64,
}

/// Observes the reader charges that `operation` takes on the calling thread.
pub fn observe_reader_charges<T>(
    _db: &Database,
    _operation: impl FnOnce() -> T,
) -> (T, ReaderCharges) {
    unimplemented!("{UNBOUND}: reader charges for index metadata")
}

/// Points inside the clustered index's own work where a cancellation is
/// driven: the path checks its cancellation immediately after the observer
/// returns from the point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexCancelPoint {
    /// A search has admitted one list page and is about to read its records.
    ListPageRead,
    /// A search is scoring candidates or re-ranking them.
    CandidateScoring,
    /// Maintenance is inside one bounded build, replacement or delta replay
    /// batch, before that batch commits its progress.
    BuildBatch,
    /// Maintenance is inside one bounded compaction or reclamation batch of a
    /// retired generation, before that batch commits its progress.
    ReclaimBatch,
}

/// Calls `observer` each time `operation`, on the calling thread, reaches one
/// of the index's cancellation points.
pub fn observe_index_cancel_points<T>(
    _observer: Arc<dyn Fn(IndexCancelPoint) + Send + Sync>,
    _operation: impl FnOnce() -> T,
) -> T {
    unimplemented!("{UNBOUND}: index cancellation points")
}

/// Runs one caller-driven maintenance cycle that stops, with the engine's
/// existing typed cancellation, at its next checkpoint once `cancellation`
/// is cancelled.
pub fn run_maintenance_cycle_cancellable(
    _db: &Database,
    _cancellation: &contextdb_core::read_contract::OwnerReadCancellation,
) -> contextdb_core::Result<()> {
    unimplemented!("{UNBOUND}: a cancellable maintenance cycle")
}

/// Bytes currently held by the index's operation reservations of every kind:
/// query workspaces, seal workspaces, maintenance step workspaces, and the
/// whole-object metadata they admitted.
pub fn outstanding_index_reservations(_db: &Database) -> u64 {
    unimplemented!("{UNBOUND}: outstanding index reservations")
}

/// The typed error that last stopped one Context's build or replacement,
/// kept until that build next makes progress. `None` when nothing stopped it.
pub fn last_build_refusal(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> Option<contextdb_core::Error> {
    unimplemented!("{UNBOUND}: the last build refusal")
}

/// While it lives, writes of the named build class are never satisfied from
/// reusable free space: each one can only be placed by growing the store.
/// Every other write reuses free space as usual.
pub struct ReusableSpaceWithheld {
    _private: (),
}

pub fn withhold_reusable_space_from(
    _db: &Database,
    _class: BuildWriteClass,
) -> ReusableSpaceWithheld {
    unimplemented!("{UNBOUND}: withholding reusable space from one build class")
}

/// A targeted write at its disk admission boundary, described before the
/// admission decides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionBoundary {
    /// The write operation about to be decided; its `Admission` event and
    /// any execution carry the same id.
    pub operation: u64,
    pub class: BuildWriteClass,
    /// The store's size at the boundary.
    pub current_store_bytes: u64,
    /// The smallest store growth with which any valid layout, including a
    /// smaller retry, could satisfy this write after reusing every free block
    /// it is allowed to reuse, computed by the allocator's own layout rules
    /// before the decision.
    pub minimum_growth_bytes: u64,
}

/// While it lives, the first write of the named class to reach its disk
/// admission calls the chooser with that write's boundary; the ceiling the
/// chooser returns is declared through the ordinary disk-limit path, and then
/// the real admission decides the write against it.
pub struct CeilingAtAdmission {
    _private: (),
}

pub fn declare_ceiling_at_next_admission(
    _db: &Database,
    _class: BuildWriteClass,
    _choose: Box<dyn FnOnce(&AdmissionBoundary) -> u64 + Send>,
) -> CeilingAtAdmission {
    unimplemented!("{UNBOUND}: declaring a ceiling at a write's admission boundary")
}

/// Store-wide test settings for the clustered index, all optional.
#[derive(Clone, Copy, Debug, Default)]
pub struct IndexTestOptions {
    /// The byte threshold at which a write buffer seals a run.
    pub write_buffer_cap_bytes: Option<u64>,
    /// The most source rows or entries any one build, replacement,
    /// assignment or remap step handles per maintenance cycle.
    pub build_batch_rows: Option<u64>,
}

/// Opens a caller-driven store with the given clustered index test settings.
pub fn open_with_index_test_options(_path: &Path, _options: IndexTestOptions) -> Database {
    unimplemented!("{UNBOUND}: store-wide clustered index test settings")
}

/// One sealed delta run of one Context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaRun {
    /// The codebook the run was sealed under and is searched through; `None`
    /// for an unclustered run sealed before the Context's first codebook.
    pub codebook: Option<u64>,
    /// Vectors in the run.
    pub entries: u64,
}

/// Every sealed delta run of one Context, oldest first.
pub fn delta_runs(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> Vec<DeltaRun> {
    unimplemented!("{UNBOUND}: sealed delta runs")
}

/// Progress of assigning or remapping one Context's sealed runs to its
/// serving codebook.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RemapProgress {
    /// Run entries not yet assigned to the serving codebook.
    pub entries_remaining: u64,
    /// The persisted position the next remap step resumes from; `None` when
    /// no remap is pending.
    pub cursor: Option<u64>,
}

pub fn remap_progress(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> RemapProgress {
    unimplemented!("{UNBOUND}: remap progress")
}

/// How one calibrated depth of a serving generation qualified.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DepthQualification {
    /// A depth from the CALIBRATION_DEPTHS policy.
    pub depth: u32,
    /// This depth's own shortlist: ceil(depth x the generation's refine
    /// factor), at least `depth`.
    pub shortlist: u64,
    /// Candidates this depth's own run actually rescored at full precision,
    /// persisted with the result: its shortlist, or fewer when fewer were
    /// eligible.
    pub effective_candidates: u64,
    /// Sample queries calibrated at this depth.
    pub sample_queries: u32,
    /// Sample queries whose answer at this depth missed the target.
    pub queries_below: u32,
    /// This depth's own run met the target on every sample query, below
    /// EXACT's work.
    pub qualified: bool,
}

/// Where a generation's calibration samples came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CalibrationBasis {
    /// Held-out stored vectors of the partition.
    StoredVectors,
    /// A declared CALIBRATION_QUERIES source, pinned at its identity and
    /// commit frontier when calibration read it.
    Queries {
        source_identity: u64,
        source_frontier: u64,
    },
}

/// The qualification receipt of one Context's serving generation.
#[derive(Clone, Debug, PartialEq)]
pub struct CalibrationReceipt {
    pub generation: u64,
    pub probes: u64,
    pub refine_factor: f64,
    pub basis: CalibrationBasis,
    /// Every calibrated depth, in ascending order.
    pub depths: Vec<DepthQualification>,
}

pub fn calibration_receipt(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> Option<CalibrationReceipt> {
    unimplemented!("{UNBOUND}: the calibration receipt")
}

/// The Context's serving generation must be requalified (its target or its
/// calibration-query source changed since it was qualified).
pub fn requalification_pending(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> bool {
    unimplemented!("{UNBOUND}: requalification pending")
}

/// While it lives, the next calibration of the named Context computes, for
/// every sample query and every calibrated depth, the exact top-`depth`
/// ranking as usual, runs that depth's own production search, and then
/// scores, in place of that run's answer, the identities that
/// `answer(query, depth, exact)` returns. `exact` holds the exact top-`depth`
/// canonical row identities. It is called once for each (sample query,
/// calibrated depth) the calibration actually runs.
pub struct CalibrationAnswersRewritten {
    _private: (),
}

pub fn rewrite_next_calibration_answers(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
    _answer: Box<dyn Fn(usize, u32, &[u64]) -> Vec<u64> + Send + Sync>,
) -> CalibrationAnswersRewritten {
    unimplemented!("{UNBOUND}: rewriting calibration answers")
}

/// Installs on the Context's serving generation a qualification receipt
/// stating that every listed depth qualified, with the given mean complete
/// work for the indexed route and for EXACT, as if calibration had measured
/// them. The receipt keeps the generation's calibration basis; nothing else
/// about the generation changes.
pub fn plant_qualification(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
    _depths: &[u32],
    _indexed_mean_work: u64,
    _exact_mean_work: u64,
) {
    unimplemented!("{UNBOUND}: planting a qualification")
}

/// Declares `source_table.source_column` as the CALIBRATION_QUERIES source of
/// the indexed column, through the column's policy.
pub fn declare_calibration_queries(
    _db: &Database,
    _table: &str,
    _column: &str,
    _source_table: &str,
    _source_column: &str,
) {
    unimplemented!("{UNBOUND}: declaring a calibration-query source")
}

/// The durable replacement-switch state of one Context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwitchPhase {
    /// One generation serves; any replacement is unpublished.
    Serving,
    /// A replacement has published; legacy entries sealed under the old
    /// codebook are being remapped.
    Remapping,
}

/// The legacy entries fixed at a replacement's publication commit: every
/// delta entry with commit LSN in (`after_lsn`, `through_lsn`], that is
/// (F0, F1].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LegacyEntries {
    /// F0: the replacement base covers source versions at or below it.
    pub after_lsn: u64,
    /// F1: the publication commit.
    pub through_lsn: u64,
    /// How many entries the legacy set holds.
    pub entries: u64,
    /// An immutable digest of the legacy set's (row identity, version)
    /// membership, fixed at the publication commit.
    pub identity: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SwitchState {
    pub phase: SwitchPhase,
    /// The legacy set recorded at the publication commit; `None` outside the
    /// remap phase.
    pub legacy: Option<LegacyEntries>,
    /// Legacy entries remapped so far, counted per entry.
    pub legacy_entries_remapped: u64,
    /// The retained old codebook while the remap runs.
    pub retained_codebook: Option<u64>,
    /// Bytes charged for the retained old codebook.
    pub retained_codebook_charge_bytes: u64,
}

/// One delta entry of one Context.
#[derive(Clone, Debug, PartialEq)]
pub struct DeltaEntry {
    /// The primary key of the row whose version the entry holds.
    pub primary_key: Value,
    /// The commit LSN of the row version the entry holds.
    pub commit_lsn: u64,
    /// The codebook the entry is searched through; `None` while unsealed in
    /// the write buffer or in an unclustered run.
    pub codebook: Option<u64>,
    /// The entry has been sealed out of the write buffer.
    pub sealed: bool,
}

/// Every delta entry of one Context, in commit order.
pub fn delta_entries(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> Vec<DeltaEntry> {
    unimplemented!("{UNBOUND}: delta entries")
}

pub fn switch_state(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> SwitchState {
    unimplemented!("{UNBOUND}: the replacement switch state")
}

/// Legacy entries one Context has prepared for remap into its serving
/// codebook's pages since the Context was created, counted once per entry as
/// each bounded preparation step commits. Persisted, and carried across every
/// switch phase transition; it counts preparation work, not the moment a
/// prepared run's searchable root switches.
pub fn legacy_entries_prepared_total(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> u64 {
    unimplemented!("{UNBOUND}: cumulative legacy remap preparation")
}

/// The Context's rows ordered by the serving generation's own quantized score
/// for `query`, best first, as primary keys: the order a shortlist is cut
/// from.
pub fn quantized_order(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
    _query: &[f32],
) -> Vec<Value> {
    unimplemented!("{UNBOUND}: the quantized candidate order")
}

/// While it lives, the next calibration of the named Context evaluates only
/// this (PROBES, REFINE_FACTOR) point, PROBES clamped to the active list
/// count: every calibrated depth runs its own search with its own shortlist
/// at this point, each depth is qualified or not from its own runs, and the
/// selector does not move to another point. The generation then serves at
/// this point.
pub struct CalibrationSettingPlanted {
    _private: (),
}

pub fn plant_calibration_setting(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
    _probes: u64,
    _refine_factor: f64,
) -> CalibrationSettingPlanted {
    unimplemented!("{UNBOUND}: planting a calibration setting")
}

/// The quantization step of each dimension of the serving generation's
/// codec: the full-precision width one code step spans.
pub fn quantizer_steps(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
) -> Vec<f32> {
    unimplemented!("{UNBOUND}: the codec's quantization steps")
}

/// The per-dimension codes the serving generation's codec writes for
/// `vector` in the list it is assigned to.
pub fn quantized_codes(
    _db: &Database,
    _table: &str,
    _column: &str,
    _context: &[Value],
    _vector: &[f32],
) -> Vec<i32> {
    unimplemented!("{UNBOUND}: the codec's codes for a vector")
}
