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
#[derive(Clone, Debug, Default)]
pub struct SearchWork {
    /// The search ran on the clustered list layout (centroids, then list
    /// pages), not on another route.
    pub clustered_route: bool,
    /// List pages (base lists and sealed runs) the search read, counting each
    /// page read once per read.
    pub list_pages_read: u64,
    /// The partition-key tuple of every Context whose list-segment pages the
    /// search read, each tuple once. An unpartitioned column reports one empty
    /// tuple.
    pub contexts_read: Vec<Vec<Value>>,
    /// Distance evaluations on quantized (int8) list records.
    pub quantized_evaluations: u64,
    /// Distance evaluations on the canonical stored representation, each
    /// stored vector compared once per comparison.
    pub full_precision_evaluations: u64,
}

impl SearchWork {
    pub fn distance_evaluations(&self) -> u64 {
        self.quantized_evaluations + self.full_precision_evaluations
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

/// What one operation charged to the reader budget for index metadata.
#[derive(Clone, Debug, Default)]
pub struct ReaderCharges {
    /// Size of the loaded centroid table object.
    pub centroid_table_bytes: u64,
    /// Every reader charge taken for the centroid table, in order.
    pub centroid_table_charges: Vec<u64>,
    /// Size of each selected Context directory object, in selection order.
    pub directory_object_bytes: Vec<u64>,
    /// Every reader charge taken for directory objects, in order.
    pub directory_object_charges: Vec<u64>,
    /// Reader bytes the operation still held after it returned.
    pub outstanding_after: u64,
}

/// Observes the reader charges that `operation` takes on the calling thread.
pub fn observe_reader_charges<T>(
    _db: &Database,
    _operation: impl FnOnce() -> T,
) -> (T, ReaderCharges) {
    unimplemented!("{UNBOUND}: reader charges for index metadata")
}
