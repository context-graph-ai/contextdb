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
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, QueryResult, ReadSession,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// SQL execution shared by writable stores and bounded read-only sessions.
pub trait QuerySession {
    fn verification_window(&self) -> u64;
    fn execute(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> contextdb_core::Result<QueryResult>;
}

impl QuerySession for Database {
    fn verification_window(&self) -> u64 {
        self.native_ivf_verification_window_for_test()
            .expect("actual writer verification window")
    }
    fn execute(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> contextdb_core::Result<QueryResult> {
        Database::execute(self, sql, params)
    }
}

impl QuerySession for ReadSession {
    fn verification_window(&self) -> u64 {
        self.native_ivf_verification_window_for_test()
            .expect("actual read-session verification window")
    }
    fn execute(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> contextdb_core::Result<QueryResult> {
        ReadSession::execute(self, sql, params)
    }
}

pub const UNBOUND: &str = "the engine does not expose this clustered vector index observation";

/// A cycle budget from this fixture's population, lower bound on rows per
/// batch, and training passes. Ingestion, each training pass, assignment,
/// and seeding each need at most ceil(rows / batch) cycles. Three more cover
/// the exhausted-source probe, ingestion completion, and publication/setup.
/// An additional cycle per row allows post-publication work to drain sealed
/// runs, allowing one cycle for each inserted row.
/// When the fixture leaves the workspace-derived batch unspecified, one is a
/// conservative lower bound; explicit batch policies are passed as declared.
pub const fn maintenance_cycles(rows: usize, batch: usize, training: usize) -> usize {
    rows.div_ceil(batch) * (training + 3) + rows + 3
}

/// The clustered index declaration with every member explicit; the refine
/// factor multiplies LIMIT to give the candidates rescored at full precision.
pub fn ivf(lists: u32, probes: u32, refine_factor: u32) -> String {
    format!("IVF (LISTS = {lists}, PROBES = {probes}, REFINE_FACTOR = {refine_factor}.00)")
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
    let db = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the caller-driven store");
    watch_real_buffers(&db);
    watch_verifications();
    db
}

/// Drives finite maintenance cycles until no vector partition has build or
/// replacement work left.
pub fn drive_until_serving(db: &Database, rows: usize, batch: usize, training: usize) {
    let cycles = maintenance_cycles(rows, batch, training);
    for _ in 0..cycles {
        let report = db
            .run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle returns");
        if report.vector.remaining_partitions == 0 {
            return;
        }
    }
    panic!("vector maintenance still had work after {cycles} finite cycles");
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
    /// The calibrated depth whose (PROBES, REFINE_FACTOR) pair and shortlist
    /// served the search, as the route record reports it: the smallest
    /// calibrated depth at least LIMIT. None when the search took EXACT or no
    /// calibrated depth is at least LIMIT.
    pub served_depth: Option<u32>,
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
pub fn observe_search<T>(_db: &dyn QuerySession, operation: impl FnOnce() -> T) -> (T, SearchWork) {
    observe_native_search(operation)
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
pub fn open_with_write_buffer_cap(path: &Path, cap_bytes: u64) -> Database {
    open_with_index_test_options(
        path,
        IndexTestOptions {
            write_buffer_cap_bytes: Some(cap_bytes),
            ..Default::default()
        },
    )
}

thread_local! {
    static BUFFER_TOTALS: std::cell::RefCell<std::collections::BTreeMap<(usize,String,String),WriteBuffer>> = const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
}
fn watch_real_buffers(db: &Database) {
    let owner = db
        .native_ivf_original_owner_for_test()
        .expect("real writer owner");
    BUFFER_TOTALS.with(|totals| totals.borrow_mut().retain(|(held, _, _), _| *held != owner));
    contextdb_engine::vector_ivf_observations::set_native_ivf_buffer_observer_for_test(Arc::new(
        |event| {
            BUFFER_TOTALS.with(|totals| {
                let mut totals = totals.borrow_mut();
                assert!(
                    totals.len() < 512,
                    "bounded host buffer observation overflow"
                );
                let total = totals
                    .entry((
                        event.original_owner,
                        event.index.table.clone(),
                        event.index.column.clone(),
                    ))
                    .or_default();
                total.cap_bytes = event.state.cap_bytes as u64;
                total.high_water_bytes = total
                    .high_water_bytes
                    .max(event.state.high_water_bytes as u64);
                total.sealed_runs = total
                    .sealed_runs
                    .checked_add(event.state.sealed_runs)
                    .expect("actual seal counter overflow");
            });
        },
    ));
}
pub fn write_buffer(db: &Database, table: &str, column: &str) -> WriteBuffer {
    let owner = db
        .native_ivf_original_owner_for_test()
        .expect("real writer owner");
    let observed = || {
        BUFFER_TOTALS.with(|totals| {
            totals
                .borrow()
                .get(&(owner, table.into(), column.into()))
                .copied()
        })
    };
    if let Some(buffer) = observed() {
        return buffer;
    }
    // Reopen has made no writer loan yet. Inspect the real empty bounded
    // writer through the ordinary abort-only factory, rather than requiring a
    // prior SQL write or inventing its cap and counters.
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::unpartitioned();
    let resolved = db
        .native_ivf_resolved_policy_for_test(&index, &tuple)
        .expect("real reopened writer policy");
    db.with_aborting_native_ivf_write_for_test(
        &index,
        &tuple,
        contextdb_engine::vector_ivf_observations::NativeIvfDriveCallbacks {
            checkpoint: &|| Ok(()),
            before_distance: &|_| Ok(()),
        },
        |scope| scope.with_clustered_writer(resolved.policy, resolved.training, |_| Ok(())),
    )
    .expect("observe the actual reopened bounded writer");
    observed().expect("real native writer construction emitted its buffer")
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

pub fn serving(db: &Database, table: &str, column: &str, context: &[Value]) -> Option<Serving> {
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).expect("typed tuple");
    db.native_ivf_serving_descriptor_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &tuple,
    )
    .expect("inspect real native descriptor")
    .filter(|d| d.manifest.active_lists != 0)
    .map(|d| Serving {
        generation: d.manifest.generation,
        lists: u64::from(d.manifest.lists),
        codebook: u64::from_le_bytes(d.manifest.codebook[..8].try_into().unwrap()),
    })
}

/// Every generation of one Context whose pages are still physically held:
/// the serving one, plus any a reader or a pending reclamation keeps.
pub fn retained_generations(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> Vec<u64> {
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    let mut generations = Vec::new();
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        scope.visit_physical_pages(&mut |page| {
            if page.key.generation != 0 {
                generations.push(page.key.generation);
            }
        })
    })
    .expect("scan actual retained native families");
    generations.sort_unstable();
    generations.dedup();
    generations
}

/// Opens a caller-driven store whose maintenance advances a clustered build
/// or replacement by at most `rows_per_cycle` source rows per cycle, so a
/// build spans many cycles.
pub fn open_with_build_batch_rows(path: &Path, rows_per_cycle: u64) -> Database {
    open_with_index_test_options(
        path,
        IndexTestOptions {
            build_batch_rows: Some(rows_per_cycle),
            ..Default::default()
        },
    )
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
    observer: Arc<dyn Fn(&BuildWriteEvent) + Send + Sync>,
    operation: impl FnOnce() -> T,
) -> T {
    use contextdb_engine::vector_ivf_observations::*;
    observe_index_cancel_points(Arc::new(|_| {}), || {
        with_native_ivf_disk_observer_for_test(
            Arc::new(move |event| match event {
                NativeIvfDiskEventForTest::Admission {
                    scope,
                    boundary,
                    requested_bytes,
                    projected_store_bytes,
                    ceiling,
                    admitted,
                } => {
                    observer(&BuildWriteEvent::Admission {
                        operation: boundary.operation,
                        class: observed_build_class(boundary.class),
                        context: observed_disk_context(scope.tuple),
                        requested_bytes,
                        minimum_growth_bytes: boundary.minimum_growth_bytes,
                        current_store_bytes: boundary.current_store_bytes,
                        projected_store_bytes,
                        ceiling,
                        admitted,
                    });
                }
                NativeIvfDiskEventForTest::Written {
                    physical_owner,
                    scope,
                    operation,
                    class,
                    bytes,
                } => {
                    LAST_NATIVE_DISK_REFUSALS.with(|records| {
                        records.borrow_mut().retain(|(owner, index, tuple, _)| {
                            *owner != physical_owner || index != scope.index || tuple != scope.tuple
                        })
                    });
                    observer(&BuildWriteEvent::Written {
                        operation: Some(operation),
                        class: observed_build_class(class),
                        context: observed_disk_context(scope.tuple),
                        bytes,
                    });
                }
                NativeIvfDiskEventForTest::Refused {
                    physical_owner,
                    index,
                    tuple,
                    error,
                } => {
                    let error = copy_native_disk_refusal(error);
                    LAST_NATIVE_DISK_REFUSALS.with(|records| {
                        let mut records = records.borrow_mut();
                        records.retain(|(owner, i, t, _)| {
                            *owner != physical_owner || i != index || t != tuple
                        });
                        records.push((physical_owner, index.clone(), tuple.clone(), error));
                    });
                }
            }),
            operation,
        )
    })
}

thread_local! {
    static LAST_NATIVE_DISK_REFUSALS: std::cell::RefCell<Vec<(usize,contextdb_core::VectorIndexRef,contextdb_core::VectorPartitionKey,contextdb_core::Error)>>=const{std::cell::RefCell::new(Vec::new())};
}
fn copy_native_disk_refusal(error: &contextdb_core::Error) -> contextdb_core::Error {
    match error {
        contextdb_core::Error::DiskBudgetExceeded {
            operation,
            current_bytes,
            budget_limit_bytes,
            hint,
        } => contextdb_core::Error::DiskBudgetExceeded {
            operation: operation.clone(),
            current_bytes: *current_bytes,
            budget_limit_bytes: *budget_limit_bytes,
            hint: hint.clone(),
        },
        other => panic!("native build stopped with a non-disk error: {other:?}"),
    }
}
fn observed_disk_context(tuple: &contextdb_core::VectorPartitionKey) -> Vec<Value> {
    use contextdb_core::VectorPartitionComponent as C;
    tuple
        .components()
        .iter()
        .map(|c| match c {
            C::Uuid(v) => Value::Uuid(*v),
            C::Text(v) => Value::Text(v.clone()),
            C::Integer(v) => Value::Int64(*v),
            C::Boolean(v) => Value::Bool(*v),
            C::Timestamp(v) => Value::Timestamp(*v),
            C::TxId(v) => Value::TxId(*v),
        })
        .collect()
}
fn observed_build_class(
    class: contextdb_engine::vector_ivf_observations::NativeIvfBuildWriteClassForTest,
) -> BuildWriteClass {
    use contextdb_engine::vector_ivf_observations::NativeIvfBuildWriteClassForTest as C;
    match class {
        C::TrainingReservoir => BuildWriteClass::TrainingReservoir,
        C::BuildAccumulator => BuildWriteClass::BuildAccumulator,
        C::UnpublishedRun => BuildWriteClass::UnpublishedRun,
        C::PreparedReplacement => BuildWriteClass::PreparedReplacement,
    }
}
fn native_build_class(
    class: BuildWriteClass,
) -> contextdb_engine::vector_ivf_observations::NativeIvfBuildWriteClassForTest {
    use contextdb_engine::vector_ivf_observations::NativeIvfBuildWriteClassForTest as C;
    match class {
        BuildWriteClass::TrainingReservoir => C::TrainingReservoir,
        BuildWriteClass::BuildAccumulator => C::BuildAccumulator,
        BuildWriteClass::UnpublishedRun => C::UnpublishedRun,
        BuildWriteClass::PreparedReplacement => C::PreparedReplacement,
    }
}

pub fn build_progress(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> BuildProgress {
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).expect("typed tuple");
    let progress = db
        .native_ivf_build_progress_for_test(
            &contextdb_core::VectorIndexRef::new(table, column),
            &tuple,
        )
        .expect("inspect actual native build");
    BuildProgress {
        aborted: progress.aborted_builds,
        in_flight: progress
            .build
            .is_some_and(|b| b.phase != contextdb_vector::ivf::storage::BuildPhase::Ready),
        next_write: progress.build.and_then(|build| {
            use contextdb_vector::ivf::storage::BuildPhase as P;
            match build.phase {
                P::Ingest if progress.ingested_vectors < progress.frozen_live_entries => {
                    Some(BuildWriteClass::TrainingReservoir)
                }
                P::Ingest | P::Ready => None,
                P::Seed | P::Train => Some(BuildWriteClass::BuildAccumulator),
                P::Assign
                    if build
                        .cursor
                        .saturating_add(build.training.phase_batch_entries as u64)
                        < build.manifest.slots =>
                {
                    Some(BuildWriteClass::UnpublishedRun)
                }
                P::Assign | P::Publish => Some(BuildWriteClass::PreparedReplacement),
            }
        }),
        replacement_pending: progress.replacement_trigger.is_some(),
        pending_trigger: progress.replacement_trigger.map(|trigger| match trigger {
            "change_percent" => ReplacementTrigger::ChangePercent,
            "tombstone_percent" => ReplacementTrigger::TombstonePercent,
            "policy_revision" => ReplacementTrigger::PolicyRevision,
            "requalification" => ReplacementTrigger::Requalification,
            other => panic!("unknown producer replacement trigger {other}"),
        }),
    }
}

/// Damage planted in a Context's serving generation's derived index pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexDamage {
    /// A serving codebook page no longer matches the digest its generation's
    /// codebook identity binds.
    CodebookDigest,
    /// The directory page of one of the Context's sealed delta runs is
    /// missing.
    MissingRunDirectory,
}

/// Plants `damage` in the serving generation of the Context, below the
/// engine, as a lost or corrupted native page would leave it.
pub fn damage_serving_index(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    damage: IndexDamage,
) {
    use contextdb_vector::ivf::storage::PageKind;
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    let manifest = db
        .native_ivf_serving_descriptor_for_test(&index, &tuple)
        .unwrap()
        .unwrap()
        .manifest;
    let mut target = None;
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        scope.visit_physical_pages(&mut |page| {
            if target.is_some() || page.key.generation != manifest.generation {
                return;
            }
            let selected = match damage {
                IndexDamage::CodebookDigest => {
                    page.key.kind == PageKind::Codebook && page.key.ordinal == 1
                }
                IndexDamage::MissingRunDirectory => {
                    page.key.kind == PageKind::Directory && page.key.run != 0
                }
            };
            if selected {
                target = Some((page.storage, page.key));
            }
        })
    })
    .expect("locate the actual derived page");
    let (storage, key) = target.expect("fixture holds the required native page");
    match damage {
        IndexDamage::CodebookDigest => db
            .corrupt_native_ivf_page_for_test(storage, key, true, 0, 1)
            .expect("flip actual codebook bytes preserving expected authority"),
        IndexDamage::MissingRunDirectory => db
            .remove_native_ivf_page_for_test(storage, key)
            .expect("remove the actual required run directory"),
    }
}

/// The recovery action inspection shows for a Context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryAction {
    /// The serving generation is unserviceable; maintenance rebuilds it from
    /// the committed rows.
    Rebuild,
}

/// The recovery action inspection shows for the Context, if any.
pub fn recovery_action(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> Option<RecoveryAction> {
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    db.native_ivf_catalog_for_test(&index, &tuple)
        .expect("actual native damage inspection")
        .and_then(|catalog| match catalog.recovery_action {
            Some("rebuild") => Some(RecoveryAction::Rebuild),
            None => None,
            Some(action) => panic!("unknown actual recovery action {action}"),
        })
}

/// Opens a read-only session on the store at `path`. It never writes the
/// store; damage it detects is quarantined in its own memory.
pub fn open_reader(path: &Path) -> ReadSession {
    watch_verifications();
    ReadSession::open(path).expect("open the read-only session")
}

// Actual derived-page verifications observed for an admitted window.
thread_local! {static VERIFIED_WINDOWS:std::cell::RefCell<std::collections::HashMap<u64,u64>>=std::cell::RefCell::new(std::collections::HashMap::new());}
fn watch_verifications() {
    contextdb_engine::vector_ivf_observations::set_native_ivf_verification_observer_for_test(
        Arc::new(|event| {
            VERIFIED_WINDOWS.with(|windows| {
                let mut windows = windows.borrow_mut();
                assert!(
                    windows.len() < 16_384,
                    "host verification observation overflow"
                );
                let count = windows.entry(event.window).or_default();
                *count = count
                    .checked_add(1)
                    .expect("actual verification counter overflow");
            });
        }),
    );
}
pub fn damage_verifications(db: &dyn QuerySession) -> u64 {
    VERIFIED_WINDOWS.with(|windows| {
        windows
            .borrow()
            .get(&db.verification_window())
            .copied()
            .unwrap_or(0)
    })
}

/// Whether the Context's damage verdict is recorded in the store, as a
/// writer persists it, rather than held live in this session's memory.
pub fn verdict_recorded(
    db: &dyn QuerySession,
    table: &str,
    column: &str,
    context: &[Value],
) -> bool {
    partition_inspection(db, table, column, context)
        .note
        .as_deref()
        == Some("recorded")
}

/// The Context's row of `SHOW VECTOR_PARTITIONS`, as an operator reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionInspection {
    /// The `recovery_action` column.
    pub recovery_action: String,
    /// The inspection note: the state / quarantine-reason column that says,
    /// in plain words, why the partition is not served as usual; `None` when
    /// the column is empty.
    pub note: Option<String>,
    /// Every text value of the row, for checks that span all columns.
    pub texts: Vec<String>,
}

pub fn partition_inspection(
    db: &dyn QuerySession,
    table: &str,
    column: &str,
    context: &[Value],
) -> PartitionInspection {
    let result = db
        .execute(
            &format!("SHOW VECTOR_PARTITIONS FOR {table}.{column}"),
            &empty(),
        )
        .expect("actual native partition SHOW");
    let position = |name: &str| {
        result
            .columns()
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("SHOW lacks {name}: {:?}", result.columns()))
    };
    let rows = result
        .rows()
        .iter()
        .filter(|row| {
            if let Value::Json(serde_json::Value::Object(key)) = &row[position("partition_key")] {
                // The current damage fixtures select the unpartitioned tuple.
                // A nonempty tuple must identify its real fields, never take an
                // arbitrary partition's verdict.
                key.len() == context.len()
                    && key
                        .values()
                        .zip(context)
                        .all(|(actual, wanted)| match wanted {
                            Value::Text(value) => actual.as_str() == Some(value),
                            Value::Int64(value) => actual.as_i64() == Some(*value),
                            Value::Bool(value) => actual.as_bool() == Some(*value),
                            Value::Null => actual.is_null(),
                            _ => false,
                        })
            } else {
                false
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1, "SHOW identifies exactly the requested tuple");
    let row = rows[0];
    let Value::Text(recovery) = &row[position("recovery_action")] else {
        panic!("text recovery action")
    };
    let note = match &row[position("state")] {
        Value::Null => None,
        Value::Text(state) => Some(state.clone()),
        other => panic!("actual state has invalid type {other:?}"),
    };
    PartitionInspection {
        recovery_action: recovery.clone(),
        note,
        texts: row
            .iter()
            .filter_map(|value| match value {
                Value::Text(text) => Some(text.clone()),
                _ => None,
            })
            .collect(),
    }
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
    observer: Arc<dyn Fn(PublicationCut) + Send + Sync>,
    operation: impl FnOnce() -> T,
) -> T {
    use contextdb_engine::vector_ivf_observations::*;
    #[derive(Default)]
    struct Captured {
        committed: std::collections::HashMap<[u8; 64], Option<u64>>,
        staged: std::collections::HashMap<[u8; 64], Option<u64>>,
        publication: bool,
        before_publication_delivered: bool,
        remap: bool,
    }
    let captured = Arc::new(std::sync::Mutex::new(Captured::default()));
    let events = captured.clone();
    let event_observer = observer.clone();
    let boundaries = captured.clone();
    let boundary_observer = observer.clone();
    with_native_ivf_observer(
        NativeObserver {
            event: Arc::new(move |event| {
                let mut state = events.lock().unwrap();
                match event {
                    NativeIvfSealEvent::ScopeWritten { stored, .. } => {
                        let generation = stored.serving.map(|manifest| manifest.generation);
                        let prior = *state
                            .committed
                            .entry(*stored.scope_key)
                            .or_insert(generation);
                        state.staged.insert(*stored.scope_key, generation);
                        if generation.is_some() && generation != prior {
                            state.publication = true;
                        }
                    }
                    NativeIvfSealEvent::LegacyEntriesPrepared { entries, .. } => {
                        assert!(
                            entries > 0,
                            "a real preparation changed its persisted counter"
                        );
                        state.remap = true;
                    }
                    NativeIvfSealEvent::OuterCommitted { .. } => {
                        let published = state.publication;
                        let staged = std::mem::take(&mut state.staged);
                        state.committed.extend(staged);
                        state.publication = false;
                        state.before_publication_delivered = false;
                        state.remap = false;
                        drop(state);
                        // The producer emits this only after the actual native
                        // commit has flushed its new primary root and returned.
                        if published {
                            event_observer(PublicationCut::AfterGenerationPublication);
                        }
                    }
                    NativeIvfSealEvent::OuterAborted => {
                        state.staged.clear();
                        state.publication = false;
                        state.before_publication_delivered = false;
                        state.remap = false;
                    }
                    _ => {}
                }
            }),
            boundary: Arc::new(move |boundary| {
                if matches!(boundary, NativeIvfFaultBoundary::BeforeOuterCommit) {
                    let remap = boundaries.lock().unwrap().remap;
                    if remap {
                        boundary_observer(PublicationCut::DuringRemapBatch);
                    }
                }
                Ok(())
            }),
        },
        || {
            redb::commit_work::observe_root_work_for_test(
                Arc::new(move |work| {
                    if work.kind == redb::commit_work::RootWorkKindForTest::HeaderBegin {
                        let mut state = captured.lock().unwrap();
                        if state.publication && !state.before_publication_delivered {
                            state.before_publication_delivered = true;
                            drop(state);
                            // This real header boundary follows write_barrier: the
                            // new data pages reached the file, and neither header
                            // naming the new committed root has yet been written.
                            observer(PublicationCut::BeforeGenerationPublication);
                        }
                    }
                    Ok(())
                }),
                operation,
            )
        },
    )
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
pub fn physical_pages(db: &Database, table: &str, column: &str) -> Vec<PhysicalPage> {
    use contextdb_vector::ivf::storage::PageKind;
    let mut pages = Vec::new();
    db.visit_native_ivf_physical_pages_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &mut |page| {
            let place = if page.building {
                match page.key.kind {
                    PageKind::Reservoir => PhysicalPlace::TrainingReservoir,
                    PageKind::Accumulators | PageKind::Codebook => PhysicalPlace::BuildAccumulator,
                    _ => PhysicalPlace::UnpublishedRun,
                }
            } else {
                match page.key.kind {
                    PageKind::Lists => PhysicalPlace::ListPage {
                        generation: page.key.generation,
                    },
                    PageKind::Tail => PhysicalPlace::WriteBuffer,
                    _ => PhysicalPlace::Other(format!(
                        "native {:?} generation {}",
                        page.key.kind, page.key.generation
                    )),
                }
            };
            pages.push(PhysicalPage {
                place,
                bytes: page.bytes.to_vec(),
            });
        },
    )
    .expect("scan actual native storage inventory");
    pages
}

/// The exact byte sequence a list record stores for `vector`'s quantized
/// representation on an unpartitioned column: under the serving generation's
/// quantizer, or under an in-flight build's trained quantizer when nothing is
/// serving yet. Empty while no quantizer has been trained.
pub fn quantized_record(db: &Database, table: &str, column: &str, vector: &[f32]) -> Vec<u8> {
    db.native_ivf_quantized_record_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        vector,
    )
    .expect("read actual stored quantized record")
}

/// The size of one list page of one indexed column.
pub fn list_page_bytes(_db: &Database, _table: &str, _column: &str) -> u64 {
    contextdb_vector::ivf::storage::PAGE_BYTES as u64
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
    operation: impl FnOnce() -> T,
) -> (T, ReaderCharges) {
    use contextdb_engine::vector_ivf_observations::*;
    #[derive(Default)]
    struct Seen {
        result: ReaderCharges,
        context: Vec<Value>,
        native: Vec<(u64, usize, usize)>,
        books: Vec<(u64, usize, usize)>,
        read_owner: Option<usize>,
    }
    struct BookObserver {
        state: Arc<std::sync::Mutex<Seen>>,
    }
    impl contextdb_vector::ivf::IvfConstructorObserverForTest for BookObserver {
        fn admitted(&self, _: contextdb_vector::ivf::IvfConstructorAllocationForTest) {}
        fn debited(&self, id: u64, owner: usize, bytes: usize, operation: &'static str) {
            if operation != "clustered_codebook" {
                return;
            }
            let mut state = self.state.lock().unwrap();
            assert!(
                state.result.objects.len() < 4096,
                "host metadata observation overflow"
            );
            assert_eq!(
                state.read_owner,
                Some(owner),
                "the codebook uses the captured scope original reader"
            );
            let at = state.result.objects.len();
            let context = state.context.clone();
            state.result.objects.push(MetadataObject {
                kind: MetadataObjectKind::Codebook,
                context,
                object_bytes: bytes as u64,
                charges: vec![bytes as u64],
                outstanding_after: bytes as u64,
            });
            state.books.push((id, at, owner));
        }
        fn released(&self, id: u64, owner: usize, _bytes: usize, operation: &'static str) {
            if operation != "clustered_codebook" {
                return;
            }
            let mut state = self.state.lock().unwrap();
            let pos = state
                .books
                .iter()
                .position(|(held, _, _)| *held == id)
                .expect("the original book debit was observed");
            let (_, at, original) = state.books.remove(pos);
            assert_eq!(original, owner);
            state.result.objects[at].outstanding_after = 0;
        }
    }
    let state = Arc::new(std::sync::Mutex::new(Seen::default()));
    let sink = Arc::clone(&state);
    let result = with_native_ivf_seal_observer_for_test(
        Arc::new(NativeObserver {
            event: Arc::new(move |event| {
                let mut state = sink.lock().unwrap();
                match event {
                    NativeIvfSealEvent::ObjectAdmitted {
                        instance,
                        kind: NativeIvfObjectKind::Scope,
                        layout,
                        original_owner,
                        ..
                    } => {
                        assert!(
                            state.result.objects.len() < 4096,
                            "host metadata observation overflow"
                        );
                        let at = state.result.objects.len();
                        // The native scope is the complete bounded directory of
                        // generation/kind roots plus its admitted decode window.
                        // Its actual original invoice covers the entire Layout.
                        state.result.objects.push(MetadataObject {
                            kind: MetadataObjectKind::DirectoryChunk,
                            context: Vec::new(),
                            object_bytes: layout.size() as u64,
                            charges: vec![layout.size() as u64],
                            outstanding_after: layout.size() as u64,
                        });
                        state.native.push((instance.0, at, original_owner));
                        state.result.directory_window = 1; // one scope lent by the query factory
                        state.result.peak_resident_directory_chunks = state
                            .result
                            .peak_resident_directory_chunks
                            .max(state.native.len() as u64);
                    }
                    NativeIvfSealEvent::ScopeOpened {
                        scope,
                        source_instance: NativeIvfSourceInstance::Read { .. },
                        ..
                    } => {
                        let context = scope
                            .tuple
                            .components()
                            .iter()
                            .map(|c| match c {
                                contextdb_core::VectorPartitionComponent::Uuid(v) => {
                                    Value::Uuid(*v)
                                }
                                contextdb_core::VectorPartitionComponent::Text(v) => {
                                    Value::Text(v.clone())
                                }
                                contextdb_core::VectorPartitionComponent::Integer(v) => {
                                    Value::Int64(*v)
                                }
                                contextdb_core::VectorPartitionComponent::Boolean(v) => {
                                    Value::Bool(*v)
                                }
                                contextdb_core::VectorPartitionComponent::Timestamp(v) => {
                                    Value::Timestamp(*v)
                                }
                                contextdb_core::VectorPartitionComponent::TxId(v) => {
                                    Value::TxId(*v)
                                }
                            })
                            .collect::<Vec<_>>();
                        if let Some(&(_, at, owner)) = state.native.last() {
                            state.result.objects[at].context = context.clone();
                            state.read_owner = Some(owner);
                        }
                        state.context = context;
                    }
                    NativeIvfSealEvent::ObjectReleased {
                        instance,
                        kind: NativeIvfObjectKind::Scope,
                        original_owner,
                        ..
                    } => {
                        let pos = state
                            .native
                            .iter()
                            .position(|(held, _, _)| *held == instance.0)
                            .expect("the original directory debit was observed");
                        let (_, at, owner) = state.native.remove(pos);
                        assert_eq!(owner, original_owner);
                        state.result.objects[at].outstanding_after = 0;
                    }
                    _ => {}
                }
            }),
            boundary: Arc::new(|_| Ok(())),
        }),
        || {
            contextdb_vector::ivf::with_ivf_constructor_observer_for_test(
                Arc::new(BookObserver {
                    state: Arc::clone(&state),
                }),
                operation,
            )
        },
    );
    let mut state = state.lock().unwrap();
    (result, std::mem::take(&mut state.result))
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
#[derive(Default)]
struct IndexReservationTally {
    held: std::sync::atomic::AtomicUsize,
    overflow: std::sync::atomic::AtomicBool,
}
impl IndexReservationTally {
    fn debit(&self, bytes: usize) {
        if self
            .held
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| n.checked_add(bytes),
            )
            .is_err()
        {
            self.overflow
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    fn release(&self, bytes: usize) {
        if self
            .held
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| n.checked_sub(bytes),
            )
            .is_err()
        {
            self.overflow
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}
impl contextdb_vector::ivf::IvfConstructorObserverForTest for IndexReservationTally {
    fn admitted(&self, _: contextdb_vector::ivf::IvfConstructorAllocationForTest) {}
    fn debited(&self, _invoice: u64, owner: usize, bytes: usize, operation: &'static str) {
        assert_ne!(owner, 0);
        // The actual persistent source invoice belongs to state B, rather
        // than this operation's workspace. switch_state observes that debt.
        if operation != "clustered_retained_codebook" {
            self.debit(bytes);
        }
    }
    fn released(&self, _invoice: u64, owner: usize, bytes: usize, operation: &'static str) {
        assert_ne!(owner, 0);
        if operation != "clustered_retained_codebook" {
            self.release(bytes);
        }
    }
}
thread_local! {
    static LAST_INDEX_RESERVATIONS:std::cell::RefCell<Option<Arc<IndexReservationTally>>>=const {std::cell::RefCell::new(None)};
}
pub fn observe_index_cancel_points<T>(
    observer: Arc<dyn Fn(IndexCancelPoint) + Send + Sync>,
    operation: impl FnOnce() -> T,
) -> T {
    use contextdb_engine::vector_ivf_observations::*;
    let tally = Arc::new(IndexReservationTally::default());
    LAST_INDEX_RESERVATIONS.with(|slot| slot.replace(Some(tally.clone())));
    let sink = tally.clone();
    contextdb_vector::ivf::with_ivf_constructor_observer_for_test(tally, || {
        with_native_ivf_observer(
            NativeObserver {
                event: Arc::new(move |event| match event {
                    NativeIvfSealEvent::ReservationDebited {
                        original_owner,
                        bytes,
                        operation,
                        ..
                    } => {
                        assert_ne!(original_owner, 0);
                        if !matches!(
                            operation,
                            "clustered_retained_registry"
                                | "clustered_retained_control"
                                | "clustered_retained_directory"
                        ) {
                            sink.debit(bytes);
                        }
                    }
                    NativeIvfSealEvent::ReservationReleased {
                        original_owner,
                        bytes,
                        operation,
                        ..
                    } => {
                        assert_ne!(original_owner, 0);
                        if !matches!(
                            operation,
                            "clustered_retained_registry"
                                | "clustered_retained_control"
                                | "clustered_retained_directory"
                        ) {
                            sink.release(bytes);
                        }
                    }
                    _ => {}
                }),
                boundary: Arc::new(|_| Ok(())),
            },
            || {
                with_native_ivf_work_observer_for_test(
                    Arc::new(move |point| {
                        observer(match point {
                            NativeIvfWorkPoint::ListPageRead => IndexCancelPoint::ListPageRead,
                            NativeIvfWorkPoint::CandidateScoring => {
                                IndexCancelPoint::CandidateScoring
                            }
                            NativeIvfWorkPoint::BuildBatch => IndexCancelPoint::BuildBatch,
                            NativeIvfWorkPoint::ReclaimBatch => IndexCancelPoint::ReclaimBatch,
                        })
                    }),
                    operation,
                )
            },
        )
    })
}

/// Runs one caller-driven maintenance cycle that stops, with the engine's
/// existing typed cancellation, at its next checkpoint once `cancellation`
/// is cancelled.
pub fn run_maintenance_cycle_cancellable(
    db: &Database,
    cancellation: &contextdb_core::read_contract::OwnerReadCancellation,
) -> contextdb_core::Result<()> {
    contextdb_engine::vector_ivf_observations::with_native_ivf_maintenance_cancellation_for_test(
        cancellation,
        || db.run_maintenance_cycle().map(|_| ()),
    )
}

/// Bytes currently held by the index's operation reservations of every kind:
/// query workspaces, seal workspaces, maintenance step workspaces, and the
/// whole-object metadata they admitted.
pub fn outstanding_index_reservations(_db: &Database) -> u64 {
    LAST_INDEX_RESERVATIONS.with(|slot| {
        let tally = slot.borrow();
        let tally = tally
            .as_ref()
            .expect("an actual index operation was observed");
        assert!(
            !tally.overflow.load(std::sync::atomic::Ordering::SeqCst),
            "actual reservation capture overflow or unmatched release"
        );
        tally.held.load(std::sync::atomic::Ordering::SeqCst) as u64
    })
}

/// The typed error that last stopped one Context's build or replacement,
/// kept until that build next makes progress. `None` when nothing stopped it.
pub fn last_build_refusal(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> Option<contextdb_core::Error> {
    let owner = db
        .native_ivf_physical_disk_owner_for_test()
        .unwrap()
        .identity();
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    LAST_NATIVE_DISK_REFUSALS.with(|records| {
        records
            .borrow()
            .iter()
            .find(|(o, i, t, _)| *o == owner && *i == index && *t == tuple)
            .map(|(_, _, _, error)| copy_native_disk_refusal(error))
    })
}

/// While it lives, writes of the named build class are never satisfied from
/// reusable free space: each one can only be placed by growing the store.
/// Every other write reuses free space as usual.
pub struct ReusableSpaceWithheld {
    _guard: contextdb_engine::vector_ivf_observations::NativeIvfReusableSpaceWithheldForTest,
}

pub fn withhold_reusable_space_from(
    db: &Database,
    class: BuildWriteClass,
) -> ReusableSpaceWithheld {
    ReusableSpaceWithheld {
        _guard:
            contextdb_engine::vector_ivf_observations::withhold_native_ivf_reusable_space_for_test(
                db.native_ivf_physical_disk_owner_for_test().unwrap(),
                native_build_class(class),
            ),
    }
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
pub struct CeilingAtAdmission<'db> {
    db: &'db Database,
    guard: Option<contextdb_engine::vector_ivf_observations::NativeIvfCeilingAtAdmissionForTest>,
}
impl Drop for CeilingAtAdmission<'_> {
    fn drop(&mut self) {
        let guard = self.guard.take().expect("ceiling guard");
        let chosen = guard.chosen();
        drop(guard);
        if let Some(ceiling) = chosen {
            self.db
                .set_disk_limit(Some(ceiling))
                .expect("publish the chosen physical ceiling after the cycle");
        }
    }
}

pub fn declare_ceiling_at_next_admission<'db>(
    db: &'db Database,
    class: BuildWriteClass,
    choose: Box<dyn FnOnce(&AdmissionBoundary) -> u64 + Send>,
) -> CeilingAtAdmission<'db> {
    // Test-seam convenience, not a user path: the unlocked real admission
    // applies the existing physical-owner setter while the write is active.
    // After the cycle returns, Drop publishes that same value through the
    // ordinary Database::set_disk_limit path, outside the transaction lock.
    let guard = contextdb_engine::vector_ivf_observations::choose_native_ivf_ceiling_for_test(
        db.native_ivf_physical_disk_owner_for_test().unwrap(),
        native_build_class(class),
        Box::new(move |at| {
            choose(&AdmissionBoundary {
                operation: at.operation,
                class: observed_build_class(at.class),
                current_store_bytes: at.current_store_bytes,
                minimum_growth_bytes: at.minimum_growth_bytes,
            })
        }),
    );
    CeilingAtAdmission {
        db,
        guard: Some(guard),
    }
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
pub fn open_with_index_test_options(path: &Path, options: IndexTestOptions) -> Database {
    let db = open_caller_driven(path);
    db.native_ivf_set_test_options(
        contextdb_engine::vector_ivf_observations::NativeIvfIndexTestOptions {
            write_buffer_cap_bytes: options.write_buffer_cap_bytes,
            build_batch_rows: options.build_batch_rows,
        },
    )
    .expect("set native test inputs");
    db
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
pub fn delta_runs(db: &Database, table: &str, column: &str, context: &[Value]) -> Vec<DeltaRun> {
    native_run_inventory(db, table, column, context).0
}

/// Decode only the native catalog and its immutable run descriptors for observation.
fn native_run_inventory(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> (Vec<DeltaRun>, RemapProgress) {
    use contextdb_vector::ivf::storage::{Manifest, PAGE_BYTES, PageKind, ReadPages};
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        let Some(manifest) = scope.manifest(0)? else {
            return Ok((Vec::new(), RemapProgress::default()));
        };
        let mut bytes = [0; PAGE_BYTES];
        if !scope.read_admitted_page(manifest.key(PageKind::Catalog, 0, 0), &mut bytes)? {
            return Err(contextdb_core::Error::Other(
                "native run catalog is missing".into(),
            ));
        }
        if &bytes[..8] != b"IVFCAT02" {
            return Err(contextdb_core::Error::Other(
                "native run catalog has invalid magic".into(),
            ));
        }
        let word = |at: usize| u64::from_le_bytes(bytes[at * 8..at * 8 + 8].try_into().unwrap());
        let current = (word(2), word(4));
        let legacy = if word(9) == 0 {
            None
        } else {
            Some((
                Manifest {
                    partition: manifest.partition,
                    generation: word(10),
                    dimensions: word(11) as usize,
                    lists: word(12) as u32,
                    active_lists: word(13) as u32,
                    slots: word(14),
                    sealed_slots: word(15),
                    codebook: bytes[256..288].try_into().unwrap(),
                    run: 0,
                },
                word(16),
                word(17),
            ))
        };
        let cursor = word(20);
        let examined = word(21);
        let mut runs = Vec::new();
        let mut legacy_entries = 0u64;
        for (view, mut next, count, is_legacy) in legacy
            .into_iter()
            .map(|(view, next, count)| (view, next, count, true))
            .chain(std::iter::once((manifest, current.0, current.1, false)))
        {
            if count > view.slots {
                return Err(contextdb_core::Error::Other(
                    "native run count exceeds source extent".into(),
                ));
            }
            for _ in 0..count {
                if next == 0
                    || !scope
                        .read_admitted_page(view.key(PageKind::Catalog, 1, next), &mut bytes)?
                {
                    return Err(contextdb_core::Error::Other(
                        "native run chain is incomplete".into(),
                    ));
                }
                if &bytes[..8] != b"IVFCAT02" {
                    return Err(contextdb_core::Error::Other(
                        "native run descriptor has invalid magic".into(),
                    ));
                }
                let entries = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
                next = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
                if is_legacy {
                    legacy_entries = legacy_entries.checked_add(entries).ok_or_else(|| {
                        contextdb_core::Error::Other("native run extent overflow".into())
                    })?;
                }
                runs.push(DeltaRun {
                    codebook: (view.active_lists != 0)
                        .then(|| u64::from_le_bytes(view.codebook[..8].try_into().unwrap())),
                    entries,
                });
            }
            if next != 0 {
                return Err(contextdb_core::Error::Other(
                    "native run chain exceeds its declared count".into(),
                ));
            }
        }
        Ok((
            runs,
            RemapProgress {
                entries_remaining: legacy_entries.checked_sub(examined).ok_or_else(|| {
                    contextdb_core::Error::Other(
                        "native remap cursor exceeds retained run extent".into(),
                    )
                })?,
                cursor: (legacy_entries != 0).then_some(cursor),
            },
        ))
    })
    .expect("inspect actual immutable native run descriptors")
}

/// The legacy runs of the Context's replacement still to retire; zero
/// outside the remap phase.
pub fn legacy_runs(db: &Database, table: &str, column: &str, context: &[Value]) -> u64 {
    db.native_ivf_catalog_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
    )
    .expect("inspect actual native run catalog")
    .map_or(0, |catalog| catalog.legacy_runs)
}

/// One physical page a generation holds, of any kind, by its role and its
/// logical identity within the generation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GenerationPage {
    pub role: GenerationPageRole,
    /// The page's logical identity within the generation (its ordinal for
    /// the role), stable across copy-on-write rewrites of the same page. A
    /// released page leaves the set; a page allocated later joins it.
    pub page: u64,
}

/// What a generation's page holds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GenerationPageRole {
    ListPage,
    RunDirectory,
    Visibility,
    Canonical,
    JournalLink,
    Codebook,
    Other(String),
}

/// Every physical page the Context's `generation` still holds, of every
/// kind, each by its native identity.
pub fn generation_pages(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    generation: u64,
) -> std::collections::BTreeSet<GenerationPage> {
    use contextdb_vector::ivf::storage::PageKind;
    let mut pages = std::collections::BTreeSet::new();
    db.with_native_ivf_read_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        |scope| {
            scope.visit_physical_pages(&mut |page| {
                if page.key.generation != generation {
                    return;
                }
                let role = if page.key.run != 0 {
                    GenerationPageRole::Other(format!("{:?}/run/{}", page.key.kind, page.key.run))
                } else {
                    match page.key.kind {
                        PageKind::Lists => GenerationPageRole::ListPage,
                        PageKind::Directory | PageKind::DirectoryPresence => {
                            GenerationPageRole::RunDirectory
                        }
                        PageKind::Visibility => GenerationPageRole::Visibility,
                        PageKind::Canonical => GenerationPageRole::Canonical,
                        PageKind::RowCopies => GenerationPageRole::JournalLink,
                        PageKind::Codebook => GenerationPageRole::Codebook,
                        other => GenerationPageRole::Other(format!("{other:?}")),
                    }
                };
                pages.insert(GenerationPage {
                    role,
                    page: page.key.ordinal,
                });
            })
        },
    )
    .expect("visit actual native generation payloads");
    pages
}

/// The primary keys of rows whose old copies sit in legacy runs the
/// Context's remap has already retired, while other legacy runs remain.
pub fn retired_legacy_rows(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> Vec<Value> {
    use contextdb_vector::ivf::storage::{Manifest, PAGE_BYTES, PageKind, ReadPages};
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        let Some(current) = scope.manifest(0)? else {
            return Ok(Vec::new());
        };
        let mut page = [0; PAGE_BYTES];
        assert!(scope.read_admitted_page(current.key(PageKind::Catalog, 0, 0), &mut page)?);
        assert_eq!(&page[..8], b"IVFCAT02");
        let word = |at: usize| u64::from_le_bytes(page[at * 8..at * 8 + 8].try_into().unwrap());
        if word(9) == 0 {
            return Ok(Vec::new());
        }
        let old = Manifest {
            partition: 0,
            generation: word(10),
            dimensions: word(11) as usize,
            lists: word(12) as u32,
            active_lists: word(13) as u32,
            slots: word(14),
            sealed_slots: word(15),
            codebook: page[256..288].try_into().unwrap(),
            run: 0,
        };
        let mut next = word(16);
        let count = word(17);
        let mut remaining = std::collections::BTreeSet::new();
        let mut remaining_slots = std::collections::BTreeSet::new();
        for _ in 0..count {
            assert!(
                next != 0 && remaining.insert(next),
                "actual legacy chain is finite"
            );
            assert!(scope.read_admitted_page(old.key(PageKind::Catalog, 1, next), &mut page)?);
            assert_eq!(&page[..8], b"IVFCAT02");
            next = u64::from_le_bytes(page[32..40].try_into().unwrap());
            let mut slot = u64::from_le_bytes(page[8..16].try_into().unwrap());
            let entries = u64::from_le_bytes(page[24..32].try_into().unwrap());
            assert!(entries <= old.slots);
            // Coalescing keeps each entry's origin-run marker. Membership
            // comes from the immutable run's actual entry chain, not that marker.
            for _ in 0..entries {
                assert!(slot < old.slots && remaining_slots.insert(slot));
                let metadata =
                    observed_native_bytes::<40>(scope, old, PageKind::SlotMetadata, slot * 40)?;
                slot = u64::from_le_bytes(metadata[8..16].try_into().unwrap());
            }
        }
        assert_eq!(next, 0);
        let mut retired = Vec::new();
        for slot in 0..old.slots {
            let metadata =
                observed_native_bytes::<40>(scope, old, PageKind::SlotMetadata, slot * 40)?;
            let lsn = u64::from_le_bytes(metadata[..8].try_into().unwrap());
            let run = u64::from_le_bytes(metadata[16..24].try_into().unwrap());
            if run == 0 || remaining_slots.contains(&slot) {
                continue;
            }
            let row = u64::from_le_bytes(observed_native_bytes::<8>(
                scope,
                old,
                PageKind::Canonical,
                slot * (old.dimensions as u64 * 4 + 8),
            )?);
            retired.push(db.native_ivf_primary_key_for_row_for_test(table, row, Some(lsn))?);
        }
        Ok(retired)
    })
    .expect("read old copies in runs absent from the actual remaining legacy chain")
}

/// Read the old generation's own visibility bit, including copies whose
/// primary keys now resolve only through their genuine historical row version.
pub fn old_copy_visible(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    generation: u64,
    primary_key: Value,
) -> Option<bool> {
    use contextdb_vector::ivf::storage::PageKind;
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        let Some(manifest) = scope.generation_manifest_for_test(generation)? else {
            return Ok(None);
        };
        let mut found = None;
        for slot in 0..manifest.slots {
            let metadata =
                observed_native_bytes::<40>(scope, manifest, PageKind::SlotMetadata, slot * 40)?;
            let lsn = u64::from_le_bytes(metadata[..8].try_into().unwrap());
            let row = u64::from_le_bytes(observed_native_bytes::<8>(
                scope,
                manifest,
                PageKind::Canonical,
                slot * (manifest.dimensions as u64 * 4 + 8),
            )?);
            if db.native_ivf_primary_key_for_row_for_test(table, row, Some(lsn))? != primary_key {
                continue;
            }
            let byte =
                observed_native_bytes::<1>(scope, manifest, PageKind::Visibility, slot / 8)?[0];
            let visible = byte & (1 << (slot % 8)) != 0;
            found = Some(found.unwrap_or(false) || visible);
        }
        Ok(found)
    })
    .expect("read physical old-copy identity and its original visibility bit")
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
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> RemapProgress {
    native_run_inventory(db, table, column, context).1
}

/// How one calibrated depth of a serving generation qualified.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DepthQualification {
    /// A depth from the CALIBRATION_DEPTHS policy.
    pub depth: u32,
    /// This depth's own qualified PROBES.
    pub probes: u64,
    /// This depth's own qualified REFINE_FACTOR.
    pub refine_factor: f64,
    /// This depth's own shortlist: ceil(depth x its own refine factor), at
    /// least `depth`.
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

/// The prefix hit summary one executed calibrated-depth route recorded at one
/// LIMIT it serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrefixCoverage {
    /// The calibrated depth d whose route was run: the smallest calibrated
    /// depth at least `k`.
    pub depth: u32,
    /// A LIMIT in (the previous calibrated depth, d].
    pub k: u32,
    /// Sample queries, over both halves, whose exact top `k` the depth-d
    /// route's answer held below the target.
    pub queries_below: u32,
    /// No sample query was below the target at `k`: a LIMIT `k` query is
    /// covered.
    pub covered: bool,
}

/// Every prefix summary of the Context's serving generation, ordered by `k`:
/// for each calibrated depth d, the summary its own executed d-route recorded
/// at every k in (the previous calibrated depth, d].
pub fn prefix_coverage(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> Vec<PrefixCoverage> {
    use contextdb_engine::vector_ivf_observations::NativeIvfQualificationRecordForTest as Record;
    let mut result = Vec::new();
    let mut target = 9500;
    db.with_native_ivf_read_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        |scope| {
            scope.visit_qualification(&mut |record| match record {
                Record::Header(h) => target = h.target_hundredths,
                Record::Prefix(p) => result.push(PrefixCoverage {
                    depth: p.depth,
                    k: p.k,
                    queries_below: p.queries_below,
                    covered: p.meets(target),
                }),
                _ => {}
            })
        },
    )
    .expect("read the real same-route prefix receipt");
    result
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
    pub basis: CalibrationBasis,
    /// Every calibrated depth with its own qualified (PROBES, REFINE_FACTOR)
    /// pair, in ascending depth order.
    pub depths: Vec<DepthQualification>,
}

pub fn calibration_receipt(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> Option<CalibrationReceipt> {
    use contextdb_engine::vector_ivf_observations::NativeIvfQualificationRecordForTest as Record;
    let mut receipt = None;
    db.with_native_ivf_read_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        |scope| {
            scope.visit_qualification(&mut |record| match record {
                Record::Header(header) => {
                    receipt = Some(CalibrationReceipt {
                        generation: header.generation,
                        basis: if header.query_source {
                            CalibrationBasis::Queries {
                                source_identity: header.source_identity,
                                source_frontier: header.source_frontier,
                            }
                        } else {
                            CalibrationBasis::StoredVectors
                        },
                        depths: Vec::new(),
                    });
                }
                Record::Depth(d) => receipt.as_mut().unwrap().depths.push(DepthQualification {
                    depth: d.depth,
                    probes: u64::from(d.probes),
                    refine_factor: f64::from(d.factor_hundredths) / 100.0,
                    shortlist: d.shortlist,
                    // This frozen scalar fixture observes the maximum actual run;
                    // production retains every sample and min/mean/max.
                    effective_candidates: d.reranks.map_or(0, |counts| counts.maximum),
                    sample_queries: d.samples,
                    queries_below: d.queries_below,
                    qualified: d.qualified,
                }),
                Record::Prefix(_)
                | Record::SampleWork(_)
                | Record::Sample(_)
                | Record::Rows(_)
                | Record::Rewrite(_)
                | Record::Oracle(_) => {}
            })
        },
    )
    .expect("read actual persisted qualification");
    receipt
}

/// The Context's serving generation must be requalified (its target or its
/// calibration-query source changed since it was qualified).
pub fn requalification_pending(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> bool {
    db.native_ivf_requalification_pending_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
    )
    .expect("inspect the actual served receipt's declaration and source dependency")
}

/// Supplies the answer scored for one (sample query, calibrated depth) from
/// that depth's exact ranking.
pub type CalibrationAnswer = Box<dyn Fn(usize, u32, &[u64]) -> Vec<u64> + Send + Sync>;

/// While it lives, the next calibration of the named Context computes, for
/// every sample query and every calibrated depth, the exact top-`depth`
/// ranking as usual, runs that depth's own production search, and then
/// scores, in place of that run's answer, the identities that
/// `answer(query, depth, exact)` returns. `exact` holds the exact top-`depth`
/// canonical row identities. It is called once for each (sample query,
/// calibrated depth) the calibration actually runs.
pub struct CalibrationAnswersRewritten {
    _guard: contextdb_engine::vector_ivf_observations::NativeIvfCalibrationAnswersForTest,
}
pub fn rewrite_next_calibration_answers(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    answer: CalibrationAnswer,
) -> CalibrationAnswersRewritten {
    CalibrationAnswersRewritten {
        _guard: db
            .rewrite_native_ivf_calibration_answers_for_test(
                &contextdb_core::VectorIndexRef::new(table, column),
                &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
                answer,
            )
            .expect("rewrite only the scored identities of each actual production run"),
    }
}

/// Installs on the Context's serving generation a qualification receipt
/// stating that every listed depth qualified, with every prefix summary from
/// the previous calibrated depth up to it covered, and with the given mean
/// complete work for the indexed route and for EXACT, as if calibration had
/// measured them. The receipt keeps the generation's calibration basis; nothing else
/// about the generation changes.
pub fn plant_qualification(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    depths: &[u32],
    indexed_mean_work: u64,
    exact_mean_work: u64,
) {
    db.plant_native_ivf_qualification_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        depths,
        indexed_mean_work,
        exact_mean_work,
    )
    .expect("plant supplied facts in the actual native qualification receipt");
}

/// Declares `source_table.source_column` as the CALIBRATION_QUERIES source of
/// the indexed column, through the column's policy.
pub fn declare_calibration_queries(
    db: &Database,
    table: &str,
    column: &str,
    source_table: &str,
    source_column: &str,
) {
    db.execute(&format!("ALTER TABLE {table} ALTER COLUMN {column} SET WITH (calibration_queries = {source_table}.{source_column})"),&empty()).expect("declare actual calibration source");
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
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> Vec<DeltaEntry> {
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    let mut entries = Vec::new();
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        scope.visit_visible_identities_for_test(&mut |identity, book| {
            entries.push(DeltaEntry {
                primary_key: db.native_ivf_primary_key_for_row_for_test(
                    table,
                    identity.row.row,
                    Some(identity.row.commit_lsn),
                )?,
                commit_lsn: identity.row.commit_lsn,
                codebook: book.map(|book| u64::from_le_bytes(book[..8].try_into().unwrap())),
                sealed: identity.source != contextdb_vector::ivf::CandidateSource::UnsealedTail,
            });
            Ok(())
        })
    })
    .expect("visit real logically visible canonical copies");
    entries.sort_by_key(|entry| entry.commit_lsn);
    entries
}

/// Copies only the requested bytes from authenticated physical native pages.
fn observed_native_bytes<const N: usize>(
    source: &impl contextdb_vector::ivf::storage::ReadPages,
    manifest: contextdb_vector::ivf::storage::Manifest,
    kind: contextdb_vector::ivf::storage::PageKind,
    offset: u64,
) -> contextdb_core::Result<[u8; N]> {
    use contextdb_vector::ivf::storage::PAGE_BYTES;
    let mut bytes = [0; N];
    let mut copied = 0;
    while copied < N {
        let at = offset
            .checked_add(copied as u64)
            .expect("observed offset fits");
        let mut page = [0; PAGE_BYTES];
        assert!(
            source.read_admitted_page(manifest.key(kind, 0, at / PAGE_BYTES as u64), &mut page)?,
            "observed native page exists"
        );
        let start = (at % PAGE_BYTES as u64) as usize;
        let take = (N - copied).min(PAGE_BYTES - start);
        bytes[copied..copied + take].copy_from_slice(&page[start..start + take]);
        copied += take;
    }
    Ok(bytes)
}

pub fn switch_state(db: &Database, table: &str, column: &str, context: &[Value]) -> SwitchState {
    use contextdb_vector::ivf::storage::{Manifest, PAGE_BYTES, PageKind, ReadPages};
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        let mut result = SwitchState {
            phase: SwitchPhase::Serving,
            legacy: None,
            legacy_entries_remapped: 0,
            retained_codebook: None,
            retained_codebook_charge_bytes: 0,
        };
        let Some(manifest) = scope.manifest(0)? else {
            return Ok(result);
        };
        let mut page = [0; PAGE_BYTES];
        assert!(scope.read_admitted_page(manifest.key(PageKind::Catalog, 0, 0), &mut page)?);
        assert_eq!(&page[..8], b"IVFCAT02");
        let word = |at: usize| u64::from_le_bytes(page[at * 8..at * 8 + 8].try_into().unwrap());
        if word(9) == 0 {
            return Ok(result);
        };
        let old = Manifest {
            partition: 0,
            generation: word(10),
            dimensions: word(11) as usize,
            lists: word(12) as u32,
            active_lists: word(13) as u32,
            slots: word(14),
            sealed_slots: word(15),
            codebook: page[256..288].try_into().unwrap(),
            run: 0,
        };
        let after = word(18);
        let through = word(19);
        // F1 froze this old extent. Retired runs and invisible versions remain
        // physically present until C, so this raw membership remains immutable.
        let mut count = 0u64;
        let mut digest = [0u8; 32];
        for slot in 0..old.slots {
            let metadata =
                observed_native_bytes::<40>(scope, old, PageKind::SlotMetadata, slot * 40)?;
            let lsn = u64::from_le_bytes(metadata[..8].try_into().unwrap());
            if lsn <= after || lsn > through {
                continue;
            }
            let row = observed_native_bytes::<8>(
                scope,
                old,
                PageKind::Canonical,
                slot * (old.dimensions as u64 * 4 + 8),
            )?;
            let mut hash = blake3::Hasher::new();
            hash.update(&row);
            hash.update(&lsn.to_le_bytes());
            for (dst, src) in digest.iter_mut().zip(hash.finalize().as_bytes()) {
                *dst ^= *src
            }
            count += 1;
        }
        for slot in 0..manifest.slots {
            let metadata =
                observed_native_bytes::<40>(scope, manifest, PageKind::SlotMetadata, slot * 40)?;
            let lsn = u64::from_le_bytes(metadata[..8].try_into().unwrap());
            if lsn > after
                && lsn <= through
                && u64::from_le_bytes(metadata[32..40].try_into().unwrap()) == 1
            {
                result.legacy_entries_remapped += 1;
            }
        }
        let held = scope.retained_codebook_for_test();
        result.phase = SwitchPhase::Remapping;
        result.legacy = Some(LegacyEntries {
            after_lsn: after,
            through_lsn: through,
            entries: count,
            identity: u64::from_le_bytes(digest[..8].try_into().unwrap()),
        });
        result.retained_codebook = held
            .manifest
            .map(|manifest| u64::from_le_bytes(manifest.codebook[..8].try_into().unwrap()));
        result.retained_codebook_charge_bytes = held.charged_bytes as u64;
        Ok(result)
    })
    .expect("inspect durable F1 extent, actual prepared slots and retained invoice")
}

/// Legacy entries one Context has prepared for remap into its serving
/// codebook's pages since the Context was created, counted once per entry as
/// each bounded preparation step commits. Persisted, and carried across every
/// switch phase transition; it counts preparation work, not the moment a
/// prepared run's searchable root switches.
pub fn legacy_entries_prepared_total(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> u64 {
    db.native_ivf_maintenance_state_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
    )
    .expect("actual durable remap preparation count")
    .legacy_entries_prepared
}

/// The Context's rows ordered by the serving generation's own quantized score
/// for `query`, best first, as primary keys: the order a shortlist is cut
/// from.
pub fn quantized_order(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    query: &[f32],
) -> Vec<Value> {
    let manifest = db
        .native_ivf_maintenance_state_for_test(
            &contextdb_core::VectorIndexRef::new(table, column),
            &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        )
        .expect("read native serving manifest")
        .manifest
        .expect("a serving generation");
    assert_eq!(query.len(), manifest.dimensions);
    // Decode the revision-one signed int8 wire format. All row codes below
    // come from the saved physical list records, never from a test index.
    let mut query_codes = vec![0i8; query.len()];
    contextdb_vector::ivf::quantized_query_codes_for_test(query, &mut query_codes);
    let mut ranked = Vec::new();
    db.with_native_ivf_read_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        |scope| {
            scope.visit_physical_pages(&mut |page| {
                if page.key.generation != manifest.generation
                    || page.key.kind != contextdb_vector::ivf::storage::PageKind::Lists
                {
                    return;
                }
                let count = u32::from_le_bytes(page.bytes[112..116].try_into().unwrap()) as usize;
                for record in page
                    .payload
                    .chunks_exact(manifest.dimensions + 16)
                    .take(count)
                {
                    let row = u64::from_le_bytes(record[8..16].try_into().unwrap());
                    let (mut dot, mut aa, mut bb) = (0i64, 0i64, 0i64);
                    for (&a, &b) in query_codes.iter().zip(&record[16..]) {
                        let (a, b) = (i64::from(a), i64::from(b as i8));
                        dot += a * b;
                        aa += a * a;
                        bb += b * b;
                    }
                    let distance = if aa == 0 || bb == 0 {
                        1.0
                    } else {
                        1.0 - dot as f64 / ((aa as f64).sqrt() * (bb as f64).sqrt())
                    };
                    ranked.push((distance, row));
                }
            })
        },
    )
    .expect("read actual quantized list bytes");
    ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    ranked.dedup_by_key(|entry| entry.1);
    ranked
        .into_iter()
        .map(|(_, row)| {
            db.native_ivf_primary_key_for_row_for_test(table, row, None)
                .expect("resolve actual native row primary key")
        })
        .collect()
}

/// While it lives, the next calibration of the named Context evaluates only
/// this (PROBES, REFINE_FACTOR) point, PROBES clamped to the active list
/// count, at every calibrated depth: each depth runs its own search with its
/// own shortlist at this point, is qualified or not from its own runs, and
/// the selector moves no depth to another point. Every depth's pair is then
/// this point.
pub struct CalibrationSettingPlanted {
    _guard: contextdb_engine::vector_ivf_observations::NativeIvfCalibrationSettingForTest,
}
pub fn plant_calibration_setting(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    probes: u64,
    refine_factor: f64,
) -> CalibrationSettingPlanted {
    let factor = (refine_factor * 100.0).round();
    assert!(factor >= 100.0 && factor <= u32::MAX as f64);
    CalibrationSettingPlanted {
        _guard: db
            .plant_native_ivf_calibration_setting_for_test(
                &contextdb_core::VectorIndexRef::new(table, column),
                &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
                u32::try_from(probes).unwrap(),
                factor as u32,
            )
            .expect("pin the actual calibration lattice to the supplied point"),
    }
}

/// The quantization step of each dimension of the serving generation's
/// codec: the full-precision width one code step spans.
pub fn quantizer_steps(db: &Database, table: &str, column: &str, context: &[Value]) -> Vec<f32> {
    let state = db
        .native_ivf_maintenance_state_for_test(
            &contextdb_core::VectorIndexRef::new(table, column),
            &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        )
        .expect("inspect native codec manifest");
    // The persisted CIVF revision-one list format is unit-normalized signed
    // int8. This decodes its format, rather than implementing its encoder.
    state
        .manifest
        .map(|m| vec![1.0 / 127.0; m.dimensions])
        .unwrap_or_default()
}

/// The per-dimension codes the serving generation's codec writes for
/// `vector` in the list it is assigned to.
pub fn quantized_codes(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    vector: &[f32],
) -> Vec<i32> {
    let manifest = db
        .native_ivf_maintenance_state_for_test(
            &contextdb_core::VectorIndexRef::new(table, column),
            &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        )
        .expect("inspect serving codec")
        .manifest
        .expect("a serving generation");
    assert_eq!(manifest.dimensions, vector.len());
    let mut encoded = vec![0i8; vector.len()];
    contextdb_vector::ivf::quantized_query_codes_for_test(vector, &mut encoded);
    encoded.into_iter().map(i32::from).collect()
}

/// The list of the serving generation that holds the row with this primary
/// key.
pub fn assigned_list(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    key: &Value,
) -> u64 {
    let Value::Int64(key) = key else {
        panic!("the source selector requires an integer primary key")
    };
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let identity = db
        .native_ivf_source_identity_for_test(&index, *key)
        .expect("select actual row identity");
    let generation = serving(db, table, column, context)
        .expect("serving native generation")
        .generation;
    let mut list = None;
    let meta = db.table_meta(table).expect("declared table");
    let column = meta
        .columns
        .iter()
        .find(|c| c.name == column)
        .expect("declared vector column");
    let contextdb_core::ColumnType::Vector(dimensions) = column.column_type else {
        panic!("declared vector dimensions")
    };
    db.with_native_ivf_read_for_test(
        &index,
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        |scope| {
            scope.visit_physical_pages(&mut |page| {
                if page.key.generation == generation
                    && page.key.kind == contextdb_vector::ivf::storage::PageKind::Lists
                {
                    let count =
                        u32::from_le_bytes(page.bytes[112..116].try_into().unwrap()) as usize;
                    for record in page.payload.chunks_exact(dimensions + 16).take(count) {
                        if record[8..16] == identity.row.to_le_bytes() {
                            list = Some(u64::from(page.key.list));
                        }
                    }
                }
            })
        },
    )
    .expect("read actual row assignment");
    list.expect("source row has a serving list record")
}

/// The `n` lists whose centroids the serving generation ranks nearest to
/// `query`, nearest first: the lists an `n`-probe search reads.
pub fn nearest_lists(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
    query: &[f32],
    n: u64,
) -> Vec<u64> {
    let mut distances = Vec::new();
    db.with_native_ivf_read_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        |scope| {
            scope.visit_codebook_centroids(&mut |list, centroid| {
                distances.push((cosine_distance(query, centroid), u64::from(list)))
            })
        },
    )
    .expect("read actual trained centroids");
    distances.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    distances
        .into_iter()
        .take(n as usize)
        .map(|(_, list)| list)
        .collect()
}

/// Declares the indexed column's TRAINING_SEED through its engineering
/// policy: the seed of the deterministic reservoir sample and k-means
/// initialisation. Zero is a valid seed.
pub fn declare_training_seed(db: &Database, table: &str, column: &str, seed: u64) {
    db.execute(
        &format!("ALTER TABLE {table} ALTER COLUMN {column} SET WITH (training_seed = {seed})"),
        &empty(),
    )
    .unwrap_or_else(|error| panic!("declare TRAINING_SEED = {seed}: {error:?}"));
}

/// The work the build of one Context's serving generation did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildWork {
    /// Training iterations the build ran (its resolved TRAINING_ITERATIONS).
    pub training_iterations: u64,
    /// Vectors in the training reservoir (its resolved TRAINING_SAMPLES).
    pub training_samples: u64,
    /// Lists the trained codebook has.
    pub lists: u64,
    /// Rows the build assigned to lists.
    pub assigned_rows: u64,
    /// Distance evaluations spent training the codebook.
    pub training_distance_evaluations: u64,
    /// Distance evaluations spent assigning rows to lists.
    pub assignment_distance_evaluations: u64,
}

pub fn build_work(db: &Database, table: &str, column: &str, context: &[Value]) -> BuildWork {
    let state = db
        .native_ivf_maintenance_state_for_test(
            &contextdb_core::VectorIndexRef::new(table, column),
            &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        )
        .expect("read actual native construction state");
    let build = state.build.expect("persisted native build");
    BuildWork {
        training_iterations: build.training.iterations as u64,
        training_samples: build
            .manifest
            .slots
            .min(build.training.reservoir_samples as u64),
        lists: build.manifest.active_lists as u64,
        assigned_rows: if build.phase == contextdb_vector::ivf::storage::BuildPhase::Ready {
            build.manifest.slots
        } else {
            build.cursor
        },
        training_distance_evaluations: state.training_evaluations,
        assignment_distance_evaluations: state.assignment_evaluations,
    }
}

/// The pages the serving generation's list holds, base and sealed runs.
pub fn list_pages(db: &Database, table: &str, column: &str, context: &[Value], list: u64) -> u64 {
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    let generation = serving(db, table, column, context)
        .expect("serving native generation")
        .generation;
    let mut count = 0;
    db.with_native_ivf_read_for_test(&index, &tuple, |scope| {
        scope.visit_physical_pages(&mut |page| {
            if page.key.generation == generation
                && page.key.kind == contextdb_vector::ivf::storage::PageKind::Lists
                && u64::from(page.key.list) == list
            {
                count += 1;
            }
        })
    })
    .expect("count actual list pages");
    count
}

/// The training policy one Context's serving generation was built under, as
/// inspection reports it: the resolved TRAINING_ITERATIONS and the resolved
/// TRAINING_SAMPLES cap (the declared value or the engine default).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InspectedTraining {
    pub iterations: u64,
    pub sample_cap: u64,
    /// The declared TRAINING_SEED; `None` when undeclared.
    pub declared_seed: Option<u64>,
    /// The seed the next build uses: the declared one, or the seed derived
    /// from the partition's own identity when undeclared.
    pub effective_seed: u64,
    /// The seed the serving generation was trained with; `None` before the
    /// first publication.
    pub serving_seed: Option<u64>,
}

pub fn inspected_training(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> InspectedTraining {
    let index = contextdb_core::VectorIndexRef::new(table, column);
    let tuple = contextdb_core::VectorPartitionKey::from_values(context).unwrap();
    let meta = db.table_meta(table).expect("declared table");
    let column = meta
        .columns
        .iter()
        .find(|c| c.name == column)
        .expect("declared vector column");
    let resolved = db
        .native_ivf_resolved_policy_for_test(&index, &tuple)
        .expect("resolve actual next training inputs");
    let serving_seed = db
        .native_ivf_maintenance_state_for_test(&index, &tuple)
        .ok()
        .and_then(|state| state.serving_seed);
    InspectedTraining {
        iterations: resolved.training.iterations as u64,
        sample_cap: column
            .vector_engine_policy
            .training_samples
            .unwrap_or(300000),
        declared_seed: column.vector_engine_policy.training_seed,
        effective_seed: resolved.training.seed,
        serving_seed,
    }
}

/// One depth's search setting, as inspection lists it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DepthSetting {
    pub depth: u32,
    pub probes: u64,
    pub refine_factor: f64,
}

/// The per-depth (PROBES, REFINE_FACTOR) pairs inspection lists for one
/// Context's serving generation, in ascending depth order: each calibrated
/// depth's qualified pair, or, under an explicit IVF declaration, the declared
/// pair at every depth.
pub fn inspected_search_settings(
    db: &Database,
    table: &str,
    column: &str,
    context: &[Value],
) -> Vec<DepthSetting> {
    use contextdb_engine::vector_ivf_observations::NativeIvfQualificationRecordForTest as Record;
    let mut settings = Vec::new();
    db.with_native_ivf_read_for_test(
        &contextdb_core::VectorIndexRef::new(table, column),
        &contextdb_core::VectorPartitionKey::from_values(context).unwrap(),
        |scope| {
            scope.visit_qualification(&mut |record| {
                if let Record::Depth(depth) = record {
                    settings.push(DepthSetting {
                        depth: depth.depth,
                        probes: u64::from(depth.probes),
                        refine_factor: f64::from(depth.factor_hundredths) / 100.0,
                    });
                }
            })
        },
    )
    .expect("inspect each actual served depth's measured production pair");
    settings
}

/// Work from a real ReadSession file query, forwarded from its production
/// vector work record and actual native page callbacks (including workers).
pub fn observe_file_search<T>(
    _reader: &contextdb_engine::ReadSession,
    operation: impl FnOnce() -> T,
) -> (T, SearchWork) {
    observe_native_search(operation)
}

/// Fixed projection of the durable descriptor accessor. Inspection neither
/// warms a query cache nor emits normal-query load events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeServingDescriptor {
    pub bytes: [u8; 200],
    pub generation: u64,
}
pub fn native_ivf_serving_descriptor_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
) -> contextdb_core::Result<Option<NativeServingDescriptor>> {
    db.native_ivf_serving_descriptor_for_test(index, tuple)
        .map(|d| {
            d.map(|d| NativeServingDescriptor {
                bytes: d.bytes,
                generation: d.manifest.generation,
            })
        })
}
/// Projection of ServingDescriptorLoaded, QueryManifestSelected and
/// RootControlLoaded plus the existing generic generation counter. Copies
/// only fixed descriptor bytes; never supplies the query's manifest.
#[derive(Clone, Debug, Default)]
pub struct NativeDescriptorWork {
    pub descriptor_loads: u64,
    pub descriptor_bytes: u64,
    pub loaded: Option<[u8; 200]>,
    pub selected: Option<[u8; 200]>,
    pub selected_generation: Option<u64>,
    pub saved_generation_loads: u64,
    pub saved_generation_bytes: u64,
    pub root_control_loads: u64,
    pub root_control_bytes: u64,
    pub index_builds: u64,
    pub index_repairs: u64,
    pub quarantined: bool,
    pub route: Option<SearchRoute>,
    pub capture_complete: bool,
    pub overflow: bool,
}
pub fn observe_native_ivf_descriptor<T>(
    _db: &Database,
    operation: impl FnOnce() -> T,
) -> (T, NativeDescriptorWork) {
    use contextdb_engine::vector_ivf_observations::*;
    let state = Arc::new(std::sync::Mutex::new(NativeDescriptorWork::default()));
    let sink = state.clone();
    let (result, generic) = contextdb_engine::writer_query_test_seams::observe(|| {
        with_native_ivf_observer(
            NativeObserver {
                event: Arc::new(move |event| {
                    let mut state = sink.lock().unwrap();
                    match event {
                        NativeIvfSealEvent::ServingDescriptorLoaded { stored, .. } => {
                            state.descriptor_loads += 1;
                            state.descriptor_bytes += stored.len() as u64;
                            state.loaded = Some(*stored);
                        }
                        NativeIvfSealEvent::QueryManifestSelected {
                            descriptor,
                            manifest,
                            ..
                        } => {
                            state.selected = Some(*descriptor);
                            state.selected_generation = Some(manifest.generation);
                        }
                        NativeIvfSealEvent::RootControlLoaded { stored_bytes, .. } => {
                            state.root_control_loads += 1;
                            state.root_control_bytes += stored_bytes as u64;
                        }
                        NativeIvfSealEvent::QueryExecuted { indexed, .. } => {
                            state.route = Some(if indexed {
                                SearchRoute::Indexed
                            } else {
                                SearchRoute::Exact
                            })
                        }
                        NativeIvfSealEvent::BuildStarted { .. } => state.index_builds += 1,
                        NativeIvfSealEvent::QueryRejected { .. } => state.quarantined = true,
                        NativeIvfSealEvent::CaptureEnded {
                            attempted,
                            delivered,
                            producers_open,
                            overflow,
                            reentry,
                            ..
                        } => {
                            state.capture_complete = attempted == delivered
                                && producers_open == 0
                                && !overflow
                                && !reentry;
                            state.overflow |= overflow;
                        }
                        _ => {}
                    }
                }),
                boundary: Arc::new(|_| Ok(())),
            },
            operation,
        )
    });
    let mut observed = state.lock().unwrap().clone();
    observed.saved_generation_loads = generic.saved_graph_loads;
    observed.saved_generation_bytes = generic.saved_graph_bytes;
    (result, observed)
}
pub fn corrupt_native_ivf_serving_descriptor_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
    offset: usize,
    mask: u8,
) -> contextdb_core::Result<()> {
    db.corrupt_native_ivf_serving_descriptor_for_test(index, tuple, offset, mask)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeRowIdentity {
    pub row: u64,
    pub commit_lsn: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeJournalIdentity {
    pub initialized: bool,
    pub identity: Option<NativeRowIdentity>,
    pub deleted: bool,
    pub copies: u64,
}
/// The real authoritative row id/version selected by the SQL primary key;
/// no assumption equates the primary key, canonical slot and internal RowId.
pub fn native_ivf_source_identity_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    primary_key: i64,
) -> contextdb_core::Result<NativeRowIdentity> {
    db.native_ivf_source_identity_for_test(index, primary_key)
        .map(|i| NativeRowIdentity {
            row: i.row,
            commit_lsn: i.commit_lsn,
        })
}
pub fn native_ivf_journal_identity_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
    row: contextdb_core::RowId,
) -> contextdb_core::Result<NativeJournalIdentity> {
    db.native_ivf_journal_identity_for_test(index, tuple, row)
        .map(|j| NativeJournalIdentity {
            initialized: j.initialized,
            deleted: j.deleted,
            copies: j.copies,
            identity: j.identity.map(|i| NativeRowIdentity {
                row: i.row,
                commit_lsn: i.commit_lsn,
            }),
        })
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeBuildCursor {
    pub canonical: [u8; 32],
    pub membership: [u8; 24],
    pub ingested_vectors: u64,
    pub canonical_complete: bool,
}
pub fn native_ivf_build_progress_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
) -> contextdb_core::Result<NativeBuildCursor> {
    let progress = db.native_ivf_build_progress_for_test(index, tuple)?;
    let cursor = progress.source_cursor.ok_or_else(|| {
        contextdb_core::Error::Other("native build has no persisted source position".into())
    })?;
    Ok(NativeBuildCursor {
        canonical: cursor.canonical,
        membership: cursor.membership,
        ingested_vectors: progress.ingested_vectors,
        canonical_complete: progress.canonical_complete,
    })
}
/// SourceRowIngested identities delivered by the actual first bounded cycle.
/// The fixed recorder cannot silently drop events or grow with population.
#[derive(Clone, Debug)]
pub struct NativeIngestionWork {
    pub identities: [Option<NativeRowIdentity>; 16],
    pub count: usize,
    pub capture_complete: bool,
    pub overflow: bool,
}
pub fn observe_native_ivf_ingestion<T>(
    _db: &Database,
    operation: impl FnOnce() -> T,
) -> (T, NativeIngestionWork) {
    use contextdb_engine::vector_ivf_observations::*;
    let state = Arc::new(std::sync::Mutex::new(NativeIngestionWork {
        identities: [None; 16],
        count: 0,
        capture_complete: false,
        overflow: false,
    }));
    let sink = state.clone();
    let result = with_native_ivf_observer(
        NativeObserver {
            event: Arc::new(move |event| {
                let mut state = sink.lock().unwrap();
                match event {
                    NativeIvfSealEvent::SourceRowIngested { identity, .. } => {
                        let n = state.count;
                        if n < state.identities.len() {
                            state.identities[n] = Some(NativeRowIdentity {
                                row: identity.row,
                                commit_lsn: identity.commit_lsn,
                            });
                        } else {
                            state.overflow = true;
                        }
                        state.count += 1;
                    }
                    NativeIvfSealEvent::CaptureEnded {
                        attempted,
                        delivered,
                        producers_open,
                        overflow,
                        reentry,
                        ..
                    } => {
                        state.capture_complete =
                            attempted == delivered && producers_open == 0 && !overflow && !reentry;
                        state.overflow |= overflow;
                    }
                    _ => {}
                }
            }),
            boundary: Arc::new(|_| Ok(())),
        },
        operation,
    );
    let observed = state.lock().unwrap().clone();
    (result, observed)
}
#[derive(Clone, Copy, Debug)]
pub enum NativeConditionalMutation<'a> {
    Update(&'a [f32]),
    Delete,
}
/// Invokes the real adapter compare-existing mutation under an aborting outer
/// transaction with exactly this expected identity. Never retries, initializes
/// an initialized record, substitutes an expected version or commits a fake row.
pub fn try_native_ivf_conditional_mutation_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
    expected: NativeRowIdentity,
    mutation: NativeConditionalMutation<'_>,
) -> contextdb_core::Result<()> {
    use contextdb_engine::vector_ivf_observations::*;
    let resolved = db.native_ivf_resolved_policy_for_test(index, tuple)?;
    db.with_aborting_native_ivf_write_for_test(
        index,
        tuple,
        NativeIvfDriveCallbacks {
            checkpoint: &|| Ok(()),
            before_distance: &|_| Ok(()),
        },
        |scope| {
            scope.with_clustered_writer(resolved.policy, resolved.training, |writer| {
                let expected = contextdb_vector::ivf::RowIdentity {
                    row: expected.row,
                    commit_lsn: expected.commit_lsn,
                };
                match mutation {
                    NativeConditionalMutation::Update(values) => {
                        writer.commit_update(0, expected, values).map(|_| ())
                    }
                    NativeConditionalMutation::Delete => writer.delete_row(0, expected),
                }
            })
        },
    )
}
/// Installs the actual AfterAuthoritativeAndJournalStaged before-commit fault
/// once for the chosen row; records that it fired after both writes staged.
pub fn with_native_ivf_after_staged_failure_for_test<T>(
    row: contextdb_core::RowId,
    operation: impl FnOnce() -> T,
) -> (T, bool) {
    use contextdb_engine::vector_ivf_observations::*;
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sink = fired.clone();
    let result = with_native_ivf_observer(
        NativeObserver {
            event: Arc::new(|_| ()),
            boundary: Arc::new(move |boundary| {
                if matches!(boundary,NativeIvfFaultBoundary::AfterAuthoritativeAndJournalStaged{identity,..} if identity.row==row.0)
                    && !sink.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(contextdb_core::Error::Other(
                        "injected failure after authoritative and native journal staging".into(),
                    ));
                }
                Ok(())
            }),
        },
        operation,
    );
    (result, fired.load(std::sync::atomic::Ordering::SeqCst))
}

/// Fixed projections of the producer-owned exported allocation records. Each
/// Layout below comes directly from the shared producer calculation consumed
/// by its real constructor; support must not copy a private size formula.
#[derive(Clone, Copy, Debug)]
pub struct ProducerCodebookLayouts {
    pub control: std::alloc::Layout,
    pub values: std::alloc::Layout,
    pub counts: std::alloc::Layout,
    pub charged_bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct ProducerWorkspaceLayouts {
    pub controls: std::alloc::Layout,
    pub buffer: std::alloc::Layout,
    pub raw: std::alloc::Layout,
    pub values: std::alloc::Layout,
    pub encoded: std::alloc::Layout,
    pub order: std::alloc::Layout,
    pub combined: std::alloc::Layout,
    pub page_scratch: std::alloc::Layout,
    pub catalog: std::alloc::Layout,
    pub buffer_record_bytes: usize,
    pub charged_bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct NativePolicyInputs {
    pub dimensions: usize,
    pub lists: u32,
    pub probes: u32,
    pub rerank: usize,
    pub buffer_bytes: usize,
    pub extend_to_k: bool,
    pub training_iterations: u32,
    pub training_samples: u64,
    pub training_seed: u64,
    pub ingest_batch_keys: usize,
    pub phase_batch_entries: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeResolvedPolicy {
    pub inputs: NativePolicyInputs,
    pub active_lists: u32,
    pub population: u64,
    pub expected_vectors: u64,
    pub build_budget_bytes: u64,
    pub codebook_budget_bytes: u64,
    /// Actual independent remaining whole-workspace allowance after the
    /// codebook/output/other-owner reservations, before choosing the buffer.
    pub workspace_budget_bytes: usize,
    pub codebook: ProducerCodebookLayouts,
    pub workspace: ProducerWorkspaceLayouts,
}
pub fn native_ivf_resolved_policy_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
) -> contextdb_core::Result<NativeResolvedPolicy> {
    let r = db.native_ivf_resolved_policy_for_test(index, tuple)?;
    let p = r.policy;
    let t = r.training;
    let inputs = NativePolicyInputs {
        dimensions: p.dimensions(),
        lists: p.lists(),
        probes: p.probes(),
        rerank: p.rerank(),
        buffer_bytes: p.buffer_cap_bytes(),
        extend_to_k: p.extend_to_k(),
        training_iterations: t.iterations,
        training_samples: t.reservoir_samples as u64,
        training_seed: t.seed,
        ingest_batch_keys: t.ingest_batch_keys,
        phase_batch_entries: t.phase_batch_entries,
    };
    Ok(NativeResolvedPolicy {
        inputs,
        active_lists: r.active_lists,
        population: r.population,
        expected_vectors: r.expected_vectors,
        build_budget_bytes: r.build_budget_bytes,
        codebook_budget_bytes: r.codebook_budget_bytes,
        workspace_budget_bytes: r.workspace_budget_bytes,
        codebook: codebook_allocation_layout(p.dimensions(), r.active_lists, true)?,
        workspace: workspace_allocation_layout(inputs)?,
    })
}
pub fn codebook_allocation_layout(
    dimensions: usize,
    active_lists: u32,
    counts: bool,
) -> contextdb_core::Result<ProducerCodebookLayouts> {
    let l = contextdb_vector::ivf::codebook_allocation_layout(dimensions, active_lists, counts)?;
    Ok(ProducerCodebookLayouts {
        control: l.control,
        values: l.values,
        counts: l.counts,
        charged_bytes: l.charged_bytes,
    })
}
pub fn workspace_allocation_layout(
    i: NativePolicyInputs,
) -> contextdb_core::Result<ProducerWorkspaceLayouts> {
    let policy = contextdb_vector::ivf::Policy::checked(
        i.dimensions,
        i.lists,
        i.probes,
        i.rerank,
        i.buffer_bytes,
        i.extend_to_k,
    )
    .ok_or_else(|| contextdb_core::Error::Other("invalid policy projection".into()))?;
    let training = contextdb_vector::ivf::TrainingPolicy {
        iterations: i.training_iterations,
        reservoir_samples: i.training_samples as usize,
        seed: i.training_seed,
        ingest_batch_keys: i.ingest_batch_keys,
        phase_batch_entries: i.phase_batch_entries,
    };
    let l = contextdb_vector::ivf::workspace_allocation_layout(policy, training)?;
    Ok(ProducerWorkspaceLayouts {
        controls: l.controls,
        buffer: l.buffer,
        raw: l.raw,
        values: l.values,
        encoded: l.encoded,
        order: l.order,
        combined: l.combined,
        page_scratch: l.page_scratch,
        catalog: l.catalog,
        buffer_record_bytes: l.buffer_record_bytes,
        charged_bytes: l.charged_bytes,
    })
}
#[derive(Clone, Copy, Debug)]
pub enum ConstructorAllocation {
    Codebook { training_counts: bool },
    WriterWorkspace,
}
#[derive(Clone, Copy, Debug)]
pub enum ConstructorEvent {
    Debited(usize),
    Allocated,
    PhysicallyFreed,
    Released(usize),
}
#[derive(Debug)]
pub struct ConstructorAttempt {
    pub outcome: contextdb_core::Result<()>,
    pub events: [Option<ConstructorEvent>; 16],
    pub event_count: usize,
    pub retained_after_drop: usize,
    pub overflow: bool,
}
/// Drives the real producer constructor through its existing native
/// read/build or WriteScope::with_clustered_writer loan. Its original target
/// owner has exactly available bytes; independently invoiced source frames
/// remain with their real owners. This is not a trial allocation in DEFAULT
/// resolution. Observe actual debit/allocation/physical-free/release boundaries
/// and IvfConstructorObserverForTest; do not construct a fixture object.
pub fn native_ivf_constructor_attempt_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
    allocation: ConstructorAllocation,
    available: usize,
) -> ConstructorAttempt {
    use contextdb_vector::ivf::{
        IvfConstructorAllocationForTest as Allocation, IvfConstructorObserverForTest as Observer,
    };
    let resolved = db
        .native_ivf_resolved_policy_for_test(index, tuple)
        .expect("resolve actual native tuple");
    if matches!(allocation, ConstructorAllocation::Codebook { .. }) && resolved.active_lists == 0 {
        return ConstructorAttempt {
            outcome: Ok(()),
            events: [None; 16],
            event_count: 0,
            retained_after_drop: 0,
            overflow: false,
        };
    }
    struct State {
        events: [Option<ConstructorEvent>; 16],
        count: usize,
        held: usize,
        invoice: Option<u64>,
        constructed: bool,
        overflow: bool,
    }
    impl State {
        fn push(&mut self, event: ConstructorEvent) {
            if self.count == self.events.len() {
                self.overflow = true;
            } else {
                self.events[self.count] = Some(event);
                self.count += 1;
            }
        }
    }
    struct Capture {
        target: ConstructorAllocation,
        state: Arc<std::sync::Mutex<State>>,
    }
    impl Observer for Capture {
        fn admitted(&self, _: Allocation) {}
        fn debited(&self, invoice: u64, owner: usize, bytes: usize, operation: &'static str) {
            let selected = matches!(
                (self.target, operation),
                (
                    ConstructorAllocation::WriterWorkspace,
                    "clustered_writer_workspace"
                ) | (ConstructorAllocation::Codebook { .. }, "clustered_codebook")
            );
            if selected {
                assert_ne!(owner, 0);
                let mut state = self.state.lock().unwrap();
                if state.invoice.is_some() {
                    state.overflow = true;
                    return;
                }
                state.invoice = Some(invoice);
                state.held = bytes;
                state.push(ConstructorEvent::Debited(bytes));
            }
        }
        fn constructed(&self, allocation: Allocation) {
            let selected = match (self.target, allocation) {
                (ConstructorAllocation::WriterWorkspace, Allocation::WriterWorkspace(_)) => true,
                (
                    ConstructorAllocation::Codebook { training_counts },
                    Allocation::Codebook(layout),
                ) => training_counts == (layout.counts.size() != 0),
                _ => false,
            };
            if selected {
                let mut state = self.state.lock().unwrap();
                if state.invoice.is_none() || state.constructed {
                    state.overflow = true;
                    return;
                }
                state.constructed = true;
                state.push(ConstructorEvent::Allocated);
            }
        }
        fn buffers_freed(
            &self,
            invoice: u64,
            _owner: usize,
            _bytes: usize,
            _operation: &'static str,
        ) {
            let mut state = self.state.lock().unwrap();
            if state.invoice == Some(invoice) && state.constructed {
                state.push(ConstructorEvent::PhysicallyFreed);
            }
        }
        fn released(&self, invoice: u64, _owner: usize, bytes: usize, _operation: &'static str) {
            let mut state = self.state.lock().unwrap();
            if state.invoice == Some(invoice) {
                if state.held != bytes {
                    state.overflow = true;
                } else {
                    state.held = 0;
                }
                state.push(ConstructorEvent::Released(bytes));
            }
        }
    }
    let state = Arc::new(std::sync::Mutex::new(State {
        events: [None; 16],
        count: 0,
        held: 0,
        invoice: None,
        constructed: false,
        overflow: false,
    }));
    let target=match allocation {
        ConstructorAllocation::Codebook{training_counts}=>contextdb_engine::vector_ivf_observations::NativeIvfConstructorTargetForTest::Codebook{training_counts},
        ConstructorAllocation::WriterWorkspace=>contextdb_engine::vector_ivf_observations::NativeIvfConstructorTargetForTest::WriterWorkspace,
    };
    let outcome = db.native_ivf_probe_constructor_for_test(
        index,
        tuple,
        target,
        available,
        Arc::new(Capture {
            target: allocation,
            state: state.clone(),
        }),
    );
    let state = state.lock().unwrap();
    ConstructorAttempt {
        outcome,
        events: state.events,
        event_count: state.count,
        retained_after_drop: state.held,
        overflow: state.overflow,
    }
}

/// Borrowed projections of NativeIvfSealEvent. Outer identity combines the
/// admitted write-session identity with its actual outer-write object; an
/// operation number alone is never treated as globally unique.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeSealOperation {
    pub session: u64,
    pub outer: usize,
    pub operation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeSealPage {
    pub storage: u64,
    pub partition: u64,
    pub generation: u64,
    pub kind: u8,
    pub list: u32,
    pub ordinal: u64,
    pub run: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct NativeSealWindow {
    pub offset: usize,
    pub layout: std::alloc::Layout,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeSealObjectKind {
    Scope,
    SealWorkspace,
}
#[derive(Clone, Copy, Debug)]
pub enum NativeSealEvent<'a> {
    CaptureBegan {
        capture: u64,
        attempted: u64,
        delivered: u64,
        producers_open: u64,
        overflow: bool,
        reentry: bool,
    },
    CaptureEnded {
        capture: u64,
        attempted: u64,
        delivered: u64,
        producers_open: u64,
        overflow: bool,
        reentry: bool,
    },
    MarkerInserted {
        operation: NativeSealOperation,
        key: NativeSealPage,
        newly_inserted: bool,
    },
    MarkerRemoved {
        operation: NativeSealOperation,
        key: NativeSealPage,
    },
    PayloadWritten {
        operation: NativeSealOperation,
        key: NativeSealPage,
    },
    HeaderSealed {
        operation: NativeSealOperation,
        key: NativeSealPage,
        header: &'a [u8; 192],
        payload: &'a [u8; 3904],
    },
    PageOpened {
        key: NativeSealPage,
        header: &'a [u8; 192],
        payload: &'a [u8; 3904],
    },
    OperationPrefixChecked {
        operation: NativeSealOperation,
        after_drain: bool,
        empty: bool,
    },
    GlobalPendingChecked {
        before_final_binding: bool,
        empty: bool,
    },
    KindHandlesClosed {
        operation: NativeSealOperation,
        kind: u8,
    },
    FamilyFingerprintCaptured {
        operation: NativeSealOperation,
        kind: u8,
        root: [u8; 32],
    },
    ScopeWritten {
        operation: NativeSealOperation,
        present: u16,
        expected_roots: &'a [[u8; 32]; 16],
    },
    ConfigurationBound,
    OuterCommitted {
        actual_commit_lsn: u64,
    },
    OuterAborted,
    ObjectAdmitted {
        instance: u64,
        kind: NativeSealObjectKind,
        layout: std::alloc::Layout,
        original_owner: usize,
        adapter_owner: usize,
        windows: Option<&'a [NativeSealWindow; 3]>,
    },
    ObjectReleased {
        instance: u64,
        kind: NativeSealObjectKind,
        layout: std::alloc::Layout,
        original_owner: usize,
        adapter_owner: usize,
    },
    /// Every otherwise unselected producer event still counts as delivered;
    /// a projection must not silently drop it from capture completeness.
    Other,
}
/// Direct forwarding of the intended scoped NativeIvfSealObserver, including
/// callbacks from actual executing threads. No producer log or page is retained.
pub fn with_native_ivf_seal_observer_for_test<T>(
    observer: Arc<dyn for<'a> Fn(NativeSealEvent<'a>) + Send + Sync>,
    operation: impl FnOnce() -> T,
) -> T {
    use contextdb_engine::vector_ivf_observations::*;
    let projection = std::sync::Mutex::new(NativeSealProjection {
        operations: [None; 32],
        latest: None,
        overflow: false,
    });
    with_native_ivf_observer(
        NativeObserver {
            event: Arc::new(move |event| {
                let mut p = projection.lock().unwrap();
                match event {
                    NativeIvfSealEvent::CaptureBegan {
                        capture,
                        attempted,
                        delivered,
                        producers_open,
                        overflow,
                        reentry,
                    } => observer(NativeSealEvent::CaptureBegan {
                        capture,
                        attempted,
                        delivered,
                        producers_open,
                        overflow,
                        reentry,
                    }),
                    NativeIvfSealEvent::CaptureEnded {
                        capture,
                        attempted,
                        delivered,
                        producers_open,
                        overflow,
                        reentry,
                    } => observer(NativeSealEvent::CaptureEnded {
                        capture,
                        attempted,
                        delivered,
                        producers_open,
                        overflow: overflow || p.overflow,
                        reentry,
                    }),
                    NativeIvfSealEvent::MarkerInserted {
                        operation,
                        key,
                        newly_inserted,
                    } => {
                        if let Some(id) = native_operation(operation) {
                            p.remember(id);
                            observer(NativeSealEvent::MarkerInserted {
                                operation: id,
                                key: native_page(operation, key),
                                newly_inserted,
                            });
                        } else {
                            observer(NativeSealEvent::Other)
                        }
                    }
                    NativeIvfSealEvent::MarkerRemoved { operation, key } => {
                        if let Some(id) = native_operation(operation) {
                            p.remember(id);
                            observer(NativeSealEvent::MarkerRemoved {
                                operation: id,
                                key: native_page(operation, key),
                            });
                        } else {
                            observer(NativeSealEvent::Other)
                        }
                    }
                    NativeIvfSealEvent::PayloadWritten { operation, key } => {
                        if let Some(id) = native_operation(operation) {
                            p.remember(id);
                            observer(NativeSealEvent::PayloadWritten {
                                operation: id,
                                key: native_page(operation, key),
                            });
                        } else {
                            observer(NativeSealEvent::Other)
                        }
                    }
                    NativeIvfSealEvent::HeaderSealed {
                        operation,
                        key,
                        header,
                        payload,
                        ..
                    } => {
                        if let Some(id) = native_operation(operation) {
                            p.remember(id);
                            observer(NativeSealEvent::HeaderSealed {
                                operation: id,
                                key: native_page(operation, key),
                                header,
                                payload,
                            });
                        } else {
                            observer(NativeSealEvent::Other)
                        }
                    }
                    NativeIvfSealEvent::PageOpened {
                        operation,
                        key,
                        header,
                        payload,
                    } => observer(NativeSealEvent::PageOpened {
                        key: native_page(operation, key),
                        header,
                        payload,
                    }),
                    NativeIvfSealEvent::OperationPrefixChecked {
                        source_instance,
                        scope_key: _,
                        operation,
                        empty,
                        stage,
                        ..
                    } => {
                        if let NativeIvfSourceInstance::Write {
                            admitted_session,
                            outer_write_object,
                        } = source_instance
                        {
                            let id = NativeSealOperation {
                                session: admitted_session,
                                outer: outer_write_object,
                                operation,
                            };
                            p.remember(id);
                            observer(NativeSealEvent::OperationPrefixChecked {
                                operation: id,
                                after_drain: stage == NativeIvfOperationPrefixStage::AfterDrain,
                                empty,
                            });
                        } else {
                            observer(NativeSealEvent::Other)
                        }
                    }
                    NativeIvfSealEvent::GlobalPendingChecked { empty, stage, .. } => {
                        observer(NativeSealEvent::GlobalPendingChecked {
                            before_final_binding: stage
                                == NativeIvfGlobalPendingStage::BeforeFinalBinding,
                            empty,
                        })
                    }
                    NativeIvfSealEvent::KindHandlesClosed { operation, kind } => {
                        if let Some(id) = native_operation(operation) {
                            p.remember(id);
                            observer(NativeSealEvent::KindHandlesClosed {
                                operation: id,
                                kind: native_kind_tag(kind),
                            });
                        } else {
                            observer(NativeSealEvent::Other)
                        }
                    }
                    NativeIvfSealEvent::FamilyFingerprintCaptured {
                        operation,
                        kind,
                        root,
                    } => {
                        if let Some(id) = native_operation(operation) {
                            if let Some(op) = p.remember(id) {
                                op.generations[usize::from(native_kind_tag(kind) - 1)] =
                                    Some(operation.generation);
                            }
                            observer(NativeSealEvent::FamilyFingerprintCaptured {
                                operation: id,
                                kind: native_kind_tag(kind),
                                root,
                            });
                        } else {
                            observer(NativeSealEvent::Other)
                        }
                    }
                    NativeIvfSealEvent::ScopeWritten {
                        operation: Some(operation),
                        stored,
                        ..
                    } => {
                        if let Some(latest) = p.latest.filter(|id| id.operation == operation) {
                            let generations = p.remember(latest).map(|op| op.generations);
                            if let Some(generations) = generations {
                                let mut roots = [[0; 32]; 16];
                                let mut present = 0u16;
                                for (at, generation) in generations.into_iter().enumerate() {
                                    if let Some(generation) = generation {
                                        if let Some(family) = stored
                                            .families
                                            .iter()
                                            .flatten()
                                            .find(|f| f.generation == generation)
                                        {
                                            roots[at] = family.roots[at];
                                            present |= family.present & (1u16 << at);
                                        } else {
                                            p.overflow = true;
                                        }
                                    }
                                }
                                observer(NativeSealEvent::ScopeWritten {
                                    operation: latest,
                                    present,
                                    expected_roots: &roots,
                                });
                            } else {
                                observer(NativeSealEvent::Other)
                            }
                        } else {
                            observer(NativeSealEvent::Other)
                        }
                    }
                    NativeIvfSealEvent::ConfigurationBound { .. } => {
                        observer(NativeSealEvent::ConfigurationBound)
                    }
                    NativeIvfSealEvent::OuterCommitted { actual_commit_lsn } => {
                        observer(NativeSealEvent::OuterCommitted {
                            actual_commit_lsn: actual_commit_lsn.0,
                        })
                    }
                    NativeIvfSealEvent::OuterAborted => observer(NativeSealEvent::OuterAborted),
                    NativeIvfSealEvent::ObjectAdmitted {
                        instance,
                        kind,
                        layout,
                        original_owner,
                        adapter_owner,
                        windows,
                        ..
                    } => {
                        let windows = windows.map(|w| {
                            w.map(|r| NativeSealWindow {
                                offset: r.offset,
                                layout: r.layout,
                            })
                        });
                        observer(NativeSealEvent::ObjectAdmitted {
                            instance: instance.0,
                            kind: native_object_kind(kind),
                            layout,
                            original_owner,
                            adapter_owner,
                            windows: windows.as_ref(),
                        });
                    }
                    NativeIvfSealEvent::ObjectReleased {
                        instance,
                        kind,
                        layout,
                        original_owner,
                        adapter_owner,
                        ..
                    } => observer(NativeSealEvent::ObjectReleased {
                        instance: instance.0,
                        kind: native_object_kind(kind),
                        layout,
                        original_owner,
                        adapter_owner,
                    }),
                    _ => observer(NativeSealEvent::Other),
                }
            }),
            boundary: Arc::new(|_| Ok(())),
        },
        operation,
    )
}
pub fn native_ivf_original_owner_for_test(db: &Database) -> usize {
    db.native_ivf_original_owner_for_test()
        .expect("retained original native payer")
}
/// Control-only cases invoke the real always-aborting WriteScope driver with
/// explicit checkpoint/distance callbacks and actual PageStore::begin,
/// put_page/stage_build/publish_manifest, then WritePages::commit. The enclosing
/// authoritative write ALWAYS aborts. Cases describe actual controls/payloads,
/// never expected header values, counts, roots or a fixture sealer. The supplied
/// cancellation is passed through the real checkpoint and distance callbacks;
/// neither callback may be replaced by an always-successful function.
#[derive(Clone, Copy, Debug)]
pub enum NativeSemanticCase {
    CanonicalCrossing,
    CanonicalContinuation,
    NonconsecutiveListRecords,
    SlotMetadataCrossing,
    DirectoryBoundary,
    DirectoryPresenceExtent,
    VisibilityBoundary,
    CodebookCoordinates,
    AccumulatorBoundary,
    ReservoirBoundary,
    CatalogHeaderAndRun,
    RetainedTail,
    ListUsedBoundary,
    ListLinksBoundary,
    AllocatorBoundary,
    SparseRowVersions,
    SparseRowCopies,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeSemanticSetup {
    pub dimensions: usize,
    pub slots: u64,
    pub lists: u32,
    pub active_lists: u32,
    pub samples: u64,
    pub tail_head: u64,
    pub tail_end: u64,
    pub allocated_list_pages: u64,
    pub retained_run: u64,
    pub sparse_records: [Option<(u64, u64, u8)>; 3],
}
#[derive(Clone, Copy, Debug)]
pub struct NativeSemanticDriveResult {
    /// Actual final control-change operation and actual target page identities;
    /// no expected counts/header bytes are supplied by the implementation.
    pub final_operation: NativeSealOperation,
    pub targets: [Option<NativeSealPage>; 3],
}
pub fn drive_aborting_native_ivf_semantic_case_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
    case: NativeSemanticCase,
    setup: NativeSemanticSetup,
    cancellation: &contextdb_core::read_contract::OwnerReadCancellation,
) -> contextdb_core::Result<NativeSemanticDriveResult> {
    use NativeSemanticCase::*;
    use contextdb_engine::vector_ivf_observations::*;
    use contextdb_vector::ivf::{
        TrainingPolicy,
        storage::{
            BuildPhase, BuildState, Manifest, PAGE_BYTES, PageKey, PageKind, PageStore, ReadPages,
            WritePages,
        },
    };
    let manifest = Manifest {
        partition: 0,
        generation: 1,
        dimensions: setup.dimensions,
        lists: setup.lists.max(1),
        active_lists: setup.active_lists,
        slots: setup.slots.max(setup.tail_end),
        sealed_slots: 0,
        codebook: [0; 32],
        run: 0,
    };
    let key = |kind, ordinal| manifest.key(kind, 0, ordinal);
    let mut payloads: Vec<(PageKey, [u8; PAGE_BYTES])> = Vec::with_capacity(6);
    let mut targets = [None; 3];
    let mut add = |key: PageKey, bytes: [u8; PAGE_BYTES], target: Option<usize>| {
        payloads.push((key, bytes));
        if let Some(at) = target {
            targets[at] = Some(key);
        }
    };
    let zero = [0; PAGE_BYTES];
    match case {
        CanonicalCrossing | CanonicalContinuation => {
            let pages =
                (setup.slots * (setup.dimensions as u64 * 4 + 8)).div_ceil(PAGE_BYTES as u64);
            for ordinal in 0..pages {
                add(
                    key(PageKind::Canonical, ordinal),
                    zero,
                    Some(ordinal as usize),
                );
            }
        }
        SlotMetadataCrossing => {
            for ordinal in 0..(setup.slots * 40).div_ceil(PAGE_BYTES as u64) {
                add(
                    key(PageKind::SlotMetadata, ordinal),
                    zero,
                    Some(ordinal as usize),
                );
            }
        }
        DirectoryBoundary => {
            for ordinal in 0..u64::from(setup.lists).div_ceil((PAGE_BYTES / 24) as u64) {
                add(
                    key(PageKind::Directory, ordinal),
                    zero,
                    Some(ordinal as usize),
                );
            }
        }
        DirectoryPresenceExtent => {
            let mut bytes = zero;
            bytes[0] = 3;
            add(key(PageKind::DirectoryPresence, 0), bytes, Some(0));
        }
        VisibilityBoundary => {
            for ordinal in 0..setup.slots.div_ceil((PAGE_BYTES * 8) as u64) {
                add(
                    key(PageKind::Visibility, ordinal),
                    zero,
                    Some(ordinal as usize),
                );
            }
        }
        CodebookCoordinates => {
            add(key(PageKind::Codebook, 0), zero, Some(0));
            for ordinal in 0..(setup.dimensions as u64 * u64::from(setup.active_lists))
                .div_ceil((PAGE_BYTES / 4) as u64)
            {
                add(
                    key(PageKind::Codebook, ordinal + 1),
                    zero,
                    Some(ordinal as usize + 1),
                );
            }
        }
        AccumulatorBoundary => {
            for ordinal in 0..u64::from(setup.active_lists).div_ceil((PAGE_BYTES / 8) as u64) {
                add(
                    key(PageKind::Accumulators, ordinal),
                    zero,
                    Some(ordinal as usize),
                );
            }
        }
        ReservoirBoundary => {
            for ordinal in 0..setup
                .samples
                .min(setup.slots)
                .div_ceil((PAGE_BYTES / 8) as u64)
            {
                add(
                    key(PageKind::Reservoir, ordinal),
                    zero,
                    Some(ordinal as usize),
                );
            }
        }
        CatalogHeaderAndRun => {
            let mut catalog = zero;
            catalog[..8].copy_from_slice(b"IVFCAT02");
            add(key(PageKind::Catalog, 0), catalog, Some(0));
            let mut run = key(PageKind::Catalog, setup.retained_run);
            run.list = 1;
            let mut bytes = zero;
            bytes[..8].copy_from_slice(b"IVFCAT02");
            add(run, bytes, Some(1));
        }
        RetainedTail => {
            let mut catalog = zero;
            catalog[..8].copy_from_slice(b"IVFCAT02");
            catalog[6 * 8..7 * 8].copy_from_slice(&setup.tail_head.to_le_bytes());
            catalog[7 * 8..8 * 8].copy_from_slice(&setup.tail_end.to_le_bytes());
            add(key(PageKind::Catalog, 0), catalog, None);
            for ordinal in 0..setup.tail_end.div_ceil((PAGE_BYTES / 8) as u64) {
                add(key(PageKind::Tail, ordinal), zero, Some(ordinal as usize));
            }
        }
        ListUsedBoundary | ListLinksBoundary => {
            let mut allocator = zero;
            allocator[..8].copy_from_slice(&setup.allocated_list_pages.to_le_bytes());
            add(key(PageKind::ListAllocator, 0), allocator, None);
            let kind = if matches!(case, ListUsedBoundary) {
                PageKind::ListUsed
            } else {
                PageKind::ListLinks
            };
            for ordinal in 0..setup.allocated_list_pages.div_ceil((PAGE_BYTES / 8) as u64) {
                add(key(kind, ordinal), zero, Some(ordinal as usize));
            }
        }
        AllocatorBoundary => {
            for ordinal in 0..u64::from(setup.lists).div_ceil((PAGE_BYTES / 8) as u64) {
                add(
                    key(PageKind::ListAllocator, ordinal),
                    zero,
                    Some(ordinal as usize),
                );
            }
        }
        NonconsecutiveListRecords => {
            let mut allocator = zero;
            allocator[..8].copy_from_slice(&1u64.to_le_bytes());
            add(key(PageKind::ListAllocator, 0), allocator, None);
            let mut used = zero;
            used[..8].copy_from_slice(&3u64.to_le_bytes());
            add(key(PageKind::ListUsed, 0), used, None);
            add(key(PageKind::ListLinks, 0), zero, None);
            let mut list = zero;
            for (at, slot) in [0u64, 2, 4].into_iter().enumerate() {
                let offset = at * (setup.dimensions + 16);
                list[offset..offset + 8].copy_from_slice(&slot.to_le_bytes());
                list[offset + 8..offset + 16].copy_from_slice(&(slot + 1).to_le_bytes());
            }
            add(key(PageKind::Lists, 0), list, Some(0));
        }
        SparseRowVersions | SparseRowCopies => {
            let (kind, width, flags) = if matches!(case, SparseRowVersions) {
                (PageKind::RowVersions, 40, 32)
            } else {
                (PageKind::RowCopies, 24, 0)
            };
            let capacity = PAGE_BYTES / width;
            for ordinal in 0..=setup
                .sparse_records
                .into_iter()
                .flatten()
                .map(|(address, _, _)| address / capacity as u64)
                .max()
                .unwrap_or(0)
            {
                let mut bytes = zero;
                for (address, value, flag) in setup
                    .sparse_records
                    .into_iter()
                    .flatten()
                    .filter(|(address, _, _)| address / capacity as u64 == ordinal)
                {
                    let offset = address as usize % capacity * width;
                    bytes[offset + flags..offset + flags + 8]
                        .copy_from_slice(&u64::from(flag).to_le_bytes());
                    let value_at = if flags == 0 { 8 } else { 0 };
                    bytes[offset + value_at..offset + value_at + 8]
                        .copy_from_slice(&value.to_le_bytes());
                }
                let mut page = key(kind, ordinal);
                page.generation = 0;
                if kind == PageKind::RowCopies {
                    page.run = setup.retained_run;
                }
                add(page, bytes, Some(ordinal as usize));
            }
        }
    }
    let captured = Arc::new(std::sync::Mutex::new((None, [None; 3], targets)));
    let seen = captured.clone();
    let checkpoint = || {
        if cancellation.is_cancelled() {
            Err(contextdb_core::Error::ReadCancelled)
        } else {
            Ok(())
        }
    };
    with_native_ivf_observer(
        NativeObserver {
            event: Arc::new(move |event| {
                if let NativeIvfSealEvent::HeaderSealed { operation, key, .. } = event {
                    let mut seen = seen.lock().unwrap();
                    for at in 0..3 {
                        if seen.2[at] == Some(key) {
                            seen.0 = native_operation(operation);
                            seen.1[at] = Some(native_page(operation, key));
                        }
                    }
                }
            }),
            boundary: Arc::new(|_| Ok(())),
        },
        || {
            db.with_aborting_native_ivf_write_for_test(
                index,
                tuple,
                NativeIvfDriveCallbacks {
                    checkpoint: &checkpoint,
                    before_distance: &|_| checkpoint(),
                },
                |scope| {
                    let source_lsn = scope.snapshot_lsn()?;
                    let generation = scope.manifest(0)?.map_or(Ok(1), |m| {
                        m.generation.checked_add(1).ok_or_else(|| {
                            contextdb_core::Error::Other("generation overflow".into())
                        })
                    })?;
                    let manifest = Manifest {
                        generation,
                        ..manifest
                    };
                    let mut payloads = payloads;
                    for (key, _) in &mut payloads {
                        if key.generation != 0 {
                            key.generation = generation;
                        }
                    }
                    let mut targets = targets;
                    for key in targets.iter_mut().flatten() {
                        if key.generation != 0 {
                            key.generation = generation;
                        }
                    }
                    captured.lock().unwrap().2 = targets;
                    let mut tx = scope.begin()?;
                    tx.stage_build(BuildState {
                        manifest,
                        phase: BuildPhase::Ingest,
                        cursor: 0,
                        iteration: 0,
                        training: TrainingPolicy {
                            iterations: 1,
                            reservoir_samples: setup.samples.max(1) as usize,
                            seed: 0,
                            ingest_batch_keys: 1,
                            phase_batch_entries: 1,
                        },
                        serving_at_start: None,
                        source_lsn,
                        publication_coalesce: None,
                    })?;
                    tx.publish_manifest(manifest)?;
                    for (key, bytes) in &payloads {
                        tx.put_page(*key, bytes)?;
                    }
                    tx.commit()?;
                    if matches!(case, RetainedTail) {
                        let replacement = Manifest {
                            generation: generation.checked_add(1).ok_or_else(|| {
                                contextdb_core::Error::Other("generation overflow".into())
                            })?,
                            slots: 0,
                            ..manifest
                        };
                        let mut tx = scope.begin()?;
                        tx.stage_build(BuildState {
                            manifest: replacement,
                            phase: BuildPhase::Ingest,
                            cursor: 0,
                            iteration: 0,
                            training: TrainingPolicy {
                                iterations: 1,
                                reservoir_samples: 1,
                                seed: 0,
                                ingest_batch_keys: 1,
                                phase_batch_entries: 1,
                            },
                            serving_at_start: Some(manifest),
                            source_lsn,
                            publication_coalesce: None,
                        })?;
                        tx.publish_manifest(replacement)?;
                        for (key, bytes) in &payloads {
                            tx.put_page(*key, bytes)?;
                        }
                        tx.commit()?;
                    }
                    let mut bytes = [0; PAGE_BYTES];
                    for key in targets.into_iter().flatten() {
                        if !scope.read_admitted_page(key, &mut bytes)? {
                            return Err(contextdb_core::Error::Other(
                                "semantic target disappeared".into(),
                            ));
                        }
                    }
                    Ok(())
                },
            )
        },
    )?;
    let seen = captured.lock().unwrap();
    Ok(NativeSemanticDriveResult {
        final_operation: seen.0.ok_or_else(|| {
            contextdb_core::Error::Other("semantic drive emitted no final seal".into())
        })?,
        targets: seen.1,
    })
}
#[derive(Clone, Copy, Debug)]
#[expect(
    clippy::enum_variant_names,
    reason = "Mirror the native fallible before-commit boundary names."
)]
pub enum NativeSealFailurePoint {
    BeforeMarkerWrite,
    BeforePayloadWrite,
    BeforeHeaderSeal,
    BeforeKindFingerprint,
    BeforeScopeWrite,
    BeforeConfigurationBind,
    BeforeOuterCommit,
}
/// Returns whether the selected actual fallible before-boundary was reached.
/// A successful commit is never changed into an error by a passive callback.
pub fn with_native_ivf_seal_failure_for_test<T>(
    point: NativeSealFailurePoint,
    operation: impl FnOnce() -> T,
) -> (T, bool) {
    use contextdb_engine::vector_ivf_observations::*;
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sink = fired.clone();
    let result = with_native_ivf_observer(
        NativeObserver {
            event: Arc::new(|_| ()),
            boundary: Arc::new(move |boundary| {
                let matches = matches!(
                    (point, boundary),
                    (
                        NativeSealFailurePoint::BeforeMarkerWrite,
                        NativeIvfFaultBoundary::BeforeMarkerWrite { .. }
                    ) | (
                        NativeSealFailurePoint::BeforePayloadWrite,
                        NativeIvfFaultBoundary::BeforePayloadWrite { .. }
                    ) | (
                        NativeSealFailurePoint::BeforeHeaderSeal,
                        NativeIvfFaultBoundary::BeforeHeaderSeal { .. }
                    ) | (
                        NativeSealFailurePoint::BeforeKindFingerprint,
                        NativeIvfFaultBoundary::BeforeKindFingerprint { .. }
                    ) | (
                        NativeSealFailurePoint::BeforeScopeWrite,
                        NativeIvfFaultBoundary::BeforeScopeWrite { .. }
                    ) | (
                        NativeSealFailurePoint::BeforeConfigurationBind,
                        NativeIvfFaultBoundary::BeforeConfigurationBind
                    ) | (
                        NativeSealFailurePoint::BeforeOuterCommit,
                        NativeIvfFaultBoundary::BeforeOuterCommit
                    )
                );
                if matches && !sink.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return Err(contextdb_core::Error::Other(
                        "injected native precommit seal failure".into(),
                    ));
                }
                Ok(())
            }),
        },
        operation,
    );
    (result, fired.load(std::sync::atomic::Ordering::SeqCst))
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeScopeRoots {
    pub index_digest: [u8; 32],
    pub tuple_digest: [u8; 32],
    pub snapshot_lsn: u64,
    pub table_generation: u64,
    pub column_incarnation: u64,
    pub frozen_frontier: u64,
    pub frozen_source_identity: [u8; 32],
    pub current_frontier: u64,
    pub current_digest: [u8; 32],
    pub current_entries: u64,
    pub current_live_entries: u64,
    pub expected_roots: [[u8; 32]; 16],
    pub actual_roots: [[u8; 32]; 16],
    pub present: u16,
}
/// Borrows the actual stored scope/control through the retained native read
/// at the requested snapshot, with actual captured table fingerprints. Never
/// reconstructs roots from historical write events or opens latest instead.
pub fn native_ivf_scope_roots_at_snapshot_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
    snapshot: contextdb_core::SnapshotId,
) -> contextdb_core::Result<NativeScopeRoots> {
    let r = db.native_ivf_scope_roots_at_snapshot_for_test(index, tuple, snapshot)?;
    Ok(NativeScopeRoots {
        index_digest: r.index_digest,
        tuple_digest: r.tuple_digest,
        snapshot_lsn: r.snapshot_lsn,
        table_generation: r.table_generation,
        column_incarnation: r.column_incarnation,
        frozen_frontier: r.frozen_frontier,
        frozen_source_identity: r.frozen_source_identity,
        current_frontier: r.current_frontier,
        current_digest: r.current_digest,
        current_entries: r.current_entries,
        current_live_entries: r.current_live_entries,
        expected_roots: r.expected_roots,
        actual_roots: r.actual_roots,
        present: r.present,
    })
}
pub struct NativeSourceRecord<'a> {
    pub key: &'a [u8],
    pub encoded: &'a [u8],
    pub live: bool,
}
/// Visits the real authenticated selected source at the SAME captured view;
/// bytes are actual native keys/records, not re-encoded IVF/source summaries.
pub fn visit_native_ivf_selected_source_for_test(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    tuple: &contextdb_core::VectorPartitionKey,
    snapshot: contextdb_core::SnapshotId,
    visit: &mut dyn FnMut(NativeSourceRecord<'_>),
) -> contextdb_core::Result<()> {
    db.visit_native_ivf_selected_source_for_test(index, tuple, snapshot, &mut |record| {
        visit(NativeSourceRecord {
            key: record.key,
            encoded: record.encoded,
            live: record.live,
        })
    })
}

/// Maps to FlipHeaderByte/FlipPayloadByte with the exact actual
/// storage+kind+complete PageKey target. Expected authority stays unchanged.
pub fn corrupt_native_ivf_page_for_test(
    db: &Database,
    target: NativeSealPage,
    payload: bool,
    offset: usize,
    xor_mask: u8,
) -> contextdb_core::Result<()> {
    use contextdb_vector::ivf::storage::{PageKey, PageKind};
    let kinds = [
        PageKind::Canonical,
        PageKind::Lists,
        PageKind::Directory,
        PageKind::DirectoryPresence,
        PageKind::Visibility,
        PageKind::Codebook,
        PageKind::Accumulators,
        PageKind::Reservoir,
        PageKind::Catalog,
        PageKind::SlotMetadata,
        PageKind::Tail,
        PageKind::ListLinks,
        PageKind::ListAllocator,
        PageKind::ListUsed,
        PageKind::RowVersions,
        PageKind::RowCopies,
    ];
    let kind = *kinds
        .get(usize::from(target.kind).wrapping_sub(1))
        .ok_or_else(|| contextdb_core::Error::Other("invalid native kind tag".into()))?;
    db.corrupt_native_ivf_page_for_test(
        target.storage,
        PageKey {
            partition: target.partition,
            generation: target.generation,
            kind,
            list: target.list,
            ordinal: target.ordinal,
            run: target.run,
        },
        payload,
        offset,
        xor_mask,
    )
}

struct NativeObserver {
    event: Arc<
        dyn for<'a> Fn(contextdb_engine::vector_ivf_observations::NativeIvfSealEvent<'a>)
            + Send
            + Sync,
    >,
    boundary: Arc<
        dyn for<'a> Fn(
                contextdb_engine::vector_ivf_observations::NativeIvfFaultBoundary<'a>,
            ) -> contextdb_core::Result<()>
            + Send
            + Sync,
    >,
}
impl contextdb_engine::vector_ivf_observations::NativeIvfSealObserver for NativeObserver {
    fn observe(&self, event: contextdb_engine::vector_ivf_observations::NativeIvfSealEvent<'_>) {
        (self.event)(event)
    }
    fn before_boundary(
        &self,
        boundary: contextdb_engine::vector_ivf_observations::NativeIvfFaultBoundary<'_>,
    ) -> contextdb_core::Result<()> {
        (self.boundary)(boundary)
    }
}
fn observe_native_search<T>(operation: impl FnOnce() -> T) -> (T, SearchWork) {
    use contextdb_engine::vector_ivf_observations::*;
    let state = Arc::new(std::sync::Mutex::new(SearchWork::default()));
    let sink = state.clone();
    let result = with_native_ivf_observer(
        NativeObserver {
            event: Arc::new(move |event| {
                if let NativeIvfSealEvent::QueryExecuted {
                    tuple,
                    generation,
                    indexed,
                    served_depth,
                    objective_met,
                    work,
                    ..
                } = event
                {
                    let mut state = sink.lock().unwrap();
                    state.clustered_route |= indexed;
                    state.centroid_evaluations += work.centroid_evaluations;
                    state.quantized_evaluations += work.quantized_evaluations;
                    state.full_precision_evaluations += work.full_precision_evaluations;
                    state.buffer_evaluations += work.buffer_evaluations;
                    state.list_pages_read += work.list_pages_read;
                    state.visibility_pages_read +=
                        work.visibility_pages_read + work.row_version_pages_read;
                    state.rerank_pages_read += work.canonical_pages_read;
                    state.directory_objects_read += work.directory_pages_read
                        + work.directory_presence_pages_read
                        + work.catalog_pages_read;
                    let values = tuple
                        .components()
                        .iter()
                        .map(|c| match c {
                            contextdb_core::VectorPartitionComponent::Uuid(v) => Value::Uuid(*v),
                            contextdb_core::VectorPartitionComponent::Text(v) => {
                                Value::Text(v.clone())
                            }
                            contextdb_core::VectorPartitionComponent::Integer(v) => {
                                Value::Int64(*v)
                            }
                            contextdb_core::VectorPartitionComponent::Boolean(v) => Value::Bool(*v),
                            contextdb_core::VectorPartitionComponent::Timestamp(v) => {
                                Value::Timestamp(*v)
                            }
                            contextdb_core::VectorPartitionComponent::TxId(v) => Value::TxId(*v),
                        })
                        .collect::<Vec<_>>();
                    if !state.contexts_read.contains(&values) {
                        state.contexts_read.push(values);
                    }
                    state.outcome = Some(SearchOutcome::Complete);
                    state.lists_probed += work.lists_probed;
                    state.served_generation = Some(generation);
                    state.served_depth = served_depth;
                    state.objective_status = Some(if objective_met {
                        ObjectiveStatus::Met
                    } else {
                        ObjectiveStatus::Unqualified
                    });
                    state.route = Some(if indexed {
                        SearchRoute::Indexed
                    } else {
                        SearchRoute::Exact
                    });
                }
            }),
            boundary: Arc::new(|_| Ok(())),
        },
        operation,
    );
    let value = state.lock().unwrap().clone();
    (result, value)
}
fn with_native_ivf_observer<T>(observer: NativeObserver, operation: impl FnOnce() -> T) -> T {
    contextdb_engine::vector_ivf_observations::with_native_ivf_seal_observer_for_test(
        Arc::new(observer),
        operation,
    )
}

fn native_kind_tag(kind: contextdb_vector::ivf::storage::PageKind) -> u8 {
    use contextdb_vector::ivf::storage::PageKind::*;
    match kind {
        Canonical => 1,
        Lists => 2,
        Directory => 3,
        DirectoryPresence => 4,
        Visibility => 5,
        Codebook => 6,
        Accumulators => 7,
        Reservoir => 8,
        Catalog => 9,
        SlotMetadata => 10,
        Tail => 11,
        ListLinks => 12,
        ListAllocator => 13,
        ListUsed => 14,
        RowVersions => 15,
        RowCopies => 16,
    }
}
fn native_operation(
    operation: contextdb_engine::vector_ivf_observations::NativeIvfOperationRef<'_>,
) -> Option<NativeSealOperation> {
    use contextdb_engine::vector_ivf_observations::NativeIvfSourceInstance;
    match operation.source_instance {
        NativeIvfSourceInstance::Write {
            admitted_session,
            outer_write_object,
        } => Some(NativeSealOperation {
            session: admitted_session,
            outer: outer_write_object,
            operation: operation.operation?,
        }),
        _ => None,
    }
}
fn native_page(
    operation: contextdb_engine::vector_ivf_observations::NativeIvfOperationRef<'_>,
    key: contextdb_vector::ivf::storage::PageKey,
) -> NativeSealPage {
    NativeSealPage {
        storage: operation.storage,
        partition: key.partition,
        generation: key.generation,
        kind: native_kind_tag(key.kind),
        list: key.list,
        ordinal: key.ordinal,
        run: key.run,
    }
}
fn native_object_kind(
    kind: contextdb_engine::vector_ivf_observations::NativeIvfObjectKind,
) -> NativeSealObjectKind {
    match kind {
        contextdb_engine::vector_ivf_observations::NativeIvfObjectKind::Scope => {
            NativeSealObjectKind::Scope
        }
        contextdb_engine::vector_ivf_observations::NativeIvfObjectKind::SealWorkspace => {
            NativeSealObjectKind::SealWorkspace
        }
    }
}
#[derive(Clone, Copy)]
struct NativeProjectionOperation {
    operation: NativeSealOperation,
    generations: [Option<u64>; 16],
}
struct NativeSealProjection {
    operations: [Option<NativeProjectionOperation>; 32],
    latest: Option<NativeSealOperation>,
    overflow: bool,
}
impl NativeSealProjection {
    fn remember(&mut self, id: NativeSealOperation) -> Option<&mut NativeProjectionOperation> {
        self.latest = Some(id);
        let at = if let Some(at) = self
            .operations
            .iter()
            .position(|v| v.is_some_and(|v| v.operation == id))
        {
            at
        } else if let Some(at) = self.operations.iter().position(Option::is_none) {
            self.operations[at] = Some(NativeProjectionOperation {
                operation: id,
                generations: [None; 16],
            });
            at
        } else {
            self.overflow = true;
            return None;
        };
        self.operations[at].as_mut()
    }
}
