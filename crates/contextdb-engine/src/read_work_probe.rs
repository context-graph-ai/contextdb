//! Test-only accounting at the file reader's expensive decode and digest loops.
//!
//! This is deliberately scoped to one thread and one operation phase.  It is
//! not a process-wide census: a test that did not install an observer has no
//! value to read, and nested scopes restore their parent even while unwinding.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadWorkPhase {
    Fixture,
    Mutation,
    Open,
    Query,
    Commit,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadWorkCounters {
    /// Stored bytes handed to a row-version decode, before any projection.
    /// A read that decodes whole rows it will not return pays for their
    /// vector columns too, and this is where that shows up.
    pub stored_row_bytes: u64,
    pub decoded_rows: u64,
    /// Durable row reads by a version range or by its complete physical key.
    pub row_version_range_reads: u64,
    pub row_version_point_reads: u64,
    pub decoded_vectors: u64,
    pub vector_body_accesses: u64,
    /// Authenticated posting windows this read built, and the postings they
    /// walked. A find asks the graph for k rows; if these grow with the SIZE
    /// of a partition, or with the NUMBER of partitions, the read is paying
    /// for the scope instead of for the answer.
    pub verified_runs_built: u64,
    pub verified_postings_walked: u64,
    /// Reads (and physical index opens) those windows reconciled through. One
    /// statement's windows share one.
    pub verified_reads_opened: u64,
    /// Authenticated descents that proved a key prefix holds no posting.
    pub verified_prefix_probes: u64,
    pub hashed_rows: u64,
    pub hashed_vectors: u64,
    pub hashed_bytes: u64,
    pub authoritative_bodies_verified: u64,
    pub proof_nodes_read: u64,
    pub proof_nodes_hashed: u64,
    pub ordered_proof_nodes_read: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphReadWorkCounters {
    pub graph_supplied_primary_reads: u64,
    pub graph_range_reads: u64,
    pub graph_iteration_steps: u64,
    pub graph_population_walks: u64,
    pub graph_payload_decodes: u64,
    pub graph_private_history_decodes: u64,
    pub graph_root_reads: u64,
    pub graph_addressed_gets: u64,
    pub graph_maximum_seeks: u64,
    pub graph_commit_gets: u64,
    pub graph_support_probes: u64,
    pub graph_forward_mutations: u64,
    pub graph_reverse_mutations: u64,
    pub graph_creation_contribution_mutations: u64,
    pub graph_lifecycle_mutations: u64,
    pub graph_deletion_contribution_mutations: u64,
    pub graph_location_mutations: u64,
    pub graph_history_mutations: u64,
    pub graph_survivor_history_touches: u64,
    pub graph_survivor_location_touches: u64,
    pub graph_survivor_deletion_contribution_touches: u64,
    pub graph_publication_commits: u64,
    pub graph_snapshots_constructed: u64,
    pub graph_descriptors_at_last_capture: u64,
    pub worker_admitted_pages: u64,
    pub worker_admitted_page_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadWorkObservation {
    pub open: ReadWorkCounters,
    pub query: ReadWorkCounters,
    pub commit: ReadWorkCounters,
}

#[derive(Default)]
struct AtomicCounters {
    graph_supplied_primary_reads: AtomicU64,
    graph_range_reads: AtomicU64,
    graph_iteration_steps: AtomicU64,
    graph_population_walks: AtomicU64,
    graph_payload_decodes: AtomicU64,
    graph_private_history_decodes: AtomicU64,
    graph_root_reads: AtomicU64,
    graph_addressed_gets: AtomicU64,
    graph_maximum_seeks: AtomicU64,
    graph_commit_gets: AtomicU64,
    graph_support_probes: AtomicU64,
    graph_forward_mutations: AtomicU64,
    graph_reverse_mutations: AtomicU64,
    graph_creation_contribution_mutations: AtomicU64,
    graph_lifecycle_mutations: AtomicU64,
    graph_deletion_contribution_mutations: AtomicU64,
    graph_location_mutations: AtomicU64,
    graph_history_mutations: AtomicU64,
    graph_survivor_history_touches: AtomicU64,
    graph_survivor_location_touches: AtomicU64,
    graph_survivor_deletion_contribution_touches: AtomicU64,
    graph_publication_commits: AtomicU64,
    graph_snapshots_constructed: AtomicU64,
    graph_descriptors_at_last_capture: AtomicU64,
    worker_admitted_pages: AtomicU64,
    worker_admitted_page_bytes: AtomicU64,

    stored_row_bytes: AtomicU64,
    verified_runs_built: AtomicU64,
    verified_postings_walked: AtomicU64,
    verified_reads_opened: AtomicU64,
    verified_prefix_probes: AtomicU64,
    decoded_rows: AtomicU64,
    row_version_range_reads: AtomicU64,
    row_version_point_reads: AtomicU64,
    decoded_vectors: AtomicU64,
    vector_body_accesses: AtomicU64,
    hashed_rows: AtomicU64,
    hashed_vectors: AtomicU64,
    hashed_bytes: AtomicU64,
    authoritative_bodies_verified: AtomicU64,
    proof_nodes_read: AtomicU64,
    proof_nodes_hashed: AtomicU64,
    ordered_proof_nodes_read: AtomicU64,
}

impl AtomicCounters {
    fn snapshot(&self) -> ReadWorkCounters {
        ReadWorkCounters {
            stored_row_bytes: self.stored_row_bytes.load(Ordering::Relaxed),
            verified_runs_built: self.verified_runs_built.load(Ordering::Relaxed),
            verified_postings_walked: self.verified_postings_walked.load(Ordering::Relaxed),
            verified_reads_opened: self.verified_reads_opened.load(Ordering::Relaxed),
            verified_prefix_probes: self.verified_prefix_probes.load(Ordering::Relaxed),
            decoded_rows: self.decoded_rows.load(Ordering::Relaxed),
            row_version_range_reads: self.row_version_range_reads.load(Ordering::Relaxed),
            row_version_point_reads: self.row_version_point_reads.load(Ordering::Relaxed),
            decoded_vectors: self.decoded_vectors.load(Ordering::Relaxed),
            vector_body_accesses: self.vector_body_accesses.load(Ordering::Relaxed),
            hashed_rows: self.hashed_rows.load(Ordering::Relaxed),
            hashed_vectors: self.hashed_vectors.load(Ordering::Relaxed),
            hashed_bytes: self.hashed_bytes.load(Ordering::Relaxed),
            authoritative_bodies_verified: self
                .authoritative_bodies_verified
                .load(Ordering::Relaxed),
            proof_nodes_read: self.proof_nodes_read.load(Ordering::Relaxed),
            proof_nodes_hashed: self.proof_nodes_hashed.load(Ordering::Relaxed),
            ordered_proof_nodes_read: self.ordered_proof_nodes_read.load(Ordering::Relaxed),
        }
    }

    fn graph_snapshot(&self) -> GraphReadWorkCounters {
        GraphReadWorkCounters {
            graph_supplied_primary_reads: self.graph_supplied_primary_reads.load(Ordering::Relaxed),
            graph_range_reads: self.graph_range_reads.load(Ordering::Relaxed),
            graph_iteration_steps: self.graph_iteration_steps.load(Ordering::Relaxed),
            graph_population_walks: self.graph_population_walks.load(Ordering::Relaxed),
            graph_payload_decodes: self.graph_payload_decodes.load(Ordering::Relaxed),
            graph_private_history_decodes: self.graph_private_history_decodes.load(Ordering::Relaxed),
            graph_root_reads: self.graph_root_reads.load(Ordering::Relaxed),
            graph_addressed_gets: self.graph_addressed_gets.load(Ordering::Relaxed),
            graph_maximum_seeks: self.graph_maximum_seeks.load(Ordering::Relaxed),
            graph_commit_gets: self.graph_commit_gets.load(Ordering::Relaxed),
            graph_support_probes: self.graph_support_probes.load(Ordering::Relaxed),
            graph_forward_mutations: self.graph_forward_mutations.load(Ordering::Relaxed),
            graph_reverse_mutations: self.graph_reverse_mutations.load(Ordering::Relaxed),
            graph_creation_contribution_mutations: self.graph_creation_contribution_mutations.load(Ordering::Relaxed),
            graph_lifecycle_mutations: self.graph_lifecycle_mutations.load(Ordering::Relaxed),
            graph_deletion_contribution_mutations: self.graph_deletion_contribution_mutations.load(Ordering::Relaxed),
            graph_location_mutations: self.graph_location_mutations.load(Ordering::Relaxed),
            graph_history_mutations: self.graph_history_mutations.load(Ordering::Relaxed),
            graph_survivor_history_touches: self.graph_survivor_history_touches.load(Ordering::Relaxed),
            graph_survivor_location_touches: self.graph_survivor_location_touches.load(Ordering::Relaxed),
            graph_survivor_deletion_contribution_touches: self.graph_survivor_deletion_contribution_touches.load(Ordering::Relaxed),
            graph_publication_commits: self.graph_publication_commits.load(Ordering::Relaxed),
            graph_snapshots_constructed: self.graph_snapshots_constructed.load(Ordering::Relaxed),
            graph_descriptors_at_last_capture: self.graph_descriptors_at_last_capture.load(Ordering::Relaxed),
            worker_admitted_pages: self.worker_admitted_pages.load(Ordering::Relaxed),
            worker_admitted_page_bytes: self.worker_admitted_page_bytes.load(Ordering::Relaxed),
        }
    }
}

/// A paired sample of actual directory residency, separate from work counters.
#[derive(Debug, Clone, Copy)]
struct CommitDirectoryResidency {
    resident_entries: u64,
    durable_entries: u64,
}

/// A live sample of the actual declared child and its native parent counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredReadChildFunding {
    pub owner_identity: usize,
    pub alive: bool,
    pub charged_bytes: u64,
    pub parent_identity: usize,
    pub parent_alive: bool,
    pub parent_charged_bytes: u64,
}

/// Funding observation is separate from graph work and directory residency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitDirectoryFunding {
    pub open_child: DeclaredReadChildFunding,
    pub request_child: Option<DeclaredReadChildFunding>,
    pub image_alive: Option<bool>,
    pub image_held_bytes: Option<usize>,
    pub image_accountant_alive: Option<bool>,
    pub image_accountant_used_bytes: Option<usize>,
}

/// Immutable native source-registration window on the directory funding seam.
/// The after values are the baseline before directory construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitDirectorySourceRegistrationFunding {
    pub image_owner_identity: usize,
    pub image_accountant_identity: usize,
    pub image_held_before_reservation: usize,
    pub image_held_after_reservation: usize,
    pub image_accountant_used_before_reservation: usize,
    pub image_accountant_used_after_reservation: usize,
}

#[derive(Clone)]
struct CommitDirectoryFundingReceipt {
    open_child: crate::vector_read_memory::DeclaredReadChildObserver,
    request_child: Option<crate::vector_read_memory::DeclaredReadChildObserver>,
    image: Option<std::sync::Weak<crate::read_image_memory::ImageMemory>>,
    image_accountant: Option<std::sync::Weak<crate::memory_accounting::MemoryAccountant>>,
    source_registration: Option<CommitDirectorySourceRegistrationFunding>,
}
impl CommitDirectoryFundingReceipt {
    fn sample(&self) -> CommitDirectoryFunding {
        let image = self.image.as_ref().and_then(std::sync::Weak::upgrade);
        let accountant = self.image_accountant.as_ref().and_then(std::sync::Weak::upgrade);
        CommitDirectoryFunding {
            open_child: self.open_child.sample(),
            request_child: self.request_child.as_ref().map(|child| child.sample()),
            image_alive: self.image.as_ref().map(|_| image.is_some()),
            image_held_bytes: self.image.as_ref().map(|_| image.as_ref().map_or(0, |image| image.held())),
            image_accountant_alive: self.image_accountant.as_ref().map(|_| accountant.is_some()),
            image_accountant_used_bytes: self.image_accountant.as_ref().map(|_| accountant.as_ref().map_or(0, |accountant| accountant.usage().used)),
        }
    }
}

#[derive(Clone)]
struct ActiveObservation {
    operation_id: Option<String>,
    worker_request_id: Option<String>,
    phase: ReadWorkPhase,
    graph: Arc<GraphObservation>,
    counters: Arc<AtomicCounters>,
    commit_directory_residency: Arc<Mutex<Option<CommitDirectoryResidency>>>,
    commit_directory_funding: Arc<Mutex<Option<CommitDirectoryFundingReceipt>>>,
}

thread_local! {
    static ACTIVE: RefCell<Option<ActiveObservation>> = const { RefCell::new(None) };
}

/// An installed observation scope.  There is intentionally no global
/// "observed" accessor: unobserved work cannot be misreported as zero work.
pub struct ReadWorkObservationScope {
    phase: ReadWorkPhase,
    counters: Arc<AtomicCounters>,
    commit_directory_residency: Arc<Mutex<Option<CommitDirectoryResidency>>>,
    commit_directory_funding: Arc<Mutex<Option<CommitDirectoryFundingReceipt>>>,
    previous: Option<ActiveObservation>,
    graph: Arc<GraphObservation>,
    previous_root: Option<String>,
}

impl ReadWorkObservationScope {
    pub fn enter(phase: ReadWorkPhase) -> Self {
        let counters = Arc::new(AtomicCounters::default());
        let commit_directory_residency = Arc::new(Mutex::new(None));
        let commit_directory_funding = Arc::new(Mutex::new(None));
        let graph = Arc::new(GraphObservation::default());
        let previous = ACTIVE.with(|active| {
            active.replace(Some(ActiveObservation {
                operation_id: None,
                worker_request_id: None,
                phase,
                graph: graph.clone(),
                counters: Arc::clone(&counters),
                commit_directory_residency: Arc::clone(&commit_directory_residency),
                commit_directory_funding: Arc::clone(&commit_directory_funding),
            }))
        });
        Self {
            phase,
            previous_root: GRAPH_ROOT.with(|r| r.replace(None)),
            graph,
            counters,
            commit_directory_residency,
            commit_directory_funding,
            previous,
        }
    }

    pub fn phase(&self) -> ReadWorkPhase {
        self.phase
    }

    pub fn counters(&self) -> ReadWorkCounters {
        self.counters.snapshot()
    }
}

impl Drop for ReadWorkObservationScope {
    fn drop(&mut self) {
        GRAPH_ROOT.with(|r| {
            r.replace(self.previous_root.take());
        });
        let previous = self.previous.take();
        ACTIVE.with(|active| {
            let _departed = active.replace(previous);
        });
    }
}

fn with_active(note: impl FnOnce(&AtomicCounters)) {
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref() {
            note(&active.counters);
        }
    });
}

pub(crate) fn note_stored_row_bytes(bytes: usize) {
    with_active(|counters| {
        counters
            .stored_row_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    });
}

pub(crate) fn note_verified_run_built(postings: usize) {
    with_active(|counters| {
        counters.verified_runs_built.fetch_add(1, Ordering::Relaxed);
        counters
            .verified_postings_walked
            .fetch_add(postings as u64, Ordering::Relaxed);
    });
}

pub(crate) fn note_verified_read_opened() {
    with_active(|counters| {
        counters
            .verified_reads_opened
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_verified_prefix_probe() {
    with_active(|counters| {
        counters
            .verified_prefix_probes
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_decoded_row() {
    with_active(|counters| {
        counters.decoded_rows.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_row_version_range_read() {
    with_active(|counters| {
        counters
            .row_version_range_reads
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_row_version_point_read() {
    with_active(|counters| {
        counters
            .row_version_point_reads
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_decoded_vector() {
    with_active(|counters| {
        counters.decoded_vectors.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_vector_body_access() {
    with_active(|counters| {
        counters
            .vector_body_accesses
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_hashed_row() {
    with_active(|counters| {
        counters.hashed_rows.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_hashed_vector() {
    with_active(|counters| {
        counters.hashed_vectors.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_hashed_bytes(bytes: usize) {
    with_active(|counters| {
        counters
            .hashed_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    });
}

pub(crate) fn note_authoritative_body_verified() {
    with_active(|counters| {
        counters
            .authoritative_bodies_verified
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_proof_node_read() {
    with_active(|counters| {
        counters.proof_nodes_read.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_proof_node_hashed() {
    with_active(|counters| {
        counters.proof_nodes_hashed.fetch_add(1, Ordering::Relaxed);
    });
}

pub(crate) fn note_ordered_proof_node_read() {
    with_active(|counters| {
        counters
            .ordered_proof_nodes_read
            .fetch_add(1, Ordering::Relaxed);
    });
}

/// Attempted storage events. A mutation event does not imply publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphWorkEvent {
    pub phase: ReadWorkPhase,
    pub operation_id: Option<String>,
    /// Executing connection and request ordinal, separate from fixture identity.
    pub worker_request_id: Option<String>,
    /// Exact descriptor acquired at a root event, rather than a later owner root.
    pub acquired_root: Option<String>,
    pub observation_status: String,
    pub captured_root: Option<String>,
    pub family: String,
    pub operation: String,
    pub key: Vec<u8>,
    pub queried_lsn: Option<u64>,
    pub role: Option<String>,
    pub funding_owner: Option<String>,
    pub control_reservation_bytes: Option<usize>,
}
/// Installation on an actual executing owner path, with its request identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphWorkerInstallation {
    pub path: String,
    pub worker_request_id: String,
}
#[derive(Default)]
struct GraphObservation {
    events: std::sync::Mutex<Vec<GraphWorkEvent>>,
    retained: std::sync::Mutex<std::collections::HashSet<(String, Vec<u8>)>>,
    worker_paths: std::sync::Mutex<Vec<GraphWorkerInstallation>>,
    page_admissions: std::sync::Mutex<std::collections::BTreeMap<String, (u64, u64)>>,
}
thread_local! {
    static FUNDING: RefCell<Option<(String, usize, usize)>> = const { RefCell::new(None) };
    static GRAPH_ROOT: RefCell<Option<String>> = const { RefCell::new(None) };
}
/// Capability identity is process-local and remains stable while the owner lives.
pub(crate) fn funding_owner_name<T: ?Sized + contextdb_core::read_memory::ReadMemoryBudget>(owner: &std::sync::Arc<T>) -> String {
    if let Some(funder) = owner.funding_owner_for_test() {
        return funding_owner_name(funder);
    }
    format!("{}@{:p}", std::any::type_name::<T>(), std::sync::Arc::as_ptr(owner) as *const ())
}
pub(crate) fn funding_owner_id<T: ?Sized + contextdb_core::read_memory::ReadMemoryBudget>(owner: &std::sync::Arc<T>) -> usize {
    if let Some(funder) = owner.funding_owner_for_test() {
        return funding_owner_id(funder);
    }
    std::sync::Arc::as_ptr(owner) as *const () as usize
}
pub(crate) fn observed_funding_identity() -> Option<usize> {
    FUNDING.with(|f| f.borrow().as_ref().map(|v| v.1))
}
pub(crate) struct FundingScope(Option<(String, usize, usize)>);
impl Drop for FundingScope {
    fn drop(&mut self) { FUNDING.with(|f| { f.replace(self.0.take()); }); }
}
pub(crate) fn observe_funding(owner: String, identity: usize, controls: usize) -> FundingScope {
    FundingScope(FUNDING.with(|f| f.replace(Some((owner, identity, controls)))))
}
pub(crate) fn observed_funding() -> Option<(String, usize)> {
    FUNDING.with(|f| f.borrow().as_ref().map(|v| (v.0.clone(), v.2)))
}
fn owner_scopes()
-> &'static std::sync::Mutex<std::collections::HashMap<String, Vec<ActiveObservation>>> {
    static SCOPES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, Vec<ActiveObservation>>>,
    > = std::sync::OnceLock::new();
    SCOPES.get_or_init(Default::default)
}
/// Registration is keyed by the executing owner's store path, never a global census.
pub struct OwnerGraphObservationBinding {
    path: String,
}
impl Drop for OwnerGraphObservationBinding {
    fn drop(&mut self) {
        let mut scopes = owner_scopes().lock().unwrap_or_else(|e| e.into_inner());
        if let Some(stack) = scopes.get_mut(&self.path) {
            stack.pop();
            if stack.is_empty() {
                scopes.remove(&self.path);
            }
        }
    }
}
impl ReadWorkObservationScope {
    pub fn set_operation_id(&self, operation_id: &str) {
        ACTIVE.with(|a| {
            if let Some(a) = a.borrow_mut().as_mut() {
                a.operation_id = Some(operation_id.into());
            }
        });
    }
    pub fn graph_events(&self) -> Vec<GraphWorkEvent> {
        self.graph
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// Fixture-supplied keys; the engine never discovers or enumerates survivors.
    pub fn retain_graph_key(&self, family: &str, key: &[u8]) {
        self.graph
            .retained
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((family.into(), key.into()));
    }
    pub fn bind_owner(
        &self,
        database: &crate::Database,
        operation_id: &str,
    ) -> OwnerGraphObservationBinding {
        let path = database.bounded_store_path();
        owner_scopes()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(path.clone())
            .or_default()
            .push(ActiveObservation {
                operation_id: Some(operation_id.into()),
                worker_request_id: None,
                phase: self.phase,
                counters: self.counters.clone(),
                commit_directory_residency: self.commit_directory_residency.clone(),
                commit_directory_funding: self.commit_directory_funding.clone(),
                graph: self.graph.clone(),
            });
        OwnerGraphObservationBinding { path }
    }
    /// None is UNOBSERVED: caller TLS was never installed on the executing worker.
    pub fn graph_counters(&self) -> GraphReadWorkCounters {
        self.counters.graph_snapshot()
    }
    /// An actual paired sample; unobserved directory work has no zero value.
    pub fn commit_directory_residency(&self) -> Option<(u64, u64)> {
        self.commit_directory_residency
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|sample| (sample.resident_entries, sample.durable_entries))
    }
    /// None means the actual open funder was never observed. Samples retain
    /// only Weak handles; a known dropped owner is reported as dead with zero.
    pub fn commit_directory_funding(&self) -> Option<CommitDirectoryFunding> {
        self.commit_directory_funding.lock().unwrap_or_else(|error| error.into_inner())
            .as_ref().map(CommitDirectoryFundingReceipt::sample)
    }
    /// None means no successful source registration was observed on this seam.
    /// Snapshot values never resample or retain the ImageMemory owner.
    pub fn commit_directory_source_registration_funding(&self) -> Option<CommitDirectorySourceRegistrationFunding> {
        self.commit_directory_funding.lock().unwrap_or_else(|error| error.into_inner())
            .as_ref().and_then(|receipt| receipt.source_registration)
    }
    pub fn owner_worker_paths(&self) -> Vec<GraphWorkerInstallation> {
        self.graph.worker_paths.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    /// None means no executing worker path was observed; pages/bytes live on
    /// the same shared probe and are recorded only after successful admission.
    pub fn owner_page_admission(&self) -> Option<(u64, u64)> {
        self.owner_graph_counters().map(|c| (c.worker_admitted_pages, c.worker_admitted_page_bytes))
    }
    /// Per-capability successful funding totals; None remains UNOBSERVED.
    pub fn owner_page_admission_by_owner(&self) -> Option<std::collections::BTreeMap<String, (u64, u64)>> {
        self.owner_graph_counters().map(|_| self.graph.page_admissions.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }
    pub fn owner_graph_counters(&self) -> Option<GraphReadWorkCounters> {
        (!self.owner_worker_paths().is_empty()).then(|| self.graph_counters())
    }
}
pub(crate) struct GraphWorkerScope {
    previous: Option<ActiveObservation>,
    root: Option<String>,
}
impl Drop for GraphWorkerScope {
    fn drop(&mut self) {
        ACTIVE.with(|a| {
            a.replace(self.previous.take());
        });
        GRAPH_ROOT.with(|r| {
            r.replace(self.root.take());
        });
    }
}
pub(crate) fn enter_owner_graph_scope(
    database: &crate::Database,
    connection: [u8; 16],
    ordinal: u64,
    path: &str,
) -> GraphWorkerScope {
    let mut active = owner_scopes()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&database.bounded_store_path())
        .and_then(|s| s.last())
        .cloned();
    if let Some(a) = &mut active {
        a.worker_request_id = Some(format!("{:?}:{}", connection, ordinal));
        a.graph.worker_paths.lock().unwrap_or_else(|e| e.into_inner()).push(GraphWorkerInstallation {
            path: path.into(),
            worker_request_id: a.worker_request_id.clone().expect("worker identity assigned above"),
        });
    }
    GraphWorkerScope {
        previous: ACTIVE.with(|a| a.replace(active)),
        root: GRAPH_ROOT.with(|r| r.replace(None)),
    }
}
pub(crate) fn bind_graph_root(root: impl std::fmt::Debug) {
    GRAPH_ROOT.with(|r| {
        r.replace(Some(format!("{root:?}")));
    });
}
pub(crate) fn note_graph(
    family: &str,
    operation: &str,
    key: &[u8],
    lsn: Option<u64>,
    role: Option<&str>,
) {
    ACTIVE.with(|a| {
        let borrowed = a.borrow();
        let Some(a) = borrowed.as_ref() else {
            return;
        };
        let family = match family {
            "change_log" | "writer_change_log" => "history",
            "graph_contribution" if key.len() < 18 => "graph_contribution",
            "graph_contribution" => if key.get(17) == Some(&1) { "graph_deletion" } else { "graph_creation" },
            "graph_contributions_v1" => {
                if key.get(17) == Some(&1) {
                    "graph_deletion"
                } else {
                    "graph_creation"
                }
            }
            "graph_occurrence_locations_v1" => "graph_location",
            "graph_predecessor_lifecycle_v1" => "graph_lifecycle",
            other => other,
        };
        let c = &a.counters;
        let add = |n: &AtomicU64| {
            n.fetch_add(1, Ordering::Relaxed);
        };
        match operation {
            "supplied_primary" => add(&c.graph_supplied_primary_reads),
            "range" => add(&c.graph_range_reads),
            "iteration_step" => add(&c.graph_iteration_steps),
            "population_walk" => add(&c.graph_population_walks),
            "payload_decode" => add(&c.graph_payload_decodes),
            "private_history_decode" => add(&c.graph_private_history_decodes),
            "root" if family.starts_with("graph_") => add(&c.graph_root_reads),
            "support_probe" => add(&c.graph_support_probes),
            "maximum_seek" => {
                add(&c.graph_maximum_seeks);
                add(&c.graph_addressed_gets);
            }
            "committed" => add(&c.graph_publication_commits),
            "get" if family == "commit_index" => {
                add(&c.graph_commit_gets);
                add(&c.graph_addressed_gets);
            }
            "get" if family.starts_with("graph_") || family == "history" => {
                add(&c.graph_addressed_gets)
            }
            _ => (),
        }
        if matches!(operation, "get" | "insert" | "remove" | "iteration_step") {
            if matches!(
                a.phase,
                ReadWorkPhase::Mutation | ReadWorkPhase::Commit | ReadWorkPhase::Fixture
            ) {
                match family {
                    "graph_fwd" => add(&c.graph_forward_mutations),
                    "graph_rev" => add(&c.graph_reverse_mutations),
                    "graph_creation" => add(&c.graph_creation_contribution_mutations),
                    "graph_deletion" => add(&c.graph_deletion_contribution_mutations),
                    "graph_lifecycle" => add(&c.graph_lifecycle_mutations),
                    "graph_location" => add(&c.graph_location_mutations),
                    "history" => add(&c.graph_history_mutations),
                    _ => (),
                }
            }
            if a.graph
                .retained
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&(family.into(), key.into()))
            {
                match family {
                    "history" => add(&c.graph_survivor_history_touches),
                    "graph_location" => add(&c.graph_survivor_location_touches),
                    "graph_deletion" => add(&c.graph_survivor_deletion_contribution_touches),
                    _ => (),
                }
            }
        }
        a.graph
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(GraphWorkEvent {
                phase: a.phase,
                operation_id: a.operation_id.clone(),
                worker_request_id: a.worker_request_id.clone(),
                observation_status: if GRAPH_ROOT.with(|r| r.borrow().is_some()) { "OBSERVED" } else { "UNOBSERVED" }.into(),
                acquired_root: if operation == "root" { role.map(Into::into) } else { None },
                captured_root: GRAPH_ROOT.with(|r| r.borrow().clone()),
                family: family.into(),
                operation: operation.into(),
                key: key.into(),
                queried_lsn: lsn,
                role: role.map(Into::into),
                funding_owner: observed_funding().map(|v| v.0),
                control_reservation_bytes: observed_funding().map(|v| v.1),
            });
    });
}

pub(crate) struct GraphPublicationAttempt {
    committed: bool,
    previous_root: Option<String>,
}
impl GraphPublicationAttempt {
    pub(crate) fn begin_in_write(write: &redb::WriteTransaction) -> Self {
        let previous_root = GRAPH_ROOT.with(|r| r.borrow().clone());
        bind_graph_write_root(write);
        note_graph("publication", "begin", &[], None, None);
        Self { committed: false, previous_root }
    }
    pub(crate) fn committed(&mut self) {
        self.committed = true;
        note_graph("publication", "committed", &[], None, None);
    }
}
impl Drop for GraphPublicationAttempt {
    fn drop(&mut self) {
        if !self.committed {
            note_graph("publication", "aborted", &[], None, None);
        }
        GRAPH_ROOT.with(|r| { r.replace(self.previous_root.take()); });
    }
}

pub(crate) fn graph_storage_key<K: AsRef<[u8]>>(family: &str, operation: &str, key: K) -> K {
    let bytes = key.as_ref();
    let family = if family == "graph_contribution" {
        if bytes.get(17) == Some(&1) {
            "graph_deletion"
        } else {
            "graph_creation"
        }
    } else {
        family
    };
    note_graph(family, operation, bytes, None, None);
    key
}

/// Initial roots of this executing transaction, captured before any open handles.
pub(crate) fn bind_graph_write_root(write: &redb::WriteTransaction) {
    let mut roots = ["graph_fwd", "graph_rev", "graph_contributions_v1", "graph_occurrence_locations_v1", "graph_predecessor_lifecycle_v1"]
        .map(|name| (name, write.table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(name)))).into_iter().collect::<Vec<_>>();
    roots.push(("change_log", write.table_root_fingerprint(redb::TableDefinition::<&str, &[u8]>::new("change_log"))));
    roots.push(("writer_change_log", write.table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new("writer_change_log"))));
    for (name, root) in &roots { note_graph_root("root_binding", name.as_bytes(), root); }
    if roots.iter().any(|(_, root)| root.is_err()) {
        GRAPH_ROOT.with(|r| { r.replace(None); });
        note_graph("root_binding", "UNOBSERVED", &[], None, Some("transaction roots unavailable"));
    } else { bind_graph_root(&roots); }
}
pub(crate) fn note_graph_root(family: &str, key: &[u8], root: impl std::fmt::Debug) {
    let acquired = format!("{root:?}");
    note_graph(family, "root", key, None, Some(&acquired));
}

/// Marks the completed adapter construction, before any edge cursor load.
pub(crate) fn note_graph_snapshot_capture(root: impl std::fmt::Debug, owner: String, identity: usize, controls: usize) {
    let _funding = observe_funding(owner, identity, controls);
    let mut sample = None;
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref() {
            let c = &active.counters;
            let constructed = c.graph_snapshots_constructed.fetch_add(1, Ordering::Relaxed) + 1;
            let descriptors = c.graph_root_reads.load(Ordering::Relaxed);
            c.graph_descriptors_at_last_capture.store(descriptors, Ordering::Relaxed);
            sample = Some(format!("constructed={constructed};descriptor_reads_so_far={descriptors}"));
        }
    });
    // A snapshot event describes this construction only. Restore the caller's
    // root even if event recording unwinds; the later source binds its root.
    struct EventRoot(Option<String>);
    impl Drop for EventRoot {
        fn drop(&mut self) {
            GRAPH_ROOT.with(|r| { r.replace(self.0.take()); });
        }
    }
    let _root = EventRoot(GRAPH_ROOT.with(|r| r.replace(Some(format!("{root:?}")))));
    if let Some(sample) = sample {
        note_graph("graph_snapshot", "SNAPSHOT_CAPTURE", &[], None, Some(&sample));
    }
}
pub(crate) fn note_graph_root_bound(root: impl std::fmt::Debug) {
    bind_graph_root(root);
    note_graph("graph_snapshot", "ROOT_BOUND", &[], None, None);
}
/// Called by actual storage-page admission callbacks after funding succeeds.
pub(crate) fn note_worker_page_admitted(bytes: usize, owner: String) {
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref().filter(|a| a.worker_request_id.is_some()) {
            let mut admissions = active.graph.page_admissions.lock().unwrap_or_else(|e| e.into_inner());
            let total = admissions.entry(owner).or_default();
            total.0 += 1;
            total.1 += bytes as u64;
            active.counters.worker_admitted_pages.fetch_add(1, Ordering::Relaxed);
            active.counters.worker_admitted_page_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    });
}

pub(crate) fn note_owner_graph_path(path: &str) {
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref()
            && let Some(worker_request_id) = &active.worker_request_id
        {
            active.graph.worker_paths.lock().unwrap_or_else(|e| e.into_inner()).push(GraphWorkerInstallation {
                path: path.into(),
                worker_request_id: worker_request_id.clone(),
            });
        }
    });
}

/// Sample the actual directory representation separately from its durable census.
pub(crate) fn note_commit_directory_residency(resident: u64, durable: u64) {
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref() {
            *active.commit_directory_residency
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(CommitDirectoryResidency {
                    resident_entries: resident,
                    durable_entries: durable,
                });
        }
    });
}

/// Bind the exact child selected for native open, never an inferred byte sum.
pub(crate) fn note_commit_directory_open_funding(memory: &Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) {
    let child = crate::vector_read_memory::declared_child_observer(memory);
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref() {
            let mut receipt = active.commit_directory_funding.lock().unwrap_or_else(|error| error.into_inner());
            match child {
                Some(open_child) => {
                    if receipt.as_ref().is_some_and(|previous| previous.open_child.sample().owner_identity == open_child.sample().owner_identity) {
                        return;
                    }
                    *receipt = Some(CommitDirectoryFundingReceipt { open_child, request_child: None, image: None, image_accountant: None, source_registration: None });
                }
                None => *receipt = None,
            }
        }
    });
}
pub(crate) fn note_commit_directory_image_memory(image: &Arc<crate::read_image_memory::ImageMemory>) {
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref()
            && let Some(receipt) = active.commit_directory_funding.lock().unwrap_or_else(|error| error.into_inner()).as_mut() {
            receipt.image = Some(Arc::downgrade(image));
            receipt.image_accountant = Some(Arc::downgrade(&image.accountant));
        }
    });
}
pub(crate) fn note_commit_directory_request_funding(memory: &Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) {
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref()
            && let Some(receipt) = active.commit_directory_funding.lock().unwrap_or_else(|error| error.into_inner()).as_mut() {
            receipt.request_child = crate::vector_read_memory::declared_child_observer(memory);
        }
    });
}

/// Record only a successfully reserved wrapper on its actual native owner.
pub(crate) fn note_commit_directory_source_registration_funding(
    image: &Arc<crate::read_image_memory::ImageMemory>,
    before: (usize, usize),
) {
    let window = CommitDirectorySourceRegistrationFunding {
        image_owner_identity: Arc::as_ptr(image) as usize,
        image_accountant_identity: Arc::as_ptr(&image.accountant) as usize,
        image_held_before_reservation: before.0,
        image_held_after_reservation: image.held(),
        image_accountant_used_before_reservation: before.1,
        image_accountant_used_after_reservation: image.accountant.usage().used,
    };
    ACTIVE.with(|active| {
        if let Some(active) = active.borrow().as_ref()
            && let Some(receipt) = active.commit_directory_funding.lock().unwrap_or_else(|error| error.into_inner()).as_mut() {
            receipt.source_registration = Some(window);
        }
    });
}
