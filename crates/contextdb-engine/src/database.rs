#![allow(clippy::enum_variant_names)] // Existing compatibility and test seam shape.
#![allow(clippy::field_reassign_with_default)] // Existing compatibility and test seam shape.
#![allow(clippy::too_many_arguments)] // Existing compatibility and test seam shape.
#![allow(clippy::type_complexity)] // Existing compatibility and test seam shape.
#![allow(unused_doc_comments)] // Existing compatibility and test seam shape.
// Custody-only persistence seam.
#[cfg(feature = "sync-orchestration")]
pub(crate) mod custody_pull;
mod custody_remainder;
mod custody_store;
use contextdb_core::read_memory::ReadMemoryBudget as _;
#[cfg(feature = "test-seams")]
pub use custody_store::HeldCustodyRootForTest;
mod erasure_frontiers;
mod schema_history_memory;
use schema_history_memory::{PreparedSchemaHistory, SchemaHistoryMemory};
mod graph_cursor_claims;
mod maintenance_frontier;
use erasure_frontiers::MemoryErasureFrontiers;
pub(crate) use graph_cursor_claims::{EngineGraphBfsCursor, GraphCursorReadGuard};
#[cfg(feature = "sync-orchestration")]
pub(crate) mod native_custody_selection;
use contextdb_relational::source::RelationalCursor as _;
use maintenance_frontier::MaintenanceFrontier;
pub(crate) mod graph_read_memory;
#[cfg(test)]
mod published_vector_install_tests;
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub mod resident_history_test_seams;
pub(crate) mod vector_captured_serving;
#[cfg(test)]
mod vector_frontier_metadata_admission_tests;
#[cfg(test)]
mod vector_generation_decode_owner_tests;
mod vector_import_tail;
#[cfg(test)]
mod vector_journal_cleanup_cancellation_tests;
mod vector_maintenance_inventory;
pub(crate) mod vector_paged_build;
#[cfg(all(test, feature = "test-seams"))]
mod vector_public_cycle_admission_tests;
#[cfg(test)]
mod vector_published_retry_source_admission_tests;
#[cfg(test)]
mod vector_quarantine_inventory_admission_tests;
#[cfg(test)]
mod vector_retry_frontier_integrity_tests;
mod vector_source_page_admission;
#[cfg(test)]
mod vector_suffix_inspection_tests;
mod vector_writer_session;
#[doc(hidden)]
pub use vector_writer_session::WriterVectorSession;
mod received_row_graph;
mod row_graph;
#[cfg(any(test, feature = "test-seams"))]
pub(crate) mod writer_ownership_receipt;
pub(crate) mod writer_read;
#[cfg(test)]
mod writer_row_charge_tests;
use vector_maintenance_inventory::{
    FailedMaintenancePartitions, MaintenanceBuildCandidates, MaintenanceMetadataFailures,
};
// Compare held units even before their first custody outcome.
mod custody_incumbent;
pub(crate) mod discard;
// Self-contained selections for node-local purge delivery.
pub(crate) mod purge_predicate;
use crate::blob_repository::BlobRepository;
use crate::composite_store::{
    ApplyPhasePause, ChangeLogEntry, ChangeLogLsnRefcounts, ChangeLogTableIndex, CompositeStore,
    VectorPartitionSnapshotStageRegistry, build_received_schema_change_log_entries,
    publish_prepared_change_log_entries, record_change_log_entries,
};
pub use crate::custody_types::{DeliveryManifest, DeliveryOutcome};
#[cfg(any(test, feature = "test-seams"))]
use crate::executor::bounded_read_test_support::{ExecutionProbe, TestSourceTouch, TestWorkSource};
use crate::executor::{
    analyze_passive_relational_route, apply_on_conflict_updates, execute_plan,
    validate_plan_columns,
};
use crate::memory_accounting::{MemoryAccountant, OwnedMemoryReservation, WriterBudget};
use crate::persistence::{
    AuthoritativePurgePersistenceProjection, LoadedVectorPartitionGenerationCatalog,
    LocalSchemaPersistenceProjection, ReadOnlyVectorGenerationSource,
    ReceivedSchemaPersistenceProjection, RedbPersistence, VectorPartitionBaseGenerationRecord,
    VectorPartitionChangeGenerationRecord, VectorPartitionGenerationCandidate,
    VectorPartitionGenerationCatalogRecord, VectorPartitionGenerationDescriptor,
    VectorPartitionGraphBytes, VectorPartitionMembershipRecord, VectorPartitionVersionIdentity,
    VectorPartitionVersionSelector,
};
use crate::persistent_store::PersistentCompositeStore;
use crate::plugin::{
    CommitEvent, CommitSource, CorePlugin, DatabasePlugin, PluginHealth, QueryOutcome,
    SubscriptionMetrics,
};
use crate::rank_formula::{FormulaEvalError, RankFormula};
use crate::read_session::{
    DatabaseOpenOptions, OpenDisposition, OwnerReadConfig, ReadSession,
    owner_read_not_implemented_status,
};
#[cfg(any(test, feature = "test-seams"))]
use crate::read_session::{
    ReadKernelSource, ReadKernelTestObserver, ReadRouteResourceSnapshot, ReadSessionEvent,
    ReadSessionTestObserver,
};
use crate::schema_enforcer::validate_dml;
use contextdb_vector::store::VectorServingPolicy;

/// The pathname that names a throwaway database rather than a file.
const MEMORY_DATABASE_PATH: &str = ":memory:";

#[cfg(any(test, feature = "test-seams"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum StagedDeleteRacePointForTest {
    BeforeRetirement,
    AdmissionReadObserved,
    RetiredCharge(usize),
}

#[cfg(any(test, feature = "test-seams"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum StagedDeleteRaceModeForTest {
    Repaired,
    SplitLock,
}

#[cfg(any(test, feature = "test-seams"))]
struct StagedDeleteRaceHookForTest {
    mode: StagedDeleteRaceModeForTest,
    observe: Box<dyn Fn(StagedDeleteRacePointForTest)>,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static STAGED_DELETE_RACE_HOOK_FOR_TEST: std::cell::RefCell<Option<StagedDeleteRaceHookForTest>> =
        const { std::cell::RefCell::new(None) };
}

/// The engine format values written beside the independently versioned HNSW
/// byte envelope. A loader refuses either mismatch before exposing a route.
pub(crate) const VECTOR_GRAPH_ALGORITHM_VERSION: u16 = 1;
pub(crate) const VECTOR_GRAPH_FORMAT_VERSION: u16 = 1;

/// One private replay slot per authenticated edge identity. A later request
/// from the same edge can exist on the ordinary client path only after the
/// preceding response was received, so replacing this slot is the existing
/// protocol's proof that the preceding request is no longer retry-reachable.
/// The result stores request ordinals instead of natural keys; an exact retry
/// supplies those keys again, while an authoritative purge therefore has no
/// hidden diagnostic copy to erase.
const SYNC_PUSH_OUTCOME_PREFIX: &str = "sync_push_applied.v2.";

// Scoped to the calling OS thread, not the whole process: the default test
// harness dedicates one thread to a test for its whole run, so a process-wide
// flag here let one test's armed failure be consumed by an unrelated test's
// `Database` running on another thread at the same time (observed on
// `restoring_default_schema_keeps_authored_history_held_until_the_peer_upgrades`,
// which never arms this seam itself).
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static RECEIVED_VECTOR_IMAGE_FAILURE_ARMED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static RECEIVED_VECTOR_IMAGE_FAILURE_CONSUMED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}
#[cfg(any(test, feature = "test-seams"))]
static VECTOR_DURABLE_PURGE_FAILURE_ARMED: AtomicBool = AtomicBool::new(false);
#[cfg(any(test, feature = "test-seams"))]
static VECTOR_DURABLE_PURGE_FAILURE_CONSUMED: AtomicBool = AtomicBool::new(false);
/// The vector store whose next durable compaction fails, by address; 0 when
/// unarmed. Scoped to the arming database so another test's maintenance
/// cycle in the same process never consumes it.
#[cfg(any(test, feature = "test-seams"))]
static VECTOR_DURABLE_COMPACTION_FAILURE_ARMED_FOR: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
/// The vector store that consumed the injected compaction failure, by address.
#[cfg(any(test, feature = "test-seams"))]
static VECTOR_DURABLE_COMPACTION_FAILURE_CONSUMED_BY: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

mod transaction_host;
pub(crate) use transaction_host::*;

/// Announce one startup resource to the route observer this open was given,
/// when the build carries the observation seam at all.
macro_rules! observe_open_event {
    ($observer:expr, $event:ident) => {
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(observer) = &$observer {
            observer.observe_event(ReadSessionEvent::$event);
        }
    };
}

use crate::sync_types::{
    ApplyResult, ChangeSet, Conflict, ConflictPolicies, ConflictPolicy, DdlChange,
    DurableSchemaSyncHoldback, EdgeChange, NaturalKey, RefusalCause, RowChange, SyncAdoption,
    SyncDirection, SyncDirectionHistory, VectorChange, natural_key_column_for_meta,
    natural_key_columns_for_meta, natural_key_from_row_values,
};
use contextdb_core::read_contract::{
    DeadlineClock, OwnerReadCancellation, OwnerReadLimits, OwnerReadStatus, OwnerServiceTimeouts,
    OwnerServingReason, OwnerServingState, ReadClientTimeouts, ReadFailure, ReadFailureDetail,
    ReadFailureKind, ReadLimits,
};
use contextdb_core::*;
use contextdb_graph::{
    GraphCursor, GraphStore, MemGraphExecutor, PreparedDagEdgeTypes, PreparedGraphPublication,
};
use contextdb_parser::Statement;
use contextdb_parser::ast::{AlterAction, CreateTable, DataType, Expr};
use contextdb_planner::{OnConflictPlan, PhysicalPlan};
use contextdb_relational::{
    MemRelationalExecutor, PreparedRelationalPublication, RelationalStore, TableProjection,
    index_key_from_values,
};
use contextdb_tx::manager::EngineTransactionHandle;
use contextdb_tx::{
    RelationalDeletePredicate, TransactionManager, WriteSet, WriteSetApplicator,
    row_matches_delete_predicates,
};
#[cfg(any(test, feature = "test-seams"))]
use contextdb_vector::HnswGraphStats;
use contextdb_vector::store::{
    DormantRawVectorLoader, DormantRawVectorPartition, DormantVectorGraphGeneration,
    DormantVectorGraphLoader, DormantVectorLoadRequest, LoadedRawVectorCandidate,
    LoadedRawVectorPartition, LoadedVectorFreshTail, LoadedVectorGraphGeneration,
    RawVectorBodyScan, RawVectorDirectoryEntry, VectorGraphGeneration, VectorPartitionRef,
    VectorRouteQuarantineReason,
};
use contextdb_vector::{
    HnswIndex, MemVectorExecutor, MemoryBudget, PartitionedVectorEntry, PreparedVectorPublication,
    VectorIndexLayout, VectorMaintenanceFailure, VectorMaintenanceFailureDetails,
    VectorMaintenanceReport, VectorPartitionSearchRoute, VectorPartitionSearchTrace,
    VectorSearchDebugTrace, VectorStore, VectorVersionIdentity, cosine_similarity,
};
#[cfg(any(test, feature = "test-seams"))]
use contextdb_vector::{
    VectorGraphCallbackPhaseForTest, VectorJournalTruncationFaultHandle,
    VectorJournalTruncationPhaseForTest, VectorLifecyclePauseHandle,
    VectorMaintenancePreparationPhaseForTest, VectorMemoryWorkspacePhaseForTest,
};
#[cfg(any(test, feature = "test-seams"))]
use contextdb_vector::{VectorMaintenanceProgressPauseHandle, VectorPassiveActivityCounters};

// Raw sync application and its progress writers are only public to this
// crate's test harness. Downstream/default builds retain no capability door.
#[cfg(any(test, feature = "test-seams"))]
macro_rules! sync_test_seam {
    ($(#[$attr:meta])* fn $($item:tt)*) => {
        $(#[$attr])*
        pub fn $($item)*
    };
}

#[cfg(all(test, feature = "sync-orchestration", feature = "test-seams"))]
mod authoritative_purge_kernel_tests;

#[cfg(test)]
mod authoritative_purge_public_contract_tests;

#[cfg(test)]
mod authoritative_purge_blob_storage_contract_tests;

#[cfg(test)]
mod snapshot_inspection_tests;

#[cfg(test)]
mod durable_read_snapshot_admission_tests;

#[cfg(test)]
mod store_finalization_settles_writer_debits_tests;

#[cfg(test)]
mod policy_bounded_load_tests;

#[cfg(test)]
mod policy_hold_scoped_load_tests;

#[cfg(test)]
mod received_schema_stage_tests;

#[cfg(test)]
mod persisted_state_snapshot_tests;

#[cfg(test)]
mod pending_delete_reconciliation_tests;

#[cfg(not(any(test, feature = "test-seams")))]
macro_rules! sync_test_seam {
    ($(#[$attr:meta])* fn $($item:tt)*) => {
        $(#[$attr])*
        pub(crate) fn $($item)*
    };
}

#[cfg(test)]
mod ddl_constraint_rendering_tests;
#[cfg(all(test, feature = "sync-orchestration"))]
mod dependency_unit_pull_adoption_tests;
#[cfg(all(test, feature = "sync-orchestration"))]
mod in_memory_lineage_tests;
mod pending_delete_replay;
#[cfg(test)]
mod projected_generation_accepted_delete_replay_tests;
use pending_delete_replay::PendingReplay;
pub(crate) mod connected_replay;
mod pull_history;
#[cfg(all(test, feature = "test-seams"))]
mod pull_page_contract_tests;
#[cfg(all(test, feature = "test-seams"))]
mod pull_source_page_tests;
#[cfg(test)]
mod query_trace_rows_examined_contract_tests;
#[cfg(test)]
mod received_row_restatement_tests;
#[cfg(test)]
mod received_schema_atomic_retry_tests;
#[cfg(test)]
mod received_schema_atomic_side_effects_tests;
#[cfg(test)]
mod received_schema_bootstrap_echo_tests;
mod resident_pending;
use connected_replay::{ConnectedCoordinates, ConnectedOverlay};
#[cfg(test)]
mod retired_generation_refusal_tests;
#[cfg(test)]
mod same_key_replace_apply_tests;
#[cfg(test)]
mod sync_trigger_vector_epoch_tests;
#[cfg(test)]
mod vector_owner_pairing_ordering_tests;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg(any(test, feature = "test-seams"))]
pub struct SyncApplyReceipt {
    pub tenant_id: TenantId,
    pub node_id: String,
    pub incarnation: Incarnation,
    pub source_lsn: Lsn,
    pub dependency_complete: bool,
    pub hub_contact_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg(not(any(test, feature = "test-seams")))]
pub(crate) struct SyncApplyReceipt {
    pub tenant_id: TenantId,
    pub node_id: String,
    pub incarnation: Incarnation,
    pub source_lsn: Lsn,
    pub dependency_complete: bool,
    pub hub_contact_ms: Option<i64>,
}

/// Private source of schema provenance for one outbound extraction. It is
/// carried by sync only and never stored on the database or exposed through
/// `ChangeSet`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DdlProvenanceKind {
    Historical,
    SyntheticSnapshot,
}

/// The received occurrences validated for one outbound extraction. A
/// synthetic snapshot has replay positions rather than authorship positions,
/// so it cannot carry received schema across a destination frontier.
#[derive(Debug)]
enum ReceivedDdlArrivalValidation {
    Marked(HashSet<(Lsn, u32)>),
}

#[derive(Debug, Clone)]
struct SourceDdlIdentity {
    ddl: DdlChange,
    source_lsn: Lsn,
    ordinal: u32,
}

/// Extraction-scoped schema identities, captured before direction, paging,
/// dependency, or size transforms can remove neighboring DDL.  A surviving
/// historical entry must keep its ordinal so it binds the same immutable
/// sidecar. Synthetic entries use these identities for matching, then take
/// their signed positions from the filtered outbound batch.
#[derive(Debug, Clone)]
pub(crate) struct DdlProvenanceSource {
    kind: DdlProvenanceKind,
    entries: Arc<Vec<SourceDdlIdentity>>,
}

/// Durable receipt shape for one authenticated source DDL identity.  The
/// marker's on-disk contract is fixed before the atomic schema plan writes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableReceivedDdlMarker {
    digest: Vec<u8>,
}

/// Receiver-local proof that one persisted DDL occurrence arrived through an
/// authenticated received-schema commit. This deliberately does not reuse the
/// remote `sync_ddl_apply.v1` marker: destination echo policy needs the local
/// commit position and cannot compare remote source positions to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableReceivedDdlArrival {
    local_lsn: Lsn,
    persisted_ordinal: u32,
    digest: Vec<u8>,
    /// The schema author's own identity for this occurrence -- node,
    /// incarnation, source LSN, ordinal, table generation and attested
    /// digest -- carried unchanged across every forwarding hop, so a
    /// wipe-restored or further peer names the author, not whichever peer
    /// delivered it here. Separate from the delivering peer's transport
    /// bookkeeping (markers, manifests, watermarks), which stays keyed on the
    /// peer that delivered. An unattested unit has no author identity.
    #[serde(default)]
    author: Option<crate::protocol::WireDdlAuthor>,
}

/// Receiver-local proof that one locally authored DDL occurrence is already
/// represented by authoritative schema this node received. It is deliberately
/// a separate mark class from `DurableReceivedDdlArrival`: an arrival names an
/// occurrence this node received and replayed, and destination rebuild proves
/// itself by replaying exactly those occurrences. A superseded occurrence was
/// authored here and is never part of that replay; it only stops outbound echo
/// of schema the peer demonstrably already has.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableSupersededDdlOccurrence {
    local_lsn: Lsn,
    persisted_ordinal: u32,
    digest: Vec<u8>,
}

/// One complete, ordered received-DDL vector and the exact outcome published
/// by its original receiver commit. Individual markers prove per-item
/// provenance; this record makes retries an all-or-nothing operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableReceivedSchemaManifest {
    markers: Vec<DurableReceivedSchemaManifestMarker>,
    result: crate::protocol::WireApplyResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DurableReceivedSchemaManifestMarker {
    key: String,
    digest: Vec<u8>,
}

enum ReceivedSchemaManifestClassification {
    Apply,
    Replay(crate::protocol::WireApplyResult),
}

/// Memory databases have no restart contract, but authenticated sync still
/// runs for the life of the handle. Keep the same whole-vector evidence as
/// Redb so a lost reply cannot reapply it in that lifetime.
#[derive(Default)]
struct InMemoryReceivedSchemaManifestState {
    markers: HashMap<String, DurableReceivedDdlMarker>,
    manifests: HashMap<String, DurableReceivedSchemaManifest>,
}

/// One immutable source-DDL item, retained in the exact author order rather
/// than regrouped into table/event/trigger categories.  `persisted_ordinal` is
/// local only: it addresses the receiver's one ordered DDL log and generation
/// sidecars, while `source_ordinal` remains part of the authenticated marker
/// identity.
#[derive(Debug, Clone)]
struct ReceivedSchemaOrderEntry {
    change: DdlChange,
    persisted_ordinal: u32,
    generation: DurableDdlGenerationSidecar,
    local_arrival: DurableReceivedDdlArrival,
}

/// Durable values whose order and bytes are fixed before Redb starts its
/// transaction.  Later phases hand this whole payload to the existing single
/// transaction; they must not encode, repartition, or category-sort it there.
#[derive(Debug, Clone, Default)]
struct ReceivedSchemaPersistencePayload {
    ddl_log_replacement: Vec<(Lsn, DdlChange)>,
    generation_values: Vec<(String, Vec<u8>)>,
    marker_values: Vec<(String, Vec<u8>)>,
    received_arrival_values: Vec<(String, Vec<u8>)>,
    superseded_occurrence_values: Vec<(String, Vec<u8>)>,
    lineage_values: Vec<(String, Vec<u8>)>,
    accepted_author_values: Vec<(String, Vec<u8>)>,
    source_state_values: Vec<(String, Vec<u8>)>,
    receipt_values: Vec<(String, Vec<u8>)>,
    applied_push_watermark_values: Vec<(String, Vec<u8>)>,
    terminal_values: Vec<(String, Vec<u8>)>,
    config_values: Vec<(String, Vec<u8>)>,
    config_max_u64_keys: Vec<String>,
}

/// Owned entries for the received unit's affected keys. Publication merges
/// history and replaces only the affected tables' mutable mirrors under the
/// existing schema gate, leaving unrelated committed state borrowed throughout
/// preparation.
#[derive(Default)]
struct ReceivedSchemaMemoryMirrors {
    table_generations: HashMap<String, u64>,
    ddl_generations: HashMap<(Lsn, u32), (Option<String>, Option<u64>)>,
    received_ddl_arrivals: HashMap<(Lsn, u32), DurableReceivedDdlArrival>,
    superseded_ddl_occurrences: HashMap<(Lsn, u32), DurableSupersededDdlOccurrence>,
    source_state: HashMap<(String, String), SyncTombstoneArrival>,
    graph_arrivals: HashMap<GraphIdentity, FundedGraphArrival>,
    accepted_authors: HashMap<(String, Vec<u8>), String>,
    terminal_markers: HashMap<TerminalRefusalMarkerKey, TerminalRefusalMarkerRecord>,
    terminal_scans: HashMap<TerminalRefusalContextKey, TerminalRefusalScanState>,
    /// The durable receipt and per-source applied-push watermark share the
    /// engine's one in-memory watermark mirror; preparation merges both
    /// before this post-durability swap.
    receipt_and_applied_watermarks: HashMap<String, Lsn>,
}

/// One immutable authored declaration and its original funding. The payload
/// precedes its credit so even the last Arc's refusal/unwind destruction cannot
/// refund a declaration that another accepted occurrence still owns.
pub(crate) struct AuthoredRowShape {
    meta: TableMeta,
    _credit: contextdb_core::read_memory::ReadCredit,
}

/// Private, commit-LSN keyed state for one authenticated received schema
/// unit.  It never crosses the public `ChangeSet` boundary.  Everything in
/// it is prepared before durability; `publish_received_schema_stage` contains
/// only memory replacement after that durability succeeds.
pub(crate) struct ReceivedSchemaStage {
    memory_history: Option<PreparedSchemaHistory>,
    custody_memory: Option<custody_store::CustodyDelta>,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    source_order: Vec<ReceivedSchemaOrderEntry>,
    relational: Vec<TableProjection>,
    affected_tables: HashSet<String>,
    graph: Option<PreparedGraphPublication>,
    affected_graph_edge_types: HashSet<String>,
    dag_edge_types: Option<PreparedDagEdgeTypes>,
    rank_formulas: Option<HashMap<(String, String), Arc<RankFormula>>>,
    vector: Option<contextdb_vector::PreparedVectorTablePublication>,
    /// Policy-only layout changes to live vector columns this unit does not
    /// replace, admitted before durability and applied in place after it.
    vector_policy_layouts: Vec<contextdb_vector::PreparedVectorPolicyLayout>,
    /// Tables whose only vector change in this unit was policy: their
    /// vectors were not replaced, so they await no received vector image.
    vector_policy_tables: HashSet<String>,
    event_bus: Option<event_bus::PreparedEventBusPublication>,
    /// Acquired while the received-schema unit is still being prepared and
    /// retained through its matching durable config transaction plus queue
    /// publication, so a purge cannot split that queue-definition change.
    queue_mutation_lease: Option<event_bus::QueueMutationLease>,
    trigger: Option<trigger::PreparedTriggerPublication>,
    native_trigger: Option<trigger::OriginalPreparedTriggerPublication>,
    pub(crate) sink_events: Vec<event_bus::PreparedSinkEvent>,
    next_sink_queue_id: u64,
    pub(crate) trigger_audits: Vec<(u64, TriggerAuditEntry)>,
    native_audit_memory: Option<trigger::OriginalAuditMemoryPublication>,
    next_trigger_audit_index: u64,
    mirrors: ReceivedSchemaMemoryMirrors,
    memory_swap: PreparedMemorySwap,
    /// The detached projection owns row allocation while it is being
    /// adjudicated.  It becomes visible to the live allocator only after the
    /// matching Redb transaction commits and the memory image is published.
    next_row_id: RowId,
    persistence: ReceivedSchemaPersistencePayload,
    /// The detached adjudicator's exact outcome. It stays private until the
    /// staged receiver commit has crossed durability; test-only receipt
    /// observation copies it immediately before this stage is consumed.
    apply_result: ApplyResult,
    /// Absent for an unattested unit: a manifest is the replay identity of a
    /// SIGNED migration, and a unit with no provenance has nothing to be
    /// idempotent against.
    received_schema_manifest: Option<(String, DurableReceivedSchemaManifest)>,
    in_memory_lineage_delta: InMemoryLineageDelta,
    /// Moved (never copied) into the committing transaction by the
    /// pre-commit callback, through the registry's shared access; the lock is
    /// inline and allocates nothing.
    pub(crate) write_set: parking_lot::Mutex<WriteSet>,
    /// The funding of the ownership keys custody retirement moved into
    /// `write_set`; declared after it, so it is released only after the write
    /// set is dropped. Moved out together with the write set.
    retired_custody_keys: parking_lot::Mutex<Vec<crate::custody::store::CustodyCharge>>,
    pub(crate) durable_projection: ReceivedSchemaPersistenceProjection,
    pub(crate) structurally_dropped_tables: HashSet<String>,
    pub(crate) change_log_entries: Vec<ChangeLogEntry>,
    operational_memberships: Option<contextdb_relational::store::PreparedMembershipBatch>,
    /// Existing private image invoice moves with its row source. The stores
    /// drop before the metadata/copy credits that funded their construction.
    _working_row_owner: Option<RetainedNonVectorMemoryOwner>,
    operational_copy_admission: Option<OwnedMemoryReservation>,
    metadata_copy_charges: MetadataCopyCharges,
    _vector_copy_owner: contextdb_vector::VectorCopyAdmission,
    _connected_credits: Option<(
        Arc<contextdb_core::read_memory::ReadCredit>,
        Arc<contextdb_core::read_memory::ReadCredit>,
    )>,
    _private_prefix_effects: parking_lot::Mutex<Option<connected_replay::PrivatePrefixEffects>>,
    _native_trigger_config_fee: Option<OwnedMemoryReservation>,
    // Declared after persistence so its copied edge bodies retire first.
    _received_edge_input_original: Option<OwnedMemoryReservation>,
}

/// The prepared vector payload precedes its exact executing export owner on
/// every failure path as well as its final publication handoff.
struct ReceivedRestampedVectorImage {
    entries: Vec<PartitionedVectorEntry>,
    owner: contextdb_vector::VectorCopyAdmission,
}

/// The write set leaves the stage before persistence can destroy that stage.
/// Its exact preparation owners leave with it, and stay in the enclosing
/// commit scope until the returned/failed write set has physically dropped.
/// Private image payloads precede their construction invoices on destruction.
struct ReceivedSchemaPreparationCustody {
    _working_row_owner: Option<RetainedNonVectorMemoryOwner>,
    _metadata_copy_charges: MetadataCopyCharges,
    _vector_copy_owner: contextdb_vector::VectorCopyAdmission,
    _connected_credits: Option<(
        Arc<contextdb_core::read_memory::ReadCredit>,
        Arc<contextdb_core::read_memory::ReadCredit>,
    )>,
}

pub(crate) type ReceivedSchemaStageRegistry = Arc<Mutex<schema_stage_registry::ReceivedRegistry>>;

/// Holds a registered received-schema stage only until somebody takes
/// ownership of it, and removes it otherwise.
///
/// Stages live in a database-owned map, and stack unwinding does not clear a
/// map. A panic between registration and the durable commit would leave the
/// stage there for the life of the database, holding its prepared memory
/// reservation, its projections and its queue-mutation lease -- the receiver
/// would keep serving while a slice of its declared limit stayed spent on a
/// unit that never landed. The `Result` branches that remove the stage on a
/// prepare or apply failure stay exactly as they are; this makes the panic
/// path agree with them instead of adding a second answer.
struct ReceivedSchemaStageRegistration {
    /// The same commit LSN's arriving shape, staged so `CompositeStore::
    /// apply_exact` can resolve this unit's own vector writes against the
    /// schema it is installing. Removed here unconditionally alongside the
    /// stage itself: the durable-persistence path for a registered stage
    /// never calls `apply_exact`, so nothing else is guaranteed to consume
    /// it, and removing an already-consumed entry is a harmless no-op.
    arriving_table_meta: crate::composite_store::ReceivedArrivingTableMetaRegistry,
    /// Vector indexes registered early, for tables this delivery is
    /// creating, so preparing this unit's own vector writes has a live
    /// layout to resolve against. Deregistered here unconditionally --
    /// publication re-registers them from the durable image on success, and
    /// a refusal must leave no trace of a table that never durably existed.
    arriving_vector_indexes: Vec<VectorIndexRef>,
    next_arriving_index_cleanup: usize,
    arriving_metadata_cleaned: bool,
    arriving_metadata_installed: bool,
    published: bool,
    occupancy: Option<schema_stage_registry::ReceivedToken>,
    vector_store: Arc<VectorStore>,
    accountant: crate::memory_accounting::AccountantHandle,
    lsn: Lsn,
    armed: bool,

    // Original registry custody is last: all preparation children and the
    // occupancy token finish before its counted creating handle can retire.
    registry: ReceivedSchemaStageRegistry,
}

struct ReceivedRegistrationCleanup<'a>(&'a mut ReceivedSchemaStageRegistration);

/// The actual image work one received-schema apply performed on the calling
/// thread. This is test-only instrumentation; it changes neither the staged
/// image nor its durable commit.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReceivedSchemaApplyObservationForTest {
    pub staged_table_images_copied: usize,
    pub staged_table_names: std::collections::BTreeSet<String>,
    pub staged_rows_copied: usize,
    pub staged_graph_edges_copied: usize,
    pub staged_vectors_copied: usize,
    pub durable_table_rewrites: usize,
    pub durable_table_names: std::collections::BTreeSet<String>,
    pub durable_rows_rewritten: usize,
    pub profile_nanos: std::collections::BTreeMap<String, u128>,
    pub profile_work: std::collections::BTreeMap<String, u64>,
    #[cfg(any(test, feature = "test-seams"))]
    pub artifact_writes: std::collections::BTreeMap<redb::TableWriteForTest, u64>,
    #[cfg(any(test, feature = "test-seams"))]
    pub durable_observer_attached: bool,
}

#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Default)]
struct ReceivedSchemaApplyObservationStateForTest {
    traces_started: usize,
    observation: ReceivedSchemaApplyObservationForTest,
}

#[cfg(any(test, feature = "test-seams"))]
type SharedReceivedSchemaApplyObservationForTest =
    Arc<Mutex<ReceivedSchemaApplyObservationStateForTest>>;

/// Caller-owned observation sink for one or more explicitly scoped received
/// schema applies. `observation` returns `None` until the real received-schema
/// stage has attached, so a missing instrumentation path cannot look like a
/// successful zero-work replay.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct ReceivedSchemaApplyObserverForTest {
    inner: SharedReceivedSchemaApplyObservationForTest,
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub struct ReceivedSchemaApplyObservationScopeForTest {
    #[cfg(any(test, feature = "test-seams"))]
    _durable: Option<redb::TableWriteObservationGuardForTest>,
    slot: Arc<Mutex<Option<SharedReceivedSchemaApplyObservationForTest>>>,
    installed: SharedReceivedSchemaApplyObservationForTest,
    previous: Option<SharedReceivedSchemaApplyObservationForTest>,
}

#[cfg(any(test, feature = "test-seams"))]
struct ReceivedSchemaApplyThreadObservationScopeForTest {
    previous: Option<SharedReceivedSchemaApplyObservationForTest>,
}

#[cfg(any(test, feature = "test-seams"))]
std::thread_local! {
    static RECEIVED_SCHEMA_APPLY_OBSERVER: std::cell::RefCell<
        Option<SharedReceivedSchemaApplyObservationForTest>
    > = const { std::cell::RefCell::new(None) };
}

type PreparedDeliveryLineages = HashMap<(String, Vec<u8>, Lsn), crate::protocol::WireRowLineage>;

type DdlGenerationKey = (Lsn, u32);
type DdlGenerationValue = (Option<String>, Option<u64>);

/// A locally authored table-schema vector held privately until its owning
/// transaction receives a commit LSN.  Unlike received-schema staging, this
/// carries no sync receipts or lineage adjudication: it is solely the
/// durable-before-memory hand-off for SQL BEGIN/COMMIT DDL.
pub(crate) struct LocalSchemaStage {
    memory_history: Option<PreparedSchemaHistory>,
    relational: Vec<TableProjection>,
    dag_edge_types: PreparedDagEdgeTypes,
    rank_formulas: HashMap<(String, String), Arc<RankFormula>>,
    vector: Option<contextdb_vector::PreparedVectorTablePublication>,
    event_bus: Option<event_bus::PreparedEventBusPublication>,
    native_event_bus: std::cell::RefCell<Option<event_bus::OwnedDefinitionPublication>>,
    _queue_mutation_lease: Option<event_bus::QueueMutationLease>,
    trigger: trigger::PreparedTriggerPublication,
    native_trigger: Option<trigger::OriginalPreparedTriggerPublication>,
    table_generations: Vec<(String, u64)>,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    ddl_log: Vec<(Lsn, DdlChange)>,
    memory_swap: PreparedMemorySwap,
    pub(crate) durable_projection: LocalSchemaPersistenceProjection,
    _vector_copy_owner: contextdb_vector::VectorCopyAdmission,
    _temporary_memory: TemporaryMemoryReservation,
}

pub(crate) type LocalSchemaStageRegistry = Arc<Mutex<schema_stage_registry::LocalRegistry>>;

/// Holds one commit LSN's entry in a database-owned stage map -- the local
/// schema stage, the local erasure stage, or the vector snapshot sample --
/// only until ownership of it legitimately passes on, and removes it
/// otherwise.
///
/// The same rule as `ReceivedSchemaStageRegistration`: stack unwinding does
/// not clear a map. A panic between registration and the point where the
/// commit consumes the entry would leave it there for the life of the
/// database, still holding its prepared memory swap, its temporary
/// reservation, its vector workspace and its queue-mutation lease. The
/// `Result` branch that removes these entries on a commit failure stays as it
/// is; this makes the unwind path agree with it. Removing an entry the commit
/// already consumed is a harmless no-op, and the removed value is dropped
/// after the map's lock is released, so its own `Drop` never runs under it.
pub(crate) struct CommitStageRegistration<V> {
    registry: Arc<Mutex<HashMap<Lsn, V>>>,
    lsn: Lsn,
    armed: bool,
}

/// Watches the stretch from the durable commit to the end of in-memory
/// publication, and makes the database stop serving if it is left by an
/// unwind.
///
/// Publication after the durable commit contains no fallible work, so a panic
/// there is an engine bug, not a
/// resource outcome. It can still happen -- publication contains `expect`s,
/// and the transaction manager advances its committed watermark only after
/// publication returns -- and once it has, this handle's memory no longer
/// matches the durable image and there is no correct partial publication to
/// patch it back to. So the watch is armed just before the durable apply and
/// completed only when publication has returned or the commit has failed
/// with an ordinary error; dropping it armed (an unwind) marks the database
/// `publication_interrupted`, every later read, write and transaction is
/// refused with `Error::PublicationInterrupted`, and reopening recovers the
/// complete committed state from the durable image.
struct PublicationInterruptionWatch {
    interrupted: Arc<AtomicBool>,
    armed: bool,
}

/// Where a test makes a LOCAL commit unwind, immediately after it has put a
/// prepared entry into one of the database-owned stage maps.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum LocalCommitStageFaultPoint {
    /// The transactional local schema stage is in `local_schema_stages`.
    AfterLocalSchemaStageRegistration,
    /// This commit's registered-snapshot sample is in
    /// `vector_partition_snapshot_stages`.
    AfterVectorSnapshotStageRegistration,
    /// The prepared erasure image is in `local_erasure_stages`.
    AfterLocalErasureStageRegistration,
    /// The LOCAL commit's durable apply has already succeeded and its
    /// prepared schema image is about to replace live memory. Past this
    /// point publication does no fallible work, so an unwind here is an
    /// engine bug with no correct partial publication to return to: the
    /// handle must stop serving and a reopen must recover the complete
    /// committed state. The receiver-side twin is
    /// `ReceivedUnitFaultPoint::AfterDurableCommitBeforePublish`.
    AfterDurableCommitBeforeLocalPublication,
    /// The same boundary on a SCHEDULED JOB's commit: its durable apply has
    /// succeeded and its sink events, trigger audits and commit event have
    /// not been published to memory yet.
    AfterScheduledJobDurableCommit,
    /// The same boundary on the RESERVED-LSN commit that stamps expired
    /// rows: durable, and the in-memory commit bookkeeping the transaction
    /// manager does for it not yet complete.
    AfterReservedLsnDurableCommit,
}

#[cfg(test)]
thread_local! {
    static LOCAL_COMMIT_STAGE_FAULT: std::cell::Cell<Option<LocalCommitStageFaultPoint>> =
        const { std::cell::Cell::new(None) };
}

struct PendingLocalSchemaStage {
    working: Database,
    base_tables: HashMap<String, TableMeta>,
    base_event_bus: event_bus::EventBusDefinitions,
    base_triggers: BTreeMap<String, TriggerDeclaration>,
    base_table_generations: HashMap<String, u64>,
    temporary_memory: TemporaryMemoryReservation,
}

/// The caller-owned facts that govern a received unit's existing
/// conflict/adoption adjudication.  Schema preparation must carry these
/// unchanged; replacing them with a convenient default can accept a row that
/// the real authenticated apply would refuse.
/// Which leg of sync an apply is serving. An absent receipt does NOT identify
/// the pull leg on its own: a hub applies a push from an unidentified peer
/// receipt-less too, and there the local value is the first-accepted one that
/// KEEP FIRST exists to protect. Only the pull leg adopts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncApplyRole {
    PullLeg,
    HubPushApply,
}

struct ReceivedSchemaAdjudicationInputs<'a> {
    arrivals: &'a HashMap<Lsn, Option<Lsn>>,
    adoption: SyncAdoption,
    apply_role: SyncApplyRole,
    receipt: Option<SyncApplyReceipt>,
    push_outcome_key: Option<&'a SyncPushOutcomeContext>,
    dependency_complete: bool,
    terminal_refusal_context: Option<&'a TerminalRefusalPullContext>,
    hub_local_author: Option<&'a str>,
    /// The transaction manager's reserved visibility identity.  It is
    /// intentionally separate from the commit LSN.
    receiver_tx: TxId,
    /// The conflict policies the LOCAL caller declared, carried only for a
    /// unit with no provenance. The door this routing replaced adjudicated an
    /// unattested delivery with exactly these, so routing it through the stage
    /// has to as well. An AUTHENTICATED unit keeps the receiver's own declared
    /// policies: a peer does not get to choose how its rows win.
    unattested_policies: Option<&'a ConflictPolicies>,
}

/// Immutable transaction controls captured under one write-set guard. Payloads
/// retain their mutation-time owners; search never borrows a mutable WriteSet.
struct CapturedVectorWriteSet {
    vector: crate::executor::TransactionVectorOverlay,
    rows: Option<Arc<contextdb_tx::StatementRows>>,
    deleted_rows: HashSet<RowId>,
    staged_entries: usize,
    touches: bool,
}

struct StrictVectorOverlayRequest<'a> {
    index: VectorIndexRef,
    query: &'a [f32],
    k: usize,
    candidates: Option<&'a RoaringTreemap>,
    snapshot: SnapshotId,
    search_mode: contextdb_core::VectorSearchMode,
}

/// What one received DDL item's DURABLE half asks the accountant for, on top
/// of the shape the item installs: the content-identity proof maintenance every
/// `persist_table_meta` runs, and the metadata write itself.
///
/// A stated allowance, not a model. Measured at 209 bytes for a single index
/// item, and it is the receiver's own durable bookkeeping rather than anything
/// the arriving unit describes, so it is given room. It is what stops a unit
/// whose SHAPE fits from being refused inside its first item's durable half --
/// after that item's in-memory change is already complete.
const RECEIVED_DDL_ITEM_DURABLE_ALLOWANCE_BYTES: usize = 4 * 1024;

/// The source a received schema unit is held against for memory, and its
/// position in that source's order. See `Database::received_unit_hold_identity`.
struct ReceivedUnitHoldIdentity {
    tenant_id: TenantId,
    source_node_id: String,
    position: Lsn,
}

/// What this node's declared table policy does to one received batch: which
/// DDL entries install (whole authored transactions only), which data groups
/// are held behind a refused transaction, the typed answer for each refused
/// or held unit, the durable holds the refusals leave, and the tables whose
/// holds an installing transaction releases. See
/// `Database::received_policy_plan`.
#[derive(Default)]
struct ReceivedPolicyPlan {
    keep_ddl: Vec<bool>,
    held_data_lsns: BTreeSet<Lsn>,
    held_data_tables: BTreeMap<Lsn, BTreeSet<String>>,
    conflicts: Vec<Conflict>,
    new_holds: Vec<DurablePolicyHold>,
    released: BTreeSet<String>,
    refused_units: Vec<(Vec<usize>, String, String)>,
}

/// Read-only policy state accumulated in source order while validating a pull
/// page. Nothing here is durable until the validated units reach canonical apply.
#[derive(Default)]
pub(crate) struct PullPolicyProjection {
    holds: Vec<DurablePolicyHold>,
    released: BTreeSet<String>,
    created: BTreeSet<String>,
}

/// The policy-excluded portion is kept for canonical apply to record its refusal
/// once, after every unit on the page has passed provenance validation.
pub(crate) struct PullPolicyPreflight {
    original_ddl: Vec<DdlChange>,
    original_ddl_lsn: Vec<Lsn>,
    held_data: ChangeSet,
    pub(crate) conflicts: Vec<Conflict>,
}

/// One table a receiver holds later deliveries back on, because an authored
/// transaction touching it was refused by this node's declared table policy.
/// Durable, so the hold outlives the call that refused the transaction, later
/// pages and a restart. See `Database::received_policy_plan`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DurablePolicyHold {
    /// The peer the refused transaction arrived from; empty for a delivery
    /// that named none.
    source_node_id: String,
    #[serde(default)]
    tenant_id: Option<TenantId>,
    #[serde(default)]
    source_incarnation: Option<Incarnation>,
    /// The author who signed the refused transaction, when it was signed.
    author_node_id: Option<String>,
    /// The refused transaction's source position.
    source_lsn: Lsn,
    table: String,
    /// The typed refusal the author has to act on.
    refusal: String,
    /// The refused transaction would have created this table, and it does not
    /// exist here: nothing from any peer can install into it.
    table_absent: bool,
    /// Declared edge ownership remains available even when creation was refused.
    #[serde(default)]
    dag_edge_types: Vec<EdgeType>,
}

/// A selected collection keeps its admission until its last consumer finishes.
struct ScopedPolicyHolds {
    entries: Vec<(String, DurablePolicyHold)>,
    memory: crate::read_image_memory::ImageMemoryHandle,
}

struct ScopedPolicyHoldReleases {
    entries: Vec<(String, DurablePolicyHoldRelease)>,
    memory: crate::read_image_memory::ImageMemoryHandle,
}

/// Committed alongside the exact accepted schema correction. A stale hold
/// surviving post-commit cleanup is ignored only when this fact names it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurablePolicyHoldRelease {
    hold: DurablePolicyHold,
    tenant_id: TenantId,
    source_node_id: String,
    source_incarnation: Incarnation,
    correction_digest: [u8; 32],
}

/// Exact pending data, including the evidence needed by authenticated replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurablePolicyHeldData {
    changes: ChangeSet,
    arrivals: HashMap<Lsn, Option<Lsn>>,
    receipt: SyncApplyReceipt,
    hub_node_id: String,
    lineages: Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>,
    outcome_key: SyncPushOutcomeKey,
    /// Schema dependencies of data whose own payload names no table.
    #[serde(default)]
    held_tables: BTreeSet<String>,
}

/// One table this receiver holds received data back on because an authored
/// transaction touching it was refused by this node's declared table policy.
/// See `Database::policy_held_received_tables`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PolicyHeldReceivedTable {
    /// The peer the refused transaction arrived from.
    pub source_node_id: String,
    /// The author who signed the refused transaction, when it was signed.
    pub author_node_id: Option<String>,
    /// The refused transaction's source position.
    pub source_lsn: Lsn,
    pub table: String,
    /// The refused transaction would have created the table and it does not
    /// exist here, so later deliveries for it are held from every peer.
    pub creation_refused: bool,
    /// The typed refusal the author has to act on.
    pub refusal: String,
}

/// The one source every received schema unit with no provenance is held
/// against: it has no author, but it still has a table to wait on.
const UNATTESTED_RECEIVED_UNIT_HOLD_SOURCE: &str = "unattested-received-schema";

/// The refusal a unit gets for arriving behind a memory hold on its table.
const RECEIVED_SCHEMA_MEMORY_HOLDBACK_OPERATION: &str = "received_schema_memory_holdback";

/// What an operator can do about a receiver that cannot afford to take
/// delivery of an arriving unit: the copy is proportional to the tables the
/// unit reshapes, not to the unit itself.
const STAGE_RECEIVED_UNIT_COPY_HINT: &str = "Raise MEMORY_LIMIT on the receiver, or send the schema change over smaller tables: taking \
     delivery copies every table the unit reshapes before it publishes anything.";

/// Set while a test needs the receiver's durable memory-hold record to refuse
/// every write, so the answer an installed unit's author gets can be observed
/// rather than argued.
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static REFUSE_MEMORY_HOLDBACK_WRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Where a test makes taking delivery stop.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ReceivedUnitFaultPoint {
    /// Its widest point: the whole affected image copied into the private
    /// worker, no arriving DDL applied to it, nothing published and nothing
    /// durable. Only the copy exists, which is where a receiver's bill has to
    /// come back to exactly what it was.
    AfterWorkingCopy,
    /// After the prepared stage has been put in the database's stage map and
    /// before the durable commit. Nothing owns the stage here but that map,
    /// and unwinding does not clear a map.
    AfterStageRegistration,
    /// Immediately before the durable (Redb) commit. The stage is still
    /// registered and nothing is durable yet: a caught unwind or process
    /// death here both leave the OLD complete state.
    BeforeDurableCommit,
    /// After the durable commit has succeeded, before publication moves the
    /// prepared image into place -- ahead of every publication component,
    /// the custody delta included. The durable image already carries the NEW
    /// state; a caught unwind here must still see that state after a
    /// restart, because in-memory publication is derived from the durable
    /// image, never the other way round.
    AfterDurableCommitBeforePublish,
    /// Between two of publication's own components -- the relational
    /// projection replaced, the vector image not yet. Publication is
    /// documented to do no fallible work past the durable commit, so this
    /// point exists to prove a panic here is recovered by restart rather than
    /// patched over with a partial-publication unwind.
    BetweenPublicationComponents,
}

/// How it stops.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ReceivedUnitCopyFault {
    /// The ordinary typed error path.
    Error,
    /// A caught unwind, which no `Result` branch cleans up after.
    Unwind,
    /// A real process death -- no unwind, no `Drop`, no panic hook, nothing
    /// this process was doing runs again. Proves the design's answer does
    /// not secretly depend on cleanup code a genuine crash would never reach:
    /// only the durable image the next `Database::open` re-derives from.
    Abort,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static RECEIVED_UNIT_FAULT: std::cell::Cell<
        Option<(ReceivedUnitFaultPoint, ReceivedUnitCopyFault)>,
    > = const { std::cell::Cell::new(None) };
}

/// One table whose arriving schema unit this receiver refused because its
/// declared `MEMORY_LIMIT` could not hold the whole unit.
///
/// The author is not told the unit landed: it is re-delivered and retried when
/// memory allows. Units for tables not listed here keep applying normally.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MemoryHeldReceivedSchemaUnit {
    pub peer_node_id: String,
    pub table: String,
    pub held_at: Lsn,
    pub required_bytes: usize,
}

/// Actual durable vector tables from one pinned storage read transaction.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableVectorStorageInventoryForTest {
    pub partition_summaries: usize,
    pub empty_partition_summaries: usize,
    pub catalog_frontiers: Option<(TxId, Option<TxId>)>,
    pub catalogs: usize,
    pub base_generations: usize,
    pub change_generations: usize,
    pub graph_pages: usize,
    pub paged_pages: usize,
    pub paged_restart_records: usize,
    pub paged_cleanup_entries: usize,
}

/// A private reservation for replacing the in-memory database image. The
/// complete arriving image is admitted while the outgoing image remains paid.
/// Refusal or rollback releases only the arriving reservation. After the old
/// physical image has been destroyed, publication transfers the new charge to
/// the store and releases the old charge.
pub(crate) struct PreparedMemorySwap {
    history: Option<contextdb_relational::schema_generation::PreparedSchemaGeneration>,
    accountant: crate::memory_accounting::AccountantHandle,
    reserved_new_image: usize,
    release_after_swap: usize,
    /// What each row version STAGED in a still-open transaction is admitted
    /// for once this swap's shape is the live one.
    ///
    /// The re-pricing has to land exactly when the shape does. Written before
    /// the publication, a change that is then refused or fails to persist
    /// leaves every staged row recorded at a figure nothing charged, and the
    /// release hands back more than admission took.
    staged_admissions: Option<StagedAdmissionRepricing>,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

/// The staged row versions one shape change re-prices, per open transaction.
struct StagedAdmissionRepricing {
    tx_mgr: DatabaseTransactionHost,
    per_tx: HashMap<TxId, Vec<(String, RowId, usize)>>,
}

/// Real-accountant reservation for private staging allocations that disappear
/// after commit or rollback. Unlike [`PreparedMemorySwap`], none of these
/// bytes becomes retained database state.
struct TemporaryMemoryReservation {
    original: Option<OwnedMemoryReservation>,
    accountant: crate::memory_accounting::AccountantHandle,
    bytes: usize,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

/// A charge admitted for something the detached received-schema copy is about
/// to hold, owned here until whatever will release it takes it over.
///
/// The copy admits each object BEFORE allocating it, and the detached
/// database's drop owner releases only what actually reached its stores. In
/// between -- an index membership refused after its row was admitted, a
/// vector copy refused while it is being attached -- nothing else owns the
/// charge, so an error or an unwind there would leave it on the receiver's
/// bill for good. This guard returns it on every such exit; `transfer`
/// hands it over once the store holding the object will release it instead.
struct StagedCopyCharge {
    vector_accountant: std::mem::ManuallyDrop<contextdb_vector::MemoryBudgetOwner>,
    accountant: std::mem::ManuallyDrop<crate::memory_accounting::AccountantHandle>,
    bytes: usize,
    _writer_use: std::mem::ManuallyDrop<Option<crate::memory_accounting::WriterUse>>,
    // Inline charges own no Box. Only the prepaid handoff fills this liability.
    own_control: Option<crate::memory_accounting::ControlCredit>,
}

struct StagedCopyChargeCleanup {
    source: *mut StagedCopyCharge,
    next: u8,
    refund: Option<crate::memory_accounting::PendingControlRefund>,
}

/// A private attempt must remember omitted-table accesses even when a
/// transactional callback catches the lookup error. Only the first miss is
/// needed: replay discovers further tables after this one has been staged.
struct ReceivedStagingAccess {
    source: std::mem::ManuallyDrop<Arc<RelationalStore>>,
    staged_tables: std::mem::ManuallyDrop<HashSet<String>>,
    accountant: std::mem::ManuallyDrop<crate::memory_accounting::AccountantHandle>,
    miss: std::mem::ManuallyDrop<Mutex<Option<ReceivedStagingMiss>>>,
    _charge: std::mem::ManuallyDrop<StagedCopyCharge>,
    _writer_use: std::mem::ManuallyDrop<Option<crate::memory_accounting::WriterUse>>,
    vector_accountant: std::mem::ManuallyDrop<contextdb_vector::MemoryBudgetOwner>,
    own_control: Option<crate::memory_accounting::ControlCredit>,
}

enum ReceivedStagingMiss {
    Table {
        name: String,
        _charge: StagedCopyCharge,
    },
    Refused(Error),
}

struct ReceivedStagingAccessCleanup {
    source: *mut ReceivedStagingAccess,
    next: u8,
    refund: Option<crate::memory_accounting::PendingControlRefund>,
}

/// A read-only memory settlement plan.  Unlike [`PreparedMemorySwap`], this
/// contains no accountant and performs no allocation; it makes the vector
/// payload/HNSW retirement visible to the future atomic commit kernel.
#[derive(Debug, Clone, Copy)]
struct PreparedMemorySwapAccounting {
    old_bytes: usize,
    new_bytes: usize,
    retired_bytes_released_by_store: usize,
}

struct ReceivedSchemaSemanticProjection {
    tables: HashMap<String, TableMeta>,
    event_bus: event_bus::EventBusDefinitions,
    trigger: BTreeMap<String, TriggerDeclaration>,
    catalog_charge: Option<StagedCopyCharge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableDdlGenerationSidecar {
    pub(crate) table: Option<String>,
    pub(crate) table_generation: Option<u64>,
}

/// Provenance for the CURRENT delete marker of a natural key. This is a
/// process-local guard only: row lineage lives in config-keyed sidecars, while
/// a deleted row deliberately does not retain an unbounded historical record.
#[derive(Debug, Clone, Copy)]
struct SyncTombstoneArrival {
    arrival: Lsn,
    delete_lsn: Lsn,
    /// Local owner identity for exact vector tombstone classification. This is
    /// intentionally process-local with the rest of delete provenance.
    row_id: Option<RowId>,
    kind: contextdb_relational::store::SyncSourceKind,
    /// A status regression proved this accepted-local delete's old hub order
    /// no longer exists. Keep suppressing live replays until the delete is
    /// reaccepted, but never suppress the delete outbound.
    accepted_local_order_invalidated: bool,
}

/// Config-keyed per-lineage state shared by ordinary delete adjudication and
/// the authoritative purge plane. Redb retains it across reopen; an in-memory
/// database retains it only for that shared database life. The latter has no
/// SQL or wire entry point in this slice, but it deliberately occupies the
/// same record so a future purge cannot create a second deletion lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableLineageRecord {
    table: String,
    natural_key: NaturalKey,
    table_generation: u64,
    #[serde(default)]
    local_row_id: Option<RowId>,
    /// Whether this delete belongs to a root created by this database. Missing
    /// pre-release bytes default to false so decoding never invents local
    /// authorship for an already recorded delete.
    #[serde(default)]
    locally_created: bool,
    /// Immutable identity of the creation lineage.  Existing stored rows do
    /// not yet carry this on the wire; for a pre-foundation row its committed
    /// version is the only committed creation evidence available locally.
    lineage_root: String,
    #[serde(default)]
    lineage_attestation: Vec<u8>,
    author_node_id: Option<String>,
    author_database_incarnation: Option<String>,
    author_local_mutation_position: u64,
    delete_lsn: u64,
    delete_obligation: DurableDeleteObligation,
    accepted_hub_lsn: Option<u64>,
    bound_hub_node_id: Option<String>,
    /// Reserved for the authoritative purge slice.  Keeping the permanent
    /// frontier beside the ordinary obligation is the product's one shared
    /// lineage state model, not a second tombstone store.
    purge_frontier: Option<String>,
}

/// What reopen must restore from lifecycle records and purged-life history:
/// no LSN is issued at or below a permanent frontier, and no RowId is issued
/// again after the row that held it was physically erased.
#[derive(Default)]
struct DurableLineageRecoveryBounds {
    purge_frontier_max_lsn: Lsn,
    max_row_id: RowId,
}

#[derive(Debug, Clone)]
struct DurablePendingDelete {
    row: RowChange,
    local_row_id: Option<RowId>,
}

/// Private pull-filter result. Suppressed accepted-delete replays are consumed
/// by an ordinary serving cursor but are not applied work and never alter wire
/// accounting.
pub(crate) struct AcceptedDeleteSuppression {
    pub(crate) changes: ChangeSet,
    pub(crate) suppressed_live_replay: bool,
}

type SyncPushApplyLocks = Mutex<HashMap<String, std::sync::Weak<Mutex<()>>>>;
#[cfg(feature = "sync-orchestration")]
type SyncClientPushLocks = Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncPushDiagnosticIdentity {
    table: Option<String>,
    natural_key: NaturalKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SyncPushOutcomeKey {
    storage_key: String,
    incarnation: Incarnation,
    request_digest: [u8; 32],
    diagnostic_identities: Vec<SyncPushDiagnosticIdentity>,
}

#[derive(Clone)]
struct SyncPushOutcomeContext {
    key: SyncPushOutcomeKey,
    committed_lsn: Option<Lsn>,
    refused_rows: usize,
    conflicts: Vec<Conflict>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableSyncPushOutcome {
    incarnation: Incarnation,
    request_digest: [u8; 32],
    result: DurableSyncPushApplyResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableSyncPushApplyResult {
    applied_rows: usize,
    skipped_rows: usize,
    conflicts: Vec<DurableSyncPushConflict>,
    new_lsn: Lsn,
}

/// A durable refusal deliberately has no natural key. The authenticated,
/// byte-identical request is the sole source for both the member key and an
/// optional connected-unit cause when the result is replayed.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableSyncPushConflict {
    request_identity_ordinal: usize,
    resolution: ConflictPolicy,
    reason: Option<String>,
    table: Option<String>,
    mutation_kind: Option<String>,
    winning_author_node_id: Option<String>,
    hub_acceptance_position: Option<Lsn>,
    refusal_cause_identity_ordinal: Option<usize>,
}

struct PurgedLineageRefusal {
    changes: ChangeSet,
    refused_rows: Vec<RowChange>,
    conflicts: Vec<Conflict>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableRowLineageSidecar {
    author_node_id: String,
    author_database_incarnation: Incarnation,
    author_local_mutation_position: Lsn,
    table_generation: u64,
    lineage_root: String,
    #[serde(default)]
    lineage_attestation: Vec<u8>,
    /// The local row instance that inherited this immutable lineage.  A new
    /// SQL INSERT after a same-key deletion receives a different RowId, so it
    /// must reseed rather than accidentally revive the former root.
    #[serde(default)]
    local_row_id: Option<RowId>,
    /// Whether this database created the root and must therefore bind it to
    /// exactly one explicit fabric identity. Missing pre-release bytes default
    /// to false so decoding never invents local authorship.
    #[serde(default)]
    locally_created: bool,
}

#[derive(Debug, Clone)]
struct AuthenticatedFreshCreatorDelete {
    table: String,
    natural_key: NaturalKey,
    lsn: Lsn,
    row_id: RowId,
    values: HashMap<ColName, Value>,
}

/// Creation evidence is written in the INSERT transaction before any fabric
/// identity is involved. The commit preparer fills the exact commit LSN, so
/// later sync binding cannot depend on a mutable change log or live-row read.
/// Redb retains it across reopen; memory retains it for this database life.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableUnboundCreationLineage {
    table: String,
    natural_key: NaturalKey,
    table_generation: u64,
    local_row_id: RowId,
    creation_lsn: Option<Lsn>,
}

/// The lineage backend for one in-memory database life.  Its keys are the
/// exact config keys used by Redb, so authenticated creation, relay, and
/// delete algorithms have one representation regardless of the backend.
/// A new `Database::open_memory()` always creates an empty instance.
#[derive(Default, Clone)]
struct InMemoryLineageState {
    erasure_frontiers: MemoryErasureFrontiers,
    connected_membership: contextdb_relational::membership::MembershipImage,
    unbound_creations: HashMap<String, DurableUnboundCreationLineage>,
    row_sidecars: HashMap<String, DurableRowLineageSidecar>,
    records: HashMap<String, DurableLineageRecord>,
    /// Ordered so one key's purged lives are read as a key range.
    purged_history: BTreeMap<String, DurableLineageRecord>,
    push_outcomes: HashMap<String, DurableSyncPushOutcome>,
}

/// Decoded from finalized config writes before a memory transaction applies.
/// Publishing this delta cannot fail and only replaces the keys prepared by
/// that transaction; it never copies a detached working snapshot wholesale.
#[derive(Default)]
struct InMemoryLineageDelta {
    connected_membership: Option<contextdb_relational::membership::MembershipImage>,
    unbound_creations: HashMap<String, DurableUnboundCreationLineage>,
    row_sidecars: HashMap<String, DurableRowLineageSidecar>,
    records: HashMap<String, DurableLineageRecord>,
    pending: Option<resident_pending::PreparedPendingDelta>,
    purged_history: HashMap<String, DurableLineageRecord>,
    push_outcomes: HashMap<String, DurableSyncPushOutcome>,
}

/// Edge-client sync progress for one in-memory database life. It deliberately
/// has no disk or reopen contract: scoped handles share this state, while a
/// new `Database::open_memory()` starts with no tenant progress.
#[derive(Default)]
struct InMemorySyncProgressState {
    tenants: HashMap<String, InMemoryTenantSyncProgress>,
    schema_holdbacks: HashMap<String, DurableSchemaSyncHoldback>,
    /// Holds behind refused authored transactions. See
    /// `Database::received_policy_plan`.
    policy_holds: HashMap<String, DurablePolicyHold>,
    refused_schema_units: BTreeMap<String, crate::protocol::RefusedSchemaUnitOutcome>,
    refused_schema_lookup_checkpoints:
        HashMap<String, crate::protocol::RefusedSchemaLookupCheckpoint>,
    policy_held_data: BTreeMap<String, DurablePolicyHeldData>,
    policy_hold_releases: BTreeMap<String, DurablePolicyHoldRelease>,
}

#[derive(Default)]
struct InMemoryTenantSyncProgress {
    push_watermark: Lsn,
    pull_watermark: Lsn,
    pending_push_confirmation: Option<Lsn>,
    pending_push_result: Option<crate::protocol::WireApplyResult>,
    pull_cursor: Option<(Incarnation, Lsn)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum DurableDeleteObligation {
    Pending,
    Accepted,
    Purged,
}

type AcceptedSyncTombstone = (
    String,
    NaturalKey,
    Option<Lsn>,
    contextdb_relational::store::SyncSourceKind,
    RowId,
);
type TerminalRefusalMarkerKey = (String, String, String, Vec<u8>);
type TerminalRefusalContextKey = (String, String);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TerminalRefusalMarkerRecord {
    table: String,
    natural_key: NaturalKey,
    lsn: u64,
    row_id: u64,
    active: bool,
    #[serde(default)]
    generation: u64,
}

const SYNC_GRAPH_ARRIVAL_PREFIX: &str = "sync_graph_arrival.v1.";
type GraphIdentity = (NodeId, NodeId, EdgeType);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DurableGraphArrival {
    source: NodeId,
    target: NodeId,
    edge_type: EdgeType,
    local_lsn: Lsn,
    deleted: bool,
    pulled: bool,
}

/// The immutable record and its original payer have the same lifetime.
/// Map/root copies share this payload; each independently allocated map is
/// admitted by its preparation before copying its keys.
#[derive(Clone)]
struct FundedGraphArrival(Arc<GraphArrivalRoot>);

struct GraphArrivalRoot {
    record: DurableGraphArrival,
    _credit: contextdb_core::read_memory::ReadCredit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TerminalRefusalScanState {
    pub(crate) source: Option<Incarnation>,
    pub(crate) next_lsn: Lsn,
    active: bool,
    #[serde(default)]
    pub(crate) generation: u64,
}

/// One outbound request candidate. Ordinary changes keep their existing
/// source-LSN batching; a connected relational state carries its final
/// dependency closure in one request so the receiver can commit it once.
#[derive(Debug, Clone)]
pub(crate) struct OutboundSyncUnit {
    pub(crate) changes: ChangeSet,
    pub(crate) dependency_complete: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TerminalRefusalPullContext {
    pub(crate) tenant_id: TenantId,
    pub(crate) hub_node_id: String,
    pub(crate) generation: u64,
}
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, Condvar, Mutex, MutexGuard, RwLock};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type DynStore = Box<dyn WriteSetApplicator>;
type GatedGraphNeighbor = (NodeId, EdgeType, HashMap<String, Value>, NodeId, NodeId);
/// The visited ceiling a gated (context- or ACL-scoped) traversal keeps.
/// The unrestricted one is `contextdb_graph::mem::MAX_VISITED`; this is the
/// number the established gated walk refuses with.
const GATED_BFS_VISITED_CEILING: usize = 10_000;

const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 1024;
const MAX_STATEMENT_CACHE_ENTRIES: usize = 1024;
const SAME_PROCESS_REOPEN_RETRY: Duration = Duration::from_millis(500);
const CROSS_PROCESS_LOCK_SETTLE_RETRY: Duration = Duration::from_millis(250);

/// Causal interception of the common writable-open entrance. An installed
/// interceptor receives the complete options value before any startup
/// resource is acquired and supplies the call's result. Convenience openers
/// cannot satisfy this seam by recording a side counter and opening elsewhere.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub trait DatabaseOpenInterceptor: Send + Sync {
    fn intercept(&self, path: &Path, options: &DatabaseOpenOptions) -> Result<Database>;
}

/// Events emitted by the actual legacy `Database::execute` path. Planning is
/// observed at its production call site; pull events are reserved for the one
/// shared pull-kernel entrance and terminal drain.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadExecutionConvergenceEvent {
    PublicExecuteEntered,
    PlanReady,
    PullKernelEntered,
    PullKernelSourceTouch {
        source: ReadKernelSource,
        completed_items: u64,
    },
    PullKernelDrained,
    /// The eager executor is about to enter the shared relational scan source.
    EagerRowSourceTouch,
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub trait ReadExecutionConvergenceObserver: Send + Sync {
    fn observe(&self, event: ReadExecutionConvergenceEvent);
}

/// This adapter is accepted only by the bounded executor's production probe
/// entrance. It therefore cannot be satisfied by recording around an eager
/// result after that result has already been materialized.
#[cfg(any(test, feature = "test-seams"))]
struct ReadExecutionKernelProbe {
    observer: Arc<dyn ReadExecutionConvergenceObserver>,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static COMMON_OPTIONS_OPEN_INTERCEPTOR:
        std::cell::RefCell<Option<Arc<dyn DatabaseOpenInterceptor>>> =
        const { std::cell::RefCell::new(None) };
    static READ_EXECUTION_CONVERGENCE_OBSERVER:
        std::cell::RefCell<Option<Arc<dyn ReadExecutionConvergenceObserver>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(any(test, feature = "test-seams"))]
struct CommonOptionsOpenInterceptorGuard {
    previous: Option<Arc<dyn DatabaseOpenInterceptor>>,
}

#[cfg(any(test, feature = "test-seams"))]
struct ReadExecutionConvergenceObserverGuard {
    previous: Option<Arc<dyn ReadExecutionConvergenceObserver>>,
}
const DEFAULT_TRIGGER_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(60);
// redb may need a small metadata page on the next write, especially for a new
// file with the format metadata table. Keep the disk-limit error deterministic
// instead of starting a write that cannot commit cleanly.
const MIN_DISK_WRITE_HEADROOM_BYTES: u64 = 1024;
// Checkpoint export streams per-category batches so peak extra memory stays
// proportional to a batch, never a second whole-table copy.
const EXPORT_BATCH_SIZE: usize = 1024;
/// How many rows one run of a table's size answer inspects at a time, so the
/// answer never builds a whole-table list of keys and stored lengths.
const TABLE_SIZE_RUN_ROWS: usize = 1024;
/// What one row costs that run: its identity key, the stored length beside it,
/// and the map entry the two live in.
const TABLE_SIZE_RUN_ROW_BYTES: usize = 128;
/// Sentinel `sync_source_lsn` sidecar value meaning "this row's arrival is
/// exactly its own commit LSN on the accepting node" -- written for a
/// STAMPLESS incoming row (one whose sender never had an established
/// arrival) instead of a `current_lsn()` sample taken before this row's own
/// commit. A sampled value goes stale the instant a concurrent apply commits
/// first: two stampless rows sampled at the same pre-commit instant would
/// otherwise freeze the SAME arrival even though they land at different
/// committed positions, so equal-arrival arbitration (`sync_latest_wins_
/// incoming`) would let a peer holding the first value refuse the true later
/// one -- fleet divergence. The sentinel makes the sidecar exact by
/// construction: it always resolves (via `resolve_sync_source_lsn`) to
/// whatever LSN the row's OWN commit actually lands at, never a value
/// sampled before that commit was ordered. It never crosses the wire -- the
/// serve side (`Database::changes_since_with_arrivals`) resolves it to a
/// concrete LSN before a peer ever sees it.
const SYNC_SOURCE_LSN_OWN_COMMIT: Lsn = Lsn(u64::MAX);

mod schema_stage_registry;
mod stage_metadata_copy_charges;
use stage_metadata_copy_charges::MetadataCopyCharges;
mod cron;
pub(crate) mod event_bus;
pub(crate) mod gate;
pub(crate) mod trigger;
use cron::CronState;
use event_bus::EventBusState;
#[cfg(feature = "test-seams")]
mod write_publication_drive;
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use event_bus::{
    SinkDispatchDriveForTest, SinkDispatchIdentity, SinkDispatchPrefix, SinkDispatchReceipt,
    SinkDispatchRetryIdentity,
};
use trigger::{TriggerCallbackThreadGuard, TriggerContention, TriggerState};
#[cfg(feature = "test-seams")]
use write_publication_drive::WritePublicationControlForTest;
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use write_publication_drive::{WritePublicationDriveForTest, WritePublicationReceipt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexCandidate {
    pub name: String,
    pub rejected_reason: std::borrow::Cow<'static, str>,
}

/// Redacted shape of the authorized vector partitions selected by one query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchScopeShape {
    One,
    Few,
    All,
}

/// The vector execution route described to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchRoute {
    Exact,
    Indexed,
    FilteredIndexed,
}

/// Whether route fields describe a passive plan or work that completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchRouteEvidence {
    PassivePrediction,
    Executed,
}

/// Whether the relational rows admitted to vector work were already bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchResidual {
    None,
    Bounded,
    Unsupported,
}

/// Safe layer-presence facts. Generation identities and byte counts remain in
/// operator inspection; a query trace needs only the structure it used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchLayerPresence {
    Present,
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchTailState {
    Present,
    Empty,
}

/// Ordinary-build component receipt for memory ownership measurements.
/// Its consumer surface is supplied by measurement callers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from maintenance_and_limits.rs"
    )
)]
pub(crate) struct VectorMemoryOwnershipReceipt {
    /// Actual held source/loader metadata credits, deduplicated by Arc identity.
    pub source_metadata: usize,
    /// Held source metadata charged to the declared reader parent, not the writer.
    pub reader_source_metadata: usize,
    /// The actual retained last-query trace belongs to its reader operation.
    pub reader_last_query_trace: usize,
    /// Actual admitted controls of live declared-reader children.
    pub reader_child_controls: usize,
    /// Original declared-reader root block, measured from its retained invoice.
    pub reader_root_control: usize,
    /// Disjoint live child payload debits; source/trace figures are subsets.
    pub reader_child_payloads: usize,
    pub reader_acl_cache: usize,
    /// Actual reader parent total at the quiescent report boundary.
    pub reader_parent_total: usize,
    pub raw_vector_bodies: usize,
    pub raw_reverse_directory: usize,
    pub partition_vectors: usize,
    /// Fixed live column, index and partition controls, outside row history.
    pub vector_partition_controls: usize,
    pub base_graph: usize,
    pub change_graph: usize,
    pub mutable_tail: usize,
    pub typed_partition_keys: usize,
    pub retired_pinned_graph_bytes: usize,
    pub temporary_workspace: usize,
    pub retained_total: usize,
    pub non_vector_total: usize,
    pub accountant_total: usize,
    /// Shared process owner: count once by identity when aggregating writers.
    pub process_native_bytes: usize,
    pub process_native_owner_identity: usize,
    /// This writer's local ownership plus its shared process native envelope.
    pub combined_writer_envelope: usize,
}

/// Passive exact ownership receipt used by the memory-contract proof.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct VectorMemoryOwnershipReceiptForTest {
    /// Actual held source/loader metadata credits, deduplicated by Arc identity.
    pub source_metadata: usize,
    /// Held source metadata charged to the declared reader parent, not the writer.
    pub reader_source_metadata: usize,
    /// The actual retained last-query trace belongs to its reader operation.
    pub reader_last_query_trace: usize,
    /// Actual admitted controls of live declared-reader children.
    pub reader_child_controls: usize,
    /// Original declared-reader root block, measured from its retained invoice.
    pub reader_root_control: usize,
    /// Disjoint live child payload debits; source/trace figures are subsets.
    pub reader_child_payloads: usize,
    pub reader_acl_cache: usize,
    /// Actual reader parent total at the quiescent report boundary.
    pub reader_parent_total: usize,
    pub raw_vector_bodies: usize,
    pub raw_reverse_directory: usize,
    pub partition_vectors: usize,
    /// Fixed live column, index and partition controls, outside row history.
    pub vector_partition_controls: usize,
    pub base_graph: usize,
    pub change_graph: usize,
    pub mutable_tail: usize,
    pub typed_partition_keys: usize,
    pub retired_pinned_graph_bytes: usize,
    pub temporary_workspace: usize,
    pub retained_total: usize,
    pub non_vector_total: usize,
    pub accountant_total: usize,
    /// Shared process owner: count once by identity when aggregating writers.
    pub process_native_bytes: usize,
    pub process_native_owner_identity: usize,
    /// This writer's local ownership plus its shared process native envelope.
    pub combined_writer_envelope: usize,
}

#[cfg(any(test, feature = "test-seams"))]
struct WriterReceiptContentionObserver {
    identity: usize,
    observer: Arc<dyn Fn() + Send + Sync>,
    fired: bool,
}
#[cfg(any(test, feature = "test-seams"))]
static WRITER_RECEIPT_CONTENTION_OBSERVERS: parking_lot::Mutex<WriterReceiptContentionState> =
    parking_lot::Mutex::new(WriterReceiptContentionState {
        observers: Vec::new(),
        samples: Vec::new(),
    });
#[cfg(any(test, feature = "test-seams"))]
struct WriterReceiptContentionState {
    observers: Vec<WriterReceiptContentionObserver>,
    samples: Vec<usize>,
}
#[cfg(any(test, feature = "test-seams"))]
struct WriterReceiptSampleHeld(usize);

// The identity is installed only after acquiring the actual maintenance lock.
// Maintenance census emission can reuse its own guard, never a different DB's.
struct MaintenanceReceiptGuard<'a> {
    _guard: parking_lot::MutexGuard<'a, ()>,
    #[cfg(any(test, feature = "test-seams"))]
    identity: usize,
}
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static HELD_MAINTENANCE_RECEIPT_GUARDS: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
    static WRITER_COLLECTION_HOOK: std::cell::RefCell<Option<(usize, Box<dyn FnOnce()>)>> = const { std::cell::RefCell::new(None) };
}

/// Passive writer-owner reconciliation. Physical inventories and historical
/// row prices are observations, not additional accountant charges.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct WriterMemoryOwnershipReceiptForTest {
    /// Captured at the same boundary; the JSON emitter keeps its existing key.
    #[serde(skip)]
    pub schema_history_retained_bytes: usize,
    /// Published root observed under the same commit and maintenance exclusion.
    #[serde(skip)]
    pub published_lsn: Lsn,

    pub writer_registration: usize,
    pub engine_accountant_backing: usize,
    pub writer_adapter_controls: usize,
    pub vector: VectorMemoryOwnershipReceiptForTest,
    pub current_row_versions: usize,
    pub historical_row_versions: usize,
    pub current_row_values: usize,
    pub current_row_side_maps: usize,
    pub current_row_index_postings: usize,
    pub current_row_f32_projections: usize,
    pub current_rows_by_table: std::collections::BTreeMap<String, CurrentRowOwnershipForTest>,
    pub historical_row_priced_bytes: usize,
    pub staged_row_versions: usize,
    pub staged_row_admissions: usize,
    pub staged_vector_admissions: usize,
    /// Actual new capability/index controls and native immutable payload copies.
    /// Included in staged_index_postings, rather than added a second time.
    pub staged_vector_owner_controls: usize,
    pub staged_index_postings: usize,
    pub table_metadata: usize,
    pub graph_entries: usize,
    /// Row insert/delete, vector insert/delete, edge insert/delete, in order.
    pub change_log_entries: [usize; 6],
    pub change_log_charges: [usize; 6],
    pub commit_index_entries: usize,
    pub commit_index_charge: usize,
    pub persistence_cache: usize,
    /// The durable custody store's fixed companion memo, admitted to the writer.
    pub custody_companion_memo: usize,
    /// The durable custody store's tenant policy mirror, admitted to the writer.
    pub custody_tenant_mirror: usize,
    pub current_membership_charge: usize,
    pub captured_membership_charge: usize,
    pub current_membership_by_table: std::collections::BTreeMap<String, usize>,
    #[serde(serialize_with = "serialize_relational_inventory_for_test")]
    pub relational_inventory: contextdb_relational::store::RetainedStructureBytes,
    pub ddl_log_entries: usize,
    pub ddl_log_estimated_bytes: usize,
    pub active_transactions: usize,
    pub pending_commit_metadata: usize,
    pub pending_lineage_generation_charge: usize,
    pub pending_schema_stages: usize,
    pub named_owner_total: usize,
    pub reconciliation_gap: i64,
    pub writer_schema_charge: usize,
    pub published_root_charge: usize,
    pub captured_root_charge: usize,
    pub commit_source_reference_charge: usize,
    pub storage_maintenance_control_charge: usize,
    /// Writer charge for the resident windows of durable sink queues.
    pub sink_window_charge: usize,
    /// Process open-residue capacity; observed here, outside named writer liabilities.
    pub snapshot_pin_capacity_charge: usize,
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct CurrentRowOwnershipForTest {
    pub versions: usize,
    pub values: usize,
    pub side_maps: usize,
    pub index_postings: usize,
    pub f32_projections: usize,
}

/// The query-vector source is disclosed structurally, never by value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorQuerySourceDisclosure {
    Vector,
    RedactedRowKey,
    Unknown,
}

/// Serving graph topology/build revision and current query search breadth for
/// one selected partition. Its key is an ordinal redaction. Restricted callers
/// receive no per-partition entries: adaptive values could reveal hidden sizes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPartitionHnswDisclosure {
    pub partition: String,
    /// Work selected for this partition after aggregate AUTO admission.
    /// `None` is reserved for a partition scope that has not been bound yet.
    pub route: Option<VectorSearchRoute>,
    /// Why this partition selected `route`; absent only while scope is unbound.
    pub reason: Option<String>,
    pub hnsw_m: usize,
    pub hnsw_ef_construction: usize,
    pub hnsw_ef_search: usize,
    pub ef_search_source: String,
    pub policy_revision: u64,
}

/// Per-result vector route facts. Partition values, row ids, and bound values
/// are redacted. The aggregate count covers authorized identities only;
/// restricted callers receive no per-partition adaptive HNSW settings.
///
/// The planner resolves partition scope only after parameters are bound. A
/// passive explain of a statement whose partition key is still an unbound
/// placeholder therefore has no scope to count over: `aggregate_allowed_vectors`
/// is `None` (rendered `null`), an AUTO `resolved_mode` stays `Auto`, `route`
/// is `None`, and `fallback` names the unresolved-until-bind reason. `0` is
/// never used for this: an empty authorized scope is a complete empty answer,
/// not an unresolved one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchDisclosure {
    pub route_evidence: VectorSearchRouteEvidence,
    pub requested_mode: contextdb_core::VectorSearchMode,
    pub resolved_mode: contextdb_core::VectorSearchMode,
    pub aggregate_allowed_vectors: Option<usize>,
    pub effective_auto_index_at: usize,
    pub auto_index_at_source: String,
    /// The column's effective `FLAT_BELOW` and where it comes from: a
    /// partition with fewer captured live rows is served flat.
    pub effective_flat_below: usize,
    pub flat_below_source: String,
    /// The column's effective HNSW `SEARCH_RADIUS`, in millionths, and where
    /// it comes from.
    pub effective_search_radius_millionths: usize,
    pub search_radius_source: String,
    /// The column's effective `AUTO_EXPANSION_CEILING`, in percent, and where
    /// it comes from.
    pub effective_auto_expansion_ceiling: usize,
    pub auto_expansion_ceiling_source: String,
    pub partition_key_columns: Vec<String>,
    pub scope: VectorSearchScopeShape,
    pub route: Option<VectorSearchRoute>,
    pub base: VectorSearchLayerPresence,
    pub change: VectorSearchLayerPresence,
    pub tail: VectorSearchTailState,
    pub residual: VectorSearchResidual,
    pub fallback: Option<String>,
    pub refusal: Option<String>,
    pub recovery: Option<String>,
    pub query_source: VectorQuerySourceDisclosure,
    /// One redacted entry per selected authorized partition. Values are
    /// resolved from that partition's snapshot-visible size; no key value is
    /// representable here.
    pub partition_hnsw: Vec<VectorPartitionHnswDisclosure>,
    pub global_candidate_budget: Option<usize>,
    pub admission_evaluations: Option<usize>,
    pub expansion_evaluations: Option<usize>,
    /// Stored vectors the search read to score rows -- the walk's for rows
    /// updated since their graph was built, and final candidate scoring's --
    /// paid from the rescoring allowance.
    pub rescore_evaluations: Option<usize>,
    pub layers_admitted: Option<usize>,
    pub budget_terminal: Option<String>,
}

/// Passive `.explain` result. It is planning metadata, never an executed
/// query receipt; `.trace on` remains the surface for an actual route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainOutput {
    pub physical_plan: String,
    pub index_used: Option<String>,
    pub predicates_pushed: Vec<String>,
    pub indexes_considered: Vec<IndexCandidate>,
    pub sort_elided: bool,
    pub vector_search: Option<VectorSearchDisclosure>,
}

/// A materialized diagnostic trace retains its executing reader's allocation owner.
#[doc(hidden)]
#[derive(Debug)]
pub struct RetainedVectorSearchTrace {
    trace: VectorSearchDebugTrace,
    owner: contextdb_vector::VectorCopyAdmission,
    executing_owner: contextdb_vector::MemoryBudgetOwner,
}

/// Caller-owned observations exposed by the vector trace carrier.
#[doc(hidden)]
#[derive(Debug)]
pub struct VectorSearchTraceObservation {
    pub index: VectorIndexRef,
    pub used_hnsw: bool,
    pub hnsw_len: Option<usize>,
    pub hnsw_candidate_count: usize,
    pub hnsw_candidate_row_ids: Vec<RowId>,
    pub supplemented_row_count: usize,
    pub hnsw_ef_search: Option<usize>,
    pub selected_generations: Vec<contextdb_vector::store::VectorGraphGeneration>,
    pub final_row_ids: Vec<RowId>,
    pub partition_routes: Vec<contextdb_vector::mem::VectorPartitionSearchTrace>,
    pub fallback_reason: Option<&'static str>,
    pub budget_receipt: Option<contextdb_vector::mem::VectorSearchBudgetReceipt>,
    _memory: contextdb_core::read_memory::ReadCredit,
}

/// The actual route serving one window partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupRoute {
    Index,
    Sort,
}

/// Passive counters for a partition; the key uses the planner's redacted rendering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupDisclosure {
    #[doc(hidden)]
    pub ordinal: u64,
    pub partition_key: String,
    pub examined_entries: u64,
    pub returned_rows: u64,
    pub route: GroupRoute,
}

#[derive(Debug, Clone, Default)]
pub struct QueryTrace {
    pub groups: Box<[GroupDisclosure]>,
    pub physical_plan: &'static str,
    pub index_used: Option<String>,
    pub predicates_pushed: smallvec::SmallVec<[std::borrow::Cow<'static, str>; 4]>,
    pub indexes_considered: smallvec::SmallVec<[IndexCandidate; 4]>,
    pub sort_elided: bool,
    pub query_vector_source: Option<contextdb_core::types::VectorIndexRef>,
    /// Executed vector metadata is absent for ordinary queries. Its owning
    /// allocation is admitted by the executing reader before construction.
    pub vector_search: Option<Box<VectorSearchDisclosure>>,
    pub rows_examined: u64,
    /// Which route a `COUNT(*)` over an exact index prefix actually took, and
    /// — when the shape was eligible but this table declares no maintained
    /// count over those columns — the `ALTER TABLE` that would make it O(1).
    /// `None` for every statement that is not such a count.
    /// Boxed because it is `None` for all but an exact index-prefix
    /// `COUNT(*)`, while `QueryTrace` itself is carried BY VALUE inside
    /// the private execution payload through every recursive layer of
    /// `execute_plan_once`; the public `QueryResult` shares its frozen payload.
    /// That frame is ~252 KiB per plan layer, so a 2 MiB thread holds only a
    /// handful of layers and each inline byte here is paid many times over by
    /// a deep statement. See the four-CTE stack regression,
    /// `tests/sql/four_cte_graph_composition_runs_on_a_two_mebibyte_thread`.
    pub maintained_count: Option<Box<crate::maintained_counts::MaintainedCountDisclosure>>,
}

#[derive(Debug, Clone)]
pub struct CascadeReport {
    pub dropped_indexes: Vec<String>,
}

#[path = "database/query_result.rs"]
mod query_result;
pub use query_result::QueryResult;
pub(crate) use query_result::{OwnedQueryResult, QueryResultData};

/// Report returned by [`Database::export_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExportReport {
    pub snapshot_lsn: Lsn,
    pub rows: u64,
    pub edges: u64,
    pub vectors: u64,
    pub bytes_written: u64,
}

/// A retained row and its durable deletion lineage, read from a snapshot
/// artifact or database file. This is an inspection DTO: it grants no
/// mutation or raw-storage capability.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct KeyInspection {
    pub total_retained_versions: u64,
    pub versions_truncated: bool,
    pub retained_versions: Vec<RetainedVersionInspection>,
    pub lineage: Option<LineageInspection>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RetainedVersionInspection {
    pub row_id: u64,
    pub created_tx: u64,
    pub deleted_tx: Option<u64>,
    pub lsn: u64,
    pub values: BTreeMap<String, RetainedValueInspection>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RetainedValueInspection {
    pub kind: String,
    pub value: Option<Value>,
    pub omitted: bool,
    pub omission_reason: Option<String>,
    pub source_units: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LineageInspection {
    pub table_generation: u64,
    pub lineage_fingerprint: String,
    pub delete_obligation: DeleteObligationInspection,
    pub accepted_hub_lsn: Option<u64>,
    pub purge_frontier_lsn: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteObligationInspection {
    Pending,
    Accepted,
    Purged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlobInspection {
    pub hash: String,
    pub active_generation: Option<u64>,
    pub last_retired_generation: u64,
    pub last_purge_lsn: u64,
    pub manifest: Option<BlobManifestInspection>,
    pub partial: Option<BlobPartialInspection>,
    pub tag_roles: BlobTagRolesInspection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlobManifestInspection {
    pub total_size: u64,
    pub outboard_size: u64,
    pub validated_size: u64,
    pub payload_chunk_count: u64,
    pub outboard_chunk_count: u64,
    pub state: BlobManifestStateInspection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BlobManifestStateInspection {
    Partial,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlobPartialInspection {
    pub contiguous_payload_prefix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlobTagRolesInspection {
    pub servable: bool,
    pub fetch_protection: bool,
}

/// Opaque, deterministic census of the durable state a received sync apply
/// may change. It is intentionally comparison-only: no row values, config
/// payloads, identities, or storage keys leave the inspector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SyncApplyStateInspection {
    pub digest: String,
    pub current_lsn: u64,
    pub tables: u64,
    pub retained_row_versions: u64,
    pub index_postings: u64,
    pub edges: u64,
    pub vectors: u64,
    pub ddl_log_entries: u64,
    pub change_log_entries: u64,
    pub commit_index_entries: u64,
    pub persisted_config_entries: u64,
    pub trigger_audit_entries: u64,
    pub sink_audit_entries: u64,
    pub sink_queue_entries: u64,
}

/// Read-only operator inspection of a snapshot artifact or a database file
/// path. Opening a Redb file performs housekeeping writes, so this type
/// first copies the supplied path into a private temporary directory and
/// opens only that disposable copy -- a live file under a concurrent
/// writer is copied as-is, so the inspection reflects whatever
/// point-in-time state the copy captured, not necessarily the writer's
/// latest state. It deliberately exposes no [`Database`] handle.
pub struct SnapshotInspector {
    database: Database,
    _copy: tempfile::TempDir,
}

/// Read-only deletion/purge state exposed only to ContextDB's own integration
/// tests. `None` means the engine has no durable representation for that
/// facet. The production engine neither consults nor mutates this snapshot.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DurableDeletionStateSnapshot {
    pub table_generation: Option<u64>,
    pub lineage_root: Option<String>,
    pub delete_obligation: Option<String>,
    pub accepted_delete_marker: Option<String>,
    pub purge_frontier: Option<String>,
}

/// Immutable identity selected before an authoritative purge can prepare any
/// owned copies.  The pinned local RowId prevents a later same-key life from
/// being mistaken for the selected creator root.
#[derive(Debug, Clone, PartialEq)]
struct AuthoritativePurgeSelection {
    table: String,
    table_generation: u64,
    natural_key: NaturalKey,
    lineage_root: String,
    local_row_id: RowId,
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritativePurgeLiveRowSidecarSnapshot {
    pub author_node_id: String,
    pub author_database_incarnation: Incarnation,
    pub author_local_mutation_position: Lsn,
    pub table_generation: u64,
    pub lineage_root: String,
    pub lineage_attestation: Vec<u8>,
    pub local_row_id: Option<RowId>,
    pub locally_created: bool,
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoritativePurgeRootClassification {
    Purged { permanent_frontier: Lsn },
    NotPurged,
}

const AUTHORITATIVE_PURGE_DELIVERY_PREFIX: &str = "authoritative_purge_delivery.v1.";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct AuthoritativePurgeDeliveryItem {
    pub(crate) frontier: Lsn,
    pub(crate) ordinal: u32,
    pub(crate) table: String,
    pub(crate) table_generation: u64,
    pub(crate) natural_key: NaturalKey,
    pub(crate) purged_lineage_roots: Vec<String>,
    pub(crate) node_local_predicate: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
struct AuthoritativePurgeRowVersionKey {
    table: String,
    table_generation: u64,
    key: Vec<(String, Value)>,
    lineage_root: String,
    row_id: RowId,
    created_tx: TxId,
    lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq)]
struct AuthoritativePurgeSinkEntryKey {
    sink: String,
    queue_id: u64,
    row_id: RowId,
    /// BLAKE3 digest of the durable value the selection saw; the atomic
    /// removal checks the stored value still has it.
    durable_digest: Option<[u8; 32]>,
}

#[derive(Debug, Clone)]
struct AuthoritativePurgeLineageWitness {
    sidecar: Option<DurableRowLineageSidecar>,
    creation: Option<DurableUnboundCreationLineage>,
    lifecycle: Option<DurableLineageRecord>,
}

/// Immutable purge-lineage identity.  The frontier is intentionally absent
/// until the later commit kernel has reserved it; applying that one value
/// never re-reads mutable sidecars or lifecycle state.
#[derive(Debug, Clone)]
struct AuthoritativePurgeLifecycleTemplate {
    table: String,
    natural_key: NaturalKey,
    table_generation: u64,
    local_row_id: RowId,
    locally_created: bool,
    lineage_root: String,
    lineage_attestation: Vec<u8>,
    author_node_id: Option<String>,
    author_database_incarnation: Option<String>,
    author_local_mutation_position: u64,
}

#[derive(Debug, Clone)]
struct IncomingPurgeFrontierAdoption {
    table: String,
    natural_key: NaturalKey,
    table_generation: u64,
    lineage_root: String,
    existing: Option<DurableLineageRecord>,
}

#[derive(Default)]
struct IncomingAuthoritativePurgePlan {
    selections: BTreeMap<Lsn, Vec<AuthoritativePurgeSelection>>,
    frontier_adoptions: BTreeMap<Lsn, Vec<IncomingPurgeFrontierAdoption>>,
    instruction_receipts: BTreeMap<Lsn, Vec<String>>,
}

#[derive(Debug, Clone)]
struct AuthoritativePurgeLineageConfigOwners {
    row_sidecar_key: String,
    creation_lineage_key: String,
    accepted_author_key: String,
    accepted_author_memory_key: (String, Vec<u8>),
    // Same-key mutable metadata belongs to the current life only.
    erase_current_key_metadata: bool,
    lifecycle_record_key: String,
    /// Whether the key's lifecycle record describes the selected life rather
    /// than an earlier deleted or purged life at the same key.
    lifecycle_record_is_selected_life: bool,
    lifecycle_template: AuthoritativePurgeLifecycleTemplate,
}

struct AuthoritativePurgePublicationReplacements {
    relational: PreparedRelationalPublication,
    change_log: Vec<ChangeLogEntry>,
    change_log_table_index: ChangeLogTableIndex,
    change_log_lsn_refcounts: ChangeLogLsnRefcounts,
    graph: PreparedGraphPublication,
    vector: PreparedVectorPublication,
    event_bus: event_bus::PreparedAuthoritativePurgeEventBusPublication,
    accepted_author_memory_keys_removed: Vec<(String, Vec<u8>)>,
    graph_arrival_memory_removed: Vec<(GraphIdentity, DurableGraphArrival)>,
    memory_swap: PreparedMemorySwapAccounting,
}

struct AuthoritativePurgeKernelOutcome {
    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    frontier: Lsn,
    survivor_report: OwnedQueryResult,
}

/// A read-only inventory of every copy that a later private kernel must
/// remove for one immutable root, plus the projections that must remain.
struct AuthoritativePurgePreparedSet {
    row_version_keys: Vec<AuthoritativePurgeRowVersionKey>,
    change_log_entries: Vec<ChangeLogEntry>,
    vector_entries: Vec<VectorEntry>,
    vector_partition_identities: Vec<VectorPartitionVersionIdentity>,
    vector_generation_candidates: Vec<VectorPartitionGenerationCandidate>,
    canonical_graph_entries: Vec<AdjEntry>,
    row_graph_entries: Vec<contextdb_tx::OwnedAdjEntry>,
    row_graph_credit: contextdb_core::read_memory::ReadCredit,
    disk_source_provenance: Vec<(String, RowId, Lsn, u8)>,
    lineage_config_owners: Vec<AuthoritativePurgeLineageConfigOwners>,
    durable_sink_entries: Vec<AuthoritativePurgeSinkEntryKey>,
    /// Writer charge for the selected sink entries and their durable
    /// projection, held until the purge is done with them.
    _sink_projection_charge: Option<crate::read_image_memory::ImageMemoryHandle>,
    graph_arrival_config_keys: Vec<String>,
    publication_replacements: AuthoritativePurgePublicationReplacements,
    survivor_report: OwnedQueryResult,
}

#[derive(Debug, Clone)]
pub struct SemanticQuery {
    pub table: String,
    pub vector_column: String,
    pub query: Vec<f32>,
    pub limit: usize,
    /// Per-query route override. `None` uses the vector column's declared
    /// `SEARCH_MODE`; `Some` overrides that declaration for this query only.
    /// The mode covers the entire authorized scope before global ranking/limit.
    /// It does not change access rules or resource limits. `Indexed` refuses an
    /// unavailable route without building it; initial construction and repair
    /// advance through [`Database::run_maintenance_cycle`] or engine-owned work.
    pub search_mode: Option<contextdb_core::VectorSearchMode>,
    pub sort_key: Option<String>,
    pub min_similarity: Option<f32>,
    pub where_clause: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub row_id: RowId,
    pub values: HashMap<String, Value>,
    pub vector_score: f32,
    /// Always populated. Equals the formula's computed value when the search
    /// uses a rank policy via `sort_key`, and equals `vector_score` (raw
    /// cosine) in all other cases. Callers never unwrap this field.
    pub rank: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronAuditEntry {
    pub schedule_name: String,
    pub kind: CronAuditKind,
    pub at_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CronAuditKind {
    Fired,
    MissedSkipped,
    MissedCaughtUp { ticks: u32 },
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDeclaration {
    pub name: String,
    pub table: String,
    pub on_events: Vec<TriggerEvent>,
    // Persisted separately from the declaration payload; decoding that
    // payload alone defaults this sidecar-controlled flag to `false`.
    #[serde(skip)]
    pub including_sync: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriggerContext {
    pub trigger_name: String,
    pub table: String,
    pub event: TriggerEvent,
    pub tx: TxId,
    pub depth: u32,
    pub row_values: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerAuditEntry {
    pub trigger_name: String,
    pub firing_tx: TxId,
    pub firing_lsn: Lsn,
    pub depth: u32,
    pub cascade_row_count: u32,
    pub status: TriggerAuditStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerAuditStatus {
    Fired,
    RolledBack { reason: String },
    DepthExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerAuditStatusFilter {
    Fired,
    RolledBack,
    DepthExceeded,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TriggerAuditFilter {
    pub trigger_name: Option<String>,
    pub status: Option<TriggerAuditStatusFilter>,
}

pub struct CronPauseGuard {
    cron: Arc<CronState>,
}

#[derive(Debug)]
pub struct ApplyPhasePauseGuard {
    inner: Arc<ApplyPhasePause>,
    generation: u64,
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPartitionJournalFileForTest {
    pub truncated_through_lsn: Lsn,
    pub physical_record_lsns: Vec<Lsn>,
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPartitionGenerationRetentionForTest {
    pub durable_superseded_generations: usize,
    pub resident_superseded_generations: usize,
    pub durable_superseded_bytes: u64,
    pub resident_superseded_bytes: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SinkEvent {
    /// Stable durable queue identity for receiver deduplication across retries.
    pub action_id: u64,
    pub event_type: String,
    pub table: String,
    pub row_values: HashMap<String, Value>,
    pub severity: String,
    pub at_lsn: Lsn,
}

#[cfg(test)]
mod sink_event_encoding_tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Transient(String),
    /// The action may have succeeded, but its acknowledgement was lost.
    /// Delivery remains pending with the same action identity.
    OutcomeUncertain(String),
    Permanent(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkMetrics {
    pub delivered: u64,
    pub queued: u64,
    pub retried: u64,
    /// Delivery attempts whose external outcome could not be confirmed.
    pub uncertain: u64,
    pub permanent_failures: u64,
    /// Count of queue entries the sink dispatcher has pulled to the front and
    /// evaluated the per-registration access gate for — whether the entry was
    /// then delivered, permanently failed, or gate-denied and rotated back.
    ///
    /// This is ALWAYS-ON (not a test-only build): for admitted batches the
    /// dispatcher folds the increment into the SAME single post-batch metrics
    /// write that already records `delivered` / `permanent_failures` (zero
    /// additional lock acquisitions on the delivery path); only a denied-entry
    /// rotation pays its own acquisition (a scoped consumer that cannot see
    /// some queued events, throttled by the dispatcher's own 10 ms rotation
    /// wait). The cost is negligible
    /// and the field lets a caller prove the dispatcher has completed a full
    /// sweep of the queue — i.e. every gate-denied entry was examined and
    /// rejected, not merely not-yet-processed. Tests read it to make
    /// "denied events are never delivered" a state assertion instead of a sleep.
    pub examined: u64,
    /// Times a window of this sink's durable queue could not be held within
    /// the writer memory limit (at open or when the dispatcher read the next
    /// window). Those events stay queued in the store.
    pub window_refusals: u64,
    /// The latest window read was refused and no window has been held since:
    /// delivery is stalled until the writer budget admits the next window.
    pub window_refused: bool,
}

/// One declared `CREATE EVENT TYPE`, for `.events status`-style operator
/// introspection.
#[derive(Debug, Clone)]
pub struct EventTypeStatus {
    pub name: String,
    pub trigger: String,
    pub table: String,
}

/// One declared `CREATE SINK`, with its current delivery metrics and whether
/// a delivery callback is registered right now.
#[derive(Debug, Clone)]
pub struct SinkStatus {
    pub name: String,
    pub sink_type: String,
    pub callback_registered: bool,
    pub metrics: SinkMetrics,
}

/// One declared `CREATE ROUTE`.
#[derive(Debug, Clone)]
pub struct RouteStatus {
    pub name: String,
    pub event_type: String,
    pub sink: String,
}

/// Point-in-time snapshot of every declared event type / sink / route.
#[derive(Debug, Clone)]
pub struct EventBusStatus {
    pub event_types: Vec<EventTypeStatus>,
    pub sinks: Vec<SinkStatus>,
    pub routes: Vec<RouteStatus>,
}

/// Live publication state sampled without entering the database operation gate.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug)]
pub struct PublishedStateSnapshotForTest {
    pub table_names: Vec<String>,
    pub ddl_changes: Vec<DdlChange>,
    pub event_type_names: Vec<String>,
    pub sink_names: Vec<String>,
    pub route_names: Vec<String>,
    pub trigger_names: Vec<String>,
    pub applied_push_watermarks: Vec<(String, Lsn)>,
}

/// A point-in-time view of one declared `CREATE SCHEDULE`, for CLI/operator
/// introspection (`.events status`-style surfaces). Not a test seam — this is
/// a real public accessor any embedding consumer can use.
#[derive(Debug, Clone)]
pub struct CronScheduleStatus {
    pub name: String,
    pub every_text: String,
    pub callback: String,
    pub callback_registered: bool,
    pub next_fire_at_ms: u64,
    pub last_fire_at_ms: Option<u64>,
    pub fire_count: u64,
}

#[derive(Debug, Clone)]
struct CachedStatement {
    stmt: Statement,
    plan: PhysicalPlan,
}

/// Identity of one open database handle, handed out at construction and never
/// reused.
///
/// The thread-local registers below ask "is this work THIS database's?" -- is
/// the rows-examined scope on the stack this statement's, is the callback that
/// is running this handle's, is this handle's operation already open. An
/// address cannot answer that: a dropped handle frees its address for the next
/// one, so two handles that never coexisted can carry the same number and one
/// is then mistaken for the other. What that costs is not theoretical for the
/// first of those registers -- a colliding key mixes one database's examined
/// rows into another's trace, and the number an operator reads to judge a
/// query is then partly somebody else's.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct DatabaseId(u64);

static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Default)]
struct MaintenancePollIntervalUpdateProbe {
    entries: Mutex<u64>,
    changed: Condvar,
}

#[derive(Debug)]
struct ActiveQueryRowsExaminedScope {
    id: u64,
    database_key: DatabaseId,
    rows_examined: u64,
}

#[derive(Debug, Default)]
struct QueryRowsExaminedScopes {
    next_id: u64,
    active: Vec<ActiveQueryRowsExaminedScope>,
}

thread_local! {
    static QUERY_ROWS_EXAMINED_SCOPES: std::cell::RefCell<QueryRowsExaminedScopes> =
        std::cell::RefCell::new(QueryRowsExaminedScopes::default());
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static PASSIVE_DISCLOSURE_HIDDEN_WORK: std::cell::Cell<(u64, u64)> =
        const { std::cell::Cell::new((0, 0)) };
}

struct QueryRowsExaminedScope<'a> {
    database: &'a Database,
    database_key: DatabaseId,
    id: u64,
    finished: bool,
}

thread_local! {
    static SNAPSHOT_OVERRIDE: std::cell::RefCell<Option<SnapshotId>> =
        const { std::cell::RefCell::new(None) };
    /// Statement registrations on this thread, in nesting order. A terminal
    /// autocommit may exclude only its own top registration from the removal
    /// sample: that statement has finished every old-snapshot read before it
    /// publishes, while an outer callback statement may still continue.
    static STATEMENT_SNAPSHOT_REGISTRATION_STACK: std::cell::RefCell<Vec<u64>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// The one statement registration whose terminal commit is currently
    /// publishing. This is set only around the three SQL commit doors below;
    /// direct API commits and maintenance never infer that an unrelated
    /// in-flight statement has stopped reading.
    static COMMITTING_STATEMENT_SNAPSHOT_REGISTRATION: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
    static CRON_LSN_OVERRIDE: std::cell::RefCell<Option<Lsn>> =
        const { std::cell::RefCell::new(None) };
    static CRON_CALLBACK_TX: std::cell::Cell<Option<TxId>> =
        const { std::cell::Cell::new(None) };
    static CRON_CALLBACK_DB: std::cell::Cell<Option<DatabaseId>> =
        const { std::cell::Cell::new(None) };
    static CRON_CALLBACK_VECTOR_SCHEMA_GATE: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static CRON_CALLBACK_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static TRIGGER_CALLBACK_TX: std::cell::Cell<Option<TxId>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_CALLBACK_DB: std::cell::Cell<Option<DatabaseId>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_CALLBACK_VECTOR_SCHEMA_GATE: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_CALLBACK_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static TRIGGER_CALLBACK_NAME: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
    static TRIGGER_CALLBACK_WALLCLOCK: std::cell::Cell<Option<Wallclock>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_INSERT_STATE_MACHINE_CACHE: std::cell::RefCell<HashMap<DatabaseId, HashMap<String, bool>>> =
        std::cell::RefCell::new(HashMap::new());
    static USER_COMMIT_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static USER_COMMIT_TRIGGER_REENTRY: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH: std::cell::Cell<u32> =
        const { std::cell::Cell::new(0) };
    /// Exact internal received-schema executors that must retain their
    /// established mixed DDL+row transaction path. Unlike the trigger-ready
    /// depth above, this authority is bound to one database handle and TxId;
    /// synchronous plugin reentry cannot inherit it for caller work.
    static SYNC_APPLY_LOCAL_SCHEMA_BYPASS_STACK: std::cell::RefCell<Vec<(DatabaseId, TxId)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static SQL_WRITE_CONTROL_BYPASS_STACK: std::cell::RefCell<Vec<(DatabaseId, TxId)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static DB_OPERATION_STACK: std::cell::RefCell<Vec<DatabaseId>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// One outer SQL statement holds the shared schema-publication gate for
    /// its complete planning and execution lifetime. The key is the shared
    /// gate allocation (not a `Database` handle address), so a trigger or
    /// scoped handle nested on this thread does not self-deadlock.
    static SCHEMA_PUBLICATION_STACK: std::cell::RefCell<Vec<usize>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static VECTOR_SCHEMA_READ_STACK: std::cell::RefCell<Vec<(usize, VectorIndexRef)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// The vector indexes this thread holds through retained state — a bounded
    /// cursor's schema claim, which outlives the statement that opened it.
    ///
    /// A schema change reads its own thread's list: a session that would have
    /// to wait for a claim it holds itself is answered instead of parked. The
    /// list is shared by handle rather than borrowed per thread because such a
    /// claim may be released on a thread that did not open it; the claim is
    /// removed from the opening thread's list wherever the release happens.
    static VECTOR_SCHEMA_DETACHED_HOLDINGS: Arc<Mutex<Vec<(usize, VectorIndexRef)>>> =
        Arc::new(Mutex::new(Vec::new()));
}

struct SyncApplyTriggerGateGuard;

struct SyncApplyLocalSchemaBypassGuard {
    db_id: DatabaseId,
    tx: TxId,
}

struct SqlWriteControlBypassGuard {
    db_id: DatabaseId,
    tx: TxId,
}

#[derive(Default)]
struct VectorSchemaGates {
    gates: Mutex<HashMap<VectorIndexRef, Arc<RwLock<()>>>>,
    epochs: Mutex<HashMap<VectorIndexRef, u64>>,
}

pub(crate) struct VectorSchemaReadGuard {
    db_id: usize,
    refs: Vec<VectorIndexRef>,
    /// A statement-scoped guard is recorded on the thread that took it, so a
    /// nested read on that same thread can reuse the claim instead of taking
    /// the gate twice. A detached guard belongs to owned state — a retained
    /// cursor — that outlives the statement that opened it and may be
    /// released on a different thread than the one that opened it, so it is
    /// never recorded on any thread's stack and never reuses another guard's
    /// claim.
    detached: bool,
    /// The opening thread's detached-claim list, held by a detached guard so
    /// its entries come off wherever the guard is released.
    #[allow(clippy::type_complexity)]
    detached_holdings: Option<Arc<Mutex<Vec<(usize, VectorIndexRef)>>>>,
    _guards: Vec<ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
}

pub(crate) struct VectorSchemaWriteGuard {
    _guards: Vec<ArcRwLockWriteGuard<parking_lot::RawRwLock, ()>>,
}

struct UserCommitCallbackGuard {
    prior_active: bool,
    prior_reentry: bool,
}

/// Embedded contextdb database handle.
///
/// # Store ownership and concurrency
///
/// Writing a database file belongs to exactly one owner at a time. A second
/// writable open of the same path — whether from another thread in this process
/// or from a separate process — returns [`Error::DatabaseLocked`].
/// Single-writer ownership is a deliberate guarantee of the substrate, not a
/// missing feature, and it is enforced at two layers (an in-process open
/// registry and an on-disk PID/OS file lock).
///
/// Reading is a separate door that does not take the write lock:
/// [`crate::ReadSession`] opens a read-only session over the same store and
/// resolves one of two routes for its lifetime — the live owner's local channel
/// when a process owns the store, or the committed file itself when none does.
/// So a second process reads by opening a read session, which reaches the
/// running owner when there is one and reads the committed snapshot when there
/// is not.
///
/// Embed the writing side the way you embed any single-writer store (SQLite,
/// LMDB, redb): one process opens the handle and keeps it for its lifetime, and
/// every write goes through that one owner, which also serves the reads of the
/// application it lives in. What you never do is keep a parallel copy of the
/// data outside the owner to work around the lock. See the "Store Ownership &
/// Concurrency" section of `docs/architecture.md`.
///
/// Trigger concurrency follows the canonical callback contract documented on
/// [`Error::CallbackActiveCrossThread`]: same-DB cross-thread trigger
/// contention waits and proceeds inside the engine, unrelated databases proceed
/// independently, same-thread callback reentry returns
/// [`Error::CallbackReentry`], callback tx-bound handles are isolated to the
/// runner thread, and cron same-DB contention still returns the typed cron
/// callback-active error immediately.
///
/// # Drop semantics
///
/// Trigger waiters do not park while holding the public-operation read guard,
/// so `close()` can win the closed-handle transition after an active callback
/// exits. Production callers should call [`Database::close`] explicitly before
/// dropping the last handle when deterministic shutdown matters; a waiter that
/// wakes after close observes the normal closed-handle error.
type AcceptedSyncRowAuthors = HashMap<(String, Vec<u8>), String>;
type InMemoryDdlGenerations = HashMap<DdlGenerationKey, DdlGenerationValue>;
type OutboundRowLineages = HashMap<(String, Vec<u8>, Lsn), crate::protocol::WireRowLineage>;
type AuthoritativePurgeBlobReferences = (BTreeSet<[u8; 32]>, BTreeSet<[u8; 32]>);

#[derive(Clone, Copy)]
enum CloseProgress {
    Idle,
    Running(crate::owned_worker::CurrentThreadId),
    Complete,
}

struct CloseOwnerGuard<'a> {
    progress: &'a Mutex<CloseProgress>,
    changed: &'a Condvar,
    complete: bool,
    deferred: bool,
}
/// The row-author lineages and DDL provenance a production sync client would
/// attach to one outbound push batch.
#[cfg(all(feature = "test-seams", feature = "sync-orchestration"))]
type OutboundPushBatchProvenance = (OutboundRowLineages, Vec<crate::protocol::WireDdlProvenance>);

/// The construction route retains whether the plugin receiver was created by
/// the built-in factory or supplied by an embedder.  This is deliberately
/// private: the active transaction host is not selected in this slice.
#[derive(Clone)]
enum PluginBinding {
    Builtin(Arc<CorePlugin>),
    Supplied(Arc<dyn DatabasePlugin>),
}

/// The private half of an open request.  `PluginBinding` deliberately carries
/// the receiver separately so public options continue to mean exactly what
/// they meant before construction reaches the transaction-host selection.
struct OpenConfiguration {
    owner_reads: OwnerReadConfig,
    open_disposition: OpenDisposition,
    memory_limit: Option<usize>,
    disk_limit: Option<u64>,
    maintenance_policy: MaintenancePolicy,
    maintenance_poll_interval: Option<Duration>,
    contexts: Option<BTreeSet<ContextId>>,
    scope_labels: Option<BTreeSet<ScopeLabel>>,
    principal: Option<Principal>,
    reader_wait_stop: Option<OwnerReadCancellation>,
    reader_wait_deadline: Option<crate::ReaderWaitDeadline>,
    #[cfg(any(test, feature = "test-seams"))]
    test_observer: Option<Arc<dyn ReadSessionTestObserver>>,
    #[cfg(any(test, feature = "test-seams"))]
    test_kernel_observer: Option<Arc<dyn ReadKernelTestObserver>>,
}

/// The actual transaction owner shared by Database and its private contexts.
/// Production factories wrap their genuine Legacy manager; Owned operations
/// borrow the paired restricted host and never extract its lower manager.
#[derive(Clone)]
enum DatabaseTransactionHost {
    Legacy(Arc<TransactionManager<DynStore>>),
    Owned(EngineDatabaseHandle),
}

#[expect(
    clippy::large_enum_variant,
    reason = "Retain the inline write-set and terminal owners without adding a heap allocation or refund boundary solely to reduce enum size."
)]
enum ReceivedCommitTerminal {
    Legacy(WriteSet),
    Owned(OwnedDatabaseTerminal),
}
enum ReceivedCommitFailure {
    Legacy(contextdb_tx::manager::CommitFailure),
    Owned(OwnedDatabaseTerminalFailure),
}

/// Instance-local construction choice; production entry points select Legacy.
#[derive(Clone, Copy)]
enum DatabaseHostConstruction {
    Legacy,
    #[cfg(test)]
    Owned,
}

/// Prepared before the constructor moves WriterBudget into finalization.
/// The companion retains the actual original; later assembly consumes these
/// same capabilities rather than recreating a writer from registration.
struct OwnedHostAssembly {
    events: event_bus::OwnedEventBusHandle,
    funder: contextdb_tx::SourceFunder,
    accountant: crate::memory_accounting::AccountantHandle,
}

#[derive(Clone)]
enum DatabaseTransactionWeak {
    Legacy(std::sync::Weak<TransactionManager<DynStore>>),
    Owned(EngineDatabaseWeak),
}

pub struct Database {
    /// This handle's identity, never an address. Every handle gets its own,
    /// including one derived by narrowing another's access, exactly as each
    /// had its own address before.
    id: DatabaseId,
    tx_mgr: DatabaseTransactionHost,
    relational_store: Arc<RelationalStore>,
    graph_store: Arc<GraphStore>,
    vector_store: Arc<VectorStore>,
    vector_schema_gates: Arc<VectorSchemaGates>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    #[cfg(any(test, feature = "test-seams"))]
    forced_change_log_gap_through: Arc<AtomicU64>,
    /// Per-table shadow of `change_log`'s own `RowInsert`/`RowDelete`
    /// entries, and a global LSN reference count over every entry kind --
    /// see `ChangeLogTableIndex`/`ChangeLogLsnRefcounts` on
    /// `composite_store` for why these exist and how they stay in lockstep.
    change_log_table_index: Arc<RwLock<ChangeLogTableIndex>>,
    change_log_lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
    ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    /// In-memory provenance for the CURRENT delete marker of a natural key.
    /// A row delete has no live relational row on which to retain the ordinary
    /// sync-source sidecar, but a pulled tombstone must not be offered back to
    /// the hub as fresh local work. This deliberately lasts only for the open
    /// database lifetime; rebuilding old tombstone provenance after restart is
    /// a separate retention/recovery design.
    sync_tombstone_arrivals: Arc<RwLock<HashMap<(String, String), SyncTombstoneArrival>>>,
    /// Exact receiver-local graph occurrences that arrived through sync.
    /// Unlike a current-state flag, retaining the local LSN lets a later
    /// local mutation of the same edge propagate without resurrecting the
    /// older pulled occurrence.
    sync_graph_arrivals: Arc<RwLock<HashMap<GraphIdentity, FundedGraphArrival>>>,
    /// Private, exact-version markers for rows the hub terminally refused.
    /// They are reconciliation state, never a queryable rejection inbox.
    terminal_refusal_markers:
        Arc<RwLock<HashMap<TerminalRefusalMarkerKey, TerminalRefusalMarkerRecord>>>,
    terminal_refusal_scans:
        Arc<RwLock<HashMap<TerminalRefusalContextKey, TerminalRefusalScanState>>>,
    /// Authenticated peer identity for the currently sync-authored row. This
    /// mirrors the durable config sidecar for memory databases and is updated
    /// only after the row transaction commits.
    accepted_sync_row_authors: Arc<RwLock<AcceptedSyncRowAuthors>>,
    /// Private commit-LSN keyed received-schema stages.  Scoped database
    /// handles share this exact map because the transaction manager assigns
    /// the LSN and invokes callbacks through the shared database state.
    received_schema_stages: ReceivedSchemaStageRegistry,
    local_erasure_stages: discard::StageRegistry,
    /// The shape of every currently registered received-schema stage, staged
    /// under its own commit LSN so `CompositeStore::apply_exact` can resolve
    /// a staged unit's own vector writes against the schema it is installing
    /// rather than the receiver's current one. Cloned out of the same
    /// `CompositeStore` instance that owns and consumes it.
    received_arriving_table_meta: crate::composite_store::ReceivedArrivingTableMetaRegistry,
    /// The explicit SQL-transaction half is keyed by TxId until the commit
    /// manager assigns its one LSN; PersistentCompositeStore consumes the
    /// resulting LSN-keyed stage in that same Redb write transaction.
    pending_local_schema_stages: Arc<Mutex<schema_stage_registry::PendingRegistry>>,
    local_schema_stages: LocalSchemaStageRegistry,
    /// Private detached-adjudication probe.  It is armed only by received
    /// schema preparation and snapshots the exact WriteSet after normal sync
    /// commit preparation has stamped lineage/delete positions and receipt
    /// writes, but before the detached store applies it.
    capture_detached_sync_write_set: Arc<AtomicBool>,
    detached_sync_write_set: Arc<Mutex<Option<connected_replay::DetachedSyncCapture>>>,
    received_staging_access: Option<Arc<ReceivedStagingAccess>>,
    received_vector_image_pending: Arc<AtomicBool>,
    /// Ephemeral authenticated hub identity reserved immediately before an
    /// acknowledgement is durably promoted.  It is consumed by the same
    /// config write that changes Pending to Accepted; a crash before that
    /// promotion leaves the ordinary delete pending and resumable.
    persistence: Option<Arc<RedbPersistence>>,
    #[cfg(any(test, feature = "test-seams"))]
    commit_source_reference_charge: usize,
    /// Present only on a handle built from a committed image: the startup
    /// rows a live store would have been asked for at open.
    committed_image_startup: Option<Arc<CommittedImageStartupState>>,
    /// Present only on a handle built from a committed image that left its
    /// history in the store: where to read it the first time it is asked for.
    image_history: Option<Arc<ImageHistory>>,
    /// Descriptor-backed relational pages for a committed image whose rows
    /// were deliberately left out of memory at open.
    retained_relational_source: Option<crate::persistence::RetainedReadSource>,
    /// The sealed graph root is retained on both relational loading routes.
    retained_graph_source: Option<crate::persistence::RetainedReadSource>,
    /// Engine-owned media repository. File-backed databases share their
    /// existing Redb; memory databases own a temporary file-backed Redb so
    /// large media never becomes process-resident merely because rows do.
    blob_repository: Arc<BlobRepository>,
    open_registry_path: Mutex<Option<PathBuf>>,
    operation_gate: Arc<RwLock<()>>,
    read_publication_gate: Arc<ReadPublicationGate>,
    /// The statement-level reader/writer barrier for table metadata, rows and
    /// index publication. Shared by scoped handles of this database.
    schema_publication_gate: Arc<RwLock<()>>,
    apply_phase_pause: Arc<ApplyPhasePause>,
    /// Test-only pause point in a sync apply's per-row loop, immediately
    /// before the changeset's terminal commit -- after every row (including
    /// a stampless row's sentinel-stamped provenance sidecar write) has been
    /// staged into the transaction's write set, but before the commit that
    /// assigns this changeset's own LSN. Distinct from `apply_phase_pause`
    /// (which fires INSIDE an already-committing transaction, holding the
    /// commit lock): this fires before commit-lock acquisition, so a second,
    /// independent apply can run a full commit while this one is paused
    /// here -- exactly the interleaving a same-instant-sampled, different-
    /// commit-order race needs to reproduce deterministically. Armed
    /// globally by `Database::pause_before_sync_apply_commit_for_test`, but
    /// only actually STOPS a thread that separately marked itself via
    /// `Database::mark_this_thread_for_sync_apply_pre_commit_pause_for_test`
    /// (see `SYNC_APPLY_PRE_COMMIT_PAUSE_ARMED_HERE`) -- an unmarked thread
    /// passes this checkpoint unconditionally even while armed, so a test
    /// can pause exactly one concurrent apply while a second, independent
    /// one commits normally on another thread. The production path reads
    /// state that is never armed outside a test.
    sync_apply_pre_commit_pause: Arc<ApplyPhasePause>,
    /// Instance-owned fence after the complete received-schema write set is
    /// finalized and immediately before the durable store apply.
    #[cfg(any(test, feature = "test-seams"))]
    received_schema_pre_commit_pause: Arc<ApplyPhasePause>,
    /// Instance-owned fence after a received schema unit is durable and
    /// before its prepared memory image is published. It is test-only to arm.
    #[cfg(any(test, feature = "test-seams"))]
    received_schema_pre_publish_pause: Arc<ApplyPhasePause>,
    #[cfg(any(test, feature = "test-seams"))]
    received_schema_apply_observer: Arc<Mutex<Option<SharedReceivedSchemaApplyObservationForTest>>>,
    #[cfg(any(test, feature = "test-seams"))]
    local_schema_pre_commit_pause: Arc<ApplyPhasePause>,
    #[cfg(any(test, feature = "test-seams"))]
    local_schema_pre_publish_pause: Arc<ApplyPhasePause>,
    /// Test-only fence BETWEEN two of a local commit's own publication
    /// components -- the prepared schema image already in place, the custody
    /// and lineage deltas not yet. Never armed in production.
    #[cfg(any(test, feature = "test-seams"))]
    local_schema_between_publication_pause: Arc<ApplyPhasePause>,
    /// Test-only export fence after a consistent snapshot is captured and
    /// before its artifact can be published. It is never armed in production.
    export_after_capture_pause: Arc<ApplyPhasePause>,
    relational: MemRelationalExecutor<DynStore>,
    graph: MemGraphExecutor<DynStore>,
    vector: MemVectorExecutor<DynStore>,
    /// The store file's length as the reader found it, for a handle built
    /// from a committed image, which has no persistence to ask.
    image_store_file_bytes: Option<u64>,
    session_tx: Arc<Mutex<Option<TxId>>>,
    /// The transaction slot of the handle a READER handle was opened from.
    ///
    /// A reader handle deliberately has no transaction control of its own, but
    /// the reads it serves belong to whoever opened it: a writing session that
    /// asks its own handle for a bounded read is entitled to see what that
    /// session has done so far. So the reader watches the opener's slot rather
    /// than owning one, which also means a COMMIT or ROLLBACK on the opener is
    /// visible to the reader the moment it happens.
    origin_session_tx: Option<Arc<Mutex<Option<TxId>>>>,
    instance_id: uuid::Uuid,
    owner_thread: crate::owned_worker::CurrentThreadId,
    plugin: Arc<dyn DatabasePlugin>,
    owner_read_config: Arc<OwnerReadConfig>,
    declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
    /// The live owner-inspection service this open started, when it started
    /// one. It holds a shared handle onto the same stores rather than this
    /// value, so releasing the database releases the service with it.
    owner_read_service: Option<Arc<crate::owner_read::OwnerReadService>>,
    /// Why inspection is unavailable when serving was asked for and could not
    /// start. The database itself opened; the caller is told the operating
    /// system's own reason rather than discovering an empty result later.
    owner_read_startup_failure: Option<String>,
    #[cfg(any(test, feature = "test-seams"))]
    route_observer: Option<Arc<dyn crate::read_session::ReadSessionTestObserver>>,
    #[cfg(any(test, feature = "test-seams"))]
    kernel_observer: Option<Arc<dyn crate::read_session::ReadKernelTestObserver>>,
    /// Production-dead: counts how many times
    /// `retire_snapshot_free_vector_partitions` actually began its guarded
    /// removal pass, as opposed to skipping it because a cheap pre-check
    /// found nothing retirable. Shared across every scoped handle of the
    /// same database so a test can observe it regardless of which handle
    /// drove maintenance.
    #[cfg(any(test, feature = "test-seams"))]
    snapshot_free_retirement_pass_count: Arc<AtomicUsize>,
    access: AccessConstraints,
    accountant: crate::memory_accounting::AccountantHandle,
    conflict_policies: RwLock<ConflictPolicies>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    pruning_runtime: Mutex<PruningRuntime>,
    pruning_guard: Arc<Mutex<()>>,
    /// The ONE hub this database's retained tables are delivered to, loaded
    /// from database metadata at open so a restart cannot let a second hub
    /// claim first place.
    retention_sync_peer: Arc<Mutex<Option<String>>>,
    /// Memory databases have no restart contract, but the same public
    /// destination-change operation still needs its one-shot ancestry
    /// selection for the life of the shared database.
    in_memory_destination_reuploads: Arc<Mutex<HashMap<String, DestinationReuploadEpoch>>>,
    lineage_state_lock: Arc<Mutex<InMemoryLineageState>>,
    /// Serializes whole-migration retry classification through the matching
    /// durable stage commit. The lock is shared by derived handles, so an
    /// absent-vector observation cannot race another local stage writer.
    received_schema_manifest_lock: Arc<Mutex<()>>,
    sync_push_apply_locks: Arc<SyncPushApplyLocks>,
    #[cfg(feature = "sync-orchestration")]
    sync_client_push_locks: Arc<SyncClientPushLocks>,
    in_memory_received_schema_manifests: Arc<Mutex<InMemoryReceivedSchemaManifestState>>,
    /// The sole schema-instance generation owner. File-backed databases keep
    /// the same value in `sync_lineage.v1.table_generation.*`; memory
    /// databases retain it for this handle's life.
    in_memory_table_generations: Arc<Mutex<HashMap<String, u64>>>,
    in_memory_ddl_generations: Arc<Mutex<InMemoryDdlGenerations>>,
    /// Receiver-local arrival evidence for exact DDL occurrences. File-backed
    /// opens hydrate this from its dedicated config prefix; memory databases
    /// publish it only after the staged commit has become durable.
    received_ddl_arrivals: Arc<RwLock<HashMap<(Lsn, u32), DurableReceivedDdlArrival>>>,
    /// Locally authored DDL occurrences an authenticated received vector has
    /// since restated. Hydrated and published exactly like the arrival
    /// evidence above, and consumed only by outbound echo classification.
    superseded_ddl_occurrences: Arc<RwLock<HashMap<(Lsn, u32), DurableSupersededDdlOccurrence>>>,
    cron: Arc<CronState>,
    event_bus: Arc<EventBusState>,
    trigger: Arc<TriggerState>,
    /// How long THIS handle waits for a same-database trigger callback to go
    /// idle before the typed cross-thread error is due, when something has
    /// stated a deadline other than [`DEFAULT_TRIGGER_DEADLOCK_TIMEOUT`].
    ///
    /// The deadline is behavior, so it is never taken from the environment:
    /// no exported name moves it, and a journey that needs a shorter one
    /// states it through `set_trigger_deadlock_timeout_for_test` on the very
    /// handle under test. `None` -- the only value a shipped open produces --
    /// means the shipped default.
    trigger_deadlock_timeout_override: Mutex<Option<Duration>>,
    sync_relay_mode: Arc<AtomicBool>,
    in_memory_sync_progress: Arc<Mutex<InMemorySyncProgressState>>,
    in_memory_applied_push_watermarks: Arc<Mutex<HashMap<String, Lsn>>>,
    /// This database life's per-tenant sync incarnation, minted on first use and
    /// held for the life of the handle. Backed by a persisted config value for
    /// file-backed databases (so a plain reopen loads the same one) and by this
    /// cache alone for in-memory databases (each open is a fresh life).
    sync_incarnations: Arc<Mutex<HashMap<String, Incarnation>>>,
    // No public authority setter.
    custody_runtime: Arc<Mutex<Option<crate::custody::policy::Runtime>>>,
    custody_cache: Arc<Mutex<Option<crate::custody::store::Store>>>,
    custody_metadata: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    pending_event_bus_ddl: Mutex<HashMap<TxId, Vec<DdlChange>>>,
    pending_commit_metadata: Mutex<HashMap<TxId, PendingCommitMetadata>>,
    limit_update_lock: Arc<Mutex<()>>,
    #[cfg(any(test, feature = "test-seams"))]
    maintenance_poll_interval_update_pause: Arc<ApplyPhasePause>,
    #[cfg(any(test, feature = "test-seams"))]
    maintenance_poll_interval_update_probe: Arc<MaintenancePollIntervalUpdateProbe>,
    /// The store's disk budget, SHARED by every handle open on it.
    ///
    /// It is a property of the store, not of whoever is holding it: it is
    /// persisted with the store and it governs the same file whoever asks. A
    /// per-handle copy meant a limit set after a handle existed was invisible
    /// to that handle -- so a writer's own owner-read service answered "no
    /// disk limit" about a store that had one.
    disk_limit: Arc<AtomicU64>,
    disk_limit_startup_ceiling: Arc<AtomicU64>,
    /// Engine open-config for the engine's OWN durable trigger-audit history:
    /// an in-process, non-persisted retention window in seconds, defaulting
    /// to [`TRIGGER_AUDIT_RETENTION`]. Deliberately NOT backed by the redb
    /// config store -- it is caller policy for this handle's lifetime only,
    /// the same way a disk limit or memory ceiling is caller policy, never a
    /// stored declaration, DDL surface, or transported value. Shared (like
    /// [`Self::auto_compact_min_interval`]), not per-handle, and re-read per
    /// tick by the maintenance loop's [`MaintenanceContext`] -- a declared
    /// override reaches an ALREADY-RUNNING engine-owned loop, not only a
    /// loop spawned after the setter call.
    trigger_audit_retention_secs: Arc<AtomicU64>,
    /// Every read snapshot currently entitled to resolve a version — see
    /// [`SnapshotFloorRegistry`]. Shared (not per-handle) so a pin taken
    /// through one derived handle is honored by cleanup running through
    /// another handle onto the same underlying database.
    snapshot_registry: Arc<SnapshotFloorRegistry>,
    /// Commit-LSN bridge from the snapshot guard owned here to the erased
    /// `WriteSetApplicator` inside the transaction manager. The store consumes
    /// each sample exactly once before durable vector preparation; absence is
    /// never interpreted as permission to retire historical index state.
    vector_partition_snapshot_stages: VectorPartitionSnapshotStageRegistry,
    /// Node ids whose live row a retention pass already reclaimed while a
    /// registered reader could still see adjacency entries touching them.
    /// Those entries stay physically in place, and nothing else in the store
    /// records that the id ever carried a row, so the next pass reads this
    /// set to revisit them. Shared (not per-handle), like the registry
    /// itself, so a cycle driven through any handle sees the same carry-over.
    retention_deferred_edge_nodes: Arc<Mutex<RetentionDeferredEdges>>,
    /// Engine open-config: `true` when this database's maintenance is
    /// `MaintenancePolicy::CallerDriven`. Shared, not per-handle, so the
    /// policy is a property of the database, not of one handle onto it.
    maintenance_caller_driven: Arc<AtomicBool>,
    /// Persisted database poll setting in milliseconds. Shared across handles
    /// and sampled whenever the engine-owned worker is (re)started.
    maintenance_poll_interval_ms: Arc<AtomicU64>,
    /// The serving page cache a maintained vector partition may keep resident,
    /// as the consumer declared it. Zero means undeclared, so the engine's
    /// default share of the declared memory limit applies.
    vector_serving_cache_bytes: Arc<AtomicU64>,
    /// When the last maintenance cycle actually ran (engine-owned tick or a
    /// caller's own `run_maintenance_cycle` call), for the caller-driven
    /// backlog warning's "has a tick interval elapsed" check.
    last_maintenance_cycle_at: Arc<Mutex<Option<std::time::Instant>>>,
    /// Throttle for the caller-driven backlog warning, so a busy caller that
    /// never drives a cycle gets ONE warning per elapsed interval, not one
    /// per commit.
    caller_driven_backlog_warned_at: Arc<Mutex<Option<std::time::Instant>>>,
    /// When an automatic (threshold-gated) or explicit (`compact_now`) redb
    /// compaction last actually ran on this database — see
    /// [`AUTO_COMPACT_MIN_INTERVAL`]. Shared, not per-handle: a compaction
    /// run through one derived handle must throttle the automatic path on
    /// every other handle onto the same database too.
    last_auto_compact_at: Arc<Mutex<Option<std::time::Instant>>>,
    /// The declared storage compaction policy (`SET STORAGE_COMPACTION`) and
    /// the test-only interval override. Shared, like the stamp above.
    storage_compaction_policy: Arc<Mutex<crate::database::StorageCompactionState>>,
    sync_watermark: Arc<AtomicLsn>,
    closed: AtomicBool,
    close_progress: Arc<Mutex<CloseProgress>>,
    close_changed: Arc<Condvar>,
    /// The first cleanup or handoff failure survives an owner-read drain
    /// timeout so the retry that releases resources still reports its cause.
    close_error: Mutex<Option<Error>>,
    #[cfg(any(test, feature = "test-seams"))]
    close_follower_observer: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    resource_closed: Arc<AtomicBool>,
    /// Set when a commit reached durable storage and its in-memory
    /// publication was then interrupted by an unwind. Shared with every
    /// derived handle: from then on the database refuses service with
    /// `Error::PublicationInterrupted` until it is reopened. See
    /// `PublicationInterruptionWatch`.
    publication_interrupted: Arc<AtomicBool>,
    rows_examined: AtomicU64,
    /// Actual complete candidate rows materialized by this database's vector
    /// candidate subtree. Shared with derived handles, isolated from every
    /// independently opened database, and present only for test observation.
    #[cfg(any(test, feature = "test-seams"))]
    candidate_rows_materialized: Arc<AtomicU64>,
    last_vector_search_used_hnsw: Arc<AtomicBool>,
    last_vector_search_trace: Arc<RwLock<Option<contextdb_vector::mem::OwnedVectorSearchTrace>>>,
    statement_cache: RwLock<HashMap<String, Arc<CachedStatement>>>,
    /// The rank-policy formulas this STORE has registered, held once and read
    /// by every handle onto it. A derived handle -- a scoped view, a bounded
    /// read session -- is another way of looking at the same store, not
    /// another store, so it must see the formulas DDL registered whether that
    /// DDL ran before the handle was taken or after it. A per-handle copy
    /// answered "no such formula" to a search the other door answers.
    #[allow(clippy::type_complexity)]
    rank_formula_cache: Arc<RwLock<HashMap<(String, String), Arc<RankFormula>>>>,
    acl_grant_cache: RwLock<gate::AdmittedAclGrantCache>,
    rank_policy_eval_count: AtomicU64,
    rank_policy_formula_parse_count: AtomicU64,
    fk_indexed_tuple_probes: AtomicU64,
    fk_full_scan_fallbacks: AtomicU64,
    commit_rows_validated: AtomicU64,
    commit_indexed_probes: AtomicU64,
    commit_staged_vs_staged_comparisons: AtomicU64,
    commit_scan_rows_touched: AtomicU64,
    commit_index_maintenance_visits: AtomicU64,
    #[cfg(any(test, feature = "test-seams"))]
    commit_stage_wall_nanos: [AtomicU64; 7],
    corrupt_joined_values: RwLock<HashSet<(String, RowId, String)>>,
    resource_owner: bool,
    /// What this handle is for, and therefore whether the store waits for it.
    handle_role: HandleRole,
    // Payload fields retire before this image admission. Finalization follows
    // it, retaining the source through all payload and image cleanup.
    read_image_memory: Option<crate::read_image_memory::ImageMemoryHandle>,
    /// Shared by every handle on this store: how many holders are still live,
    /// and what the owner left for the last of them to release.
    finalization: Arc<StoreFinalization>,
}

/// Steady-state snapshot of trigger-progress telemetry counters. The
/// accessor is `#[doc(hidden)] pub` so integration tests (which compile
/// against the released crate signature, not `cfg(test)`) can read it,
/// while it remains absent from rustdoc and unsupported as a public API.
/// cg observes throughput via `Store::record_observation`, not via these
/// counters.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TriggerProgressTelemetrySnapshot {
    pub wait_observed: u64,
    /// Engine-internal "retry-eligible refusal" count for same-DB B2. Under
    /// the wait-and-proceed contract this stays 0 under healthy contention;
    /// it is positive only when the deadlock-guard timeout regime surfaces
    /// the same-DB typed Err.
    pub typed_err_observed_same_db: u64,
    pub deadlock_guard_timeout_observed: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AccessConstraints {
    contexts: Option<BTreeSet<ContextId>>,
    scope_labels: Option<BTreeSet<ScopeLabel>>,
    principal: Option<Principal>,
}

/// Why a writer will not serve the visibility one read session declared.
///
/// It exists for the axis that cannot be narrowed: two identities have no
/// ordering, so a declaration naming one the writer does not read as has no
/// answer that is both the reader's and no wider than it asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeclaredReadRefusal {
    /// The session declared it reads as `declared` and the writer serving it
    /// was opened as `serving`.
    PrincipalConflict {
        declared: Principal,
        serving: Principal,
    },
}

mod metadata_and_commit_scope;
pub(crate) use metadata_and_commit_scope::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AclGrantCacheKey {
    principal: Principal,
    ref_table: String,
    ref_column: String,
    snapshot: SnapshotId,
    source: Option<(bool, redb::StorageSnapshotIdentity)>,
}

pub(crate) enum InsertRowResult {
    Inserted(RowId),
    NoOp,
}

trait GraphRetainedBytes {
    fn retained_graph_bytes(&self) -> usize;
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct WriteSetCounts {
    relational_inserts: usize,
    relational_deletes: usize,
    adj_inserts: usize,
    adj_deletes: usize,
    row_adj_inserts: usize,
    row_adj_deletes: usize,
    vector_inserts: usize,
    vector_deletes: usize,
    vector_moves: usize,
}

pub(crate) struct UpdateReplacementContext<'a> {
    pub(crate) meta: &'a TableMeta,
    pub(crate) committed_row_exists: bool,
    pub(crate) created_at: Wallclock,
}

/// What a removal hands back to the accountant at the commit that removes it.
///
/// A deleted or replaced row version hands its whole charge back here, at
/// the commit that stamps it: the accountant is a logical attribution of
/// what the store holds for its caller, not a measurement of the pages the
/// process still has resident, so the budget a row took is available again
/// the moment the row is gone from the caller's point of view. The pass that
/// later removes the tombstoned version physically credits nothing for it
/// (`prune_expired_rows`, `compact_currency_versions_inner`), so the version
/// is released exactly once.
///
/// Adjacency has no entry here on purpose: deleting an edge stamps the entry
/// and leaves it whole in both adjacency maps, so its bytes are still held
/// and are handed back by the retention pass that physically removes it.
#[derive(Debug, Default, Clone)]
pub(crate) struct DeleteReleaseBytes {
    relational: Vec<usize>,
    vectors: Vec<usize>,
}

/// Transaction-local, writer-admitted generation memo. A verified root identity
/// names the value's source; a changed root forces a fresh lookup. Schema
/// staging uses its working database, and checkpoint rollback discards the memo.
#[derive(Debug)]
struct PendingLineageGenerations {
    entries: Mutex<Option<Box<PendingLineageGeneration>>>,
    _control: crate::memory_accounting::OwnedMemoryReservation,
}
#[derive(Debug)]
struct PendingLineageGeneration {
    table: String,
    root: redb::StorageSnapshotIdentity,
    generation: u64,
    next: Option<Box<PendingLineageGeneration>>,
    _memory: crate::memory_accounting::OwnedMemoryReservation,
}

#[derive(Debug, Default, Clone)]
struct PendingCommitMetadata {
    discard_selections: Vec<AuthoritativePurgeSelection>,
    delivery_registrations: Vec<crate::custody::preparation::SourceRegistration>,
    conditional_update_guards: Vec<PendingConditionalUpdateGuard>,
    sync_lineage_guards: Vec<PendingSyncLineageGuard>,
    upsert_intents: Vec<PendingUpsertIntent>,
    vector_schema_epochs: HashMap<VectorIndexRef, u64>,
    lineage_generations: Option<Arc<PendingLineageGenerations>>,
    row_graph_undo: Option<Arc<row_graph::LegacyRowGraphJournal>>,
}

struct LegacyWriteSetCheckpoint<'a> {
    database: &'a Database,
    tx: TxId,
    counts: [usize; 15],
    row_graph_marker: usize,
    requires_commit_lsn: bool,
    propagation_in_progress: bool,
    marker: [usize; 2],
    metadata_counts: [usize; 5],
    metadata_existed: bool,
    vector_epochs: Vec<VectorIndexRef>,
    graph_checkpoint: Option<connected_replay::PrivateGraphStatementCheckpoint>,
    _charge: crate::memory_accounting::OwnedMemoryReservation,
    active: bool,
}

// This private host is deliberately not constructed by Database's Legacy opens.
// Activation requires every producer to use EngineGuard and admitted metadata.
/// Field order keeps a rejected original body ahead of its reservation.
struct OriginalRowValues {
    values: HashMap<ColName, Value>,
    admission: OwnedMemoryReservation,
}

struct OriginalOptionalRowValues {
    values: HashMap<ColName, Value>,
    admission: Option<OwnedMemoryReservation>,
}

struct OriginalDatabaseRow {
    row: VersionedRow,
    admission: OwnedMemoryReservation,
}

struct NativeVectorCancellation {
    positions: std::cell::RefCell<Vec<usize>>,
    moved_sources: Vec<RowId>,
    candidates: Vec<RowId>,
    present: Vec<bool>,
    deletes: Vec<(VectorIndexRef, RowId, TxId)>,
    pending_move_from: bool,
}
struct OriginalDatabaseVector {
    entry: VectorEntry,
    scalar: bool,
    cancellation:
        Option<crate::custody::preparation::OwnedPreparationValue<NativeVectorCancellation>>,
    admission: OwnedMemoryReservation,
}
struct OriginalDatabaseVectors {
    entries: Vec<OriginalDatabaseVector>,
    control: OwnedMemoryReservation,
}

struct EngineDatabaseTransactions {
    transactions: EngineTransactionHandle<DynStore>,
    metadata: EngineMetadataJournal,
    events: event_bus::OwnedEventBusHandle,
    persistent_store_address: Option<usize>,
    // Retain the original provider last so the journal and lower facade have
    // released their native allocations before its source receiver can end.
    funder: contextdb_tx::SourceFunder,
}

/// A private paired strong owner for the engine-only host.  It deliberately
/// exposes only a scoped borrow, so no raw manager, `Arc`, or mutable write
/// set can cross the engine boundary.
struct EngineDatabaseHandle {
    data: Option<Arc<EngineDatabaseTransactions>>,
    credit: Option<contextdb_tx::SourceCharge>,
}

/// The weak half retains the same external invoice until std's last weak
/// header is physically freed.  It is private so no uncredited `Weak` can
/// leave the engine.
struct EngineDatabaseWeak {
    data: Option<std::sync::Weak<EngineDatabaseTransactions>>,
    credit: Option<contextdb_tx::SourceCharge>,
}

/// Native preparation vector. Capacity admission precedes allocation;
/// replacement frees the old vector before retiring only its control debit.
struct OriginalEpochInputs {
    values: Vec<(VectorIndexRef, u64)>,
    owner: OwnedMemoryReservation,
    control_bytes: usize,
}

type OriginalLineageMemo = event_bus::OwnedEventShared<NativeLineageGenerations>;
struct NativeLineageGenerations {
    entries: Mutex<Option<MetadataNode<NativeLineageGeneration>>>,
}
struct NativeLineageGeneration {
    table: String,
    root: redb::StorageSnapshotIdentity,
    generation: u64,
    next: Option<MetadataNode<NativeLineageGeneration>>,
    _table_memory: OwnedMemoryReservation,
}

/// Pair each actual metadata body with its producer's original admission before
/// validation, locking or any fallible work. There is no refund-by-byte-count.
struct AdmittedMetadataValue<T> {
    body: Option<T>,
    invoice: Option<OwnedMemoryReservation>,
}
/// Native capacity and its actual reservation move as one owner. A replacement
/// buffer is independently admitted; an old buffer stays alive for rollback.
struct MetadataBuffer<T> {
    values: Vec<T>,
    credit: Option<OwnedMemoryReservation>,
}

/// The owned counterpart of the seven Legacy pending fields. Each vector entry
/// owns its actual body admission independently from its native vector buffer.
/// Epoch maps retain their complete native keys, values, hasher and bucket debit.
struct EnginePendingCommitMetadata {
    discard_selections: MetadataBuffer<AdmittedMetadataValue<AuthoritativePurgeSelection>>,
    delivery_registrations:
        MetadataBuffer<AdmittedMetadataValue<crate::custody::preparation::SourceRegistration>>,
    conditional_update_guards: MetadataBuffer<AdmittedMetadataValue<PendingConditionalUpdateGuard>>,
    sync_lineage_guards: MetadataBuffer<AdmittedMetadataValue<PendingSyncLineageGuard>>,
    upsert_intents: MetadataBuffer<AdmittedMetadataValue<PendingUpsertIntent>>,
    vector_schema_epochs: Option<AdmittedMetadataValue<HashMap<VectorIndexRef, u64>>>,
    lineage_generations: Option<OriginalLineageMemo>,
}

enum AdmittedMetadataBatch {
    Discard(MetadataBuffer<AdmittedMetadataValue<AuthoritativePurgeSelection>>),
    Delivery(
        MetadataBuffer<AdmittedMetadataValue<crate::custody::preparation::SourceRegistration>>,
    ),
    Conditional(MetadataBuffer<AdmittedMetadataValue<PendingConditionalUpdateGuard>>),
    SyncLineage(MetadataBuffer<AdmittedMetadataValue<PendingSyncLineageGuard>>),
    Upsert(MetadataBuffer<AdmittedMetadataValue<PendingUpsertIntent>>),
}
struct MetadataSplice<T> {
    at: usize,
    installed: usize,
    original_len: usize,
    original_capacity: usize,
    selections: MetadataBuffer<usize>,
    inserted: MetadataBuffer<AdmittedMetadataValue<T>>,
    removed: MetadataBuffer<(usize, AdmittedMetadataValue<T>)>,
    storage: Option<MetadataBuffer<AdmittedMetadataValue<T>>>,
}
struct MetadataEpochChange {
    candidate: Option<AdmittedMetadataValue<HashMap<VectorIndexRef, u64>>>,
    prior: Option<AdmittedMetadataValue<HashMap<VectorIndexRef, u64>>>,
    displaced: Option<AdmittedMetadataValue<HashMap<VectorIndexRef, u64>>>,
}
struct MetadataMemoChange {
    candidate: Option<OriginalLineageMemo>,
    prior: Option<OriginalLineageMemo>,
    displaced: Option<OriginalLineageMemo>,
}
enum EngineMetadataChange {
    Discard(MetadataSplice<AuthoritativePurgeSelection>),
    Delivery(MetadataSplice<crate::custody::preparation::SourceRegistration>),
    Conditional(MetadataSplice<PendingConditionalUpdateGuard>),
    SyncLineage(MetadataSplice<PendingSyncLineageGuard>),
    Upsert(MetadataSplice<PendingUpsertIntent>),
    Epochs(MetadataEpochChange),
    Memo(MetadataMemoChange),
}

/// Keep the Box allocation itself alive until after its value is destroyed;
/// only then may its original control credit be returned.
struct MetadataNode<T> {
    value: Option<Box<T>>,
    credit: Option<OwnedMemoryReservation>,
}

struct EngineMetadataInverse {
    change: EngineMetadataChange,
    next: Option<MetadataNode<EngineMetadataInverse>>,
}
struct EngineMetadataFrame {
    identity: (TxId, u64, u64, usize),
    marker: usize,
    existed: bool,
    stale_memo: Option<OriginalLineageMemo>,
    previous: Option<MetadataNode<EngineMetadataFrame>>,
}
struct EngineMetadataRecord {
    tx: TxId,
    generation: u64,
    revision: u64,
    metadata: EnginePendingCommitMetadata,
    existed: bool,
    undo: Option<MetadataNode<EngineMetadataInverse>>,
    undo_len: usize,
    frames: Option<MetadataNode<EngineMetadataFrame>>,
    depth: usize,
    next: Option<MetadataNode<EngineMetadataRecord>>,
}
struct EngineMetadataJournal {
    events: event_bus::OwnedEventBusHandle,
    records: Mutex<Option<MetadataNode<EngineMetadataRecord>>>,
}
// Terminal custody reuses the journal's original nodes and reservations.
type MetadataHead = Option<MetadataNode<EngineMetadataRecord>>;
type OwnedDatabaseTerminal = contextdb_tx::manager::EngineTerminalWriteSet<OwnedTerminalMetadata>;
type OwnedDatabaseTerminalFailure =
    contextdb_tx::manager::EngineTerminalFailure<OwnedTerminalMetadata>;
type OwnedDatabaseDrain = contextdb_tx::manager::EnginePairedOwnedDrain<
    OwnedTerminalMetadataChain,
    OwnedTerminalMetadata,
>;

// Temporary preparation holds the exact original optional peer and its identity.
// The lower owner remains source-first; no replacement invoice is admitted.
struct OwnedTemporaryMetadata {
    identity: contextdb_tx::manager::EngineTerminalIdentity,
    original_present: bool,
    original: OwnedTerminalMetadata,
}
type TemporaryFailure = contextdb_tx::manager::EngineTerminalFailure<OwnedTemporaryMetadata>;
type PreparationResult<R> = std::result::Result<R, TemporaryFailure>;
type ConditionalBuffer = MetadataBuffer<AdmittedMetadataValue<PendingConditionalUpdateGuard>>;

struct ValidatedTemporaryMetadata<'j> {
    terminal: ValidatedMetadataTerminal<'j>,
    original_present: bool,
}
struct ValidatedMetadataReturn<'j> {
    records: parking_lot::MutexGuard<'j, MetadataHead>,
    original_present: bool,
}

/// A synchronous borrow of the selected companion's authentic immutable
/// reservation and the same unpublished write used by terminal install.
/// Every source Arc strong reference carries the same external allocation
/// charge. Local factory owners must retire the Arc before their charge, too.
struct FundedWorkingGraphSource {
    source: Arc<writer_read::ScopedGraphSource>,
    control: contextdb_tx::SourceCharge,
}

struct ReceivedWorkingPreparation<'o> {
    write: &'o crate::persistence::received_working::ReceivedWorkingWrite,
    original: &'o crate::memory_accounting::OwnedMemoryReservation,
    funder: &'o contextdb_tx::SourceFunder,
    store: &'o RedbPersistence,
}

struct DatabaseCommitScope<'s, 'b, 'm> {
    host: &'s EngineDatabaseTransactions,
    lower: &'s contextdb_tx::manager::EngineCommitScope<'b, 'm, DynStore>,
}

struct OwnedTerminalMetadata {
    node: MetadataHead,
}
struct ValidatedMetadataTerminal<'j> {
    records: parking_lot::MutexGuard<'j, MetadataHead>,
    identity: contextdb_tx::manager::EngineTerminalIdentity,
}
struct ValidatedMetadataDrain<'j> {
    records: parking_lot::MutexGuard<'j, MetadataHead>,
    len: usize,
}
struct OwnedTerminalMetadataChain {
    head: MetadataHead,
}
// A stack cleanup guard empties the remaining chain iteratively if one
// metadata payload destructor unwinds. A second destructor panic aborts,
// following Rust's ordinary double-panic behavior.
struct TerminalMetadataCleanup {
    head: MetadataHead,
}

struct PreparedMetadataCheckpoint {
    expected_revision: u64,
    record: Option<MetadataNode<EngineMetadataRecord>>,
    frame: MetadataNode<EngineMetadataFrame>,
}
struct PreparedMetadataMutation {
    identity: (TxId, u64, u64, usize),
    expected_revision: u64,
    next_revision: u64,
    next_undo_len: usize,
    inverse: Option<MetadataNode<EngineMetadataInverse>>,
}

/// Retired allocations leave the journal lock before any native destructor or
/// accountant is invoked. Reuse the already admitted links as cleanup storage.
#[derive(Default)]
struct RetiredEngineMetadata {
    inverses: Option<MetadataNode<EngineMetadataInverse>>,
    frame: Option<MetadataNode<EngineMetadataFrame>>,
    record: Option<MetadataNode<EngineMetadataRecord>>,
}
struct ValidatedMetadataClose<'a> {
    records: MutexGuard<'a, Option<MetadataNode<EngineMetadataRecord>>>,
    identity: (TxId, u64, u64, usize),
    settle: bool,
}
// Metadata has no independent consuming destructor. Installed tokens belong
// exclusively to the paired owner, including errors and unwinding exits.
struct EngineMetadataCheckpoint<'a> {
    journal: &'a EngineMetadataJournal,
    identity: (TxId, u64, u64, usize),
}
// The prepare-peer return retains the pending lock while the lower frame
// installs. It has no independent close destructor and owns no new allocation.
struct InstalledMetadataCheckpoint<'a> {
    checkpoint: EngineMetadataCheckpoint<'a>,
    records: MutexGuard<'a, Option<MetadataNode<EngineMetadataRecord>>>,
}
struct OwnedCheckpointPair<'a> {
    transaction: std::mem::ManuallyDrop<contextdb_tx::EngineCheckpoint<'a, DynStore>>,
    metadata: EngineMetadataCheckpoint<'a>,
}
struct OwnedWriteSetCheckpoint<'a> {
    pair: Option<OwnedCheckpointPair<'a>>,
}

// Keep checkpoint ownership inline; boxing would add a separate allocation and invoice.
#[allow(clippy::large_enum_variant)]
enum WriteSetCheckpointBranch<'a> {
    Legacy(LegacyWriteSetCheckpoint<'a>),
    Owned(OwnedWriteSetCheckpoint<'a>),
}
pub(crate) struct WriteSetCheckpoint<'a> {
    branch: Option<WriteSetCheckpointBranch<'a>>,
}

#[derive(Debug, Clone)]
pub(crate) struct UpsertIntentDetails {
    pub insert_values: HashMap<ColName, Value>,
    pub conflict_columns: Vec<ColName>,
    pub update_columns: Vec<(ColName, Expr)>,
    pub params: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
struct PendingConditionalUpdateGuard {
    table: TableName,
    row_id: RowId,
    predicates: Vec<(ColName, Value)>,
    before: WriteSetCounts,
    after: WriteSetCounts,
    fail_on_conflict: bool,
}

#[derive(Debug, Clone)]
struct PendingSyncLineageGuard {
    table: TableName,
    row_id: RowId,
    values: HashMap<ColName, Value>,
}

#[derive(Debug, Clone)]
struct PendingUpsertIntent {
    table: TableName,
    row_id: RowId,
    active_tx: TxId,
    insert_values: HashMap<ColName, Value>,
    conflict_columns: Vec<ColName>,
    update_columns: Vec<(ColName, Expr)>,
    params: HashMap<String, Value>,
}

struct CommitTimeUpsertLookup<'a> {
    intents: &'a [PendingUpsertIntent],
    by_row: HashMap<(TableName, RowId), Vec<usize>>,
}

#[derive(Debug, Default)]
struct CommitValidationOutcome {
    committed_tx: Option<TxId>,
    conditional_noop_count: u64,
    conditional_conflict_count: u64,
}

#[derive(Debug, Default)]
pub(crate) struct IndexScanTxOverlay {
    pub deleted_row_ids: std::collections::HashSet<RowId>,
    pub matching_inserts: Vec<VersionedRow>,
}

enum RowConstraintCheck {
    Valid,
    DuplicateUniqueNoOp,
}

enum ConstraintProbe {
    NoIndex,
    NoMatch,
    Match(RowId),
}

static OPEN_FILE_DATABASES: OnceLock<OpenFileRegistry> = OnceLock::new();

#[cfg(all(feature = "test-seams", unix))]
static PERSISTED_STATE_SCHEMA_FRONTIER_PAUSES: OnceLock<Mutex<HashMap<usize, PathBuf>>> =
    OnceLock::new();

#[cfg(test)]
static QUERY_TRACE_ROWS_EXAMINED_CAPTURE_PAUSE: OnceLock<Arc<ApplyPhasePause>> = OnceLock::new();

#[cfg(any(test, feature = "test-seams"))]
static VECTOR_JOURNAL_CLEANUP_PAUSE: OnceLock<Arc<ApplyPhasePause>> = OnceLock::new();

#[cfg(any(test, feature = "test-seams"))]
static VECTOR_REPAIR_SNAPSHOT_SAMPLE_TRANSITION_PAUSE: OnceLock<Arc<ApplyPhasePause>> =
    OnceLock::new();

#[cfg(any(test, feature = "test-seams"))]
static VECTOR_JOURNAL_CLEANUP_STEP_GAP_PAUSE: OnceLock<Arc<ApplyPhasePause>> = OnceLock::new();

struct OpenRegistryReservation {
    path: PathBuf,
    active: bool,
}

/// What a handle on a store is FOR, and therefore whether the store waits for
/// it.
///
/// A store is held open by the handle that acquired it and by every live
/// reader looking at it. It is NOT held open by the handles its own components
/// keep for themselves — the owner service's view of the store it serves, a
/// background worker's view of the store it maintains — because those live for
/// as long as the process does. Counting them would mean the store is never
/// let go and the next opener is told forever that it is taken.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandleRole {
    /// Acquired the store's resources; hands them over rather than releasing
    /// them while a reader is still looking.
    Owner,
    /// A caller reading the store. The store outlives the owner for as long as
    /// one of these is alive.
    Reader,
    /// A view the store keeps for its own machinery. Waits for nothing.
    Internal,
}

/// The three named engine DynStore Boxes own one full concrete aggregate
/// control. Public CompositeStore/PersistentCompositeStore values keep their
/// original storage and host Arc/Weak behavior.
struct AdmittedDynStore<T: contextdb_tx::WriteSetApplicator> {
    payload: std::mem::ManuallyDrop<T>,
    control: Option<crate::memory_accounting::ControlCredit>,
}

/// Owning adapter control is admitted from the companion before its Box is
/// allocated. The stable payload address qualifies the terminal apply scope.
struct OwnedPersistentDynStore {
    payload: crate::persistent_store::OwnedPersistentCompositeStore,
}
/// Structural custody while assembly still has fallible work ahead. The Box
/// allocation is physically gone before its transferred original debit drops.
struct PreparedOwnedPersistentDynStore {
    store: DynStore,
    address: usize,
    control: contextdb_tx::SourceCharge,
}

/// The memory applicator shares the owning companion; its Box control is
/// held outside this allocation by the same lower manager that owns the Box.
struct OwnedMemoryDynStore {
    payload: CompositeStore,
    events: event_bus::OwnedEventBusHandle,
}
struct PreparedOwnedMemoryDynStore {
    store: DynStore,
    control: contextdb_tx::SourceCharge,
}

/// What a store still owes, and who is left to pay it.
///
/// Every handle on one store shares this. The owner handle is the one that
/// ACQUIRED the store's resources, but it is not necessarily the last to let
/// go: a session a blocked request or a suspended cursor is holding keeps the
/// store alive after the owner handle is gone. Releasing while such a reader
/// is still inside would pull the store out from under it and tell the next
/// opener the store is free while somebody is still reading it.
///
/// So the owner hands what it holds to this record instead, and whichever
/// holder turns out to be the last one pays it out on the way down.
pub(crate) struct StoreFinalization {
    #[cfg(feature = "test-seams")]
    publication_drives_for_test:
        Mutex<HashMap<VectorIndexRef, std::sync::Weak<WritePublicationControlForTest>>>,
    #[cfg(feature = "test-seams")]
    release_completion_for_test: Arc<ReleaseCompletionForTest>,
    live_holders: AtomicUsize,
    loaded_history: Mutex<Option<LoadedHistoryBuffers>>,
    local_policy_metadata_enabled: AtomicBool,
    schema_history_memory: std::mem::ManuallyDrop<Mutex<Option<SchemaHistoryMemory>>>,
    pending: std::mem::ManuallyDrop<Mutex<Option<PendingStoreRelease>>>,
    vector: std::mem::ManuallyDrop<Arc<VectorStore>>,
    accountant: std::mem::ManuallyDrop<crate::memory_accounting::AccountantHandle>,
    non_vector_memory: std::mem::ManuallyDrop<Mutex<Option<RetainedNonVectorMemoryOwner>>>,
    staged_posting_admission:
        std::mem::ManuallyDrop<Arc<writer_read::WriterStagedPostingAdmission>>,
    vector_accountant: std::mem::ManuallyDrop<contextdb_vector::MemoryBudgetOwner>,
    _writer_use: std::mem::ManuallyDrop<Option<crate::memory_accounting::WriterUse>>,
    control: Option<crate::memory_accounting::ControlCredit>,
}

/// One owner-service operation or suspended cursor that is still using this
/// store after the writable handle may have gone away. It carries no database
/// interface and releases the owner's deferred resources when it is the last
/// real reader out.
pub(crate) struct OwnerReadStoreHold {
    finalization: Arc<StoreFinalization>,
}

/// The standing relational and graph allocations owned by one database life.
/// Their existing mutation paths admit and release the same estimates; this
/// final owner returns exactly the objects still present when the last handle
/// disappears. Vector allocations carry their own object-local reservations.
struct RetainedNonVectorMemoryOwner {
    frozen_bytes: Option<usize>,
    defer_private_row_refunds: bool,
    aborted_private_effects: Option<connected_replay::PrivatePrefixEffects>,
    relational: std::mem::ManuallyDrop<Arc<RelationalStore>>,
    graph: std::mem::ManuallyDrop<Arc<GraphStore>>,
    accountant: crate::memory_accounting::AccountantHandle,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

/// The resources an owner acquired, waiting for the last holder to release.
struct PendingStoreRelease {
    #[cfg(feature = "test-seams")]
    release_completion_for_test: Arc<ReleaseCompletionForTest>,
    persistence: Option<Arc<RedbPersistence>>,
    resource_closed: Arc<AtomicBool>,
    open_registry_path: Option<PathBuf>,
    owner_read_service: Option<std::sync::Weak<crate::owner_read::OwnerReadService>>,
    owner_read_status: Option<OwnerReadStatus>,
    /// Present only when somebody else will pay this release out. A plugin is
    /// told the store closed by whoever actually finishes it, and a handle
    /// that finishes its own store is an ordinary drop, which owes the plugin
    /// nothing.
    plugin: Option<Arc<dyn DatabasePlugin>>,
}

struct StoreFinalizationCleanup {
    source: *mut StoreFinalization,
    next: u8,
    refund: Option<crate::memory_accounting::PendingControlRefund>,
}

#[cfg(feature = "test-seams")]
struct ResourceReleaseCompletionForTest(Arc<ReleaseCompletionForTest>);

#[cfg(feature = "test-seams")]
struct ReleaseStateForTest {
    owner_tasks_observed: bool,
    resources_released: bool,
    owner_tasks_joined: bool,
    error: Option<String>,
}

#[cfg(feature = "test-seams")]
pub(crate) struct ReleaseCompletionForTest {
    generation: uuid::Uuid,
    state: Mutex<ReleaseStateForTest>,
    changed: Condvar,
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub struct LastHandleReleaseForTest {
    completion: Arc<ReleaseCompletionForTest>,
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastHandleReleaseReceipt {
    pub generation: uuid::Uuid,
}

struct OpenFileRegistry {
    entries: Mutex<BTreeMap<PathBuf, OpenRegistryState>>,
    waiters: Condvar,
}

#[derive(Clone, Copy)]
enum OpenRegistryState {
    Opening { pid: u32 },
    Open { pid: u32 },
}

struct DatabaseOperationGuard<'a> {
    db_id: DatabaseId,
    _lock: Option<parking_lot::RwLockReadGuard<'a, ()>>,
    // Every outer public operation observes one table-schema publication.
    // Local/received DDL already owns the write side before entering here, so
    // this re-entrant read acquisition never attempts a read-to-write upgrade.
    _schema_publication: Option<SchemaPublicationGuard<'a>>,
    _writer_read: Option<writer_read::WriterRootScope>,
    _memory: crate::read_image_memory::ImageMemoryScope,
}

#[derive(Default)]
struct ReadPublicationGateState {
    readers: BTreeMap<String, usize>,
    writer_scope: Option<BTreeSet<String>>,
    writer_active: bool,
    wake_generation: u64,
    cursors: Vec<std::sync::Weak<crate::executor::BoundedCursorExecutionState>>,
}

#[derive(Default)]
struct ReadPublicationGate {
    state: Mutex<ReadPublicationGateState>,
    changed: Condvar,
}

struct ReadPublicationWake(std::sync::Weak<ReadPublicationGate>);

pub(crate) struct AuthoritativeReadPublicationGuard {
    gate: Arc<ReadPublicationGate>,
    tables: BTreeSet<String>,
}

struct AuthoritativePurgePublicationGuard {
    gate: Arc<ReadPublicationGate>,
}

#[cfg(test)]
mod read_publication_wait_tests;

/// Serializes table-schema publication against a whole SQL statement. Local
/// table DDL takes the write side; ordinary statements and EXPLAIN take the
/// read side. It is deliberately separate from the close-operation gate:
/// schema writers may wait for readers without changing close semantics.
pub(crate) struct SchemaPublicationGuard<'a> {
    gate_id: usize,
    _read: Option<parking_lot::RwLockReadGuard<'a, ()>>,
    _write: Option<parking_lot::RwLockWriteGuard<'a, ()>>,
}

/// Internal transport-orchestration lease that keeps one outbound extraction
/// and its wire evidence on the same published table schema.
///
/// The compiled transport orchestration lives in this crate; the server-path copy
/// is an uncompiled exact-source audit mirror. This lease therefore remains
/// crate-private and adds no downstream database capability.
#[cfg(feature = "sync-orchestration")]
pub(crate) struct OutboundSyncSchemaReadGuard<'a> {
    _inner: SchemaPublicationGuard<'a>,
}

struct TriggerCloseWaiterGuard<'a> {
    trigger: &'a TriggerState,
}

pub(crate) struct ReadRegistrationBudget {
    clock: Arc<dyn DeadlineClock>,
    started_ms: u64,
    active_ms: u64,
}

#[cfg(test)]
mod owner_cursor_pin_retry_tests;

#[cfg(test)]
mod read_registration_budget_tests;

#[derive(Debug, Clone)]
struct PropagationQueueEntry {
    table: String,
    uuid: uuid::Uuid,
    target_state: String,
    depth: u32,
    abort_on_failure: bool,
}

#[derive(Debug, Clone, Copy)]
struct PropagationSource<'a> {
    table: &'a str,
    uuid: uuid::Uuid,
    state: &'a str,
    depth: u32,
}

#[derive(Debug, Clone, Copy)]
struct PropagationContext<'a> {
    tx: TxId,
    snapshot: SnapshotId,
    metas: &'a HashMap<String, TableMeta>,
}

#[derive(Debug)]
struct SubscriptionState {
    subscribers: Vec<SyncSender<CommitEvent>>,
    events_sent: u64,
    events_dropped: u64,
}

#[derive(Debug)]
struct PruningRuntime {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    interval_ms: Arc<AtomicU64>,
    // Wake-cycles STARTED by the current maintenance thread (incremented on
    // each wake, before the pending gate). Test-build only — production
    // carries neither the field nor the increment; the unit tests poll it as
    // the liveness half of a state-based wait.
    #[cfg(test)]
    wakes: Arc<AtomicU64>,
    worker_clock: Arc<MaintenanceWorkerClock>,
}

#[derive(Debug, Default)]
struct MaintenanceWorkerClockState {
    #[cfg(any(test, feature = "test-seams"))]
    held: bool,
    #[cfg(any(test, feature = "test-seams"))]
    requested_serial: u64,
    #[cfg(any(test, feature = "test-seams"))]
    completed_serial: u64,
    #[cfg(any(test, feature = "test-seams"))]
    completed_thread_id: Option<thread::ThreadId>,
    #[cfg(any(test, feature = "test-seams"))]
    waiting_while_held: bool,
    #[cfg(any(test, feature = "test-seams"))]
    worker_exited: bool,
}

#[derive(Debug, Default)]
struct MaintenanceWorkerClock {
    state: Mutex<MaintenanceWorkerClockState>,
    changed: Condvar,
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMaintenanceWorkerCycleForTest {
    pub cycle_serial: u64,
    pub worker_thread_id: thread::ThreadId,
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
pub struct VectorMaintenanceWorkerClockGuardForTest {
    clock: Arc<MaintenanceWorkerClock>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PruningReport {
    pub pruned_rows: u64,
    pub blocked_count: u64,
    pub blocked: Vec<String>,
    /// An estimate of retained memory released by this cycle. A file-backed
    /// writer counts retired row and log ownership; durable payloads that were
    /// never resident do not contribute memory reclamation. Vector and edge
    /// reclamation follow their retained owners. This is an accounting figure,
    /// not the number of bytes returned to the filesystem: freed pages are
    /// reused unless compaction runs. `file_bytes_before` / `file_bytes_after`
    /// carry the physical sizes.
    pub reclaimed_bytes: u64,
    /// The database file's size before and after this cycle; `None` for an
    /// in-memory database, which has no file.
    pub file_bytes_before: Option<u64>,
    pub file_bytes_after: Option<u64>,
    /// Whether the file itself actually got smaller. Freed pages are reused in
    /// place, so a prune that does not compact frees space WITHOUT shrinking
    /// the file — this field says which happened.
    pub file_shrank: bool,
    /// Whether this cycle advanced durable storage compaction. A bounded
    /// sweep may require subsequent maintenance cycles to finish.
    pub compacted: bool,
    /// The share of the file a tidy-up could give back by moving pages
    /// (interior free pages over the pages a trim would leave), as it stood
    /// BEFORE this cycle pruned anything: accumulated waste that survived page
    /// reuse from earlier cycles, not the space this prune just released. It
    /// counts whole free pages, the only space a relocation can give back, not
    /// unused bytes inside live pages, and it is no promise that this fraction
    /// of the file can be returned at once. `None` (unknown) when nothing was
    /// measured -- the allocator's totals were untrusted or the store is
    /// in-memory -- and then no compaction decision is taken. It is the
    /// value the compaction decision was taken on, so a cycle that pruned rows
    /// compacted exactly when this reached the declared start ratio
    /// ([`StorageCompactionPolicy::start_ratio`], 0.5 by default). A cycle that
    /// pruned NOTHING never compacts, whatever this reads.
    pub fragmentation_before: Option<f64>,
    /// The storage compaction policy that governed this cycle's compaction
    /// decision: each value's field is `None` when undeclared, and the
    /// default then applied.
    pub policy: StorageCompactionPolicy,
    /// Rows carrying a timestamp further into the future than
    /// [`RETENTION_CLOCK_SKEW_TOLERANCE`]. They are never pruned early — and
    /// never silently pinned either: every cycle they stay future-dated counts
    /// them here and names their table below.
    pub future_dated_rows: u64,
    pub future_dated_tables: Vec<String>,
    /// Commit-index entries this cycle removed. The index holds one entry per
    /// commit and nothing used to remove them, so it grew with every write
    /// forever; retention now trims it to what a consumer can still name.
    pub pruned_commit_index_entries: u64,
    /// Expired rows this cycle left physically in place because a REGISTERED
    /// READER managed independently of an ordinary SQL statement — a
    /// caller-held snapshot pin or a bounded reader, including a suspended
    /// cursor — can still resolve to them. They are not blocked and not lost:
    /// the next cycle that runs with no reader holding them reclaims them.
    /// Without this count a cycle that deferred everything reads exactly like
    /// a cycle with nothing to reclaim, so an operator watching disk that is
    /// not coming back cannot tell that closing a reader is what would move
    /// it. Counted from the rows the pass actually held back, never from a
    /// configured expectation.
    pub rows_deferred_for_readers: u64,
}

/// What the engine-owned maintenance loop is doing for this database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MaintenanceStatus {
    /// Exactly `active_maintenance_loops > 0`.
    pub running: bool,
    /// This database holds at least one table that declares RETAIN.
    pub retention_enabled: bool,
    /// This database holds at least one table that declares `HISTORY
    /// CURRENT ONLY` — declaration-driven, not a fixed table-name list.
    pub currency_compaction_enabled: bool,
    /// Loops spawned for this database. Both jobs share ONE loop, so this is
    /// 1 whenever anything is maintained and 0 when nothing is.
    pub active_maintenance_loops: usize,
    /// Whether this database's maintenance is engine-owned (the default) or
    /// handed to the caller — see [`MaintenancePolicy`].
    pub policy: MaintenancePolicy,
}

/// Whether a database's maintenance loop is owned by the engine or handed to
/// the caller.
///
/// `EngineOwned` (the default) is what every database gets until a caller
/// asks otherwise: the engine spawns its own background thread the moment
/// anything is declared (a `RETAIN` window, `HISTORY CURRENT ONLY`, a
/// durable trigger-audit history, or maintained vector work), and stops it
/// when nothing remains to maintain. Initial vector construction and repair
/// advance through this maintenance path, outside foreground declarations,
/// writes, open, and queries; none waits for a full graph build.
///
/// `CallerDriven` hands the schedule to the host: the engine spawns ZERO
/// threads for this database, however much is declared, and the caller is
/// expected to drive `Database::run_maintenance_cycle` itself. A caller that
/// declares something and then never calls it does not fail silently: the
/// engine warns once, lazily, the next time a commit touches a declared
/// table after a full tick interval has passed with no cycle call (there is
/// no watchdog thread to emit the warning otherwise — `CallerDriven` means
/// zero threads, not "zero threads except a warning one").
///
/// This is engine OPEN-CONFIG, exactly like [`Database::trigger_audit_
/// retention`]: it lives on this in-process handle only, is never written to
/// the redb config store, and is never a DDL surface or a transported value.
/// A fresh handle reopened on the same path with no call to the setter is
/// `EngineOwned` again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaintenancePolicy {
    #[default]
    EngineOwned,
    CallerDriven,
}

/// Everything one maintenance cycle did: generic retention, currency version
/// compaction, maintained vector-index progress, and the engine's own durable
/// trigger-audit retention.
#[derive(Debug, Clone, Default)]
pub struct MaintenanceReport {
    pub pruning: PruningReport,
    pub currency: CurrencyCompactionReport,
    pub vector: VectorMaintenanceReport,
    pub pruned_trigger_audit_rows: u64,
    /// The rare, interval-gated automatic compaction attempt this cycle made
    /// (see [`AUTO_COMPACT_MIN_INTERVAL`]) — `ran=false` on almost every
    /// cycle. Separate from, and never counted in, `currency.redb_compacted`
    /// / `pruning.compacted`, which now describe ONLY what those two scoped
    /// passes decided on their own (currency never compacts at all;
    /// retention keeps its existing per-cycle decision, unchanged).
    pub compaction: CompactionReport,
}

/// An honest per-table size answer: an ESTIMATE of the table's live bytes plus
/// its exact row count, with the whole file's size reported ALONGSIDE as
/// itself. The whole-file number is never divided between tables — several
/// tables share one file, and attributing physical bytes to one of them would
/// be a claim the engine cannot make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSizeEstimate {
    pub table: String,
    pub estimated_live_bytes: u64,
    pub row_count: u64,
    pub whole_file_bytes: Option<u64>,
}

/// Every read snapshot currently entitled to resolve a version, keyed by a
/// registration token. Version cleanup consults EVERY registered snapshot
/// (not merely the lowest), while retention consults every reader-held
/// registration but not short-lived SQL statements. Each pass defers any
/// version visible to the registrations it samples rather than physically
/// removing it out from under a protected reader.
///
/// Three classes of caller register here:
/// - Every top-level statement execution (`execute`, `execute_in_tx`, and
///   `execute_at_snapshot` riding through `execute`) registers its own
///   snapshot for exactly that call's duration -- this closes the
///   mid-statement race with version cleanup: a long read resolving rows later
///   in the same statement can no longer have a superseded version vanish
///   underneath it.
/// - `Database::pin_snapshot` registers a snapshot for as long as the
///   returned guard lives, for a caller that holds a `SnapshotId` ACROSS
///   separate calls (the shape `execute_at_snapshot` alone cannot protect,
///   since a bare `SnapshotId` is a plain `Copy` value the engine has no way
///   to know is still wanted once a call returns).
/// - Bounded reads register independently of SQL statements; a suspended
///   cursor keeps that registration between pages for exactly its published
///   idle window.
struct SnapshotFloorRegistry {
    next_token: AtomicU64,
    /// Construction floors of unpublished paged vector builds: one registered
    /// snapshot per pending partition, restored from the durable build record
    /// before any cleanup and released when the build publishes or is
    /// abandoned. Removal passes defer to them like any reader-held pin.
    construction_floors: Mutex<HashMap<VectorPartitionRef, u64>>,
    /// The registered set AND the in-flight removal pass marker, under ONE
    /// lock -- a registration and a pass's registered-set/watermark sample
    /// can never interleave un-observed. (A previous design guarded these
    /// under two SEPARATE `Mutex`es and only claimed to share one; a
    /// `register` between the pass's sample and its persisted removal could
    /// land un-accounted-for and then return, promising protection the
    /// SAME in-flight pass was still free to violate.)
    state: Arc<Mutex<SnapshotFloorState>>,
    pin_slots: Mutex<Option<Arc<SnapshotPinPool>>>,
    pending_delete_source: Mutex<Option<Arc<Mutex<resident_pending::ResidentPendingSource>>>>,
    /// Wakes a `register` call blocked in the `while` loop below when a
    /// pass's `ActiveRemovalPassGuard` drops.
    pass_finished: Arc<Condvar>,
    /// Test-only pause point between a removal pass's registered-set/
    /// watermark sample (via `begin_removal_pass`) and its persisted
    /// removal -- armed only by
    /// `Database::pause_after_currency_floor_sample_for_test`; the
    /// production path's `maybe_pause` reads state that is never armed
    /// outside a test, so it is an immediate no-op there. Same
    /// `ApplyPhasePause`/`ApplyPhasePauseGuard` shape as the sync-apply
    /// pause seams -- real cross-thread wait/release is needed here (a
    /// `pin_snapshot` call on another thread must observe the pass as
    /// genuinely in progress), which a same-thread injection flag alone
    /// cannot provide.
    removal_pass_test_pause: Arc<ApplyPhasePause>,
    #[cfg(any(test, feature = "test-seams"))]
    pull_capture_test_pause: Arc<ApplyPhasePause>,
    /// Production-dead interleaving seam: the durable read has captured its
    /// root but has not yet installed its registry hold. The commit lock must
    /// still be held here for both pulls and cursor openings.
    #[cfg(test)]
    durable_admission_test_pause: Arc<ApplyPhasePause>,
    #[cfg(test)]
    owner_cursor_busy_test_pause: Arc<ApplyPhasePause>,
}

/// Fixed, open-time-funded capacity for public caller-held snapshot pins.
struct SnapshotPinPool {
    available: AtomicUsize,
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    slots: usize,
    slot_bytes: usize,
    _credit: crate::memory_accounting::ControlCredit,
}

enum SnapshotPinSlot {
    Prepaid(Arc<SnapshotPinPool>),
    Reader {
        _credit: contextdb_core::read_memory::ReadCredit,
    },
}

/// One registered read snapshot, plus whatever the holder published about
/// how long it stays entitled to that snapshot. A statement's registration,
/// a caller-held pin, and an active bounded read carry no window: they last
/// exactly as long as the guard does. A suspended bounded cursor carries its
/// idle window, because the engine already refuses that cursor's next page
/// once the window has passed -- see [`CursorIdleWindow`].
struct RegisteredSnapshot {
    snapshot: SnapshotId,
    /// SQL statements own the rows they scan before completing the rest of
    /// their read, so retention does not let a continuous stream of those
    /// short-lived registrations pin every expired row. Reader-held
    /// registrations remain authoritative to retention as well as version
    /// cleanup.
    class: SnapshotRegistrationClass,
    /// `None` for a registration that stays entitled for as long as it is
    /// registered; `Some` for one whose holder has an idle deadline of its
    /// own.
    idle_window: Option<Arc<CursorIdleWindow>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SnapshotRegistrationClass {
    Statement,
    ReaderHeld,
    PullHistory { through: Lsn },
}

/// The idle window a suspended bounded cursor publishes to removal, read
/// off the SAME clock and the SAME `cursor_idle_ms` limit the cursor's own
/// next-page check uses, so both answer "is this reader still entitled?"
/// identically. The cursor stamps [`Self::touch`] every time it hands a page
/// back; a reader that walks away stops stamping and falls out of the
/// registered set that removal defers to.
pub(crate) struct CursorIdleWindow {
    clock: Arc<dyn DeadlineClock>,
    idle_ms: u64,
    last_used_ms: AtomicU64,
}

mod durable_vector_loading;
use durable_vector_loading::*;

#[derive(Default)]
struct SnapshotFloorState {
    active: HashMap<u64, RegisteredSnapshot>,
    /// `Some(pass)` while a physical-removal pass is in flight;
    /// `None` means no pass is active. Set by `begin_removal_pass` the
    /// moment a pass samples its registered set and watermark; cleared (and
    /// `pass_finished` notified) only by `ActiveRemovalPassGuard`'s `Drop`
    /// -- covering every exit path (success, an early return, or a
    /// `?`-propagated error) uniformly, so a pass can never leave this
    /// marker stuck.
    active_pass: Option<ActiveRemovalPass>,
}

/// One in-flight removal pass's own sampled marker -- captured ONCE, at the
/// moment `begin_removal_pass` locks `SnapshotFloorState`, and never
/// updated for the rest of the pass. The registered-snapshot set itself is
/// NOT duplicated here: it lives solely on `ActiveRemovalPassGuard` (the
/// pass's own deferral loop reads it from there), so this marker carries
/// only what `register`'s blocking rule needs.
#[derive(Clone, Copy)]
struct ActiveRemovalPass {
    /// The highest-committed `TxId` at the moment this pass began
    /// (`Database::committed_watermark`, threaded in by the caller under
    /// the SAME commit-lock hold the whole pass runs inside -- see
    /// `compact_currency_versions_inner`). Used ONLY to decide whether a
    /// registration racing in DURING this pass needs to block -- see
    /// `SnapshotFloorRegistry::register`'s blocking rule.
    watermark: TxId,
}

/// RAII marker for one in-progress physical-removal pass -- see
/// `SnapshotFloorRegistry::begin_removal_pass` and
/// `SnapshotFloorRegistry::begin_retention_removal_pass`.
struct ActiveRemovalPassGuard {
    registry: Arc<SnapshotFloorRegistry>,
    /// This pass's own sorted snapshot of the registered set at the moment
    /// it began -- see `ActiveRemovalPass::registered_snapshots`. Exposed
    /// via `Self::registered_snapshots` so the pass's deferral loop
    /// consults the EXACT SAME set `register`'s blocking rule was computed
    /// against.
    registered_snapshots: Arc<[SnapshotId]>,
    history_through: Option<Lsn>,
}

/// RAII handle for one registered read snapshot. Unregisters on drop,
/// whether that is the end of one statement's execution or the caller
/// dropping a held [`SnapshotPin`].
struct SnapshotRegistration {
    registry: Arc<SnapshotFloorRegistry>,
    token: u64,
}

/// A statement-owned snapshot registration plus its same-thread nesting
/// marker. The inner registration remains live until this wrapper has removed
/// the token from the thread-local stack.
struct StatementSnapshotRegistration {
    registration: SnapshotRegistration,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

/// A read snapshot held across separate calls. While this guard lives,
/// retention and version cleanup defer any version still visible at the
/// pinned snapshot to a later cycle.
///
/// Without a pin, a `SnapshotId` obtained from [`Database::snapshot`] and
/// reused in a LATER [`Database::execute_at_snapshot`] call is invisible to
/// the engine between the two calls: a cleanup cycle that runs in between
/// may reclaim a version that snapshot was entitled to see, and the later
/// call then silently returns fewer rows than it should — never a wrong
/// value, never an error, just missing data. This is the documented boundary
/// on a table declaring `RETAIN` or `HISTORY CURRENT ONLY`; a table that
/// declares neither keeps every version, so it never arises there. Pin the
/// snapshot for as long as you intend to reuse it if the table matters.
pub struct SnapshotPin<'a> {
    _database: &'a Database,
    _registration: SnapshotRegistration,
    _slot: SnapshotPinSlot,
    _root: Option<crate::persistence::WriterSnapshotHandle>,
    // Last: retain the pin's operation child through root and slot destruction.
    _read_memory: contextdb_core::read_memory::ReadMemoryOwner,
}

/// Whether a maintenance cycle's currency pass runs unconditionally (an
/// explicit call) or behind the cheap superseded-version gate (a scheduled
/// tick, so a quiet node never pays the commit lock).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurrencyGate {
    Always,
    Scheduled,
}

/// Persistence-owned raw-body loader for one partition. The object keeps no
/// vector values and no stale membership snapshot: each load resolves the
/// partition's current checksummed directory, which means a committed write
/// can leave the same partition evictable without publishing a new loader.
struct DurableRawVectorPartitionLoader {
    source: DurableRawVectorSource,
    accountant: crate::memory_accounting::AccountantHandle,
    partition: VectorPartitionRef,
    dimension: usize,
    quantization: VectorQuantization,
    metadata_copy_owner: Option<Arc<contextdb_vector::VectorCopyAdmission>>,
}

#[derive(Clone)]
enum DurableRawVectorSource {
    Writable(Arc<RedbPersistence>),
    CommittedImage(crate::persistence::RetainedReadSource),
}

/// One pre2-sized whole closure capture. The accountant word also retains
/// the selected writer epoch until persistence and reader-owner cleanup ends.
struct LoaderCallbackCaptures {
    persistence: std::mem::ManuallyDrop<Arc<RedbPersistence>>,
    captured_owner: std::mem::ManuallyDrop<Arc<contextdb_vector::VectorCopyAdmission>>,
    accountant: std::mem::ManuallyDrop<crate::memory_accounting::StoreAccountantHandle>,
}
struct LoaderCallbackCleanup {
    source: *mut LoaderCallbackCaptures,
    next: u8,
}

/// Persistence-owned loader attached to one lightweight generation
/// descriptor. Opening a database registers this object without reading graph
/// bytes. The first ordinary indexed read reserves the complete decode and
/// replay working set, verifies the saved graph, and replays only journal
/// entries after the selected sealed boundary.
struct DurableVectorGenerationLoader {
    source: DurableVectorGenerationSource,
    accountant: crate::memory_accounting::AccountantHandle,
    partition: VectorPartitionRef,
    descriptor: VectorPartitionGenerationDescriptor,
    base_generation_id: u64,
    is_base: bool,
    /// The base generation's coverage as the catalog names it; a change
    /// layer's saved record is checked against it.
    base_covered: (TxId, Lsn),
    dimension: usize,
    quantization: VectorQuantization,
    /// Only the final selected sealed layer owns fresh-tail replay. A base
    /// with a selected change layer leaves this `None`; the change loader owns
    /// the suffix instead.
    tail_covered_lsn: Option<Lsn>,
    layout: VectorIndexLayout,
    live_rows: usize,
    /// Where this loader reads the declared serving-cache size.
    ///
    /// A writable store carries the live cell, so a partition faulted in after
    /// `SET VECTOR_SERVING_CACHE` sizes from the value in force at LOAD time —
    /// the same moment the accountant's limit beside it is read. A committed
    /// image carries the value as of its open, which is what that reader is
    /// entitled to and cannot change under it.
    declared_serving_cache: DeclaredServingCache,
}

/// Where a loader reads the declared serving-cache size from.
#[derive(Clone)]
enum DeclaredServingCache {
    /// The store's live cell; read at load time.
    Live(Arc<AtomicU64>),
    /// The value as of a committed image's open.
    AsOfOpen(Option<usize>),
}

#[derive(Clone)]
enum DurableVectorGenerationSource {
    Writable(Arc<RedbPersistence>),
    CommittedImage(ReadOnlyVectorGenerationSource),
    /// One serving operation's already captured root: the exact read, and
    /// the writer snapshot or committed source that owns it. Every record it
    /// reads comes from that read; it never begins another one.
    CapturedRoot(Arc<vector_captured_serving::CapturedVectorRootLease>),
}

mod maintenance_runtime;
pub use maintenance_runtime::*;

/// What one disclosed partition costs an EXPLAIN before its entries are
/// pushed: its admitted figures, its AUTO price and its disclosure entry,
/// doubled for `Vec` growth, plus its three short text fields (redacted
/// label, reason and policy source).
const EXPLAIN_PARTITION_DISCLOSURE_BYTES: usize = 2
    * (std::mem::size_of::<(usize, usize, Option<VectorServingPolicy>)>()
        + std::mem::size_of::<contextdb_vector::mem::AutoPartitionCost>()
        + std::mem::size_of::<VectorPartitionHnswDisclosure>())
    + 128;

struct MetadataAuthorizedCandidates {
    ids: Option<RoaringTreemap>,
    _memory: contextdb_core::read_memory::ReadCredit,
}

// The shared builder accepts an explicit typed writer or independent read
// image. A raw accountant cannot assemble a writable database.
enum DatabaseAssemblyBudget {
    Writer(WriterBudget),
    ReadImage(crate::memory_accounting::AccountantHandle),
}

// Linux x86-64 glibc 2.39 census, 1/4/16 workers and three waves: max joined
// mapped-byte delta per worker is 71,307,264 B = 64 MiB arena + 4 MiB cached
// stack + 4096 B guard. This includes the full arena mapping, not only RSS or
// live malloc objects. Add the whole requested live stack separately. Native
// qualification must reconcile the complete process with the writer ceiling.
const RESIDENT_INSERTION_RUNTIME_RETAINED_PER_WORKER: usize = 71_307_264;

const VECTOR_GENERATION_PERSISTENCE_COPIES: usize = 6;

#[derive(Debug, Clone, Copy)]
struct VectorGenerationWorkspacePlan {
    graph_build_bytes: usize,
    insertion_workers: usize,
    insertion_worker_bytes: usize,
    raw_entry_bytes: usize,
    raw_decode_scratch_bytes: usize,
    selected_entry_bytes: usize,
    encoded_graph_bound: usize,
    encoding_scratch_bytes: usize,
    prior_base_bytes: usize,
    durable_metadata_bytes: usize,
    digest_reference_bytes: usize,
    carrier_metadata_bytes: usize,
}

#[derive(Default)]
struct RetentionDeferredEdges {
    nodes: HashSet<NodeId>,
    // The published carry-over owns its hash buckets until replacement/drop.
    _memory: Option<crate::memory_accounting::OwnedMemoryReservation>,
}

/// The engine-owned pieces one maintenance cycle needs, cloned out of the
/// `Database` handle so the SAME cycle code runs whether a caller drives it
/// synchronously (`Database::run_maintenance_cycle`) or the engine's own loop
/// dispatches it on a tick.
pub(crate) struct MaintenanceContext {
    /// The store declaration, independent of a request choosing a lower limit.
    reader_memory_limit: u64,
    read_request_provider: crate::vector_read_memory::ReadRequestProvider,
    #[cfg(any(test, feature = "test-seams"))]
    writer_receipt_observer: Option<Arc<Database>>,
    disk_limit: Arc<AtomicU64>,
    vector_serving_cache_bytes: Arc<AtomicU64>,
    resource_closed: Arc<AtomicBool>,
    publication_interrupted: Arc<AtomicBool>,
    worker_stop: Option<Arc<AtomicBool>>,
    relational: Arc<RelationalStore>,
    graph: Arc<GraphStore>,
    vector: Arc<VectorStore>,
    persistence: Option<Arc<RedbPersistence>>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    change_log_table_index: Arc<RwLock<ChangeLogTableIndex>>,
    change_log_lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
    trigger: Arc<trigger::TriggerState>,
    tx_mgr: DatabaseTransactionHost,
    sync_watermark: Arc<AtomicLsn>,
    /// Shared with `Database::trigger_audit_retention_secs` (same `Arc`,
    /// cloned at context-build time) and re-read on every
    /// `run_trigger_audit_retention` call, so a declared override reaches
    /// this already-spawned loop's next tick -- not only a loop spawned
    /// after the setter runs. See that field's doc comment.
    trigger_audit_retention_secs: Arc<AtomicU64>,
    snapshot_registry: Arc<SnapshotFloorRegistry>,
    /// Shared with the `Database` handle (same `Arc`) -- see that field's
    /// doc comment.
    retention_deferred_edge_nodes: Arc<Mutex<RetentionDeferredEdges>>,
    last_maintenance_cycle_at: Arc<Mutex<Option<std::time::Instant>>>,
    last_auto_compact_at: Arc<Mutex<Option<std::time::Instant>>>,
    storage_compaction_policy: Arc<Mutex<crate::database::StorageCompactionState>>,
    /// Shared with the `Database` handle (same `Arc`) -- see that field's
    /// doc comment.
    #[cfg(any(test, feature = "test-seams"))]
    snapshot_free_retirement_pass_count: Arc<AtomicUsize>,
    finalization: Arc<StoreFinalization>,
    pending_local_schema_stages: Arc<Mutex<schema_stage_registry::PendingRegistry>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct VectorPartitionPublicationOutcome {
    published: bool,
    cleanup_failure: Option<VectorMaintenanceFailureDetails>,
}

#[derive(Debug, Default)]
struct VectorJournalCleanupRetryOutcome {
    first_failure_details: Option<VectorMaintenanceFailureDetails>,
    failed_partitions: FailedMaintenancePartitions,
}

struct MaintenanceCleanup {
    source: *mut MaintenanceContext,
    initialized: [bool; 27],
}

/// Database-metadata key holding the ONE hub a retained table is delivered to.
pub(crate) const RETENTION_SYNC_PEER_CONFIG_KEY: &str = "retention_sync_peer";
const DESTINATION_REUPLOAD_TARGET_CONFIG_NAME: &str = "sync_destination_reupload_target";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DestinationReuploadEpoch {
    /// Fresh identity for this explicit move. The hub name and held LSN can
    /// repeat (H -> X -> H with no local writes), so neither is an epoch
    /// identity that a delayed completion may safely retire.
    #[serde(default = "uuid::Uuid::nil")]
    epoch_id: uuid::Uuid,
    hub_node_id: String,
    held_through_lsn: Lsn,
    /// Retained only to decode epochs written before receiver-local DDL
    /// arrival evidence existed. Destination filtering now uses that evidence
    /// and the held frontier, rather than treating this snapshot as policy.
    #[serde(default)]
    held_ddl: Vec<(Lsn, Vec<u8>)>,
}

thread_local! {
    /// Test-only injection seam: when armed, the NEXT
    /// [`Database::change_retention_sync_peer`] on this thread fails at the point
    /// after the confirmation record is cleared, so a test can assert the
    /// operation lands on the SAFE side of a mid-operation I/O failure. Armed
    /// only by `__arm_retention_peer_persist_fault_for_test`; default off, so the
    /// production path reads a thread-local that is never set — production-dead,
    /// following the existing `__inject_*_for_test` precedent.
    static RETENTION_PEER_PERSIST_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

    /// Test-only injection seam, same shape as
    /// [`RETENTION_PEER_PERSIST_FAULT`]: when armed, the NEXT version-cleanup
    /// pass on this thread fails immediately before durable removal and before
    /// memory publication, so a test can assert that a failed cycle retains
    /// the complete removal set for retry. Armed only by
    /// `Database::__arm_currency_compaction_persist_fault_for_test`; default
    /// off, so production reads a thread-local that is never set.
    static CURRENCY_COMPACTION_PERSIST_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

    /// Test-only race seam: when armed, `execute`/`execute_in_tx` close
    /// their own database synchronously in the gap between the
    /// `already_closed` pre-check and the fallible snapshot registration
    /// just after it -- deterministically simulating a close that lands in
    /// that exact window, on the SAME thread, no real concurrency needed.
    /// Lets a test prove the registration's own fallible form catches a
    /// close in this window instead of panicking, without depending on a
    /// second thread ever winning a real race. Armed only by
    /// `Database::__arm_close_in_snapshot_registration_race_window_for_test`;
    /// default off.
    static CLOSE_IN_SNAPSHOT_REGISTRATION_RACE_WINDOW: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

    /// Test-only, THREAD-LOCAL pause selector: when set on the calling
    /// thread, that thread's NEXT sync-apply pre-commit checkpoint
    /// (`sync_apply_pre_commit_pause`) actually pauses; a thread that never
    /// sets this reads `false` and passes the SAME checkpoint unconditionally
    /// -- even while the shared `ApplyPhasePause` is globally armed. This is
    /// what lets a test pause exactly one concurrent sync apply while a
    /// second, independent apply on another thread commits normally, proving
    /// an ordering race deterministically. Armed only by
    /// `Database::mark_this_thread_for_sync_apply_pre_commit_pause_for_test`,
    /// consumed (reset to `false`) the moment the checkpoint reads it;
    /// default off, so production reads a thread-local that is never set.
    static SYNC_APPLY_PRE_COMMIT_PAUSE_ARMED_HERE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    #[cfg(test)]
    static QUERY_TRACE_ROWS_EXAMINED_CAPTURE_PAUSE_ARMED_HERE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Version-cleanup eligibility is DECLARED per table (`HISTORY CURRENT ONLY`),
// not a hardcoded table-name list -- see `Database::version_cleanup_eligible_
// tables`/`has_version_cleanup_tables`. High-churn tables rewritten every poll
// cadence via `INSERT … ON CONFLICT DO UPDATE` mint a new MVCC version and
// tombstone the prior one on every write, and nothing reclaims the superseded
// versions of a table that declares nothing, so a long-running fleet node can
// accumulate hundreds of thousands of physical versions for a handful of live
// rows (observed: 248,996 `work_capabilities` versions in a 365MB debris
// ledger for ~10 live rows). Version cleanup (`Database::compact_currency_
// versions`) collapses each logical row of a DECLARED table back to its
// single current version. The three built-in currency tables (`work_
// capabilities`, `peer_directory`, `work_node_contacts`) declare `HISTORY
// CURRENT ONLY` in their own `CREATE TABLE` text now, alongside the `SYNC
// CONFLICT KEEP LATEST` (or `SYNC OFF`) that made reclaiming their history
// safe in the first place — see the sync-safety argument on
// `compact_currency_versions`.

/// Reclaim freed pages with a full redb `compact()` after a version-compaction
/// pass once at least this fraction of the file is dead space. A first pass over
/// an accumulated debris ledger frees most of the file and trips this; steady
/// state stays well below it (freed pages are reused in place), so compaction is
/// rare, not per-cycle.
pub const REDB_COMPACT_FRAGMENTATION_THRESHOLD: f64 = 0.5;

/// Everything one storage-compaction call produced: how long its work took, the file's size before and after, whether it
/// actually shrank, and the fragmentation ratio that led to it. `ran=false`
/// (every other field at its zero value) means nothing happened this call —
/// either there is no file to compact (an in-memory database) or, for the
/// gated automatic path (see [`AUTO_COMPACT_MIN_INTERVAL`]), the threshold
/// and the minimum interval were not both satisfied and no sweep was active.
///
/// Explicit compaction drains the whole file. Automatic compaction advances
/// bounded durable batches and releases foreground access between them. Both
/// remain separate from the scoped cleanup passes ([`CurrencyCompactionReport`],
/// [`PruningReport`]) that free the pages they reclaim.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CompactionReport {
    pub ran: bool,
    pub duration_micros: u64,
    pub bytes_before: Option<u64>,
    pub bytes_after: Option<u64>,
    pub file_shrank: bool,
    /// The share of the file a tidy-up could give back by moving pages: the
    /// allocator's whole free pages over the pages a trim would leave. Both
    /// paths report this one quantity; the automatic path compares it with the
    /// declared start ratio (0.5 by default). `None` (unknown) when nothing was
    /// measured: no decision was due, the allocator's totals were untrusted,
    /// the measurement failed, or the store has no file. Every later batch of a
    /// sweep reports the measurement that sweep started on, unknown included.
    pub fragmentation_before: Option<f64>,
    /// Explicit compaction only: the share of the bytes inside live B-tree
    /// pages that hold no data (in-page slack), from the exact storage
    /// statistics walk. Relocation moves whole pages and does not repack
    /// them, so this is NOT space a compaction gives back; it says how much a
    /// repack could. `None` on the automatic path and when the walk failed.
    pub page_slack_ratio: Option<f64>,
    /// The storage compaction policy that governed this call: each value's
    /// field is `None` when undeclared, and the default then applied.
    pub policy: StorageCompactionPolicy,
    /// Whether the redb handle was closed and reopened as part of this
    /// compaction — see `RedbPersistence::compact`'s doc comment for why
    /// this is bundled into every compaction rather than left optional: a
    /// file-level compact alone does not release the in-process
    /// allocator/region bookkeeping redb retains from a database's
    /// historical peak size, so it does not by itself restore steady-state
    /// write cost, only file size. Automatic batches keep the handle until
    /// their sweep completes; intermediate batches therefore report `false`.
    pub handle_recycled: bool,
    /// The recycle's own duration in microseconds, reported separately from
    /// `duration_micros` (which covers the whole compaction, recycle
    /// included) — the same convention this bench and the maintenance
    /// receipts already use for every compaction-adjacent cost.
    pub handle_recycle_micros: u64,
}

/// Minimum time between AUTOMATIC redb compactions the engine-owned
/// maintenance tick can trigger on its own. Currency-table version cleanup
/// (`compact_currency_versions`/`Database::run_maintenance_cycle`) NEVER
/// triggers `persistence.compact()` itself — a scoped, O(pruned) cleanup
/// pass and an O(whole-file) compaction are different-shaped costs, and
/// coupling them made steady-state churn on a small file (where routine
/// superseded-version debris is a large fraction of a small file, so the
/// SAME conservative [`REDB_COMPACT_FRAGMENTATION_THRESHOLD`] crosses on
/// every cycle) retrigger a full-file rewrite every tick — the exact
/// per-cycle-compaction cost the version-cleanup scaling bench measures and
/// bounds separately from the scoped pass's own cost. An operator who wants
/// compaction NOW has [`Database::compact_now`] (`.maintenance compact`),
/// which has no interval gate at all; this constant only throttles the
/// engine's OWN unattended background attempt.
pub const AUTO_COMPACT_MIN_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Explicit compaction ([`Database::compact_now`]) repeats its sweep while
/// more than this fraction of the file that a trim would leave is interior
/// free pages and the previous sweep still shrank the file. The default of
/// the declarable `STOP_RATIO`.
pub const EXPLICIT_COMPACTION_TIGHT_RATIO: f64 = 0.05;
/// The most sweeps one explicit compaction runs. The default of the
/// declarable `MAX_SWEEPS`.
pub const EXPLICIT_COMPACTION_MAX_SWEEPS: u32 = 8;

/// Local configuration key of the declared [`StorageCompactionPolicy`].
pub(crate) const STORAGE_COMPACTION_CONFIG_KEY: &str = "storage_compaction_policy";

/// The storage compaction settings a store declared with
/// `SET STORAGE_COMPACTION`. A field left `None` is undeclared and uses
/// today's default, which `SHOW STORAGE_COMPACTION` reports with source
/// `default`. Local store configuration: persisted with the store, never
/// part of the schema and never synced.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct StorageCompactionPolicy {
    /// Reclaimable share of the file at which automatic and pruning
    /// compaction start ([`REDB_COMPACT_FRAGMENTATION_THRESHOLD`] by default),
    /// greater than 0 and at most 1.
    pub start_ratio: Option<f64>,
    /// Minimum time between automatic compaction attempts, in milliseconds
    /// ([`AUTO_COMPACT_MIN_INTERVAL`] by default): positive, and at most
    /// `i64::MAX` so `SHOW STORAGE_COMPACTION` reports it exactly.
    pub min_interval_ms: Option<u64>,
    /// Explicit compaction stops sweeping once the reclaimable share is below
    /// this ([`EXPLICIT_COMPACTION_TIGHT_RATIO`] by default), greater than 0
    /// and at most 1. The same ratio bounds every growth of the file: one
    /// growth adds at most this share of its usable size, rounded down, or
    /// twice the allocation that needs it when that is larger; page and
    /// region-header rounding come on top, and a declared disk limit still
    /// refuses growth past it.
    pub stop_ratio: Option<f64>,
    /// The most sweeps one explicit compaction runs
    /// ([`EXPLICIT_COMPACTION_MAX_SWEEPS`] by default).
    pub max_sweeps: Option<u32>,
}

/// One storage compaction setting's value: a share of the file, or a count
/// (milliseconds or sweeps).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageCompactionValue {
    Ratio(f64),
    Count(u64),
}

/// The declared policy shared by every handle onto one database, plus the
/// test-only override of the automatic interval and the test-only update
/// seams.
#[derive(Debug, Default)]
pub(crate) struct StorageCompactionState {
    pub(crate) declared: StorageCompactionPolicy,
    pub(crate) min_interval_override: Option<Duration>,
    #[cfg(any(test, feature = "test-seams"))]
    update_pause: Arc<ApplyPhasePause>,
    #[cfg(any(test, feature = "test-seams"))]
    update_probe: Arc<MaintenancePollIntervalUpdateProbe>,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Automatic compaction checks that read the free-page measurement on
    /// this thread. Production-dead.
    static AUTOMATIC_COMPACTION_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Sweeps the last explicit compaction on this thread ran. Production-dead.
    static EXPLICIT_COMPACTION_SWEEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// How far this thread's compaction clock runs ahead of the monotonic
    /// clock. Production-dead.
    static COMPACTION_CLOCK_ADVANCE: std::cell::Cell<Duration> = const { std::cell::Cell::new(Duration::ZERO) };
    /// The next explicit compaction on this thread fails its exact
    /// measurement. Production-dead.
    static FAIL_EXACT_FRAGMENTATION_MEASUREMENT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Storage compaction batches this thread may still publish before one is
    /// refused. Production-dead.
    static STORAGE_COMPACTION_BATCHES_BEFORE_REFUSAL: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
}

/// A worker whose clock runs ahead of the holder's stamps its rows in the
/// future. Drift within this much is ordinary fleet skew: the row ages
/// normally and is never reported. Beyond it the stamp is an operator event —
/// the row is never deleted early, and every maintenance cycle counts it and
/// names its table until the clocks agree again.
pub const RETENTION_CLOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(5 * 60);

/// The shipped default retention for the engine's OWN durable trigger-audit
/// history, applied with zero consumer configuration. Generous enough that a
/// real operator still has a week of firing history to read, bounded enough
/// that a trigger-heavy node cannot grow the table without limit. Audit rows
/// orphaned by a DROP TABLE age out on the same window.
pub(crate) const TRIGGER_AUDIT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The engine-owned maintenance loop ticks at this cadence. One loop serves
/// both jobs: its currency pass is gated by
/// [`CURRENCY_COMPACTION_SUPERSEDED_THRESHOLD`], so a quiet node does near-zero
/// work, and only a tick that actually did something prints a receipt line.
pub(crate) const MAINTENANCE_TICK: Duration = Duration::from_secs(60);
/// Upper bound on bounded reclaim steps one maintenance cycle performs.
const RECLAIM_STEPS_PER_CYCLE: usize = 64;

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Test-only override of the per-cycle reclaim step limit for cycles run on this thread.
    static RECLAIM_STEPS_PER_CYCLE_FOR_TEST: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    /// Durable reclaim steps the last reclaim stage on this thread committed.
    static RECLAIM_STEPS_LAST_CYCLE_FOR_TEST: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}
pub(crate) const MAINTENANCE_POLL_INTERVAL_CONFIG_KEY: &str = "maintenance_poll_interval_ms";
pub(crate) const VECTOR_SERVING_CACHE_CONFIG_KEY: &str = "vector_serving_cache_bytes";

/// A maintenance tick compacts only once superseded versions across the eligible
/// tables reach this count, so steady-state churn below it never pays the
/// commit-lock/rewrite cost.
pub(crate) const CURRENCY_COMPACTION_SUPERSEDED_THRESHOLD: usize = 64;

/// Outcome of one `compact_currency_versions` pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CurrencyCompactionReport {
    /// Superseded relational row versions physically removed.
    pub pruned_versions: u64,
    /// Change-log entries dropped in lockstep with those versions.
    pub pruned_change_log_entries: u64,
    /// Estimated in-memory row bytes released by the pruned versions.
    pub reclaimed_bytes: u64,
    /// The eligible tables that actually had superseded versions pruned, or
    /// a deleted row's vector copy reclaimed.
    pub compacted_tables: Vec<String>,
    /// Whether a redb file compaction ran to reclaim the freed pages.
    pub redb_compacted: bool,
    /// Wholesale survivor rewrites. Scoped cleanup leaves this at zero;
    /// it is not a count of all I/O. Affected change-log LSN groups are still
    /// read and densely rewritten when selected references are removed.
    pub keys_rewritten: u64,
    /// Selected durable vector bodies removed, despite the historical field
    /// name. This excludes unrelated vectors and is zero for in-memory stores.
    pub vector_keys_rewritten: u64,
    /// Physical adjacency keys rewritten. Currency cleanup leaves this at zero:
    /// graph edges have their own identities, independent of row versions.
    pub edge_keys_rewritten: u64,
    /// Wall-clock microseconds this pass held the commit lock -- the writer
    /// stall a concurrent commit waits behind. Timed from the FIRST
    /// instruction inside the `with_commit_lock` closure in
    /// `MaintenanceContext::run_currency`/`run_currency_for_tables` (i.e.
    /// from the moment the lock is actually granted), deliberately excluding
    /// however long this thread waited to acquire it -- a busy database
    /// timed from before the call would inflate this number with queueing
    /// delay that has nothing to do with what the pass itself cost.
    pub commit_lock_hold_micros: u64,
    /// Wall-clock microseconds the follow-on redb file compaction took, when
    /// one ran (`redb_compacted`) -- reported SEPARATELY from
    /// `commit_lock_hold_micros` on purpose: compaction runs OUTSIDE the
    /// commit lock (redb's compact needs `&mut` and must not run while a
    /// txn is outstanding), so it is not writer-stall time, and conflating
    /// the two would misreport the first pass over an accumulated debris
    /// ledger as a single, much larger writer stall than it actually was.
    /// 0 whenever `redb_compacted` is false.
    pub redb_compact_micros: u64,
    /// Eligible tables whose row histories were sampled in memory. Unrelated
    /// tables are not cloned into the cleanup selection.
    pub tables_in_memory_snapshot: u64,
    /// Commit-index entries removed after losing their last change-log reference,
    /// excluding the anchor needed to resolve the oldest surviving delete.
    /// Durable removal joins the row/vector cleanup transaction; this count
    /// records the matching in-memory removal, including on in-memory stores.
    pub commit_index_keys_removed: u64,
    /// Superseded versions left in place this cycle because a registered
    /// reader's snapshot (an in-flight statement, or a caller-held
    /// `SnapshotPin`) can still resolve to them. Reclaimed on a later cycle
    /// once no registered reader still needs them.
    pub versions_deferred_for_readers: u64,
    /// Set when this pass's own content-identity proof-maintenance charge
    /// (transient bookkeeping work layered on top of the actual reclaim) was
    /// refused by the memory or disk budget. The refusal is recovered from,
    /// not propagated: the durable and in-memory removal this pass exists
    /// for still completes, admitted unaccounted for that one already-
    /// refused attempt, so an essential currency cleanup never wedges the
    /// store merely because its own bookkeeping overhead briefly outgrew a
    /// tight budget. `None` on the ordinary accounted path. The charge
    /// itself is never removed -- the accounted attempt is always tried
    /// first, every pass.
    pub first_failure: Option<VectorMaintenanceFailure>,
    /// Safe typed context for `first_failure`, in the same shape
    /// `VectorMaintenanceReport` already reports vector-side refusals in.
    pub first_failure_details: Option<VectorMaintenanceFailureDetails>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FkProbeStats {
    pub indexed_tuple_probes: u64,
    pub full_scan_fallbacks: u64,
}

/// Per-stage commit work counters. `#[doc(hidden)]` test-introspection surface
/// in the `FkProbeStats` precedent pattern. Stage order matches `commit_validate`:
/// conditional-update revalidation, UNIQUE / commit-time upsert rewrite validation,
/// FK, composite-FK, vector-schema revalidation, store apply / index maintenance,
/// and trigger-audit / event projection staging.
///
/// Tier 1 - always-on integer work counters (the acceptance arms assert on these).
/// Tier 2 - per-stage cumulative wall time, only populated under the `test-seams`
/// feature; `None` on default builds so non-feature builds compile and the field
/// is never load-bearing for correctness.
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitStageStats {
    /// Rows whose constraints were validated, summed across the validated stages.
    pub rows_validated: u64,
    /// Indexed probes issued by the validated stages (UNIQUE / FK / composite-FK).
    pub indexed_probes: u64,
    /// Staged-row-vs-staged-row comparisons performed across all validated stages.
    pub staged_vs_staged_comparisons: u64,
    /// Rows touched by full-table-scan primitives during commit validation.
    pub scan_rows_touched: u64,
    /// Index slots the apply-time maintenance loop iterated over for this write set.
    pub index_maintenance_visits: u64,
    /// Per-stage cumulative wall time in nanoseconds, ordered as in the struct doc.
    /// `None` unless built with `test-seams`. Never read by correctness assertions.
    pub stage_wall_nanos: Option<[u64; 7]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StagedTupleKey {
    table: String,
    columns: Vec<String>,
    values: Vec<DirectedValue>,
}

#[derive(Debug, Default)]
struct StagedTupleLookup {
    entries: BTreeMap<StagedTupleKey, BTreeSet<RowId>>,
}

#[derive(Debug)]
enum StagedTupleLookupEvent {
    Add { table: TableName, row: VersionedRow },
    Remove { table: TableName, row: VersionedRow },
}

#[derive(Debug, Default)]
struct StagedTupleLookupDelta {
    events: Vec<StagedTupleLookupEvent>,
}

#[derive(Debug)]
struct CommitTimeUpsertRewrite {
    table: TableName,
    row_id: RowId,
    lookup_delta: StagedTupleLookupDelta,
}

struct PreparedPropagationOptions<'a> {
    tx: TxId,
    snapshot: SnapshotId,
    lookup_delta: Option<&'a mut StagedTupleLookupDelta>,
}

struct CommitStageStatsGuard<'a> {
    db: &'a Database,
    stats: CommitStageStats,
    flushed: bool,
}

#[cfg(any(test, feature = "test-seams"))]
struct CommitStageTimer {
    last: Instant,
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn build_db(
        tx_mgr: DatabaseTransactionHost,
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector_store: Arc<VectorStore>,
        hnsw: Arc<OnceLock<parking_lot::RwLock<Option<HnswIndex>>>>,
        change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
        change_log_table_index: Arc<RwLock<ChangeLogTableIndex>>,
        change_log_lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
        ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
        received_schema_stages: ReceivedSchemaStageRegistry,
        local_erasure_stages: discard::StageRegistry,
        received_arriving_table_meta: crate::composite_store::ReceivedArrivingTableMetaRegistry,
        pending_local_schema_stages: Arc<Mutex<schema_stage_registry::PendingRegistry>>,
        local_schema_stages: LocalSchemaStageRegistry,
        snapshot_registry: Arc<SnapshotFloorRegistry>,
        vector_partition_snapshot_stages: VectorPartitionSnapshotStageRegistry,
        persistence: Option<Arc<RedbPersistence>>,
        blob_repository: Arc<BlobRepository>,
        open_registry_path: Option<PathBuf>,
        apply_phase_pause: Arc<ApplyPhasePause>,
        plugin: Arc<dyn DatabasePlugin>,
        finalization: Arc<StoreFinalization>,
        vector_serving_cache_bytes: Arc<AtomicU64>,
        disk_limit: Option<u64>,
        disk_limit_startup_ceiling: Option<u64>,
        event_bus: Arc<EventBusState>,
        trigger: Arc<TriggerState>,
        declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
    ) -> Result<Self> {
        let accountant =
            crate::memory_accounting::AccountantHandle::clone(&finalization.accountant);
        snapshot_registry.fund_pin_slots(accountant.clone())?;
        let vector_accountant =
            contextdb_vector::MemoryBudgetOwner::clone(&finalization.vector_accountant);
        {
            let mut storage = std::mem::MaybeUninit::<Database>::uninit();
            let mut construction = DatabaseConstruction {
                source: storage.as_mut_ptr(),
                initialized: [false; 138],
            };
            // Every initializer is evaluated in its original order. A field is
            // marked only after the write; an unwinding initializer never makes
            // uninitialized storage eligible for cleanup.
            unsafe {
                {
                    std::ptr::addr_of_mut!((*construction.source).id).write(DatabaseId::next());
                    construction.initialized[0] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(0);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).tx_mgr).write(tx_mgr.clone());
                    construction.initialized[1] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(1);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).relational_store)
                        .write(relational.clone());
                    construction.initialized[2] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(2);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).graph_store).write(graph.clone());
                    construction.initialized[3] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(3);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_store)
                        .write(vector_store.clone());
                    construction.initialized[4] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(4);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_schema_gates)
                        .write(Arc::new(VectorSchemaGates::default()));
                    construction.initialized[5] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(5);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log).write(change_log);
                    construction.initialized[6] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(6);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).forced_change_log_gap_through)
                        .write(Arc::new(AtomicU64::new(0)));
                    construction.initialized[7] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(7);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log_table_index)
                        .write(change_log_table_index);
                    construction.initialized[8] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(8);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log_lsn_refcounts)
                        .write(change_log_lsn_refcounts);
                    construction.initialized[9] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(9);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).ddl_log).write(ddl_log);
                    construction.initialized[10] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(10);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_tombstone_arrivals)
                        .write(Arc::new(RwLock::new(HashMap::new())));
                    construction.initialized[11] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(11);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_graph_arrivals)
                        .write(Arc::new(RwLock::new(HashMap::new())));
                    construction.initialized[12] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(12);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).terminal_refusal_markers)
                        .write(Arc::new(RwLock::new(HashMap::new())));
                    construction.initialized[13] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(13);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).terminal_refusal_scans)
                        .write(Arc::new(RwLock::new(HashMap::new())));
                    construction.initialized[14] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(14);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).accepted_sync_row_authors)
                        .write(Arc::new(RwLock::new(HashMap::new())));
                    construction.initialized[15] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(15);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_stages)
                        .write(received_schema_stages);
                    construction.initialized[16] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(16);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).local_erasure_stages)
                        .write(local_erasure_stages);
                    construction.initialized[17] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(17);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_arriving_table_meta)
                        .write(received_arriving_table_meta);
                    construction.initialized[18] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(18);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_local_schema_stages)
                        .write(pending_local_schema_stages);
                    construction.initialized[19] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(19);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_stages)
                        .write(local_schema_stages);
                    construction.initialized[20] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(20);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).capture_detached_sync_write_set)
                        .write(Arc::new(AtomicBool::new(false)));
                    construction.initialized[21] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(21);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).detached_sync_write_set)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[22] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(22);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_staging_access)
                        .write(None);
                    construction.initialized[23] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(23);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_vector_image_pending)
                        .write(Arc::new(AtomicBool::new(false)));
                    construction.initialized[24] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(24);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).persistence).write(persistence);
                    construction.initialized[25] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(25);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).commit_source_reference_charge)
                        .write(0);
                    construction.initialized[26] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(26);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).committed_image_startup)
                        .write(None);
                    construction.initialized[27] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(27);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).image_history).write(None);
                    construction.initialized[28] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(28);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retained_relational_source)
                        .write(None);
                    construction.initialized[29] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(29);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).blob_repository)
                        .write(blob_repository);
                    construction.initialized[30] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(30);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).open_registry_path)
                        .write(Mutex::new(open_registry_path));
                    construction.initialized[31] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(31);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).operation_gate)
                        .write(Arc::new(RwLock::new(())));
                    construction.initialized[32] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(32);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).read_publication_gate)
                        .write(Arc::new(ReadPublicationGate::default()));
                    construction.initialized[33] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(33);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).schema_publication_gate)
                        .write(Arc::new(RwLock::new(())));
                    construction.initialized[34] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(34);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).apply_phase_pause)
                        .write(apply_phase_pause);
                    construction.initialized[35] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(35);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_apply_pre_commit_pause)
                        .write(Arc::new(ApplyPhasePause::new()));
                    construction.initialized[36] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(36);
                }
                {
                    std::ptr::addr_of_mut!((*construction.source).retained_graph_source)
                        .write(None);
                    construction.initialized[136] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(136);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_pre_commit_pause)
                        .write(Arc::new(ApplyPhasePause::new()));
                    construction.initialized[37] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(37);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).received_schema_pre_publish_pause
                    )
                    .write(Arc::new(ApplyPhasePause::new()));
                    construction.initialized[38] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(38);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_apply_observer)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[39] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(39);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_pre_commit_pause)
                        .write(Arc::new(ApplyPhasePause::new()));
                    construction.initialized[40] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(40);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_pre_publish_pause)
                        .write(Arc::new(ApplyPhasePause::new()));
                    construction.initialized[41] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(41);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).local_schema_between_publication_pause
                    )
                    .write(Arc::new(ApplyPhasePause::new()));
                    construction.initialized[42] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(42);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).export_after_capture_pause)
                        .write(Arc::new(ApplyPhasePause::new()));
                    construction.initialized[43] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(43);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).relational).write(
                        tx_mgr
                            .relational_executor(relational)
                            .with_staged_posting_admission(Arc::<
                                writer_read::WriterStagedPostingAdmission,
                            >::clone(
                                &finalization.staged_posting_admission,
                            )),
                    );
                    construction.initialized[44] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(44);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).graph)
                        .write(tx_mgr.graph_executor(graph));
                    construction.initialized[45] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(45);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector).write(
                        tx_mgr.vector_executor(
                            vector_store,
                            hnsw,
                            contextdb_vector::MemoryBudgetOwner::clone(&vector_accountant),
                        ),
                    );
                    construction.initialized[46] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(46);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).image_store_file_bytes)
                        .write(None);
                    construction.initialized[47] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(47);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).session_tx)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[48] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(48);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).origin_session_tx).write(None);
                    construction.initialized[49] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(49);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).instance_id)
                        .write(uuid::Uuid::new_v4());
                    construction.initialized[50] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(50);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_thread)
                        .write(crate::owned_worker::current_thread_id());
                    construction.initialized[51] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(51);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).plugin).write(plugin);
                    construction.initialized[52] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(52);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_config)
                        .write(Arc::new(OwnerReadConfig::default()));
                    construction.initialized[53] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(53);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).declared_read_memory)
                        .write(declared_read_memory);
                    construction.initialized[54] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(54);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_service).write(None);
                    construction.initialized[55] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(55);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_startup_failure)
                        .write(None);
                    construction.initialized[56] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(56);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).route_observer).write(None);
                    construction.initialized[57] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(57);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).kernel_observer).write(None);
                    construction.initialized[58] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(58);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).snapshot_free_retirement_pass_count
                    )
                    .write(Arc::new(AtomicUsize::new(0)));
                    construction.initialized[59] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(59);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).access)
                        .write(AccessConstraints::default());
                    construction.initialized[60] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(60);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).accountant).write(accountant);
                    construction.initialized[61] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(61);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).conflict_policies).write(
                        RwLock::new(ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists)),
                    );
                    construction.initialized[62] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(62);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).subscriptions)
                        .write(Arc::new(Mutex::new(SubscriptionState::new())));
                    construction.initialized[63] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(63);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pruning_runtime)
                        .write(Mutex::new(PruningRuntime::new()));
                    construction.initialized[64] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(64);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pruning_guard)
                        .write(Arc::new(Mutex::new(())));
                    construction.initialized[65] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(65);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retention_sync_peer)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[66] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(66);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_destination_reuploads)
                        .write(Arc::new(Mutex::new(HashMap::new())));
                    construction.initialized[67] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(67);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).lineage_state_lock)
                        .write(Arc::new(Mutex::new(InMemoryLineageState::default())));
                    construction.initialized[68] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(68);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_manifest_lock)
                        .write(Arc::new(Mutex::new(())));
                    construction.initialized[69] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(69);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_push_apply_locks)
                        .write(Arc::new(Mutex::new(HashMap::new())));
                    construction.initialized[70] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(70);
                }
                #[cfg(feature = "sync-orchestration")]
                {
                    std::ptr::addr_of_mut!((*construction.source).sync_client_push_locks)
                        .write(Arc::new(Mutex::new(HashMap::new())));
                    construction.initialized[71] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(71);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).in_memory_received_schema_manifests
                    )
                    .write(Arc::new(Mutex::new(
                        InMemoryReceivedSchemaManifestState::default(),
                    )));
                    construction.initialized[72] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(72);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_table_generations)
                        .write(Arc::new(Mutex::new(HashMap::new())));
                    construction.initialized[73] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(73);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_ddl_generations)
                        .write(Arc::new(Mutex::new(HashMap::new())));
                    construction.initialized[74] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(74);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_ddl_arrivals)
                        .write(Arc::new(RwLock::new(HashMap::new())));
                    construction.initialized[75] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(75);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).superseded_ddl_occurrences)
                        .write(Arc::new(RwLock::new(HashMap::new())));
                    construction.initialized[76] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(76);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).cron)
                        .write(Arc::new(CronState::new()));
                    construction.initialized[77] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(77);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).event_bus).write(event_bus);
                    construction.initialized[78] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(78);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).trigger).write(trigger);
                    construction.initialized[79] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(79);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_relay_mode)
                        .write(Arc::new(AtomicBool::new(false)));
                    construction.initialized[81] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(81);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_sync_progress)
                        .write(Arc::new(Mutex::new(InMemorySyncProgressState::default())));
                    construction.initialized[82] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(82);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).in_memory_applied_push_watermarks
                    )
                    .write(Arc::new(Mutex::new(HashMap::new())));
                    construction.initialized[83] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(83);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_incarnations)
                        .write(Arc::new(Mutex::new(HashMap::new())));
                    construction.initialized[84] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(84);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_runtime)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[85] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(85);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_cache)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[86] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(86);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_metadata)
                        .write(Arc::new(Mutex::new(BTreeMap::new())));
                    construction.initialized[87] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(87);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_event_bus_ddl)
                        .write(Mutex::new(HashMap::new()));
                    construction.initialized[88] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(88);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_commit_metadata)
                        .write(Mutex::new(HashMap::new()));
                    construction.initialized[89] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(89);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).limit_update_lock)
                        .write(Arc::new(Mutex::new(())));
                    construction.initialized[90] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(90);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).maintenance_poll_interval_update_pause
                    )
                    .write(Arc::new(ApplyPhasePause::new()));
                    construction.initialized[91] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(91);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).maintenance_poll_interval_update_probe
                    )
                    .write(Arc::new(MaintenancePollIntervalUpdateProbe::default()));
                    construction.initialized[92] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(92);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).disk_limit)
                        .write(Arc::new(AtomicU64::new(disk_limit.unwrap_or(0))));
                    construction.initialized[93] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(93);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).disk_limit_startup_ceiling)
                        .write(Arc::new(AtomicU64::new(
                            disk_limit_startup_ceiling.unwrap_or(0),
                        )));
                    construction.initialized[94] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(94);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).trigger_audit_retention_secs)
                        .write(Arc::new(AtomicU64::new(TRIGGER_AUDIT_RETENTION.as_secs())));
                    construction.initialized[95] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(95);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).snapshot_registry)
                        .write(snapshot_registry);
                    construction.initialized[96] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(96);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_partition_snapshot_stages)
                        .write(vector_partition_snapshot_stages);
                    construction.initialized[97] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(97);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retention_deferred_edge_nodes)
                        .write(Arc::new(Mutex::new(RetentionDeferredEdges::default())));
                    construction.initialized[98] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(98);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).maintenance_caller_driven)
                        .write(Arc::new(AtomicBool::new(false)));
                    construction.initialized[99] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(99);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).maintenance_poll_interval_ms)
                        .write(Arc::new(
                            AtomicU64::new(MAINTENANCE_TICK.as_millis() as u64),
                        ));
                    construction.initialized[100] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(100);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_serving_cache_bytes)
                        .write(vector_serving_cache_bytes);
                    construction.initialized[101] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(101);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_maintenance_cycle_at)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[102] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(102);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).caller_driven_backlog_warned_at)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[103] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(103);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_auto_compact_at)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[104] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(104);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).storage_compaction_policy)
                        .write(Arc::new(Mutex::new(StorageCompactionState::default())));
                    construction.initialized[105] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(105);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_watermark)
                        .write(Arc::new(AtomicLsn::new(Lsn(0))));
                    construction.initialized[106] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(106);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).closed)
                        .write(AtomicBool::new(false));
                    construction.initialized[107] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(107);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_progress)
                        .write(Arc::new(Mutex::new(CloseProgress::Idle)));
                    construction.initialized[108] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(108);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_changed)
                        .write(Arc::new(Condvar::new()));
                    construction.initialized[109] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(109);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_error)
                        .write(Mutex::new(None));
                    construction.initialized[110] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(110);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).close_follower_observer)
                        .write(Mutex::new(None));
                    construction.initialized[111] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(111);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).resource_closed)
                        .write(Arc::new(AtomicBool::new(false)));
                    construction.initialized[112] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(112);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).publication_interrupted)
                        .write(Arc::new(AtomicBool::new(false)));
                    construction.initialized[113] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(113);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rows_examined)
                        .write(AtomicU64::new(0));
                    construction.initialized[114] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(114);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).candidate_rows_materialized)
                        .write(Arc::new(AtomicU64::new(0)));
                    construction.initialized[115] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(115);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_vector_search_used_hnsw)
                        .write(Arc::new(AtomicBool::new(false)));
                    construction.initialized[116] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(116);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_vector_search_trace)
                        .write(Arc::new(RwLock::new(None)));
                    construction.initialized[117] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(117);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).statement_cache)
                        .write(RwLock::new(HashMap::new()));
                    construction.initialized[118] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(118);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_formula_cache)
                        .write(Arc::new(RwLock::new(HashMap::new())));
                    construction.initialized[119] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(119);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).acl_grant_cache)
                        .write(RwLock::new(gate::AdmittedAclGrantCache::new()));
                    construction.initialized[120] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(120);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_policy_eval_count)
                        .write(AtomicU64::new(0));
                    construction.initialized[121] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(121);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_policy_formula_parse_count)
                        .write(AtomicU64::new(0));
                    construction.initialized[122] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(122);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).fk_indexed_tuple_probes)
                        .write(AtomicU64::new(0));
                    construction.initialized[123] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(123);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).fk_full_scan_fallbacks)
                        .write(AtomicU64::new(0));
                    construction.initialized[124] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(124);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_rows_validated)
                        .write(AtomicU64::new(0));
                    construction.initialized[125] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(125);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_indexed_probes)
                        .write(AtomicU64::new(0));
                    construction.initialized[126] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(126);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).commit_staged_vs_staged_comparisons
                    )
                    .write(AtomicU64::new(0));
                    construction.initialized[127] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(127);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_scan_rows_touched)
                        .write(AtomicU64::new(0));
                    construction.initialized[128] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(128);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_index_maintenance_visits)
                        .write(AtomicU64::new(0));
                    construction.initialized[129] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(129);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).commit_stage_wall_nanos)
                        .write(std::array::from_fn(|_| AtomicU64::new(0)));
                    construction.initialized[130] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(130);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).corrupt_joined_values)
                        .write(RwLock::new(HashSet::new()));
                    construction.initialized[131] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(131);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).resource_owner).write(true);
                    construction.initialized[132] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(132);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).handle_role)
                        .write(HandleRole::Owner);
                    construction.initialized[133] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(133);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).trigger_deadlock_timeout_override
                    )
                    .write(Mutex::new(None));
                    construction.initialized[80] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(80);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).finalization).write(finalization);
                    construction.initialized[135] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(135);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).read_image_memory).write(None);
                    construction.initialized[134] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(134);
                }
            }
            construction.initialized.fill(false);
            unsafe { Ok(storage.assume_init()) }
        }
    }

    /// Opens the database file at `path`, becoming its sole owner for the
    /// lifetime of the returned handle. A second open of the same path is
    /// refused, and the refusal says who is holding it: from THIS process it
    /// is [`Error::DatabaseLocked`], carrying the holding process id and the
    /// path; from ANOTHER process it is a typed `HeldByWriter` read failure
    /// whose reason names the holding process id and the store, so an operator
    /// is told what to stop rather than only that they cannot proceed. See the
    /// "Store ownership and concurrency" section on [`Database`]. Use
    /// [`Database::open_memory`] for an ephemeral instance.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_builtin_with_options(path, DatabaseOpenOptions::default())
    }

    pub fn open_memory() -> Self {
        Self::open_builtin_with_options(MEMORY_DATABASE_PATH, DatabaseOpenOptions::default())
            .expect("failed to open in-memory database")
    }

    pub fn scoped_with_constraints(
        &self,
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> Self {
        let contexts = narrowed_constraint_set(&self.access.contexts, contexts);
        let scope_labels = narrowed_constraint_set(&self.access.scope_labels, scope_labels);
        let principal = self.access.principal.clone().or(principal);
        {
            let mut storage = std::mem::MaybeUninit::<Database>::uninit();
            let mut construction = DatabaseConstruction {
                source: storage.as_mut_ptr(),
                initialized: [false; 138],
            };
            // Every initializer is evaluated in its original order. A field is
            // marked only after the write; an unwinding initializer never makes
            // uninitialized storage eligible for cleanup.
            unsafe {
                {
                    std::ptr::addr_of_mut!((*construction.source).id).write(DatabaseId::next());
                    construction.initialized[0] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(0);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).tx_mgr)
                        .write(self.tx_mgr.clone());
                    construction.initialized[1] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(1);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).relational_store)
                        .write(self.relational_store.clone());
                    construction.initialized[2] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(2);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).graph_store)
                        .write(self.graph_store.clone());
                    construction.initialized[3] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(3);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_store)
                        .write(self.vector_store.clone());
                    construction.initialized[4] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(4);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_schema_gates)
                        .write(self.vector_schema_gates.clone());
                    construction.initialized[5] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(5);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log)
                        .write(self.change_log.clone());
                    construction.initialized[6] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(6);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).forced_change_log_gap_through)
                        .write(self.forced_change_log_gap_through.clone());
                    construction.initialized[7] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(7);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log_table_index)
                        .write(self.change_log_table_index.clone());
                    construction.initialized[8] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(8);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log_lsn_refcounts)
                        .write(self.change_log_lsn_refcounts.clone());
                    construction.initialized[9] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(9);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).ddl_log)
                        .write(self.ddl_log.clone());
                    construction.initialized[10] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(10);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_tombstone_arrivals)
                        .write(self.sync_tombstone_arrivals.clone());
                    construction.initialized[11] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(11);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_graph_arrivals)
                        .write(self.sync_graph_arrivals.clone());
                    construction.initialized[12] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(12);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).terminal_refusal_markers)
                        .write(self.terminal_refusal_markers.clone());
                    construction.initialized[13] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(13);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).terminal_refusal_scans)
                        .write(self.terminal_refusal_scans.clone());
                    construction.initialized[14] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(14);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).accepted_sync_row_authors)
                        .write(self.accepted_sync_row_authors.clone());
                    construction.initialized[15] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(15);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_stages)
                        .write(self.received_schema_stages.clone());
                    construction.initialized[16] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(16);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).local_erasure_stages)
                        .write(self.local_erasure_stages.clone());
                    construction.initialized[17] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(17);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_arriving_table_meta)
                        .write(self.received_arriving_table_meta.clone());
                    construction.initialized[18] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(18);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_local_schema_stages)
                        .write(self.pending_local_schema_stages.clone());
                    construction.initialized[19] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(19);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_stages)
                        .write(self.local_schema_stages.clone());
                    construction.initialized[20] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(20);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).capture_detached_sync_write_set)
                        .write(self.capture_detached_sync_write_set.clone());
                    construction.initialized[21] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(21);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).detached_sync_write_set)
                        .write(self.detached_sync_write_set.clone());
                    construction.initialized[22] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(22);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_staging_access)
                        .write(self.received_staging_access.clone());
                    construction.initialized[23] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(23);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_vector_image_pending)
                        .write(self.received_vector_image_pending.clone());
                    construction.initialized[24] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(24);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).persistence)
                        .write(self.persistence.clone());
                    construction.initialized[25] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(25);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).commit_source_reference_charge)
                        .write(self.commit_source_reference_charge);
                    construction.initialized[26] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(26);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).committed_image_startup)
                        .write(self.committed_image_startup.clone());
                    construction.initialized[27] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(27);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).image_history)
                        .write(self.image_history.clone());
                    construction.initialized[28] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(28);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retained_relational_source)
                        .write(self.retained_relational_source.clone());
                    construction.initialized[29] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(29);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).blob_repository)
                        .write(self.blob_repository.clone());
                    construction.initialized[30] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(30);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).open_registry_path)
                        .write(Mutex::new(None));
                    construction.initialized[31] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(31);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).operation_gate)
                        .write(self.operation_gate.clone());
                    construction.initialized[32] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(32);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).read_publication_gate)
                        .write(self.read_publication_gate.clone());
                    construction.initialized[33] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(33);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).schema_publication_gate)
                        .write(self.schema_publication_gate.clone());
                    construction.initialized[34] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(34);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).apply_phase_pause)
                        .write(self.apply_phase_pause.clone());
                    construction.initialized[35] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(35);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_apply_pre_commit_pause)
                        .write(self.sync_apply_pre_commit_pause.clone());
                    construction.initialized[36] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(36);
                }
                {
                    std::ptr::addr_of_mut!((*construction.source).retained_graph_source)
                        .write(self.retained_graph_source.clone());
                    construction.initialized[136] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(136);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_pre_commit_pause)
                        .write(self.received_schema_pre_commit_pause.clone());
                    construction.initialized[37] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(37);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).received_schema_pre_publish_pause
                    )
                    .write(self.received_schema_pre_publish_pause.clone());
                    construction.initialized[38] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(38);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_apply_observer)
                        .write(self.received_schema_apply_observer.clone());
                    construction.initialized[39] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(39);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_pre_commit_pause)
                        .write(self.local_schema_pre_commit_pause.clone());
                    construction.initialized[40] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(40);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_pre_publish_pause)
                        .write(self.local_schema_pre_publish_pause.clone());
                    construction.initialized[41] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(41);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).local_schema_between_publication_pause
                    )
                    .write(self.local_schema_between_publication_pause.clone());
                    construction.initialized[42] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(42);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).export_after_capture_pause)
                        .write(self.export_after_capture_pause.clone());
                    construction.initialized[43] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(43);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).relational).write(
                        self.tx_mgr
                            .relational_executor(self.relational_store.clone())
                            .with_staged_posting_admission(Arc::<
                                writer_read::WriterStagedPostingAdmission,
                            >::clone(
                                &self.finalization.staged_posting_admission,
                            )),
                    );
                    construction.initialized[44] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(44);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).graph)
                        .write(self.tx_mgr.graph_executor(self.graph_store.clone()));
                    construction.initialized[45] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(45);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector).write(
                        self.tx_mgr.vector_executor(
                            self.vector_store.clone(),
                            Arc::new(OnceLock::new()),
                            contextdb_vector::MemoryBudgetOwner::clone(
                                &self.finalization.vector_accountant,
                            ),
                        ),
                    );
                    construction.initialized[46] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(46);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).image_store_file_bytes)
                        .write(None);
                    construction.initialized[47] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(47);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).session_tx)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[48] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(48);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).origin_session_tx).write(None);
                    construction.initialized[49] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(49);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).instance_id)
                        .write(uuid::Uuid::new_v4());
                    construction.initialized[50] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(50);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_thread)
                        .write(crate::owned_worker::current_thread_id());
                    construction.initialized[51] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(51);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).plugin)
                        .write(self.plugin.clone());
                    construction.initialized[52] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(52);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_config)
                        .write(self.owner_read_config.clone());
                    construction.initialized[53] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(53);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).declared_read_memory)
                        .write(self.declared_read_memory.clone());
                    construction.initialized[54] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(54);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_service)
                        .write(self.owner_read_service.clone());
                    construction.initialized[55] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(55);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_startup_failure)
                        .write(self.owner_read_startup_failure.clone());
                    construction.initialized[56] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(56);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).route_observer)
                        .write(self.route_observer.clone());
                    construction.initialized[57] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(57);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).kernel_observer)
                        .write(self.kernel_observer.clone());
                    construction.initialized[58] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(58);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).snapshot_free_retirement_pass_count
                    )
                    .write(self.snapshot_free_retirement_pass_count.clone());
                    construction.initialized[59] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(59);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).access).write(
                        AccessConstraints {
                            contexts,
                            scope_labels,
                            principal,
                        },
                    );
                    construction.initialized[60] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(60);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).accountant).write(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                    );
                    construction.initialized[61] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(61);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).conflict_policies)
                        .write(RwLock::new(self.conflict_policies.read().clone()));
                    construction.initialized[62] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(62);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).subscriptions)
                        .write(self.subscriptions.clone());
                    construction.initialized[63] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(63);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pruning_runtime)
                        .write(Mutex::new(PruningRuntime::new()));
                    construction.initialized[64] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(64);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pruning_guard)
                        .write(self.pruning_guard.clone());
                    construction.initialized[65] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(65);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retention_sync_peer)
                        .write(self.retention_sync_peer.clone());
                    construction.initialized[66] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(66);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_destination_reuploads)
                        .write(self.in_memory_destination_reuploads.clone());
                    construction.initialized[67] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(67);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).lineage_state_lock)
                        .write(self.lineage_state_lock.clone());
                    construction.initialized[68] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(68);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_manifest_lock)
                        .write(self.received_schema_manifest_lock.clone());
                    construction.initialized[69] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(69);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_push_apply_locks)
                        .write(self.sync_push_apply_locks.clone());
                    construction.initialized[70] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(70);
                }
                #[cfg(feature = "sync-orchestration")]
                {
                    std::ptr::addr_of_mut!((*construction.source).sync_client_push_locks)
                        .write(self.sync_client_push_locks.clone());
                    construction.initialized[71] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(71);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).in_memory_received_schema_manifests
                    )
                    .write(self.in_memory_received_schema_manifests.clone());
                    construction.initialized[72] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(72);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_table_generations)
                        .write(self.in_memory_table_generations.clone());
                    construction.initialized[73] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(73);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_ddl_generations)
                        .write(self.in_memory_ddl_generations.clone());
                    construction.initialized[74] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(74);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_ddl_arrivals)
                        .write(self.received_ddl_arrivals.clone());
                    construction.initialized[75] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(75);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).superseded_ddl_occurrences)
                        .write(self.superseded_ddl_occurrences.clone());
                    construction.initialized[76] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(76);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).cron).write(self.cron.clone());
                    construction.initialized[77] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(77);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).event_bus)
                        .write(self.event_bus.clone());
                    construction.initialized[78] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(78);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).trigger)
                        .write(self.trigger.clone());
                    construction.initialized[79] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(79);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_relay_mode)
                        .write(self.sync_relay_mode.clone());
                    construction.initialized[81] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(81);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_sync_progress)
                        .write(self.in_memory_sync_progress.clone());
                    construction.initialized[82] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(82);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).in_memory_applied_push_watermarks
                    )
                    .write(self.in_memory_applied_push_watermarks.clone());
                    construction.initialized[83] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(83);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_incarnations)
                        .write(self.sync_incarnations.clone());
                    construction.initialized[84] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(84);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_runtime)
                        .write(self.custody_runtime.clone());
                    construction.initialized[85] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(85);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_cache)
                        .write(self.custody_cache.clone());
                    construction.initialized[86] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(86);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_metadata)
                        .write(self.custody_metadata.clone());
                    construction.initialized[87] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(87);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_event_bus_ddl)
                        .write(Mutex::new(HashMap::new()));
                    construction.initialized[88] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(88);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_commit_metadata)
                        .write(Mutex::new(HashMap::new()));
                    construction.initialized[89] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(89);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).limit_update_lock)
                        .write(self.limit_update_lock.clone());
                    construction.initialized[90] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(90);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).maintenance_poll_interval_update_pause
                    )
                    .write(self.maintenance_poll_interval_update_pause.clone());
                    construction.initialized[91] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(91);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).maintenance_poll_interval_update_probe
                    )
                    .write(self.maintenance_poll_interval_update_probe.clone());
                    construction.initialized[92] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(92);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).disk_limit)
                        .write(Arc::clone(&self.disk_limit));
                    construction.initialized[93] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(93);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).disk_limit_startup_ceiling)
                        .write(Arc::clone(&self.disk_limit_startup_ceiling));
                    construction.initialized[94] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(94);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).trigger_audit_retention_secs)
                        .write(self.trigger_audit_retention_secs.clone());
                    construction.initialized[95] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(95);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).snapshot_registry)
                        .write(self.snapshot_registry.clone());
                    construction.initialized[96] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(96);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_partition_snapshot_stages)
                        .write(self.vector_partition_snapshot_stages.clone());
                    construction.initialized[97] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(97);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retention_deferred_edge_nodes)
                        .write(self.retention_deferred_edge_nodes.clone());
                    construction.initialized[98] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(98);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).maintenance_caller_driven)
                        .write(self.maintenance_caller_driven.clone());
                    construction.initialized[99] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(99);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).maintenance_poll_interval_ms)
                        .write(self.maintenance_poll_interval_ms.clone());
                    construction.initialized[100] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(100);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_serving_cache_bytes)
                        .write(self.vector_serving_cache_bytes.clone());
                    construction.initialized[101] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(101);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_maintenance_cycle_at)
                        .write(self.last_maintenance_cycle_at.clone());
                    construction.initialized[102] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(102);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).caller_driven_backlog_warned_at)
                        .write(self.caller_driven_backlog_warned_at.clone());
                    construction.initialized[103] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(103);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_auto_compact_at)
                        .write(self.last_auto_compact_at.clone());
                    construction.initialized[104] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(104);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).storage_compaction_policy)
                        .write(self.storage_compaction_policy.clone());
                    construction.initialized[105] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(105);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_watermark)
                        .write(self.sync_watermark.clone());
                    construction.initialized[106] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(106);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).closed)
                        .write(AtomicBool::new(false));
                    construction.initialized[107] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(107);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_progress)
                        .write(Arc::new(Mutex::new(CloseProgress::Idle)));
                    construction.initialized[108] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(108);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_changed)
                        .write(Arc::new(Condvar::new()));
                    construction.initialized[109] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(109);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_error)
                        .write(Mutex::new(None));
                    construction.initialized[110] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(110);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).close_follower_observer)
                        .write(Mutex::new(None));
                    construction.initialized[111] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(111);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).resource_closed)
                        .write(self.resource_closed.clone());
                    construction.initialized[112] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(112);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).publication_interrupted)
                        .write(self.publication_interrupted.clone());
                    construction.initialized[113] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(113);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rows_examined)
                        .write(AtomicU64::new(0));
                    construction.initialized[114] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(114);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).candidate_rows_materialized)
                        .write(self.candidate_rows_materialized.clone());
                    construction.initialized[115] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(115);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_vector_search_used_hnsw)
                        .write(Arc::clone(&self.last_vector_search_used_hnsw));
                    construction.initialized[116] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(116);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_vector_search_trace)
                        .write(Arc::clone(&self.last_vector_search_trace));
                    construction.initialized[117] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(117);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).statement_cache)
                        .write(RwLock::new(HashMap::new()));
                    construction.initialized[118] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(118);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_formula_cache)
                        .write(Arc::clone(&self.rank_formula_cache));
                    construction.initialized[119] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(119);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).acl_grant_cache)
                        .write(RwLock::new(gate::AdmittedAclGrantCache::new()));
                    construction.initialized[120] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(120);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_policy_eval_count)
                        .write(AtomicU64::new(0));
                    construction.initialized[121] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(121);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_policy_formula_parse_count)
                        .write(AtomicU64::new(0));
                    construction.initialized[122] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(122);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).fk_indexed_tuple_probes)
                        .write(AtomicU64::new(0));
                    construction.initialized[123] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(123);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).fk_full_scan_fallbacks)
                        .write(AtomicU64::new(0));
                    construction.initialized[124] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(124);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_rows_validated)
                        .write(AtomicU64::new(0));
                    construction.initialized[125] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(125);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_indexed_probes)
                        .write(AtomicU64::new(0));
                    construction.initialized[126] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(126);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).commit_staged_vs_staged_comparisons
                    )
                    .write(AtomicU64::new(0));
                    construction.initialized[127] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(127);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_scan_rows_touched)
                        .write(AtomicU64::new(0));
                    construction.initialized[128] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(128);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_index_maintenance_visits)
                        .write(AtomicU64::new(0));
                    construction.initialized[129] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(129);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).commit_stage_wall_nanos)
                        .write(std::array::from_fn(|_| AtomicU64::new(0)));
                    construction.initialized[130] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(130);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).corrupt_joined_values)
                        .write(RwLock::new(HashSet::new()));
                    construction.initialized[131] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(131);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).resource_owner).write(false);
                    construction.initialized[132] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(132);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).handle_role)
                        .write(HandleRole::Internal);
                    construction.initialized[133] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(133);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).trigger_deadlock_timeout_override
                    )
                    .write(Mutex::new(*self.trigger_deadlock_timeout_override.lock()));
                    construction.initialized[80] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(80);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).finalization)
                        .write(Arc::clone(&self.finalization));
                    construction.initialized[135] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(135);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).read_image_memory)
                        .write(self.read_image_memory.clone());
                    construction.initialized[134] = true;
                    #[cfg(test)]
                    r1_database_field_for_test(134);
                }
            }
            construction.initialized.fill(false);
            unsafe { storage.assume_init() }
        }
    }

    fn is_staged_local_schema_statement(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::CreateTable(_)
                | Statement::CreateIndex(_)
                | Statement::CreateEventType { .. }
                | Statement::CreateTrigger { .. }
                | Statement::CreateSink { .. }
                | Statement::CreateRoute { .. }
        ) || matches!(stmt, Statement::AlterTable(_))
    }

    fn is_local_table_schema_statement(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::CreateTable(_)
                | Statement::DropTable(_)
                | Statement::AlterTable(_)
                | Statement::CreateIndex(_)
                | Statement::DropIndex(_)
                | Statement::CreateTrigger { .. }
                | Statement::DropTrigger { .. }
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        #[cfg(any(test, feature = "test-seams"))]
        let _maintenance_phase = crate::read_image_memory::MaintenancePhase::enter("execute");
        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::execute");
        let child = self.ordinary_read_child()?;
        crate::executor::bounded::with_native_read_memory(
            child,
            |read_memory, native_trace_refund| {
                QueryResult::publish_with(read_memory.clone(), || {
                    self.execute_owned_with_native_trace_refund(
                        sql,
                        params,
                        read_memory,
                        Some(&native_trace_refund),
                    )
                })
            },
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn execute_in_tx_with_rows_examined_scope(
        &self,
        tx: TxId,
        sql: &str,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Checked BEFORE the snapshot registration below -- see `execute`'s
        // own identical, deliberately LOCK-FREE guard for why (`already_
        // closed`, never `open_operation`, which this function's own later
        // `open_operation_after_*` calls must remain free to acquire without
        // a pre-held read lock from here deadlocking a racing `close()`).
        // Narrows, but does not by itself close, the race -- a close landing
        // between this check and the registration just below is caught
        // there instead (`try_snapshot_for_read`), not by anything later in
        // this function.
        if self.already_closed() {
            return Err(closed_database_error());
        }
        self.refuse_after_interrupted_publication()?;
        // Registered for exactly this call's duration -- see
        // `SnapshotFloorRegistry`'s doc comment on `execute`. Fallible form,
        // same reason as `execute`'s own identical line.
        let cached = self.cached_statement(sql);
        let parsed_stmt;
        let (stmt, _cached_plan) = if let Some(cached) = cached.as_ref() {
            (&cached.stmt, Some(&cached.plan))
        } else {
            parsed_stmt = contextdb_parser::parse(sql)?;
            (&parsed_stmt, None)
        };
        let _schema_publication =
            self.enter_schema_publication_gate(Self::statement_changes_table_schema(stmt));
        // DDL may have invalidated a plan while this call acquired the guard.
        // Fetch the plan again under the same guard as the durable root.
        let cached = self.cached_statement(sql);
        let parsed_stmt;
        let (stmt, cached_plan) = if let Some(cached) = cached.as_ref() {
            (&cached.stmt, Some(&cached.plan))
        } else {
            parsed_stmt = contextdb_parser::parse(sql)?;
            (&parsed_stmt, None)
        };
        let _membership_capture = self.relational_store.begin_membership_capture();
        let (read_snapshot, root) =
            if self.persistence.is_some() && Self::statement_changes_table_schema(stmt) {
                self.capture_writer_schema_statement()?
            } else {
                self.capture_writer_transaction_statement(Some(tx), read_memory.clone())?
            };
        let has_root = root.is_some();
        let _writer_read = self.enter_writer_read(root, read_memory.clone());
        let _snapshot_registration = self
            .snapshot_registry
            .register_statement_root(read_snapshot, has_root);

        let trigger_callback_bound = self.trigger_callback_tx_bound_matches(tx);
        if trigger_callback_bound && matches!(stmt, Statement::Update(_) | Statement::Delete(_)) {
            if let Some(plan) = cached_plan {
                // This branch reaches execute_plan without going through
                // run_planned_statement, so it applies the pass itself.
                validate_plan_columns(self, plan)?;
                let snapshot = self.snapshot_for_read();
                return self
                    .with_snapshot_override(snapshot, || {
                        self.run_callback_dml_plan(
                            tx,
                            plan,
                            params,
                            vector_copy_owner,
                            read_memory.clone(),
                        )
                    })
                    .map(strip_internal_row_id);
            }

            let stmt = self.pre_resolve_cte_subqueries(
                stmt,
                params,
                Some(tx),
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let plan = contextdb_planner::plan(&stmt)?;
            self.cache_statement_if_eligible(sql, &stmt, &plan);
            validate_plan_columns(self, &plan)?;
            let snapshot = self.snapshot_for_read();
            return self
                .with_snapshot_override(snapshot, || {
                    self.run_callback_dml_plan(
                        tx,
                        &plan,
                        params,
                        vector_copy_owner,
                        read_memory.clone(),
                    )
                })
                .map(strip_internal_row_id);
        }
        let requires_callback_tx = Self::statement_requires_cron_bound_handle(stmt);
        if trigger_callback_bound {
            self.assert_statement_allowed_inside_cron_callback(
                stmt,
                Self::statement_forbidden_inside_cron_callback(stmt),
                requires_callback_tx,
            )?;
        } else if Self::callback_active_process_wide() {
            self.assert_callback_tx_bound_write_allowed(tx)?;
            self.assert_trigger_callback_tx_not_captured_cross_thread(tx)?;
            self.assert_statement_allowed_for_callbacks_for_surface(stmt, "execute_in_tx")?;
        }
        let _operation = if trigger_callback_bound {
            None
        } else if requires_callback_tx {
            Some(self.open_operation_after_write_control_wait(tx, "execute_in_tx")?)
        } else {
            Some(self.open_operation_after_statement_callback_wait(stmt, "execute_in_tx")?)
        };
        self.with_snapshot_override(read_snapshot, || {
            self.execute_statement_with_plan(
                stmt,
                sql,
                params,
                Some(tx),
                cached_plan,
                None,
                vector_copy_owner,
                read_memory.clone(),
                native_trace_refund,
            )
        })
    }

    /// Test-only: drop the change log alone, keeping the schema log, as a
    /// store whose change history was pruned keeps its schema history.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn __force_change_log_gap_for_test(&self) {
        let _schema_publication = self.enter_schema_publication_gate(false);
        self.with_commit_lock(|| {
            self.forced_change_log_gap_through
                .store(self.current_lsn().0, Ordering::SeqCst);
            self.change_log.write().clear();
            self.change_log_table_index.write().clear();
            self.change_log_lsn_refcounts.write().clear();
        });
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn open_operation(&self) -> Result<DatabaseOperationGuard<'_>> {
        let memory = match crate::read_image_memory::active_owner() {
            Ok(owner) if owner.accountant.as_ptr() == self.accountant.as_ptr() => owner.enter(),
            _ => crate::read_image_memory::ImageMemory::enter_writer_operation(
                self.accountant.clone(),
            )?,
        };
        let db_id = self.id;
        let nested = DB_OPERATION_STACK.with(|stack| stack.borrow().contains(&db_id));
        if nested {
            let writer_read = self.writer_root_scope()?;
            DB_OPERATION_STACK.with(|stack| stack.borrow_mut().push(db_id));
            return Ok(DatabaseOperationGuard {
                db_id,
                _lock: None,
                _schema_publication: None,
                _writer_read: Some(writer_read),
                _memory: memory,
            });
        }

        // Lock order is schema publication -> public operation -> commit /
        // vector gates. This is also the order used by outer SQL and sync
        // extraction paths, so a direct relational read cannot deadlock a
        // concurrent DDL writer while straddling its publication.
        let schema_publication = self.enter_schema_publication_gate(false);
        let lock = self.operation_gate.read();
        if self.closed.load(Ordering::SeqCst) || self.resource_closed.load(Ordering::SeqCst) {
            return Err(closed_database_error());
        }
        self.refuse_after_interrupted_publication()?;
        let writer_read = self.writer_root_scope()?;
        DB_OPERATION_STACK.with(|stack| stack.borrow_mut().push(db_id));
        Ok(DatabaseOperationGuard {
            db_id,
            _lock: Some(lock),
            _schema_publication: Some(schema_publication),
            _writer_read: Some(writer_read),
            _memory: memory,
        })
    }

    fn enter_schema_publication_gate(
        &self,
        writes_table_schema: bool,
    ) -> SchemaPublicationGuard<'_> {
        let gate_id = Arc::as_ptr(&self.schema_publication_gate) as usize;
        let nested = SCHEMA_PUBLICATION_STACK.with(|stack| stack.borrow().contains(&gate_id));
        let (read, write) = if nested {
            // Structural DDL is refused from trigger/cron callback reentry;
            // nested reads share the outer statement's gate without trying to
            // take a non-reentrant parking_lot guard a second time.
            (None, None)
        } else if writes_table_schema {
            (None, Some(self.schema_publication_gate.write()))
        } else {
            (Some(self.schema_publication_gate.read()), None)
        };
        SCHEMA_PUBLICATION_STACK.with(|stack| stack.borrow_mut().push(gate_id));
        SchemaPublicationGuard {
            gate_id,
            _read: read,
            _write: write,
        }
    }

    fn changeset_changes_table_schema(changes: &ChangeSet) -> bool {
        changes.ddl.iter().any(|change| {
            matches!(
                change,
                DdlChange::CreateTable { .. }
                    | DdlChange::DropTable { .. }
                    | DdlChange::AlterTable { .. }
                    | DdlChange::CreateIndex { .. }
                    | DdlChange::DropIndex { .. }
            )
        })
    }

    /// Commit one locally authored table projection before making that schema
    /// visible in memory.  The caller supplies the complete same-LSN DDL
    /// sequence, including any CASCADEd index drops, so provenance sidecars
    /// and Redb's indexed DDL keys share exactly one ordinal space.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_local_table_projection_and_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &[VersionedRow],
        lsn: Lsn,
        ddl: &[DdlChange],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<PreparedSchemaHistory>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let memory_history = self.prepare_memory_ddl_append(lsn, ddl, read_memory.clone())?;
        if let Some(source) =
            crate::persistence::received_working_source::WorkingReadSource::current_for(
                self.tx_mgr.identity(),
            )?
        {
            source.project_schema(name, Some(meta), ddl)?;
        }
        let values = self.ddl_generation_sidecar_values(lsn, ddl, 0, read_memory.clone())?;
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| {
                if self.local_table_projection_streamed(name, meta) {
                    let values = values
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.clone()))
                        .collect::<Vec<_>>();
                    if !persistence
                        .stream_scalar_schema_projection(name, meta, lsn, ddl, &values)?
                    {
                        return Err(Error::Other(
                            "the prepared scalar schema route changed before durability".into(),
                        ));
                    }
                    return Ok(());
                }
                persistence.rewrite_table_meta_rows_and_append_ddl_log(
                    name,
                    meta,
                    rows,
                    lsn,
                    ddl,
                    values
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.clone()))
                        .collect(),
                )
            })?;
        }
        Ok(memory_history)
    }

    /// [`Self::changes_since_base`] under a history answer's bounds: its
    /// checkpoint runs between records and every copy the extraction keeps or
    /// builds is admitted before it is made.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn changes_since_base_projected(
        &self,
        since_lsn: Lsn,
        projection: &ChangeProjection<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, DdlProvenanceSource)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let since_lsn = self.connected_history_since(since_lsn, read_memory.clone())?;
        (|| {
            // Direct helper calls establish the same admitted statement scope;
            // nested extraction calls retain their outer published root.
            let _operation = self.open_operation()?;
            // A sealed read image is not a sync hub (a hub serves from its
            // durable writer store), and its history walk authenticates the
            // whole log; it does not serve a paged pull.
            if projection.page.is_some() && self.image_history.is_some() {
                return Err(Error::ReadFailure(
                    contextdb_core::read_contract::ReadFailure::owner_route_unsupported(
                        "sync/paged_pull_from_sealed_image".into(),
                    ),
                ));
            }
            // Image history is selected from its retained root. Only requested
            // output is held; the authenticated walk never installs resident logs.
            let mut image_output =
                contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let mut image_history = if let Some(history) = self.image_history.as_ref() {
                let mut changes = Vec::new();
                let first_change = history.source.visit_changes_since(
                    since_lsn,
                    read_memory.clone(),
                    projection.checkpoint,
                    &mut |entry| {
                        image_output.try_grow_for(
                            answer_list_entry_bytes::<ChangeLogEntry>(change_log_entry_copy_bytes(
                                &entry,
                            )),
                            "history_source",
                            "history_output",
                            "Raise the reader limit to return history.",
                        )?;
                        changes.push(entry);
                        Ok(())
                    },
                )?;
                let mut ddl = Vec::new();
                let first_ddl = history.source.visit_ddl_since(
                    if projection.ddl {
                        since_lsn
                    } else {
                        Lsn(u64::MAX)
                    },
                    read_memory.clone(),
                    projection.checkpoint,
                    &mut |lsn, entry| {
                        image_output.try_grow_for(
                            answer_list_entry_bytes::<(Lsn, DdlChange)>(entry.estimated_bytes()),
                            "history_source",
                            "history_output",
                            "Raise the reader limit to return history.",
                        )?;
                        ddl.push((lsn, entry));
                        Ok(())
                    },
                )?;
                ddl.sort_by_key(|(lsn, _)| *lsn);
                Some((first_change, first_ddl, changes, ddl))
            } else {
                None
            };
            let _vector_schema =
                self.vector_schema_read_many(self.vector_store_schema_refs(vector_copy_owner)?);
            // Future watermark guard
            if since_lsn > self.current_lsn() {
                let changes = ChangeSet::default();
                let source = DdlProvenanceSource::historical(&changes);
                return Ok((changes, source));
            }

            // Deterministically select the real persisted-state fallback for the
            // one frozen race proof. The production branch below reaches the same
            // function when its logs no longer cover the requested cursor; this
            // test-only selector changes no fallback behavior.
            #[cfg(all(feature = "test-seams", unix))]
            if projection.page.is_none()
                && self.persisted_state_schema_frontier_pause_is_armed_for_test()
            {
                drop(image_history.take());
                drop(std::mem::replace(
                    &mut image_output,
                    contextdb_core::read_memory::ReadCredit::new(read_memory.clone()),
                ));
                return self.persisted_state_since_with_ddl_provenance(
                    since_lsn,
                    vector_copy_owner,
                    read_memory.clone(),
                );
            }

            // A paged pull replays the surviving log from its cursor: compaction
            // keeps each current version with its change-log entry, and every
            // other removal path removes a row's entries only with the row, so
            // the surviving log reconstructs every surviving row in authored
            // order. Persisted-state extraction serves only unpaged callers.
            let paged = projection.page.is_some();
            let resident_history = if paged && self.persistence.is_none() {
                Some(self.resident_pull_history(read_memory.clone())?)
            } else {
                None
            };
            #[cfg(any(test, feature = "test-seams"))]
            if !paged && since_lsn.0 < self.forced_change_log_gap_through.load(Ordering::SeqCst) {
                drop(image_history.take());
                drop(std::mem::replace(
                    &mut image_output,
                    contextdb_core::read_memory::ReadCredit::new(read_memory.clone()),
                ));
                return self.persisted_state_projected(
                    since_lsn,
                    projection,
                    vector_copy_owner,
                    read_memory.clone(),
                );
            }

            // Check if the ephemeral logs can serve the requested watermark.
            // After restart, both logs are empty but stores may have data — fall back to snapshot.
            let (change_first_lsn, change_log_empty, ddl_first_lsn, ddl_log_empty) = if paged {
                // Paged reads seek surviving sources directly; coverage
                // discovery and current-table inventories are unpaged work.
                (None, true, None, true)
            } else if let Some((first_change, first_ddl, _, _)) = image_history.as_ref() {
                (
                    *first_change,
                    first_change.is_none(),
                    *first_ddl,
                    first_ddl.is_none(),
                )
            } else if let (_, Some(root)) = self.capture_writer_statement()? {
                let mut changes = root.change_log_cursor(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    Lsn(0),
                    root.lsn,
                )?;
                let first_change = changes.next_change()?.map(|window| window.value.lsn());
                // A covered data-log frontier already proves the combined
                // history covers this watermark. Only a gap needs the DDL
                // head; that check needs its position, not its schema payload.
                let first_ddl =
                    if first_change.is_none_or(|lsn| lsn.0 > since_lsn.0.saturating_add(1)) {
                        let mut ddl = root.ddl_history_cursor(
                            crate::persistence::writer_source::SourceWindowOwner::Read(
                                read_memory.clone(),
                            ),
                            Lsn(0),
                        )?;
                        ddl.next_ddl_position()?.map(|window| window.value)
                    } else {
                        None
                    };
                (
                    first_change,
                    first_change.is_none(),
                    first_ddl,
                    first_ddl.is_none(),
                )
            } else {
                self.with_commit_lock(|| {
                    let log = self.change_log.read();
                    let ddl = self.ddl_log.read();
                    (
                        log.first().map(ChangeLogEntry::lsn),
                        log.is_empty(),
                        ddl.first().map(|(lsn, _)| *lsn),
                        ddl.is_empty(),
                    )
                })
            };

            let has_table_data = !paged
                && !self
                    .relational_store
                    .tables
                    .read()
                    .values()
                    .all(|rows| rows.is_empty());
            let has_table_meta = !paged && !self.relational_store.table_meta.read().is_empty();

            // If both logs are empty but stores have data → post-restart, derive deltas from
            // persisted row/edge/vector LSNs instead of replaying a full snapshot.
            if !paged && change_log_empty && ddl_log_empty && (has_table_data || has_table_meta) {
                drop(image_history.take());
                drop(std::mem::replace(
                    &mut image_output,
                    contextdb_core::read_memory::ReadCredit::new(read_memory.clone()),
                ));
                return self.persisted_state_projected(
                    since_lsn,
                    projection,
                    vector_copy_owner,
                    read_memory.clone(),
                );
            }

            // If logs have entries, check the minimum first-LSN across both covers since_lsn
            let min_first_lsn = match (change_first_lsn, ddl_first_lsn) {
                (Some(c), Some(d)) => Some(c.min(d)),
                (Some(c), None) => Some(c),
                (None, Some(d)) => Some(d),
                (None, None) => None, // both empty, stores empty — nothing to serve
            };
            if !paged && min_first_lsn.is_some_and(|min_lsn| min_lsn.0 > since_lsn.0 + 1) {
                // Log doesn't cover since_lsn — derive the delta from persisted state.
                drop(image_history.take());
                drop(std::mem::replace(
                    &mut image_output,
                    contextdb_core::read_memory::ReadCredit::new(read_memory.clone()),
                ));
                return self.persisted_state_projected(
                    since_lsn,
                    projection,
                    vector_copy_owner,
                    read_memory.clone(),
                );
            }

            let (
                mut ddl_entries,
                mut change_entries,
                mut ddl_source,
                mut purge_source,
                mut pending_source,
            ) = self.with_commit_lock(|| -> Result<_> {
                if let Some((_, _, changes, ddl)) = image_history.take() {
                    return Ok((
                        writer_read::DdlHistory::from(ddl),
                        writer_read::ChangeReplay::Resident(changes.into_iter()),
                        writer_read::DdlReplay::Empty,
                        PurgeReplay::empty(),
                        PendingReplay::empty(),
                    ));
                }
                if let Some(history) = resident_history.as_ref() {
                    let changes = writer_read::ChangeReplay::PagedResident(
                        pull_history::ResidentHistoryCursor::new(
                            self.change_log.clone(),
                            since_lsn,
                            history.through,
                            history.change_end,
                            ChangeLogEntry::lsn,
                            change_log_entry_copy_bytes,
                            read_memory.clone(),
                        ),
                    );
                    let ddl = if projection.ddl && self.access_is_admin() {
                        writer_read::DdlReplay::Resident {
                            cursor: pull_history::ResidentHistoryCursor::new(
                                self.ddl_log.clone(),
                                since_lsn,
                                history.through,
                                history.ddl_end,
                                |(lsn, _)| *lsn,
                                |(_, change)| {
                                    std::mem::size_of::<(Lsn, DdlChange)>()
                                        .saturating_add(change.estimated_bytes())
                                },
                                read_memory.clone(),
                            ),
                            peeked: None,
                        }
                    } else {
                        writer_read::DdlReplay::Empty
                    };
                    return Ok((
                        Vec::new().into(),
                        changes,
                        ddl,
                        PurgeReplay::empty(),
                        PendingReplay::resident(
                            history.pending.clone(),
                            since_lsn,
                            history.through,
                            read_memory.clone(),
                            crate::memory_accounting::AccountantHandle::clone(
                                &self.finalization.accountant,
                            ),
                        )?,
                    ));
                }
                if paged {
                    // Both histories are read at the captured root from the
                    // cursor on, one admitted occurrence at a time; nothing
                    // past the page is visited or priced.
                    let (_, root) = self.capture_writer_statement()?;
                    let root = root.ok_or_else(|| {
                        Error::Other("a paged pull requires a captured writer source".into())
                    })?;
                    if since_lsn >= root.lsn {
                        return Ok((
                            Vec::new().into(),
                            writer_read::ChangeReplay::Resident(Vec::new().into_iter()),
                            writer_read::DdlReplay::Empty,
                            PurgeReplay::empty(),
                            PendingReplay::empty(),
                        ));
                    }
                    let owner = || {
                        crate::persistence::writer_source::SourceWindowOwner::Read(
                            read_memory.clone(),
                        )
                    };
                    let changes = writer_read::ChangeReplay::Durable(root.change_log_cursor(
                        owner(),
                        Lsn(since_lsn.0 + 1),
                        root.lsn,
                    )?);
                    let ddl = if projection.ddl && self.access_is_admin() {
                        writer_read::DdlReplay::Durable {
                            cursor: root.ddl_history_cursor(owner(), since_lsn)?,
                            peeked: None,
                        }
                    } else {
                        writer_read::DdlReplay::Empty
                    };
                    let pending =
                        PendingReplay::new(root.clone(), owner(), read_memory.clone(), since_lsn)?;
                    let purge = PurgeReplay {
                        cursor: Some(root.config_cursor_from(
                            owner(),
                            AUTHORITATIVE_PURGE_DELIVERY_PREFIX,
                            &Self::authoritative_purge_delivery_key(Lsn(since_lsn.0 + 1), 0),
                        )?),
                        root: Some(root.clone()),
                        owner: Some(owner()),
                        peeked: None,
                    };
                    return Ok((Vec::new().into(), changes, ddl, purge, pending));
                }
                if projection.bounded {
                    // The two copies taken here, admitted before they exist.
                    let ddl_log = self.ddl_log.read();
                    let ddl_start = if projection.ddl {
                        ddl_log.partition_point(|(lsn, _)| *lsn <= since_lsn)
                    } else {
                        ddl_log.len()
                    };
                    let ddl_bytes = ddl_log[ddl_start..]
                        .iter()
                        .map(|(_, change)| {
                            #[cfg(any(test, feature = "test-seams"))]
                            resident_history_test_seams::note(|work| work.pull_source_entries += 1);
                            std::mem::size_of::<(Lsn, DdlChange)>()
                                .saturating_mul(2)
                                .saturating_add(change.estimated_bytes())
                        })
                        .sum::<usize>();
                    drop(ddl_log);
                    let log = self.change_log.read();
                    let start = log.partition_point(|entry| entry.lsn() <= since_lsn);
                    let change_bytes = log[start..]
                        .iter()
                        .map(|entry| {
                            #[cfg(any(test, feature = "test-seams"))]
                            resident_history_test_seams::note(|work| work.pull_source_entries += 1);
                            change_log_entry_copy_bytes(entry)
                        })
                        .sum::<usize>();
                    drop(log);
                    (projection.admit)(ddl_bytes.saturating_add(change_bytes))?;
                }
                let ddl = if projection.ddl {
                    self.ddl_log_entries_since_unlocked(since_lsn, read_memory.clone())?
                } else {
                    Vec::new().into()
                };
                let changes = self.change_replay_since_unlocked(since_lsn, read_memory.clone())?;
                Ok((
                    ddl,
                    changes,
                    writer_read::DdlReplay::Empty,
                    PurgeReplay::empty(),
                    PendingReplay::empty(),
                ))
            })?;
            if !self.access_is_admin() {
                ddl_entries.clear();
            }
            let (mut ddl_lsn, mut ddl): (Vec<_>, Vec<_>) = ddl_entries.into_iter().unzip();

            let mut rows = Vec::new();
            let mut pending_deletes = Vec::new();
            let mut edges = Vec::new();
            let mut vectors = Vec::new();
            let mut deleted_vector_owners = HashMap::<(String, RowId), Lsn>::new();
            let mut deleted_vector_owner_row_ids = HashMap::<(Vec<u8>, Lsn), RowId>::new();
            let mut inserted_vector_owners = HashMap::<(String, RowId), Lsn>::new();

            // The last source LSN this page has taken in full.
            let mut page_lsn: Option<Lsn> = None;
            let mut page_ended = false;
            while let Some(entry_window) = change_entries.next_entry()? {
                #[cfg(any(test, feature = "test-seams"))]
                resident_history_test_seams::note(|work| work.pull_source_entries += 1);
                // Declared before source row copies, so those rows drop first.
                let _row_outputs = self.writer_output_scope();
                let entry = entry_window.value;
                (projection.checkpoint)()?;
                // Declarations authored before this entry's group are earlier
                // groups of the same page stream: they are taken first.
                if self.take_paged_side_groups(
                    &mut ddl_source,
                    &mut purge_source,
                    &mut pending_source,
                    &mut pending_deletes,
                    read_memory.clone(),
                    entry.lsn().0.checked_sub(1).map(Lsn),
                    projection,
                    &mut page_lsn,
                    &mut ddl_lsn,
                    &mut ddl,
                )? {
                    page_ended = true;
                    break;
                }
                if let Some(page) = projection.page {
                    let lsn = entry.lsn();
                    // Stop only between source-LSN groups, never inside one,
                    // and only once at least one group is kept.
                    if page_lsn.is_some_and(|kept| lsn > kept) && (page.admitted)() >= page.bytes {
                        page.through.set(page_lsn);
                        page_ended = true;
                        break;
                    }
                    page_lsn = Some(
                        page_lsn
                            .unwrap_or(lsn)
                            .max(self.connected_history_end(lsn, read_memory.clone())?),
                    );
                }
                // A declaration in this entry's own group joins the group.
                self.take_paged_side_groups(
                    &mut ddl_source,
                    &mut purge_source,
                    &mut pending_source,
                    &mut pending_deletes,
                    read_memory.clone(),
                    Some(entry.lsn()),
                    projection,
                    &mut page_lsn,
                    &mut ddl_lsn,
                    &mut ddl,
                )?;
                if projection.bounded {
                    // The owner-tracking maps below key each row entry once more.
                    (projection.admit)(match &entry {
                        ChangeLogEntry::RowInsert { table, .. } => {
                            owner_tracking_slot_bytes().saturating_add(held::text(table))
                        }
                        ChangeLogEntry::RowDelete {
                            table, natural_key, ..
                        } => owner_tracking_slot_bytes()
                            .saturating_mul(2)
                            .saturating_add(held::text(table).saturating_mul(2))
                            .saturating_add(natural_key.held_bytes())
                            .saturating_add(held::ALLOCATION_BYTES),
                        _ => 0,
                    })?;
                }
                match entry {
                    ChangeLogEntry::RowInsert { table, row_id, lsn } => {
                        inserted_vector_owners
                            .entry((table.clone(), row_id))
                            .and_modify(|latest| *latest = (*latest).max(lsn))
                            .or_insert(lsn);
                        let snapshot = self.history_change_snapshot_at(
                            lsn,
                            read_memory.clone(),
                            projection.checkpoint,
                        )?;
                        if let Some(row) = self.row_for_change_projected(
                            &table,
                            row_id,
                            lsn,
                            projection,
                            read_memory.clone(),
                        )? && self.row_read_allowed_for_change(
                            &table,
                            &row,
                            snapshot,
                            read_memory.clone(),
                        )? && let Some((natural_key, values)) = self
                            .row_change_values_projected(
                                &table,
                                &row,
                                projection,
                                vector_copy_owner,
                                read_memory.clone(),
                            )?
                        {
                            rows.push(RowChange {
                                table,
                                natural_key,
                                values,
                                deleted: false,
                                lsn,
                                created_at: row.created_at,
                            });
                        }
                    }
                    ChangeLogEntry::RowDelete {
                        table,
                        natural_key,
                        lsn,
                        row_id,
                    } => {
                        deleted_vector_owners.insert((table.clone(), row_id), lsn);
                        deleted_vector_owner_row_ids
                            .insert((sync_identity_key(&table, &natural_key), lsn), row_id);
                        let snapshot = self.history_change_snapshot_at(
                            Lsn(lsn.0.saturating_sub(1)),
                            read_memory.clone(),
                            projection.checkpoint,
                        )?;
                        if !self.access_is_admin() {
                            let Some(row) = self.committed_row_at(
                                &table,
                                row_id,
                                snapshot,
                                read_memory.clone(),
                            )?
                            else {
                                continue;
                            };
                            if !self.row_read_allowed_for_change(
                                &table,
                                &row,
                                snapshot,
                                read_memory.clone(),
                            )? {
                                continue;
                            }
                        }
                        let mut values = HashMap::new();
                        values.insert("__deleted".to_string(), Value::Bool(true));
                        let change = RowChange {
                            table,
                            natural_key,
                            values,
                            deleted: true,
                            lsn,
                            created_at: None,
                        };
                        if projection.bounded {
                            (projection.admit)(answer_list_entry_bytes::<RowChange>(
                                change.held_bytes(),
                            ))?;
                        }
                        rows.push(change);
                    }
                    ChangeLogEntry::EdgeInsert {
                        source,
                        target,
                        edge_type,
                        lsn,
                    } => {
                        if !self.graph_edge_read_allowed_for_change(
                            source,
                            target,
                            &edge_type,
                            self.history_change_snapshot_at(
                                lsn,
                                read_memory.clone(),
                                projection.checkpoint,
                            )?,
                            read_memory.clone(),
                        )? {
                            continue;
                        }
                        let properties = self
                            .edge_properties(source, target, &edge_type, lsn, read_memory.clone())?
                            .unwrap_or_default();
                        let change = EdgeChange {
                            source,
                            target,
                            edge_type,
                            properties,
                            lsn,
                        };
                        if projection.bounded {
                            (projection.admit)(answer_list_entry_bytes::<EdgeChange>(
                                change.held_bytes(),
                            ))?;
                        }
                        edges.push(change);
                    }
                    ChangeLogEntry::EdgeDelete {
                        source,
                        target,
                        edge_type,
                        lsn,
                    } => {
                        if !self.graph_edge_read_allowed_for_change(
                            source,
                            target,
                            &edge_type,
                            self.history_change_snapshot_at(
                                Lsn(lsn.0.saturating_sub(1)),
                                read_memory.clone(),
                                projection.checkpoint,
                            )?,
                            read_memory.clone(),
                        )? {
                            continue;
                        }
                        let mut properties = HashMap::new();
                        properties.insert("__deleted".to_string(), Value::Bool(true));
                        let change = EdgeChange {
                            source,
                            target,
                            edge_type,
                            properties,
                            lsn,
                        };
                        if projection.bounded {
                            (projection.admit)(answer_list_entry_bytes::<EdgeChange>(
                                change.held_bytes(),
                            ))?;
                        }
                        edges.push(change);
                    }
                    ChangeLogEntry::VectorInsert { index, row_id, lsn } => {
                        let snapshot = self.history_change_snapshot_at(
                            lsn,
                            read_memory.clone(),
                            projection.checkpoint,
                        )?;
                        if let Some(row) = self.row_for_change_projected(
                            &index.table,
                            row_id,
                            lsn,
                            projection,
                            read_memory.clone(),
                        )? && self.row_read_allowed_for_change(
                            &index.table,
                            &row,
                            snapshot,
                            read_memory.clone(),
                        )? && let Some((_, values)) = self.row_change_values_projected_at(
                            &index.table,
                            &row,
                            projection,
                            snapshot,
                            vector_copy_owner,
                            read_memory.clone(),
                        )? && let Some(Value::Vector(vector)) = values.get(&index.column)
                        {
                            let change = VectorChange {
                                index,
                                row_id,
                                vector: Vec::new(),
                                lsn,
                            };
                            if projection.bounded {
                                (projection.admit)(answer_list_entry_bytes::<VectorChange>(
                                    change
                                        .held_bytes()
                                        .saturating_add(std::mem::size_of_val(vector.as_slice()))
                                        .saturating_add(held::ALLOCATION_BYTES),
                                ))?;
                            }
                            vectors.push(VectorChange {
                                vector: vector.clone(),
                                ..change
                            });
                        }
                    }
                    ChangeLogEntry::VectorDelete { index, row_id, lsn } => {
                        if self.row_id_read_allowed_for_change(
                            &index.table,
                            row_id,
                            self.history_change_snapshot_at(
                                Lsn(lsn.0.saturating_sub(1)),
                                read_memory.clone(),
                                projection.checkpoint,
                            )?,
                            read_memory.clone(),
                        )? {
                            vectors.push(VectorChange {
                                index,
                                row_id,
                                vector: Vec::new(),
                                lsn,
                            });
                        }
                    }
                }
            }
            // Declarations after the last change-log group are later groups.
            if !page_ended {
                self.take_paged_side_groups(
                    &mut ddl_source,
                    &mut purge_source,
                    &mut pending_source,
                    &mut pending_deletes,
                    read_memory.clone(),
                    None,
                    projection,
                    &mut page_lsn,
                    &mut ddl_lsn,
                    &mut ddl,
                )?;
            }

            // Deduplicate upserts: when a RowDelete is followed by a RowInsert for the same
            // (table, natural_key), the delete is part of an upsert — remove it.
            // Only remove a delete if there is a non-delete entry with a HIGHER LSN
            // (i.e., the insert came after the delete, indicating an upsert).
            // If the insert has a lower LSN, the delete is genuine and must be kept.
            // Key the upsert-pairing by the WHOLE identity: two rows that share only
            // a leading key column are different rows, and folding their delete and
            // insert together would drop one of them.
            (projection.checkpoint)()?;
            if projection.bounded {
                // The upsert pairing below keys every row once more, with its
                // natural key rendered as text, and the vector passes key every
                // vector once more.
                (projection.admit)(
                    rows.iter()
                        .map(row_pairing_entry_bytes)
                        .sum::<usize>()
                        .saturating_add(
                            vectors
                                .iter()
                                .map(|vector| {
                                    CHANGE_PAIRING_SLOT_BYTES
                                        .saturating_add(held::text(&vector.index.table))
                                        .saturating_add(held::text(&vector.index.column))
                                })
                                .sum::<usize>(),
                        ),
                )?;
            }
            let dedup_key = |r: &RowChange| -> (String, String) {
                (r.table.clone(), format!("{:?}", r.natural_key.pairs()))
            };
            let mut connected_positions = HashSet::new();
            if !paged {
                vector_copy_owner.reserve(rows.len().saturating_mul(256))?;
                let mut visited = HashSet::new();
                for row in &rows {
                    if visited.insert(row.lsn)
                        && self
                            .connected_membership_at(row.lsn, read_memory.clone())?
                            .is_some()
                    {
                        connected_positions.insert(row.lsn);
                    }
                }
            }
            let insert_max_lsn: HashMap<(String, String), Lsn> = {
                let mut map: HashMap<(String, String), Lsn> = HashMap::new();
                for r in rows.iter().filter(|r| !r.deleted) {
                    let entry = map.entry(dedup_key(r)).or_insert(Lsn(0));
                    if r.lsn > *entry {
                        *entry = r.lsn;
                    }
                }
                map
            };
            let suppressed_vector_owner_deletes: HashSet<(String, RowId, Lsn)> = rows
                .iter()
                .filter(|row| row.deleted)
                .filter_map(|row| {
                    let insert_lsn = insert_max_lsn.get(&dedup_key(row))?;
                    if *insert_lsn < row.lsn {
                        return None;
                    }
                    let row_id = deleted_vector_owner_row_ids
                        .get(&(sync_identity_key(&row.table, &row.natural_key), row.lsn))?;
                    Some((row.table.clone(), *row_id, row.lsn))
                })
                .collect();
            rows.retain(|r| {
                let key = dedup_key(r);
                if r.deleted {
                    // Keep the delete unless there is a subsequent insert (higher or equal LSN).
                    // Equal LSN means the delete+insert are part of the same upsert transaction.
                    match insert_max_lsn.get(&key) {
                        Some(&insert_lsn) => insert_lsn < r.lsn,
                        None => true,
                    }
                } else {
                    // A full-history replay (`changes_since(Lsn(0))`) walks every
                    // version a row ever had, so an updated row appears once per
                    // version. Keep only its latest, so the changeset carries the
                    // row's CURRENT value once — otherwise a first-writer-wins apply
                    // (`InsertIfNotExists`) would resurrect a superseded value.
                    // A paged replay carries each surviving authored version
                    // in its source group. Collapsing across groups would erase
                    // a retained group after its cursor had consumed it.
                    paged
                        || connected_positions.contains(&r.lsn)
                        || insert_max_lsn.get(&key) == Some(&r.lsn)
                }
            });

            let vector_reinserts: HashSet<(VectorIndexRef, RowId, Lsn)> = vectors
                .iter()
                .filter(|v| !v.vector.is_empty())
                .map(|v| (v.index.clone(), v.row_id, v.lsn))
                .collect();
            vectors.retain(|v| {
                !v.vector.is_empty()
                    || !vector_reinserts.contains(&(v.index.clone(), v.row_id, v.lsn))
            });
            deleted_vector_owners.retain(|owner, delete_lsn| {
                inserted_vector_owners
                    .get(owner)
                    .is_none_or(|insert_lsn| insert_lsn < delete_lsn)
            });
            // Do not send older vector inserts after their owner tombstone: a
            // receiver would otherwise try to attach that history to an owner
            // that the same changeset has already removed. Keep the paired empty
            // VectorDelete so apply can remove the local ANN entry before it
            // stages the relational delete.
            vectors.retain(|vector| {
                vector.vector.is_empty()
                    || deleted_vector_owners
                        .get(&(vector.index.table.clone(), vector.row_id))
                        .is_none_or(|delete_lsn| vector.lsn > *delete_lsn)
            });
            vectors.retain(|vector| {
                !vector.vector.is_empty()
                    || !suppressed_vector_owner_deletes.contains(&(
                        vector.index.table.clone(),
                        vector.row_id,
                        vector.lsn,
                    ))
            });
            (projection.checkpoint)()?;
            self.restore_vector_owner_rows_projected(
                &mut rows,
                &vectors,
                projection,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            // A page that stopped short serves schema history only through its
            // last kept group; later DDL waits for the next page.
            if let Some(through) = projection.page.and_then(|page| page.through.get()) {
                let kept = ddl_lsn.iter().take_while(|lsn| **lsn <= through).count();
                ddl_lsn.truncate(kept);
                ddl.truncate(kept);
            }

            let changes = ChangeSet {
                rows,
                edges,
                vectors,
                ddl,
                ddl_lsn,
            };
            let mut changes = Self::reconcile_durable_pending_deletes_projected(
                changes,
                pending_deletes,
                projection,
            )?;
            if paged {
                (projection.admit)(
                    changes
                        .rows
                        .len()
                        .saturating_mul(std::mem::size_of::<RowChange>()),
                )?;
                changes.rows.sort_by_key(|row| row.lsn);
            }
            let source = DdlProvenanceSource::historical(&changes);
            Ok((changes, source))
        })()
    }

    /// The persisted-state fallback under a history answer's bounds. It runs
    /// only when the change log no longer covers the requested position; its
    /// snapshot is checked and admitted as one piece once it exists.
    /// Take the side groups -- declaration occurrences and purge-delivery
    /// items -- through `upto` (all when `None`) into the page in LSN order,
    /// each admitted before it is kept. A new group once the page has reached
    /// its budget ends the page between groups. Answers whether it ended.
    #[allow(
        clippy::too_many_arguments,
        reason = "each source and output of one page stream is a distinct borrow"
    )]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn take_paged_side_groups(
        &self,
        ddl_source: &mut writer_read::DdlReplay,
        purge_source: &mut PurgeReplay,
        pending_source: &mut PendingReplay,
        pending_deletes: &mut Vec<DurablePendingDelete>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        upto: Option<Lsn>,
        projection: &ChangeProjection<'_>,
        page_lsn: &mut Option<Lsn>,
        ddl_lsn: &mut Vec<Lsn>,
        ddl: &mut Vec<DdlChange>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        loop {
            let next_ddl = ddl_source.peek_lsn()?;
            let next_purge = purge_source.peek_lsn()?;
            let next_pending = pending_source.peek_lsn()?;
            let Some(lsn) = next_ddl
                .into_iter()
                .chain(next_purge)
                .chain(next_pending)
                .min()
            else {
                return Ok(false);
            };
            if upto.is_some_and(|upto| lsn > upto) {
                return Ok(false);
            }
            if let Some(page) = projection.page {
                if page_lsn.is_some_and(|kept| lsn > kept) && (page.admitted)() >= page.bytes {
                    page.through.set(*page_lsn);
                    return Ok(true);
                }
                *page_lsn = Some(
                    page_lsn
                        .unwrap_or(lsn)
                        .max(self.connected_history_end(lsn, read_memory.clone())?),
                );
            }
            if next_ddl == Some(lsn) {
                let Some(window) = ddl_source.next_occurrence()? else {
                    return Ok(false);
                };
                let (lsn, change) = window.value;
                (projection.admit)(answer_list_entry_bytes::<(Lsn, DdlChange)>(
                    change.estimated_bytes(),
                ))?;
                ddl_lsn.push(lsn);
                ddl.push(change);
            } else if next_pending == Some(lsn) {
                let Some(window) = pending_source.next_record()? else {
                    return Ok(false);
                };
                let (value, source) = window.into_parts();
                // Locals drop in reverse order. Keep the decode owner alive
                // until every unmoved field is freed, including on refusal
                // or a generation filter's early exit.
                let _source = source;
                let record = value;
                if record.table_generation
                    != self.durable_lineage_table_generation(&record.table, read_memory.clone())?
                {
                    continue;
                }
                // Transfer the canonical record's selected fields to the
                // answer, funded before the source window can be released.
                (projection.admit)(answer_list_entry_bytes::<DurablePendingDelete>(
                    std::mem::size_of::<RowChange>()
                        .saturating_add(held::text(&record.table))
                        .saturating_add(record.natural_key.held_bytes())
                        .saturating_add(held::map::<String, Value>(1))
                        .saturating_add(held::text("__deleted")),
                ))?;
                pending_deletes.push(DurablePendingDelete {
                    row: RowChange {
                        table: record.table,
                        natural_key: record.natural_key,
                        values: HashMap::from([("__deleted".into(), Value::Bool(true))]),
                        deleted: true,
                        lsn,
                        created_at: None,
                    },
                    local_row_id: record.local_row_id,
                });
            } else if let Some(window) = purge_source.next_item()? {
                // A partial move leaves the window's root and decode credit
                // alive until after the later item binding is destroyed,
                // including when the output admission refuses this copy.
                let item = window.value;
                (projection.admit)(answer_list_entry_bytes::<AuthoritativePurgeDeliveryItem>(
                    held::text(&item.table)
                        .saturating_add(item.natural_key.held_bytes())
                        .saturating_add(
                            item.purged_lineage_roots
                                .iter()
                                .map(|root| held::text(root))
                                .sum::<usize>(),
                        )
                        .saturating_add(item.node_local_predicate.as_ref().map_or(0, Vec::len)),
                ))?;
                if let Some(page) = projection.page {
                    page.purge_items.borrow_mut().push(item);
                }
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn persisted_state_projected(
        &self,
        since_lsn: Lsn,
        projection: &ChangeProjection<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, DdlProvenanceSource)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        (projection.checkpoint)()?;
        // This fallback belongs exclusively to unpaged extraction. Paged
        // pulls enumerate surviving ordered sources and admit each copy there.
        debug_assert!(projection.page.is_none());
        if since_lsn == Lsn(0)
            && let Some(source) = self.retained_relational_source.as_ref()
        {
            let snapshot = self.retained_state_snapshot(
                source,
                projection,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            return self.persisted_state_from_snapshot_projected(
                snapshot,
                projection,
                read_memory.clone(),
            );
        }
        let vector_credit_before = vector_copy_owner.retained_bytes();
        let extracted = self.persisted_state_since_with_ddl_provenance(
            since_lsn,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        (projection.checkpoint)()?;
        if projection.bounded {
            let vector_credit = vector_copy_owner
                .retained_bytes()
                .saturating_sub(vector_credit_before);
            (projection.admit)(change_set_held_bytes(&extracted.0).saturating_sub(vector_credit))?;
        }
        Ok(extracted)
    }

    /// The full-state snapshot of a committed image, whose rows stay in the
    /// store: every current row is read from the retained source, authenticated
    /// as it is visited, and each copy the snapshot keeps -- a row's values,
    /// its vectors, a link, the rendered schema -- is admitted before it is
    /// made, with the projection's checkpoint between rows.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn retained_state_snapshot(
        &self,
        source: &crate::persistence::RetainedReadSource,
        projection: &ChangeProjection<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ChangeSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let snapshot_ceiling = self.current_lsn();
        let acl_snapshot = self.snapshot();
        let metas = {
            let guard = self.relational_store.table_meta.read();
            if projection.bounded {
                (projection.admit)(
                    guard
                        .values()
                        .map(|meta| meta.estimated_bytes().saturating_mul(4))
                        .sum(),
                )?;
            }
            let mut metas = guard
                .iter()
                .map(|(table, meta)| (table.clone(), meta.clone()))
                .collect::<Vec<_>>();
            metas.sort_by(|left, right| left.0.cmp(&right.0));
            metas
        };
        let ddl = if projection.ddl {
            self.full_state_snapshot_ddl()
        } else {
            Vec::new()
        };
        let mut rows = Vec::new();
        let mut vectors = Vec::new();
        for (table, meta) in &metas {
            if natural_key_columns_for_meta(meta).is_none() {
                continue;
            }
            let vector_columns = meta
                .columns
                .iter()
                .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            source.bounded_visit_relational_rows(
                table,
                meta,
                acl_snapshot,
                |row| -> Result<()> {
                    (projection.checkpoint)()?;
                    if row.lsn > snapshot_ceiling
                        || !self.row_read_allowed_for_change(
                            table,
                            row,
                            acl_snapshot,
                            read_memory.clone(),
                        )?
                    {
                        return Ok(());
                    }
                    let Some((natural_key, values)) = self.row_change_values_projected(
                        table,
                        row,
                        projection,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?
                    else {
                        return Ok(());
                    };
                    for column in &vector_columns {
                        if let Some(Value::Vector(vector)) = values.get(column)
                            && !vector.is_empty()
                        {
                            let change = VectorChange {
                                index: VectorIndexRef::new(table.clone(), column.clone()),
                                row_id: row.row_id,
                                vector: Vec::new(),
                                lsn: row.lsn,
                            };
                            if projection.bounded {
                                (projection.admit)(answer_list_entry_bytes::<VectorChange>(
                                    change
                                        .held_bytes()
                                        .saturating_add(std::mem::size_of_val(vector.as_slice()))
                                        .saturating_add(held::ALLOCATION_BYTES),
                                ))?;
                            }
                            vectors.push(VectorChange {
                                vector: vector.clone(),
                                ..change
                            });
                        }
                    }
                    rows.push(RowChange {
                        table: table.clone(),
                        natural_key,
                        values,
                        deleted: false,
                        lsn: row.lsn,
                        created_at: row.created_at,
                    });
                    Ok(())
                },
                read_memory.clone(),
            )?;
        }
        let mut edges = Vec::new();
        self.visit_graph_versions(
            |entry| {
                if entry.deleted_tx.is_some() || entry.lsn > snapshot_ceiling {
                    return Ok(());
                }
                (projection.checkpoint)()?;
                if !self.graph_edge_read_allowed_for_change(
                    entry.source,
                    entry.target,
                    &entry.edge_type,
                    acl_snapshot,
                    read_memory.clone(),
                )? {
                    return Ok(());
                }
                if projection.bounded {
                    (projection.admit)(answer_list_entry_bytes::<EdgeChange>(
                        std::mem::size_of::<EdgeChange>()
                            .saturating_add(held::text(&entry.edge_type))
                            .saturating_add(held::values(&entry.properties)),
                    ))?;
                }
                edges.push(EdgeChange {
                    source: entry.source,
                    target: entry.target,
                    edge_type: entry.edge_type.clone(),
                    properties: entry.properties.clone(),
                    lsn: entry.lsn,
                });
                Ok(())
            },
            read_memory.clone(),
        )?;
        let first_data_lsn = rows
            .iter()
            .map(|row| row.lsn)
            .chain(edges.iter().map(|edge| edge.lsn))
            .chain(vectors.iter().map(|vector| vector.lsn))
            .min()
            .unwrap_or(snapshot_ceiling);
        let snapshot_schema_lsn = if rows.is_empty() && edges.is_empty() && vectors.is_empty() {
            snapshot_ceiling
        } else {
            Lsn(first_data_lsn.0.saturating_sub(1))
        };
        let ddl_lsn = vec![snapshot_schema_lsn; ddl.len()];
        Ok(ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
            ddl_lsn,
        })
    }

    /// Everything a bounded history answer needs from the change log: the
    /// change set and, when asked for, each row's arrival. Unlike the
    /// inspection helper it reports why it could not answer instead of
    /// answering empty.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn projected_changes_with_arrivals(
        &self,
        projection: &ChangeProjection<'_>,
        with_arrivals: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, HashMap<Lsn, Option<Lsn>>)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        (|| {
            let _operation = self.open_operation()?;
            let (base, _) = self.changes_since_base_projected(
                Lsn(0),
                projection,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let pending =
                self.durable_pending_deletes_projected(Lsn(0), projection, read_memory.clone())?;
            let changes =
                Self::reconcile_durable_pending_deletes_projected(base, pending, projection)?;
            (projection.checkpoint)()?;
            if !with_arrivals {
                return Ok((changes, HashMap::new()));
            }
            let arrivals = self.sync_arrivals_for_changes_within(
                &changes,
                projection.checkpoint,
                projection.admit,
                read_memory.clone(),
            )?;
            Ok((changes, arrivals))
        })()
    }

    /// Builds ordinary outbound requests without separating a changed child
    /// from the final state of the rows its declared foreign keys require.
    ///
    /// Only a changeset that actually contains a live dependent row pays for
    /// the closure walk. Tables without declared foreign keys keep the normal
    /// source-LSN batches unchanged. The closure is taken from the current
    /// source state, rather than replaying historical parent versions, so an
    /// update made after a child was first committed cannot make the receiver
    /// observe the stale parent value.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn ordinary_dependency_complete_outbound_units(
        &self,
        changes: ChangeSet,
        confirmed_frontier: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<OutboundSyncUnit>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !changes.ddl.is_empty() {
            changes
                .validate_ddl_lsn_cardinality()
                .map_err(|error| Error::SyncError(error.into()))?;
            // A DDL entry and data authored at its source LSN are one
            // migration: the receiver must never see either half first.  A
            // snapshot can also contain unrelated later data, which retains
            // the ordinary fast path rather than being mislabeled as part of
            // the migration merely because schema was present somewhere in
            // the snapshot.
            let mut migrations = BTreeMap::<Lsn, ChangeSet>::new();
            for (ddl, lsn) in changes.ddl.into_iter().zip(changes.ddl_lsn) {
                let migration = migrations.entry(lsn).or_default();
                migration.ddl.push(ddl);
                migration.ddl_lsn.push(lsn);
            }
            let mut residual = ChangeSet::default();
            for row in changes.rows {
                if let Some(migration) = migrations.get_mut(&row.lsn) {
                    migration.rows.push(row);
                } else {
                    residual.rows.push(row);
                }
            }
            for edge in changes.edges {
                if let Some(migration) = migrations.get_mut(&edge.lsn) {
                    migration.edges.push(edge);
                } else {
                    residual.edges.push(edge);
                }
            }
            for vector in changes.vectors {
                if let Some(migration) = migrations.get_mut(&vector.lsn) {
                    migration.vectors.push(vector);
                } else {
                    residual.vectors.push(vector);
                }
            }

            // One source DDL LSN is one authored migration. DDL-only commits
            // remain separate units just like migrations carrying rows;
            // combining adjacent schema commits would rewrite their source
            // transaction boundaries and ordinals on the receiver.
            let mut units = migrations
                .into_values()
                .map(|changes| OutboundSyncUnit {
                    changes,
                    dependency_complete: true,
                })
                .collect::<Vec<_>>();
            if !residual.is_empty() {
                units.extend(self.dependency_complete_outbound_units(
                    residual,
                    confirmed_frontier,
                    vector_copy_owner,
                    read_memory.clone(),
                )?);
            }
            return Ok(units);
        }
        let snapshot = self.snapshot();
        let confirmed_snapshot = self.history_change_snapshot_at(
            confirmed_frontier,
            read_memory.clone(),
            &unbounded_checkpoint,
        )?;
        let no_deleted = HashSet::new();
        let history = self.try_sync_direction_history_for_changes(&changes, read_memory.clone())?;
        let roots = changes
            .rows
            .iter()
            .filter(|row| !row.deleted)
            .map(|row| {
                self.dependency_row_has_unresolved_parent_at_frontier(
                    row,
                    snapshot,
                    confirmed_snapshot,
                    &no_deleted,
                    &history,
                    vector_copy_owner,
                    read_memory.clone(),
                )
                .map(|needed| needed.then(|| sync_row_identity(row)))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if roots.is_empty() {
            return Ok(vec![OutboundSyncUnit {
                changes,
                dependency_complete: false,
            }]);
        }

        let source_identities = changes
            .rows
            .iter()
            .filter(|row| !row.deleted)
            .map(sync_row_identity)
            .collect::<HashSet<_>>();
        let mut rows = changes
            .rows
            .iter()
            .filter(|row| !row.deleted)
            .cloned()
            .map(|row| (sync_row_identity(&row), row))
            .collect::<HashMap<_, _>>();

        let mut components = Vec::<BTreeSet<Vec<u8>>>::new();
        for root in roots {
            let mut component = BTreeSet::new();
            let mut frontier = vec![root];
            while let Some(identity) = frontier.pop() {
                if !component.insert(identity.clone()) {
                    continue;
                }
                let Some(row) = rows.get(&identity).cloned() else {
                    continue;
                };
                let Some(meta) = self.table_meta(&row.table) else {
                    continue;
                };
                for column in &meta.columns {
                    let Some(reference) = &column.references else {
                        continue;
                    };
                    let Some(value) = row.values.get(&column.name) else {
                        continue;
                    };
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if let Some(parent) = self.current_sync_row_by_columns(
                        &reference.table,
                        std::slice::from_ref(&reference.column),
                        std::slice::from_ref(value),
                        snapshot,
                        &no_deleted,
                        vector_copy_owner,
                        read_memory.clone(),
                    )? {
                        let parent_identity = sync_row_identity(&parent);
                        if history.includes(
                            &parent.table,
                            parent.lsn,
                            &[SyncDirection::Push, SyncDirection::Both],
                        ) && self
                            .visible_row_by_natural_key(
                                &parent.table,
                                &parent.natural_key,
                                confirmed_snapshot,
                                &no_deleted,
                                read_memory.clone(),
                            )?
                            .is_none()
                        {
                            rows.entry(parent_identity.clone()).or_insert(parent);
                            frontier.push(parent_identity);
                        }
                    }
                }
                for reference in &meta.composite_foreign_keys {
                    let values = reference
                        .child_columns
                        .iter()
                        .map(|column| row.values.get(column))
                        .collect::<Option<Vec<_>>>();
                    let Some(values) = values else {
                        continue;
                    };
                    if values.iter().any(|value| matches!(value, Value::Null)) {
                        continue;
                    }
                    if let Some(parent) = self.current_sync_row_by_columns(
                        &reference.parent_table,
                        &reference.parent_columns,
                        &values.into_iter().cloned().collect::<Vec<_>>(),
                        snapshot,
                        &no_deleted,
                        vector_copy_owner,
                        read_memory.clone(),
                    )? {
                        let parent_identity = sync_row_identity(&parent);
                        if history.includes(
                            &parent.table,
                            parent.lsn,
                            &[SyncDirection::Push, SyncDirection::Both],
                        ) && self
                            .visible_row_by_natural_key(
                                &parent.table,
                                &parent.natural_key,
                                confirmed_snapshot,
                                &no_deleted,
                                read_memory.clone(),
                            )?
                            .is_none()
                        {
                            rows.entry(parent_identity.clone()).or_insert(parent);
                            frontier.push(parent_identity);
                        }
                    }
                }
            }

            let mut joined = component;
            let mut index = 0;
            while index < components.len() {
                if components[index].iter().any(|row| joined.contains(row)) {
                    joined.extend(components.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            components.push(joined);
        }

        components.sort_by(|left, right| left.iter().next().cmp(&right.iter().next()));
        let selected = components
            .iter()
            .flat_map(|component| component.iter().cloned())
            .collect::<HashSet<_>>();
        let selected_row_ids = selected
            .iter()
            .filter_map(|identity| rows.get(identity))
            .filter_map(|row| {
                self.visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    snapshot,
                    &no_deleted,
                    read_memory.clone(),
                )
                .ok()
                .flatten()
                .map(|row| row.row_id)
            })
            .collect::<HashSet<_>>();

        let mut units = components
            .into_iter()
            .map(|component| -> Result<OutboundSyncUnit> {
                let rows = component
                    .iter()
                    .filter_map(|identity| rows.get(identity).cloned())
                    .collect::<Vec<_>>();
                // The receiver validates the whole unit before commit. Its
                // transaction is the visibility boundary even when source
                // members originally had different LSNs.
                let component_row_ids = component
                    .iter()
                    .filter_map(|identity| {
                        rows.iter().find(|row| sync_row_identity(row) == *identity)
                    })
                    .filter_map(|row| {
                        self.visible_row_by_natural_key(
                            &row.table,
                            &row.natural_key,
                            snapshot,
                            &no_deleted,
                            read_memory.clone(),
                        )
                        .ok()
                        .flatten()
                        .map(|row| row.row_id)
                    })
                    .collect::<HashSet<_>>();
                let mut vectors = changes
                    .vectors
                    .iter()
                    .filter(|vector| component_row_ids.contains(&vector.row_id))
                    .cloned()
                    .collect::<Vec<_>>();
                let injected_row_ids = component
                    .iter()
                    .filter(|identity| !source_identities.contains(*identity))
                    .filter_map(|identity| {
                        rows.iter().find(|row| sync_row_identity(row) == *identity)
                    })
                    .filter_map(|row| {
                        self.visible_row_by_natural_key(
                            &row.table,
                            &row.natural_key,
                            snapshot,
                            &no_deleted,
                            read_memory.clone(),
                        )
                        .ok()
                        .flatten()
                        .map(|row| row.row_id)
                    })
                    .collect::<HashSet<_>>();
                for row_id in injected_row_ids {
                    for entry in self.vector_store.load_live_entries_for_row(
                        row_id,
                        snapshot,
                        vector_copy_owner,
                    )? {
                        if !vectors.iter().any(|vector| {
                            vector.index == entry.index
                                && vector.row_id == entry.row_id
                                && vector.lsn == entry.lsn
                        }) {
                            vectors.push(VectorChange {
                                index: entry.index,
                                row_id: entry.row_id,
                                vector: entry.vector,
                                lsn: entry.lsn,
                            });
                        }
                    }
                }
                Ok(OutboundSyncUnit {
                    changes: ChangeSet {
                        rows,
                        edges: Vec::new(),
                        vectors,
                        ddl: Vec::new(),
                        ddl_lsn: Vec::new(),
                    },
                    dependency_complete: true,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let residual = ChangeSet {
            rows: changes
                .rows
                .into_iter()
                .filter(|row| !selected.contains(&sync_row_identity(row)))
                .collect(),
            edges: changes.edges,
            vectors: changes
                .vectors
                .into_iter()
                .filter(|vector| !selected_row_ids.contains(&vector.row_id))
                .collect(),
            ddl: changes.ddl,
            ddl_lsn: changes.ddl_lsn,
        };
        if !residual.is_empty() {
            units.push(OutboundSyncUnit {
                changes: residual,
                dependency_complete: false,
            });
        }
        Ok(units)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn changes_since_with_read_memory(
        &self,
        since_lsn: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ChangeSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        (|| {
            let _operation = self.open_operation()?;
            // A change read is funded by the caller's declared reader memory;
            // its stored-record decodes never spend the writer's budget.
            let decode = crate::read_image_memory::ImageMemory::operation_reader(
                self.accountant.clone(),
                read_memory.clone(),
            )?;
            let _decode = decode.enter();
            self.changes_since_under_schema_gate(since_lsn, vector_copy_owner, read_memory.clone())
        })()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn changes_since_with_arrivals_with_read_memory(
        &self,
        since_lsn: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, HashMap<Lsn, Option<Lsn>>)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        (|| {
            let _operation = self.open_operation()?;
            // Match the ordinary history reader: durable decodes retain the
            // caller's supplied child instead of borrowing a writer owner.
            let decode = crate::read_image_memory::ImageMemory::operation_reader(
                self.accountant.clone(),
                read_memory.clone(),
            )?;
            let _decode = decode.enter();
            let output = std::cell::RefCell::new(contextdb_core::read_memory::ReadCredit::new(
                read_memory.clone(),
            ));
            let admit = |bytes| {
                output.borrow_mut().try_grow_for(
                    bytes,
                    "reader",
                    "changes_since_with_arrivals",
                    "Reduce the selected history or raise the declared read memory limit.",
                )
            };
            let checkpoint = || Ok(());
            let release = |bytes| output.borrow_mut().try_shrink(bytes);
            let projection = ChangeProjection {
                checkpoint: &checkpoint,
                admit: &admit,
                release: &release,
                bounded: true,
                ddl: true,
                page: None,
            };
            let pending = self.durable_pending_deletes_projected(
                since_lsn,
                &projection,
                read_memory.clone(),
            )?;
            let (base, _) = self.changes_since_base_projected(
                since_lsn,
                &projection,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let changes =
                Self::reconcile_durable_pending_deletes_projected(base, pending, &projection)?;
            let arrivals = self.sync_arrivals_for_changes_within(
                &changes,
                &checkpoint,
                &admit,
                read_memory.clone(),
            )?;
            self.retain_writer_output(output.into_inner());
            Ok((changes, arrivals))
        })()
    }

    /// Sync-only outbound view.  Unlike the public inspection helper above,
    /// this refuses to omit a corrupt durable delete obligation: sending a
    /// partial delta would silently drop a user-committed deletion.
    /// `page`, when given, is the page budget in bytes and the cell that
    /// receives the last complete source-LSN group served when the selection
    /// stopped short of the end of history (`None` when it covered everything).
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn checked_changes_since_with_arrivals(
        &self,
        since_lsn: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        selection_credit: &mut contextdb_core::read_memory::ReadCredit,
        page: Option<PullPageRequest<'_>>,
    ) -> Result<(ChangeSet, HashMap<Lsn, Option<Lsn>>, DdlProvenanceSource)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        (|| {
            if page.is_some() && self.image_history.is_some() {
                return Err(Error::ReadFailure(
                    contextdb_core::read_contract::ReadFailure::owner_route_unsupported(
                        "sync/paged_pull_from_sealed_image".into(),
                    ),
                ));
            }
            // The executing sync root retains this credit through the selected
            // units, request buffers, retries and cancellation.
            let selection_start = selection_credit.bytes();
            let selection_credit = std::cell::RefCell::new(selection_credit);
            let admitted = || {
                selection_credit
                    .borrow()
                    .bytes()
                    .saturating_sub(selection_start)
            };
            let page = page.map(|request| SelectionPage {
                bytes: request.bytes,
                admitted: &admitted,
                through: request.through,
                purge_items: request.purge_items,
            });
            let admit = |bytes| {
                selection_credit.borrow_mut().try_grow_for(
                    bytes,
                    "reader",
                    "select_sync_changes",
                    "Reduce the selected changes or raise the declared read memory limit.",
                )
            };
            let release = |bytes| selection_credit.borrow_mut().try_shrink(bytes);
            let projection = ChangeProjection {
                checkpoint: &unbounded_checkpoint,
                admit: &admit,
                release: &release,
                bounded: true,
                ddl: true,
                page: page.as_ref(),
            };

            // One operation retains the root and schema through pending deletes,
            // base reconstruction, provenance, and arrivals. Nested helpers
            // reuse that root rather than capture a later publication.
            let _operation = self.open_operation()?;
            let _pull_history = if page.is_some() {
                self.enter_resident_pull_history(read_memory.clone())?
            } else {
                None
            };
            let pending = if page.is_some() {
                Vec::new()
            } else {
                self.durable_pending_deletes_projected(since_lsn, &projection, read_memory.clone())?
            };
            let (base, provenance_source) = self.changes_since_base_projected(
                since_lsn,
                &projection,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            // Pending deletes past a short page's last group wait for its next page.
            let pending = match page.as_ref().and_then(|page| page.through.get()) {
                Some(through) => pending
                    .into_iter()
                    .filter(|pending| pending.row.lsn <= through)
                    .collect(),
                None => pending,
            };
            let changes =
                Self::reconcile_durable_pending_deletes_projected(base, pending, &projection)?;
            let arrivals = self.sync_arrivals_for_changes_within(
                &changes,
                projection.checkpoint,
                projection.admit,
                read_memory.clone(),
            )?;
            Ok((changes, arrivals, provenance_source))
        })()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn sync_arrivals_for_changes(
        &self,
        changes: &ChangeSet,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<HashMap<Lsn, Option<Lsn>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let output = std::cell::RefCell::new(contextdb_core::read_memory::ReadCredit::new(
            read_memory.clone(),
        ));
        let admit = |bytes| {
            output.borrow_mut().try_grow_for(
                bytes,
                "reader",
                "sync_arrivals_for_changes",
                "Reduce the selected changes or raise the declared read memory limit.",
            )
        };
        let arrivals =
            self.sync_arrivals_for_changes_within(changes, &|| Ok(()), &admit, read_memory)?;
        self.retain_writer_output(output.into_inner());
        Ok(arrivals)
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn apply_changes_impl(
        &self,
        mut changes: ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        apply_role: SyncApplyRole,
        receipt: Option<SyncApplyReceipt>,
        dependency_complete: bool,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        hub_local_author: Option<&str>,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        lineage_tenant: Option<&TenantId>,
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
        schema_publication_held: bool,
        delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        push_outcome_key: Option<&SyncPushOutcomeContext>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The caller may replay several independently committed requests.
        // Keep this invocation's copies until it commits or unwinds, then
        // release them before that caller advances to its next request.
        let mut apply_vector_copy_owner = vector_copy_owner.scratch();
        let vector_copy_owner = &mut apply_vector_copy_owner;
        // Received table DDL is serialized with SQL statements too. It does
        // not inherit the permissive local callback path: malformed or
        // unauthorized received DDL remains rejected by the existing
        // fail-closed validation below.
        //
        // A data-only apply starts on the shared side so independent row
        // commits remain concurrent. The plugin may add table DDL to such a
        // ChangeSet, so upgrade only after transformation: release the public
        // operation and shared schema lease, acquire the callback-safe write
        // side in canonical lock order, then reopen the operation. The plugin
        // runs exactly once.
        let entered_with_table_schema = Self::changeset_changes_table_schema(&changes);
        let mut schema_publication = if schema_publication_held {
            None
        } else if entered_with_table_schema {
            Some(self.enter_apply_changes_schema_publication_gate("apply_changes")?)
        } else {
            Some(self.enter_schema_publication_gate(false))
        };
        let mut operation =
            Some(self.open_operation_after_public_tx_control_wait("apply_changes")?);
        // Per I14: the whole batch takes the index-maintenance lock once.
        // Per-row commits reuse the same guard via the per-row apply that
        // runs inside the tx manager's commit_mutex, so no second write
        // acquisition happens for the scope of this call.
        self.relational_store.bump_index_write_lock_count();
        Self::validate_public_changeset_ddl_lsn(&changes)?;
        // Refuse whole authored transactions, never a statement out of one,
        // preserving each retained DDL entry's original signed occurrence, and
        // hold later units on a refused transaction's tables behind it (see
        // `received_policy_plan`). Decided on the delivery as it arrived --
        // before the plugin, which can only remove authenticated rows, and
        // before row provenance is checked against tables a refused
        // transaction never created -- exactly as the receipted push door
        // decides it, so both agree on whether anything may be receipted.
        let arrived_with_ddl = !changes.ddl.is_empty();
        let delivery_source =
            Self::received_delivery_source(received_ddl, receipt.as_ref()).map(str::to_string);
        let delivery_identity = received_ddl
            .map(|received| (received.tenant_id.clone(), received.source_incarnation))
            .or_else(|| {
                receipt
                    .as_ref()
                    .map(|receipt| (receipt.tenant_id.clone(), receipt.incarnation))
            });
        let mut plan = self.received_policy_plan_for_apply(
            &changes,
            received_ddl,
            receipt.as_ref(),
            apply_role,
            hub_local_author,
            delivery_source.as_deref(),
            read_memory.clone(),
        )?;
        Self::hold_complete_policy_data(&mut plan, &changes, dependency_complete);
        let wholly_refused_by_policy = Self::policy_plan_covers_all(&plan, &changes);
        // The hold must survive before the author can observe a terminal refusal.
        self.record_policy_holds(&plan.new_holds)?;
        if apply_role == SyncApplyRole::HubPushApply
            && let (Some(receipt), Some(hub)) = (receipt.as_ref(), hub_local_author)
        {
            self.stage_policy_held_received_data(
                &changes,
                arrivals,
                receipt,
                hub,
                lineages,
                received_ddl,
                read_memory.clone(),
            )?;
            if let Some(received) = received_ddl {
                self.record_refused_schema_units(
                    &plan,
                    received,
                    receipt,
                    hub,
                    read_memory.clone(),
                )?;
            }
        }
        let released_holds = plan.released.clone();
        let keep = &plan.keep_ddl;
        let filtered_received = received_ddl.map(|context| {
            let mut context = context.clone();
            context.entries = context
                .entries
                .into_iter()
                .zip(keep)
                .filter_map(|(entry, keep)| keep.then_some(entry))
                .collect();
            context
        });
        let received_ddl = filtered_received.as_ref();
        changes.ddl = changes
            .ddl
            .into_iter()
            .zip(keep)
            .filter_map(|(ddl, keep)| keep.then_some(ddl))
            .collect();
        changes.ddl_lsn = changes
            .ddl_lsn
            .into_iter()
            .zip(keep)
            .filter_map(|(lsn, keep)| keep.then_some(lsn))
            .collect();
        changes
            .rows
            .retain(|row| !plan.held_data_lsns.contains(&row.lsn));
        changes
            .vectors
            .retain(|vector| !plan.held_data_lsns.contains(&vector.lsn));
        changes
            .edges
            .retain(|edge| !plan.held_data_lsns.contains(&edge.lsn));
        let policy_refusal_conflicts = plan.conflicts;
        if wholly_refused_by_policy && self.access_is_admin() {
            // Every unit of this delivery was refused or held: nothing of it
            // is installed, so nothing may claim it was -- no schema, no
            // acceptance receipt or progress marker. The distinct durable
            // REFUSED schema outcome records the author's answer.
            return Ok(ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: policy_refusal_conflicts,
                new_lsn: self.current_lsn(),
            });
        }
        let authenticated_snapshot = received_ddl.map(|_| changes.clone());
        self.plugin.on_sync_pull(&mut changes)?;
        if let (Some(expected), Some(context)) = (authenticated_snapshot.as_ref(), received_ddl) {
            Self::validate_authenticated_received_plugin_output(expected, &changes, context)?;
            if let Some(tenant_id) = lineage_tenant {
                self.validate_received_row_lineages_with_received_ddl(
                    tenant_id,
                    &changes,
                    lineages,
                    context,
                    read_memory.clone(),
                )?;
            }
        }
        // The plugin is allowed to adjust non-identity values, but it runs
        // before mutation and can otherwise add, rekey, or retarget a row
        // after transport validation. Recheck the final exact row set here.
        if received_ddl.is_none()
            && let Some(tenant_id) = lineage_tenant
        {
            self.validate_received_row_lineages(
                tenant_id,
                &changes,
                lineages,
                read_memory.clone(),
            )?;
        }
        Self::validate_public_changeset_ddl_lsn(&changes)?;
        if !schema_publication_held
            && !entered_with_table_schema
            && Self::changeset_changes_table_schema(&changes)
        {
            drop(operation.take());
            drop(schema_publication.take());
            schema_publication =
                Some(self.enter_apply_changes_schema_publication_gate("apply_changes")?);
            operation = Some(self.open_operation_after_public_tx_control_wait("apply_changes")?);
        }
        let _schema_publication = schema_publication;
        let _operation = operation;
        // A local DROP TABLE logs a trigger tombstone for every trigger on the
        // table, right after the table's own; downstream peers see the trigger
        // go. An unattested delivery that drops a table replays the same way.
        // A signed vector already carries its author's tombstones and is never
        // added to.
        if received_ddl.is_none() && !changes.ddl.is_empty() {
            let (ddl, ddl_lsn) =
                self.with_implicit_trigger_tombstones(&changes.ddl, &changes.ddl_lsn);
            changes.ddl = ddl;
            changes.ddl_lsn = ddl_lsn;
        }
        if !self.access_is_admin() && (arrived_with_ddl || !changes.ddl.is_empty()) {
            if let Some(trigger_ddl) = changes.ddl.iter().find(|ddl| {
                matches!(
                    ddl,
                    DdlChange::CreateTrigger { .. }
                        | DdlChange::CreateTriggerIncludingSync { .. }
                        | DdlChange::DropTrigger { .. }
                )
            }) {
                let operation = match trigger_ddl {
                    DdlChange::CreateTrigger { .. }
                    | DdlChange::CreateTriggerIncludingSync { .. } => {
                        "apply_changes CREATE TRIGGER"
                    }
                    DdlChange::DropTrigger { .. } => "apply_changes DROP TRIGGER",
                    _ => "apply_changes trigger DDL",
                };
                return Err(Error::TriggerRequiresAdmin {
                    operation: operation.to_string(),
                });
            }
            return Err(Error::Other(
                ("sync DDL apply requires an admin database handle".to_string()).into(),
            ));
        }

        let durable_push_outcome = push_outcome_key.map(|context| {
            let mut context = context.clone();
            context
                .conflicts
                .extend(policy_refusal_conflicts.iter().cloned());
            context
        });
        let push_outcome_key = durable_push_outcome.as_ref();
        let add_refusals = |mut result: ApplyResult| {
            result
                .conflicts
                .extend(policy_refusal_conflicts.iter().cloned());
            result
        };

        let preflight_fresh_creator_deletes = self.authenticated_fresh_creator_deletes(
            &changes.rows,
            arrivals,
            receipt.as_ref(),
            lineages,
            read_memory.clone(),
        )?;

        // EVERY arriving unit that carries schema goes through the staged door,
        // whether or not it carries signed provenance. The door it used to take
        // without provenance reshaped the live store item by item -- meta, index
        // storage, row columns and vector registrations -- and appended each
        // item's DDL to the REPLICATED log as that item's last step, so a unit
        // refused half way left the receiver serving a proper subset of an
        // arriving schema and replicated part of it onward. Staging removes the
        // need to take any of that back: a refusal discards the stage and writes
        // nothing at all.
        //
        // A change set with no DDL keeps the direct path: there is no schema to
        // install whole.
        //
        // An unattested change set can carry several source transactions, one
        // per source LSN -- a trigger created at one position, rows written at
        // the next, the trigger dropped after that. Each is its own authored
        // unit and replays whole, in source order: schema units through the
        // staged door, data-only units through the direct door. Staging them
        // as one unit would judge every row against the unit's FINAL schema,
        // so a declaration created and dropped inside it would never reach
        // the log and rows between them would never fire it. A signed or
        // receipted delivery is already exactly one source transaction.
        if received_ddl.is_none()
            && receipt.is_none()
            && !dependency_complete
            && delivery_prepare.is_none()
            && push_outcome_key.is_none()
            && !changes.ddl.is_empty()
        {
            let source_units = changes.clone().split_by_data_lsn();
            if source_units.len() > 1 {
                // The whole change set is judged before any unit installs:
                // relationship checks see every source transaction in order,
                // and the callback gate sees the complete trigger history --
                // a trigger created and dropped around rows needs no callback.
                self.check_disk_budget("sync_pull")?;
                Self::refuse_max_txid_values(&changes)?;
                self.preflight_sync_ddl_mixed_apply(
                    &changes,
                    policies,
                    arrivals,
                    adoption,
                    &preflight_fresh_creator_deletes,
                    read_memory.clone(),
                )?;
                self.preflight_sync_apply_trigger_ready(&changes, read_memory.clone())?;
                let total = self.apply_unattested_source_units(
                    source_units,
                    policies,
                    arrivals,
                    adoption,
                    apply_role,
                    terminal_refusal_context,
                    hub_local_author,
                    lineages,
                    lineage_tenant.is_some(),
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                // Best effort, exactly as for a single installed unit below.
                let _ = self.release_policy_holds(
                    delivery_source.as_deref(),
                    delivery_identity
                        .as_ref()
                        .map(|(tenant, incarnation)| (tenant, *incarnation)),
                    &released_holds,
                );
                return Ok(add_refusals(total));
            }
        }
        let mut source_positions = changes
            .rows
            .iter()
            .map(|row| row.lsn)
            .chain(changes.edges.iter().map(|edge| edge.lsn))
            .chain(changes.vectors.iter().map(|vector| vector.lsn))
            .chain(changes.ddl_lsn.iter().copied());
        let first_position = source_positions.next();
        let ordered_multiple = source_positions.any(|lsn| Some(lsn) != first_position);
        // Ordinary data-only imports must not enter the Legacy full-image
        // preparer while constructor activation is unfinished. Connected units
        // retain their established route; the owning host supplies paging.
        let ordered_stage = ordered_multiple
            && (dependency_complete || matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)));
        if received_ddl.is_some() || !changes.ddl.is_empty() || ordered_stage {
            let _sync_trigger_gate = self.enter_sync_apply_trigger_gate_bypass();
            let holder = Self::received_unit_hold_identity(&changes.ddl_lsn, received_ddl);
            let applied = self
                .commit_received_schema_stage(
                    &changes,
                    received_ddl,
                    lineages,
                    arrivals,
                    adoption,
                    apply_role,
                    receipt,
                    dependency_complete,
                    terminal_refusal_context,
                    hub_local_author,
                    delivery_prepare,
                    push_outcome_key,
                    // The item-by-item door this replaces adjudicated an
                    // unattested delivery with the policies its caller
                    // declared. Routing that delivery through the stage keeps
                    // them; an authenticated unit still adjudicates against the
                    // receiver's own declared policies.
                    received_ddl.is_none().then_some(policies),
                    |vector_copy_owner| {
                        self.check_disk_budget("sync_pull")?;
                        // The receiver keeps either the state it had or the
                        // schema that was sent, never a proper subset (the
                        // design's "Atomic schema installation" row). The WHOLE
                        // unit is priced here, ahead of every item, and a
                        // refusal becomes a durable per-TABLE hold rather than a
                        // bare error that stops every table's sync: later units
                        // for other tables keep applying, units for this table
                        // queue behind it in author order, and the author is
                        // never told this one landed -- nothing acknowledges it,
                        // so it is re-delivered and retried when memory allows.
                        self.refuse_received_unit_behind_a_memory_holdback(&changes.ddl, &holder)?;
                        // Held across the whole preparation this unit goes on
                        // to do, not dropped at the end of this preflight:
                        // `commit_received_schema_stage` enters its scope
                        // right before the detached copy, which hands this
                        // room back to the accountant for the copy's own
                        // per-object charges to retake -- the copy meets the
                        // declared limit on the room the preflight already
                        // found for it, not on a second helping of it.
                        let unit_reservation = self.preflight_sync_apply_memory(
                            &changes,
                            policies,
                            read_memory.clone(),
                            vector_copy_owner,
                        )?;

                        // Pre-scan for TxId overflow so the allocator is untouched on rejection.
                        for row in &changes.rows {
                            for v in row.values.values() {
                                if let Value::TxId(incoming) = v
                                    && incoming.0 == u64::MAX
                                {
                                    return Err(Error::TxIdOverflow {
                                        table: row.table.clone(),
                                        incoming: u64::MAX,
                                    });
                                }
                            }
                        }
                        if dependency_complete {
                            self.preflight_connected_overlay(
                                &changes,
                                policies,
                                arrivals,
                                adoption,
                                &preflight_fresh_creator_deletes,
                                read_memory.clone(),
                            )?;
                        } else {
                            self.preflight_sync_ddl_mixed_apply(
                                &changes,
                                policies,
                                arrivals,
                                adoption,
                                &preflight_fresh_creator_deletes,
                                read_memory.clone(),
                            )?;
                        }
                        self.preflight_sync_apply_trigger_ready(&changes, read_memory.clone())?;
                        Ok(unit_reservation)
                    },
                    || Ok(()),
                    vector_copy_owner,
                    read_memory.clone(),
                )
                .map(Into::into)
                .map(add_refusals);
            if let Err(refusal) = &applied {
                // A unit that did not fit is held whether its PRICE refused it
                // or the private copy of the tables it reshapes did: either
                // way it could not install, and a later unit for the same
                // table must wait behind it rather than install out of the
                // author's order. Recorded here, after the attempt has failed
                // and its stage and copy have been dropped, so the record is
                // written with the copy's memory already returned. Best
                // effort: the refusal is the answer the author gets, and it
                // must not be replaced by a failure to WRITE the hold under
                // the very limit that just refused the unit. A hold that
                // cannot be recorded is re-derived on the next delivery.
                let _ = self.hold_back_received_unit_for_memory(&changes.ddl, &holder, refusal);
            }
            if applied.is_ok() {
                // This unit installed, so whatever memory hold was waiting on
                // its tables is over. BEST EFFORT, exactly as recording the hold
                // is. The unit is committed and durable here, and the author
                // must be told it landed: turning a successful apply into an
                // error because a hold record could not be WRITTEN would stop
                // the pull cursor advancing, have the already-installed unit
                // delivered again, and leave the very hold this call was
                // clearing to refuse every later unit for that table. A hold
                // left behind costs nothing by comparison -- the next delivery
                // for that table is at or past the held position, so it passes
                // the check and clears the record then.
                let _ = self.release_memory_schema_holdback_for_applied_unit(
                    &Self::sync_ddl_unit_tables(&changes.ddl),
                    &holder,
                );
                // Best effort for the same reason: the transaction that
                // releases a hold is installed and durable, and a hold left
                // behind is released again by the next such transaction.
                let _ = self.release_policy_holds(
                    delivery_source.as_deref(),
                    delivery_identity
                        .as_ref()
                        .map(|(tenant, incarnation)| (tenant, *incarnation)),
                    &released_holds,
                );
            }
            return applied;
        }

        self.check_disk_budget("sync_pull")?;
        // Held until every group of this change set has applied or the apply has
        // failed, so the room the unit was priced against is still the unit's
        // when its items take it.
        let unit_reservation = self.preflight_sync_apply_memory(
            &changes,
            policies,
            read_memory.clone(),
            vector_copy_owner,
        )?;

        // Pre-scan for TxId overflow so the allocator is untouched on rejection.
        for row in &changes.rows {
            for v in row.values.values() {
                if let Value::TxId(incoming) = v
                    && incoming.0 == u64::MAX
                {
                    return Err(Error::TxIdOverflow {
                        table: row.table.clone(),
                        incoming: u64::MAX,
                    });
                }
            }
        }
        self.preflight_sync_ddl_mixed_apply(
            &changes,
            policies,
            arrivals,
            adoption,
            &preflight_fresh_creator_deletes,
            read_memory.clone(),
        )?;
        self.preflight_sync_apply_trigger_ready(&changes, read_memory.clone())?;
        let _sync_trigger_gate = self.enter_sync_apply_trigger_gate_bypass();

        let lsn_groups = changes.clone().split_by_data_lsn();
        if delivery_prepare.is_some() && !dependency_complete {
            return Err(crate::custody::canonical::invalid());
        }
        if lsn_groups.len() > 1 && !dependency_complete {
            if receipt.is_some() {
                return Err(Error::SyncError(
                    ("authenticated sync receipt requires one source-LSN group".to_string()).into(),
                ));
            }
            let mut total = ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: Vec::new(),
                new_lsn: self.current_lsn(),
            };
            for group in lsn_groups {
                let _group_outputs = self.writer_output_scope();
                let mut group_vector_copy_owner = vector_copy_owner.scratch();
                let result = self.apply_changes_single_lsn_group(
                    group,
                    policies,
                    arrivals,
                    adoption,
                    apply_role,
                    None,
                    false,
                    terminal_refusal_context,
                    None,
                    lineages,
                    lineage_tenant.is_some(),
                    None,
                    None,
                    unit_reservation.as_ref(),
                    &mut group_vector_copy_owner,
                    read_memory.clone(),
                )?;
                total.applied_rows += result.applied_rows;
                total.skipped_rows += result.skipped_rows;
                total.conflicts.extend(result.conflicts);
                total.new_lsn = result.new_lsn;
            }
            return Ok(add_refusals(total));
        }

        self.apply_changes_single_lsn_group(
            if receipt.is_some() || dependency_complete {
                changes
            } else {
                lsn_groups
                    .into_iter()
                    .next()
                    .expect("split_by_data_lsn always returns at least one group")
            },
            policies,
            arrivals,
            adoption,
            apply_role,
            receipt,
            dependency_complete,
            terminal_refusal_context,
            hub_local_author,
            lineages,
            lineage_tenant.is_some(),
            delivery_prepare,
            push_outcome_key,
            unit_reservation.as_ref(),
            vector_copy_owner,
            read_memory.clone(),
        )
        .map(add_refusals)
    }
}

mod open_and_owner_reads;

mod statement_execution;

mod commit_and_row_writes;

mod received_schema_apply;

mod row_lineage;

mod constraint_validation;

mod tx_reads_and_vectors;

mod authoritative_purge_kernel;

mod bounded_reads;

mod maintenance_and_limits;

mod sync_outbound;

mod sync_apply;

mod construction_and_retention;
#[cfg(any(test, feature = "test-seams"))]
pub use construction_and_retention::*;
#[cfg(not(any(test, feature = "test-seams")))]
pub(crate) use construction_and_retention::*;

/// One scored vector still in contention for a ranked answer: the formula's
/// result and the rows that produced it, so a winner is materialised from
/// what was already read rather than fetched a second time.
///
/// Ordered exactly as the answer is: a candidate that ranks earlier compares
/// `Less`, so a max-heap of these keeps the candidate that ranks last on top,
/// and sorting ascending yields the answer's order.
struct RankedSearchCandidate {
    row_id: RowId,
    vector_score: f32,
    rank: f32,
    anchor: VersionedRow,
    joined: Option<VersionedRow>,
}

struct VectorExplainShape<'a> {
    index: VectorIndexRef,
    k: usize,
    restricted_candidates: bool,
    candidates: Option<&'a PhysicalPlan>,
    search_mode: Option<contextdb_parser::ast::VectorSearchMode>,
    query_expr: &'a Expr,
}

struct VectorPolicyExplainFacts {
    requested_mode: contextdb_core::VectorSearchMode,
    resolved_mode: contextdb_core::VectorSearchMode,
    /// `None` when the partition key is an unbound placeholder: no scope has
    /// been resolved to count over.
    aggregate_allowed_vectors: Option<usize>,
    policy: contextdb_vector::store::ResolvedVectorPolicy,
}

/// The durable startup state a handle was built from, whether that is a
/// live store or a decoded image.
///
/// Definitions, queues, and audit rings are read once at open. Sourcing
/// them through one shape is what keeps a handle over a released image
/// answering the same startup questions, in the same order, as the handle
/// that still holds its file.
pub(crate) enum StartupState<'a> {
    Durable(&'a RedbPersistence),
    Image(&'a CommittedImageStartupState),
}

/// The startup rows of a committed image, kept for the one pass that
/// reads them at open.
/// Where a committed image's history lives until something asks for it, and
/// the charge for it once it has been read.
/// The bounds a committed-history answer puts on the change extraction it
/// is built from: a checkpoint between records, so cancellation and active
/// time stop the work where it is, and admission ahead of every copy the
/// extraction keeps or builds. Admitted bytes stay admitted until the
/// answer's owner settles them against what the finished answer retains;
/// the ordinary extraction runs unbounded.
pub(crate) struct ChangeProjection<'a> {
    pub(crate) checkpoint: &'a dyn Fn() -> Result<()>,
    pub(crate) admit: &'a dyn Fn(usize) -> Result<()>,
    /// Return bytes `admit` took for transient scratch the answer does not keep.
    pub(crate) release: &'a dyn Fn(usize) -> Result<()>,
    pub(crate) bounded: bool,
    /// Whether the answer carries schema history; one that does not never
    /// copies it.
    pub(crate) ddl: bool,
    /// When set, the selection is one page: it stops at a source-LSN group
    /// boundary once its admitted bytes reach the page budget.
    pub(crate) page: Option<&'a SelectionPage<'a>>,
}

/// A page budget for a change selection. The selection stops before the first
/// entry of a new source-LSN group once `admitted()` has reached `bytes`, so an
/// authored transaction is never split, and records the last complete group it
/// kept in `through`. A first group larger than the budget is kept whole.
pub(crate) struct SelectionPage<'a> {
    pub(crate) bytes: usize,
    pub(crate) admitted: &'a dyn Fn() -> usize,
    pub(crate) through: &'a std::cell::Cell<Option<Lsn>>,
    pub(crate) purge_items: &'a std::cell::RefCell<Vec<AuthoritativePurgeDeliveryItem>>,
}

/// One hub pull page: its byte budget, where it ended (`None` when it took
/// everything through the captured ceiling) and the purge-delivery items of
/// its groups, collected in (frontier, ordinal) order.
pub(crate) struct PullPageRequest<'a> {
    pub(crate) bytes: usize,
    pub(crate) through: &'a std::cell::Cell<Option<Lsn>>,
    pub(crate) purge_items: &'a std::cell::RefCell<Vec<AuthoritativePurgeDeliveryItem>>,
}

/// Purge-delivery items for a paged pull, sought from the cursor in their
/// (frontier, ordinal) key order and taken one admitted item at a time.
struct PurgeReplay {
    cursor: Option<crate::persistence::writer_source::WriterReadCursor>,
    root: Option<crate::persistence::writer_source::WriterSnapshotHandle>,
    owner: Option<crate::persistence::writer_source::SourceWindowOwner>,
    peeked: Option<crate::persistence::writer_source::WriterWindow<(Lsn, u32)>>,
}

use crate::sync_types::held;

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static PENDING_DELETION_READ_FAULT: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// One key slot of the vector pairing set, beyond the names it copies.
const CHANGE_PAIRING_SLOT_BYTES: usize = hashed_slot_bytes::<(VectorIndexRef, RowId, Lsn)>();

/// One row's arrival entry.
const ARRIVAL_ENTRY_BYTES: usize = hashed_slot_bytes::<(Lsn, Option<Lsn>)>();

/// Why a committed handle could not read its history: an engine refusal --
/// memory, cancellation, active time, damage found while proving a record --
/// or the store's own diagnosis from the commit-index cross-check the open
/// deferred to this read.
#[derive(Debug)]
pub(crate) enum ImageHistoryError {
    Engine(Error),
    Store {
        path: contextdb_core::error::DiagnosticText,
        diagnosis: crate::persistence::LoadReadImageError,
        owner: contextdb_core::read_memory::ReadMemoryOwner,
    },
}

pub(crate) struct ImageHistory {
    source: crate::persistence::RetainedReadSource,
    load_history: bool,
    loaded: Mutex<Option<LoadedImageHistoryCharge>>,
}

/// What loaded history retains against the reader's memory limit: the
/// history charge itself, and beside it -- for exactly as long -- the charge
/// for `change_log_table_index`/`change_log_lsn_refcounts`, the two aux
/// structures `ensure_image_history_within` derives from it. Both are given
/// back together, whenever this is (there is no path that drops one without
/// the other, because they are always constructed and stored together).
struct LoadedImageHistoryCharge {
    _history: crate::persistence::ReadHistoryCharge,
    _indexes: crate::read_image_memory::ImageMemoryHandle,
}

/// The existing read-image invoices remain in ImageHistory, pinned by this
/// owner until its actual shared buffers are destroyed. This inline owner is
/// part of the admitted finalization control; it creates no new Arc control.
struct LoadedHistoryBuffers {
    history: Arc<ImageHistory>,
    ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    table_index: Arc<RwLock<ChangeLogTableIndex>>,
    lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
    _image_memory: crate::read_image_memory::ImageMemoryHandle,
}

pub(crate) struct CommittedImageStartupState {
    config_values: BTreeMap<String, Vec<u8>>,
    config_source: crate::persistence::RetainedReadSource,
    sink_queues: BTreeMap<String, Vec<(u64, Vec<u8>)>>,
    /// Depth and newest id of each queue whose payloads the image left in
    /// the store.
    sink_queue_extents: BTreeMap<String, crate::persistence::SinkQueueExtent>,
    trigger_audit: Vec<(String, Vec<u8>)>,
}

#[cfg(test)]
mod loaded_vector_supplement_tests;

struct DatabaseConstruction {
    source: *mut Database,
    initialized: [bool; 138],
}

struct DatabaseCleanup {
    source: *mut Database,
    holder_notified: bool,
    handed_over_here: bool,
}

#[derive(Default)]
struct RetentionCandidates {
    tables: HashMap<String, HashSet<(RowId, TxId)>>,
}

#[derive(Debug, Clone)]
struct ProjectedSyncRow {
    table: String,
    natural_key: NaturalKey,
    values: HashMap<String, Value>,
}

#[derive(Debug)]
struct ProjectedSyncApply {
    incoming_values: HashMap<String, Vec<HashMap<String, Value>>>,
    deleted_committed_row_ids: HashMap<String, HashSet<RowId>>,
    // Declaration order releases the maps before their writer admission.
    _incoming_values_owner: SyncParentKeyProjectionOwner,
}

#[derive(Debug)]
struct SyncParentKeyProjectionOwner(OwnedMemoryReservation);

/// The most one distinct LSN's entry in `change_log_lsn_refcounts` occupies:
/// a B-tree leaf of eleven 16-byte entries is about 190 bytes and never less
/// than half full, and interior nodes add a fraction of that again.
const LSN_REFCOUNT_ENTRY_UPPER_BYTES: usize = 48;

/// A change log built to exercise [`build_admitted_change_log_aux_indexes`]
/// directly: `tables` distinct tables whose names are `name_len` bytes long,
/// `rows_per_table` row changes each, every change at its own LSN.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub struct ChangeLogAuxIndexFixtureForTest {
    log: Vec<ChangeLogEntry>,
    memory: crate::read_image_memory::ImageMemoryHandle,
    built: Option<(ChangeLogTableIndex, ChangeLogLsnRefcounts)>,
}

mod compaction_and_ddl_rendering;
pub(crate) use compaction_and_ddl_rendering::*;

#[derive(Clone, Copy)]
enum RetentionPreparation {
    Expiry,
    Prune,
}

/// Per-(table, row_id) tombstone lookup borrowed from the relational store.
#[cfg(test)]
type RowTombstoneIndex<'a> = HashMap<&'a str, HashMap<RowId, Vec<(Lsn, TxId)>>>;
/// Per-(source, edge_type, target) tombstone lookup borrowed from the graph store.
#[cfg(test)]
type EdgeTombstoneIndex<'a> = HashMap<(NodeId, &'a str, NodeId), Vec<(Lsn, TxId)>>;
/// Per-(index, row_id) tombstone lookup borrowed from the materialized vectors.
#[cfg(test)]
type VectorTombstoneIndex = HashMap<(VectorIndexRef, RowId), Vec<(Lsn, TxId)>>;

#[cfg(test)]
mod retention_prune_persistence_tests;

#[cfg(test)]
mod commit_index_reconstruction_tests;

#[cfg(test)]
mod currency_version_compaction_tests;

#[cfg(test)]
mod currency_cleanup_atomicity_tests;

#[cfg(test)]
mod storage_compaction_overlap_tests;

#[cfg(test)]
mod maintenance_resource_tests;

/// Deterministic observation points inside a memory-limit transition, so a
/// concurrency contract can be stated as a measurement instead of an argument.
/// The hook fires on the transitioning thread while another thread inspects the
/// accountant; production carries neither the module nor its fire sites.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) mod memory_limit_transition_seam;

#[cfg(test)]
mod stale_staged_delete_tests;

#[cfg(test)]
mod checkpoint_export_body_tests;

#[cfg(test)]
mod table_size_estimate_tests;

#[cfg(test)]
mod policy_hold_legacy_format_tests;

#[cfg(test)]
#[path = "database/dormant_raw_vector_loader_tests.rs"]
mod dormant_raw_vector_loader_tests;

#[cfg(test)]
#[path = "database/bounded_retention_preparation_tests.rs"]
mod bounded_retention_preparation_tests;
#[cfg(test)]
mod generation_route_admission_tests;
#[cfg(test)]
#[path = "database/snapshot_pin_pool_tests.rs"]
mod snapshot_pin_pool_tests;

#[cfg(test)]
mod constraint_key_funding_tests;
#[cfg(test)]
mod content_size_contract_tests;

#[cfg(test)]
mod cdb141_current_root_witness_tests;

#[cfg(test)]
#[path = "database/cdb141_received_witness_tests.rs"]
mod cdb141_received_witness_tests;
#[cfg(all(test, feature = "test-seams"))]
#[path = "database/checkpointed_commit_floor_tests.rs"]
mod checkpointed_commit_floor_tests;

#[cfg(test)]
mod stable_funder_identity_proof_tests;

#[cfg(test)]
thread_local! {
    static R1_DATABASE_PARTIAL_FIELD_FOR_TEST: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static R1_DATABASE_DROP_PANIC_FOR_TEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

mod native_commit_paths;
#[cfg(test)]
pub(crate) use native_commit_paths::*;

#[cfg(test)]
#[path = "database/d1_initialized_control_proofs.rs"]
pub(crate) mod d1_initialized_control_proofs;

#[cfg(test)]
mod cdb126_reader_history_tests;

#[cfg(test)]
#[path = "database/writecost_reclaim_cancellation_tests.rs"]
mod writecost_reclaim_cancellation_tests;

struct NativeCommitUniqueScratch {
    _columns: HashMap<String, Vec<Vec<String>>>,
    lookup: StagedTupleLookup,
    deleted: HashMap<String, HashSet<RowId>>,
    deleted_rows: HashMap<String, HashMap<RowId, VersionedRow>>,
    _selected_reader_credit: contextdb_core::read_memory::ReadCredit,
    _credit: OwnedMemoryReservation,
}

struct OriginalPurgeNonEventPublication {
    relational: PreparedRelationalPublication,
    change_log: Vec<ChangeLogEntry>,
    change_log_table_index: ChangeLogTableIndex,
    change_log_lsn_refcounts: ChangeLogLsnRefcounts,
    graph: PreparedGraphPublication,
    vector: PreparedVectorPublication,
    accepted_author_memory_keys_removed: Vec<(String, Vec<u8>)>,
    graph_arrival_memory_removed: Vec<(GraphIdentity, DurableGraphArrival)>,
    memory_swap: PreparedMemorySwapAccounting,
}
struct OriginalPurgeNonEventPreparedSet {
    vector_entries: Vec<VectorEntry>,
    vector_partition_identities: Vec<VectorPartitionVersionIdentity>,
    vector_generation_candidates: Vec<VectorPartitionGenerationCandidate>,
    disk_source_provenance: Vec<(String, RowId, Lsn, u8)>,
    lineage_config_owners: Vec<AuthoritativePurgeLineageConfigOwners>,
    durable_sink_entries: Vec<AuthoritativePurgeSinkEntryKey>,
    graph_arrival_config_keys: Vec<String>,
    publication_replacements: OriginalPurgeNonEventPublication,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    survivor_report: OwnedQueryResult,
}

/// A held slice of a store's writer budget, released when dropped.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by sync")]
pub struct WriterMemoryHoldForTest(crate::memory_accounting::OwnedMemoryReservation);
#[cfg(test)]
mod received_row_graph_tests;

#[cfg(test)]
mod staged_graph_order_tests;

// This is only the pending ordinary source image, never a published population
// directory. Its actual copied names and vector controls precede their invoice.
struct OriginalOrdinarySyncBaseline {
    creation_keys: Vec<String>,
    rows: Vec<(String, RowId)>,
    _memory: OwnedMemoryReservation,
}

struct OriginalOrdinaryGraphArrivals {
    markers: Vec<(GraphIdentity, FundedGraphArrival)>,
    _memory: OwnedMemoryReservation,
}

#[cfg(all(test, feature = "test-seams"))]
mod independent_cache_reserve_lifetime_tests;

#[cfg(all(test, feature = "test-seams"))]
pub(crate) mod retained_trace_ownership_tests;

#[cfg(all(test, feature = "test-seams"))]
#[path = "database/ordinary_native_owner_tests.rs"]
pub(crate) mod ordinary_native_owner_tests;

#[cfg(test)]
pub(crate) mod ef_search_metadata_contract_tests;

#[cfg(test)]
#[path = "database/consolidation_metadata_contract_tests.rs"]
pub(crate) mod consolidation_metadata_contract_tests;

#[cfg(all(test, feature = "test-seams"))]
#[path = "database/selected_membership_proof_tests.rs"]
mod selected_membership_proof_tests;

#[cfg(all(test, feature = "test-seams", unix))]
#[path = "database/last_handle_release_tests.rs"]
mod last_handle_release_tests;
