//! Read-only ownership boundary for one committed database-file image.
//!
//! Persistence hydrates the complete image, releases the hydration transaction,
//! redb handle, load-only lock, and reader breadcrumb, then transfers the exact
//! opened descriptor and registered storage root with the owned image. The direct
//! session and its cursors retain that custody and owned memory; they do not yet
//! retain a lazy persistence or blob resolver.

use crate::QueryResult;
use crate::sync_types::{DdlChange, EdgeChange, RowChange, VectorChange};
use contextdb_core::read_contract::{
    CursorPage, DeadlineClock, OwnerReadCancellation, ReadFailure, ReadLimits,
};
use contextdb_core::{ContextId, Lsn, Principal, RowId, ScopeLabel, TxId, Value, VectorIndexRef};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

/// Inputs controlled by route assembly rather than by a file opener.
#[derive(Clone)]
pub struct DirectReaderConfig {
    pub memory_limit: Option<usize>,
    /// Resolved runtime-state directory selected by the session opener.
    pub runtime_dir: Option<PathBuf>,
    pub limits: ReadLimits,
    pub clock: Arc<dyn DeadlineClock>,
    /// The contexts this session declared it reads inside, if it declared any.
    pub contexts: Option<BTreeSet<ContextId>>,
    /// The scope labels this session declared it reads under, if it declared
    /// any.
    pub scope_labels: Option<BTreeSet<ScopeLabel>>,
    /// Who this session declared it reads as, if it declared anyone.
    pub principal: Option<Principal>,
}

impl DirectReaderConfig {
    /// The reader this builds declares nothing and therefore reads the whole
    /// committed image, which is what a direct read has always done. A caller
    /// that narrows says so with [`Self::declaring`].
    pub fn new(limits: ReadLimits, clock: Arc<dyn DeadlineClock>) -> Self {
        Self {
            memory_limit: None,
            runtime_dir: None,
            limits,
            clock,
            contexts: None,
            scope_labels: None,
            principal: None,
        }
    }

    /// The contexts, scopes and principal this reader's session declared.
    /// Every one of them `None` is the undeclared reader `new` already
    /// produces.
    pub fn declaring(
        mut self,
        contexts: Option<BTreeSet<ContextId>>,
        scope_labels: Option<BTreeSet<ScopeLabel>>,
        principal: Option<Principal>,
    ) -> Self {
        self.contexts = contexts;
        self.scope_labels = scope_labels;
        self.principal = principal;
        self
    }
}

/// Identity of the committed image carried by every result and cursor page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectSnapshot {
    pub image_id: [u8; 32],
}

/// Production-computed hashes for every typed family in one owned image.
/// `complete` is the verified identity carried by [`DirectSnapshot`]; the
/// component hashes make omissions observable without teaching tests how to
/// encode persistence state. `f32_vectors`/`sq8_vectors`/`sq4_vectors` and
/// `change_and_ddl` split a vector column's content by what it identifies,
/// not by which column it belongs to: the current stored value per
/// quantization goes into `f32_vectors`/`sq8_vectors`/`sq4_vectors`, while
/// versioned (created-transaction, LSN) vector commitments go into
/// `change_and_ddl` alongside the rest of the change log and DDL history.
/// `complete` hashes all eleven family roots together, so it covers both the
/// current vector family content and the historical vector commitments
/// carried in `change_and_ddl`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectTypedImageDigest {
    /// Current relational row content, keyed by table.
    pub relational: [u8; 32],
    /// Current graph edge content.
    pub graph: [u8; 32],
    /// Current stored F32 vector values, keyed by table/column/row.
    pub f32_vectors: [u8; 32],
    /// Current stored SQ8-quantized vector values, keyed by table/column/row.
    pub sq8_vectors: [u8; 32],
    /// Current stored SQ4-quantized vector values, keyed by table/column/row.
    pub sq4_vectors: [u8; 32],
    /// Declared schema and index structure.
    pub schema_and_indexes: [u8; 32],
    /// Declared sync/conflict/propagation policy.
    pub policy: [u8; 32],
    /// Sync watermark, source, and table-direction state.
    pub sync: [u8; 32],
    /// Change log, DDL history, commit index, and versioned (created-transaction,
    /// LSN) vector commitments — the historical record distinct from the current
    /// vector values in `f32_vectors`/`sq8_vectors`/`sq4_vectors`.
    pub change_and_ddl: [u8; 32],
    /// Declared memory/disk configuration limits.
    pub configuration: [u8; 32],
    /// Event, sink, route, schedule, and maintenance status.
    pub status: [u8; 32],
    /// Hash of all eleven family roots above; the verified whole-image identity.
    pub complete: [u8; 32],
}

/// One owned relational row in the committed image.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectStoredRow {
    pub table: String,
    pub row_id: RowId,
    pub values: BTreeMap<String, Value>,
}

/// One owned graph edge, including all persisted edge properties.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectStoredEdge {
    pub source: Uuid,
    pub target: Uuid,
    pub edge_type: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectVectorQuantization {
    F32,
    Sq8,
    Sq4,
}

/// One decoded vector entry. Quantized stores are reconstructed into their
/// owned queryable values during hydration, never by consulting the file later.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectStoredVector {
    pub index: VectorIndexRef,
    pub row_id: RowId,
    pub quantization: DirectVectorQuantization,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectReferencePropagation {
    pub on_state: String,
    pub set_state: String,
    pub max_depth: u32,
    pub abort_on_failure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectColumnReference {
    pub table: String,
    pub column: String,
    pub propagation: Option<DirectReferencePropagation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectRankPolicy {
    pub sort_key: String,
    pub formula: String,
    pub joined_table: String,
    pub joined_column: String,
}

/// Scope-label constraint as the read door reports it: an exact mirror of the
/// persisted `ScopeLabelKind`, so a consumer can check the scope labels it
/// declared against the labels the store actually knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectScopeLabelKind {
    /// One label set, governing writes.
    Simple { write_labels: Vec<String> },
    /// A read label set and a separate write label set.
    Split {
        read_labels: Vec<String>,
        write_labels: Vec<String>,
    },
}

/// Structured column shape used by the ratified schema output. Blob-like and
/// reference-like ordinary scalars remain `TEXT`; this type introduces no
/// media payload or lazy-reference value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSchemaColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub immutable: bool,
    pub expires: bool,
    pub default: Option<String>,
    pub references: Option<DirectColumnReference>,
    pub quantization: Option<DirectVectorQuantization>,
    /// Ordered local vector-partition key columns. Empty for an
    /// unpartitioned or non-vector column.
    pub partition_key_columns: Vec<String>,
    /// Effective local partition cap; absent for an unpartitioned or
    /// non-vector column.
    pub max_partitions: Option<u32>,
    /// Durable route default for a vector column. Non-vector columns retain
    /// AUTO as an inert value so this owned schema shape stays total.
    pub search_mode: contextdb_core::VectorSearchMode,
    pub rank: Option<DirectRankPolicy>,
    pub scope_label: Option<DirectScopeLabelKind>,
    /// The grant table and grant column a column declared `ACL REFERENCES`
    /// against, so a reader can see the row-level authorization the table
    /// enforces instead of inferring its absence. `propagation` is never set:
    /// an ACL declaration carries no state propagation.
    pub acl_ref: Option<DirectColumnReference>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectIndexDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectIndexColumn {
    pub column: String,
    pub direction: DirectIndexDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSchemaIndex {
    pub name: String,
    pub columns: Vec<DirectIndexColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectStateMachine {
    pub column: String,
    pub transitions: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectRetainPolicy {
    pub window: u64,
    pub unit: String,
    pub seconds: u64,
    pub sync_safe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectPropagationRule {
    Edge {
        edge_type: String,
        direction: String,
        on_state: String,
        set_state: String,
        max_depth: u32,
        abort_on_failure: bool,
    },
    VectorExclusion {
        on_state: String,
    },
    ForeignKey {
        column: String,
        references_table: String,
        references_column: String,
        on_state: String,
        set_state: String,
        max_depth: u32,
        abort_on_failure: bool,
    },
}

/// Full structured schema plus its canonical human DDL rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSchema {
    pub table: String,
    pub immutable: bool,
    pub columns: Vec<DirectSchemaColumn>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<DirectSchemaIndex>,
    pub state_machine: Option<DirectStateMachine>,
    pub retain: Option<DirectRetainPolicy>,
    pub history: Option<String>,
    pub sync_direction: Option<String>,
    pub conflict_policy: Option<String>,
    pub dag_edge_types: Vec<String>,
    pub propagate: Vec<DirectPropagationRule>,
    pub ddl: String,
}

/// The history frontiers an open reader knows without reading its history.
/// The history itself is the on-request [`DirectImageState::ChangeLog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectChangeFrontiers {
    pub current_lsn: Lsn,
    pub committed_watermark: TxId,
    pub next_tx: TxId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectChangeState {
    pub current_lsn: Lsn,
    pub committed_watermark: TxId,
    pub next_tx: TxId,
    pub commit_index: Vec<(Lsn, TxId)>,
    pub rows: Vec<RowChange>,
    pub edges: Vec<EdgeChange>,
    pub vectors: Vec<VectorChange>,
    pub ddl: Vec<DdlChange>,
    pub ddl_lsn: Vec<Lsn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSyncSource {
    pub row_lsn: Lsn,
    pub source_lsn: Option<Lsn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectTableSyncPolicy {
    pub table: String,
    pub direction: String,
    pub conflict_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSyncState {
    pub watermark: Lsn,
    pub sources: Vec<DirectSyncSource>,
    pub tables: Vec<DirectTableSyncPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectConfigurationState {
    pub memory_limit_bytes: Option<usize>,
    pub disk_limit_bytes: Option<u64>,
    pub schemas: Vec<DirectSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectEventTypeStatus {
    pub name: String,
    pub trigger: String,
    pub table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSinkStatus {
    pub name: String,
    pub sink_type: String,
    pub callback_registered: bool,
    pub delivered: u64,
    pub queued: u64,
    pub retried: u64,
    pub uncertain: u64,
    pub permanent_failures: u64,
    pub examined: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectRouteStatus {
    pub name: String,
    pub event_type: String,
    pub sink: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectScheduleStatus {
    pub name: String,
    pub every: String,
    pub callback: String,
    pub callback_registered: bool,
    pub next_fire_at_ms: u64,
    pub last_fire_at_ms: Option<u64>,
    pub fire_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectEventsStatus {
    pub event_types: Vec<DirectEventTypeStatus>,
    pub sinks: Vec<DirectSinkStatus>,
    pub routes: Vec<DirectRouteStatus>,
    pub schedules: Vec<DirectScheduleStatus>,
}

/// Runtime fields use fresh-handle semantics. Retention and currency fields
/// are durable declarations projected from the image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectMaintenanceStatus {
    pub policy: String,
    pub running: bool,
    pub retention_enabled: bool,
    pub currency_compaction_enabled: bool,
    pub active_maintenance_loops: usize,
}

/// Small, owned metadata kept beside the on-demand committed read target.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectOwnedImage {
    pub snapshot: DirectSnapshot,
    pub changes: DirectChangeFrontiers,
    pub sync: DirectSyncState,
    pub configuration: DirectConfigurationState,
    pub events: DirectEventsStatus,
    pub maintenance: DirectMaintenanceStatus,
}

/// A complete ordinary result and its route-neutral encoding.
#[allow(dead_code)]
pub struct DirectQuery {
    pub result: QueryResult,
    pub canonical_bytes: Vec<u8>,
    pub snapshot: DirectSnapshot,
}

/// Metadata operations projected from the owned committed image.
#[derive(Debug, Clone, PartialEq)]
pub enum DirectMetadataRequest {
    DeliveryStatus {
        root_table: String,
    },
    DeliveryOutcome {
        root_table: String,
        root_key: crate::sync_types::NaturalKey,
    },
    Tables,
    Schema {
        table: String,
    },
    /// Plan a statement against the caller's own binding without running
    /// it. The binding travels with the statement: a partition scope resolves
    /// only after its key binds, so an explain that left the binding behind
    /// would describe a scope the caller never named. Values inform route
    /// selection only and are never echoed back.
    Explain {
        sql: String,
        params: HashMap<String, Value>,
    },
    EventsStatus,
    MaintenanceStatus,
    ImageState {
        kind: DirectImageMetadataKind,
    },
}

pub(crate) fn metadata_publication_tables(request: &DirectMetadataRequest) -> BTreeSet<String> {
    match request {
        DirectMetadataRequest::DeliveryStatus { root_table }
        | DirectMetadataRequest::DeliveryOutcome { root_table, .. }
        | DirectMetadataRequest::Schema { table: root_table } => {
            BTreeSet::from([root_table.clone()])
        }
        _ => BTreeSet::from(["*".to_owned()]),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectImageMetadataKind {
    Sync,
    ChangeLog,
    Configuration,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DirectImageState {
    Sync(DirectSyncState),
    ChangeLog(DirectChangeState),
    Configuration(DirectConfigurationState),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DirectMetadataBody {
    DeliveryStatus {
        counts: Option<crate::DeliveryStatusCounts>,
        has_more: bool,
    },
    DeliveryOutcome {
        outcome: Option<crate::DeliveryOutcome>,
        has_more: bool,
    },
    Tables {
        items: Vec<String>,
        has_more: bool,
    },
    Schema {
        schema: DirectSchema,
    },
    Explain {
        sql: String,
        physical_plan: String,
        index: Option<String>,
        vector_search: Option<crate::database::VectorSearchDisclosure>,
    },
    EventsStatus {
        status: DirectEventsStatus,
        has_more: bool,
        continuation: Option<String>,
    },
    MaintenanceStatus {
        status: DirectMaintenanceStatus,
    },
    ImageState {
        state: DirectImageState,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectMetadataResponse {
    pub body: DirectMetadataBody,
    pub canonical_bytes: Vec<u8>,
    pub snapshot: DirectSnapshot,
    /// Where to resume, when this answer was one page of an inventory and more
    /// of it remains. It sits here rather than in the body because the body is
    /// the published document -- the same bytes whoever answered -- and where
    /// a paged read left off belongs to the exchange, not the document.
    pub continuation: Option<String>,
    pub(crate) charge: crate::read_image_memory::AnswerCharge,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectCursorPage {
    pub page: CursorPage,
    pub canonical_bytes: Vec<u8>,
    pub snapshot: DirectSnapshot,
}

pub struct DirectCursorOpen {
    pub cursor: DirectCursor,
    pub first_page: DirectCursorPage,
}

/// The active form retains only owned image-backed resources and bounded pull
/// state. The exhausted form keeps the small snapshot/column identity required
/// to return later empty-success pages; it retains no image or memory charge.
struct DirectCursorSnapshotRetention {
    _image: Arc<DirectOwnedImage>,
    _image_memory: Arc<crate::read_image_memory::ImageMemory>,
    _retained_source: Arc<crate::persistence::RetainedReadSource>,
}

struct DirectCursorMemoryReservation {
    _retained_bytes: usize,
}

/// Dropping these releases the cursor's image and memory charge.
struct DirectCursorOwnedResources {
    _snapshot: Arc<DirectCursorSnapshotRetention>,
    _memory: Arc<DirectCursorMemoryReservation>,
}

enum DirectCursorState {
    Active {
        _resources: DirectCursorOwnedResources,
        load: Arc<dyn crate::executor::ReadExecutionTarget>,
        clock: Arc<dyn DeadlineClock>,
        _opened_at_ms: u64,
        last_activity_at_ms: u64,
        _idle_limit_ms: u64,
        _lifetime_limit_ms: u64,
    },
    /// A drained read keeps answering the page it ended on: the caller asked
    /// where it left off, and it left off at the end.
    Exhausted {
        request: DirectCursorPage,
    },
    Closed,
}

pub struct DirectCursor {
    state: DirectCursorState,
    publication: Arc<crate::persistence::ReadSnapshotRegistrationGuard>,
    publication_scope: crate::executor::ReadPublicationScope,
}

/// Weak, privately constructed evidence for the two production-owned cursor
/// resources. Tests can observe their destruction but cannot manufacture or
/// release either resource.
#[cfg(feature = "test-seams")]
pub struct DirectCursorResourceWitness {
    snapshot: std::sync::Weak<DirectCursorSnapshotRetention>,
    memory: std::sync::Weak<DirectCursorMemoryReservation>,
}

#[cfg(feature = "test-seams")]
impl DirectCursorResourceWitness {
    pub fn snapshot_is_retained(&self) -> bool {
        self.snapshot.upgrade().is_some()
    }

    pub fn memory_is_retained(&self) -> bool {
        self.memory.upgrade().is_some()
    }
}

/// Production-owned phases carry no target reference, so observing one can
/// delay a test without mutating the image, target, or publication state.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HydrationPhase {
    PersistenceReleaseSealed { breadcrumb_path: Option<PathBuf> },
    BackendTargetConstructed { breadcrumb_path: Option<PathBuf> },
    ExternallyUsable { breadcrumb_path: Option<PathBuf> },
}

/// Test-only copies of production-owned hydration phases. In particular,
/// `PersistenceReleaseSealed` can only follow consumption of persistence's
/// privately sealed receipt; `BackendTargetConstructed` can only follow the
/// private target-construction receipt.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HydrationCheckpoint {
    PersistenceReleaseSealed { breadcrumb_path: Option<PathBuf> },
    BackendTargetConstructed { breadcrumb_path: Option<PathBuf> },
    ExternallyUsable { breadcrumb_path: Option<PathBuf> },
}

#[cfg(feature = "test-seams")]
pub trait HydrationObserver: Send + Sync {
    fn checkpoint(&self, checkpoint: HydrationCheckpoint);
}

struct HydrationObservation {
    #[cfg(feature = "test-seams")]
    observer: Option<Arc<dyn HydrationObserver>>,
}

impl HydrationObservation {
    fn disabled() -> Self {
        Self {
            #[cfg(feature = "test-seams")]
            observer: None,
        }
    }

    #[cfg(feature = "test-seams")]
    fn from_test_observer(observer: Arc<dyn HydrationObserver>) -> Self {
        Self {
            observer: Some(observer),
        }
    }

    fn checkpoint(&self, phase: HydrationPhase) {
        #[cfg(feature = "test-seams")]
        if let Some(observer) = self.observer.as_ref() {
            let checkpoint = match phase {
                HydrationPhase::PersistenceReleaseSealed { breadcrumb_path } => {
                    HydrationCheckpoint::PersistenceReleaseSealed { breadcrumb_path }
                }
                HydrationPhase::BackendTargetConstructed { breadcrumb_path } => {
                    HydrationCheckpoint::BackendTargetConstructed { breadcrumb_path }
                }
                HydrationPhase::ExternallyUsable { breadcrumb_path } => {
                    HydrationCheckpoint::ExternallyUsable { breadcrumb_path }
                }
            };
            observer.checkpoint(checkpoint);
            return;
        }
        let _ = phase;
    }
}

/// Aggregate of persistence, bounded-target, and subsystem-owned probes. The
/// direct module does not maintain or self-certify any of these counters.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectReaderCounters {
    pub persistence_read_only_opens: u64,
    pub persistence_source_accesses: u64,
    pub persistence_release_receipts: u64,
    pub persistence_writer_open_attempts: u64,
    pub persistence_companion_mutations: u64,
    pub post_release_source_accesses: u64,
    pub active_cursors: u64,
    pub retained_cursor_bytes: u64,
    pub released_cursor_bytes: u64,
    pub cursor_refusals: u64,
    pub bounded_source_touches: u64,
    pub writable_database_opens: u64,
    pub persistence_writer_opens: u64,
    pub plugin_starts: u64,
    pub sync_client_starts: u64,
    pub cron_worker_starts: u64,
    pub maintenance_worker_starts: u64,
    pub event_delivery_starts: u64,
    pub trigger_callback_starts: u64,
    pub background_worker_starts: u64,
    pub blob_repository_opens: u64,
    pub owner_service_starts: u64,
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectStorageRegistrationObservation {
    pub physical_device: u64,
    pub physical_inode: u64,
    pub database_identity: Option<contextdb_core::read_contract::DatabaseIdentity>,
    pub storage_transaction_id: u64,
    pub storage_root_page: Option<[u8; 8]>,
    pub storage_root_checksum: Option<u128>,
    pub storage_root_length: Option<u64>,
    pub handle_identity: [u8; 16],
    pub process_id: u32,
    pub descriptor_number: u32,
    pub registration_path: Option<PathBuf>,
    pub descriptor_open: bool,
    pub load_lock_held: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectReaderPrerequisite {
    PersistenceLoadReadImage,
    PersistenceSealedReleaseReceipt,
    VerifiedImageIdentityObservation,
    #[allow(dead_code)]
    CursorResourceObservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectStoreDiagnostic {
    MalformedRecord,
    LegacyLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectRepairRequiredCause {
    NonMonotonicCommitIndex,
    MissingCommitIndex,
    InvalidVectorReference,
    RowVectorDivergence,
    WritableSanitizerWouldChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectCursorUnavailable {
    Closed,
}

#[derive(Debug)]
pub enum DirectFileReaderError {
    MissingPrerequisite(DirectReaderPrerequisite),
    LegacyLayout(DirectStoreDiagnostic, String),
    CorruptStore(DirectStoreDiagnostic, String),
    DirectReadRequiresWriter {
        failure: ReadFailure,
        #[allow(dead_code)]
        cause: DirectRepairRequiredCause,
    },
    CursorUnavailable(DirectCursorUnavailable),
    /// There is no store at this path to read.
    StoreNotFound {
        failure: ReadFailure,
    },
    /// Somebody else holds this store's source right now -- in practice a
    /// writer that took ownership between deciding to read the file and
    /// reaching it. Whoever holds it is the one to ask.
    Contended {
        reason: String,
    },
    Cancelled,
    ReadFailure(ReadFailure),
    /// The engine refused the read for a reason that is not a read refusal,
    /// in the words it used.
    ///
    /// This module deliberately does not speak the engine's error vocabulary,
    /// so what travels here is what the engine said, as prose. The engine's
    /// own typed answer is kept beside it by the layer that produced it, and
    /// a route that must report the same fault the same way whichever way it
    /// reached the store picks the typed answer up there.
    Engine(String),
    /// Startup admission failed before a target existed to retain the error.
    MemoryBudget {
        subsystem: String,
        operation: String,
        requested_bytes: usize,
        available_bytes: usize,
        budget_limit_bytes: usize,
        hint: String,
    },
}

impl fmt::Display for DirectFileReaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPrerequisite(prerequisite) => {
                write!(
                    formatter,
                    "direct file reader prerequisite is missing: {prerequisite:?}"
                )
            }
            // The words a writer would have got about the same file travel
            // with the tag, so a reader is not told less about a store nobody
            // can read than the writer beside it.
            Self::LegacyLayout(diagnostic, reason) => {
                write!(
                    formatter,
                    "unsupported direct-read store layout: {diagnostic:?}: {reason}"
                )
            }
            Self::CorruptStore(diagnostic, reason) => {
                write!(
                    formatter,
                    "corrupt direct-read store: {diagnostic:?}: {reason}"
                )
            }
            Self::DirectReadRequiresWriter { .. } => formatter.write_str(
                "direct read requires a writable reopen; close any holder and reopen with --write",
            ),
            Self::CursorUnavailable(state) => {
                write!(formatter, "direct cursor is unavailable: {state:?}")
            }
            Self::StoreNotFound { .. } => {
                formatter.write_str("there is no store at this path to read")
            }
            Self::Contended { reason } => {
                write!(formatter, "the store is held by another opener: {reason}")
            }
            Self::Cancelled => formatter.write_str("direct read was cancelled"),
            Self::ReadFailure(failure) => write!(formatter, "direct read failed: {failure:?}"),
            Self::Engine(reason) => write!(formatter, "direct read failed: {reason}"),
            Self::MemoryBudget {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => write!(
                formatter,
                "memory budget exceeded: {subsystem}/{operation} requested {requested_bytes} bytes, {available_bytes} available of {budget_limit_bytes} budget. Hint: {hint}"
            ),
        }
    }
}

impl std::error::Error for DirectFileReaderError {}

/// Internal owner of one released committed image and the shared bounded
/// execution target built from it.
pub struct DirectFileReader {
    image: Arc<DirectOwnedImage>,
    target: Arc<dyn crate::executor::ReadExecutionTarget>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    #[allow(dead_code)]
    verified_digest: DirectTypedImageDigest,
    image_memory: Arc<crate::read_image_memory::ImageMemory>,
    retained_source: Arc<crate::persistence::RetainedReadSource>,
}

/// Privately constructible proof that target construction, owned-image
/// identity verification, and all fallible target setup have completed. The
/// observer never receives this value or a reference to its fields.
struct BackendTargetConstructionReceipt {
    target: Arc<dyn crate::executor::ReadExecutionTarget>,
    image: Arc<DirectOwnedImage>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    verified_digest: DirectTypedImageDigest,
    image_memory: Arc<crate::read_image_memory::ImageMemory>,
    retained_source: Arc<crate::persistence::RetainedReadSource>,
}

impl BackendTargetConstructionReceipt {
    fn publish(self) -> Result<DirectFileReader, DirectFileReaderError> {
        if self.verified_digest.complete != self.image.snapshot.image_id {
            return Err(DirectFileReaderError::MissingPrerequisite(
                DirectReaderPrerequisite::VerifiedImageIdentityObservation,
            ));
        }
        Ok(DirectFileReader {
            image_memory: self.image_memory,
            image: self.image,
            target: self.target,
            limits: self.limits,
            clock: self.clock,
            verified_digest: self.verified_digest,
            retained_source: self.retained_source,
        })
    }
}

fn construct_backend_neutral_target(
    pending_image: crate::persistence::ReadPersistenceImage,
    retained_source: Arc<crate::persistence::RetainedReadSource>,
    config: DirectReaderConfig,
) -> Result<BackendTargetConstructionReceipt, DirectFileReaderError> {
    let limits = config.limits;
    let constructed = pending_image.into_committed_image(
        Arc::clone(&retained_source),
        config.memory_limit,
        config.contexts,
        config.scope_labels,
        config.principal,
    )?;
    Ok(BackendTargetConstructionReceipt {
        image_memory: constructed.memory,
        target: constructed.target,
        image: constructed.image,
        limits,
        clock: config.clock,
        verified_digest: constructed.digest,
        retained_source,
    })
}

fn handoff_to_backend_neutral_reader(
    pending_image: crate::persistence::ReadPersistenceImage,
    release_receipt: crate::persistence::ReadPersistenceReleaseReceipt,
    retained_source: Arc<crate::persistence::RetainedReadSource>,
    config: DirectReaderConfig,
    observation: HydrationObservation,
) -> Result<DirectFileReader, DirectFileReaderError> {
    if release_receipt.source_accesses() == 0 {
        return Err(DirectFileReaderError::MissingPrerequisite(
            DirectReaderPrerequisite::PersistenceSealedReleaseReceipt,
        ));
    }
    let breadcrumb_path = release_receipt.breadcrumb_path().map(Path::to_path_buf);
    if let Some(path) = breadcrumb_path.as_deref() {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) | Ok(_) => {
                return Err(DirectFileReaderError::MissingPrerequisite(
                    DirectReaderPrerequisite::PersistenceSealedReleaseReceipt,
                ));
            }
        }
    }

    // Persistence can construct this sealed value only after every source
    // table/transaction/database handle is dropped and its best-effort
    // breadcrumb, when one exists, is removed. Consuming it is the release
    // boundary; a missing breadcrumb is valid and carries no cleanup burden.
    drop(release_receipt);
    observation.checkpoint(HydrationPhase::PersistenceReleaseSealed {
        breadcrumb_path: breadcrumb_path.clone(),
    });

    // No target observation is possible until this fallible call returns its
    // private receipt. The current narrow stub returns the typed missing seam.
    let constructed = construct_backend_neutral_target(pending_image, retained_source, config)?;
    observation.checkpoint(HydrationPhase::BackendTargetConstructed {
        breadcrumb_path: breadcrumb_path.clone(),
    });

    // Constructing `DirectFileReader` consumes the private receipt. External
    // usability is observed only after that final release transition.
    let reader = constructed.publish()?;
    observation.checkpoint(HydrationPhase::ExternallyUsable { breadcrumb_path });
    Ok(reader)
}

fn consume_released_persistence_load(
    load: crate::persistence::ReadPersistenceLoad,
    config: DirectReaderConfig,
    observation: HydrationObservation,
) -> Result<DirectFileReader, DirectFileReaderError> {
    let (pending_image, release_receipt, retained_source): (
        crate::persistence::ReadPersistenceImage,
        crate::persistence::ReadPersistenceReleaseReceipt,
        Arc<crate::persistence::RetainedReadSource>,
    ) = load.into_parts();
    handoff_to_backend_neutral_reader(
        pending_image,
        release_receipt,
        retained_source,
        config,
        observation,
    )
}

#[allow(dead_code)]
fn execute_on_backend_neutral_target(
    reader: &DirectFileReader,
    sql: &str,
    params: &HashMap<String, Value>,
    cancellation: &OwnerReadCancellation,
) -> Result<DirectQuery, DirectFileReaderError> {
    let constructed = reader.target.read_query(
        sql,
        params,
        reader.limits,
        Arc::clone(&reader.clock),
        cancellation,
    )?;
    let result = DirectQuery {
        result: constructed.0,
        canonical_bytes: constructed.1,
        snapshot: reader.image.snapshot,
    };
    publish_direct_result(
        reader.retained_source.registration(),
        &constructed.2,
        result,
    )
}

fn publish_direct_result<T>(
    registration: &Arc<crate::persistence::ReadSnapshotRegistrationGuard>,
    scope: &crate::executor::ReadPublicationScope,
    value: T,
) -> Result<T, DirectFileReaderError> {
    registration
        .publish_bounded_result(scope, value)
        .map_err(|error| match error {
            contextdb_core::Error::ReadFailure(failure) => {
                DirectFileReaderError::ReadFailure(failure)
            }
            error => DirectFileReaderError::Engine(error.to_string()),
        })
}

fn open_cursor_on_backend_neutral_target(
    reader: &DirectFileReader,
    sql: &str,
    params: &HashMap<String, Value>,
    rows: NonZeroUsize,
    cancellation: &OwnerReadCancellation,
) -> Result<DirectCursorOpen, DirectFileReaderError> {
    let mut limits = reader.limits;
    limits.cursor_page_rows = rows.get() as u64;
    let constructed = Arc::clone(&reader.target).open_read_cursor(
        sql,
        params,
        limits,
        Arc::clone(&reader.clock),
        cancellation,
        reader.image.snapshot,
    )?;
    let observation = reader.clock.now_ms();
    let opened = DirectCursorOpen {
        cursor: DirectCursor {
            state: match constructed.2 {
                Some(request) => DirectCursorState::Exhausted { request },
                None => DirectCursorState::Active {
                    _resources: DirectCursorOwnedResources {
                        _snapshot: Arc::new(DirectCursorSnapshotRetention {
                            _image: Arc::clone(&reader.image),
                            _image_memory: Arc::clone(&reader.image_memory),
                            _retained_source: Arc::clone(&reader.retained_source),
                        }),
                        _memory: Arc::new(DirectCursorMemoryReservation {
                            _retained_bytes: constructed.1.canonical_bytes.len(),
                        }),
                    },
                    load: constructed.0,
                    clock: Arc::clone(&reader.clock),
                    _opened_at_ms: observation,
                    last_activity_at_ms: observation,
                    _idle_limit_ms: limits.cursor_idle_ms,
                    _lifetime_limit_ms: limits.cursor_lifetime_ms,
                },
            },
            publication: Arc::clone(reader.retained_source.registration()),
            publication_scope: constructed.3.clone(),
        },
        first_page: constructed.1,
    };
    publish_direct_result(
        reader.retained_source.registration(),
        &constructed.3,
        opened,
    )
}

fn fetch_backend_neutral_cursor(
    cursor: &mut DirectCursor,
    rows: Option<NonZeroUsize>,
    cancellation: &OwnerReadCancellation,
) -> Result<DirectCursorPage, DirectFileReaderError> {
    let load = match &cursor.state {
        DirectCursorState::Active { load, .. } => Arc::clone(load),
        DirectCursorState::Exhausted { request } => {
            return publish_direct_result(
                &cursor.publication,
                &cursor.publication_scope,
                request.clone(),
            );
        }
        DirectCursorState::Closed => {
            return Err(DirectFileReaderError::CursorUnavailable(
                DirectCursorUnavailable::Closed,
            ));
        }
    };
    let constructed = match load.read_cursor_pages(rows, cancellation) {
        Ok(constructed) => constructed,
        // A withdrawn fetch consumes no continuation position and releases
        // nothing: the read is exactly where the caller left it.
        Err(DirectFileReaderError::Cancelled) => return Err(DirectFileReaderError::Cancelled),
        Err(error) => {
            // A refusal that answered the REQUEST rather than the read leaves
            // the suspended read where it was, and the target that holds it
            // says so. The cursor then survives its own refusal, which is what
            // makes "ask for a smaller page" an escape the caller can take.
            if !load.read_cursor_retains_continuation() {
                cursor.state = DirectCursorState::Closed;
            }
            return Err(error);
        }
    };
    let published = match publish_direct_result(&cursor.publication, &constructed.2, constructed.0)
    {
        Ok(published) => published,
        Err(error) => {
            let _ = load.close_read_cursor();
            cursor.state = DirectCursorState::Closed;
            return Err(error);
        }
    };
    cursor.publication_scope = constructed.2;
    cursor.note_activity();
    if let Some(request) = constructed.1 {
        cursor.state = DirectCursorState::Exhausted { request };
    }
    Ok(published)
}

impl DirectFileReader {
    /// Current bytes charged to this committed-image reader's accountant.
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.image_memory
            .accountant
            .usage()
            .used
            .try_into()
            .unwrap_or(!0)
    }

    /// The execution target this reader already holds for the committed
    /// image it opened.
    ///
    /// A caller that must report the same fault the same way whichever way it
    /// reached the store asks the target directly, because this module
    /// deliberately does not speak the engine's error vocabulary and can only
    /// relay what happened as prose. Nothing here is created or opened: it is
    /// the same target every read through this reader already runs on.
    pub(crate) fn execution_target(&self) -> Arc<dyn crate::executor::ReadExecutionTarget> {
        Arc::clone(&self.target)
    }

    pub(crate) fn publish_bounded_result<T>(
        &self,
        scope: &crate::executor::ReadPublicationScope,
        value: T,
    ) -> contextdb_core::Result<T> {
        self.retained_source
            .registration()
            .publish_bounded_result(scope, value)
    }

    pub fn open(
        path: impl AsRef<Path>,
        config: DirectReaderConfig,
    ) -> Result<Self, DirectFileReaderError> {
        Self::open_inner(path.as_ref(), config, HydrationObservation::disabled())
    }

    pub(crate) fn from_owner_snapshot(
        path: &Path,
        config: DirectReaderConfig,
        registration: crate::persistence::ReadSnapshotRegistrationGuard,
    ) -> Result<Self, DirectFileReaderError> {
        let load = crate::persistence::load_read_image_at_registration(
            path,
            config.memory_limit,
            config.runtime_dir.as_deref(),
            Some(registration),
        )
        .map_err(|error| error.into_direct_reader_error())?;
        consume_released_persistence_load(load, config, HydrationObservation::disabled())
    }

    fn open_inner(
        path: &Path,
        config: DirectReaderConfig,
        observation: HydrationObservation,
    ) -> Result<Self, DirectFileReaderError> {
        let load = crate::persistence::load_read_image_with_memory_in_runtime_dir(
            path,
            config.memory_limit,
            config.runtime_dir.as_deref(),
        )
        .map_err(|error| error.into_direct_reader_error())?;
        consume_released_persistence_load(load, config, observation)
    }

    #[allow(dead_code)]
    pub fn execute(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> Result<DirectQuery, DirectFileReaderError> {
        let cancellation = OwnerReadCancellation::new();
        self.execute_with_cancellation(sql, params, &cancellation)
    }

    #[allow(dead_code)]
    pub fn execute_with_cancellation(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<DirectQuery, DirectFileReaderError> {
        execute_on_backend_neutral_target(self, sql, params, cancellation)
    }

    /// Answer one metadata question from the beginning.
    #[allow(dead_code)]
    pub fn metadata(
        &self,
        request: DirectMetadataRequest,
    ) -> Result<DirectMetadataResponse, DirectFileReaderError> {
        self.metadata_from(request, None)
    }

    /// Answer one metadata question, resuming an inventory strictly after the
    /// item a previous answer left off at.
    ///
    /// An inventory that does not fit the caller's byte ceiling arrives a page
    /// at a time, and the page says where it stopped; handing that back here
    /// continues from exactly there. A question that answers in one piece
    /// ignores nothing -- it never issues a continuation, so there is never
    /// one to hand back.
    pub fn metadata_from(
        &self,
        request: DirectMetadataRequest,
        continuation: Option<&str>,
    ) -> Result<DirectMetadataResponse, DirectFileReaderError> {
        self.metadata_from_with_cancellation(request, continuation, &OwnerReadCancellation::new())
    }

    pub(crate) fn metadata_from_with_cancellation(
        &self,
        request: DirectMetadataRequest,
        continuation: Option<&str>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<DirectMetadataResponse, DirectFileReaderError> {
        let tables = metadata_publication_tables(&request);
        let started_ms = self.clock.now_ms();
        let response = self.target.read_metadata(
            request,
            &self.image,
            self.limits,
            Arc::clone(&self.clock),
            continuation,
            cancellation,
        )?;
        let publication = crate::executor::ReadPublicationScope::bounded(
            self.image.changes.current_lsn,
            tables,
            self.limits,
            Arc::clone(&self.clock),
            cancellation.clone(),
            started_ms,
        );
        self.retained_source
            .registration()
            .publish_bounded_result(&publication, response)
            .map_err(|error| match error {
                contextdb_core::Error::ReadFailure(failure) => {
                    DirectFileReaderError::ReadFailure(failure)
                }
                contextdb_core::Error::ReadCancelled => DirectFileReaderError::Cancelled,
                error => DirectFileReaderError::Engine(error.to_string()),
            })
    }

    #[allow(dead_code)]
    pub fn open_cursor(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        rows: NonZeroUsize,
    ) -> Result<DirectCursorOpen, DirectFileReaderError> {
        let cancellation = OwnerReadCancellation::new();
        self.open_cursor_with_cancellation(sql, params, rows, &cancellation)
    }

    pub fn open_cursor_with_cancellation(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        rows: NonZeroUsize,
        cancellation: &OwnerReadCancellation,
    ) -> Result<DirectCursorOpen, DirectFileReaderError> {
        open_cursor_on_backend_neutral_target(self, sql, params, rows, cancellation)
    }
}

impl DirectCursor {
    #[allow(dead_code)]
    pub fn fetch(
        &mut self,
        rows: Option<NonZeroUsize>,
    ) -> Result<DirectCursorPage, DirectFileReaderError> {
        let cancellation = OwnerReadCancellation::new();
        self.fetch_with_cancellation(rows, &cancellation)
    }

    pub fn fetch_with_cancellation(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> Result<DirectCursorPage, DirectFileReaderError> {
        fetch_backend_neutral_cursor(self, rows, cancellation)
    }

    /// Closing an active cursor releases it. Closing an exhaustion-drained
    /// cursor is a no-op; closing an explicitly closed, refused, or expired
    /// cursor returns the frozen closed-cursor diagnostic.
    pub fn close(&mut self) -> Result<(), DirectFileReaderError> {
        match &self.state {
            DirectCursorState::Active { load, .. } => {
                let load = Arc::clone(load);
                self.state = DirectCursorState::Closed;
                load.close_read_cursor()
            }
            DirectCursorState::Exhausted { .. } => Ok(()),
            DirectCursorState::Closed => Err(DirectFileReaderError::CursorUnavailable(
                DirectCursorUnavailable::Closed,
            )),
        }
    }

    /// Whether this cursor still holds the suspended read it was opened with.
    ///
    /// A refusal that ended the read released it here, and a caller holding the
    /// handle has no other way to tell: the next fetch or close would only
    /// answer that the cursor is gone.
    pub fn is_live(&self) -> bool {
        match self.state {
            DirectCursorState::Active { .. } | DirectCursorState::Exhausted { .. } => true,
            DirectCursorState::Closed => false,
        }
    }

    fn note_activity(&mut self) {
        if let DirectCursorState::Active {
            clock,
            last_activity_at_ms: observation,
            ..
        } = &mut self.state
        {
            *observation = clock.now_ms();
        }
    }
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod test_seams {
    use std::path::Path;
    use std::sync::Arc;

    pub use super::{
        DirectChangeFrontiers, DirectChangeState, DirectColumnReference, DirectConfigurationState,
        DirectCursor, DirectCursorOpen, DirectCursorPage, DirectCursorResourceWitness,
        DirectCursorUnavailable, DirectEventTypeStatus, DirectEventsStatus, DirectFileReader,
        DirectFileReaderError, DirectImageMetadataKind, DirectImageState, DirectIndexColumn,
        DirectIndexDirection, DirectMaintenanceStatus, DirectMetadataBody, DirectMetadataRequest,
        DirectMetadataResponse, DirectOwnedImage, DirectPropagationRule, DirectQuery,
        DirectRankPolicy, DirectReaderConfig, DirectReaderCounters, DirectReaderPrerequisite,
        DirectReferencePropagation, DirectRepairRequiredCause, DirectRetainPolicy,
        DirectRouteStatus, DirectScheduleStatus, DirectSchema, DirectSchemaColumn,
        DirectSchemaIndex, DirectScopeLabelKind, DirectSinkStatus, DirectSnapshot,
        DirectStateMachine, DirectStorageRegistrationObservation, DirectStoreDiagnostic,
        DirectStoredEdge, DirectStoredRow, DirectStoredVector, DirectSyncSource, DirectSyncState,
        DirectTableSyncPolicy, DirectTypedImageDigest, DirectVectorQuantization,
        HydrationCheckpoint, HydrationObserver,
    };

    pub use crate::read_image::{AnswerEncodingHookGuard, while_encoding_answers_for_test};
    pub use crate::read_work_probe::{
        ReadWorkCounters, ReadWorkObservation, ReadWorkObservationScope, ReadWorkPhase,
    };
    pub use contextdb_core::read_contract::OwnerReadCancellation as DirectCancellation;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CancellationOperation {
        Execute,
        CursorOpen,
        CursorFetch,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CancellationTokenObservation {
        pub operation: CancellationOperation,
        pub identity: u64,
        pub initially_cancelled: bool,
    }

    pub fn open_for_test(
        path: impl AsRef<Path>,
        config: DirectReaderConfig,
    ) -> Result<DirectFileReader, DirectFileReaderError> {
        DirectFileReader::open(path, config)
    }

    pub fn open_with_hydration_observer_for_test(
        path: impl AsRef<Path>,
        config: DirectReaderConfig,
        observer: Arc<dyn HydrationObserver>,
    ) -> Result<DirectFileReader, DirectFileReaderError> {
        DirectFileReader::open_inner(
            path.as_ref(),
            config,
            super::HydrationObservation::from_test_observer(observer),
        )
    }

    pub fn reset_counters_for_test() -> Result<(), DirectFileReaderError> {
        crate::read_probe::reset();
        Ok(())
    }

    /// Run `operation` with this thread's own bounded-source-touch counter in
    /// force, and hand back the count that operation alone produced.
    ///
    /// `bounded_source_touches` (read by [`counters_for_test`]) is process-wide
    /// by design (`crate::read_probe`'s own doc comment: "what has this
    /// process done", not "what has THIS read done"), so a proof that diffs it
    /// before and after a call is measuring every other read the process runs
    /// concurrently, not only this one. `read_session.rs`'s owner/file routes
    /// already avoid that by installing a session-scoped counter for the span
    /// of one operation (`with_route_source_counter`); this does the same for
    /// a bare `DirectFileReader` call, which has no `ReadSession` to install
    /// one for it.
    pub fn with_isolated_bounded_source_touches_for_test<T>(
        operation: impl FnOnce() -> T,
    ) -> (T, u64) {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let previous =
            crate::read_probe::install_session_source_counter(Some(Arc::clone(&counter)));
        let result = operation();
        crate::read_probe::install_session_source_counter(previous);
        (result, counter.load(std::sync::atomic::Ordering::SeqCst))
    }

    pub fn counters_for_test() -> Result<DirectReaderCounters, DirectFileReaderError> {
        let observed = crate::read_probe::observed();
        Ok(DirectReaderCounters {
            persistence_read_only_opens: observed.persistence_read_only_opens,
            persistence_source_accesses: observed.persistence_source_accesses,
            persistence_release_receipts: observed.persistence_release_receipts,
            persistence_writer_open_attempts: observed.persistence_writer_open_attempts,
            persistence_companion_mutations: observed.persistence_companion_mutations,
            post_release_source_accesses: observed.post_release_source_accesses,
            active_cursors: observed
                .active_cursors
                .saturating_sub(observed.closed_cursors),
            retained_cursor_bytes: observed.retained_cursor_bytes,
            released_cursor_bytes: observed.released_cursor_bytes,
            cursor_refusals: observed.cursor_refusals,
            bounded_source_touches: observed.bounded_source_touches,
            writable_database_opens: observed.writable_database_opens,
            persistence_writer_opens: observed.persistence_writer_opens,
            plugin_starts: observed.plugin_starts,
            sync_client_starts: observed.sync_client_starts,
            cron_worker_starts: observed.cron_worker_starts,
            maintenance_worker_starts: observed.maintenance_worker_starts,
            event_delivery_starts: observed.event_delivery_starts,
            trigger_callback_starts: observed.trigger_callback_starts,
            background_worker_starts: observed.background_worker_starts,
            blob_repository_opens: observed.blob_repository_opens,
            owner_service_starts: observed.owner_service_starts,
        })
    }

    pub fn cancellation_observations_for_test()
    -> Result<Vec<CancellationTokenObservation>, DirectFileReaderError> {
        Ok(crate::read_probe::observed_cancellations()
            .into_iter()
            .map(|observed| CancellationTokenObservation {
                operation: match observed.operation {
                    crate::read_probe::CANCELLATION_EXECUTE => CancellationOperation::Execute,
                    crate::read_probe::CANCELLATION_CURSOR_OPEN => {
                        CancellationOperation::CursorOpen
                    }
                    _ => CancellationOperation::CursorFetch,
                },
                identity: observed.identity,
                initially_cancelled: observed.initially_cancelled,
            })
            .collect())
    }

    pub fn image_memory_for_test(
        reader: &DirectFileReader,
    ) -> (
        crate::memory_accounting::MemoryUsage,
        usize,
        std::sync::Weak<dyn Send + Sync>,
    ) {
        let erased: Arc<dyn Send + Sync> = reader.image_memory.clone();
        (
            reader.image_memory.accountant.usage(),
            reader.image_memory.held(),
            Arc::downgrade(&erased),
        )
    }

    /// Read the immutable identity already attached to this reader. No scan,
    /// projection, digest calculation or extra reader is opened by this observer.
    pub fn snapshot_for_test(reader: &DirectFileReader) -> DirectSnapshot {
        reader.image.snapshot
    }

    pub fn storage_registration_for_test(
        reader: &DirectFileReader,
    ) -> DirectStorageRegistrationObservation {
        let observed = reader.retained_source.registration().observation();
        let root = observed.snapshot.storage_snapshot.user_root;
        DirectStorageRegistrationObservation {
            physical_device: observed.snapshot.physical_identity.0,
            physical_inode: observed.snapshot.physical_identity.1,
            database_identity: observed.snapshot.database_identity,
            storage_transaction_id: observed.snapshot.storage_snapshot.transaction_id,
            storage_root_page: root.map(|root| root.page),
            storage_root_checksum: root.map(|root| root.checksum),
            storage_root_length: root.map(|root| root.length),
            handle_identity: observed.snapshot.handle_identity,
            process_id: observed.snapshot.process.process_id,
            descriptor_number: observed.snapshot.descriptor_number,
            registration_path: observed.path,
            descriptor_open: observed.descriptor_open,
            load_lock_held: observed.load_lock_held,
        }
    }

    pub fn charged_admissions_for_test(reader: &DirectFileReader) -> usize {
        reader.image_memory.accountant.admitted_bytes_for_test()
    }

    /// Ask one metadata question under a caller-held cancellation, as a
    /// reading route does, so a proof can cancel the request mid-answer.
    pub fn metadata_with_cancellation_for_test(
        reader: &DirectFileReader,
        request: DirectMetadataRequest,
        cancellation: &DirectCancellation,
    ) -> Result<DirectMetadataResponse, DirectFileReaderError> {
        reader.metadata_from_with_cancellation(request, None, cancellation)
    }

    /// Make the reader's next history answer take the persisted-state
    /// fallback: its history is loaded, then its logs are dropped from
    /// memory. The store file is untouched.
    pub fn force_history_log_gap_for_test(reader: &DirectFileReader) {
        reader.target.force_history_log_gap_for_test();
    }

    /// Make the reader's next history answer take the persisted-state
    /// fallback with its schema history intact: its history is loaded, then
    /// only its change log is dropped from memory. The store is untouched.
    pub fn force_change_log_gap_for_test(reader: &DirectFileReader) {
        reader.target.force_change_log_gap_for_test();
    }

    /// The memory an answer holds against its reader's limit while it is
    /// held.
    pub fn held_answer_bytes_for_test(response: &DirectMetadataResponse) -> usize {
        response.charge.held()
    }

    /// Start the reader's high-water mark again from what it holds now.
    pub fn reset_charged_high_water_for_test(reader: &DirectFileReader) {
        reader.image_memory.accountant.reset_high_water_for_test();
    }

    pub fn charged_high_water_for_test(reader: &DirectFileReader) -> usize {
        reader.image_memory.accountant.high_water_for_test()
    }

    /// Bytes this reader's session accountant took through the writer's
    /// unconditional retention door, `charge_retained`.
    pub fn retained_charge_bytes_for_test(reader: &DirectFileReader) -> usize {
        reader
            .image_memory
            .accountant
            .retained_charge_bytes_for_test()
    }

    /// Read completed generation load/eviction receipts from the production
    /// execution target. Taking this snapshot performs no lifecycle work.
    pub fn vector_passive_activity_for_test(
        reader: &DirectFileReader,
    ) -> contextdb_vector::VectorPassiveActivityCounters {
        reader.target.vector_passive_activity_for_test()
    }

    pub fn vector_store_for_test(reader: &DirectFileReader) -> Arc<contextdb_vector::VectorStore> {
        reader.target.vector_store_for_test()
    }

    /// The bytes this reader's current statement is holding in its shared
    /// authenticated proof-node/leaf set (`content_proof`) (first), and how
    /// many of those bytes are actually accounted for by a session charge or
    /// the reader's free floor (second).
    pub fn retained_content_proof_bytes_for_test(reader: &DirectFileReader) -> (usize, usize) {
        reader
            .retained_source
            .retained_content_proof_bytes_for_test()
    }

    /// Give back this reader's session-scoped shared proof-page set
    /// (`content_proof`), as ending the statement whose retained pages it
    /// holds does in production. Releases whatever charge those pages still
    /// carry back to the declared session.
    pub fn clear_statement_proofs_for_test(reader: &DirectFileReader) {
        reader.retained_source.clear_statement_proofs();
    }

    /// The authenticated chunks this reader's resident cache is holding,
    /// their bytes, and how many of those bytes are charged to the declared
    /// session -- the cache that persists across statements on the same
    /// reader, distinct from one statement's own `content_proof` set.
    pub fn resident_proof_chunks_for_test(reader: &DirectFileReader) -> (usize, usize, usize) {
        reader.retained_source.resident_proof_chunks_for_test()
    }

    /// The bytes this reader's own open statement is charged for directly
    /// through its own admission (e.g. a leaf-locator entry the resident set
    /// does not cover), separate from whatever the resident cache charges or
    /// floor-covers for the same decoded pages. Released together with the
    /// resident set's own charge only when the statement itself ends
    /// (`clear_statement_proofs_for_test`); dropping an answer built from
    /// this statement does not release it.
    pub fn statement_own_charge_for_test(reader: &DirectFileReader) -> usize {
        reader.retained_source.statement_own_charge_for_test()
    }

    /// Drop every resident proof chunk WITHOUT resetting this reader's own
    /// open statement's retained proof-page set, so a page the statement
    /// copied out of residency stays reachable at exactly its pre-eviction
    /// bytes while the resident set's charge for it disappears.
    pub fn evict_resident_proof_chunks_only_for_test(reader: &DirectFileReader) -> usize {
        reader
            .retained_source
            .evict_resident_proof_chunks_only_for_test()
    }

    /// Force the retained ordered-run cache to disappear after the stated
    /// number of cached postings. This test-only control exercises the real
    /// source-key fallback in the bounded kernel.
    pub fn arm_verified_ordered_run_eviction_for_test(reader: &DirectFileReader, postings: usize) {
        reader
            .retained_source
            .arm_verified_ordered_run_eviction_for_test(postings);
    }

    pub fn owned_image_for_test(
        reader: &DirectFileReader,
    ) -> Result<DirectOwnedImage, DirectFileReaderError> {
        let mut image = (*reader.image).clone();
        image.sync = reader.target.committed_sync_state(
            &|| Ok(()),
            &crate::read_image_memory::ImageMemory::charging(Arc::clone(
                &reader.image_memory.accountant,
            )),
        )?;
        Ok(image)
    }

    pub fn typed_digest_for_test(
        image: &DirectOwnedImage,
    ) -> Result<DirectTypedImageDigest, DirectFileReaderError> {
        crate::read_image::typed_digest_for_test(image)
            .map_err(|error| DirectFileReaderError::Engine(error.to_string()))
    }

    pub fn verified_image_digest_for_test(
        reader: &DirectFileReader,
    ) -> Result<DirectTypedImageDigest, DirectFileReaderError> {
        if reader.verified_digest.complete != reader.image.snapshot.image_id {
            return Err(DirectFileReaderError::MissingPrerequisite(
                DirectReaderPrerequisite::VerifiedImageIdentityObservation,
            ));
        }
        Ok(reader.verified_digest.clone())
    }

    pub fn verified_image_digest_wire_for_test(
        reader: &DirectFileReader,
    ) -> Result<String, DirectFileReaderError> {
        fn hexadecimal(bytes: &[u8; 32]) -> String {
            const DIGITS: &[u8; 16] = b"0123456789abcdef";
            let mut encoded = String::with_capacity(64);
            for byte in bytes {
                encoded.push(DIGITS[(byte >> 4) as usize] as char);
                encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
            }
            encoded
        }

        let digest = verified_image_digest_for_test(reader)?;
        Ok([
            ("relational", digest.relational),
            ("graph", digest.graph),
            ("f32", digest.f32_vectors),
            ("sq8", digest.sq8_vectors),
            ("sq4", digest.sq4_vectors),
            ("schema_indexes", digest.schema_and_indexes),
            ("policy", digest.policy),
            ("sync", digest.sync),
            ("change_ddl", digest.change_and_ddl),
            ("configuration", digest.configuration),
            ("status", digest.status),
            ("complete", digest.complete),
        ]
        .into_iter()
        .map(|(family, bytes)| format!("{family}={}", hexadecimal(&bytes)))
        .collect::<Vec<_>>()
        .join(";"))
    }

    pub fn canonical_metadata_bytes_for_test(
        body: &DirectMetadataBody,
    ) -> Result<Vec<u8>, DirectFileReaderError> {
        crate::read_contract::encode_metadata_body(body)
            .map_err(|error| DirectFileReaderError::Engine(error.to_string()))
    }

    pub fn cursor_resource_witness_for_test(
        cursor: &DirectCursor,
    ) -> Result<DirectCursorResourceWitness, DirectFileReaderError> {
        match &cursor.state {
            super::DirectCursorState::Active { _resources, .. } => {
                Ok(DirectCursorResourceWitness {
                    snapshot: Arc::downgrade(&_resources._snapshot),
                    memory: Arc::downgrade(&_resources._memory),
                })
            }
            super::DirectCursorState::Exhausted { .. } | super::DirectCursorState::Closed => {
                Err(DirectFileReaderError::MissingPrerequisite(
                    DirectReaderPrerequisite::CursorResourceObservation,
                ))
            }
        }
    }
}

#[cfg(all(test, not(feature = "test-seams")))]
mod no_test_seams_contract {
    use super::{DirectFileReader, DirectReaderConfig};
    use contextdb_core::Value;
    use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
    use std::collections::HashMap;
    use std::sync::Arc;

    struct ManualClock;

    impl DeadlineClock for ManualClock {
        fn now_ms(&self) -> u64 {
            0
        }

        fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
            Box::pin(async {})
        }
    }

    #[test]
    fn production_cfg_open_and_execute_the_same_owned_image_path() {
        let root = tempfile::tempdir().expect("no-feature direct-reader scratch directory");
        let mut seed_hasher = blake3::Hasher::new();
        seed_hasher.update(b"contextdb-direct-file-no-feature-runtime-seed-v1");
        seed_hasher.update(root.path().as_os_str().as_encoded_bytes());
        seed_hasher.update(&std::process::id().to_le_bytes());
        let seed = seed_hasher.finalize().to_hex().to_string();
        eprintln!("DIRECT_FILE_FIXTURE_SEED no-test-seams={seed}");
        let token = &seed[..20];
        let table = format!("no_feature_{token}");
        let path = root.path().join(format!("store_{token}.db"));

        let database = crate::Database::open(&path).expect("seed no-feature store");
        database
            .execute(
                &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"),
                &HashMap::new(),
            )
            .expect("declare no-feature table");
        let id = i64::from_le_bytes(
            seed_hasher.finalize().as_bytes()[..8]
                .try_into()
                .expect("eight-byte runtime id"),
        ) & i64::MAX;
        let payload = format!("owned-{seed}");
        database
            .execute(
                &format!("INSERT INTO {table} (id, payload) VALUES ($id, $payload)"),
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("payload".to_owned(), Value::Text(payload.clone())),
                ]),
            )
            .expect("insert no-feature row");
        database.close().expect("close no-feature writer");

        let limits = ReadLimits {
            result_rows: 64,
            result_bytes: 1024 * 1024,
            work: 10_000,
            active_ms: 10_000,
            memory: 4 * 1024 * 1024,
            cursor_page_rows: 8,
            cursor_page_bytes: 512 * 1024,
            cursor_idle_ms: 5_000,
            cursor_lifetime_ms: 10_000,
        };
        let reader = DirectFileReader::open(
            &path,
            DirectReaderConfig::new(limits, Arc::new(ManualClock)),
        )
        .expect("production cfg hydrates the direct image");
        let result = reader
            .execute(
                &format!("SELECT id, payload FROM {table} WHERE id = $id"),
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("production cfg executes through the bounded target");
        assert_eq!(result.result.columns, vec!["id", "payload"]);
        assert_eq!(
            result.result.rows,
            vec![vec![Value::Int64(id), Value::Text(payload)]]
        );
    }
}
