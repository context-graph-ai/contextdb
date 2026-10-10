#![allow(clippy::too_many_arguments)] // Existing compatibility and test seam shape.
use crate::database::{
    Database, InsertRowResult, MaintenancePolicy, OwnedQueryResult, QueryResult, QueryTrace,
    UpdateReplacementContext, UpsertIntentDetails, VectorSearchDisclosure, rank_index_name,
};
pub(crate) use crate::memory_accounting::VectorLoadReservationTransfer;
use crate::owned_cursor_page::OwnedCursorPage as CursorPage;
use crate::rank_formula::RankFormula;
use crate::sync_types::{DdlChange, natural_key_from_row_values};
use contextdb_core::read_contract::{
    DeadlineClock, OwnerReadCancellation, ReadFailure, ReadLimits,
};
use contextdb_core::*;
use contextdb_parser::ast::{
    AlterAction, BinOp, ColumnRef, Cte, DataType, Expr, Literal, SelectStatement,
    SetDiskLimitValue, SetMemoryLimitValue, SortDirection, Statement, UnaryOp, WindowBindingId,
};
use contextdb_planner::{
    DeletePlan, GraphStepPlan, InsertPlan, OnConflictPlan, PhysicalPlan, PurgePlan, UpdatePlan,
    plan,
};
use roaring::RoaringTreemap;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

// This never reaches a caller: the public entrypoint catches it and reruns
// the complete local DDL validation/projection after another schema writer
// released the allocation lock.
const RETRY_LOCAL_SCHEMA_PROJECTION: &str = "__contextdb_internal_retry_local_schema_projection__";

pub(crate) mod bounded;

/// One suspended cursor's snapshot pin: the snapshot it reads, the idle
/// window its registration is judged by, and the registration holding both.
pub(crate) type BoundedCursorSnapshotPin = (
    SnapshotId,
    Lsn,
    Arc<crate::database::CursorIdleWindow>,
    Option<crate::persistence::OwnerCursorStoragePin>,
    Box<dyn Send + Sync>,
    Option<Arc<crate::persistence::WriterSnapshot>>,
);
pub(crate) type BoundedReadSnapshotPin = (
    SnapshotId,
    Lsn,
    Box<dyn Send + Sync>,
    Option<Arc<crate::persistence::WriterSnapshot>>,
);

/// Exact lineage a completed read may expose at its publication boundary.
#[derive(Clone)]
pub(crate) struct ReadPublicationScope {
    pub(crate) snapshot_lsn: Lsn,
    pub(crate) tables: BTreeSet<String>,
    // Committed operator state does not need the maintenance commit mutex.
    // Purge publication and lineage validation still enclose its response.
    pub(crate) inspection: bool,
    pub(crate) budget: ReadPublicationBudget,
}

#[derive(Clone)]
pub(crate) struct ReadPublicationBudget {
    pub(crate) clock: Arc<dyn DeadlineClock>,
    pub(crate) cancellation: OwnerReadCancellation,
    pub(crate) started_ms: u64,
    pub(crate) active_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadPublicationDeadline {
    Active(std::time::Duration),
    Elapsed,
}

impl ReadPublicationBudget {
    /// The first monotonic millisecond at which the strict active-time
    /// comparison can refuse this publication.
    pub(crate) fn deadline_ms(&self) -> u64 {
        self.started_ms
            .saturating_add(self.active_ms)
            .saturating_add(1)
    }

    pub(crate) fn elapsed(&self) -> bool {
        matches!(self.remaining_wait(), ReadPublicationDeadline::Elapsed)
    }

    /// A native blocking fallback for a timer future that completed before
    /// the authoritative clock reached its deadline. The same fixed deadline
    /// is used on every wake, so spurious wakes cannot extend the budget. An
    /// elapsed deadline is a distinct state and can never be mistaken for an
    /// unbounded wait.
    pub(crate) fn remaining_wait(&self) -> ReadPublicationDeadline {
        let now_ms = self.clock.now_ms();
        if now_ms.saturating_sub(self.started_ms) > self.active_ms {
            return ReadPublicationDeadline::Elapsed;
        }
        ReadPublicationDeadline::Active(std::time::Duration::from_millis(
            self.deadline_ms().saturating_sub(now_ms).max(1),
        ))
    }

    pub(crate) fn exceeded_failure(&self) -> ReadFailure {
        ReadFailure::owner_limit_exceeded(contextdb_core::read_contract::OwnerLimitExceededDetail {
            limit: contextdb_core::read_contract::ReadFailureLimit::ActiveMs,
            value: self.active_ms,
            required: None,
            statement: None,
        })
    }
}

impl std::fmt::Debug for ReadPublicationBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadPublicationBudget")
            .field("started_ms", &self.started_ms)
            .field("active_ms", &self.active_ms)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ReadPublicationScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadPublicationScope")
            .field("snapshot_lsn", &self.snapshot_lsn)
            .field("tables", &self.tables)
            .field("budget", &self.budget)
            .finish()
    }
}

impl ReadPublicationScope {
    pub(crate) fn bounded(
        snapshot_lsn: Lsn,
        tables: BTreeSet<String>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: OwnerReadCancellation,
        started_ms: u64,
    ) -> Self {
        Self {
            snapshot_lsn,
            tables,
            inspection: false,
            budget: ReadPublicationBudget {
                clock,
                cancellation,
                started_ms,
                active_ms: limits.active_ms,
            },
        }
    }

    pub(crate) fn refreshed(&self, cancellation: OwnerReadCancellation) -> Self {
        let mut refreshed = self.clone();
        refreshed.budget.started_ms = refreshed.budget.clock.now_ms();
        refreshed.budget.cancellation = cancellation;
        refreshed
    }
}

/// The copy a caller takes of one stored vector, with the bytes it charged
/// for that copy.
pub(crate) type BoundedRowVectorClone = (Vec<f32>, usize);

/// Scored vector candidates, with the bytes their scores retain.
pub(crate) type BoundedVectorCandidates = (Vec<(RowId, f32)>, usize);

/// A complete maintained answer, or the explicit reason the caller must not
/// publish graph rows. The vector layer keeps the detailed maintenance
/// reason; this seam keeps the route decision that the SQL operator must
/// honour for the already-resolved search mode.
#[derive(Debug)]
pub(crate) enum BoundedVectorSearchOutcome {
    CompleteEmpty,
    Complete(
        BoundedVectorCandidates,
        contextdb_vector::mem::BoundedVectorSearchTrace,
    ),
    ExactRequired(contextdb_vector::mem::BoundedVectorSearchTrace),
    Unavailable,
    Incomplete,
}

/// The store a bounded read runs against.
///
/// The kernel reaches every source through this seam, so a read is described
/// by the questions it asks rather than by which handle answers them. A live
/// engine handle and a read-only image hydrated from a file are one shape
/// here, which is what keeps both reading routes on the same planner, the
/// same statistics, and the same physical decisions rather than on two
/// projections that drift.
///
/// Callbacks arrive erased and every fallible source answer carries the
/// kernel's own refusal type. The underlying store methods are generic over
/// the caller's error only because they sit below this module; this kernel is
/// their sole caller, so the seam fixes that error and stays usable behind a
/// shared pointer, which is what a suspended cursor retains between fetches.
// Source signatures mirror the store methods this seam fronts, one for one,
// so a source cannot answer a differently shaped question than the store.
#[allow(clippy::too_many_arguments)]
/// What a transaction a handle has open changes about what its own reads see
/// of one table.
///
/// The uncapped read path performs this merge in one pass over the whole
/// table; a bounded read cannot, because every row it touches has to be
/// charged before it is touched. So the cheap half -- which committed rows the
/// transaction hides, and where its own rows are -- is captured once here, and
/// the rows themselves are pulled one at a time and charged like any other.
#[derive(Debug, Default)]
pub(crate) struct TransactionTableOverlay {
    /// Committed rows the transaction has staged for deletion by identity.
    pub(crate) deleted: std::collections::HashSet<RowId>,
    /// Deletions the transaction staged by value rather than by identity.
    pub(crate) delete_predicates: Vec<contextdb_tx::RelationalDeletePredicate>,
    /// Immutable table-scoped row versions and maintained index order.
    /// Capturing a statement clones the owner; later write-set mutations
    /// reserve their replacement controls before detaching from it.
    pub(crate) staged_rows: Option<Arc<contextdb_tx::StatementRows>>,
    pub(crate) _memory: Option<contextdb_core::read_memory::ReadCredit>,
    /// What holding the above costs.
    pub(crate) bytes: usize,
}

impl TransactionTableOverlay {
    /// Nothing staged, nothing hidden: the read is the same read it would
    /// have been with no transaction open.
    pub(crate) fn is_empty(&self) -> bool {
        self.deleted.is_empty()
            && self.delete_predicates.is_empty()
            && self.staged_rows.as_ref().is_none_or(|rows| rows.is_empty())
    }
}

/// What an open transaction changes about the vectors ONE index can be
/// searched over.
///
/// A transaction's own vectors are no more in the store than its rows are, and
/// no approximate graph can describe them, so a read that has to see them
/// scores the merged image instead. That is the choice the established door
/// already makes for exactly this case.
#[derive(Default)]
pub(crate) struct TransactionVectorOverlay {
    /// Entries the transaction has taken out of this index: the vectors it
    /// deleted, and the rows it deleted relationally without putting a row
    /// back at the same identity.
    pub(crate) removed: std::collections::HashSet<RowId>,
    /// Entries it re-filed under another identity. A cosine score does not
    /// depend on the identity it is filed under, so a move is published by
    /// re-keying what the committed walk already scored -- the vector never
    /// has to be read a second time to move it.
    pub(crate) moved: HashMap<RowId, RowId>,
    /// Identities the transaction staged a vector for, so the committed entry
    /// is passed over and the staged one published in its place.
    pub(crate) staged_identities: std::collections::HashSet<RowId>,
    /// Immutable staged identity/payload owners, newest version per row,
    /// in the order the transaction staged them.
    pub(crate) staged_vectors: Vec<Arc<contextdb_tx::ordered_staged::StagedVectorCapability>>,
    /// How many transaction-delta entries can remove, move, or add to the
    /// source answer. A source that stops at the statement's LIMIT would
    /// truncate before the overlay has had its say, so this many extra
    /// committed candidates are requested and final truncation happens after.
    pub(crate) searchable_entry_count: usize,
    /// Retains the real container admission through the overlay lifetime.
    pub(crate) _memory: Option<crate::memory_accounting::OwnedMemoryReservation>,
}

impl TransactionVectorOverlay {
    /// Nothing staged, nothing withdrawn, nothing moved: the search is the
    /// search it would have been with no transaction open.
    pub(crate) fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.moved.is_empty() && self.staged_vectors.is_empty()
    }

    /// Kept only when it actually changes the search, so every later read of
    /// it is a plain `is_some` rather than a re-test of three collections.
    pub(crate) fn into_option_if_not_empty(self) -> Option<Self> {
        (!self.is_empty()).then_some(self)
    }
}

fn content_size_result(
    totals: crate::persistence::content_size::ContentSizeTotals,
) -> Result<OwnedQueryResult> {
    let values = [
        totals.live_row_bytes,
        totals.live_vector_bytes,
        totals.live_content_bytes()?,
        totals.retained_row_bytes,
        totals.retained_vector_bytes,
    ]
    .into_iter()
    .map(|value| {
        i64::try_from(value)
            .map(Value::Int64)
            .map_err(|_| Error::Other("content size exceeds SQL integer range".into()))
    })
    .collect::<Result<Vec<_>>>()?;
    Ok(OwnedQueryResult::new(QueryResult {
        columns: [
            "live_row_bytes",
            "live_vector_bytes",
            "live_content_bytes",
            "retained_row_bytes",
            "retained_vector_bytes",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        rows: vec![values],
        rows_affected: 0,
        trace: crate::database::QueryTrace::scan(),
        cascade: None,
    }))
}

pub(crate) trait ReadExecutionTarget: Send + Sync {
    #[cfg(feature = "test-seams")]
    fn resident_history_database_for_test(&self) -> Result<&Database> {
        Err(Error::PlanError(
            "this execution target has no installed history database".into(),
        ))
    }

    fn register_writer_cursor(&self, _cursor: &Arc<BoundedCursorExecutionState>) -> Result<()> {
        Ok(())
    }

    fn enter_writer_read(
        &self,
        _root: Option<Arc<crate::persistence::WriterSnapshot>>,
        _read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Option<crate::database::writer_read::WriterReadScope> {
        None
    }

    /// The value a row's vector column holds, taken from the store that holds
    /// it. A column declared with space-saving storage keeps its value there
    /// and nowhere else, so a read that names such a column asks here.
    fn load_row_vector_for_column(
        &self,
        tx: Option<TxId>,
        table: &str,
        column: &str,
        row_id: RowId,
        lsn: Lsn,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Vec<f32>>>;

    #[allow(clippy::too_many_arguments)]
    fn bounded_score_committed_vector_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        before_checkpoint: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<f32>, BoundedExecutionError>;

    /// Which of a table's vector columns keep their value in the store alone.
    fn quantized_vector_columns(&self, table: &str) -> Vec<String>;

    fn plugin(&self) -> &dyn crate::plugin::DatabasePlugin;

    fn table_meta(&self, table: &str) -> Option<TableMeta>;

    fn natural_key_column_for_table(&self, table: &str) -> Result<String>;

    fn row_id_for_natural_key_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        key_col: &str,
        key_value: &Value,
        snapshot: SnapshotId,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<RowId>>;

    fn prepare_bounded_read_plan(
        &self,
        parsed: &Statement,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(Statement, PhysicalPlan)>;

    fn bounded_read_accountant(&self) -> Arc<crate::memory_accounting::MemoryAccountant>;

    fn declared_read_request_provider(
        &self,
    ) -> Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>;

    /// Whether this target's single-column automatic indexes are ordered
    /// trees a bounded cursor can walk. A durable store keeps every posting in
    /// one ordered family; a memory store keeps automatic single-column
    /// postings as an exact hash that only answers point probes.
    fn automatic_indexes_are_ordered(&self) -> bool {
        false
    }

    /// How the store this target reads names itself, for a damage report.
    fn bounded_store_path(&self) -> String;

    /// Answer one of the read-classified statements a store makes about
    /// ITSELF -- the memory and disk budgets in force, the sync conflict
    /// policy each table declares, the vector indexes it holds.
    ///
    /// These read no rows, so they have no source to walk and no page to cut;
    /// they read live engine state and hand back a small fixed answer. They
    /// are still READS (`statement_effect` classifies them so), so a reading
    /// session has to be able to run them, which is why they arrive here
    /// rather than being refused as "not a SELECT". A target with no engine
    /// state to describe keeps the old refusal.
    fn delivery_metadata(
        &self,
        request: crate::direct_file_reader::DirectMetadataRequest,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::direct_file_reader::DirectMetadataBody>;

    /// `checkpoint` is the asking request's own cancellation and active-time
    /// check, called as the history walk and the answer's projection
    /// advance. `charge` is the request's own memory charge: every copy the
    /// projection keeps or builds is admitted to it first, and it is left
    /// holding what the finished answer retains.
    fn committed_change_state(
        &self,
        _checkpoint: &dyn Fn() -> Result<()>,
        _charge: &Arc<crate::read_image_memory::ImageMemory>,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectChangeState,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        Err(crate::direct_file_reader::DirectFileReaderError::Engine(
            Error::PlanError("bounded execution has no committed change history".to_string())
                .to_string(),
        ))
    }

    fn committed_sync_state(
        &self,
        _checkpoint: &dyn Fn() -> Result<()>,
        _charge: &Arc<crate::read_image_memory::ImageMemory>,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectSyncState,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        Err(crate::direct_file_reader::DirectFileReaderError::Engine(
            Error::PlanError("bounded execution has no committed sync provenance".to_string())
                .to_string(),
        ))
    }

    fn content_size_answer(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_table: &mut dyn FnMut() -> Result<()>,
    ) -> Result<OwnedQueryResult> {
        let _ = (read_memory, before_table);
        Err(Error::PlanError(
            "content-size inspection is unavailable on this execution target".into(),
        ))
    }

    fn store_state_answer(
        &self,
        plan: &PhysicalPlan,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<OwnedQueryResult> {
        let _ = (plan, vector_copy_owner, read_memory);
        Err(Error::PlanError(
            "bounded execution accepts read-only SELECT plans".to_string(),
        ))
    }

    /// The plan the engine WOULD run for this statement, rendered without
    /// running it. Executed-route facts are exposed by the query trace, not by
    /// this metadata operation.
    fn explain_plan_without_running_it(
        &self,
        sql: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<String> {
        let _ = sql;
        Err(Error::PlanError(
            "bounded execution accepts read-only SELECT plans".to_string(),
        ))
    }

    /// Structured passive explanation used by metadata and CLI surfaces,
    /// planned against the caller's own binding: a partition scope resolves
    /// only once its key binds, so the binding decides the route explained.
    fn explain_output_without_running_it(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::database::ExplainOutput> {
        let _ = params;
        self.explain_plan_without_running_it(sql, vector_copy_owner, read_memory)
            .map(|physical_plan| crate::database::ExplainOutput {
                physical_plan,
                index_used: None,
                predicates_pushed: Vec::new(),
                indexes_considered: Vec::new(),
                sort_elided: false,
                vector_search: None,
            })
    }

    /// The transaction this target has open, if it has one.
    ///
    /// A transaction's own rows are not in the store: they sit in its write
    /// set until it commits, so no snapshot can reveal them. A read over the
    /// handle that owns the transaction therefore has to be TOLD about it to
    /// see its own writes. A committed file, and a target that owns no
    /// transaction, answers `None` and reads exactly as before.
    fn active_read_transaction(&self) -> Option<TxId> {
        None
    }

    /// What that transaction changes about what a read of one table sees.
    fn transaction_table_overlay(
        &self,
        tx: TxId,
        table: &str,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<TransactionTableOverlay> {
        let _ = (tx, table, read_memory);
        Ok(TransactionTableOverlay::default())
    }

    /// What that transaction changes about the vectors one index can be
    /// searched over.
    fn transaction_vector_overlay(
        &self,
        tx: TxId,
        index: &VectorIndexRef,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<TransactionVectorOverlay, BoundedExecutionError> {
        let _ = (tx, index, before_retain);
        Ok(TransactionVectorOverlay::default())
    }

    /// One vector the transaction has staged, cloned under the caller's
    /// charge, exactly as a committed entry is. The immutable capability comes
    /// from [`TransactionVectorOverlay::staged_vectors`].
    fn transaction_staged_vector(
        &self,
        tx: TxId,
        capability: &contextdb_tx::ordered_staged::StagedVectorCapability,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<(RowId, Vec<f32>)>, BoundedExecutionError> {
        let _ = (tx, capability, before_touch, before_clone);
        Ok(None)
    }

    #[cfg(feature = "test-seams")]
    fn note_vector_candidate_materialized(&self);

    /// Snapshot completed cache lifecycle events without causing a load or
    /// eviction. This is used only to prove the file reader's physical
    /// working-set behavior through its real execution target.
    #[cfg(feature = "test-seams")]
    fn vector_passive_activity_for_test(&self) -> contextdb_vector::VectorPassiveActivityCounters;

    /// Load this committed image's history, then drop its change and schema
    /// logs from memory, so the next history answer is built by the
    /// persisted-state fallback a store takes when its logs do not reach
    /// back to the position asked for. The store itself is untouched.
    #[cfg(feature = "test-seams")]
    fn force_history_log_gap_for_test(&self) {}

    /// Load this committed image's history, then drop only its change log
    /// from memory; its schema log stays.
    #[cfg(feature = "test-seams")]
    fn force_change_log_gap_for_test(&self) {}

    /// The vector store this target searches, so a proof can arm a store seam
    /// on the reader it measures.
    #[cfg(feature = "test-seams")]
    fn vector_store_for_test(&self) -> Arc<contextdb_vector::VectorStore>;

    fn capture_read_schema(&self) -> crate::database::SchemaPublicationGuard<'_>;

    fn relational_membership_store(&self) -> &contextdb_relational::RelationalStore;

    fn retained_relational_membership_source(&self) -> bool {
        false
    }

    /// Give back leaf pages this statement shared across its own row and
    /// body reads. The resident chunk cache remains; a later statement
    /// reuses those pages or re-proves after they are evicted.
    fn release_statement_proof_reuse(&self) {}

    fn authenticated_relational_index_prefix_count(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &IndexKey,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<u64>> {
        let _ = (table, columns, prefix, read_memory);
        Ok(None)
    }

    fn bounded_read_snapshot_registration(
        &self,
        needs_vector_graph: bool,
        budget: &crate::database::ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<BoundedReadSnapshotPin>>;

    fn bounded_cursor_snapshot_registration(
        &self,
        needs_vector_graph: bool,
        clock: Arc<dyn DeadlineClock>,
        idle_ms: u64,
        budget: &crate::database::ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<BoundedCursorSnapshotPin>>;

    fn assert_table_read_allowed(&self, table: &str) -> Result<()>;

    fn bounded_read_requires_candidate_filter(&self, table: &str) -> Result<bool>;

    fn vector_access_predicate(&self, table: &str) -> Result<Option<Expr>>;

    /// The reader's own row restrictions on this table, as the row gate holds
    /// them. Never a substitute for the gate: a caller uses it only to ask
    /// whether a statement's own predicate already stays inside them.
    fn vector_access_restrictions(&self, table: &str) -> Result<Vec<(String, Vec<Value>)>>;

    fn bounded_vector_visit_visible_ids(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        before_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),
        visit: &mut dyn FnMut(RowId) -> std::result::Result<(), BoundedExecutionError>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<(), BoundedExecutionError>;

    fn bounded_read_allowed_for_row(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<bool, BoundedExecutionError>;

    /// Whether this index pick PINS the row it is asking about -- an equality
    /// on a key that identifies at most one row. A read shaped like that is
    /// asking about a row it names, so being told "no such row" about a row
    /// that exists is a lie; a read that is not so shaped is asking what it
    /// may see, and hiding is the honest answer. The executor draws this line
    /// itself; the kernel asks the same question rather than forming a second
    /// opinion.
    fn read_pick_names_one_row(
        &self,
        table: &str,
        pick: &IndexPick,
        filter: Option<&Expr>,
        params: &HashMap<String, Value>,
    ) -> bool {
        let _ = (table, pick, filter, params);
        false
    }

    /// The same question for a source that asks an index whole keys rather
    /// than picking a run.
    ///
    /// Being built on columns that identify one row is not enough: a
    /// primary-key index is unique whether the statement names ONE key or a
    /// list of them, and a list is a set. So the statement is asked too, the
    /// same way the pick above asks it -- one equality per anchor column, or
    /// this read is a set read and hides what it may not see.
    fn read_index_names_one_row(
        &self,
        table: &str,
        index: &str,
        filter: Option<&Expr>,
        params: &HashMap<String, Value>,
    ) -> bool {
        let _ = (table, index, filter, params);
        false
    }

    /// The same decision with the REASON a row was withheld, for a caller that
    /// must refuse rather than skip. An entitlement decision is not a missing
    /// row, so the reader is told which it is.
    fn bounded_read_denial_for_row(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<Error>, BoundedExecutionError>;

    /// One row named by its identity, read the bounded way.
    ///
    /// A caller that already knows WHICH rows it wants asks for those rows.
    /// Finding them by walking the table instead costs what the table has ever
    /// been written, not what the caller asked for -- every superseded version
    /// of every row, inspected and discarded, for each row published.
    fn bounded_row_by_identity(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError>;

    fn bounded_physical_table_cursor(
        &self,
        table: &str,
    ) -> Result<contextdb_relational::mem::BoundedPhysicalCursor>;

    fn bounded_physical_table_row_next(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedPhysicalCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        observed: &mut dyn FnMut(&VersionedRow) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError>;

    fn vector_read_preflight(
        &self,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        copies: &mut contextdb_vector::VectorCopyAdmission,
        checkpoint: Option<bounded::ReadOpenContext>,
    ) -> Result<Option<crate::persistence::vector_membership_source::VectorReadScope>>;

    fn preflight_historical_graph(
        &self, snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(), BoundedExecutionError>;

    fn preflight_historical_index(
        &self, table: &str, index: &str, snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(), BoundedExecutionError>;

    /// Exact correspondence with this statement's captured source root.
    fn ordered_prefix_snapshot_is_current(
        &self,
        _snapshot: SnapshotId,
        _read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_ordered_table_row_next_observed(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        column: &str,
        direction: contextdb_core::SortDirection,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        observed: &mut dyn FnMut(
            &IndexKey,
            RowId,
        ) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError>;

    fn bounded_visit_verified_ordered_identities(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        visit: &mut dyn FnMut(RowId) -> std::result::Result<(), BoundedExecutionError>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        let _ = (table, index, snapshot, cursor, before_touch, visit);
        Ok(false)
    }

    /// Whether the committed index proves that no posting lies under any of
    /// `prefixes`. `None` means no proof either way.
    fn bounded_verified_index_prefixes_are_empty(
        &self,
        table: &str,
        index: &str,
        prefixes: &[IndexKey],
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<bool>, BoundedExecutionError> {
        let _ = (table, index, prefixes, check_boundary, before_touch);
        Ok(None)
    }

    /// One visible row at a time from the exact keys a predicate names, for
    /// an index that keeps postings by key rather than in order.
    #[allow(clippy::too_many_arguments)]
    fn bounded_exact_index_row_next(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedExactIndexCursor,
        before_storage_step: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError>;

    fn bounded_vector_schema_read_many(
        &self,
        indexes: Vec<VectorIndexRef>,
    ) -> crate::database::VectorSchemaReadGuard;

    fn assert_vector_index_exists_under_schema_read(&self, index: &VectorIndexRef) -> Result<()>;

    fn validate_vector_under_schema_read(
        &self,
        index: &VectorIndexRef,
        actual: usize,
    ) -> Result<()>;

    #[allow(clippy::too_many_arguments)]
    fn bounded_vector_candidate_groups(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        before_checkpoint: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_placement: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        contextdb_vector::mem::BoundedVectorCandidateGroups,
        BoundedExecutionError,
    >;

    #[allow(clippy::too_many_arguments)]
    fn bounded_vector_candidate_k(
        &self,
        index: &VectorIndexRef,
        requested: usize,
        sort_key: Option<&str>,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        before_source: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<usize, BoundedExecutionError>;

    fn bounded_vector_entry_count(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<usize>;

    /// The structural account of the route this vector read actually used.
    /// Values and authorization decisions have no representation in it, so a
    /// cursor can retain and republish it without retaining caller secrets.
    #[allow(clippy::too_many_arguments)]
    fn completed_vector_search_disclosure(
        &self,
        index: VectorIndexRef,
        k: usize,
        restricted_candidates: bool,
        candidates: Option<&PhysicalPlan>,
        search_mode: Option<contextdb_parser::ast::VectorSearchMode>,
        query_expr: &Expr,
        snapshot: SnapshotId,
        params: &HashMap<String, Value>,
        aggregate_allowed_vectors: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        partition_routes: &[contextdb_vector::VectorPartitionSearchTrace],
        used_hnsw: bool,
        fallback_reason: Option<&'static str>,
        budget_receipt: Option<contextdb_vector::mem::VectorSearchBudgetReceipt>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<VectorSearchDisclosure>;

    fn publish_vector_search_trace(&self, trace: contextdb_vector::OwnedVectorSearchTrace);

    /// The copy the caller makes while the row is still borrowed under the
    /// schema read, reported with the bytes it charged for.
    fn with_bounded_row_vector(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        use_vector: &mut dyn FnMut(
            &[f32],
        ) -> std::result::Result<
            BoundedRowVectorClone,
            BoundedExecutionError,
        >,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<BoundedRowVectorClone>, BoundedExecutionError>;

    #[allow(clippy::too_many_arguments)]
    fn bounded_brute_force_vector_cursor(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: contextdb_vector::mem::BoundedVectorCandidateGroups,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedBruteForceCursor, BoundedExecutionError>;

    fn bounded_brute_force_vector_step(
        &self,
        cursor: &mut contextdb_vector::mem::BoundedBruteForceCursor,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_precharged_candidate: &mut dyn FnMut() -> std::result::Result<
            (),
            BoundedExecutionError,
        >,
        before_exhaustive_candidate: &mut dyn FnMut() -> std::result::Result<
            (),
            BoundedExecutionError,
        >,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedVectorStep, BoundedExecutionError>;

    #[allow(clippy::too_many_arguments)]
    fn bounded_hnsw_vector_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        mode: VectorSearchMode,
        is_reader_memory_refusal: &dyn Fn(&BoundedExecutionError) -> bool,
        allowed_search_limits: contextdb_vector::hnsw::HnswAllowedSearchLimits,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_exact_candidate: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        load_transfer: crate::memory_accounting::VectorLoadReservationTransfer,
    ) -> std::result::Result<BoundedVectorSearchOutcome, BoundedExecutionError>;

    /// The standing visited ceiling a traversal keeps when the caller declared
    /// no budget of its own, and `None` when it declared one -- a declared
    /// budget is what replaces the ceiling.
    fn bounded_legacy_visited_cap(&self, declares_no_ceilings: bool) -> Option<usize> {
        let _ = declares_no_ceilings;
        None
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_rank_candidate(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        index: &VectorIndexRef,
        sort_key: &str,
        anchor_row_id: RowId,
        anchor_values: &HashMap<String, Value>,
        vector_score: f32,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        before_candidate: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_joined_clone: &mut dyn FnMut(
            usize,
        )
            -> std::result::Result<(), BoundedExecutionError>,
        release_joined_clone: &mut dyn FnMut(
            usize,
        )
            -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(Option<f32>, usize), BoundedExecutionError>;

    fn bounded_graph_edge_cursor(
        &self,
        direction: contextdb_core::Direction,
        snapshot: SnapshotId,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<crate::database::gate::BoundedGraphEdgeCursor, BoundedExecutionError>;

    #[allow(clippy::too_many_arguments)]
    fn bounded_graph_edge_next(
        &self,
        cursor: &mut crate::database::gate::BoundedGraphEdgeCursor,
        edge_types: Option<&[EdgeType]>,
        tx: Option<TxId>,
        before_edge: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        entry_read: &mut dyn FnMut(),

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        Option<crate::database::gate::CountedGraphScanEdge>,
        BoundedExecutionError,
    >;

    #[allow(clippy::too_many_arguments)]
    fn bounded_graph_bfs_cursor(
        &self,
        tx: Option<TxId>,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: contextdb_core::Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<contextdb_graph::mem::BoundedBfsCursor>, BoundedExecutionError>;

    #[allow(clippy::too_many_arguments)]
    fn bounded_graph_bfs_next(
        &self,
        cursor: &mut contextdb_graph::mem::BoundedBfsCursor,
        before_edge: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_path_element: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),
        staged_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        staged_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        staged_hand_back: &mut dyn FnMut(usize),
        entry_read: &mut dyn FnMut(),
        visited_cap: Option<usize>,
        tx: Option<TxId>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        Option<contextdb_graph::mem::BoundedTraversalNode>,
        BoundedExecutionError,
    >;

    /// Predicate resolution and value coercion are part of what a source
    /// answers: an index probe and a graph filter must be read against the
    /// same column types and the same snapshot that produced the rows.
    #[allow(dead_code)]
    fn resolve_graph_filter_at_snapshot(
        &self,
        expr: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        ctes: &[Cte],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Expr>;

    /// The in-process store, when this target is one. A subquery has to be
    /// PLANNED against a real store before it can be read, and planning is the
    /// half both doors share; a target that is not a store never carries a
    /// subquery to plan.
    fn as_database_for_subquery_plan(&self) -> Option<&Database> {
        None
    }

    /// The start vertices a filter names, resolved against the store the way
    /// the executor resolves them. A start named by anything other than its id
    /// is still a pinned start; a door that cannot resolve one walks every edge
    /// in the store to answer a probe.
    #[allow(dead_code)]
    fn resolve_graph_start_nodes_from_filter(
        &self,
        filter: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        start_alias: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<GraphStartResolution>;

    #[allow(clippy::too_many_arguments)]
    fn graph_filter_matches_bindings_with_work(
        &self,
        filter: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        bindings: &HashMap<String, uuid::Uuid>,
        work: &mut dyn FnMut(GraphPredicateWork) -> Result<()>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool>;

    fn coerce_into_column(
        &self,
        table: &str,
        col: &str,
        value: Value,
        current_tx_max: Option<TxId>,
        active_tx: Option<TxId>,
    ) -> Result<Value>;

    fn coerce_pick_shape_to_column_type(&self, table: &str, pick: &IndexPick) -> Result<IndexPick>;

    /// Run one complete bounded read and answer in the reading surface's own
    /// words: the result and the canonical bytes that publish it.
    ///
    /// The reading routes hold this seam and nothing below it, so the answer
    /// they get has to be sayable in their vocabulary: a complete result, a
    /// typed refusal, or the withdrawal a cancelled caller asked for. The
    /// bytes are produced here, by the one shared encoder, so two routes
    /// answering the same question publish the same bytes rather than two
    /// encodings that agree until they do not.
    fn read_query(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        (OwnedQueryResult, Vec<u8>, ReadPublicationScope),
        crate::direct_file_reader::DirectFileReaderError,
    >;

    /// Open a paged read and hand back its first page together with the
    /// continuation that produces the rest.
    ///
    /// The continuation is itself a target: a suspended read is still a thing
    /// you ask for rows, so it answers the same seam rather than a second one
    /// the reading routes would have to learn.
    ///
    /// The answer is the continuation, the first page as the route publishes
    /// it, and -- when that page already drained the read -- the empty page
    /// the drained cursor keeps answering from then on.
    #[allow(clippy::type_complexity)]
    fn open_read_cursor(
        self: Arc<Self>,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: &OwnerReadCancellation,
        snapshot: crate::direct_file_reader::DirectSnapshot,
    ) -> std::result::Result<
        (
            Arc<dyn ReadExecutionTarget>,
            crate::direct_file_reader::DirectCursorPage,
            Option<crate::direct_file_reader::DirectCursorPage>,
            ReadPublicationScope,
        ),
        crate::direct_file_reader::DirectFileReaderError,
    >;

    /// The next page of a suspended read, and the drained page that replaces
    /// the read once this one ends it.
    fn read_cursor_pages(
        &self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        (
            crate::direct_file_reader::DirectCursorPage,
            Option<crate::direct_file_reader::DirectCursorPage>,
            ReadPublicationScope,
        ),
        crate::direct_file_reader::DirectFileReaderError,
    >;

    fn read_cursor_observation(
        &self,
    ) -> std::result::Result<
        Option<CursorObservation>,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        Ok(None)
    }

    /// Whether the suspended read behind this target is still there.
    ///
    /// A refusal that answered the REQUEST -- an explicit page count above the
    /// row ceiling, say -- pulls nothing and consumes no continuation position,
    /// so the read is exactly where the caller left it. A target that keeps a
    /// suspended read says so here, and the routes above it then refuse the
    /// request without throwing away the cursor the refusal tells the caller to
    /// use. A target holding no cursor at all answers no.
    fn read_cursor_retains_continuation(&self) -> bool {
        false
    }

    /// Release a suspended read and everything it still holds.
    fn close_read_cursor(
        &self,
    ) -> std::result::Result<(), crate::direct_file_reader::DirectFileReaderError>;

    /// Answer one metadata question about a committed image, including the
    /// plan question, which is answered by this same planner rather than by a
    /// second description of it.
    fn read_metadata(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        request: crate::direct_file_reader::DirectMetadataRequest,
        image: &crate::direct_file_reader::DirectOwnedImage,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        continuation: Option<&str>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectMetadataResponse,
        crate::direct_file_reader::DirectFileReaderError,
    >;
}

/// One page, as a reading route publishes it.
fn published_page(
    page: CursorPage,
    observation: CursorObservation,
    snapshot: crate::direct_file_reader::DirectSnapshot,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> std::result::Result<
    crate::direct_file_reader::DirectCursorPage,
    crate::direct_file_reader::DirectFileReaderError,
> {
    let body =
        crate::read_contract::cursor_observation::BorrowedObservedPage::new(&page, &observation);
    let size = read_answer(bounded::serialized_size(&body))?;
    let canonical_credit = contextdb_core::read_memory::ReadCredit::try_new(
        read_memory,
        size,
        "read_channel",
        "encode_cursor_page",
        "Reduce the page or raise the reader memory limit.",
    )
    .map_err(crate::read_image::history_answer_error)?;
    let mut canonical_bytes = vec![0u8; size];
    let written =
        bincode::serde::encode_into_slice(&body, &mut canonical_bytes, bincode::config::standard())
            .map_err(|error| {
                crate::direct_file_reader::DirectFileReaderError::Engine(error.to_string())
            })?;
    if written != size {
        return Err(crate::direct_file_reader::DirectFileReaderError::Engine(
            "cursor page encoding size changed".into(),
        ));
    }
    Ok(crate::direct_file_reader::DirectCursorPage {
        page,
        observation,
        canonical_bytes,
        snapshot,
        canonical_credit: Some(canonical_credit),
    })
}

/// The empty page a read answers with once it is drained, or nothing while
/// there is still more to come.
fn drained_page(
    published: &crate::direct_file_reader::DirectCursorPage,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> std::result::Result<
    Option<crate::direct_file_reader::DirectCursorPage>,
    crate::direct_file_reader::DirectFileReaderError,
> {
    if published.page.has_more {
        return Ok(None);
    }
    published_page(
        CursorPage::try_empty(&published.page.columns, read_memory.clone())
            .map_err(crate::read_image::history_answer_error)?,
        published.observation.clone(),
        published.snapshot,
        read_memory,
    )
    .map(Some)
}

/// The reading surface's answer to one bounded-kernel outcome.
pub(crate) fn read_answer<T>(
    outcome: std::result::Result<T, BoundedExecutionError>,
) -> std::result::Result<T, crate::direct_file_reader::DirectFileReaderError> {
    use crate::direct_file_reader::DirectFileReaderError;

    outcome.map_err(|error| match error {
        BoundedExecutionError::Refused(failure) => DirectFileReaderError::ReadFailure(failure),
        BoundedExecutionError::Cancelled => DirectFileReaderError::Cancelled,
        BoundedExecutionError::Unimplemented => {
            DirectFileReaderError::Engine("this read has no approved bounded plan".to_owned())
        }
        BoundedExecutionError::Engine(Error::StoreCorrupted { path, reason }) => {
            DirectFileReaderError::CorruptStore(
                crate::direct_file_reader::DirectStoreDiagnostic::MalformedRecord,
                format!("store {path} is corrupt: {reason}"),
            )
        }
        BoundedExecutionError::Engine(engine) => DirectFileReaderError::Engine(engine.to_string()),
    })
}

/// A suspended bounded read, kept behind the same seam the store answers so a
/// reading route retains one shape rather than two.
pub(crate) struct BoundedCursorTarget {
    store: Arc<dyn ReadExecutionTarget>,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    cursor: std::sync::Mutex<BoundedCursorHandle>,
    /// What the engine actually reserved to keep this read suspended, and
    /// therefore what is returned when it ends. It drops to zero on the
    /// release, so however a cursor ends -- drained, closed, refused -- the
    /// bytes come back exactly once.
    retained: std::sync::atomic::AtomicU64,
    released: std::sync::atomic::AtomicBool,
    snapshot: crate::direct_file_reader::DirectSnapshot,
}

impl BoundedCursorTarget {
    fn new(
        store: Arc<dyn ReadExecutionTarget>,
        cursor: BoundedCursorHandle,
        retained: u64,
        snapshot: crate::direct_file_reader::DirectSnapshot,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Arc<Self> {
        #[cfg(feature = "test-seams")]
        {
            crate::read_probe::note_cursor_opened();
            crate::read_probe::note_cursor_bytes_charged(retained);
        }
        Arc::new(Self {
            store,
            read_memory,
            cursor: std::sync::Mutex::new(cursor),
            retained: std::sync::atomic::AtomicU64::new(retained),
            released: std::sync::atomic::AtomicBool::new(false),
            snapshot,
        })
    }

    /// End this read once, whatever ended it.
    fn release(&self) -> std::result::Result<(), crate::direct_file_reader::DirectFileReaderError> {
        if self
            .released
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(());
        }
        let retained = self.retained.swap(0, std::sync::atomic::Ordering::SeqCst);
        #[cfg(feature = "test-seams")]
        {
            crate::read_probe::note_cursor_closed();
            crate::read_probe::note_cursor_bytes_returned(retained);
        }
        #[cfg(not(feature = "test-seams"))]
        let _ = retained;
        self.store.release_statement_proof_reuse();
        self.with_cursor(close_bounded_cursor)
    }

    fn with_cursor<T>(
        &self,
        act: impl FnOnce(&mut BoundedCursorHandle) -> std::result::Result<T, BoundedExecutionError>,
    ) -> std::result::Result<T, crate::direct_file_reader::DirectFileReaderError> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        read_answer(act(&mut cursor))
    }
}

/// The live engine handle is the production source, and the file route reads
/// through a hydrated handle of the same shape rather than a second answer to
/// these questions.
#[allow(clippy::too_many_arguments)]
impl ReadExecutionTarget for Database {
    #[cfg(feature = "test-seams")]
    fn resident_history_database_for_test(&self) -> Result<&Database> {
        Ok(self)
    }

    fn register_writer_cursor(&self, cursor: &Arc<BoundedCursorExecutionState>) -> Result<()> {
        Database::register_writer_cursor(self, cursor)
    }

    fn enter_writer_read(
        &self,
        root: Option<Arc<crate::persistence::WriterSnapshot>>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Option<crate::database::writer_read::WriterReadScope> {
        Some(Database::enter_writer_read(self, root, read_memory))
    }

    fn load_row_vector_for_column(
        &self,
        tx: Option<TxId>,
        table: &str,
        column: &str,
        row_id: RowId,
        lsn: Lsn,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Vec<f32>>> {
        self.load_row_vector_for_column_identity(
            tx,
            table,
            column,
            row_id,
            lsn,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_score_committed_vector_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        before_checkpoint: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<f32>, BoundedExecutionError> {
        Database::bounded_score_committed_vector_row(
            self,
            index,
            row_id,
            snapshot,
            query,
            before_checkpoint,
            before_distance,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn quantized_vector_columns(&self, table: &str) -> Vec<String> {
        Database::quantized_vector_columns(self, table)
    }

    fn plugin(&self) -> &dyn crate::plugin::DatabasePlugin {
        Database::plugin(self)
    }

    fn table_meta(&self, table: &str) -> Option<TableMeta> {
        Database::table_meta(self, table)
    }

    fn natural_key_column_for_table(&self, table: &str) -> Result<String> {
        Database::natural_key_column_for_table(self, table)
    }

    fn row_id_for_natural_key_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        key_col: &str,
        key_value: &Value,
        snapshot: SnapshotId,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<RowId>> {
        Database::row_id_for_natural_key_in_tx(
            self,
            tx,
            table,
            key_col,
            key_value,
            snapshot,
            read_memory.clone(),
        )
    }

    fn prepare_bounded_read_plan(
        &self,
        parsed: &Statement,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(Statement, PhysicalPlan)> {
        Database::prepare_bounded_read_plan(
            self,
            parsed,
            params,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn automatic_indexes_are_ordered(&self) -> bool {
        Database::automatic_indexes_are_ordered(self)
    }

    fn declared_read_request_provider(
        &self,
    ) -> Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider> {
        Database::declared_read_request_provider(self)
    }

    fn bounded_read_accountant(&self) -> Arc<crate::memory_accounting::MemoryAccountant> {
        Database::bounded_read_accountant(self)
    }

    fn bounded_store_path(&self) -> String {
        Database::bounded_store_path(self)
    }

    fn delivery_metadata(
        &self,
        request: crate::direct_file_reader::DirectMetadataRequest,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::direct_file_reader::DirectMetadataBody> {
        crate::custody::inspection::metadata(self, request, read_memory)
    }

    fn committed_change_state(
        &self,
        checkpoint: &dyn Fn() -> Result<()>,
        charge: &Arc<crate::read_image_memory::ImageMemory>,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectChangeState,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        crate::read_image::project_change_state(self, checkpoint, charge)
    }

    fn committed_sync_state(
        &self,
        checkpoint: &dyn Fn() -> Result<()>,
        charge: &Arc<crate::read_image_memory::ImageMemory>,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectSyncState,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        crate::read_image::project_sync_state(self, checkpoint, charge)
    }

    fn content_size_answer(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_table: &mut dyn FnMut() -> Result<()>,
    ) -> Result<OwnedQueryResult> {
        content_size_result(self.content_size_totals(read_memory, before_table)?)
    }

    fn store_state_answer(
        &self,
        plan: &PhysicalPlan,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<OwnedQueryResult> {
        // The same arms the uncapped path runs, asked of the same handle:
        // one implementation, so a budget read over a channel and the same
        // read against the file cannot drift apart.
        execute_plan(
            self,
            plan,
            &HashMap::new(),
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn explain_plan_without_running_it(
        &self,
        sql: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<String> {
        Database::explain_with_read_memory(self, sql, vector_copy_owner, read_memory)
    }

    fn explain_output_without_running_it(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::database::ExplainOutput> {
        Database::explain_output_with_params(self, sql, params, vector_copy_owner, read_memory)
    }

    fn active_read_transaction(&self) -> Option<TxId> {
        Database::active_read_transaction(self)
    }

    fn transaction_table_overlay(
        &self,
        tx: TxId,
        table: &str,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<TransactionTableOverlay> {
        Database::transaction_table_overlay(self, tx, table, read_memory)
    }

    fn transaction_vector_overlay(
        &self,
        tx: TxId,
        index: &VectorIndexRef,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<TransactionVectorOverlay, BoundedExecutionError> {
        Database::transaction_vector_overlay(self, tx, index, before_retain)
    }

    fn transaction_staged_vector(
        &self,
        tx: TxId,
        capability: &contextdb_tx::ordered_staged::StagedVectorCapability,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<(RowId, Vec<f32>)>, BoundedExecutionError> {
        Database::transaction_staged_vector(self, tx, capability, before_touch, before_clone)
    }

    #[cfg(feature = "test-seams")]
    fn note_vector_candidate_materialized(&self) {
        self.__bump_candidate_rows_materialized_for_test(1);
    }

    #[cfg(feature = "test-seams")]
    fn vector_passive_activity_for_test(&self) -> contextdb_vector::VectorPassiveActivityCounters {
        self.__vector_passive_activity_counters_for_test()
    }

    #[cfg(feature = "test-seams")]
    fn force_history_log_gap_for_test(&self) {
        self.ensure_image_history()
            .expect("the committed image's history loads before its logs are dropped");
        self.__force_sync_log_gap_for_test();
    }

    #[cfg(feature = "test-seams")]
    fn force_change_log_gap_for_test(&self) {
        self.ensure_image_history()
            .expect("the committed image's history loads before its change log is dropped");
        self.__force_change_log_gap_for_test();
    }

    #[cfg(feature = "test-seams")]
    fn vector_store_for_test(&self) -> Arc<contextdb_vector::VectorStore> {
        Database::vector_store_for_test(self)
    }

    fn capture_read_schema(&self) -> crate::database::SchemaPublicationGuard<'_> {
        Database::capture_read_schema(self)
    }

    fn relational_membership_store(&self) -> &contextdb_relational::RelationalStore {
        self.relational_store()
    }

    fn retained_relational_membership_source(&self) -> bool {
        self.uses_retained_relational_source()
    }

    fn release_statement_proof_reuse(&self) {
        Database::release_statement_proof_reuse(self);
    }

    fn authenticated_relational_index_prefix_count(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &IndexKey,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<u64>> {
        Database::authenticated_relational_index_prefix_count(
            self,
            table,
            columns,
            prefix,
            read_memory,
        )
    }

    fn bounded_read_snapshot_registration(
        &self,
        needs_vector_graph: bool,
        budget: &crate::database::ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<BoundedReadSnapshotPin>> {
        Database::bounded_read_snapshot_registration_with_root(
            self,
            needs_vector_graph,
            budget,
            withdrawn,
            read_memory,
        )
    }

    fn bounded_cursor_snapshot_registration(
        &self,
        needs_vector_graph: bool,
        clock: Arc<dyn DeadlineClock>,
        idle_ms: u64,
        budget: &crate::database::ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<BoundedCursorSnapshotPin>> {
        Database::bounded_cursor_snapshot_registration_with_root(
            self,
            needs_vector_graph,
            clock,
            idle_ms,
            budget,
            withdrawn,
            read_memory,
        )
    }

    fn assert_table_read_allowed(&self, table: &str) -> Result<()> {
        Database::assert_table_read_allowed(self, table)
    }

    fn bounded_read_requires_candidate_filter(&self, table: &str) -> Result<bool> {
        Database::bounded_read_requires_candidate_filter(self, table)
    }

    fn vector_access_predicate(&self, table: &str) -> Result<Option<Expr>> {
        Database::vector_access_predicate(self, table)
    }

    fn vector_access_restrictions(&self, table: &str) -> Result<Vec<(String, Vec<Value>)>> {
        Database::vector_access_restrictions(self, table)
    }

    fn bounded_vector_visit_visible_ids(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        before_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),
        visit: &mut dyn FnMut(RowId) -> std::result::Result<(), BoundedExecutionError>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        Database::bounded_vector_visit_visible_ids(
            self,
            index,
            snapshot,
            before_entry,
            before_retain,
            release_retained,
            visit,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_read_allowed_for_row(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        Database::bounded_read_allowed_for_row(
            self,
            tx,
            table,
            meta,
            row,
            snapshot,
            before_access_row,
            read_memory.clone(),
        )
    }

    fn bounded_read_denial_for_row(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<Error>, BoundedExecutionError> {
        Database::bounded_read_denial_for_row(
            self,
            tx,
            table,
            meta,
            row,
            snapshot,
            before_access_row,
            read_memory.clone(),
        )
    }

    fn read_pick_names_one_row(
        &self,
        table: &str,
        pick: &IndexPick,
        filter: Option<&Expr>,
        params: &HashMap<String, Value>,
    ) -> bool {
        match filter {
            Some(filter) => self
                .table_meta(table)
                .is_some_and(|meta| statement_names_one_row(&meta, filter, params)),
            None => pick_names_one_row(self, table, pick).is_some(),
        }
    }

    fn read_index_names_one_row(
        &self,
        table: &str,
        index: &str,
        filter: Option<&Expr>,
        params: &HashMap<String, Value>,
    ) -> bool {
        let Some(meta) = self.table_meta(table) else {
            return false;
        };
        // A source with no residual predicate cannot be naming one row: the keys it is probing
        // came from somewhere, and without the statement to confirm an equality there is nothing
        // that says the somewhere was a single key.
        let _ = index;
        filter.is_some_and(|filter| statement_names_one_row(&meta, filter, params))
    }

    fn vector_read_preflight(
        &self,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        copies: &mut contextdb_vector::VectorCopyAdmission,
        checkpoint: Option<bounded::ReadOpenContext>,
    ) -> Result<Option<crate::persistence::vector_membership_source::VectorReadScope>> {
        Database::vector_read_preflight(self, snapshot, read_memory, copies, checkpoint)
    }

    fn preflight_historical_graph(
        &self, snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        Database::preflight_historical_graph(self, snapshot, read_memory, before)
    }

    fn preflight_historical_index(
        &self, table: &str, index: &str, snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        Database::preflight_historical_index(self, table, index, snapshot, read_memory, before)
    }

    fn bounded_row_by_identity(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError> {
        Database::bounded_row_by_identity_admitted(
            self,
            read_memory,
            tx,
            table,
            row_id,
            snapshot,
            before_touch,
            before_clone,
        )
    }

    fn bounded_physical_table_cursor(
        &self,
        table: &str,
    ) -> Result<contextdb_relational::mem::BoundedPhysicalCursor> {
        Database::bounded_physical_table_cursor(self, table)
    }

    fn bounded_physical_table_row_next(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedPhysicalCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        observed: &mut dyn FnMut(&VersionedRow) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError> {
        Database::bounded_physical_table_row_next_admitted(
            self,
            read_memory,
            table,
            snapshot,
            cursor,
            before_touch,
            observed,
            before_clone,
        )
    }

    fn bounded_exact_index_row_next(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedExactIndexCursor,
        before_storage_step: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(
            usize) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError> {
        Database::bounded_exact_index_row_next(
            self,
            read_memory,
            table,
            index,
            snapshot,
            cursor,
            before_storage_step,
            before_touch,
            before_retain,
            release_retained,
            before_clone,
        )
    }

    fn ordered_prefix_snapshot_is_current(
        &self,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        Database::ordered_prefix_snapshot_is_current(self, snapshot, read_memory)
    }

    fn bounded_ordered_table_row_next_observed(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        column: &str,
        direction: contextdb_core::SortDirection,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        observed: &mut dyn FnMut(
            &IndexKey,
            RowId,
        ) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError> {
        Database::bounded_ordered_table_row_next_observed(
            self,
            read_memory,
            table,
            column,
            direction,
            snapshot,
            cursor,
            check_boundary,
            before_touch,
            observed,
            before_clone,
        )
    }

    fn bounded_visit_verified_ordered_identities(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        visit: &mut dyn FnMut(RowId) -> std::result::Result<(), BoundedExecutionError>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        Database::bounded_visit_verified_ordered_identities(
            self,
            table,
            index,
            snapshot,
            cursor,
            before_touch,
            visit,
            read_memory,
        )
    }

    fn bounded_verified_index_prefixes_are_empty(
        &self,
        table: &str,
        index: &str,
        prefixes: &[IndexKey],
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<bool>, BoundedExecutionError> {
        Database::bounded_verified_index_prefixes_are_empty(
            self,
            table,
            index,
            prefixes,
            check_boundary,
            before_touch,
            read_memory,
        )
    }

    fn bounded_vector_schema_read_many(
        &self,
        indexes: Vec<VectorIndexRef>,
    ) -> crate::database::VectorSchemaReadGuard {
        Database::bounded_vector_schema_read_many(self, indexes)
    }

    fn assert_vector_index_exists_under_schema_read(&self, index: &VectorIndexRef) -> Result<()> {
        Database::assert_vector_index_exists_under_schema_read(self, index)
    }

    fn validate_vector_under_schema_read(
        &self,
        index: &VectorIndexRef,
        actual: usize,
    ) -> Result<()> {
        Database::validate_vector_under_schema_read(self, index, actual)
    }

    fn bounded_vector_candidate_groups(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        before_checkpoint: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_placement: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        contextdb_vector::mem::BoundedVectorCandidateGroups,
        BoundedExecutionError,
    > {
        Database::bounded_vector_candidate_groups(
            self,
            index,
            candidates,
            snapshot,
            before_checkpoint,
            before_placement,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_vector_candidate_k(
        &self,
        index: &VectorIndexRef,
        requested: usize,
        sort_key: Option<&str>,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        before_source: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<usize, BoundedExecutionError> {
        Database::bounded_vector_candidate_k(
            self,
            index,
            requested,
            sort_key,
            groups,
            before_source,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_vector_entry_count(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<usize> {
        Database::vector_entry_count(self, index, snapshot, vector_copy_owner)
    }

    fn completed_vector_search_disclosure(
        &self,
        index: VectorIndexRef,
        k: usize,
        restricted_candidates: bool,
        candidates: Option<&PhysicalPlan>,
        search_mode: Option<contextdb_parser::ast::VectorSearchMode>,
        query_expr: &Expr,
        snapshot: SnapshotId,
        params: &HashMap<String, Value>,
        aggregate_allowed_vectors: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        partition_routes: &[contextdb_vector::VectorPartitionSearchTrace],
        used_hnsw: bool,
        fallback_reason: Option<&'static str>,
        budget_receipt: Option<contextdb_vector::mem::VectorSearchBudgetReceipt>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<VectorSearchDisclosure> {
        Database::completed_vector_search_disclosure(
            self,
            index,
            k,
            restricted_candidates,
            candidates,
            search_mode,
            query_expr,
            snapshot,
            params,
            aggregate_allowed_vectors,
            groups,
            partition_routes,
            used_hnsw,
            fallback_reason,
            budget_receipt,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn publish_vector_search_trace(&self, trace: contextdb_vector::OwnedVectorSearchTrace) {
        Database::publish_vector_search_trace(self, trace);
    }

    fn with_bounded_row_vector(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        use_vector: &mut dyn FnMut(
            &[f32],
        ) -> std::result::Result<
            BoundedRowVectorClone,
            BoundedExecutionError,
        >,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<BoundedRowVectorClone>, BoundedExecutionError> {
        Database::with_row_vector_source(
            self,
            index,
            row_id,
            snapshot,
            tx,
            before_access_row,
            |vector| use_vector(&vector),
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_brute_force_vector_cursor(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: contextdb_vector::mem::BoundedVectorCandidateGroups,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedBruteForceCursor, BoundedExecutionError>
    {
        Database::bounded_brute_force_vector_cursor(
            self,
            index,
            query,
            k,
            groups,
            before_source_entry,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_brute_force_vector_step(
        &self,
        cursor: &mut contextdb_vector::mem::BoundedBruteForceCursor,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_precharged_candidate: &mut dyn FnMut() -> std::result::Result<
            (),
            BoundedExecutionError,
        >,
        before_exhaustive_candidate: &mut dyn FnMut() -> std::result::Result<
            (),
            BoundedExecutionError,
        >,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedVectorStep, BoundedExecutionError> {
        Database::bounded_brute_force_vector_step(
            self,
            cursor,
            before_source_entry,
            before_precharged_candidate,
            before_exhaustive_candidate,
            before_distance,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_hnsw_vector_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        mode: VectorSearchMode,
        is_reader_memory_refusal: &dyn Fn(&BoundedExecutionError) -> bool,
        allowed_search_limits: contextdb_vector::hnsw::HnswAllowedSearchLimits,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_exact_candidate: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        load_transfer: crate::memory_accounting::VectorLoadReservationTransfer,
    ) -> std::result::Result<BoundedVectorSearchOutcome, BoundedExecutionError> {
        Database::bounded_hnsw_vector_search(
            self,
            index,
            query,
            k,
            groups,
            mode,
            is_reader_memory_refusal,
            allowed_search_limits,
            before_source_entry,
            before_exact_candidate,
            before_distance,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
            load_transfer,
        )
    }

    fn bounded_legacy_visited_cap(&self, declares_no_ceilings: bool) -> Option<usize> {
        Database::bounded_legacy_visited_cap(self, declares_no_ceilings)
    }

    fn bounded_rank_candidate(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        index: &VectorIndexRef,
        sort_key: &str,
        anchor_row_id: RowId,
        anchor_values: &HashMap<String, Value>,
        vector_score: f32,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        before_candidate: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_joined_clone: &mut dyn FnMut(
            usize,
        )
            -> std::result::Result<(), BoundedExecutionError>,
        release_joined_clone: &mut dyn FnMut(
            usize,
        )
            -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(Option<f32>, usize), BoundedExecutionError> {
        Database::bounded_rank_candidate(
            self,
            read_memory,
            index,
            sort_key,
            anchor_row_id,
            anchor_values,
            vector_score,
            snapshot,
            tx,
            before_candidate,
            before_access,
            before_joined_clone,
            release_joined_clone,
        )
    }

    fn bounded_graph_edge_cursor(
        &self,
        direction: contextdb_core::Direction,
        snapshot: SnapshotId,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<crate::database::gate::BoundedGraphEdgeCursor, BoundedExecutionError>
    {
        Database::bounded_graph_edge_cursor(
            self,
            direction,
            snapshot,
            before_retain,
            release_retained,
        )
    }

    fn bounded_graph_edge_next(
        &self,
        cursor: &mut crate::database::gate::BoundedGraphEdgeCursor,
        edge_types: Option<&[EdgeType]>,
        tx: Option<TxId>,
        before_edge: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        entry_read: &mut dyn FnMut(),

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        Option<crate::database::gate::CountedGraphScanEdge>,
        BoundedExecutionError,
    > {
        Database::bounded_graph_edge_next(
            self,
            cursor,
            edge_types,
            tx,
            before_edge,
            before_access,
            before_retain,
            release_retained,
            entry_read,
            read_memory.clone(),
        )
    }

    fn bounded_graph_bfs_cursor(
        &self,
        tx: Option<TxId>,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: contextdb_core::Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<contextdb_graph::mem::BoundedBfsCursor>, BoundedExecutionError>
    {
        Database::bounded_graph_bfs_cursor(
            self,
            tx,
            start,
            edge_types,
            direction,
            min_depth,
            max_depth,
            snapshot,
            before_access,
            before_retain,
            release_retained,
            read_memory.clone(),
        )
    }

    fn bounded_graph_bfs_next(
        &self,
        cursor: &mut contextdb_graph::mem::BoundedBfsCursor,
        before_edge: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_path_element: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),
        staged_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        staged_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        staged_hand_back: &mut dyn FnMut(usize),
        entry_read: &mut dyn FnMut(),
        visited_cap: Option<usize>,
        tx: Option<TxId>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        Option<contextdb_graph::mem::BoundedTraversalNode>,
        BoundedExecutionError,
    > {
        Database::bounded_graph_bfs_next(
            self,
            cursor,
            before_edge,
            before_path_element,
            before_access,
            before_retain,
            release_retained,
            staged_touch,
            staged_retain,
            staged_hand_back,
            entry_read,
            visited_cap,
            tx,
            read_memory.clone(),
        )
    }

    fn resolve_graph_filter_at_snapshot(
        &self,
        expr: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        ctes: &[Cte],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Expr> {
        resolve_graph_filter_at_snapshot(
            self,
            expr,
            params,
            tx,
            snapshot,
            ctes,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn as_database_for_subquery_plan(&self) -> Option<&Database> {
        Some(self)
    }

    fn resolve_graph_start_nodes_from_filter(
        &self,
        filter: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        start_alias: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<GraphStartResolution> {
        resolve_graph_start_nodes_from_filter(
            self,
            filter,
            params,
            tx,
            snapshot,
            start_alias,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn graph_filter_matches_bindings_with_work(
        &self,
        filter: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        bindings: &HashMap<String, uuid::Uuid>,
        work: &mut dyn FnMut(GraphPredicateWork) -> Result<()>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        graph_filter_matches_bindings_with_work(
            self,
            filter,
            params,
            tx,
            snapshot,
            bindings,
            work,
            read_memory.clone(),
        )
    }

    fn coerce_into_column(
        &self,
        table: &str,
        col: &str,
        value: Value,
        current_tx_max: Option<TxId>,
        active_tx: Option<TxId>,
    ) -> Result<Value> {
        coerce_into_column(self, table, col, value, current_tx_max, active_tx)
    }

    fn coerce_pick_shape_to_column_type(&self, table: &str, pick: &IndexPick) -> Result<IndexPick> {
        coerce_pick_shape_to_column_type(self, table, pick)
    }

    fn read_query(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        (OwnedQueryResult, Vec<u8>, ReadPublicationScope),
        crate::direct_file_reader::DirectFileReaderError,
    > {
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_cancellation(crate::read_probe::CANCELLATION_EXECUTE, cancellation);
        let complete = read_answer(bounded::execute(
            self,
            sql,
            params,
            read_memory.clone(),
            limits,
            clock,
            cancellation.clone(),
            #[cfg(feature = "test-seams")]
            crate::read_session::session_kernel_probe_for_test(),
        ))?;
        let canonical =
            crate::read_contract::encode_query_result(&complete.result).map_err(|error| {
                crate::direct_file_reader::DirectFileReaderError::Engine(error.to_string())
            })?;
        Ok((complete.result, canonical, complete.publication))
    }

    fn open_read_cursor(
        self: Arc<Self>,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: &OwnerReadCancellation,
        snapshot: crate::direct_file_reader::DirectSnapshot,
    ) -> std::result::Result<
        (
            Arc<dyn ReadExecutionTarget>,
            crate::direct_file_reader::DirectCursorPage,
            Option<crate::direct_file_reader::DirectCursorPage>,
            ReadPublicationScope,
        ),
        crate::direct_file_reader::DirectFileReaderError,
    > {
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_cancellation(
            crate::read_probe::CANCELLATION_CURSOR_OPEN,
            cancellation,
        );
        let store: Arc<dyn ReadExecutionTarget> = self;
        let opened = read_answer(bounded::open_cursor(
            Arc::clone(&store),
            sql,
            params,
            read_memory.clone(),
            limits,
            clock,
            cancellation.clone(),
            #[cfg(feature = "test-seams")]
            crate::read_session::session_kernel_probe_for_test(),
        ))?;
        let retained = opened.cursor.retained_read_bytes();
        let continuation = BoundedCursorTarget::new(
            store,
            opened.cursor,
            retained as u64,
            snapshot,
            read_memory.clone(),
        );
        let constructed = (|| {
            let published = published_page(
                opened.first_page,
                opened.observation,
                snapshot,
                read_memory.clone(),
            )?;
            let drained = drained_page(&published, read_memory.clone())?;
            Ok((published, drained))
        })();
        let (published, drained) = match constructed {
            Ok(pages) => pages,
            Err(error) => {
                continuation.close_read_cursor()?;
                return Err(error);
            }
        };
        if drained.is_some() {
            continuation.close_read_cursor()?;
        }
        Ok((continuation, published, drained, opened.publication))
    }

    fn read_cursor_pages(
        &self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        (
            crate::direct_file_reader::DirectCursorPage,
            Option<crate::direct_file_reader::DirectCursorPage>,
            ReadPublicationScope,
        ),
        crate::direct_file_reader::DirectFileReaderError,
    > {
        let _ = (rows, cancellation);
        Err(
            crate::direct_file_reader::DirectFileReaderError::CursorUnavailable(
                crate::direct_file_reader::DirectCursorUnavailable::Closed,
            ),
        )
    }

    fn close_read_cursor(
        &self,
    ) -> std::result::Result<(), crate::direct_file_reader::DirectFileReaderError> {
        Err(
            crate::direct_file_reader::DirectFileReaderError::CursorUnavailable(
                crate::direct_file_reader::DirectCursorUnavailable::Closed,
            ),
        )
    }

    fn read_metadata(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        request: crate::direct_file_reader::DirectMetadataRequest,
        image: &crate::direct_file_reader::DirectOwnedImage,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        continuation: Option<&str>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectMetadataResponse,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        crate::read_image::project_metadata(
            self,
            read_memory,
            request,
            image,
            limits,
            clock,
            continuation,
            cancellation,
        )
    }
}

/// Local structural DDL validates cross-table rules (notably foreign keys)
/// before taking the allocation lock. The complete map, rather than only the
/// target table, is therefore the concurrency baseline.
fn schema_snapshot_matches(db: &Database, expected: &HashMap<TableName, TableMeta>) -> bool {
    *db.relational_store().table_meta.read() == *expected
}

fn plan_contains_window(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::WindowRank { .. } => true,
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::MaterializeCte { input, .. } => plan_contains_window(input),
        PhysicalPlan::Join { left, right, .. } => {
            plan_contains_window(left) || plan_contains_window(right)
        }
        PhysicalPlan::Union { inputs, .. } | PhysicalPlan::Pipeline(inputs) => {
            inputs.iter().any(plan_contains_window)
        }
        _ => false,
    }
}

fn execute_kernel_plan(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    bounded::drain_chosen_plan(
        db,
        plan,
        params,
        tx,
        db.snapshot_for_read(),
        Arc::new(crate::local_transport::MonotonicDeadlineClock::new()),
        #[cfg(feature = "test-seams")]
        None,
        read_memory,
    )
    .map_err(|error| match error {
        BoundedExecutionError::Engine(error) => error,
        BoundedExecutionError::Refused(failure) => Error::ReadFailure(failure),
        BoundedExecutionError::Cancelled => Error::ReadCancelled,
        BoundedExecutionError::Unimplemented => {
            Error::PlanError("window plan has no read source".into())
        }
    })
}

pub(crate) fn execute_plan(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    if plan_contains_window(plan) {
        return execute_kernel_plan(db, plan, params, tx, read_memory);
    }
    execute_plan_with_materialized_columns(
        db,
        plan,
        params,
        tx,
        None,
        None,
        vector_copy_owner,
        read_memory.clone(),
    )
}

fn execute_plan_with_materialized_columns(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    materialized_columns: Option<&[String]>,
    pending_sort_keys: Option<&[contextdb_planner::SortKey]>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    loop {
        match execute_plan_once(
            db,
            plan,
            params,
            tx,
            materialized_columns,
            pending_sort_keys,
            vector_copy_owner,
            read_memory.clone(),
        ) {
            Err(Error::Other(message)) if message == RETRY_LOCAL_SCHEMA_PROJECTION => continue,
            result => return result,
        }
    }
}

/// The single internal entrance to bounded read production.
///
/// This remains separate from [`execute_plan`] so existing uncapped callers do
/// not acquire bounded-read behavior implicitly.  The test-feature adapter
/// reaches this entrance through the real relational, graph, and vector stores.
/// Bounded results and cursors share one production pull kernel for each
/// source shape that has a fallible source handoff.
pub(crate) fn execute_bounded(
    db: &Database,
    sql: &str,
    params: &HashMap<String, Value>,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedExecutionResult, BoundedExecutionError> {
    bounded::execute(
        db,
        sql,
        params,
        read_memory,
        limits,
        clock,
        cancellation,
        #[cfg(feature = "test-seams")]
        probe,
    )
}

/// Run one bounded read against a target that is not the writable database --
/// a released committed image, say -- and answer in the engine's own words.
///
/// This is the same kernel entrance as [`execute_bounded`] with its answer
/// left un-flattened. A route that must report the same fault the same way
/// whichever way it reached the store needs the typed answer, and the direct
/// reader's own doors deliberately cannot carry one.
pub(crate) fn execute_bounded_on_target(
    db: &dyn ReadExecutionTarget,
    sql: &str,
    params: &HashMap<String, Value>,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedExecutionResult, BoundedExecutionError> {
    bounded::execute(
        db,
        sql,
        params,
        read_memory,
        limits,
        clock,
        cancellation,
        #[cfg(feature = "test-seams")]
        probe,
    )
}

/// Open a bounded cursor over the same pull kernel as [`execute_bounded`].
///
/// Cursor suspension owns the same kernel continuation; this is not a second
/// cursor implementation and a fetch never reparses or reruns the statement.
pub(crate) fn open_bounded_cursor(
    db: Arc<Database>,
    sql: &str,
    params: &HashMap<String, Value>,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedCursorOpen, BoundedExecutionError> {
    bounded::open_cursor(
        db,
        sql,
        params,
        read_memory,
        limits,
        clock,
        cancellation,
        #[cfg(feature = "test-seams")]
        probe,
    )
}

/// Resume one cursor fetch.  A cursor retains pull state; it must not rerun
/// the statement or collect the complete result before slicing a page.
pub(crate) fn fetch_bounded_cursor(
    cursor: &mut BoundedCursorHandle,
    rows: Option<NonZeroUsize>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedCursorFetch, BoundedExecutionError> {
    bounded::fetch_cursor(
        cursor,
        rows,
        cancellation,
        #[cfg(feature = "test-seams")]
        probe,
    )
}

/// Whether a bounded cursor still holds a read a caller can ask anything of.
pub(crate) fn bounded_cursor_is_live(cursor: &BoundedCursorHandle) -> bool {
    bounded::cursor_is_live(cursor)
}

/// Close a bounded cursor and release every retained kernel charge.
pub(crate) fn close_bounded_cursor(
    cursor: &mut BoundedCursorHandle,
) -> std::result::Result<(), BoundedExecutionError> {
    bounded::close_cursor(cursor)
}

#[derive(Debug)]
pub(crate) enum BoundedExecutionError {
    /// Compatibility variant retained for the owner-protocol error mapping;
    /// approved read plans no longer return it from the bounded kernel.
    #[allow(dead_code)]
    Unimplemented,
    /// A limit refusal preserves canonical typed detail rather than turning a
    /// completed eager collection into an opaque error string.
    Refused(ReadFailure),
    /// Cancellation is distinct from a limit refusal and publishes no partial
    /// ordinary result.
    Cancelled,
    Engine(Error),
}

impl From<Error> for BoundedExecutionError {
    fn from(error: Error) -> Self {
        match error {
            // A refusal that already says which ceiling stopped the read, or
            // what moved underneath it, stays a refusal the caller can branch
            // on rather than collapsing into an engine fault.
            Error::ReadFailure(failure) => Self::Refused(failure),
            Error::ReadCancelled => Self::Cancelled,
            other => Self::Engine(other),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BoundedWorkSource {
    TableScan,
    IndexRange,
    UnindexedSort,
    GraphTraversal,
    VectorCandidates,
    RankCandidates,
    AccessControl,
}

impl BoundedWorkSource {
    /// What a reservation for this source is FOR, in the operation vocabulary
    /// a store-budget refusal reports.
    ///
    /// A read governed by the store's `MEMORY_LIMIT` is refused with a typed
    /// answer that names the work the budget stopped. An unnamed reservation
    /// reaches an operator as "memory/allocate", which tells them a number and
    /// nothing they can act on -- which of a statement's parts to make smaller.
    pub(crate) const fn reservation_operation(self) -> &'static str {
        match self {
            Self::TableScan => "read/table_scan",
            Self::IndexRange => "read/index_range",
            Self::UnindexedSort => "read/sort_buffer",
            Self::GraphTraversal => "read/graph_traversal",
            Self::VectorCandidates => "read/vector_candidates",
            Self::RankCandidates => "read/rank_candidates",
            Self::AccessControl => "read/access_control",
        }
    }
}

/// The concrete item a bounded source is about to inspect.  This stays more
/// specific than [`BoundedWorkSource`] because the test seam must prove that
/// the production loop stopped before touching the next index entry, graph
/// edge, vector candidate, sort candidate, or access-controlled row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BoundedSourceTouch {
    /// Storage/proof work charged without claiming another examined row.
    StorageStep,
    TableRow,
    IndexEntry,
    SortCandidate,
    GraphEdge,
    /// One adjacency entry actually read, counted once however many steps the
    /// walk takes over it. `GraphEdge` charges the WORK a walk does -- reading
    /// the entry, testing its type, deciding whether to expand it, growing the
    /// visited set -- several times for one entry; this counts the entries
    /// themselves, which is what the executor reports as examined.
    AdjacencyEntry,
    BruteForceVectorCandidate,
    #[cfg(feature = "test-seams")]
    HnswCandidate,
    RankCandidate,
    AccessRow,
}

/// Test-feature observability names upstream work and memory so enforcing only
/// final result size cannot satisfy the bounded-read contract.
#[derive(Debug, Clone, Default)]
pub(crate) struct BoundedExecutionTelemetry {
    pub(crate) work_units: u64,
    pub(crate) source_work: BTreeMap<BoundedWorkSource, u64>,
    pub(crate) source_peak_temporary_bytes: BTreeMap<BoundedWorkSource, u64>,
    pub(crate) encoded_bytes: u64,
    pub(crate) encoded_trace_bytes: u64,
    pub(crate) peak_temporary_bytes: u64,
    pub(crate) cancellation_observed: bool,
}

#[derive(Debug)]
pub(crate) struct BoundedExecutionResult {
    pub(crate) result: OwnedQueryResult,
    pub(crate) publication: ReadPublicationScope,
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    pub(crate) telemetry: BoundedExecutionTelemetry,
}

/// Lifetime-free cursor ownership for route/session registries.
///
/// The handle owns one `Arc` state instead of borrowing `Database`.  The
/// kernel populates the open resource bundle exactly once;
/// refusal, expiry, exhaustion, or explicit close must atomically replace it
/// with `Released`.  Cancellation never performs that transition.  Dropping
/// the last handle also drops every still-open RAII resource without a
/// self-reference or lifetime extension.
pub(crate) struct BoundedCursorHandle {
    _state: Arc<BoundedCursorExecutionState>,
}

/// Actual kernel work and shared immutable plan metadata at a publication boundary.
#[derive(Clone)]
pub(crate) struct CursorObservation {
    pub(crate) trace: Arc<QueryTrace>,
    pub(crate) rows_examined: u64,
    pub(crate) charge: Option<Arc<bounded::SourcePageCharge>>,
    pub(crate) group_page_returns: Option<Arc<bounded::GroupPageReturns>>,
}
impl CursorObservation {
    pub(crate) fn groups_published_through(
        &self,
        offset: usize,
    ) -> std::result::Result<Self, BoundedExecutionError> {
        bounded::groups_published_through(self, offset)
    }

    pub(crate) fn unpublished_groups_withdrawn(
        &self,
    ) -> std::result::Result<Self, BoundedExecutionError> {
        bounded::unpublished_groups_withdrawn(self)
    }
}

impl std::fmt::Debug for CursorObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CursorObservation")
            .field("trace", &self.trace)
            .field("rows_examined", &self.rows_examined)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct BoundedCursorOpen {
    pub(crate) cursor: BoundedCursorHandle,
    pub(crate) first_page: CursorPage,
    pub(crate) observation: CursorObservation,
    pub(crate) storage_snapshot: Option<redb::StorageSnapshotIdentity>,
    pub(crate) publication: ReadPublicationScope,
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code))]
    pub(crate) telemetry: BoundedExecutionTelemetry,
}

#[derive(Debug)]
pub(crate) struct BoundedCursorFetch {
    pub(crate) page: CursorPage,
    pub(crate) observation: CursorObservation,
    pub(crate) publication: ReadPublicationScope,
    pub(crate) telemetry: BoundedExecutionTelemetry,
}

impl BoundedCursorHandle {
    pub(crate) fn work_observation(
        &mut self,
    ) -> std::result::Result<Option<CursorObservation>, BoundedExecutionError> {
        bounded::cursor_observation(self)
    }
}

#[cfg(feature = "test-seams")]
impl BoundedCursorHandle {
    /// What an open cursor is holding, part by part. A cursor that has
    /// already ended holds nothing, and says so with an all-zero breakdown
    /// rather than refusing the question.
    pub(crate) fn continuation_bytes(
        &self,
    ) -> std::result::Result<bounded::ContinuationBytes, BoundedExecutionError> {
        let lifecycle = match self._state.lifecycle.lock() {
            Ok(lifecycle) => lifecycle,
            Err(poisoned) => poisoned.into_inner(),
        };
        match &*lifecycle {
            BoundedCursorLifecycle::Open(state) => state.continuation_bytes(),
            BoundedCursorLifecycle::Exhausted(_, _) | BoundedCursorLifecycle::Released(_, _, _) => {
                Ok(bounded::ContinuationBytes::default())
            }
        }
    }
}

impl std::fmt::Debug for BoundedCursorHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundedCursorHandle")
            .finish_non_exhaustive()
    }
}

pub(crate) struct BoundedCursorExecutionState {
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    lifecycle: std::sync::Mutex<BoundedCursorLifecycle>,
    pub(crate) publication: ReadPublicationScope,
    erased: std::sync::atomic::AtomicBool,
    active_cancellation: std::sync::Mutex<OwnerReadCancellation>,
    expiry_watch: std::sync::Mutex<Option<Arc<bounded::CursorExpiryWatch>>>,
}

impl BoundedCursorExecutionState {
    fn wake_expiry(&self) {
        let watch = self
            .expiry_watch
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(watch) = watch {
            watch.drive();
        }
    }

    fn retire_expiry(&self) {
        let watch = self
            .expiry_watch
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(watch) = watch {
            watch.retire();
        }
    }

    pub(crate) fn release_for_erasure(&self) {
        self.erased.store(true, std::sync::atomic::Ordering::SeqCst);
        let cancellation = self
            .active_cancellation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        cancellation.cancel();
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|p| p.into_inner());
        bounded::release_cursor_lifecycle(&mut lifecycle, BoundedCursorTerminalReason::Erased);
        drop(lifecycle);
        self.retire_expiry();
    }
}

impl Drop for BoundedCursorExecutionState {
    fn drop(&mut self) {
        self.retire_expiry();
    }
}

#[allow(clippy::large_enum_variant)]
enum BoundedCursorLifecycle {
    Open(bounded::CursorState),
    Exhausted(CursorPage, CursorObservation),
    Released(BoundedCursorTerminalReason, Option<CursorObservation>, bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedCursorTerminalReason {
    Refused,
    Expired(contextdb_core::read_contract::CursorExpiryKind),
    ExplicitClose,
    Erased,
}

pub(crate) fn observe_bounded_access_control_row() {
    bounded::observe_access_control_row();
}

pub(crate) fn observe_bounded_rank_candidate() {
    bounded::observe_rank_candidate();
}

/// A deterministic probe is compiled only with the existing test-seams
/// feature. Supported bounded source loops call `before_source_touch`
/// immediately before inspecting a real item. HNSW observation comes from the
/// sealed distance event while the adjacent fallible control callback enforces
/// work, time, and cancellation before distance evaluation. Temporary memory
/// callbacks bracket the real database-accountant reservation.
///
/// Tests can advance the shared monotonic clock or cancel inside a source
/// batch without delay.  The pull kernel also calls `cancellation_observed`
/// immediately before returning the cancellation refusal.
#[cfg(feature = "test-seams")]
pub(crate) trait BoundedExecutionProbe: Send + Sync {
    fn after_snapshot_registration(&self) {}
    /// The actual immutable MVCC identity already registered by this operation.
    /// Test observation only; no digest, snapshot acquisition or protocol field.
    fn registered_snapshot_for_test(&self, _snapshot: contextdb_core::SnapshotId) {}
    fn after_source_capture(&self) {}
    fn before_work(&self, source: BoundedWorkSource, completed_work: u64);
    fn before_source_touch(&self, touch: BoundedSourceTouch, completed_items: u64);
    /// This callback can only arrive through the vector crate's sealed
    /// `HnswCandidateDistanceEvent`, emitted immediately before real distance
    /// work.  Calling `before_source_touch(HnswCandidate, ..)` in executor
    /// post-processing is rejected by the test adapter.
    fn before_hnsw_candidate_distance(
        &self,
        event: contextdb_vector::hnsw::HnswCandidateDistanceEvent,
    );
    fn after_hnsw_residual_rejection(
        &self,
        _event: contextdb_vector::hnsw::HnswResidualRejectionEvent,
    ) {
    }
    fn before_temporary_reservation(
        &self,
        source: BoundedWorkSource,
        requested_bytes: u64,
        held_temporary_bytes: u64,
    );
    fn after_temporary_reservation(
        &self,
        source: BoundedWorkSource,
        reserved_bytes: u64,
        held_temporary_bytes: u64,
    );
    fn cancellation_observed(&self, completed_work: u64);
    /// One cold vector graph load's credit move, observed on the real
    /// transfer before the destination admits, after it admits, and after
    /// this read's credit is split off.
    fn graph_credit_transfer(&self, _cut: bounded::GraphCreditTransferCut) {}
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod bounded_read_test_support;

const VECTOR_INDEX_INSPECTION_COLUMNS: &[&str] = &[
    "table",
    "column",
    "dimension",
    "quantization",
    "vector_count",
    "bytes",
    "partition_key_columns",
    "max_partitions",
    "live_partitions",
    "retained_partitions",
    "search_mode",
    "declared_auto_index_at",
    "effective_auto_index_at",
    "declared_hnsw_m",
    "declared_hnsw_ef_construction",
    "declared_hnsw_ef_search",
    "declared_consolidation_mode",
    "declared_consolidation_change_percent",
    "declared_consolidation_tombstone_percent",
    "effective_consolidation_mode",
    "effective_consolidation_change_percent",
    "effective_consolidation_tombstone_percent",
    "oldest_base_tx",
    "newest_base_tx",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "charged_vector_bytes",
    "durable_index_bytes",
    "charged_index_bytes",
    "query_state",
    "maintenance_state",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "unavailable_partitions",
    "stalled_partitions",
    "broad_route",
    "broad_route_state",
    "broad_route_base_tx",
    "broad_route_vectors_total",
    "broad_route_vectors_done",
    "broad_route_vectors_remaining",
    "broad_route_reason",
    "broad_route_recovery_action",
];

const VECTOR_PARTITION_INSPECTION_COLUMNS: &[&str] = &[
    "table",
    "column",
    "partition_key",
    "live_rows",
    "retained_rows",
    "base_generation",
    "base_tx",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "charged_vector_bytes",
    "durable_index_bytes",
    "charged_index_bytes",
    "query_state",
    "availability_reason",
    "maintenance_state",
    "maintenance_reason",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "maintenance_checkpoint_tx",
    "desired_hnsw_m",
    "desired_hnsw_ef_construction",
    "desired_hnsw_ef_search",
    "serving_hnsw_m",
    "serving_hnsw_ef_construction",
    "serving_hnsw_ef_search",
    "declared_consolidation_mode",
    "declared_consolidation_change_percent",
    "declared_consolidation_tombstone_percent",
    "effective_consolidation_mode",
    "effective_consolidation_change_percent",
    "effective_consolidation_tombstone_percent",
    "desired_policy_revision",
    "serving_policy_revision",
    "recovery_action",
];

fn vector_inspection_columns(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|column| (*column).to_owned()).collect()
}

fn vector_inspection_usize(value: usize, field: &str) -> Result<Value> {
    i64::try_from(value).map(Value::Int64).map_err(|_| {
        Error::Other(format!(
            "vector inspection field '{field}' exceeds the SQL INTEGER range"
        ))
    })
}

fn vector_inspection_u64(value: u64, field: &str) -> Result<Value> {
    i64::try_from(value).map(Value::Int64).map_err(|_| {
        Error::Other(format!(
            "vector inspection field '{field}' exceeds the SQL INTEGER range"
        ))
    })
}

fn vector_inspection_optional_usize(value: Option<usize>, field: &str) -> Result<Value> {
    value
        .map(|value| vector_inspection_usize(value, field))
        .transpose()
        .map(|value| value.unwrap_or(Value::Null))
}

fn vector_inspection_optional_tx(value: Option<TxId>, field: &str) -> Result<Value> {
    value
        .map(|value| vector_inspection_u64(value.0, field))
        .transpose()
        .map(|value| value.unwrap_or(Value::Null))
}

fn vector_inspection_partition_sum(
    partitions: &[contextdb_vector::store::VectorPartitionInfo],
    field: &str,
    value: impl Fn(&contextdb_vector::store::VectorPartitionInfo) -> usize,
) -> Result<usize> {
    partitions.iter().try_fold(0usize, |total, partition| {
        total
            .checked_add(value(partition))
            .ok_or_else(|| Error::Other(format!("vector inspection {field} overflow")))
    })
}

fn vector_partition_component_json(component: &VectorPartitionComponent) -> serde_json::Value {
    match component {
        VectorPartitionComponent::Uuid(value) => serde_json::Value::String(value.to_string()),
        VectorPartitionComponent::Text(value) => serde_json::Value::String(value.clone()),
        VectorPartitionComponent::Integer(value) => serde_json::Value::Number((*value).into()),
        VectorPartitionComponent::Boolean(value) => serde_json::Value::Bool(*value),
        VectorPartitionComponent::Timestamp(value) => serde_json::Value::Number((*value).into()),
        VectorPartitionComponent::TxId(value) => serde_json::Value::Number(value.0.into()),
    }
}

fn vector_partition_key_json(columns: &[String], key: &VectorPartitionKey) -> Result<Value> {
    if columns.len() != key.components().len() {
        return Err(Error::Other(
            "vector partition key does not match its declared column shape".to_owned(),
        ));
    }
    let mut object = serde_json::Map::new();
    for (column, component) in columns.iter().zip(key.components()) {
        object.insert(column.clone(), vector_partition_component_json(component));
    }
    Ok(Value::Json(serde_json::Value::Object(object).into()))
}

fn vector_partition_query_state(
    partition: &contextdb_vector::store::VectorPartitionInfo,
) -> &'static str {
    if partition.quarantine_reason.is_some() {
        "unavailable"
    } else if partition.live_rows == 0 {
        "empty"
    } else if partition.graph_available {
        "ready"
    } else {
        "unavailable"
    }
}

fn vector_index_query_state(
    partitions: &[contextdb_vector::store::VectorPartitionInfo],
) -> &'static str {
    let mut routes = 0usize;
    let mut ready = 0usize;
    for partition in partitions {
        if partition.live_rows == 0 && partition.quarantine_reason.is_none() {
            continue;
        }
        routes = routes.saturating_add(1);
        if partition.graph_available && partition.quarantine_reason.is_none() {
            ready = ready.saturating_add(1);
        }
    }
    match (routes, ready) {
        (0, _) => "empty",
        (routes, ready) if routes == ready => "ready",
        (_, 0) => "unavailable",
        _ => "partial",
    }
}

fn vector_maintenance_recovery_action(policy: MaintenancePolicy) -> &'static str {
    match policy {
        MaintenancePolicy::CallerDriven => "run_maintenance_cycle",
        MaintenancePolicy::EngineOwned => "wait_for_automatic_work",
    }
}

fn published_vector_maintenance_reason(reason: &'static str) -> &'static str {
    match reason {
        // Storage retains this implementation label in active progress while
        // inspection publishes the settled lifecycle vocabulary.
        "policy_replacement" => "new_changes",
        published => published,
    }
}

fn vector_unavailable_failure_reason(
    failure: contextdb_vector::store::VectorMaintenanceFailure,
) -> &'static str {
    match failure {
        contextdb_vector::store::VectorMaintenanceFailure::MemoryLimit
        | contextdb_vector::store::VectorMaintenanceFailure::DiskLimit => "resource_stalled",
        contextdb_vector::store::VectorMaintenanceFailure::BuildFailure => "build_failure",
    }
}

fn vector_maintenance_failure_lifecycle(
    failure: contextdb_vector::store::VectorMaintenanceFailure,
    graph_available: bool,
    checkpoint_tx: Option<TxId>,
) -> VectorInspectionLifecycle {
    VectorInspectionLifecycle {
        availability_reason: if graph_available {
            "none"
        } else {
            vector_unavailable_failure_reason(failure)
        },
        maintenance_state: "stalled",
        maintenance_reason: failure.reason(),
        vectors_total: None,
        vectors_done: None,
        vectors_remaining: None,
        checkpoint_tx,
        recovery_action: failure.recovery_action(),
    }
}

fn broad_route_recovery_action(
    lifecycle: &[VectorInspectionLifecycle],
    broad_route_reason: &str,
) -> &'static str {
    lifecycle
        .iter()
        .find(|lifecycle| lifecycle.availability_reason == broad_route_reason)
        .filter(|_| broad_route_reason != "none")
        .map(|lifecycle| lifecycle.recovery_action)
        .unwrap_or("none")
}

fn whole_index_maintenance_state(lifecycle: &[VectorInspectionLifecycle]) -> &'static str {
    let mut has_stalled = false;
    let mut common_live_activity = None;
    let mut several_live_activities = false;

    for lifecycle in lifecycle {
        match lifecycle.maintenance_state {
            "idle" => {}
            "stalled" => has_stalled = true,
            activity => match common_live_activity {
                None => common_live_activity = Some(activity),
                Some(common) if common == activity => {}
                Some(_) => several_live_activities = true,
            },
        }
    }

    match (has_stalled, common_live_activity, several_live_activities) {
        (false, None, _) => "idle",
        (false, Some(activity), false) => activity,
        (false, Some(_), true) => "active",
        (true, None, _) => "stalled",
        (true, Some(_), _) => "mixed",
    }
}

fn fanout_broad_route_state(
    partitions: &[contextdb_vector::store::VectorPartitionInfo],
) -> &'static str {
    match vector_index_query_state(partitions) {
        "empty" => "ready",
        state => state,
    }
}

#[derive(Clone, Copy)]
struct VectorInspectionLifecycle {
    availability_reason: &'static str,
    maintenance_state: &'static str,
    maintenance_reason: &'static str,
    vectors_total: Option<usize>,
    vectors_done: Option<usize>,
    vectors_remaining: Option<usize>,
    checkpoint_tx: Option<TxId>,
    recovery_action: &'static str,
}

#[cfg(test)]
mod vector_inspection_aggregation_tests {
    use super::*;

    fn lifecycle(maintenance_state: &'static str) -> VectorInspectionLifecycle {
        VectorInspectionLifecycle {
            availability_reason: "none",
            maintenance_state,
            maintenance_reason: "none",
            vectors_total: None,
            vectors_done: None,
            vectors_remaining: None,
            checkpoint_tx: None,
            recovery_action: "none",
        }
    }

    #[test]
    fn whole_index_maintenance_keeps_live_and_stalled_work_distinct() {
        for (states, expected) in [
            (&["idle", "idle"][..], "idle"),
            (&["idle", "building", "building"][..], "building"),
            (&["idle", "replaying", "replaying"][..], "replaying"),
            (&["compacting", "compacting"][..], "compacting"),
            (&["repairing", "idle"][..], "repairing"),
            (&["building", "replaying"][..], "active"),
            (&["idle", "stalled", "stalled"][..], "stalled"),
            (&["stalled", "building"][..], "mixed"),
        ] {
            let lifecycle = states.iter().copied().map(lifecycle).collect::<Vec<_>>();
            assert_eq!(whole_index_maintenance_state(&lifecycle), expected);
        }
    }

    #[test]
    fn an_empty_fanout_is_ready_while_the_index_stays_empty() {
        let partitions = Vec::<contextdb_vector::store::VectorPartitionInfo>::new();
        assert_eq!(vector_index_query_state(&partitions), "empty");
        assert_eq!(fanout_broad_route_state(&partitions), "ready");
    }

    #[test]
    fn healthy_and_failed_layout_actions_use_only_published_recovery_words() {
        let healthy = lifecycle("idle");
        assert_eq!(broad_route_recovery_action(&[healthy], "none"), "none");

        for (failure, available, maintenance_reason, action) in [
            (
                contextdb_vector::store::VectorMaintenanceFailure::MemoryLimit,
                "resource_stalled",
                "memory_limit",
                "raise_memory_limit",
            ),
            (
                contextdb_vector::store::VectorMaintenanceFailure::DiskLimit,
                "resource_stalled",
                "disk_limit",
                "raise_disk_limit_or_free_space",
            ),
            (
                contextdb_vector::store::VectorMaintenanceFailure::BuildFailure,
                "build_failure",
                "build_failure",
                "inspect_build_failure",
            ),
        ] {
            let stalled = vector_maintenance_failure_lifecycle(failure, false, Some(TxId(7)));
            assert_eq!(stalled.availability_reason, available);
            assert_eq!(stalled.maintenance_reason, maintenance_reason);
            assert_eq!(stalled.recovery_action, action);
            assert_eq!(broad_route_recovery_action(&[stalled], available), action);

            let serving = vector_maintenance_failure_lifecycle(failure, true, Some(TxId(7)));
            assert_eq!(serving.availability_reason, "none");
            assert_eq!(serving.maintenance_reason, maintenance_reason);
            assert_eq!(serving.recovery_action, action);
            assert_eq!(broad_route_recovery_action(&[serving], "none"), "none");
        }

        assert_eq!(
            contextdb_vector::store::VectorRouteQuarantineReason::CorruptChanges.as_str(),
            "corrupt_changes"
        );
        assert_eq!(
            published_vector_maintenance_reason("policy_replacement"),
            "new_changes"
        );
    }
}

/// The store snapshot distinguishes an empty partition, a partition with a
/// complete maintained route, and a populated partition that has not yet had
/// its first route built.  Do not turn the last case into a generic failure:
/// a newly written partition is waiting for ordinary maintenance, and the
/// operator needs that answer without causing a build by inspecting it.
fn vector_partition_inspection_lifecycle(
    layout: &contextdb_vector::store::VectorIndexLayout,
    partition: &contextdb_vector::store::VectorPartitionInfo,
    maintenance_policy: MaintenancePolicy,
) -> VectorInspectionLifecycle {
    let recovery_action = vector_maintenance_recovery_action(maintenance_policy);
    let maintenance_need = layout.maintenance_need(partition);
    let maintenance_failure = partition
        .maintenance_failure
        .filter(|_| maintenance_need.is_some() || partition.maintenance_progress.is_some());
    let availability_reason = if let Some(reason) = partition.quarantine_reason {
        reason.as_str()
    } else if partition.graph_available || partition.live_rows == 0 {
        "none"
    } else if let Some(failure) = maintenance_failure {
        vector_unavailable_failure_reason(failure)
    } else {
        "initial_build"
    };
    // A registered build retains committed progress even when its last batch
    // failed. Report that failure before progress, without hiding its checkpoint.
    if let Some(progress) = partition.maintenance_progress
        && let Some(failure) = partition.maintenance_failure
    {
        let mut lifecycle = vector_maintenance_failure_lifecycle(
            failure,
            partition.graph_available,
            Some(progress.checkpoint_tx),
        );
        lifecycle.availability_reason = availability_reason;
        lifecycle.vectors_total = Some(progress.vectors_total);
        lifecycle.vectors_done = Some(progress.vectors_done);
        lifecycle.vectors_remaining =
            Some(progress.vectors_total.saturating_sub(progress.vectors_done));
        return lifecycle;
    }
    if let Some(progress) = partition.maintenance_progress {
        let reason = published_vector_maintenance_reason(progress.reason);
        return VectorInspectionLifecycle {
            availability_reason,
            maintenance_state: progress.state,
            maintenance_reason: reason,
            vectors_total: Some(progress.vectors_total),
            vectors_done: Some(progress.vectors_done),
            vectors_remaining: Some(progress.vectors_total.saturating_sub(progress.vectors_done)),
            checkpoint_tx: Some(progress.checkpoint_tx),
            recovery_action,
        };
    }
    if let Some(failure) = maintenance_failure
        && matches!(
            failure,
            contextdb_vector::store::VectorMaintenanceFailure::MemoryLimit
                | contextdb_vector::store::VectorMaintenanceFailure::DiskLimit
        )
    {
        let mut lifecycle = vector_maintenance_failure_lifecycle(
            failure,
            partition.graph_available,
            partition.base_tx,
        );
        lifecycle.availability_reason = availability_reason;
        return lifecycle;
    }
    if let Some(reason) = partition.quarantine_reason {
        return VectorInspectionLifecycle {
            availability_reason,
            maintenance_state: "stalled",
            maintenance_reason: reason.as_str(),
            vectors_total: None,
            vectors_done: None,
            vectors_remaining: None,
            checkpoint_tx: partition.base_tx,
            recovery_action,
        };
    }
    if partition.live_rows == 0 {
        return VectorInspectionLifecycle {
            availability_reason,
            maintenance_state: "idle",
            maintenance_reason: "none",
            vectors_total: None,
            vectors_done: None,
            vectors_remaining: None,
            checkpoint_tx: None,
            recovery_action: "none",
        };
    }
    if let Some(failure) = maintenance_failure {
        return vector_maintenance_failure_lifecycle(
            failure,
            partition.graph_available,
            partition.base_tx,
        );
    }
    if partition.maintenance_policy_revision.is_some() {
        return VectorInspectionLifecycle {
            availability_reason: if partition.graph_available {
                "none"
            } else {
                "initial_build"
            },
            maintenance_state: "building",
            maintenance_reason: maintenance_need.map_or("none", |need| need.reason()),
            vectors_total: Some(partition.live_rows),
            vectors_done: Some(0),
            vectors_remaining: Some(partition.live_rows),
            checkpoint_tx: partition.base_tx,
            recovery_action,
        };
    }
    if partition.graph_available {
        let maintenance_reason = maintenance_need.map_or("none", |need| need.reason());
        let recovery_action = if maintenance_reason == "none" {
            "none"
        } else {
            recovery_action
        };
        return VectorInspectionLifecycle {
            availability_reason: "none",
            maintenance_state: "idle",
            maintenance_reason,
            vectors_total: None,
            vectors_done: None,
            vectors_remaining: None,
            checkpoint_tx: partition.base_tx,
            recovery_action,
        };
    }

    // A maintained partition is omitted from this inspection surface once it
    // has been retired for historical-only reads.  Therefore a listed,
    // populated partition without a graph is the first build still pending,
    // rather than a retired or partially usable graph.
    VectorInspectionLifecycle {
        availability_reason: "initial_build",
        maintenance_state: "idle",
        maintenance_reason: "initial_build",
        vectors_total: None,
        vectors_done: None,
        vectors_remaining: None,
        checkpoint_tx: None,
        recovery_action,
    }
}

fn vector_index_inspection_row(
    declaration: &ColumnDef,
    info: contextdb_vector::store::VectorIndexLayoutInfo,
    partitions: &[contextdb_vector::store::VectorPartitionInfo],
    maintenance_policy: MaintenancePolicy,
) -> Result<Vec<Value>> {
    let vector_count =
        vector_inspection_partition_sum(partitions, "live-row count", |row| row.live_rows)?;
    let live_partitions = partitions
        .iter()
        .filter(|partition| partition.live_rows != 0)
        .count();
    let retained_partitions = partitions
        .iter()
        .filter(|partition| partition.retained_rows != 0)
        .count();
    let unavailable_partitions = partitions
        .iter()
        .filter(|partition| {
            partition.quarantine_reason.is_some()
                || (partition.live_rows != 0 && !partition.graph_available)
        })
        .count();
    let lifecycle = partitions
        .iter()
        .map(|partition| {
            vector_partition_inspection_lifecycle(&info.layout, partition, maintenance_policy)
        })
        .collect::<Vec<_>>();
    let stalled_partitions = lifecycle
        .iter()
        .filter(|lifecycle| lifecycle.maintenance_state == "stalled")
        .count();
    let maintenance_state = whole_index_maintenance_state(&lifecycle);
    let active_progress = lifecycle
        .iter()
        .filter(|lifecycle| lifecycle.vectors_total.is_some())
        .copied()
        .collect::<Vec<_>>();
    let maintenance_vectors_total = if active_progress.is_empty() {
        None
    } else {
        Some(
            active_progress
                .iter()
                .try_fold(0usize, |total, lifecycle| {
                    total
                        .checked_add(lifecycle.vectors_total.unwrap_or(0))
                        .ok_or_else(|| Error::Other("vector maintenance total overflow".to_owned()))
                })?,
        )
    };
    let maintenance_vectors_done = if active_progress.is_empty() {
        None
    } else {
        Some(
            active_progress
                .iter()
                .try_fold(0usize, |total, lifecycle| {
                    total
                        .checked_add(lifecycle.vectors_done.unwrap_or(0))
                        .ok_or_else(|| Error::Other("vector maintenance done overflow".to_owned()))
                })?,
        )
    };
    let maintenance_vectors_remaining = if active_progress.is_empty() {
        None
    } else {
        Some(
            active_progress
                .iter()
                .try_fold(0usize, |total, lifecycle| {
                    total
                        .checked_add(lifecycle.vectors_remaining.unwrap_or(0))
                        .ok_or_else(|| {
                            Error::Other("vector maintenance remaining overflow".to_owned())
                        })
                })?,
        )
    };
    let oldest_base_tx = partitions.iter().filter_map(|row| row.base_tx).min();
    let newest_base_tx = partitions.iter().filter_map(|row| row.base_tx).max();
    let pending_inserts =
        vector_inspection_partition_sum(partitions, "pending inserts", |row| row.pending_inserts)?;
    let tombstones =
        vector_inspection_partition_sum(partitions, "tombstones", |row| row.tombstones)?;
    let durable_vector_bytes =
        vector_inspection_partition_sum(partitions, "durable vector bytes", |row| {
            row.durable_vector_bytes
        })?;
    let charged_vector_bytes =
        vector_inspection_partition_sum(partitions, "charged vector bytes", |row| {
            row.charged_vector_bytes
        })?;
    let durable_index_bytes =
        vector_inspection_partition_sum(partitions, "durable index bytes", |row| {
            row.durable_index_bytes
        })?;
    let charged_index_bytes =
        vector_inspection_partition_sum(partitions, "charged index bytes", |row| {
            row.charged_index_bytes
        })?;
    let bytes = charged_vector_bytes
        .checked_add(charged_index_bytes)
        .ok_or_else(|| Error::Other("vector inspection charged byte total overflow".to_owned()))?;
    let broad_route_state = fanout_broad_route_state(partitions);
    let broad_route_reason = lifecycle
        .iter()
        .map(|lifecycle| lifecycle.availability_reason)
        .find(|reason| *reason != "none")
        .unwrap_or("none");
    let broad_route_recovery_action = broad_route_recovery_action(&lifecycle, broad_route_reason);
    let partition_key_columns = serde_json::Value::Array(
        declaration
            .partition_key_columns
            .as_deref()
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(serde_json::Value::String)
            .collect(),
    );

    // Every value below comes from the one passive partition snapshot. The
    // row builder aggregates it without loading a vector or graph page and
    // leaves facts absent when the store has no represented value.
    Ok(vec![
        Value::Text(info.index.table),
        Value::Text(info.index.column),
        vector_inspection_usize(info.layout.dimension, "dimension")?,
        Value::Text(info.layout.quantization.as_str().to_owned()),
        vector_inspection_usize(vector_count, "vector_count")?,
        vector_inspection_usize(bytes, "bytes")?,
        Value::Json(partition_key_columns.into()),
        declaration
            .max_partitions
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        vector_inspection_usize(live_partitions, "live_partitions")?,
        vector_inspection_usize(retained_partitions, "retained_partitions")?,
        Value::Text(declaration.search_mode.as_str().to_owned()),
        declaration
            .auto_index_at
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        vector_inspection_usize(
            info.layout.effective_auto_index_at(),
            "effective_auto_index_at",
        )?,
        declaration
            .hnsw_m
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        declaration
            .hnsw_ef_construction
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        declaration
            .hnsw_ef_search
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        if declaration.consolidation_disabled {
            Value::Text("none".to_owned())
        } else if declaration.consolidation_change_percent.is_some()
            || declaration.consolidation_tombstone_percent.is_some()
        {
            Value::Text("thresholds".to_owned())
        } else {
            Value::Null
        },
        declaration
            .consolidation_change_percent
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        declaration
            .consolidation_tombstone_percent
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        Value::Text(
            if info.layout.consolidation_disabled {
                "none"
            } else {
                "thresholds"
            }
            .to_owned(),
        ),
        if info.layout.consolidation_disabled {
            Value::Null
        } else {
            Value::Int64(i64::from(
                info.layout.effective_consolidation_change_percent(),
            ))
        },
        if info.layout.consolidation_disabled {
            Value::Null
        } else {
            Value::Int64(i64::from(
                info.layout.effective_consolidation_tombstone_percent(),
            ))
        },
        vector_inspection_optional_tx(oldest_base_tx, "oldest_base_tx")?,
        vector_inspection_optional_tx(newest_base_tx, "newest_base_tx")?,
        vector_inspection_usize(pending_inserts, "pending_inserts")?,
        vector_inspection_usize(tombstones, "tombstones")?,
        vector_inspection_usize(durable_vector_bytes, "durable_vector_bytes")?,
        vector_inspection_usize(charged_vector_bytes, "charged_vector_bytes")?,
        vector_inspection_usize(durable_index_bytes, "durable_index_bytes")?,
        vector_inspection_usize(charged_index_bytes, "charged_index_bytes")?,
        Value::Text(vector_index_query_state(partitions).to_owned()),
        Value::Text(maintenance_state.to_owned()),
        vector_inspection_optional_usize(maintenance_vectors_total, "maintenance_vectors_total")?,
        vector_inspection_optional_usize(maintenance_vectors_done, "maintenance_vectors_done")?,
        vector_inspection_optional_usize(
            maintenance_vectors_remaining,
            "maintenance_vectors_remaining",
        )?,
        vector_inspection_usize(unavailable_partitions, "unavailable_partitions")?,
        vector_inspection_usize(stalled_partitions, "stalled_partitions")?,
        Value::Text("fanout".to_owned()),
        Value::Text(broad_route_state.to_owned()),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Text(broad_route_reason.to_owned()),
        Value::Text(broad_route_recovery_action.to_owned()),
    ])
}

fn vector_partition_inspection_row(
    partition_key_columns: &[String],
    layout: &contextdb_vector::store::VectorIndexLayout,
    partition: contextdb_vector::store::VectorPartitionInfo,
    maintenance_policy: MaintenancePolicy,
) -> Result<Vec<Value>> {
    let query_state = vector_partition_query_state(&partition);
    let lifecycle = vector_partition_inspection_lifecycle(layout, &partition, maintenance_policy);
    let desired = partition.desired_policy;
    let serving = partition.serving_policy;
    Ok(vec![
        Value::Text(partition.partition.index.table),
        Value::Text(partition.partition.index.column),
        vector_partition_key_json(partition_key_columns, &partition.partition.partition_key)?,
        vector_inspection_usize(partition.live_rows, "live_rows")?,
        vector_inspection_usize(partition.retained_rows, "retained_rows")?,
        partition
            .base_generation
            .map(|generation| vector_inspection_u64(generation, "base_generation"))
            .transpose()?
            .unwrap_or(Value::Null),
        vector_inspection_optional_tx(partition.base_tx, "base_tx")?,
        vector_inspection_usize(partition.pending_inserts, "pending_inserts")?,
        vector_inspection_usize(partition.tombstones, "tombstones")?,
        vector_inspection_usize(partition.durable_vector_bytes, "durable_vector_bytes")?,
        vector_inspection_usize(partition.charged_vector_bytes, "charged_vector_bytes")?,
        vector_inspection_usize(partition.durable_index_bytes, "durable_index_bytes")?,
        vector_inspection_usize(partition.charged_index_bytes, "charged_index_bytes")?,
        Value::Text(query_state.to_owned()),
        Value::Text(lifecycle.availability_reason.to_owned()),
        Value::Text(lifecycle.maintenance_state.to_owned()),
        Value::Text(lifecycle.maintenance_reason.to_owned()),
        vector_inspection_optional_usize(lifecycle.vectors_total, "maintenance_vectors_total")?,
        vector_inspection_optional_usize(lifecycle.vectors_done, "maintenance_vectors_done")?,
        vector_inspection_optional_usize(
            lifecycle.vectors_remaining,
            "maintenance_vectors_remaining",
        )?,
        vector_inspection_optional_tx(lifecycle.checkpoint_tx, "maintenance_checkpoint_tx")?,
        vector_inspection_usize(desired.hnsw_m, "desired_hnsw_m")?,
        vector_inspection_usize(desired.hnsw_ef_construction, "desired_hnsw_ef_construction")?,
        vector_inspection_usize(desired.hnsw_ef_search, "desired_hnsw_ef_search")?,
        serving
            .map(|policy| vector_inspection_usize(policy.hnsw_m, "serving_hnsw_m"))
            .transpose()?
            .unwrap_or(Value::Null),
        serving
            .map(|policy| {
                vector_inspection_usize(policy.hnsw_ef_construction, "serving_hnsw_ef_construction")
            })
            .transpose()?
            .unwrap_or(Value::Null),
        serving
            .map(|_| vector_inspection_usize(desired.hnsw_ef_search, "serving_hnsw_ef_search"))
            .transpose()?
            .unwrap_or(Value::Null),
        if layout.consolidation_disabled {
            Value::Text("none".to_owned())
        } else if layout.consolidation_change_percent.is_some()
            || layout.consolidation_tombstone_percent.is_some()
        {
            Value::Text("thresholds".to_owned())
        } else {
            Value::Null
        },
        layout
            .consolidation_change_percent
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        layout
            .consolidation_tombstone_percent
            .map(|value| Value::Int64(i64::from(value)))
            .unwrap_or(Value::Null),
        Value::Text(
            if layout.consolidation_disabled {
                "none"
            } else {
                "thresholds"
            }
            .to_owned(),
        ),
        if layout.consolidation_disabled {
            Value::Null
        } else {
            Value::Int64(i64::from(layout.effective_consolidation_change_percent()))
        },
        if layout.consolidation_disabled {
            Value::Null
        } else {
            Value::Int64(i64::from(
                layout.effective_consolidation_tombstone_percent(),
            ))
        },
        vector_inspection_u64(desired.policy_revision, "desired_policy_revision")?,
        serving
            .map(|policy| vector_inspection_u64(policy.policy_revision, "serving_policy_revision"))
            .transpose()?
            .unwrap_or(Value::Null),
        Value::Text(lifecycle.recovery_action.to_owned()),
    ])
}

/// `CREATE TABLE`, off the recursing executor's own stack frame.
///
/// `execute_plan_once` is one `match` over every plan variant, and a debug
/// build gives EVERY arm's locals their own slot in that one frame -- which
/// is the frame the executor re-enters once per plan layer. A DDL arm can
/// never appear inside a recursive read plan, yet its `TableMeta` clones and
/// schema vectors were sizing every level of, say, a four-CTE graph
/// statement. `#[inline(never)]` keeps those locals in a frame of their own
/// that is entered once and only for DDL.
#[inline(never)]
fn execute_create_table_plan(
    db: &Database,
    p: &contextdb_planner::plan::CreateTablePlan,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    require_admin_for_create_table(db)?;
    db.check_disk_budget("CREATE TABLE")?;
    // Foreign-key and duplicate-table validation below reads the
    // complete schema, not only this table. Keep that baseline until
    // the DDL allocation lock so another structural write cannot make
    // a previously valid CREATE overwrite or contradict it.
    let validated_schema = db.relational_store().table_meta.read().clone();
    // A name the store already carries is answered from that same
    // baseline, before any other door judges the declaration: the
    // decision and the allocation lock's `schema_snapshot_matches`
    // guard must read one snapshot, or a table created between a
    // fresh read and the lock would slip past both and be redefined.
    //
    // Redefining an existing table is a SYNC operation only. The
    // arriving-DDL adopt arm (`database.rs`'s `CreateTable` branch)
    // takes a peer's declaration on purpose; a statement a person
    // typed locally never may, because replacing the column list
    // drops the values in every column the new text omits. So the
    // bare spelling refuses and destroys nothing, and `IF NOT
    // EXISTS` -- which the parser has always accepted and the
    // executor has never read -- is the documented no-op: the
    // existing table is left exactly as it is and the statement
    // answers ok.
    if validated_schema.contains_key(&p.name) {
        if p.if_not_exists {
            return Ok(OwnedQueryResult::empty_with_affected(0));
        }
        return Err(Error::SchemaInvalid {
            reason: format!(
                "table '{}' already exists: CREATE TABLE never redefines a table, so \
                 nothing was changed -- use CREATE TABLE IF NOT EXISTS to make a \
                 re-declaration a no-op, ALTER TABLE to change the shape, or DROP \
                 TABLE first",
                p.name
            ),
        });
    }
    if p.name.eq_ignore_ascii_case("acl_grants")
        && p.columns.iter().any(|column| column.acl_ref.is_some())
    {
        return Err(Error::SchemaInvalid {
            reason: "acl_grants cannot itself declare ACL-protected columns".to_string(),
        });
    }
    // Refused before anything else is validated: a consumer typing
    // one of the nine engine-owned reserved names with any other
    // shape (columns OR table-level options) never gets far enough
    // to build a half-made table.
    refuse_engine_owned_reserved_name_shape(p)?;
    // The contradiction is refused before anything is created, so a
    // rejected declaration never leaves a half-made table behind.
    refuse_promise_with_no_delivery(
        &p.name,
        p.retain.as_ref().is_some_and(|retain| retain.sync_safe),
        p.sync_direction,
    )?;
    let expires_column = expires_column_name(&p.columns)?;
    // Auto-generate implicit indexes for PK / UNIQUE columns and
    // composite UNIQUE constraints, so constraint probes run at
    // O(log n) instead of O(n) per insert.
    let mut auto_indexes: Vec<contextdb_core::IndexDecl> = Vec::new();
    for c in &p.columns {
        if c.primary_key && exact_constraint_key_indexable(&map_column_type(&c.data_type)) {
            auto_indexes.push(contextdb_core::IndexDecl {
                name: format!("__pk_{}", c.name),
                columns: vec![(c.name.clone(), contextdb_core::SortDirection::Asc)],
                kind: contextdb_core::IndexKind::Auto,
            });
        }
        if c.unique
            && !c.primary_key
            && exact_constraint_key_indexable(&map_column_type(&c.data_type))
        {
            auto_indexes.push(contextdb_core::IndexDecl {
                name: format!("__unique_{}", c.name),
                columns: vec![(c.name.clone(), contextdb_core::SortDirection::Asc)],
                kind: contextdb_core::IndexKind::Auto,
            });
        }
    }
    for uc in &p.unique_constraints {
        // Only index composite UNIQUE constraints whose columns are
        // all B-tree indexable.
        let all_indexable = uc.iter().all(|col_name| {
            p.columns
                .iter()
                .find(|c| c.name == *col_name)
                .map(|c| exact_constraint_key_indexable(&map_column_type(&c.data_type)))
                .unwrap_or(false)
        });
        if !all_indexable || uc.is_empty() {
            continue;
        }
        let name = format!("__unique_{}", uc.join("_"));
        let cols: Vec<(String, contextdb_core::SortDirection)> = uc
            .iter()
            .map(|c| (c.clone(), contextdb_core::SortDirection::Asc))
            .collect();
        auto_indexes.push(contextdb_core::IndexDecl {
            name,
            columns: cols,
            kind: contextdb_core::IndexKind::Auto,
        });
    }
    let composite_foreign_keys = p
        .composite_foreign_keys
        .iter()
        .map(|fk| contextdb_core::CompositeForeignKey {
            child_columns: fk.child_columns.clone(),
            parent_table: fk.parent_table.clone(),
            parent_columns: fk.parent_columns.clone(),
        })
        .collect::<Vec<_>>();

    let mut resolved_policies = HashMap::<String, ResolvedRankPolicy>::new();
    for column in &p.columns {
        if let Some(resolved) = validate_rank_policy_for_column(db, &p.name, column, &p.columns)? {
            resolved_policies.insert(column.name.clone(), resolved);
        }
    }
    let mut meta = TableMeta {
        columns: p
            .columns
            .iter()
            .map(|c| -> Result<contextdb_core::ColumnDef> {
                let mut column = core_column_from_ast(
                    &p.name,
                    c,
                    resolved_policies
                        .get(&c.name)
                        .map(|resolved| resolved.policy.clone()),
                )?;
                if p.primary_key_columns.contains(&c.name) {
                    column.nullable = false;
                }
                Ok(column)
            })
            .collect::<Result<Vec<_>>>()?,
        immutable: p.immutable,
        state_machine: p.state_machine.as_ref().map(|sm| StateMachineConstraint {
            column: sm.column.clone(),
            transitions: sm
                .transitions
                .iter()
                .map(|(from, tos)| (from.clone(), tos.clone()))
                .collect(),
        }),
        dag_edge_types: p.dag_edge_types.clone(),
        unique_constraints: p.unique_constraints.clone(),
        natural_key_column: None,
        propagation_rules: p.propagation_rules.clone(),
        default_ttl_seconds: p.retain.as_ref().map(|r| r.duration_seconds),
        sync_safe: p.retain.as_ref().is_some_and(|r| r.sync_safe),
        expires_column,
        // Auto-indexes land in `TableMeta.indexes` at CREATE TABLE
        // time with `kind == IndexKind::Auto`. The planner sees them
        // (so PK / UNIQUE point probes pick IndexScan); the user-
        // visible `.schema` render suppresses them by default but
        // keeps them in `EXPLAIN <query>` so agents can assert
        // routing programmatically.
        indexes: auto_indexes.clone(),
        composite_foreign_keys,
        sync_direction: p.sync_direction,
        retain_declared_unit: p.retain.as_ref().map(|retain| retain.declared_unit),
        primary_key_columns: p.primary_key_columns.clone(),
        conflict_policy: p.conflict_policy,
        history_policy: p.history,
        delivery_manifest_tables: p.delivery_manifest_tables.clone(),
        edge_discard: p.edge_discard,
        maintained_counts: p.maintained_counts.clone(),
    };
    validate_vector_partition_declarations(&p.name, &meta)?;
    // The policy half of the reserved-name door (the column-shape
    // half already ran above): an EXPLICIT axis this local CREATE
    // declares that differs from one of the four tables' canonical
    // policy is refused, exactly like the local ALTER door and the
    // sync-apply doors judge the identical question -- silence is
    // still tolerated for an undeclared table, as constructed by this file's
    // reconciliation tests.
    // Validate before publishing any local CREATE.
    crate::custody::policy::check_binding(db, &p.name, &meta, read_memory.clone())?;
    refuse_engine_owned_policy_axes(&p.name, &meta)?;
    // The narrower SYNC-CONFLICT-only counterpart of the axis door
    // above, for the five hub-refereed work-ledger tables that door
    // does not cover (see the function's own doc comment for why
    // they need a separate, axis-scoped door rather than a widened
    // `ENGINE_OWNED_LEDGER_TABLES`).
    refuse_hub_refereed_ledger_sync_conflict_mismatch(&p.name, meta.conflict_policy)?;
    refuse_sync_safe_without_key(&p.name, &meta)?;
    // Refused before anything is created: a table that both
    // delivers and arbitrates non-overwriting must not declare
    // HISTORY CURRENT ONLY (see the door's shared doc comment).
    refuse_reclaimed_history_under_keep_first(&p.name, &meta)?;
    let candidate_meta = meta.clone();
    validate_exact_constraint_keys_for_meta(&p.name, &meta)?;
    validate_single_column_foreign_keys_for_meta(&p.name, &meta, |parent| {
        if parent == p.name {
            Some(candidate_meta.clone())
        } else {
            db.table_meta(parent)
        }
    })?;
    validate_composite_foreign_keys_for_meta(&p.name, &meta, |parent| {
        if parent == p.name {
            Some(candidate_meta.clone())
        } else {
            db.table_meta(parent)
        }
    })?;
    meta.indexes = auto_indexes_for_table_meta(&meta);
    auto_indexes = meta.indexes.clone();
    // Judged once the index set this table will actually carry is
    // final, because a declared list must be one some index leads
    // with.
    crate::maintained_counts::validate_maintained_counts_for_meta(&p.name, &meta)?;
    let metadata_bytes =
        crate::database::retained_table_metadata_bytes(db.relational_store(), &meta);
    let has_vector_columns = meta
        .columns
        .iter()
        .any(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)));
    if has_vector_columns {
        let vector_schema_refs = meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)))
            .map(|column| VectorIndexRef::new(&p.name, column.name.clone()));
        let _vector_schema = db.vector_schema_write_many(vector_schema_refs)?;
        db.allocate_ddl_lsn(|lsn| {
            if !schema_snapshot_matches(db, &validated_schema) {
                return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
            }
            // A generation belongs to the CREATE that actually won
            // the schema lock, never to a stale validation attempt.
            let table_generation =
                db.next_table_generation_for_create(&p.name, read_memory.clone())?;
            db.accountant().try_allocate_for(
                metadata_bytes,
                "ddl",
                "create_table",
                "Reduce schema size or raise MEMORY_LIMIT before creating more tables.",
            )?;
            if let Err(error) =
                db.persist_created_table_generation_and_ddl(&p.name, &meta, table_generation, lsn)
            {
                db.accountant().release(metadata_bytes);
                return Err(error);
            }
            db.relational_store().create_table(&p.name, meta);
            for idx in &auto_indexes {
                db.relational_store().create_auto_index_storage_with_budget(
                    &p.name,
                    &idx.name,
                    idx.columns.clone(),
                    db.bounded_read_accountant(),
                );
            }
            // Publication installs vector layouts from the newly prepared
            // schema, rather than the statement's pre-DDL read descriptor.
            if let Some(table_meta) = db.relational_store().table_meta(&p.name) {
                for column in &table_meta.columns {
                    db.register_vector_index_for_column(
                        &p.name,
                        column,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                }
            }
            for (column, resolved) in resolved_policies {
                db.register_rank_formula(&p.name, &column, resolved.formula);
            }
            Ok(())
        })?;
        db.clear_statement_cache();
        db.reconcile_maintenance_thread();
        return Ok(OwnedQueryResult::empty_with_affected(0));
    } else {
        db.allocate_ddl_lsn(|lsn| {
            if !schema_snapshot_matches(db, &validated_schema) {
                return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
            }
            let table_generation =
                db.next_table_generation_for_create(&p.name, read_memory.clone())?;
            db.accountant().try_allocate_for(
                metadata_bytes,
                "ddl",
                "create_table",
                "Reduce schema size or raise MEMORY_LIMIT before creating more tables.",
            )?;
            if let Err(error) =
                db.persist_created_table_generation_and_ddl(&p.name, &meta, table_generation, lsn)
            {
                db.accountant().release(metadata_bytes);
                return Err(error);
            }
            db.relational_store().create_table(&p.name, meta);
            for idx in &auto_indexes {
                db.relational_store().create_auto_index_storage_with_budget(
                    &p.name,
                    &idx.name,
                    idx.columns.clone(),
                    db.bounded_read_accountant(),
                );
            }
            Ok(())
        })?;
    }
    db.clear_statement_cache();
    db.reconcile_maintenance_thread();
    Ok(OwnedQueryResult::empty_with_affected(0))
}

/// `ALTER TABLE`, off the recursing executor's own stack frame. See
/// [`execute_create_table_plan`] for why.
#[inline(never)]
fn execute_alter_table_plan(
    db: &Database,
    p: &contextdb_planner::plan::AlterTablePlan,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    require_admin_for_ddl(db)?;
    db.check_disk_budget("ALTER TABLE")?;
    let store = db.relational_store();
    // ADD/DROP/RENAME validation may depend on foreign keys in other
    // tables. Compare the complete map while holding the DDL lock,
    // then transparently re-run validation if any schema writer won.
    let validated_schema = store.table_meta.read().clone();
    let metadata_only_projection: Option<(TableMeta, TableMeta)>;
    let mut vector_layout_update = None;
    match &p.action {
        AlterAction::AddColumn(col) => {
            // Existence first, like every other ALTER clause in this
            // match (see `SetSyncConflict`'s own comment): an absent
            // table must report "table not found", never the
            // engine-owned refusal, which implies a real, governed
            // table exists.
            if db.table_meta(&p.table).is_none() {
                return Err(Error::Other(format!("table '{}' not found", p.table)));
            }
            refuse_engine_owned_reserved_name_column_alter(&p.table)?;
            if col.primary_key {
                return Err(Error::Other(
                    "adding a primary key column via ALTER TABLE is not supported".to_string(),
                ));
            }
            validate_expires_column(col)?;
            // If the new column is flagged IMMUTABLE, refuse to add it if any
            // existing propagation rule would write into a column of that name.
            // This closes the DROP-then-ADD-as-flagged loophole (Gotcha 13).
            if col.immutable
                && let Some(existing_meta) = db.table_meta(&p.table)
            {
                let targets_col = existing_meta
                    .propagation_rules
                    .iter()
                    .any(|rule| match rule {
                        contextdb_core::table_meta::PropagationRule::ForeignKey {
                            target_state,
                            ..
                        } => *target_state == col.name,
                        contextdb_core::table_meta::PropagationRule::Edge {
                            target_state, ..
                        } => *target_state == col.name,
                        contextdb_core::table_meta::PropagationRule::VectorExclusion { .. } => {
                            false
                        }
                    });
                if targets_col {
                    return Err(Error::ImmutableColumn {
                        table: p.table.clone(),
                        column: col.name.clone(),
                    });
                }
            }
            let mut all_columns = db
                .table_meta(&p.table)
                .map(|meta| {
                    meta.columns
                        .into_iter()
                        .map(ast_column_from_core)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            all_columns.push((**col).clone());
            let resolved_policy = validate_rank_policy_for_column(db, &p.table, col, &all_columns)?;
            let core_col = core_column_from_ast(
                &p.table,
                col,
                resolved_policy
                    .as_ref()
                    .map(|resolved| resolved.policy.clone()),
            )?;
            if let Some(existing_meta) = db.table_meta(&p.table) {
                let mut candidate_meta = existing_meta;
                candidate_meta.columns.push(core_col.clone());
                validate_vector_partition_declarations(&p.table, &candidate_meta)?;
                let candidate_lookup = candidate_meta.clone();
                validate_exact_constraint_keys_for_meta(&p.table, &candidate_meta)?;
                validate_single_column_foreign_keys_for_meta(
                    &p.table,
                    &candidate_meta,
                    |parent| {
                        if parent == p.table {
                            Some(candidate_lookup.clone())
                        } else {
                            db.table_meta(parent)
                        }
                    },
                )?;
            }
            let added_vector = matches!(col.data_type, DataType::Vector(_));
            let _vector_schema = added_vector
                .then(|| db.vector_schema_write(&VectorIndexRef::new(&p.table, col.name.clone())))
                .transpose()?;
            db.allocate_ddl_lsn(|lsn| {
                let old_meta = db
                    .table_meta(&p.table)
                    .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                if !schema_snapshot_matches(db, &validated_schema) {
                    return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
                }
                if old_meta
                    .columns
                    .iter()
                    .any(|column| column.name == col.name)
                {
                    return Err(Error::Other(format!(
                        "column '{}' already exists in table '{}'",
                        col.name, p.table
                    )));
                }
                let mut projected_meta = old_meta.clone();
                projected_meta.columns.push(core_col.clone());
                if col.expires {
                    projected_meta.expires_column = Some(col.name.clone());
                }
                let projected_meta = db.project_table_meta_with_auto_indexes(projected_meta);
                let projected_rows = db.table_versions_for_projection(&p.table)?;
                let ddl = vec![db.alter_table_ddl_for_meta(&p.table, &projected_meta, Vec::new())];
                let mut memory_swap = db.prepare_local_table_projection_memory(
                    &p.table,
                    &old_meta,
                    &projected_meta,
                    &projected_rows,
                )?;
                let projected_indexes =
                    db.prepare_local_index_projection(&projected_meta, &projected_rows)?;
                db.persist_local_table_projection_and_ddl(
                    &p.table,
                    &projected_meta,
                    &projected_rows,
                    lsn,
                    &ddl,
                    read_memory.clone(),
                )?;
                db.publish_local_table_projection_and_ddl(
                    &p.table,
                    projected_meta.clone(),
                    projected_rows.clone(),
                    lsn,
                    &ddl,
                    projected_indexes,
                )?;
                memory_swap.commit_after_swap();
                if added_vector {
                    db.register_vector_index_for_column(
                        &p.table,
                        &core_col,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                }
                if let Some(resolved) = &resolved_policy {
                    db.register_rank_formula(&p.table, &col.name, resolved.formula.clone());
                }
                Ok(())
            })?;
            db.clear_statement_cache();
            return Ok(OwnedQueryResult::empty_with_affected(0));
        }
        AlterAction::DropColumn {
            column: name,
            cascade,
        } => {
            // Existence first -- see `AddColumn`'s own comment above.
            if db.table_meta(&p.table).is_none() {
                return Err(Error::Other(format!("table '{}' not found", p.table)));
            }
            refuse_engine_owned_reserved_name_column_alter(&p.table)?;
            if let Some(block) = vector_partition_key_component_blocker(db, &p.table, name) {
                return Err(block);
            }
            if let Some(block) = rank_policy_drop_column_blocker(db, &p.table, name) {
                return Err(block);
            }
            let dropped_vector_column = db
                .table_meta(&p.table)
                .and_then(|meta| meta.columns.into_iter().find(|column| column.name == *name))
                .is_some_and(|column| {
                    matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                });
            if let Some(existing_meta) = db.table_meta(&p.table)
                && let Some(col) = existing_meta.columns.iter().find(|c| c.name == *name)
                && col.immutable
            {
                return Err(Error::ImmutableColumn {
                    table: p.table.clone(),
                    column: name.clone(),
                });
            }
            // PK check precedes index-dependency reporting: a column
            // flagged PRIMARY KEY cannot be dropped (the auto-index
            // would also show as a dependency, but the actionable
            // error is that the PK cannot be removed).
            if let Some(existing_meta) = db.table_meta(&p.table)
                && let Some(col) = existing_meta.columns.iter().find(|c| c.name == *name)
                && col.primary_key
            {
                return Err(Error::Other(format!(
                    "cannot drop primary key column {}.{}",
                    p.table, name
                )));
            }
            // A table-level composite PRIMARY KEY (col, col, ...) does not
            // set the column-level primary_key flag above, so guard its key
            // columns explicitly: a composite key column is as un-droppable
            // as a single-column one. Dropping one would leave
            // primary_key_columns naming an absent column and strand rows
            // out of outgoing sync.
            if let Some(existing_meta) = db.table_meta(&p.table)
                && existing_meta.primary_key_columns.iter().any(|c| c == name)
            {
                return Err(Error::Other(format!(
                    "cannot drop primary key column {}.{}",
                    p.table, name
                )));
            }
            validate_projected_foreign_keys_after_drop_column(db, &p.table, name)?;
            // RESTRICT / CASCADE on indexed columns. Only user-declared
            // indexes gate the RESTRICT path — auto-indexes dissolve
            // naturally when their defining column leaves.
            let dependent_user_indexes: Vec<String> = db
                .table_meta(&p.table)
                .map(|m| {
                    m.indexes
                        .iter()
                        .filter(|i| {
                            i.kind == contextdb_core::IndexKind::UserDeclared
                                && i.columns.iter().any(|(c, _)| c == name)
                        })
                        .map(|i| i.name.clone())
                        .collect()
                })
                .unwrap_or_default();
            let dependent_indexes: Vec<String> = db
                .table_meta(&p.table)
                .map(|m| {
                    m.indexes
                        .iter()
                        .filter(|i| i.columns.iter().any(|(c, _)| c == name))
                        .map(|i| i.name.clone())
                        .collect()
                })
                .unwrap_or_default();
            if !*cascade && !dependent_user_indexes.is_empty() {
                return Err(Error::ColumnInIndex {
                    table: p.table.clone(),
                    column: name.clone(),
                    index: dependent_user_indexes[0].clone(),
                });
            }
            let _vector_schema = dropped_vector_column
                .then(|| {
                    let index = VectorIndexRef::new(&p.table, name.clone());
                    db.drain_vector_index_maintenance_for_ddl(&index, vector_copy_owner)?;
                    db.vector_schema_write(&index)
                })
                .transpose()?;
            db.allocate_ddl_lsn(|lsn| {
                let old_meta = db
                    .table_meta(&p.table)
                    .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                if !schema_snapshot_matches(db, &validated_schema) {
                    return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
                }
                if !old_meta.columns.iter().any(|column| column.name == *name) {
                    return Err(Error::Other(format!(
                        "column '{}' does not exist in table '{}'",
                        name, p.table
                    )));
                }
                let mut projected_meta = old_meta.clone();
                projected_meta.columns.retain(|column| column.name != *name);
                if *cascade {
                    projected_meta
                        .indexes
                        .retain(|index| !index.columns.iter().any(|(column, _)| column == name));
                }
                if projected_meta.expires_column.as_deref() == Some(name.as_str()) {
                    projected_meta.expires_column = None;
                }
                let projected_meta = db.project_table_meta_with_auto_indexes(projected_meta);
                let projected_rows = db
                    .table_versions_for_projection(&p.table)?
                    .into_iter()
                    .map(|mut row| {
                        row.values.remove(name);
                        row
                    })
                    .collect::<Vec<_>>();
                let mut ddl = dependent_user_indexes
                    .iter()
                    .filter(|index_name| *cascade && dependent_indexes.contains(index_name))
                    .map(|index_name| DdlChange::DropIndex {
                        table: p.table.clone(),
                        name: index_name.clone(),
                    })
                    .collect::<Vec<_>>();
                // The author's drop rides the signed ALTER as its own
                // marker: a receiver removes only a column a marker names,
                // never one an ALTER merely leaves out.
                ddl.push(db.alter_table_ddl_for_meta(
                    &p.table,
                    &projected_meta,
                    vec![crate::database::sync_column_drop_constraint(name)],
                ));
                let mut memory_swap = db.prepare_local_table_projection_memory(
                    &p.table,
                    &old_meta,
                    &projected_meta,
                    &projected_rows,
                )?;
                let projected_indexes =
                    db.prepare_local_index_projection(&projected_meta, &projected_rows)?;
                db.persist_local_table_projection_and_ddl(
                    &p.table,
                    &projected_meta,
                    &projected_rows,
                    lsn,
                    &ddl,
                    read_memory.clone(),
                )?;
                db.publish_local_table_projection_and_ddl(
                    &p.table,
                    projected_meta.clone(),
                    projected_rows.clone(),
                    lsn,
                    &ddl,
                    projected_indexes,
                )?;
                memory_swap.commit_after_swap();
                db.remove_rank_formula(&p.table, name);
                if dropped_vector_column {
                    db.deregister_vector_index(&p.table, name);
                }
                Ok(())
            })?;
            db.clear_statement_cache();
            return Ok(OwnedQueryResult::new(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: if *cascade {
                    Some(crate::database::CascadeReport {
                        dropped_indexes: dependent_indexes,
                    })
                } else {
                    None
                },
            }));
        }
        AlterAction::RenameColumn { from, to } => {
            // Existence first -- see `AddColumn`'s own comment above.
            if db.table_meta(&p.table).is_none() {
                return Err(Error::Other(format!("table '{}' not found", p.table)));
            }
            refuse_engine_owned_reserved_name_column_alter(&p.table)?;
            if let Some(block) = vector_partition_key_component_blocker(db, &p.table, from) {
                return Err(block);
            }
            if let Some(block) = rank_policy_drop_column_blocker(db, &p.table, from) {
                return Err(block);
            }
            let renamed_vector_column = db
                .table_meta(&p.table)
                .and_then(|meta| meta.columns.into_iter().find(|column| column.name == *from))
                .is_some_and(|column| {
                    matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                });
            if let Some(existing_meta) = db.table_meta(&p.table)
                && let Some(col) = existing_meta.columns.iter().find(|c| c.name == *from)
                && col.immutable
            {
                return Err(Error::ImmutableColumn {
                    table: p.table.clone(),
                    column: from.clone(),
                });
            }
            if let Some(existing_meta) = db.table_meta(&p.table)
                && let Some(col) = existing_meta.columns.iter().find(|c| c.name == *from)
                && col.primary_key
            {
                return Err(Error::Other(format!(
                    "cannot rename primary key column '{}'",
                    from
                )));
            }
            // A table-level composite PRIMARY KEY (col, col, ...) does not
            // set the column-level primary_key flag above, so guard its key
            // columns explicitly: renaming one leaves primary_key_columns
            // naming an absent column.
            if let Some(existing_meta) = db.table_meta(&p.table)
                && existing_meta.primary_key_columns.iter().any(|c| c == from)
            {
                return Err(Error::Other(format!(
                    "cannot rename primary key column '{}'",
                    from
                )));
            }
            validate_projected_foreign_keys_after_rename_column(db, &p.table, from, to)?;
            if renamed_vector_column
                && db
                    .table_meta(&p.table)
                    .is_some_and(|meta| meta.columns.iter().any(|c| c.name == *to))
            {
                return Err(Error::Other(format!(
                    "column '{}' already exists in table '{}'",
                    to, p.table
                )));
            }
            let _vector_schema = if renamed_vector_column {
                vector_copy_owner.reserve(
                    2 * std::mem::size_of::<VectorIndexRef>()
                        + 2 * p.table.len()
                        + from.len()
                        + to.len(),
                )?;
                let old_index = VectorIndexRef::new(&p.table, from.clone());
                let new_index = VectorIndexRef::new(&p.table, to.clone());
                db.validate_vector_index_rename_for_ddl(
                    &p.table,
                    from,
                    to,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                db.drain_vector_index_maintenance_for_ddl(&old_index, vector_copy_owner)?;
                Some(db.vector_schema_write_many([old_index, new_index])?)
            } else {
                None
            };
            db.allocate_ddl_lsn(|lsn| {
                let old_meta = db
                    .table_meta(&p.table)
                    .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                if !schema_snapshot_matches(db, &validated_schema) {
                    return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
                }
                if old_meta.columns.iter().any(|column| column.name == *to) {
                    return Err(Error::Other(format!(
                        "column '{}' already exists in table '{}'",
                        to, p.table
                    )));
                }
                let mut projected_meta = old_meta.clone();
                let column = projected_meta
                    .columns
                    .iter_mut()
                    .find(|column| column.name == *from)
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "column '{}' does not exist in table '{}'",
                            from, p.table
                        ))
                    })?;
                column.name = to.clone();
                for index in &mut projected_meta.indexes {
                    for (column, _) in &mut index.columns {
                        if *column == *from {
                            *column = to.clone();
                        }
                    }
                }
                if projected_meta.expires_column.as_deref() == Some(from.as_str()) {
                    projected_meta.expires_column = Some(to.clone());
                }
                let projected_meta = db.project_table_meta_with_auto_indexes(projected_meta);
                let projected_rows = db
                    .table_versions_for_projection(&p.table)?
                    .into_iter()
                    .map(|mut row| {
                        if let Some(value) = row.values.remove(from) {
                            row.values.insert(to.clone(), value);
                        }
                        row
                    })
                    .collect::<Vec<_>>();
                let ddl = vec![db.alter_table_ddl_for_meta(
                    &p.table,
                    &projected_meta,
                    if renamed_vector_column {
                        vec![crate::database::sync_vector_rename_constraint(from, to)]
                    } else {
                        // Every authored rename carries its marker in
                        // the signed constraints, so a receiver renames
                        // only what the author renamed.
                        vec![crate::database::sync_column_rename_constraint(from, to)]
                    },
                )];
                let mut memory_swap = db.prepare_local_table_projection_memory(
                    &p.table,
                    &old_meta,
                    &projected_meta,
                    &projected_rows,
                )?;
                let projected_indexes =
                    db.prepare_local_index_projection(&projected_meta, &projected_rows)?;
                // Finish vector-consuming preparation before the token takes
                // the registry gates. The executing DDL owner funds both refs
                // and the complete replacement registry/source handles.
                let prepared_vector_rename = if renamed_vector_column {
                    let mut rename_owner = vector_copy_owner.scratch();
                    rename_owner.reserve(
                        2 * std::mem::size_of::<VectorIndexRef>()
                            + 2 * p.table.len()
                            + from.len()
                            + to.len(),
                    )?;
                    let old_index = VectorIndexRef::new(&p.table, from.clone());
                    let new_index = VectorIndexRef::new(&p.table, to.clone());
                    Some(db.prepare_vector_index_rename(&old_index, &new_index, rename_owner)?)
                } else {
                    None
                };
                db.persist_local_table_projection_and_ddl(
                    &p.table,
                    &projected_meta,
                    &projected_rows,
                    lsn,
                    &ddl,
                    read_memory.clone(),
                )?;
                if let Some(prepared) = prepared_vector_rename {
                    prepared.publish();
                }
                db.publish_local_table_projection_and_ddl(
                    &p.table,
                    projected_meta.clone(),
                    projected_rows.clone(),
                    lsn,
                    &ddl,
                    projected_indexes,
                )?;
                memory_swap.commit_after_swap();
                Ok(())
            })?;
            db.clear_statement_cache();
            return Ok(OwnedQueryResult::empty_with_affected(0));
        }
        AlterAction::SetVectorMaxPartitions {
            column,
            max_partitions,
        } => {
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            refuse_engine_owned_reserved_name_column_alter(&p.table)?;
            let mut table_meta = old_meta.clone();
            set_vector_max_partitions(&p.table, column, max_partitions, &mut table_meta)?;
            validate_vector_partition_declarations(&p.table, &table_meta)?;
            vector_layout_update = Some(VectorIndexRef::new(p.table.clone(), column.clone()));
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetVectorSearchMode {
            column,
            search_mode,
        } => {
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            refuse_engine_owned_reserved_name_column_alter(&p.table)?;
            let mut table_meta = old_meta.clone();
            set_vector_search_mode(&p.table, column, *search_mode, &mut table_meta)?;
            validate_vector_partition_declarations(&p.table, &table_meta)?;
            vector_layout_update = Some(VectorIndexRef::new(p.table.clone(), column.clone()));
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetVectorAutoIndexAt {
            column,
            auto_index_at,
        } => {
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            refuse_engine_owned_reserved_name_column_alter(&p.table)?;
            let mut table_meta = old_meta.clone();
            set_vector_auto_index_at(&p.table, column, auto_index_at.as_deref(), &mut table_meta)?;
            validate_vector_partition_declarations(&p.table, &table_meta)?;
            vector_layout_update = Some(VectorIndexRef::new(p.table.clone(), column.clone()));
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetVectorHnsw { column, hnsw } => {
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            refuse_engine_owned_reserved_name_column_alter(&p.table)?;
            let mut table_meta = old_meta.clone();
            set_vector_hnsw(&p.table, column, hnsw.as_ref(), &mut table_meta)?;
            validate_vector_partition_declarations(&p.table, &table_meta)?;
            vector_layout_update = Some(VectorIndexRef::new(p.table.clone(), column.clone()));
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetVectorConsolidation {
            column,
            consolidation,
        } => {
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            refuse_engine_owned_reserved_name_column_alter(&p.table)?;
            let mut table_meta = old_meta.clone();
            set_vector_consolidation(&p.table, column, consolidation.as_ref(), &mut table_meta)?;
            validate_vector_partition_declarations(&p.table, &table_meta)?;
            vector_layout_update = Some(VectorIndexRef::new(p.table.clone(), column.clone()));
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetRetain {
            duration_seconds,
            sync_safe,
            declared_unit,
        } => {
            // Existence first, like every other ALTER clause in this
            // match: an absent table must report "table not found",
            // not the engine-owned refusal below -- that refusal
            // implies a real, governed table exists, which is not
            // true for a store that never installed the ledger.
            db.table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            // Refused before the write lock is taken, so a rejected
            // ALTER applies no part of itself — not the new window,
            // not the SYNC SAFE flag.
            refuse_engine_owned_policy_mutation(
                &p.table,
                EngineOwnedMutation::Retain {
                    window: Some((*duration_seconds, *declared_unit)),
                    sync_safe: *sync_safe,
                },
            )?;
            if *sync_safe {
                let existing = db
                    .table_meta(&p.table)
                    .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                refuse_promise_with_no_delivery(
                    &p.table,
                    true,
                    Some(effective_sync_direction(&existing)),
                )?;
                refuse_sync_safe_without_key_for(&p.table, &existing)?;
            }
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            let mut table_meta = old_meta.clone();
            if table_meta.immutable {
                return Err(Error::Other(
                    "IMMUTABLE and RETAIN are mutually exclusive".to_string(),
                ));
            }
            table_meta.default_ttl_seconds = Some(*duration_seconds);
            table_meta.sync_safe = *sync_safe;
            table_meta.retain_declared_unit = Some(*declared_unit);
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::DropRetain => {
            // Existence first, like every other ALTER clause in this
            // match: an absent table must report "table not found",
            // not the engine-owned refusal below -- that refusal
            // implies a real, governed table exists, which is not
            // true for a store that never installed the ledger.
            db.table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            // Refused before the write lock, matching SET RETAIN's
            // own comment.
            refuse_engine_owned_policy_mutation(
                &p.table,
                EngineOwnedMutation::Retain {
                    window: None,
                    sync_safe: false,
                },
            )?;
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            let mut table_meta = old_meta.clone();
            table_meta.default_ttl_seconds = None;
            table_meta.sync_safe = false;
            table_meta.retain_declared_unit = None;
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetHistory(policy) => {
            // Refused before the write lock, matching SET RETAIN's
            // own comment: a rejected ALTER applies no part of
            // itself. Projects the post-change meta (existing table,
            // history swapped for the new declaration) so the same
            // question is asked as at CREATE.
            let existing = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            let projected = TableMeta {
                history_policy: Some(*policy),
                ..existing
            };
            refuse_engine_owned_policy_mutation(&p.table, EngineOwnedMutation::History(*policy))?;
            refuse_reclaimed_history_under_keep_first(&p.table, &projected)?;
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            let mut table_meta = old_meta.clone();
            table_meta.history_policy = Some(*policy);
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetMaintainedCounts(declared) => {
            // Existence first, like every other ALTER clause in this
            // match. Refused before the write lock, so a rejected
            // ALTER applies no part of itself.
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            let mut table_meta = old_meta.clone();
            table_meta.maintained_counts = match declared {
                contextdb_parser::ast::MaintainedCountsDecl::Default => None,
                other => {
                    Some(contextdb_planner::planner::map_maintained_counts_declaration(other)?)
                }
            };
            crate::maintained_counts::validate_maintained_counts_for_meta(&p.table, &table_meta)?;
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetSyncConflict(policy) => {
            // Existence first, like every other ALTER clause in this
            // match: an absent table must report "table not found",
            // not the engine-owned refusal below -- that refusal
            // implies a real, governed table exists, which is not
            // true for a store that never installed the ledger.
            let existing = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            // A hub-refereed work-ledger table (work_jobs /
            // work_claims / work_results / work_failures /
            // work_cancellations) has no locally-settable declared
            // SYNC CONFLICT axis for the mirror guard below to
            // compare against -- but a restate of its own true
            // arbitration is still legal here, exactly like the
            // CREATE door and the four older engine-owned tables'
            // own ALTER door both tolerate; only a mismatch refuses.
            refuse_hub_refereed_ledger_sync_conflict_mismatch(&p.table, Some(*policy))?;
            // The mirror guard: narrowing an already-declared HISTORY
            // CURRENT ONLY table BACK to keep-first arbitration while
            // it still delivers is exactly the CREATE-time hazard
            // arriving through the back door, so it is refused here
            // too, before the write lock.
            let projected = TableMeta {
                conflict_policy: Some(*policy),
                ..existing
            };
            refuse_engine_owned_policy_mutation(
                &p.table,
                EngineOwnedMutation::SyncConflict(*policy),
            )?;
            refuse_reclaimed_history_under_keep_first(&p.table, &projected)?;
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            let mut table_meta = old_meta.clone();
            table_meta.conflict_policy = Some(*policy);
            metadata_only_projection = Some((old_meta, table_meta));
        }
        AlterAction::SetSyncDirection(direction) => {
            // Existence first, like every other ALTER clause in this
            // match: an absent table must report "table not found",
            // not the engine-owned refusal below -- that refusal
            // implies a real, governed table exists, which is not
            // true for a store that never installed the ledger.
            let existing = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            // Refused before the write lock, for the same reason: a
            // rejected ALTER applies no part of itself -- an
            // operator's own `SET SYNC ...` on one of the four
            // engine-owned tables is engine policy exactly like
            // `SET RETAIN` / `SET HISTORY` / `SET SYNC CONFLICT`
            // already are.
            refuse_engine_owned_policy_mutation(
                &p.table,
                EngineOwnedMutation::SyncDirection(*direction),
            )?;
            refuse_promise_with_no_delivery(&p.table, existing.sync_safe, Some(*direction))?;
            // Widening sync direction after CREATE to include delivery
            // while this table still carries HISTORY CURRENT ONLY under
            // keep-first arbitration recreates the CREATE-time hazard.
            let projected = TableMeta {
                sync_direction: Some(*direction),
                ..existing
            };
            refuse_reclaimed_history_under_keep_first(&p.table, &projected)?;
            let old_meta = db
                .table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            let mut table_meta = old_meta.clone();
            table_meta.sync_direction = Some(*direction);
            metadata_only_projection = Some((old_meta, table_meta));
        }
    }
    if let Some((old_meta, projected_meta)) = metadata_only_projection {
        // ALTER must retain every bound clause.
        crate::custody::policy::check_binding(db, &p.table, &projected_meta, read_memory.clone())?;
        let _vector_schema = vector_layout_update
            .as_ref()
            .map(|index| db.vector_schema_write(index))
            .transpose()?;
        db.allocate_ddl_lsn(|lsn| {
            db.table_meta(&p.table)
                .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
            if !schema_snapshot_matches(db, &validated_schema) {
                return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
            }
            if let Some(index) = &vector_layout_update {
                let column = projected_meta
                    .columns
                    .iter()
                    .find(|column| column.name == index.column)
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: index.clone(),
                    })?;
                db.validate_vector_layout_policy_for_ddl(
                    index,
                    column,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
            let projected_rows = db.table_versions_for_projection(&p.table)?;
            let ddl = vec![db.alter_table_ddl_for_meta(&p.table, &projected_meta, Vec::new())];
            let mut memory_swap = db.prepare_local_table_projection_memory(
                &p.table,
                &old_meta,
                &projected_meta,
                &projected_rows,
            )?;
            let projected_indexes =
                db.prepare_local_index_projection(&projected_meta, &projected_rows)?;
            db.persist_local_table_projection_and_ddl(
                &p.table,
                &projected_meta,
                &projected_rows,
                lsn,
                &ddl,
                read_memory.clone(),
            )?;
            db.publish_local_table_projection_and_ddl(
                &p.table,
                projected_meta.clone(),
                projected_rows.clone(),
                lsn,
                &ddl,
                projected_indexes,
            )?;
            memory_swap.commit_after_swap();
            if let Some(index) = &vector_layout_update {
                let column = projected_meta
                    .columns
                    .iter()
                    .find(|column| column.name == index.column)
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: index.clone(),
                    })?;
                db.register_vector_index_for_column(
                    &p.table,
                    column,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
            Ok(())
        })?;
    }
    db.clear_statement_cache();
    db.reconcile_maintenance_thread();
    Ok(OwnedQueryResult::empty_with_affected(0))
}

/// What ONE layer of the eager executor's recursion costs in stack, pinned.
///
/// This frame is paid by each nested layer of an eager recursive query.
/// `Database::execute_at_snapshot` runs `Database::execute`, which runs
/// `execute_plan_once`, and every `Project`/`Sort`/`Limit`/`Filter`/`Join`
/// arm re-enters it for its input -- so a deep statement pays this frame once
/// per plan layer. `execute_plan_once` is one `match` over every plan
/// variant, and an unoptimised build gives EVERY arm's locals a slot in that
/// single frame, whether or not the arm runs. That is why moving the
/// `CREATE TABLE` and `ALTER TABLE` bodies out of it reduced the layer from
/// 250,464 bytes to 121,200. The ceiling below allows eight layers to consume
/// at most two-thirds of a two-mebibyte thread, reserving one-third for the
/// surrounding call stack and storage engine.
///
/// The bounded kernel's own per-layer ceiling lives in
/// `executor::bounded::plan_layer_frame_size` and guards a different path.
#[cfg(test)]
mod eager_plan_layer_frame {
    use super::*;

    /// Stack and plan depth exercised by the recursive CTE query below.
    const RECURSIVE_QUERY_THREAD_STACK_BYTES: usize = 2 << 20;
    const RECURSIVE_QUERY_PLAN_LAYERS: usize = 8;

    /// A third of the thread is reserved for everything that is NOT this
    /// recursion -- the frames above it, the storage engine's own, and the
    /// layer that starts it. The rest, divided by the recursive query's
    /// eight plan layers, is what one layer may cost.
    const EAGER_PLAN_LAYER_CEILING_BYTES: usize =
        RECURSIVE_QUERY_THREAD_STACK_BYTES * 2 / 3 / RECURSIVE_QUERY_PLAN_LAYERS;

    thread_local! {
        /// `(depth, stack address at depth 1, widest per-layer delta, deepest)`.
        static FRAME: std::cell::Cell<(usize, usize, usize, usize)> =
            const { std::cell::Cell::new((0, 0, 0, 0)) };
    }

    pub(super) struct Layer;

    impl Layer {
        /// `here` is the address of a local in the RECURSING frame, taken by
        /// the caller. It must not be `&0u8` or any other promotable rvalue:
        /// those become `'static` and measure nothing, which this module's
        /// own first run proved by reporting zero.
        pub(super) fn enter(here: usize) -> Self {
            FRAME.with(|cell| {
                let (depth, top, widest, deepest) = cell.get();
                let depth = depth + 1;
                let top = if depth == 1 { here } else { top };
                let widest = if depth > 1 {
                    widest.max(top.saturating_sub(here) / (depth - 1))
                } else {
                    widest
                };
                cell.set((depth, top, widest, deepest.max(depth)));
            });
            Self
        }
    }

    impl Drop for Layer {
        fn drop(&mut self) {
            FRAME.with(|cell| {
                let (depth, top, widest, deepest) = cell.get();
                cell.set((depth.saturating_sub(1), top, widest, deepest));
            });
        }
    }

    /// The recursive CTE statement pins the frame it actually pays rather
    /// than a synthetic stand-in whose plan shape might
    /// not nest at all. Kept in step with
    /// `tests/sql/four_cte_graph_composition_runs_on_a_two_mebibyte_thread`.
    const FOUR_CTE_DISCOVERY: &str = "WITH based AS (
    SELECT d_id FROM GRAPH_TABLE (edges MATCH (e)<-[:BASED_ON]-(d) WHERE e.id = $entity COLUMNS (d.id AS d_id))
), about AS (
    SELECT d_id FROM GRAPH_TABLE (edges MATCH (e)<-[:ABOUT]-(d) WHERE e.id = $entity COLUMNS (d.id AS d_id))
), served_by_based AS (
    SELECT i_id FROM GRAPH_TABLE (edges MATCH (d)-[:SERVES]->(i) WHERE d.id IN (SELECT d_id FROM based) COLUMNS (i.id AS i_id))
), served_by_about AS (
    SELECT i_id FROM GRAPH_TABLE (edges MATCH (d)-[:SERVES]->(i) WHERE d.id IN (SELECT d_id FROM about) COLUMNS (i.id AS i_id))
)
SELECT id FROM nodes
WHERE id IN (SELECT i_id FROM served_by_based)
   OR id IN (SELECT i_id FROM served_by_about)";

    fn node(value: u128) -> contextdb_core::Value {
        contextdb_core::Value::Uuid(uuid::Uuid::from_u128(
            0xC7E0_0000_0000_0000_0000_0000_0000_0000 + value,
        ))
    }

    fn measure() -> (usize, usize) {
        FRAME.with(|cell| cell.set((0, 0, 0, 0)));
        let db = Database::open_memory();
        db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &HashMap::new())
            .expect("node table");
        db.execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &HashMap::new(),
        )
        .expect("edge table");
        for n in 1..=5u128 {
            db.execute(
                "INSERT INTO nodes (id) VALUES ($id)",
                &HashMap::from([("id".to_string(), node(n))]),
            )
            .expect("node row");
        }
        for (id, source, target, kind) in [
            (100u128, 2u128, 1u128, "BASED_ON"),
            (101, 3, 1, "ABOUT"),
            (102, 2, 4, "SERVES"),
            (103, 3, 5, "SERVES"),
        ] {
            db.execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, $edge_type)",
                &HashMap::from([
                    ("id".to_string(), node(id)),
                    ("source".to_string(), node(source)),
                    ("target".to_string(), node(target)),
                    (
                        "edge_type".to_string(),
                        contextdb_core::Value::Text(kind.to_string()),
                    ),
                ]),
            )
            .expect("edge row");
        }
        db.execute(
            FOUR_CTE_DISCOVERY,
            &HashMap::from([("entity".to_string(), node(1))]),
        )
        .expect("the measured read");
        let (_, _, widest, deepest) = FRAME.with(|cell| cell.get());
        (widest, deepest)
    }

    #[test]
    fn one_eager_plan_layer_stays_inside_the_recursive_query_stack_budget() {
        let (per_layer, deepest) = measure();
        eprintln!(
            "eager plan layer: {per_layer} bytes/layer over {deepest} nested layers, ceiling \
             {EAGER_PLAN_LAYER_CEILING_BYTES}, {RECURSIVE_QUERY_PLAN_LAYERS} layers = {} of a \
             {RECURSIVE_QUERY_THREAD_STACK_BYTES}-byte thread",
            per_layer * RECURSIVE_QUERY_PLAN_LAYERS
        );
        assert!(
            deepest > 1 && per_layer > 0,
            "the probe saw {deepest} nested plan layers and measured {per_layer} bytes, so it \
             measured nothing: the statement did not recurse, or the sampled address is not on \
             the stack"
        );
        assert!(
            per_layer <= EAGER_PLAN_LAYER_CEILING_BYTES,
            "one eager plan layer is {per_layer} bytes; {RECURSIVE_QUERY_PLAN_LAYERS} of them is {} of \
             the {RECURSIVE_QUERY_THREAD_STACK_BYTES}-byte thread used by the four-CTE query, over the \
             {EAGER_PLAN_LAYER_CEILING_BYTES} a layer may cost. `execute_plan_once` gives every \
             arm's locals a slot in the one frame it recurses through, so a heavy new arm -- or a \
             heavy local in an existing one -- is paid by every layer of every deep statement. \
             Move the arm's body into an `#[inline(never)]` function instead of raising this.",
            per_layer * RECURSIVE_QUERY_PLAN_LAYERS
        );
    }
}

fn execute_plan_once(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    materialized_columns: Option<&[String]>,
    pending_sort_keys: Option<&[contextdb_planner::SortKey]>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    #[cfg(test)]
    let __frame_probe = 0u8;
    #[cfg(test)]
    let _layer = eager_plan_layer_frame::Layer::enter(&__frame_probe as *const u8 as usize);
    match plan {
        PhysicalPlan::WindowRank { .. } => execute_kernel_plan(db, plan, params, tx, read_memory),
        // The SQL doors read and write the canonical authority journal.
        PhysicalPlan::DeclareTenantTablePolicy(p) => crate::custody::policy::declare(
            db,
            &p.declaration,
            vector_copy_owner,
            read_memory.clone(),
        )
        .map(OwnedQueryResult::new),
        PhysicalPlan::ShowTenantTablePolicy { table } => {
            crate::custody::policy::show_policy(db, table.as_deref(), read_memory.clone())
                .map(OwnedQueryResult::new)
        }
        PhysicalPlan::ShowSyncBindings => {
            crate::custody::policy::show_bindings(db, read_memory.clone())
                .map(OwnedQueryResult::new)
        }
        PhysicalPlan::ShowDeliveryOutcomes(p) => crate::custody::inspection::show(
            db,
            &p.query.table,
            p.query.where_clause.as_ref(),
            p.query.limit.map(|n| n as usize),
            p.query.offset as usize,
            params,
            read_memory.clone(),
        )
        .map(OwnedQueryResult::new),
        PhysicalPlan::Discard(p) => {
            exec_discard(db, p, params, tx, vector_copy_owner, read_memory.clone())
        }
        PhysicalPlan::CreateTable(p) => {
            execute_create_table_plan(db, p, vector_copy_owner, read_memory.clone())
        }
        PhysicalPlan::DropTable(name) => {
            require_admin_for_ddl(db)?;
            let validated_schema = db.relational_store().table_meta.read().clone();
            if let Some(block) = rank_policy_drop_table_blocker(db, name) {
                return Err(block);
            }
            validate_projected_foreign_keys_after_drop_table(db, name)?;
            let bytes_to_release =
                estimate_drop_table_bytes(db, name, vector_copy_owner, read_memory.clone())?;
            db.drain_vector_table_maintenance_for_ddl(name, vector_copy_owner)?;
            let _vector_schema = db.vector_schema_write_table(name)?;
            db.allocate_ddl_lsn(|lsn| {
                if !schema_snapshot_matches(db, &validated_schema) {
                    return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
                }
                db.log_drop_table_ddl_and_remove_triggers(name, lsn, read_memory.clone())
            })?;
            db.accountant().release(bytes_to_release);
            db.clear_statement_cache();
            Ok(OwnedQueryResult::empty_with_affected(0))
        }
        PhysicalPlan::AlterTable(p) => {
            execute_alter_table_plan(db, p, vector_copy_owner, read_memory.clone())
        }
        PhysicalPlan::Insert(p) => exec_insert(db, p, params, tx, read_memory.clone()),
        PhysicalPlan::Purge(p) => {
            exec_purge(db, p, params, tx, vector_copy_owner, read_memory.clone())
        }
        PhysicalPlan::Delete(p) => {
            exec_delete(db, p, params, tx, vector_copy_owner, read_memory.clone())
        }
        PhysicalPlan::Update(p) => {
            exec_update(db, p, params, tx, vector_copy_owner, read_memory.clone())
        }
        PhysicalPlan::Scan { table, filter, .. } => {
            if table == "dual" {
                return Ok(OwnedQueryResult::new(QueryResult {
                    columns: vec![],
                    rows: vec![vec![]],
                    rows_affected: 0,
                    trace: crate::database::QueryTrace::scan(),
                    cascade: None,
                }));
            }
            let snapshot = db.snapshot_for_read();
            let schema_columns = db.table_meta(table).map(|meta| {
                meta.columns
                    .into_iter()
                    .map(|column| column.name)
                    .collect::<Vec<_>>()
            });
            let resolved_filter = filter
                .as_ref()
                .map(|expr| {
                    resolve_in_subqueries(
                        db,
                        expr,
                        params,
                        tx,
                        vector_copy_owner,
                        read_memory.clone(),
                    )
                })
                .transpose()?;
            let scan_materialized_columns =
                merge_expression_columns(materialized_columns, resolved_filter.iter());

            // Try to route through an IndexScan if the filter is index-eligible.
            // Analyze the PRE-resolve filter so `a IN (SELECT …)` disqualifies
            // the outer IndexScan even after the subquery has been executed.
            let meta_for_indexes = db.table_meta(table);
            let analysis = filter
                .as_ref()
                .filter(|_| {
                    meta_for_indexes
                        .as_ref()
                        .is_some_and(|meta| !meta.indexes.is_empty())
                })
                .and_then(|filter| {
                    meta_for_indexes.as_ref().map(|meta| {
                        // The eager executor never reverses an index walk
                        // (`execute_index_scan` has no such mode): allow_reverse=false,
                        // so it never picks a candidate whose only sort-eliding
                        // route it cannot actually take.
                        analyze_filter_for_index_with_sort(
                            table,
                            meta,
                            filter,
                            params,
                            pending_sort_keys,
                            false,
                        )
                    })
                });

            if let Some(a) = analysis {
                if let Some(pick) = a.pick {
                    // IndexScan path. Fetch by BTree range; apply residual filter.
                    let (rows, examined) = execute_index_scan(
                        db,
                        table,
                        &pick,
                        snapshot,
                        tx,
                        IndexScanAccessMode::Select,
                        scan_materialized_columns.as_deref(),
                        resolved_filter.as_ref(),
                        params,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    db.__bump_rows_examined(examined);
                    let mut result = materialize_rows(
                        rows,
                        resolved_filter.as_ref(),
                        params,
                        schema_columns.as_deref(),
                    )?;
                    let mut pushed: smallvec::SmallVec<[std::borrow::Cow<'static, str>; 4]> =
                        smallvec::SmallVec::new();
                    pushed.extend(
                        pick.pushed_columns
                            .iter()
                            .cloned()
                            .map(std::borrow::Cow::Owned),
                    );
                    let considered: smallvec::SmallVec<[crate::database::IndexCandidate; 4]> = a
                        .considered
                        .iter()
                        .filter(|c| c.name != pick.name)
                        .cloned()
                        .collect();
                    result.trace = crate::database::QueryTrace {
                        physical_plan: "IndexScan",
                        index_used: Some(pick.name.clone()),
                        predicates_pushed: pushed,
                        indexes_considered: considered,
                        sort_elided: false,
                        query_vector_source: None,
                        vector_search: None,
                        rows_examined: 0,
                        maintained_count: None,
                        groups: Box::new([]),
                    };
                    return Ok(result);
                } else {
                    // Scan with rejection trace.
                    let rows = scan_rows_for_select_statement(
                        db,
                        table,
                        snapshot,
                        tx,
                        resolved_filter.as_ref(),
                        params,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    db.__bump_rows_examined(rows.len() as u64);
                    let mut result = materialize_rows(
                        rows,
                        resolved_filter.as_ref(),
                        params,
                        schema_columns.as_deref(),
                    )?;
                    let considered: smallvec::SmallVec<[crate::database::IndexCandidate; 4]> =
                        a.considered.into_iter().collect();
                    result.trace = crate::database::QueryTrace {
                        physical_plan: "Scan",
                        index_used: None,
                        predicates_pushed: Default::default(),
                        indexes_considered: considered,
                        sort_elided: false,
                        query_vector_source: None,
                        vector_search: None,
                        rows_examined: 0,
                        maintained_count: None,
                        groups: Box::new([]),
                    };
                    return Ok(result);
                }
            }

            let rows = scan_rows_for_select_statement(
                db,
                table,
                snapshot,
                tx,
                resolved_filter.as_ref(),
                params,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            db.__bump_rows_examined(rows.len() as u64);
            let mut result = materialize_rows(
                rows,
                resolved_filter.as_ref(),
                params,
                schema_columns.as_deref(),
            )?;
            result.trace = crate::database::QueryTrace::scan();
            Ok(result)
        }
        PhysicalPlan::GraphBfs { .. } => {
            execute_kernel_plan(db, plan, params, tx, read_memory.clone())
        }
        PhysicalPlan::VectorSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            sort_key,
            search_mode,
            materialized_columns,
            ..
        }
        | PhysicalPlan::HnswSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            sort_key,
            search_mode,
            materialized_columns,
            ..
        } => {
            let snapshot = db.snapshot_for_read();
            let index = contextdb_core::VectorIndexRef::new(table.clone(), column.clone());
            let mut resolved_search_mode =
                db.resolved_vector_search_mode(&index, (*search_mode).map(map_vector_search_mode))?;
            bind_vector_search_parameters(query_expr, candidates.as_deref(), params)?;
            let route = vector_candidate_route(db, &index, candidates.as_deref(), params)?;
            let partition_scope = route.scope;
            let complete_relational_route = route.complete;
            let access_candidates = route.candidates;
            // Decide this before executing the candidate subtree.  Otherwise
            // INDEXED can quietly turn a broad WHERE clause into a table scan
            // and only then search the graph, despite promising a maintained
            // indexed route for the whole answer.
            if resolved_search_mode == contextdb_core::VectorSearchMode::Indexed
                && !complete_relational_route
            {
                return Err(Error::VectorFilteredRouteUnavailable {
                    index: index.clone(),
                    predicate_columns: vector_candidate_refusal_columns(
                        db,
                        &index,
                        access_candidates.as_ref(),
                        route.access_filter,
                    ),
                });
            }
            if resolved_search_mode == contextdb_core::VectorSearchMode::Auto
                && candidates.is_some()
                && !complete_relational_route
            {
                let exact_score_bytes = db
                    .vector_entry_count(&index, snapshot, vector_copy_owner)?
                    .saturating_mul(std::mem::size_of::<(contextdb_core::RowId, f32)>());
                if contextdb_core::read_memory::ReadCredit::try_new(
                    read_memory.clone(),
                    exact_score_bytes,
                    "vector_search",
                    "preflight_filtered_exact_scores",
                    "Add a relational index for the filter, reduce vector volume, or raise MEMORY_LIMIT.",
                )
                .is_err()
                {
                    return Err(Error::VectorFilteredRouteUnavailable {
                        index: index.clone(),
            predicate_columns: vector_candidate_refusal_columns(db, &index, access_candidates.as_ref(), route.access_filter),
                    });
                }
            }
            let forced_exact_for_relational_fallback = resolved_search_mode
                == contextdb_core::VectorSearchMode::Auto
                && !complete_relational_route;
            if forced_exact_for_relational_fallback {
                resolved_search_mode = contextdb_core::VectorSearchMode::Exact;
            }
            let mut candidate_trace = None;
            let unrestricted_scan_candidates = !route.access_filter
                && (partition_scope
                    .as_ref()
                    .is_some_and(VectorPartitionScope::covers_filter_for_execution)
                    || candidates
                        .as_deref()
                        .is_some_and(|plan| is_unrestricted_scan_for_table(plan, table)));
            let candidate_bitmap = if unrestricted_scan_candidates {
                candidate_trace = Some(QueryTrace::scan());
                None
            } else if route.directory_route {
                let mut source = candidates.as_deref();
                while let Some(PhysicalPlan::Project { input, .. }) = source {
                    source = Some(input);
                }
                let filter = match source {
                    Some(PhysicalPlan::Scan { filter, .. }) => filter.as_ref(),
                    _ => None,
                };
                Some(db.vector_directory_candidate_bitmap(
                    tx,
                    &index,
                    snapshot,
                    filter,
                    params,
                    partition_scope.as_ref(),
                    vector_copy_owner,
                    read_memory.clone(),
                )?)
            } else if let Some(cands_plan) = access_candidates.as_ref() {
                let qr = db.with_snapshot_override(snapshot, || {
                    execute_plan(
                        db,
                        cands_plan,
                        params,
                        tx,
                        vector_copy_owner,
                        read_memory.clone(),
                    )
                })?;
                candidate_trace = Some(qr.trace.clone());
                #[cfg(any(test, feature = "test-seams"))]
                db.__bump_candidate_rows_materialized_for_test(qr.rows.len() as u64);
                let mut bm = RoaringTreemap::new();
                let row_id_idx = qr.columns.iter().position(|column| {
                    column == "row_id" || column.rsplit('.').next() == Some("row_id")
                });
                let id_idx = qr
                    .columns
                    .iter()
                    .position(|column| column == "id" || column.rsplit('.').next() == Some("id"));

                if let Some(idx) = row_id_idx {
                    for row in qr.result.rows {
                        if let Some(Value::Int64(id)) = row.get(idx) {
                            bm.insert(*id as u64);
                        }
                    }
                } else if let Some(idx) = id_idx {
                    let uuid_to_row_id = uuid_to_row_id_map(
                        db,
                        table,
                        snapshot,
                        tx,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    for row in qr.result.rows {
                        if let Some(Value::Uuid(uuid)) = row.get(idx)
                            && let Some(row_id) = uuid_to_row_id.get(uuid)
                        {
                            bm.insert(row_id.0);
                        }
                    }
                }
                Some(bm)
            } else {
                None
            };

            let mut vector_schema_refs = vec![index.clone()];
            if let Some(source) = row_vector_source_ref(query_expr) {
                vector_schema_refs.push(source);
            }
            let _vector_schema = db.vector_schema_read_many(vector_schema_refs);
            db.assert_vector_index_exists_under_schema_read(&index)?;
            let (query_vec, query_vector_source) = resolve_query_vector_from_expr(
                db,
                query_expr,
                params,
                tx,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let vector_bytes = estimate_vector_search_bytes(query_vec.len(), *k as usize);
            let _vector_memory = contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                vector_bytes,
                "vector_search",
                "search",
                "Reduce LIMIT/dimensionality or raise MEMORY_LIMIT before vector search.",
            )?;
            if let Some(sort_key) = sort_key {
                let mut semantic_query = crate::database::SemanticQuery::new(
                    table.clone(),
                    column.clone(),
                    query_vec,
                    *k as usize,
                );
                semantic_query.sort_key = Some(sort_key.clone());
                semantic_query.search_mode = Some(resolved_search_mode);
                let search = || {
                    db.with_snapshot_override(snapshot, || {
                        db.semantic_search_with_candidates_under_schema_read_in_tx_with_strategy(
                            tx,
                            semantic_query,
                            candidate_bitmap,
                            vector_copy_owner,
                            read_memory.clone(),
                            Arc::new(bounded::TraceMemoryOwner(read_memory.clone())),
                        )
                    })
                };
                let res = if let Some(prefixes) = partition_scope
                    .as_ref()
                    .and_then(VectorPartitionScope::resolved_prefixes)
                {
                    contextdb_vector::mem::with_selected_partition_prefixes(
                        &index,
                        prefixes.to_vec(),
                        search,
                    )
                } else {
                    search()
                };
                let (results, mut route_trace) = res?;
                if forced_exact_for_relational_fallback {
                    mark_filtered_exact_route(&mut route_trace);
                }
                let used_hnsw = route_trace.used_hnsw;
                drop(_vector_memory);
                let schema_columns = db.table_meta(table).map(|meta| {
                    meta.columns
                        .into_iter()
                        .map(|column| column.name)
                        .collect::<Vec<_>>()
                });
                let keys = schema_columns.unwrap_or_else(|| {
                    let mut ks = BTreeSet::new();
                    for result in &results {
                        for key in result.values.keys() {
                            ks.insert(key.clone());
                        }
                    }
                    ks.into_iter().collect()
                });
                let mut columns = vec!["row_id".to_string()];
                columns.extend(keys.iter().cloned());
                columns.push("score".to_string());
                let rows = results
                    .into_iter()
                    .map(|result| {
                        let mut out = vec![Value::Int64(result.row_id.0 as i64)];
                        for key in &keys {
                            out.push(result.values.get(key).cloned().unwrap_or(Value::Null));
                        }
                        out.push(Value::Float64(result.rank as f64));
                        out
                    })
                    .collect();
                let mut trace = vector_search_trace_with_source(
                    if used_hnsw {
                        "HNSWSearch"
                    } else {
                        "VectorSearch"
                    },
                    candidate_trace,
                    query_vector_source,
                );
                trace.vector_search =
                    Some(db.vector_search_disclosure_for_runtime(
                        index.clone(),
                        *k as usize,
                        candidates.as_deref().is_some_and(|candidate| {
                            !is_unrestricted_scan_for_table(candidate, table)
                        }),
                        candidates.as_deref(),
                        *search_mode,
                        query_expr,
                        snapshot,
                        params,
                        &route_trace,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?);
                return Ok(OwnedQueryResult::new(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                    trace,
                    cascade: None,
                }));
            }
            let mut search = || {
                db.query_vector_strict_in_tx_with_strategy(
                    tx,
                    index.clone(),
                    &query_vec,
                    *k as usize,
                    candidate_bitmap.as_ref(),
                    snapshot,
                    resolved_search_mode,
                    vector_copy_owner,
                    read_memory.clone(),
                    Arc::new(bounded::TraceMemoryOwner(read_memory.clone())),
                )
            };
            let res = if let Some(prefixes) = partition_scope
                .as_ref()
                .and_then(VectorPartitionScope::resolved_prefixes)
            {
                contextdb_vector::mem::with_selected_partition_prefixes(
                    &index,
                    prefixes.to_vec(),
                    search,
                )
            } else {
                search()
            };
            let (res, mut route_trace) = res?;
            if forced_exact_for_relational_fallback {
                mark_filtered_exact_route(&mut route_trace);
            }
            let used_hnsw = route_trace.used_hnsw;
            drop(_vector_memory);

            // Re-materialize: look up actual rows by row_id so SELECT * returns user columns
            let result_row_ids = res.iter().map(|(rid, _)| *rid).collect::<Vec<_>>();
            let result_rows = rows_by_row_id(
                db,
                table,
                &result_row_ids,
                snapshot,
                tx,
                materialized_columns.as_deref(),
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let schema_columns = db.table_meta(table).map(|meta| {
                meta.columns
                    .into_iter()
                    .map(|column| column.name)
                    .collect::<Vec<_>>()
            });
            let keys = if let Some(ref sc) = schema_columns {
                sc.clone()
            } else {
                let mut ks = BTreeSet::new();
                for r in &result_rows {
                    for k in r.values.keys() {
                        ks.insert(k.clone());
                    }
                }
                ks.into_iter().collect::<Vec<_>>()
            };

            let row_map: HashMap<RowId, &VersionedRow> =
                result_rows.iter().map(|r| (r.row_id, r)).collect();

            let mut columns = vec!["row_id".to_string()];
            columns.extend(keys.iter().cloned());
            columns.push("score".to_string());

            let rows = res
                .into_iter()
                .filter_map(|(rid, score)| {
                    row_map.get(&rid).map(|row| {
                        let mut out = vec![Value::Int64(rid.0 as i64)];
                        for k in &keys {
                            out.push(row.values.get(k).cloned().unwrap_or(Value::Null));
                        }
                        out.push(Value::Float64(score as f64));
                        out
                    })
                })
                .collect();

            let mut trace = vector_search_trace_with_source(
                if used_hnsw {
                    "HNSWSearch"
                } else {
                    "VectorSearch"
                },
                candidate_trace,
                query_vector_source,
            );
            trace.vector_search = Some(
                db.vector_search_disclosure_for_runtime(
                    index,
                    *k as usize,
                    candidates
                        .as_deref()
                        .is_some_and(|candidate| !is_unrestricted_scan_for_table(candidate, table)),
                    candidates.as_deref(),
                    *search_mode,
                    query_expr,
                    snapshot,
                    params,
                    &route_trace,
                    vector_copy_owner,
                    read_memory.clone(),
                )?,
            );
            Ok(OwnedQueryResult::new(QueryResult {
                columns,
                rows,
                rows_affected: 0,
                trace,
                cascade: None,
            }))
        }
        PhysicalPlan::MaterializeCte { input, .. } => execute_materialized_cte(
            db,
            input,
            params,
            tx,
            vector_copy_owner,
            read_memory.clone(),
        ),
        PhysicalPlan::Project { input, columns } => {
            let projected_columns = expression_columns(columns.iter().map(|column| &column.expr));
            let input_result = execute_plan_with_materialized_columns(
                db,
                input,
                params,
                tx,
                Some(&projected_columns),
                pending_sort_keys,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            if projection_aggregates(columns) {
                let mut running = Vec::with_capacity(columns.len());
                for column in columns {
                    let Some(aggregate) = column_aggregate(column) else {
                        return Err(mixed_aggregate_error());
                    };
                    running.push(AggregateState::new(aggregate));
                }

                for row in &input_result.rows {
                    for (column, state) in columns.iter().zip(running.iter_mut()) {
                        let Expr::FunctionCall { args, .. } = &column.expr else {
                            return Err(mixed_aggregate_error());
                        };
                        match aggregate_input(args) {
                            AggregateInput::Row => state.admit(AggregateItem::Row)?,
                            AggregateInput::Nothing => state.admit(AggregateItem::Nothing)?,
                            AggregateInput::Value(argument) => {
                                let value = eval_query_result_expr(
                                    argument,
                                    row,
                                    &input_result.columns,
                                    params,
                                )?;
                                state.admit(AggregateItem::Value(&value))?;
                            }
                        }
                    }
                }

                let aggregate_row = running
                    .iter()
                    .map(AggregateState::finish)
                    .collect::<Result<Vec<_>>>()?;

                return Ok(OwnedQueryResult::new(QueryResult {
                    columns: project_output_columns(columns),
                    rows: vec![aggregate_row],
                    rows_affected: 0,
                    trace: input_result.trace.clone(),
                    cascade: None,
                }));
            }

            let output_columns = project_output_columns(columns);

            let mut output_rows = Vec::with_capacity(input_result.rows.len());
            for row in &input_result.rows {
                let mut projected = Vec::with_capacity(columns.len());
                for col in columns {
                    projected.push(eval_project_expr(
                        &col.expr,
                        row,
                        &input_result.columns,
                        params,
                    )?);
                }
                output_rows.push(projected);
            }

            Ok(OwnedQueryResult::new(QueryResult {
                columns: output_columns,
                rows: output_rows,
                rows_affected: 0,
                trace: input_result.trace.clone(),
                cascade: None,
            }))
        }
        PhysicalPlan::Sort { input, keys } => {
            let sort_columns =
                merge_expression_columns(materialized_columns, keys.iter().map(|key| &key.expr));
            // Sort elision path A: input is a Scan and an index's direction
            // prefix matches `keys`. We rewrite the input into an IndexScan
            // and skip the re-sort.
            let elided = try_elide_sort(
                db,
                input,
                keys,
                params,
                tx,
                sort_columns.as_deref(),
                vector_copy_owner,
                read_memory.clone(),
            )?;
            if let Some(mut result) = elided {
                result.trace.sort_elided = true;
                return Ok(result);
            }
            let mut input_result = execute_plan_with_materialized_columns(
                db,
                input,
                params,
                tx,
                sort_columns.as_deref(),
                Some(keys.as_slice()),
                vector_copy_owner,
                read_memory.clone(),
            )?;

            // Sort elision path B: the input already used an IndexScan whose
            // column list + directions prefix-match the ORDER BY keys. The
            // IndexScan already delivers rows in the requested order, so the
            // Sort is a no-op; skip it and mark `sort_elided`.
            if input_result.trace.physical_plan == "IndexScan"
                && let Some(idx_name) = &input_result.trace.index_used
                && sort_keys_match_index_prefix(db, input, idx_name, keys)
            {
                input_result.trace.sort_elided = true;
                return Ok(input_result);
            }
            // Evaluate every sort key for every row ONCE, before a single pair
            // is compared: the comparator cannot return an error, so an
            // evaluation failure would otherwise be swallowed as
            // `Ordering::Equal` and the caller would get rows in storage
            // order with no signal. Plan-time validation
            // (`validate_sort_key`) has already proven each key is a
            // supported, resolvable expression; this is the sort's own
            // evaluation of that identical expression, sharing the same
            // column-resolution rule (`resolve_query_result_column`) so the
            // two can never disagree.
            let sort_values = input_result
                .rows
                .iter()
                .map(|row| {
                    keys.iter()
                        .map(|key| {
                            eval_query_result_expr(&key.expr, row, &input_result.columns, params)
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            let mut order: Vec<usize> = (0..input_result.rows.len()).collect();
            order.sort_by(|&a, &b| {
                for (i, key) in keys.iter().enumerate() {
                    let ordering =
                        compare_sort_values(&sort_values[a][i], &sort_values[b][i], key.direction);
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                Ordering::Equal
            });
            let unsorted_rows = std::mem::take(&mut input_result.result.rows);
            input_result.rows = order
                .into_iter()
                .map(|i| unsorted_rows[i].clone())
                .collect();
            // Preserve data-source trace labels through the post-read sort.
            // A plain `Scan` child gets relabeled to `Sort` to match the
            // plan's ORDER BY-without-index expectations.
            if !trace_label_survives_sort(input_result.trace.physical_plan) {
                input_result.trace.physical_plan = "Sort";
            }
            input_result.trace.sort_elided = false;
            Ok(input_result)
        }
        PhysicalPlan::Limit { input, count } => {
            let mut input_result = execute_plan_with_materialized_columns(
                db,
                input,
                params,
                tx,
                materialized_columns,
                pending_sort_keys,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            input_result.rows.truncate(*count as usize);
            Ok(input_result)
        }
        PhysicalPlan::Filter { input, predicate } => {
            let filter_columns =
                merge_expression_columns(materialized_columns, std::iter::once(predicate));
            let mut input_result = execute_plan_with_materialized_columns(
                db,
                input,
                params,
                tx,
                filter_columns.as_deref(),
                pending_sort_keys,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            // A genuine evaluation error (e.g. dividing by zero, negating a
            // non-numeric value) must fail the statement, never be treated
            // as "row excluded" — the same contract `filter_rows_by_predicate`
            // already enforces for a plain scan's pushed-down predicate, and
            // the join's own `?`-propagating match loop below already holds
            // for `JOIN ON`. This is the CTE/derived-table WHERE path (a
            // `Filter` node over an arbitrary input), so it goes through a
            // fallible loop rather than `Vec::retain`, which cannot propagate
            // an `Err`.
            let unfiltered_rows = std::mem::take(&mut input_result.result.rows);
            let mut kept = Vec::with_capacity(unfiltered_rows.len());
            for row in unfiltered_rows {
                if query_result_row_matches(&row, &input_result.columns, predicate, params)? {
                    kept.push(row);
                }
            }
            input_result.rows = kept;
            Ok(input_result)
        }
        PhysicalPlan::Distinct { input } => {
            let mut input_result = execute_plan_with_materialized_columns(
                db,
                input,
                params,
                tx,
                materialized_columns,
                pending_sort_keys,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let mut seen = HashSet::<Vec<u8>>::new();
            let rows = std::mem::take(&mut input_result.result.rows)
                .into_iter()
                .filter(|row| seen.insert(distinct_row_key(row)))
                .collect();
            input_result.rows = rows;
            input_result.cascade = None;
            Ok(input_result)
        }
        PhysicalPlan::Join {
            left,
            right,
            condition,
            join_type,
            left_alias,
            right_alias,
        } => {
            let left_result =
                execute_plan(db, left, params, tx, vector_copy_owner, read_memory.clone())?;
            let right_result = execute_plan(
                db,
                right,
                params,
                tx,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let right_duplicate_names =
                duplicate_column_names(&left_result.columns, &right_result.columns);
            let right_prefix = right_alias
                .clone()
                .unwrap_or_else(|| right_table_name(right));
            let right_columns = right_result
                .columns
                .iter()
                .map(|column| {
                    if right_duplicate_names.contains(column) {
                        format!("{right_prefix}.{column}")
                    } else {
                        column.clone()
                    }
                })
                .collect::<Vec<_>>();

            let mut columns = left_result.columns.clone();
            columns.extend(right_columns);

            let mut rows = Vec::new();
            for left_row in &left_result.rows {
                let mut matched = false;
                for right_row in &right_result.rows {
                    let combined = concatenate_rows(left_row, right_row);
                    if query_result_row_matches(&combined, &columns, condition, params)? {
                        matched = true;
                        rows.push(combined);
                    }
                }

                if !matched && matches!(join_type, contextdb_planner::JoinType::Left) {
                    let mut combined = left_row.clone();
                    combined.extend(std::iter::repeat_n(Value::Null, right_result.columns.len()));
                    rows.push(combined);
                }
            }

            let output_columns = qualify_join_columns(
                &columns,
                &left_result.columns,
                &right_result.columns,
                left_alias,
                &right_prefix,
            );

            Ok(OwnedQueryResult::new(QueryResult {
                columns: output_columns,
                rows,
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            }))
        }
        PhysicalPlan::CreateIndex(p) => {
            require_admin_for_ddl(db)?;
            exec_create_index(db, p, read_memory.clone())
        }
        PhysicalPlan::DropIndex(p) => {
            require_admin_for_ddl(db)?;
            exec_drop_index(db, p, read_memory.clone())
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            range: _,
        } => {
            // Stub: always return empty rows + an IndexScan trace marker. Impl
            // must walk the BTreeMap at the named index, apply visibility,
            // materialize rows, and populate the trace fully.
            let _ = (table, index);
            Ok(OwnedQueryResult::new(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                trace: crate::database::QueryTrace {
                    physical_plan: "IndexScan",
                    index_used: None,
                    ..crate::database::QueryTrace::default()
                },
                cascade: None,
            }))
        }
        PhysicalPlan::SetMemoryLimit(val) => {
            let limit = match val {
                SetMemoryLimitValue::Bytes(bytes) => Some(*bytes),
                SetMemoryLimitValue::None => None,
            };
            db.set_memory_limit(limit)?;
            Ok(OwnedQueryResult::empty())
        }
        PhysicalPlan::ShowMemoryLimit => {
            let usage = db.accountant().usage();
            Ok(OwnedQueryResult::new(QueryResult {
                columns: vec![
                    "limit".to_string(),
                    "used".to_string(),
                    "available".to_string(),
                    "startup_ceiling".to_string(),
                ],
                rows: vec![vec![
                    usage
                        .limit
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                    Value::Int64(usage.used as i64),
                    usage
                        .available
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                    usage
                        .startup_ceiling
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                ]],
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            }))
        }
        PhysicalPlan::SetDiskLimit(val) => {
            let limit = match val {
                SetDiskLimitValue::Bytes(bytes) => Some(*bytes),
                SetDiskLimitValue::None => None,
            };
            db.set_disk_limit(limit)?;
            Ok(OwnedQueryResult::empty())
        }
        PhysicalPlan::ShowContentSize => {
            content_size_result(db.content_size_totals(read_memory.clone(), &mut || Ok(()))?)
        }
        PhysicalPlan::ShowDiskLimit => {
            let limit = db.disk_limit();
            let used = db.disk_file_size();
            let startup_ceiling = db.disk_limit_startup_ceiling();
            Ok(OwnedQueryResult::new(QueryResult {
                columns: vec![
                    "limit".to_string(),
                    "used".to_string(),
                    "available".to_string(),
                    "startup_ceiling".to_string(),
                ],
                rows: vec![vec![
                    limit
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                    used.map(|value| Value::Int64(value as i64))
                        .unwrap_or(Value::Null),
                    match (limit, used) {
                        (Some(limit), Some(used)) => {
                            Value::Int64(limit.saturating_sub(used) as i64)
                        }
                        _ => Value::Null,
                    },
                    startup_ceiling
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                ]],
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            }))
        }
        PhysicalPlan::SetMaintenancePollInterval(milliseconds) => {
            db.set_maintenance_poll_interval(std::time::Duration::from_millis(*milliseconds))?;
            Ok(OwnedQueryResult::empty())
        }
        PhysicalPlan::SetVectorServingCache(bytes) => {
            db.set_vector_serving_cache_bytes(bytes.map(|bytes| bytes as usize))?;
            Ok(OwnedQueryResult::empty())
        }
        PhysicalPlan::ShowVectorServingCache => Ok(OwnedQueryResult::new(QueryResult {
            columns: vec!["bytes".to_string()],
            rows: vec![vec![match db.vector_serving_cache_bytes() {
                Some(bytes) => Value::Int64(i64::try_from(bytes).unwrap_or(i64::MAX)),
                None => Value::Null,
            }]],
            rows_affected: 0,
            trace: crate::database::QueryTrace::scan(),
            cascade: None,
        })),
        PhysicalPlan::ShowMaintenancePollInterval => Ok(OwnedQueryResult::new(QueryResult {
            columns: vec!["milliseconds".to_string()],
            rows: vec![vec![Value::Int64(
                i64::try_from(db.maintenance_poll_interval().as_millis()).unwrap_or(i64::MAX),
            )]],
            rows_affected: 0,
            trace: crate::database::QueryTrace::scan(),
            cascade: None,
        })),
        PhysicalPlan::ShowSyncConflictPolicy => {
            // The per-table policy is DECLARED on each table's meta (via
            // `CREATE ... SYNC CONFLICT ...`), which is what the sync apply
            // resolves against. Read it from there so SHOW reports what the sync
            // path actually uses; the runtime layer only carries the deployment
            // default.
            let mut rows = vec![vec![Value::Text("keep_first".to_string())]];
            // One combined list, not two: every ENGINE_OWNED_LEDGER_TABLES
            // member that actually declares a real conflict policy
            // (`peer_directory`'s own `SYNC CONFLICT KEEP LATEST`) joins the
            // seven work-ledger tables' engine-owned rows here, so both the
            // skip-set below and the render loop treat them identically.
            // `work_node_contacts` declares NO conflict policy at all
            // (`engine_owned_ledger_policy("work_node_contacts").conflict
            // == None`) and so has no entry to fold in -- it never renders,
            // in any form, exactly like a plain table that never declared
            // SYNC CONFLICT.
            let engine_owned_display_rows: Vec<(&'static str, &'static str)> =
                crate::work_ledger::engine_owned_work_ledger_conflict_policy_display()
                    .into_iter()
                    .chain(crate::peer_directory::peer_directory_conflict_policy_display())
                    .collect();
            let engine_owned_display_tables: std::collections::HashSet<&str> =
                engine_owned_display_rows
                    .iter()
                    .map(|(table, _)| *table)
                    .collect();
            let mut tables = db.table_names();
            tables.sort();
            let existing_tables: std::collections::HashSet<&str> =
                tables.iter().map(|t| t.as_str()).collect();
            for table in &tables {
                // The engine-owned tables render below, marked
                // "(engine-owned)" with their TRUE arbitration -- skip the
                // generic declared-`TableMeta` row here so a table whose own
                // `CREATE TABLE` text also carries a `SYNC CONFLICT` clause
                // (`work_jobs`, `work_failures`, `work_inputs`, `peer_directory`)
                // never leaves an unmarked duplicate that reads as
                // operator-settable.
                if engine_owned_display_tables.contains(table.as_str()) {
                    continue;
                }
                if let Some(policy) = db.table_meta(table).and_then(|meta| meta.conflict_policy) {
                    rows.push(vec![Value::Text(format!(
                        "{}={}",
                        table,
                        declared_conflict_policy_to_string(policy)
                    ))]);
                }
            }
            for (table, word) in engine_owned_display_rows {
                // `install_work_ledger_schema` / `install_peer_directory_schema`
                // are opt-in -- a plain open has none of these tables.
                // Report what actually governs THIS store: render a row
                // only for a table that really exists here, never a ghost
                // row for one that would exist if it were installed.
                if existing_tables.contains(table) {
                    rows.push(vec![Value::Text(format!("{table}={word} (engine-owned)"))]);
                }
            }
            Ok(OwnedQueryResult::new(QueryResult {
                columns: vec!["policy".to_string()],
                rows,
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            }))
        }
        PhysicalPlan::ShowVectorIndexes => {
            let maintenance_policy = db.maintenance_policy();
            let rows = db
                .vector_inspection_infos(None, vector_copy_owner, read_memory.clone())?
                .into_iter()
                .map(|(declaration, info, partitions)| {
                    vector_index_inspection_row(&declaration, info, &partitions, maintenance_policy)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(OwnedQueryResult::new(QueryResult {
                columns: vector_inspection_columns(VECTOR_INDEX_INSPECTION_COLUMNS),
                rows,
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            }))
        }
        PhysicalPlan::ShowVectorPartitions {
            table,
            column,
            limit,
            offset,
        } => {
            let target = match (table.as_deref(), column.as_deref()) {
                (Some(table), Some(column)) => Some(VectorIndexRef::new(table, column)),
                (Some(_), None) | (None, None) => None,
                _ => {
                    return Err(Error::PlanError(
                        "vector partition inspection target requires both table and column"
                            .to_owned(),
                    ));
                }
            };
            let maintenance_policy = db.maintenance_policy();
            let mut rows = Vec::new();
            for (declaration, info, partitions) in
                db.vector_inspection_infos(target.as_ref(), vector_copy_owner, read_memory.clone())?
            {
                if table
                    .as_deref()
                    .is_some_and(|table| info.index.table != table)
                {
                    continue;
                }
                let partition_key_columns = declaration
                    .partition_key_columns
                    .as_deref()
                    .unwrap_or_default();
                for partition in partitions {
                    rows.push(vector_partition_inspection_row(
                        partition_key_columns,
                        &info.layout,
                        partition,
                        maintenance_policy,
                    )?);
                }
            }
            let offset = match offset {
                Some(value) => usize::try_from(*value).unwrap_or(usize::MAX),
                None => 0,
            };
            let limit = match limit {
                Some(value) => usize::try_from(*value).unwrap_or(usize::MAX),
                None => usize::MAX,
            };
            let rows = rows.into_iter().skip(offset).take(limit).collect();
            Ok(OwnedQueryResult::new(QueryResult {
                columns: vector_inspection_columns(VECTOR_PARTITION_INSPECTION_COLUMNS),
                rows,
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            }))
        }
        PhysicalPlan::Pipeline(plans) => {
            let mut last = OwnedQueryResult::empty();
            for p in plans {
                last = execute_plan(db, p, params, tx, vector_copy_owner, read_memory.clone())?;
            }
            Ok(last)
        }
        _ => Err(Error::PlanError(
            "unsupported plan node in executor".to_string(),
        )),
    }
}

#[inline(never)]

fn execute_materialized_cte(
    db: &Database,
    input: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    execute_plan(
        db,
        input,
        params,
        tx,
        vector_copy_owner,
        read_memory.clone(),
    )
}

/// The one typed binding and its private value storage in a SELECT scope.
/// Named columns never share this position namespace.
#[derive(Clone, Copy)]
struct WindowRankValues {
    binding: (WindowBindingId, usize),
    values: [i64; 1],
}

impl WindowRankValues {
    fn new(binding: WindowBindingId, rank: i64) -> Self {
        Self {
            binding: (binding, 0),
            values: [rank],
        }
    }

    fn get(self, binding: WindowBindingId) -> Result<Value> {
        if binding == self.binding.0 {
            if let Some(rank) = self.values.get(self.binding.1) {
                return Ok(Value::Int64(*rank));
            }
        }
        Err(Error::PlanError(
            "window rank binding is outside its scope".to_string(),
        ))
    }
}

fn eval_project_expr(
    expr: &Expr,
    row: &[Value],
    input_columns: &[String],
    params: &HashMap<String, Value>,
) -> Result<Value> {
    eval_project_expr_with_window(expr, row, input_columns, params, None)
}

fn eval_query_result_expr(
    expr: &Expr,
    row: &[Value],
    input_columns: &[String],
    params: &HashMap<String, Value>,
) -> Result<Value> {
    eval_query_result_expr_with_window(expr, row, input_columns, params, None)
}

fn eval_project_expr_with_window(
    expr: &Expr,
    row: &[Value],
    input_columns: &[String],
    params: &HashMap<String, Value>,
    window: Option<WindowRankValues>,
) -> Result<Value> {
    match expr {
        Expr::WindowRank(binding) => match window {
            Some(values) => values.get(*binding),
            _ => Err(Error::PlanError(
                "window rank binding is outside its scope".to_string(),
            )),
        },
        Expr::Column(c) => lookup_query_result_column(row, input_columns, c),
        Expr::Literal(lit) => resolve_expr(&Expr::Literal(lit.clone()), params),
        Expr::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {}", name))),
        Expr::BinaryOp { left, op, right } => {
            let left =
                eval_query_result_expr_with_window(left, row, input_columns, params, window)?;
            let right =
                eval_query_result_expr_with_window(right, row, input_columns, params, window)?;
            eval_binary_op(op, &left, &right)
        }
        Expr::UnaryOp { op, operand } => {
            let value =
                eval_query_result_expr_with_window(operand, row, input_columns, params, window)?;
            match op {
                UnaryOp::Not => Ok(if value == Value::Null {
                    Value::Null
                } else {
                    Value::Bool(!value_to_bool(&value))
                }),
                UnaryOp::Neg => match value {
                    Value::Int64(v) => checked_neg_i64(v),
                    Value::Float64(v) => Ok(Value::Float64(-v)),
                    _ => Err(Error::PlanError(
                        "cannot negate non-numeric value".to_string(),
                    )),
                },
            }
        }
        Expr::FunctionCall { name, args } => {
            let values = args
                .iter()
                .map(|arg| {
                    eval_query_result_expr_with_window(arg, row, input_columns, params, window)
                })
                .collect::<Result<Vec<_>>>()?;
            eval_function(name, &values)
        }
        Expr::IsNull { expr, negated } => {
            let is_null =
                eval_query_result_expr_with_window(expr, row, input_columns, params, window)?
                    == Value::Null;
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } if matches!(expr.as_ref(), Expr::Row(_)) => Ok(
            match row_value_in_list(expr, list, &|inner| {
                eval_query_result_expr_with_window(inner, row, input_columns, params, window)
            })? {
                Some(matched) => Value::Bool(matched != *negated),
                None => Value::Null,
            },
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle =
                eval_query_result_expr_with_window(expr, row, input_columns, params, window)?;
            if needle == Value::Null {
                return Ok(Value::Null);
            }
            let mut saw_unknown = false;
            for item in list {
                let candidate =
                    eval_query_result_expr_with_window(item, row, input_columns, params, window)?;
                if candidate == Value::Null {
                    saw_unknown = true;
                } else if matches!(compare_values(&needle, &candidate), Some(Ordering::Equal))
                    || needle == candidate
                {
                    return Ok(Value::Bool(!*negated));
                }
            }
            Ok(if saw_unknown {
                Value::Null
            } else {
                Value::Bool(*negated)
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let matches = match (
                eval_query_result_expr_with_window(expr, row, input_columns, params, window)?,
                eval_query_result_expr_with_window(pattern, row, input_columns, params, window)?,
            ) {
                (Value::Null, _) | (_, Value::Null) => return Ok(Value::Null),
                (Value::Text(value), Value::Text(pattern)) => like_matches(&value, &pattern),
                _ => false,
            };
            Ok(Value::Bool(if *negated { !matches } else { matches }))
        }
        _ => resolve_expr(expr, params),
    }
}

fn eval_query_result_expr_with_window(
    expr: &Expr,
    row: &[Value],
    input_columns: &[String],
    params: &HashMap<String, Value>,
    window: Option<WindowRankValues>,
) -> Result<Value> {
    match expr {
        Expr::WindowRank(binding) => match window {
            Some(values) => values.get(*binding),
            _ => Err(Error::PlanError(
                "window rank binding is outside its scope".to_string(),
            )),
        },
        Expr::Column(c) => lookup_query_result_column(row, input_columns, c),
        Expr::Literal(lit) => resolve_expr(&Expr::Literal(lit.clone()), params),
        Expr::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {}", name))),
        Expr::FunctionCall { name, args } => {
            let values = args
                .iter()
                .map(|arg| {
                    eval_query_result_expr_with_window(arg, row, input_columns, params, window)
                })
                .collect::<Result<Vec<_>>>()?;
            eval_function(name, &values)
        }
        Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::IsNull { .. }
        | Expr::InList { .. }
        | Expr::Like { .. } => {
            eval_project_expr_with_window(expr, row, input_columns, params, window)
        }
        _ => resolve_expr(expr, params),
    }
}

fn require_admin_for_create_table(db: &Database) -> Result<()> {
    if db.has_context_or_principal_constraints() {
        return Err(Error::Other(
            "DDL requires an admin database handle".to_string(),
        ));
    }
    Ok(())
}

fn require_admin_for_ddl(db: &Database) -> Result<()> {
    if db.has_access_constraints_for_query() {
        return Err(Error::Other(
            "DDL requires an admin database handle".to_string(),
        ));
    }
    Ok(())
}

fn exec_insert(
    db: &Database,
    p: &InsertPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    let mut vector_admission = crate::vector_read_memory::admission(read_memory.clone());
    let vector_copy_owner = &mut vector_admission;
    db.check_disk_budget("INSERT")?;
    let txid = tx.ok_or_else(|| Error::Other("missing tx for insert".to_string()))?;

    let mut insert_meta = db
        .table_meta(&p.table)
        .ok_or_else(|| Error::TableNotFound(p.table.clone()))?;
    let _vector_schema = if !vector_columns_for_meta(&insert_meta).is_empty() {
        let refs = vector_refs_for_meta(&p.table, &insert_meta);
        let guard = db.vector_schema_read_many(refs);
        insert_meta = db
            .table_meta(&p.table)
            .ok_or_else(|| Error::TableNotFound(p.table.clone()))?;
        Some(guard)
    } else {
        None
    };
    // When no column list is provided (INSERT INTO t VALUES (...)),
    // infer column names from table metadata in declaration order.
    let columns: Vec<String> = if p.columns.is_empty() {
        insert_meta.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        p.columns.clone()
    };

    // Statement-scoped snapshot of the committed TxId watermark for TXID bound checks.
    let current_tx_max = Some(db.committed_watermark());
    let route_inserts_to_graph = has_edge_columns(&insert_meta);
    let vector_columns = vector_columns_for_meta(&insert_meta);
    let has_insert_completion = insert_meta.columns.iter().any(|column| {
        column.default.is_some()
            || (!column.nullable && matches!(column.column_type, ColumnType::TxId))
    });

    if !vector_columns.is_empty() {
        for row in &p.values {
            let mut values = HashMap::new();
            for (idx, expr) in row.iter().enumerate() {
                let col = columns
                    .get(idx)
                    .ok_or_else(|| Error::PlanError("column/value count mismatch".to_string()))?;
                let v = resolve_expr(expr, params)?;
                values.insert(
                    col.clone(),
                    coerce_insert_value_for_column_with_meta(
                        &p.table,
                        &insert_meta,
                        col,
                        v,
                        current_tx_max,
                        Some(txid),
                    )?,
                );
            }
            if has_insert_completion {
                apply_missing_column_defaults(db, &p.table, &mut values, Some(txid))?;
            }
            db.complete_insert_access_values(&p.table, &mut values)?;
            validate_vector_columns(db, &p.table, &values)?;
        }
    }

    let mut rows_affected = 0;
    for row in &p.values {
        let mut values = HashMap::new();
        for (idx, expr) in row.iter().enumerate() {
            let col = columns
                .get(idx)
                .ok_or_else(|| Error::PlanError("column/value count mismatch".to_string()))?;
            let v = resolve_expr(expr, params)?;
            values.insert(
                col.clone(),
                coerce_insert_value_for_column_with_meta(
                    &p.table,
                    &insert_meta,
                    col,
                    v,
                    current_tx_max,
                    Some(txid),
                )?,
            );
        }

        if has_insert_completion {
            apply_missing_column_defaults(db, &p.table, &mut values, Some(txid))?;
        }
        db.complete_insert_access_values(&p.table, &mut values)?;

        if !vector_columns.is_empty() {
            validate_vector_columns(db, &p.table, &values)?;
        }
        let checkpoint = db.write_set_checkpoint(txid)?;
        let mut row_admission = Some(db.admit_retained_row_owned(
            &p.table,
            &values,
            &insert_meta,
            "insert",
            "row_insert",
            "Reduce row size or raise MEMORY_LIMIT before inserting more data.",
        )?);
        let graph_edge = if route_inserts_to_graph {
            match (
                values.get("source_id"),
                values.get("target_id"),
                values.get("edge_type"),
            ) {
                (
                    Some(Value::Uuid(source)),
                    Some(Value::Uuid(target)),
                    Some(Value::Text(edge_type)),
                ) => Some((*source, *target, edge_type.clone())),
                _ => None,
            }
        } else {
            None
        };

        let _row_id = if let Some(on_conflict) = &p.on_conflict {
            if on_conflict.columns.is_empty() {
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(Error::Other(
                    "ON CONFLICT target must include at least one column".to_string(),
                ));
            }
            let conflict_values = match on_conflict
                .columns
                .iter()
                .map(|column| {
                    values.get(column).cloned().ok_or_else(|| {
                        Error::Other(format!("conflict column {column} not in values"))
                    })
                })
                .collect::<Result<Vec<_>>>()
            {
                Ok(values) => values,
                Err(err) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            };
            let existing = match db.conflict_lookup_in_tx(
                txid,
                &p.table,
                &on_conflict.columns,
                &conflict_values,
                db.snapshot_for_read(),
                read_memory.clone(),
            ) {
                Ok(existing) => existing,
                Err(err) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            };
            let upsert_values = if let Some(existing_row) = existing.as_ref() {
                match apply_on_conflict_updates(
                    db,
                    &p.table,
                    values.clone(),
                    existing_row,
                    on_conflict,
                    params,
                    Some(txid),
                ) {
                    Ok(v) => v,
                    Err(err) => {
                        let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                        return Err(err);
                    }
                }
            } else {
                values.clone()
            };
            match existing {
                None => {
                    let intent_insert_values = upsert_values.clone();
                    match db.insert_row_prepaid(
                        txid,
                        &p.table,
                        upsert_values,
                        vector_copy_owner,
                        read_memory.clone(),
                    ) {
                        Ok(row_id) => {
                            if let Err(err) = db.record_upsert_intent(
                                txid,
                                p.table.clone(),
                                row_id,
                                UpsertIntentDetails {
                                    insert_values: intent_insert_values,
                                    conflict_columns: on_conflict.columns.clone(),
                                    update_columns: on_conflict.update_columns.clone(),
                                    params: params.clone(),
                                },
                            ) {
                                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                                return Err(err);
                            }
                            row_id
                        }
                        Err(err) => {
                            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                            return Err(err);
                        }
                    }
                }
                Some(existing_row) => {
                    let changed = upsert_values
                        .iter()
                        .any(|(k, v)| existing_row.values.get(k) != Some(v));
                    if !changed {
                        RowId(0)
                    } else {
                        if let Err(err) = validate_update_state_transition(
                            db,
                            &p.table,
                            &existing_row,
                            &upsert_values,
                        ) {
                            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                            return Err(err);
                        }
                        if db.has_live_vector_with_read_memory(
                            existing_row.row_id,
                            db.snapshot_for_read(),
                            read_memory.clone(),
                        )? {
                            for index in vector_indexes_for_table(db, &p.table) {
                                if db.vector_store_has_live_entry_for_row(
                                    &index,
                                    existing_row.row_id,
                                    db.snapshot_for_read(),
                                    vector_copy_owner,
                                )? && let Err(err) = db.delete_vector_with_read_memory(
                                    txid,
                                    index,
                                    existing_row.row_id,
                                    vector_copy_owner,
                                    read_memory.clone(),
                                ) {
                                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                                    return Err(err);
                                }
                            }
                        }
                        let row_uuid = upsert_values.get("id").and_then(Value::as_uuid).copied();
                        let new_state = db
                            .table_meta(&p.table)
                            .and_then(|meta| meta.state_machine)
                            .and_then(|sm| upsert_values.get(&sm.column))
                            .and_then(Value::as_text)
                            .map(std::borrow::ToOwned::to_owned);
                        let row_id = match db.insert_row_replacing(
                            txid,
                            &p.table,
                            upsert_values,
                            existing_row.row_id,
                            true,
                            vector_copy_owner,
                            read_memory.clone(),
                        ) {
                            Ok(row_id) => row_id,
                            Err(err) => {
                                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                                return Err(err);
                            }
                        };
                        db.record_owned_row_admission(
                            txid,
                            &p.table,
                            row_id,
                            row_admission.take().expect("row payload admission"),
                        );
                        if let (Some(uuid), Some(state)) = (row_uuid, new_state.as_deref())
                            && let Err(err) = db.propagate_state_change_if_needed(
                                txid,
                                &p.table,
                                Some(uuid),
                                Some(state),
                                vector_copy_owner,
                                read_memory.clone(),
                            )
                        {
                            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                            return Err(err);
                        }
                        row_id
                    }
                }
            }
        } else {
            match db.insert_row_with_unique_noop(
                txid,
                &p.table,
                values,
                vector_copy_owner,
                read_memory.clone(),
            ) {
                Ok(InsertRowResult::Inserted(row_id)) => row_id,
                Ok(InsertRowResult::NoOp) => {
                    continue;
                }
                Err(err) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            }
        };
        // The admitted figure rides with the version it paid for, so the
        // release is that same number however the table's index set changes
        // while the transaction stays open.
        if _row_id != RowId(0)
            && let Some(admission) = row_admission.take()
        {
            db.record_owned_row_admission(txid, &p.table, _row_id, admission);
        }

        if let Some((source, target, edge_type)) = graph_edge {
            match db.insert_edge_with_read_memory(
                txid,
                source,
                target,
                edge_type,
                HashMap::new(),
                read_memory.clone(),
            ) {
                Ok(true) => {}
                Ok(false) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    continue;
                }
                Err(err) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            }
        }

        rows_affected += 1;
    }

    Ok(OwnedQueryResult::empty_with_affected(rows_affected))
}

// Mode and direction refusal precede all selection sources.
fn exec_discard(
    db: &Database,
    p: &contextdb_planner::DiscardPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    let txid = tx.ok_or_else(|| Error::Other("missing transaction for discard".into()))?;
    let mut names = BTreeSet::new();
    for selection in &p.selections {
        if !names.insert(&selection.table) {
            return Err(Error::SchemaInvalid {
                reason: format!("table {} is listed twice", selection.table),
            });
        }
        db.discard_policy(&selection.table, read_memory.clone())?;
        let meta = db
            .table_meta(&selection.table)
            .ok_or_else(|| Error::TableNotFound(selection.table.clone()))?;
        if meta.natural_key_column.is_none()
            && meta.primary_key_columns.is_empty()
            && !meta.columns.iter().any(|c| c.primary_key)
        {
            return Err(Error::NotSyncEligible(selection.table.clone()));
        }
    }
    let snapshot = db.snapshot_for_read();
    let mut tables = Vec::new();
    for selection in &p.selections {
        let rows = db.scan_in_tx_raw(
            txid,
            &selection.table,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let rows =
            db.filter_rows_for_read(&selection.table, rows, snapshot, read_memory.clone())?;
        let predicate = selection
            .where_clause
            .as_ref()
            .map(|e| {
                resolve_in_subqueries(db, e, params, tx, vector_copy_owner, read_memory.clone())
            })
            .transpose()?;
        let matched = filter_rows_by_predicate(rows, predicate.as_ref(), params)?;
        let meta = db
            .table_meta(&selection.table)
            .ok_or_else(|| Error::TableNotFound(selection.table.clone()))?;
        let mut pinned = Vec::new();
        for row in matched {
            db.assert_row_write_allowed(
                &selection.table,
                row.row_id,
                &row.values,
                snapshot,
                read_memory.clone(),
            )?;
            pinned.push((
                natural_key_from_row_values(&meta, &row.values)
                    .ok_or_else(|| Error::NotSyncEligible(selection.table.clone()))?,
                row.row_id,
            ));
        }
        tables.push((selection.table.clone(), pinned));
    }
    db.stage_discard(txid, &tables, read_memory.clone())
        .map(OwnedQueryResult::new)
}

fn exec_purge(
    db: &Database,
    p: &PurgePlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    // The authority refusal must precede every read, including subquery
    // evaluation, so an edge never learns predicate-selected hub rows.
    if let Some(hub_node_id) = db.retention_sync_peer() {
        return Err(Error::PurgeRequiresAuthoritativeHub { hub_node_id });
    }
    if tx.is_some() {
        return Err(Error::PurgeRequiresStandaloneExecution);
    }

    // Every table is validated before any selection source runs.
    let mut names = BTreeSet::new();
    let mut node_local = Vec::new();
    for selection in &p.selections {
        if !names.insert(&selection.table) {
            return Err(Error::SchemaInvalid {
                reason: format!("table {} is listed twice", selection.table),
            });
        }
        let meta = db
            .table_meta(&selection.table)
            .ok_or_else(|| Error::TableNotFound(selection.table.clone()))?;
        if effective_sync_direction(&meta) == SyncDirection::None {
            node_local.push((
                selection.table.clone(),
                crate::database::purge_predicate::NodeLocalPredicate::capture(
                    selection.where_clause.as_ref(),
                    params,
                )?,
            ));
        }
        if meta.natural_key_column.is_none()
            && meta.primary_key_columns.is_empty()
            && !meta.columns.iter().any(|c| c.primary_key)
        {
            return Err(Error::SchemaInvalid {
                reason: format!("table {} has no declared erasure key", selection.table),
            });
        }
    }
    let snapshot = db.snapshot_for_read();
    let mut tables = Vec::new();
    for selection in &p.selections {
        let rows = db.scan_with_read_memory(
            &selection.table,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let resolved = selection
            .where_clause
            .as_ref()
            .map(|expr| {
                resolve_in_subqueries(
                    db,
                    expr,
                    params,
                    None,
                    vector_copy_owner,
                    read_memory.clone(),
                )
            })
            .transpose()?;
        let matched = filter_rows_by_predicate(rows, resolved.as_ref(), params)?;
        let meta = db
            .table_meta(&selection.table)
            .ok_or_else(|| Error::TableNotFound(selection.table.clone()))?;
        let mut pinned = Vec::new();
        for row in matched {
            db.assert_row_write_allowed(
                &selection.table,
                row.row_id,
                &row.values,
                snapshot,
                read_memory.clone(),
            )?;
            pinned.push((
                natural_key_from_row_values(&meta, &row.values)
                    .ok_or_else(|| Error::NotSyncEligible(selection.table.clone()))?,
                row.row_id,
            ));
        }
        tables.push((selection.table.clone(), pinned));
    }
    db.commit_authoritative_purge_tables(
        &tables,
        &node_local,
        vector_copy_owner,
        read_memory.clone(),
    )
}

fn exec_delete(
    db: &Database,
    p: &DeletePlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    let txid = tx.ok_or_else(|| Error::Other("missing tx for delete".to_string()))?;
    let _vector_schema = db.vector_schema_read_table(&p.table);
    let snapshot = db.snapshot_for_read();
    let resolved_where = p
        .where_clause
        .as_ref()
        .map(|expr| {
            resolve_in_subqueries(db, expr, params, tx, vector_copy_owner, read_memory.clone())
        })
        .transpose()?;
    let delete_predicates = resolved_where
        .as_ref()
        .map(|expr| collect_simple_equality_predicates(expr, params, false))
        .transpose()?
        .flatten();
    // DELETE uses the same admitted indexed candidate source as UPDATE.
    // Keep the residual predicate and access checks on the selected rows;
    // returning every unrelated row would charge a full-table output copy.
    let rows = if let Some(where_clause) = resolved_where.as_ref() {
        let indexed = db
            .table_meta(&p.table)
            .as_ref()
            .and_then(|meta| analyze_filter_for_index(&p.table, meta, where_clause, params).pick)
            .map(|pick| {
                execute_index_scan(
                    db,
                    &p.table,
                    &pick,
                    snapshot,
                    Some(txid),
                    IndexScanAccessMode::Predicate,
                    None,
                    resolved_where.as_ref(),
                    params,
                    vector_copy_owner,
                    read_memory.clone(),
                )
            })
            .transpose()?;
        if let Some((rows, examined)) = indexed {
            db.__bump_rows_examined(examined);
            rows
        } else {
            db.scan_in_tx_raw(
                txid,
                &p.table,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )?
        }
    } else {
        db.scan_in_tx_raw(
            txid,
            &p.table,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )?
    };
    let rows = db.filter_rows_for_read(&p.table, rows, snapshot, read_memory.clone())?;
    let matched = filter_rows_by_predicate(rows, resolved_where.as_ref(), params)?;

    for row in &matched {
        db.assert_row_write_allowed(
            &p.table,
            row.row_id,
            &row.values,
            snapshot,
            read_memory.clone(),
        )?;
    }

    for row in &matched {
        for index in vector_indexes_for_table(db, &p.table) {
            if db.vector_store_has_live_entry_for_row(
                &index,
                row.row_id,
                snapshot,
                vector_copy_owner,
            )? {
                db.delete_vector_with_read_memory(
                    txid,
                    index,
                    row.row_id,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
        }
        // The row and its durable owed-to-hub delete record commit together.
        // A later close/open may have no delete history left to reconstruct,
        // so staging this after the row is gone would lose the obligation.
        db.stage_local_delete_obligation(txid, &p.table, row, read_memory.clone())?;
        db.delete_row_with_read_memory(
            txid,
            &p.table,
            row.row_id,
            vector_copy_owner,
            read_memory.clone(),
        )?;
    }
    if !matched.is_empty()
        && let Some(predicates) = delete_predicates
    {
        db.record_relational_delete_predicate(txid, p.table.clone(), predicates)?;
    }

    Ok(OwnedQueryResult::empty_with_affected(matched.len() as u64))
}

fn collect_conditional_update_predicates(
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Option<Vec<(String, Value)>>> {
    collect_simple_equality_predicates(expr, params, true)
}

fn collect_simple_equality_predicates(
    expr: &Expr,
    params: &HashMap<String, Value>,
    require_non_id: bool,
) -> Result<Option<Vec<(String, Value)>>> {
    fn collect_into(
        expr: &Expr,
        params: &HashMap<String, Value>,
        out: &mut Vec<(String, Value)>,
    ) -> Result<bool> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => Ok(collect_into(left, params, out)? && collect_into(right, params, out)?),
            Expr::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } => {
                if let Expr::Column(column) = left.as_ref()
                    && matches!(right.as_ref(), Expr::Literal(_) | Expr::Parameter(_))
                {
                    out.push((column.column.clone(), resolve_expr(right, params)?));
                    return Ok(true);
                }
                if let Expr::Column(column) = right.as_ref()
                    && matches!(left.as_ref(), Expr::Literal(_) | Expr::Parameter(_))
                {
                    out.push((column.column.clone(), resolve_expr(left, params)?));
                    return Ok(true);
                }
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    let mut predicates = Vec::new();
    if collect_into(expr, params, &mut predicates)?
        && (!require_non_id || predicates.iter().any(|(column, _)| column != "id"))
    {
        Ok(Some(predicates))
    } else {
        Ok(None)
    }
}

fn unique_update_lookup_from_predicates(
    meta: &TableMeta,
    predicates: &[(String, Value)],
) -> Option<(Vec<String>, Vec<Value>)> {
    let predicate_value = |wanted: &str| {
        predicates
            .iter()
            .rev()
            .find_map(|(column, value)| (column == wanted).then_some(value))
    };

    for column in &meta.columns {
        if !(column.primary_key || column.unique) {
            continue;
        }
        if let Some(value) = predicate_value(&column.name) {
            return Some((vec![column.name.clone()], vec![value.clone()]));
        }
    }

    for columns in &meta.unique_constraints {
        if columns.is_empty() {
            continue;
        }
        let Some(values) = columns
            .iter()
            .map(|column| predicate_value(column).cloned())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        return Some((columns.clone(), values));
    }

    None
}

struct TriggerUniqueUpdateLookup<'a> {
    table: &'a str,
    table_meta: Option<&'a TableMeta>,
    snapshot: SnapshotId,
    resolved_where: Option<&'a Expr>,
    conditional_predicates: Option<&'a [(String, Value)]>,
}

fn trigger_bound_unique_update_rows(
    db: &Database,
    txid: TxId,
    lookup: TriggerUniqueUpdateLookup<'_>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<Vec<VersionedRow>>> {
    let (Some(meta), Some(_where_clause), Some(predicates)) = (
        lookup.table_meta,
        lookup.resolved_where,
        lookup.conditional_predicates,
    ) else {
        return Ok(None);
    };
    let Some((columns, values)) = unique_update_lookup_from_predicates(meta, predicates) else {
        return Ok(None);
    };
    let Some(row) = db.unique_row_lookup_in_tx(
        txid,
        lookup.table,
        &columns,
        &values,
        lookup.snapshot,
        read_memory.clone(),
    )?
    else {
        return Ok(Some(Vec::new()));
    };
    Ok(Some(vec![row]))
}

fn exec_update(
    db: &Database,
    p: &UpdatePlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    db.check_disk_budget("UPDATE")?;
    let txid = tx.ok_or_else(|| Error::Other("missing tx for update".to_string()))?;
    let table_meta = db.table_meta(&p.table);
    let vector_indexes = table_meta
        .as_ref()
        .map(|meta| vector_refs_for_meta(&p.table, meta))
        .unwrap_or_default();
    let _vector_schema =
        (!vector_indexes.is_empty()).then(|| db.vector_schema_read_many(vector_indexes.clone()));
    let vector_columns = table_meta
        .as_ref()
        .map(|meta| {
            vector_columns_for_meta(meta)
                .into_iter()
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let has_vector_columns = !vector_columns.is_empty();
    let snapshot = db.snapshot_for_read();
    let resolved_where = p
        .where_clause
        .as_ref()
        .map(|expr| {
            resolve_in_subqueries(db, expr, params, tx, vector_copy_owner, read_memory.clone())
        })
        .transpose()?;
    let current_tx_max = Some(db.committed_watermark());
    let conditional_predicates = resolved_where
        .as_ref()
        .map(|expr| collect_conditional_update_predicates(expr, params))
        .transpose()?
        .flatten()
        .map(|predicates| {
            predicates
                .into_iter()
                .map(|(column, value)| {
                    Ok((
                        column.clone(),
                        coerce_value_for_column(
                            db,
                            &p.table,
                            &column,
                            value,
                            current_tx_max,
                            Some(txid),
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let trigger_callback_bound = db.trigger_callback_tx_bound_matches(txid);
    let skip_trigger_access_checks =
        trigger_callback_bound && !db.has_access_constraints_for_query();
    let direct_rows = if trigger_callback_bound && !has_vector_columns {
        trigger_bound_unique_update_rows(
            db,
            txid,
            TriggerUniqueUpdateLookup {
                table: &p.table,
                table_meta: table_meta.as_ref(),
                snapshot,
                resolved_where: resolved_where.as_ref(),
                conditional_predicates: conditional_predicates.as_deref(),
            },
            read_memory.clone(),
        )?
    } else {
        None
    };
    let direct_unique_update = direct_rows.is_some();
    let direct_unique_lookup_exhausts_where = direct_unique_update
        && table_meta
            .as_ref()
            .zip(conditional_predicates.as_ref())
            .and_then(|(meta, predicates)| {
                unique_update_lookup_from_predicates(meta, predicates)
                    .map(|(columns, _)| columns.len() == predicates.len())
            })
            .unwrap_or(false);
    let state_machine_column = table_meta
        .as_ref()
        .and_then(|meta| meta.state_machine.as_ref())
        .map(|sm| sm.column.clone());
    let state_column_assigned = state_machine_column
        .as_ref()
        .is_some_and(|column| p.assignments.iter().any(|(assigned, _)| assigned == column));
    // Use the same IndexScan candidate selection as SELECT when the UPDATE
    // predicate can narrow by an indexed first column. The residual WHERE is
    // still evaluated below, so this only reduces the candidate set.
    let rows = if let Some(rows) = direct_rows {
        rows
    } else if let Some(where_clause) = resolved_where.as_ref() {
        let indexed_rows = table_meta
            .as_ref()
            .and_then(|meta| analyze_filter_for_index(&p.table, meta, where_clause, params).pick)
            .map(|pick| {
                execute_index_scan(
                    db,
                    &p.table,
                    &pick,
                    snapshot,
                    Some(txid),
                    IndexScanAccessMode::Predicate,
                    None,
                    resolved_where.as_ref(),
                    params,
                    vector_copy_owner,
                    read_memory.clone(),
                )
            })
            .transpose()?;
        if let Some((rows, examined)) = indexed_rows {
            db.__bump_rows_examined(examined);
            rows
        } else {
            // Use in-tx scan so prior statements in a BEGIN/COMMIT block are
            // visible: the old row must not shadow a previously-updated row.
            let rows = db.scan_in_tx_raw(
                txid,
                &p.table,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            db.filter_rows_for_read(&p.table, rows, snapshot, read_memory.clone())?
        }
    } else {
        let rows = db.scan_in_tx_raw(
            txid,
            &p.table,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        db.filter_rows_for_read(&p.table, rows, snapshot, read_memory.clone())?
    };
    let matched: Vec<_> = if direct_unique_lookup_exhausts_where {
        rows
    } else {
        filter_rows_by_predicate(rows, resolved_where.as_ref(), params)?
    };

    if direct_unique_update && trigger_callback_bound && !has_vector_columns {
        let mut affected = 0_u64;
        for row in matched {
            if !skip_trigger_access_checks {
                db.assert_row_write_allowed(
                    &p.table,
                    row.row_id,
                    &row.values,
                    snapshot,
                    read_memory.clone(),
                )?;
            }
            let mut values = row.values.clone();
            for (k, vexpr) in &p.assignments {
                let value = eval_assignment_expr(vexpr, &row.values, params)?;
                values.insert(
                    k.clone(),
                    coerce_value_for_column(db, &p.table, k, value, current_tx_max, Some(txid))?,
                );
            }
            if state_column_assigned {
                validate_update_state_transition(db, &p.table, &row, &values)?;
            }
            let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
            let new_state = table_meta
                .as_ref()
                .and_then(|meta| meta.state_machine.as_ref())
                .and_then(|sm| values.get(&sm.column))
                .and_then(Value::as_text)
                .map(std::borrow::ToOwned::to_owned);
            if !skip_trigger_access_checks {
                db.assert_row_write_allowed(
                    &p.table,
                    row.row_id,
                    &values,
                    snapshot,
                    read_memory.clone(),
                )?;
            }
            let row_admission = admit_table_row_bytes(
                db,
                &p.table,
                &values,
                "update",
                "row_replace",
                "Reduce row growth or raise MEMORY_LIMIT before updating this row.",
            )?;
            let propagation_possible = row_uuid.is_some()
                && new_state
                    .as_deref()
                    .is_some_and(|state| db.propagation_rules_can_react(&p.table, state));
            if !propagation_possible {
                let Some(meta) = table_meta.as_ref() else {
                    return Err(Error::TableNotFound(p.table.clone()));
                };
                let committed_row_exists = row.created_tx != txid
                    || db
                        .committed_row_at(&p.table, row.row_id, snapshot, read_memory.clone())?
                        .is_some();
                let (new_row_id, before_counts, after_counts) = match db
                    .replace_row_after_update_validation_counted(
                        txid,
                        &p.table,
                        row.row_id,
                        values,
                        UpdateReplacementContext {
                            meta,
                            committed_row_exists,
                            created_at: db.trigger_callback_wallclock(),
                        },
                    ) {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        return Err(err);
                    }
                };
                db.record_owned_row_admission(txid, &p.table, new_row_id, row_admission);
                if let Some(predicates) = conditional_predicates.clone() {
                    db.record_conditional_update_guard(
                        txid,
                        p.table.clone(),
                        row.row_id,
                        predicates,
                        before_counts,
                        after_counts,
                        false,
                    )?;
                }
                affected = affected.saturating_add(1);
                continue;
            }
            let checkpoint = match db.write_set_checkpoint(txid) {
                Ok(checkpoint) => checkpoint,
                Err(error) => {
                    return Err(error);
                }
            };
            let before_counts = db.write_set_counts(txid)?;
            let new_row_id = match db.replace_row_after_update_validation(
                txid,
                &p.table,
                row.row_id,
                values,
                snapshot,
                read_memory.clone(),
            ) {
                Ok(row_id) => row_id,
                Err(err) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            };
            db.record_owned_row_admission(txid, &p.table, new_row_id, row_admission);
            if let Err(err) = db.propagate_state_change_if_needed(
                txid,
                &p.table,
                row_uuid,
                new_state.as_deref(),
                vector_copy_owner,
                read_memory.clone(),
            ) {
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(err);
            }
            if let Some(predicates) = conditional_predicates.clone() {
                let after_counts = db.write_set_counts(txid)?;
                db.record_conditional_update_guard(
                    txid,
                    p.table.clone(),
                    row.row_id,
                    predicates,
                    before_counts,
                    after_counts,
                    false,
                )?;
            }
            affected = affected.saturating_add(1);
        }
        return Ok(OwnedQueryResult::empty_with_affected(affected));
    }

    struct PlannedUpdate {
        row: VersionedRow,
        values: HashMap<String, Value>,
        row_uuid: Option<uuid::Uuid>,
        new_state: Option<String>,
        assigned_vector_values: Vec<(String, Vec<f32>)>,
        /// The assigned vector columns in the order the statement assigns them,
        /// so the vector records this update queues -- and the change history
        /// they are committed in -- follow the author's statement, not a hash
        /// order that differs from one process to the next.
        assigned_vector_columns: Vec<String>,
        conditional_predicates: Option<Vec<(String, Value)>>,
    }

    let mut planned = Vec::with_capacity(matched.len());
    for row in &matched {
        if !skip_trigger_access_checks {
            db.assert_row_write_allowed(
                &p.table,
                row.row_id,
                &row.values,
                snapshot,
                read_memory.clone(),
            )?;
        }
        let mut values = row.values.clone();
        for (k, vexpr) in &p.assignments {
            let value = eval_assignment_expr(vexpr, &row.values, params)?;
            values.insert(
                k.clone(),
                coerce_value_for_column(db, &p.table, k, value, current_tx_max, Some(txid))?,
            );
        }
        if state_column_assigned {
            validate_update_state_transition(db, &p.table, row, &values)?;
        }
        let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
        let new_state = table_meta
            .as_ref()
            .and_then(|meta| meta.state_machine.as_ref())
            .and_then(|sm| values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);

        if has_vector_columns {
            validate_vector_columns(db, &p.table, &values)?;
        }
        if !skip_trigger_access_checks {
            db.assert_row_write_allowed(
                &p.table,
                row.row_id,
                &values,
                snapshot,
                read_memory.clone(),
            )?;
        }
        let assigned_vector_columns: Vec<String> = if has_vector_columns {
            let mut assigned = Vec::new();
            for (column, _) in &p.assignments {
                if vector_columns.contains(column) && !assigned.contains(column) {
                    assigned.push(column.clone());
                }
            }
            assigned
        } else {
            Vec::new()
        };
        let assigned_vector_values: Vec<(String, Vec<f32>)> = if has_vector_columns {
            p.assignments
                .iter()
                .filter_map(|(column, _)| match values.get(column) {
                    Some(Value::Vector(vector)) if assigned_vector_columns.contains(column) => {
                        Some((column.clone(), vector.clone()))
                    }
                    _ => None,
                })
                .collect()
        } else {
            Vec::new()
        };
        planned.push(PlannedUpdate {
            row: row.clone(),
            values,
            row_uuid,
            new_state,
            assigned_vector_values,
            assigned_vector_columns,
            conditional_predicates: conditional_predicates.clone(),
        });
    }

    for plan in planned {
        let row = plan.row;
        let values = plan.values;
        let row_uuid = plan.row_uuid;
        let new_state = plan.new_state;
        let assigned_vector_values = plan.assigned_vector_values;
        let assigned_vector_columns = plan.assigned_vector_columns;
        let conditional_predicates = plan.conditional_predicates;
        let checkpoint = db.write_set_checkpoint(txid)?;
        let row_admission = admit_table_row_bytes(
            db,
            &p.table,
            &values,
            "update",
            "row_replace",
            "Reduce row growth or raise MEMORY_LIMIT before updating this row.",
        )?;
        let before_counts = db.write_set_counts(txid)?;

        let vector_free_trigger_update = trigger_callback_bound
            && assigned_vector_columns.is_empty()
            && vector_indexes.is_empty();
        let new_row_id = if vector_free_trigger_update {
            match db.replace_row_after_update_validation(
                txid,
                &p.table,
                row.row_id,
                values,
                snapshot,
                read_memory.clone(),
            ) {
                Ok(row_id) => row_id,
                Err(err) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            }
        } else {
            for column in &assigned_vector_columns {
                if let Err(err) = db.delete_vector_with_read_memory(
                    txid,
                    contextdb_core::VectorIndexRef::new(&p.table, column.clone()),
                    row.row_id,
                    vector_copy_owner,
                    read_memory.clone(),
                ) {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            }
            let new_row_id = match db.replace_row_after_update_validation(
                txid,
                &p.table,
                row.row_id,
                values,
                snapshot,
                read_memory.clone(),
            ) {
                Ok(row_id) => row_id,
                Err(err) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            };
            for index in &vector_indexes {
                if assigned_vector_columns.contains(&index.column) {
                    continue;
                }
                if let Err(err) = db.move_vector(
                    txid,
                    index.clone(),
                    row.row_id,
                    new_row_id,
                    vector_copy_owner,
                    read_memory.clone(),
                ) {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            }
            new_row_id
        };
        db.record_owned_row_admission(txid, &p.table, new_row_id, row_admission);
        for (column, vector) in assigned_vector_values {
            let index = contextdb_core::VectorIndexRef::new(&p.table, column);
            if let Err(err) = db.insert_vector_strict(
                txid,
                index,
                new_row_id,
                vector,
                vector_copy_owner,
                read_memory.clone(),
            ) {
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(err);
            }
        }
        if let Err(err) = db.propagate_state_change_if_needed(
            txid,
            &p.table,
            row_uuid,
            new_state.as_deref(),
            vector_copy_owner,
            read_memory.clone(),
        ) {
            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
            return Err(err);
        }
        if let Some(predicates) = conditional_predicates {
            let after_counts = db.write_set_counts(txid)?;
            db.record_conditional_update_guard(
                txid,
                p.table.clone(),
                row.row_id,
                predicates,
                before_counts,
                after_counts,
                false,
            )?;
        }
    }

    Ok(OwnedQueryResult::empty_with_affected(matched.len() as u64))
}

/// The one pseudo-column a predicate may name without it being declared on the
/// table: `eval_expr_value` serves it from the row header, not the row values.
const PREDICATE_PSEUDO_COLUMN: &str = "row_id";

/// Reject a predicate that names a column the table does not declare.
///
/// Standard SQL treats an unknown column reference in a predicate as an error.
/// The row-level evaluator instead reads absent names as NULL, so a typo turns
/// into a wrong answer with no signal: zero rows from `WHERE nope = 'x'`, every
/// row from `WHERE nope IS NULL`, and — on DELETE — the whole table gone.
/// Checking the whole predicate before any row is touched also makes the answer
/// independent of which branches a short-circuit would have reached.
///
/// Precedence matches `CREATE INDEX` (TableNotFound before ColumnNotFound): an
/// absent table is left to the scan/DML path that reports it.
fn validate_predicate_columns(
    db: &Database,
    table: &str,
    alias: Option<&str>,
    predicate: &Expr,
) -> Result<()> {
    let Some(meta) = db.table_meta(table) else {
        return Ok(());
    };
    check_predicate_columns_declared(table, alias, &meta, predicate)
}

fn check_predicate_columns_declared(
    table: &str,
    alias: Option<&str>,
    meta: &TableMeta,
    expr: &Expr,
) -> Result<()> {
    walk_predicate_columns(expr, &mut |column_ref| {
        // A qualifier is only legal when it names THIS scan's own table or
        // its alias — a single-table scan has no other relation in scope, so
        // any other qualifier is unrecognized even when the bare column name
        // it qualifies is real on this table.
        if let Some(qualifier) = &column_ref.table
            && qualifier != table
            && Some(qualifier.as_str()) != alias
        {
            return Err(Error::ColumnNotFound {
                table: table.to_string(),
                column: column_ref.column.clone(),
            });
        }
        if column_ref.column == PREDICATE_PSEUDO_COLUMN
            || meta
                .columns
                .iter()
                .any(|column| column.name == column_ref.column)
        {
            Ok(())
        } else {
            Err(Error::ColumnNotFound {
                table: table.to_string(),
                column: column_ref.column.clone(),
            })
        }
    })
}

/// Visit every column reference a predicate makes, in source order, so the
/// first unknown name is the one reported.
fn walk_predicate_columns(
    expr: &Expr,
    check: &mut dyn FnMut(&ColumnRef) -> Result<()>,
) -> Result<()> {
    match expr {
        Expr::RowNumber(window) => {
            for column in &window.partition_by {
                check(column)?;
            }
            for order in &window.order_by {
                check(&order.column)?;
            }
            Ok(())
        }
        Expr::WindowRank(_) => Ok(()),
        Expr::Column(column_ref) => check(column_ref),
        Expr::Row(elements) => {
            for element in elements {
                walk_predicate_columns(element, check)?;
            }
            Ok(())
        }
        Expr::Literal(_) | Expr::Parameter(_) => Ok(()),
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            walk_predicate_columns(left, check)?;
            walk_predicate_columns(right, check)
        }
        Expr::UnaryOp { operand, .. } => walk_predicate_columns(operand, check),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                walk_predicate_columns(arg, check)?;
            }
            Ok(())
        }
        Expr::InList { expr, list, .. } => {
            walk_predicate_columns(expr, check)?;
            for item in list {
                walk_predicate_columns(item, check)?;
            }
            Ok(())
        }
        Expr::Like { expr, pattern, .. } => {
            walk_predicate_columns(expr, check)?;
            walk_predicate_columns(pattern, check)
        }
        // Only the outer operand belongs to this relation; the subquery body
        // names its own columns and is validated when it is planned and run.
        Expr::InSubquery { expr, .. } => walk_predicate_columns(expr, check),
        Expr::IsNull { expr, .. } => walk_predicate_columns(expr, check),
        // `ROW_VECTOR(table, column, key)` names another table's column and is
        // legal only on the right of `<=>` in ORDER BY, never in a predicate.
        Expr::RowVectorSource { .. } => Ok(()),
    }
}

fn expression_columns<'a>(expressions: impl IntoIterator<Item = &'a Expr>) -> Vec<String> {
    let mut columns = BTreeSet::new();
    for expression in expressions {
        let _ = walk_predicate_columns(expression, &mut |column| {
            columns.insert(column.column.clone());
            Ok(())
        });
    }
    columns.into_iter().collect()
}

fn merge_expression_columns<'a>(
    selected: Option<&[String]>,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> Option<Vec<String>> {
    let selected = selected?;
    let mut columns = selected.iter().cloned().collect::<BTreeSet<_>>();
    columns.extend(expression_columns(expressions));
    Some(columns.into_iter().collect())
}

/// The table an error should name for a plan that is not a single bare scan.
/// Over a join this is the left (anchor) relation — a join has no one owner,
/// and naming the anchor beats inventing a table name or reporting none.
/// Validate every column a plan names against the catalog, running none of it.
///
/// This is the plan-time seam: it runs after physical planning and before
/// execution, and `Database::explain` calls it too, so a statically-invalid
/// query is refused there instead of rendering a plan for something that
/// cannot run. Because it derives each node's columns from the catalog rather
/// than from materialized rows, a predicate over a join is refused before the
/// join's left side is scanned at all — where the previous execution-time
/// check could only run after the whole input had already been built.
///
/// It reuses the same predicate walk and the same resolution rules the
/// executor uses, so relocating *when* validation happens changed no verdict:
/// the deliberate bare-name/qualifier semantics are untouched.
pub(crate) fn validate_plan_columns(db: &Database, plan: &PhysicalPlan) -> Result<()> {
    match plan {
        PhysicalPlan::WindowRank { input, window, .. } => {
            validate_plan_columns(db, input)?;
            if let Some(columns) = plan_output_columns(db, input) {
                for column in window
                    .partition_by
                    .iter()
                    .chain(window.order_by.iter().map(|key| &key.column))
                {
                    validate_filter_predicate_columns(
                        &columns,
                        input,
                        &Expr::Column(column.clone()),
                    )?;
                }
            }
        }
        PhysicalPlan::Scan {
            table,
            alias,
            filter: Some(predicate),
        } => validate_predicate_columns(db, table, alias.as_deref(), predicate)?,
        PhysicalPlan::Purge(p) => {
            for selection in &p.selections {
                if let Some(predicate) = &selection.where_clause {
                    validate_predicate_columns(db, &selection.table, None, predicate)?;
                }
            }
        }
        PhysicalPlan::Discard(p) => {
            for selection in &p.selections {
                if let Some(predicate) = &selection.where_clause {
                    validate_predicate_columns(db, &selection.table, None, predicate)?;
                }
            }
        }
        PhysicalPlan::Delete(p) => {
            if let Some(predicate) = &p.where_clause {
                validate_predicate_columns(db, &p.table, None, predicate)?;
            }
        }
        PhysicalPlan::Update(p) => {
            if let Some(predicate) = &p.where_clause {
                validate_predicate_columns(db, &p.table, None, predicate)?;
            }
        }
        PhysicalPlan::Filter { input, predicate } => {
            validate_plan_columns(db, input)?;
            if let Some(columns) = plan_output_columns(db, input) {
                validate_filter_predicate_columns(&columns, input, predicate)?;
            }
        }
        PhysicalPlan::Sort { input, keys } => {
            validate_plan_columns(db, input)?;
            if let Some(columns) = plan_output_columns(db, input) {
                for key in keys {
                    validate_sort_key(&columns, input, key)?;
                }
            }
        }
        PhysicalPlan::Project { input, columns } => {
            validate_plan_columns(db, input)?;
            // GraphBfs's `COLUMNS (...)` aliasing is not tracked by
            // `plan_output_columns` at all (it always reports the fixed
            // `{alias}.id` / `id` / `depth` shape, never the query's own
            // aliases), so a Project sitting over a GraphBfs subtree is left
            // unchecked here rather than rejecting valid GRAPH_TABLE column
            // aliases as unknown.
            if !plan_contains_graph_bfs(input)
                && let Some(input_columns) = plan_output_columns(db, input)
            {
                for column in columns {
                    validate_filter_predicate_columns(&input_columns, input, &column.expr)?;
                }
            }
        }
        PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. } => validate_plan_columns(db, input)?,
        PhysicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            validate_plan_columns(db, left)?;
            validate_plan_columns(db, right)?;
            // The ON clause is evaluated per row PAIR, so an unknown column in
            // it only ever surfaced when both sides happened to produce rows —
            // an empty side skipped the check entirely and the query silently
            // succeeded. Validate it here against the list the executor will
            // actually evaluate it against.
            if let Some(columns) = join_column_lists(db, plan) {
                validate_filter_predicate_columns(&columns.condition, plan, condition)?;
            }
        }
        PhysicalPlan::VectorSearch { candidates, .. }
        | PhysicalPlan::HnswSearch { candidates, .. } => {
            if let Some(candidates) = candidates {
                validate_plan_columns(db, candidates)?;
            }
        }
        // The traversal's own filter names graph aliases rather than table
        // columns, so it stays with the runtime evaluator; the candidate
        // sub-plan is an ordinary relational plan and is validated.
        PhysicalPlan::GraphBfs {
            start_candidates: Some(candidates),
            ..
        } => validate_plan_columns(db, candidates)?,
        PhysicalPlan::Union { inputs, .. } => {
            for input in inputs {
                validate_plan_columns(db, input)?;
            }
        }
        PhysicalPlan::Pipeline(plans) => {
            for plan in plans {
                validate_plan_columns(db, plan)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Whether an ORDER BY key expression is a supported, evaluated form: a
/// plain column, or a function call this engine already evaluates for a
/// SELECT list (`COALESCE`, and the desugared `+`/`-`/`*`/`/` arithmetic
/// forms). Any other shape stays a typed refusal rather than a silent
/// no-op sort.
fn sort_key_expr_supported(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::WindowRank(_))
        || matches!(expr, Expr::FunctionCall { name, .. } if is_known_scalar_function(name))
}

fn is_known_scalar_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "__add" | "__sub" | "__mul" | "__div" | "coalesce" | "now"
    )
}

/// Validate one ORDER BY key at plan time: the expression shape must be one
/// the runtime sort actually evaluates, and every column it references must
/// resolve — reporting the unknown/ambiguous cases the way every other
/// column error in the engine reads. The runtime sort evaluates the
/// identical expression per row (see the `PhysicalPlan::Sort` executor arm),
/// sharing this same column-resolution rule so the two can never disagree.
fn validate_sort_key(
    columns: &[String],
    input: &PhysicalPlan,
    key: &contextdb_planner::SortKey,
) -> Result<()> {
    if !sort_key_expr_supported(&key.expr) {
        return Err(Error::OrderByExpressionNotSupported);
    }
    validate_filter_predicate_columns(columns, input, &key.expr)
}

/// The columns a plan node yields, derived from the catalog without running it.
///
/// `None` means "cannot be derived statically" — the caller then skips the
/// check rather than guessing. Where a node's runtime column list varies by
/// branch this deliberately returns a SUPERSET, so the validator can only ever
/// reject a name that no branch could have produced. Each arm mirrors the
/// column list the matching executor arm builds; the join arm reuses the very
/// same helpers, so the two cannot drift.
fn plan_output_columns(db: &Database, plan: &PhysicalPlan) -> Option<Vec<String>> {
    match plan {
        // `dual` names a zero-column constant source only while no table
        // carries that name; once an operator creates one, its own columns are
        // what a query over it may name.
        PhysicalPlan::Scan { table, .. } if table == "dual" && db.table_meta(table).is_none() => {
            Some(Vec::new())
        }
        PhysicalPlan::Scan { table, .. } | PhysicalPlan::IndexScan { table, .. } => {
            scan_output_columns(db, table)
        }
        PhysicalPlan::VectorSearch { table, .. } | PhysicalPlan::HnswSearch { table, .. } => {
            let mut columns = scan_output_columns(db, table)?;
            columns.push("score".to_string());
            Some(columns)
        }
        PhysicalPlan::GraphBfs {
            start_alias, steps, ..
        } => {
            let mut columns = vec![format!("{start_alias}.id")];
            for step in steps {
                columns.push(format!("{}.id", step.target_alias));
            }
            columns.push("id".to_string());
            columns.push("depth".to_string());
            Some(columns)
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::WindowRank { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. } => plan_output_columns(db, input),
        PhysicalPlan::Project { columns, .. } => Some(project_output_columns(columns)),
        PhysicalPlan::Join { .. } => Some(join_column_lists(db, plan)?.output),
        _ => None,
    }
}

/// The two DIFFERENT column lists a join builds, kept together because using
/// the wrong one is the easy mistake here.
///
/// `condition` is what the `ON` clause is evaluated against: the left columns
/// followed by the right columns, with only the names that COLLIDE across the
/// two sides prefixed. `output` is what the join hands upward to a `Filter` or
/// `Sort`: fully qualified. They are not interchangeable — validating an `ON`
/// clause against `output` would reject valid SQL, because a name the condition
/// legitimately uses unqualified only exists in qualified form there.
struct JoinColumnLists {
    condition: Vec<String>,
    output: Vec<String>,
}

fn join_column_lists(db: &Database, plan: &PhysicalPlan) -> Option<JoinColumnLists> {
    let PhysicalPlan::Join {
        left,
        right,
        left_alias,
        right_alias,
        ..
    } = plan
    else {
        return None;
    };
    let left_columns = plan_output_columns(db, left)?;
    let right_columns = plan_output_columns(db, right)?;
    let right_duplicate_names = duplicate_column_names(&left_columns, &right_columns);
    let right_prefix = right_alias
        .clone()
        .unwrap_or_else(|| right_table_name(right));
    let mut condition = left_columns.clone();
    condition.extend(right_columns.iter().map(|column| {
        if right_duplicate_names.contains(column) {
            format!("{right_prefix}.{column}")
        } else {
            column.clone()
        }
    }));
    let output = qualify_join_columns(
        &condition,
        &left_columns,
        &right_columns,
        left_alias,
        &right_prefix,
    );
    Some(JoinColumnLists { condition, output })
}

/// A scan's columns: the synthetic `row_id` the executor prepends, then the
/// table's declared columns in declaration order.
fn scan_output_columns(db: &Database, table: &str) -> Option<Vec<String>> {
    let meta = db.table_meta(table)?;
    let mut columns = vec!["row_id".to_string()];
    columns.extend(meta.columns.into_iter().map(|column| column.name));
    Some(columns)
}

/// An aggregate a projected column can name.
///
/// Recognising an aggregate is decided HERE and nowhere else. Every read
/// route -- the eager executor, the bounded kernel, and the column-naming
/// helper all three doors share -- asks this, so a function added to this
/// list is an aggregate on every route at once instead of on whichever route
/// remembered to match its name.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Aggregate {
    /// `COUNT(*)` counts rows; `COUNT(<expr>)` counts the rows whose argument
    /// is not NULL.
    Count,
    /// `SUM(<expr>)` adds the argument's values, skipping NULLs. Over no rows
    /// at all it is NULL, which is a different answer from zero.
    Sum,
}

impl Aggregate {
    /// The aggregate a function name asks for, matched the way SQL matches
    /// function names.
    fn named(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("count") {
            Some(Self::Count)
        } else if name.eq_ignore_ascii_case("sum") {
            Some(Self::Sum)
        } else {
            None
        }
    }
}

/// The aggregate this projected column computes, or `None` when it projects
/// an ordinary expression.
pub(crate) fn column_aggregate(column: &contextdb_planner::ProjectColumn) -> Option<Aggregate> {
    match &column.expr {
        Expr::FunctionCall { name, .. } => Aggregate::named(name),
        _ => None,
    }
}

/// Whether this projection is an aggregate projection at all.
pub(crate) fn projection_aggregates(columns: &[contextdb_planner::ProjectColumn]) -> bool {
    columns
        .iter()
        .any(|column| column_aggregate(column).is_some())
}

/// The refusal a projection earns when it names an aggregate beside a plain
/// column and has no GROUP BY to put the plain column under.
pub(crate) fn mixed_aggregate_error() -> Error {
    Error::PlanError("mixed aggregate and non-aggregate columns without GROUP BY".to_string())
}

/// The standard-SQL out-of-range refusal, raised instead of wrapping an
/// aggregate's running total around.
fn aggregate_out_of_range() -> Error {
    Error::PlanError("integer out of range".to_string())
}

/// What an aggregate reads out of each input row.
pub(crate) enum AggregateInput<'expr> {
    /// The `*` argument: the row itself, whatever it holds.
    Row,
    /// An expression the row is asked for.
    Value(&'expr Expr),
    /// No argument at all.
    Nothing,
}

/// What an aggregate reads out of each input row, decided from the call's
/// argument list once so both doors read the same argument.
pub(crate) fn aggregate_input(args: &[Expr]) -> AggregateInput<'_> {
    match args.first() {
        None => AggregateInput::Nothing,
        Some(Expr::Column(ColumnRef {
            table: None,
            column,
        })) if column == "*" => AggregateInput::Row,
        Some(first) => AggregateInput::Value(first),
    }
}

/// One input row's contribution to an aggregate.
pub(crate) enum AggregateItem<'value> {
    /// The row itself.
    Row,
    /// What the aggregate's argument evaluated to on this row.
    Value(&'value Value),
    /// The aggregate names no argument, so this row contributes nothing.
    Nothing,
}

/// One aggregate's running answer over the rows read so far.
///
/// Both doors fold their rows in here, so the eager answer and the bounded
/// one cannot disagree about NULLs, about overflow, or about what an
/// aggregate over no rows at all is. The bounded door additionally keeps this
/// between cursor fetches, which is what lets an aggregate finish across
/// pages.
pub(crate) enum AggregateState {
    Count(u64),
    /// A SUM that has admitted no value yet.
    SumEmpty,
    SumInt(i64),
    SumFloat(f64),
}

impl AggregateState {
    pub(crate) fn new(aggregate: Aggregate) -> Self {
        match aggregate {
            Aggregate::Count => Self::Count(0),
            Aggregate::Sum => Self::SumEmpty,
        }
    }

    /// Fold one row's contribution into the running answer.
    pub(crate) fn admit(&mut self, item: AggregateItem<'_>) -> Result<()> {
        if let Self::Count(count) = self {
            if matches!(
                item,
                AggregateItem::Value(Value::Null) | AggregateItem::Nothing
            ) {
                return Ok(());
            }
            *count = count.checked_add(1).ok_or_else(aggregate_out_of_range)?;
            return Ok(());
        }
        let AggregateItem::Value(value) = item else {
            return Err(Error::PlanError("SUM needs a column to add".to_string()));
        };
        // A running total widens to a float the first time a float reaches
        // it, and never wraps: an integer total that cannot hold one more
        // value is the standard out-of-range refusal.
        let next = match (&*self, value) {
            (_, Value::Null) => return Ok(()),
            (Self::SumEmpty, Value::Int64(added)) => Self::SumInt(*added),
            (Self::SumEmpty, Value::Float64(added)) => Self::SumFloat(*added),
            (Self::SumInt(total), Value::Int64(added)) => Self::SumInt(
                total
                    .checked_add(*added)
                    .ok_or_else(aggregate_out_of_range)?,
            ),
            (Self::SumInt(total), Value::Float64(added)) => Self::SumFloat(*total as f64 + *added),
            (Self::SumFloat(total), Value::Int64(added)) => Self::SumFloat(*total + *added as f64),
            (Self::SumFloat(total), Value::Float64(added)) => Self::SumFloat(*total + *added),
            _ => return Err(Error::PlanError("SUM adds a numeric column".to_string())),
        };
        *self = next;
        Ok(())
    }

    /// The single value this aggregate publishes.
    pub(crate) fn finish(&self) -> Result<Value> {
        match self {
            Self::Count(count) => i64::try_from(*count)
                .map(Value::Int64)
                .map_err(|_| aggregate_out_of_range()),
            Self::SumEmpty => Ok(Value::Null),
            Self::SumInt(total) => Ok(Value::Int64(*total)),
            Self::SumFloat(total) => Ok(Value::Float64(*total)),
        }
    }
}

/// A projection's output names, mirroring both executor derivations: an
/// aggregate names the column after the function it called, everything else
/// after the projected column, and an explicit alias always wins.
fn project_output_columns(columns: &[contextdb_planner::ProjectColumn]) -> Vec<String> {
    let has_aggregate = projection_aggregates(columns);
    columns
        .iter()
        .map(|column| {
            column.alias.clone().unwrap_or_else(|| match &column.expr {
                Expr::FunctionCall { name, .. } if has_aggregate => name.clone(),
                Expr::Column(col) if !has_aggregate => col.column.clone(),
                _ => "expr".to_string(),
            })
        })
        .collect()
}

fn plan_anchor_table(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::Scan { table, .. }
        | PhysicalPlan::IndexScan { table, .. }
        | PhysicalPlan::VectorSearch { table, .. }
        | PhysicalPlan::HnswSearch { table, .. } => Some(table.clone()),
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::WindowRank { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. } => plan_anchor_table(input),
        PhysicalPlan::Join { left, .. } => plan_anchor_table(left),
        _ => None,
    }
}

fn is_predicate_pseudo_column(name: &str) -> bool {
    name == PREDICATE_PSEUDO_COLUMN || name == "*"
}

/// The relation names (real table names and declared aliases) that may
/// legally qualify a column reference against this plan node's output —
/// used only to check that an explicit qualifier (`t.col`) names something
/// actually in scope. A shape this helper does not model returns an empty
/// list, which is safe: callers only consult it alongside a column list for
/// that same shape (`plan_output_columns`), so an unmodeled shape already
/// skips the check entirely rather than reaching here.
fn known_relation_names(plan: &PhysicalPlan) -> Vec<String> {
    match plan {
        PhysicalPlan::MaterializeCte { name, .. } => vec![name.clone()],
        PhysicalPlan::Scan { table, alias, .. } => {
            vec![alias.clone().unwrap_or_else(|| table.clone())]
        }
        PhysicalPlan::IndexScan { table, .. } => vec![table.clone()],
        // The vector table's own name is always a valid qualifier, but a
        // qualifier from the CANDIDATE subtree (e.g. a join alias feeding
        // the vector search) is equally valid — union both rather than
        // picking one, since this is only ever consulted as a permissive
        // whitelist alongside a column list for the same shape.
        PhysicalPlan::VectorSearch {
            table, candidates, ..
        }
        | PhysicalPlan::HnswSearch {
            table, candidates, ..
        } => {
            let mut names = vec![table.clone()];
            if let Some(candidates) = candidates {
                names.extend(known_relation_names(candidates));
            }
            names
        }
        PhysicalPlan::Join {
            left,
            right,
            left_alias,
            right_alias,
            ..
        } => {
            let mut names = known_relation_names(left);
            if let Some(alias) = left_alias {
                names.push(alias.clone());
            }
            names.push(
                right_alias
                    .clone()
                    .unwrap_or_else(|| right_table_name(right)),
            );
            names
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::WindowRank { input, .. }
        | PhysicalPlan::Project { input, .. } => known_relation_names(input),
        PhysicalPlan::CteRef { name } => vec![name.clone()],
        PhysicalPlan::Union { inputs, .. } => {
            inputs.iter().flat_map(known_relation_names).collect()
        }
        _ => Vec::new(),
    }
}

/// Whether a plan subtree contains a `GraphBfs` node anywhere beneath it —
/// used to skip a validator wherever `plan_output_columns` cannot be
/// trusted for the shape (GraphBfs's `COLUMNS (...)` aliasing is not
/// modeled there at all; see `plan_output_columns`'s `GraphBfs` arm).
fn plan_contains_graph_bfs(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::GraphBfs { .. } => true,
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::WindowRank { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. }
        | PhysicalPlan::Project { input, .. } => plan_contains_graph_bfs(input),
        PhysicalPlan::Join { left, right, .. } => {
            plan_contains_graph_bfs(left) || plan_contains_graph_bfs(right)
        }
        PhysicalPlan::Union { inputs, .. } => inputs.iter().any(plan_contains_graph_bfs),
        PhysicalPlan::Pipeline(plans) => plans.iter().any(plan_contains_graph_bfs),
        _ => false,
    }
}

/// Reject a post-scan predicate/expression — the `WHERE` of a join or of a
/// CTE-sourced SELECT, an ORDER BY key, or a SELECT-list expression — that
/// names a column its input does not produce, or that carries an explicit
/// qualifier naming no relation in scope.
///
/// The single-table WHERE path is guarded directly at the scan against the
/// declared schema (`check_predicate_columns_declared`); everything reaching
/// here plans as a `Filter` / `Sort` / `Project` / `Join` node instead, over
/// an input whose own column list is the authority for what the expression
/// will actually be evaluated against.
///
/// An UNqualified reference matches any column with that bare name. A
/// QUALIFIED reference matches its exact qualified name, or — only once the
/// qualifier itself is confirmed to name a real relation in this scope via
/// `known_relation_names` — a column carrying that bare name with no
/// qualifier of its own (nothing else claims it, so the reference's
/// qualifier cannot contradict it). What it must never do is match a column
/// qualified to a DIFFERENT relation, or accept a qualifier that names no
/// relation at all: a join's OUTPUT list is fully qualified, so rejecting
/// the cross-table match is what makes `... JOIN s ... WHERE s.b = 1` name
/// `s` when `b` lives only on `t`; a join's ON-CONDITION list qualifies only
/// the names that COLLIDE across the two sides, leaving the rest bare — so
/// accepting the unqualified match (for a REAL qualifier) is what keeps
/// `ON decisions.id = outcomes.decision_id` working, where `decision_id` is
/// unique and therefore stays bare — while a qualifier naming no relation in
/// scope at all (a typo'd alias) is rejected rather than silently resolving
/// through that same bare-name fallback.
fn validate_filter_predicate_columns(
    input_columns: &[String],
    input: &PhysicalPlan,
    expr: &Expr,
) -> Result<()> {
    let known_relations = known_relation_names(input);
    walk_predicate_columns(expr, &mut |column_ref| {
        if is_predicate_pseudo_column(&column_ref.column) {
            return Ok(());
        }
        match resolve_query_result_column_scoped(input_columns, column_ref, &known_relations) {
            QueryResultColumn::Found(_) => Ok(()),
            QueryResultColumn::Ambiguous => Err(Error::PlanError(format!(
                "ambiguous column reference: {}",
                column_ref.column
            ))),
            QueryResultColumn::NotFound => Err(Error::ColumnNotFound {
                table: column_ref
                    .table
                    .clone()
                    .or_else(|| plan_anchor_table(input))
                    .unwrap_or_else(|| "<query>".to_string()),
                column: column_ref.column.clone(),
            }),
        }
    })
}

fn exec_create_index(
    db: &Database,
    plan: &contextdb_planner::CreateIndexPlan,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    let validated_schema = db.relational_store().table_meta.read().clone();
    // Reserved-prefix guard: user-declared indexes must not collide with the
    // auto-index namespace used for PRIMARY KEY / UNIQUE backing indexes.
    if let Some(prefix) = reserved_index_prefix(&plan.name) {
        return Err(Error::ReservedIndexName {
            table: plan.table.clone(),
            name: plan.name.clone(),
            prefix: prefix.to_string(),
        });
    }

    // Error precedence: TableNotFound > ColumnNotFound
    // > ColumnNotIndexable > DuplicateIndex. Check in that exact order so
    // "structural" bugs surface before "naming" bugs.
    let meta = db
        .table_meta(&plan.table)
        .ok_or_else(|| Error::TableNotFound(plan.table.clone()))?;

    // 2. Check every column exists.
    for (col_name, _) in &plan.columns {
        if !meta.columns.iter().any(|c| c.name == *col_name) {
            return Err(Error::ColumnNotFound {
                table: plan.table.clone(),
                column: col_name.clone(),
            });
        }
    }

    // 3. Check every column type is B-tree indexable.
    for (col_name, _) in &plan.columns {
        let col = meta
            .columns
            .iter()
            .find(|c| c.name == *col_name)
            .expect("column existence verified above");
        if !btree_indexable(&col.column_type) {
            return Err(Error::ColumnNotIndexable {
                table: plan.table.clone(),
                column: col_name.clone(),
                column_type: col.column_type.clone(),
            });
        }
    }

    // 4. Duplicate-name check (last).
    if meta.indexes.iter().any(|i| i.name == plan.name) {
        return Err(Error::DuplicateIndex {
            table: plan.table.clone(),
            index: plan.name.clone(),
        });
    }

    // All validations passed. Reserve the DDL LSN before publishing the
    // IndexDecl/storage so concurrent DML cannot appear before the index DDL
    // in changes_since().  Redb receives the projected metadata, unchanged
    // rows/vectors, generation sidecar, and DDL before memory sees the index.
    db.allocate_ddl_lsn(|lsn| {
        let old_meta = db
            .table_meta(&plan.table)
            .ok_or_else(|| Error::TableNotFound(plan.table.clone()))?;
        if !schema_snapshot_matches(db, &validated_schema) {
            return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
        }
        let mut projected_meta = old_meta.clone();
        projected_meta.indexes.push(contextdb_core::IndexDecl {
            name: plan.name.clone(),
            columns: plan.columns.clone(),
            kind: contextdb_core::IndexKind::UserDeclared,
        });
        let projected_meta = db.project_table_meta_with_auto_indexes(projected_meta);
        let projected_rows = db.table_versions_for_projection(&plan.table)?;
        let ddl = vec![DdlChange::CreateIndex {
            table: plan.table.clone(),
            name: plan.name.clone(),
            columns: plan.columns.clone(),
        }];
        let mut memory_swap = db.prepare_local_table_projection_memory(
            &plan.table,
            &old_meta,
            &projected_meta,
            &projected_rows,
        )?;
        let projected_indexes =
            db.prepare_local_index_projection(&projected_meta, &projected_rows)?;
        db.persist_local_table_projection_and_ddl(
            &plan.table,
            &projected_meta,
            &projected_rows,
            lsn,
            &ddl,
            read_memory.clone(),
        )?;
        db.publish_local_table_projection_and_ddl(
            &plan.table,
            projected_meta,
            projected_rows,
            lsn,
            &ddl,
            projected_indexes,
        )?;
        memory_swap.commit_after_swap();
        Ok(())
    })?;

    db.clear_statement_cache();
    Ok(OwnedQueryResult::empty_with_affected(0))
}

fn exec_drop_index(
    db: &Database,
    plan: &contextdb_planner::DropIndexPlan,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedQueryResult> {
    let validated_schema = db.relational_store().table_meta.read().clone();
    let meta = db
        .table_meta(&plan.table)
        .ok_or_else(|| Error::TableNotFound(plan.table.clone()))?;
    let exists = meta.indexes.iter().any(|i| i.name == plan.name);
    if !exists {
        if plan.if_exists {
            return Ok(OwnedQueryResult::empty_with_affected(0));
        }
        return Err(Error::IndexNotFound {
            table: plan.table.clone(),
            index: plan.name.clone(),
        });
    }
    if let Some(prefix) = reserved_index_prefix(&plan.name) {
        return Err(Error::ReservedIndexName {
            table: plan.table.clone(),
            name: plan.name.clone(),
            prefix: prefix.to_string(),
        });
    }
    if let Some(block) = rank_policy_drop_index_blocker(db, &plan.table, &plan.name) {
        return Err(block);
    }
    db.allocate_ddl_lsn(|lsn| {
        let old_meta = db
            .table_meta(&plan.table)
            .ok_or_else(|| Error::TableNotFound(plan.table.clone()))?;
        if !schema_snapshot_matches(db, &validated_schema) {
            return Err(Error::Other(RETRY_LOCAL_SCHEMA_PROJECTION.to_string()));
        }
        let mut projected_meta = old_meta.clone();
        projected_meta
            .indexes
            .retain(|index| index.name != plan.name);
        let projected_meta = db.project_table_meta_with_auto_indexes(projected_meta);
        let projected_rows = db.table_versions_for_projection(&plan.table)?;
        let ddl = vec![DdlChange::DropIndex {
            table: plan.table.clone(),
            name: plan.name.clone(),
        }];
        let mut memory_swap = db.prepare_local_table_projection_memory(
            &plan.table,
            &old_meta,
            &projected_meta,
            &projected_rows,
        )?;
        let projected_indexes =
            db.prepare_local_index_projection(&projected_meta, &projected_rows)?;
        db.persist_local_table_projection_and_ddl(
            &plan.table,
            &projected_meta,
            &projected_rows,
            lsn,
            &ddl,
            read_memory.clone(),
        )?;
        db.publish_local_table_projection_and_ddl(
            &plan.table,
            projected_meta,
            projected_rows,
            lsn,
            &ddl,
            projected_indexes,
        )?;
        memory_swap.commit_after_swap();
        Ok(())
    })?;
    db.clear_statement_cache();
    Ok(OwnedQueryResult::empty_with_affected(0))
}

pub(crate) fn reserved_index_prefix(name: &str) -> Option<&'static str> {
    ["__pk_", "__unique_", "__fk_", "__graph_edge_"]
        .into_iter()
        .find(|prefix| name.starts_with(prefix))
}

/// Admit a replacement row version's retained bytes through the engine's
/// one row-admission door, so an F32 vector column's projection is charged
/// under its own `vector_insert@<table>.<column>` tag. Returns the whole
/// admitted amount, which is what the caller releases if the statement
/// fails afterwards.
fn admit_table_row_bytes(
    db: &Database,
    table: &str,
    values: &HashMap<String, Value>,
    subsystem: &str,
    operation: &str,
    hint: &str,
) -> Result<crate::memory_accounting::OwnedMemoryReservation> {
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    db.admit_retained_row_owned(table, values, &meta, subsystem, operation, hint)
}

// ========================= Index scan planning + execution =========================

/// Shape of a predicate on the first indexed column. Drives IndexScan
/// eligibility: equality narrows to a point, range to a range walk, IN-list
/// to multiple point lookups, IS NULL to the NULL partition.
#[derive(Debug, Clone)]
pub(crate) enum IndexPredicateShape {
    Equality(Value),
    NotEqual(Value),
    Range {
        lower: std::ops::Bound<Value>,
        upper: std::ops::Bound<Value>,
    },
    InList(Arc<Vec<Value>>),
    IsNull,
    IsNotNull,
}

impl IndexPredicateShape {
    /// Selectivity tier — lower is more selective.
    fn selectivity_tier(&self) -> u8 {
        match self {
            IndexPredicateShape::Equality(_) | IndexPredicateShape::InList(_) => 0,
            IndexPredicateShape::Range { .. } | IndexPredicateShape::NotEqual(_) => 1,
            IndexPredicateShape::IsNull | IndexPredicateShape::IsNotNull => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexPick {
    name: String,
    columns: Vec<(String, contextdb_core::SortDirection)>,
    /// Shape on the FIRST indexed column. Only one shape drives the scan.
    shape: IndexPredicateShape,
    /// Pushed column name (engine column name) for trace.
    pushed_column: String,
    /// Full pushed prefix in index-column order.
    pushed_columns: Vec<String>,
    /// Finite equality/IN/null shapes for pushed suffix columns, aligned with
    /// `pushed_columns[1..]`. Keeping the shape, rather than one representative
    /// value, lets every pushed column contribute to the tuple-run product.
    suffix_shapes: Vec<IndexPredicateShape>,
    /// Physical bounds on the first range after the finite prefix.
    trailing_range: Option<(std::ops::Bound<IndexKey>, std::ops::Bound<IndexKey>)>,
    /// The prefix is provably empty before touching the index tree
    /// (contradictory suffix constants, NULL/NaN suffix binds, or an
    /// incoercible leading equality/range value).
    prefix_empty: bool,
    /// Every finite probe value already has the declared column type and each
    /// finite set is sorted and deduplicated. Analyzer-created picks set this
    /// once so execution does not normalize the same bound list again.
    probe_values_normalized: bool,
    /// Whole key tuples a row-value `IN` names over the leading pushed
    /// columns, in key order, deduplicated. When present the runs are these
    /// tuples -- each extended by the remaining suffix shapes -- and never the
    /// cross product of the per-column value lists, which only bound them.
    tuple_prefix: Option<Arc<Vec<Vec<Value>>>>,
}

impl IndexPick {
    /// How many leading key columns the tuple prefix pins.
    fn tuple_arity(&self) -> usize {
        self.tuple_prefix
            .as_ref()
            .and_then(|tuples| tuples.first())
            .map_or(0, Vec::len)
    }

    /// The row-value tuples this pick probes, when a row-value `IN` drives it.
    pub(crate) fn tuple_prefix(&self) -> Option<&[Vec<Value>]> {
        self.tuple_prefix.as_deref().map(Vec::as_slice)
    }
}

#[derive(Debug, Clone)]
struct IndexCandidatePlan {
    pick: IndexPick,
    match_count: usize,
    tier: u8,
    whole_key: bool,
    column_count: usize,
    creation_index: usize,
    /// Whether this index enforces uniqueness (a declared `UNIQUE`, or the
    /// table's own primary key). Combined with `single_row_probe` below, a
    /// unique whole-key index reads at most one row, so a still-needed
    /// `ORDER BY` is moot for it -- see the winner comparator below.
    unique: bool,
    /// Whether every matched column (the driving shape plus every pushed
    /// suffix shape) is a single-value equality -- not an `IN` list of more
    /// than one value, not a range, not `IS NULL`. `whole_key` alone only
    /// means every declared column was matched; combined with `unique`, this
    /// is what actually implies the match names at most one row. `u IN (1,
    /// 2)` and a range are both whole-key matches on a UNIQUE column that
    /// can still return more than one row.
    single_row_probe: bool,
    /// Whether this index's declared column order, past the columns the
    /// filter pins to a single equality, already answers a still-needed
    /// `ORDER BY` -- the same question [`sort_keys_match_index_prefix`] asks
    /// of an already-chosen index, asked here of every candidate so the
    /// winner comparator can weigh it.
    sort_elides: bool,
}

/// One fixed scale every candidate is ranked against, lowest (best) first.
/// Comparing candidates pairwise instead of against a common scale is what
/// produces a preference cycle among three or more candidates; a scalar
/// rank cannot cycle, because every candidate's place on the scale is fixed
/// independently of which other candidates are present.
///
/// 0. A UNIQUE whole-key index whose match genuinely names one row (every
///    matched column a single-value equality, not an `IN` list of more than
///    one value, a range, or `IS NULL`) -- but ONLY when an `ORDER BY` is
///    actually pending; with none, this rank does not exist and cannot
///    override the unordered tie (rank 2, decided below by
///    shorter declaration then creation order). A UNIQUE index whose match
///    cannot be trusted to name one row (an `IN` list, a range, ...) never
///    reaches this rank, ordered or not; the same fact ALSO decides whether
///    EXPLAIN may honestly claim "at most one row" for it.
/// 1. Any index (whole-key or not) whose own order already answers a
///    still-needed `ORDER BY`.
/// 2. A whole-key index that does not answer the order (the preference
///    remains when there is no `ORDER BY` at all,
///    since `sort_elides` is then false for every candidate and rank 0 is
///    unreachable).
/// 3. Everything else.
fn candidate_placement_rank(candidate: &IndexCandidatePlan, order_by_pending: bool) -> u8 {
    let implies_one_row = candidate.whole_key && candidate.unique && candidate.single_row_probe;
    if order_by_pending && implies_one_row {
        0
    } else if candidate.sort_elides {
        1
    } else if candidate.whole_key {
        2
    } else {
        3
    }
}

#[cfg(test)]
mod candidate_placement_rank_tests {
    //! A UNIQUE whole-key candidate must not get rank 0 without a pending
    //! `ORDER BY`: the unordered tie uses shorter declaration and then
    //! creation order. These are pure unit tests of the ranking function against
    //! synthetic candidates, since the automatic index this engine creates
    //! for a column-level `UNIQUE` always sorts before any later explicit
    //! index in `TableMeta.indexes` -- a real DDL fixture cannot put a
    //! lower-`creation_index` non-unique candidate ahead of it to observe
    //! the regression directly.
    use super::*;

    fn candidate(
        whole_key: bool,
        unique: bool,
        sort_elides: bool,
        single_row_probe: bool,
        column_count: usize,
        creation_index: usize,
    ) -> IndexCandidatePlan {
        IndexCandidatePlan {
            pick: IndexPick {
                name: "synthetic".to_string(),
                columns: Vec::new(),
                shape: IndexPredicateShape::Equality(Value::Int64(0)),
                pushed_column: "col".to_string(),
                pushed_columns: Vec::new(),
                suffix_shapes: Vec::new(),
                trailing_range: None,
                prefix_empty: false,
                probe_values_normalized: true,
                tuple_prefix: None,
            },
            match_count: column_count,
            tier: 0,
            whole_key,
            column_count,
            creation_index,
            unique,
            sort_elides,
            single_row_probe,
        }
    }

    #[test]
    fn without_order_by_uniqueness_never_outranks_a_non_unique_whole_key_candidate() {
        let earlier_non_unique = candidate(true, false, false, false, 1, 0);
        let later_unique = candidate(true, true, false, true, 1, 1);
        assert_eq!(
            candidate_placement_rank(&earlier_non_unique, false),
            candidate_placement_rank(&later_unique, false),
            "with no ORDER BY pending, uniqueness must not change a whole-key candidate's rank -- \
             the pre-61ca8bfc tie (shorter declaration, then creation order) must still decide it"
        );
    }

    #[test]
    fn with_order_by_pending_a_single_row_unique_candidate_outranks_sort_elision() {
        let unique_single_row = candidate(true, true, false, true, 1, 5);
        let sort_eliding = candidate(false, false, true, false, 2, 0);
        assert!(
            candidate_placement_rank(&unique_single_row, true)
                < candidate_placement_rank(&sort_eliding, true),
            "a UNIQUE candidate that genuinely reads at most one row must outrank sort elision"
        );
    }

    #[test]
    fn with_order_by_pending_a_unique_candidate_without_a_single_row_probe_does_not_outrank_sort_elision()
     {
        // e.g. `u IN (1, 2)` or a range on a UNIQUE column: whole-key and
        // unique, but the probe does not imply a single row, so it must not
        // out-rank a candidate that genuinely elides the sort.
        let unique_multi_row = candidate(true, true, false, false, 1, 5);
        let sort_eliding = candidate(false, false, true, false, 2, 0);
        assert!(
            candidate_placement_rank(&unique_multi_row, true)
                > candidate_placement_rank(&sort_eliding, true),
            "a UNIQUE candidate whose probe can still return more than one row must not out-rank \
             sort elision"
        );
    }
}

fn automatic_index_supports_pick(
    decl: &contextdb_core::IndexDecl,
    shape: &IndexPredicateShape,
    pushed_columns: usize,
    automatic_ordered: bool,
) -> bool {
    if decl.kind != contextdb_core::IndexKind::Auto || automatic_ordered {
        return true;
    }
    let finite = matches!(
        shape,
        IndexPredicateShape::Equality(_)
            | IndexPredicateShape::InList(_)
            | IndexPredicateShape::IsNull
    );
    finite && (decl.columns.len() > 1 || pushed_columns == decl.columns.len())
}

/// Whether `decl`'s exact column set is a declared `UNIQUE` or the table's
/// own primary key -- in either case this index reads at most one row, so
/// there is nothing left to sort.
fn index_is_unique(decl: &contextdb_core::IndexDecl, meta: &TableMeta) -> bool {
    // A single-column UNIQUE or PRIMARY KEY is carried on the column itself,
    // not in `unique_constraints` (which the parser only accepts for two or
    // more columns) or `primary_key_columns` (only for a composite key).
    if let [(column, _)] = decl.columns.as_slice()
        && meta
            .columns
            .iter()
            .any(|c| c.name == *column && (c.unique || c.primary_key))
    {
        return true;
    }
    let decl_cols: std::collections::BTreeSet<&str> =
        decl.columns.iter().map(|(c, _)| c.as_str()).collect();
    let single_column_primary_key = (meta.primary_key_columns.len() == 1)
        .then_some(&meta.primary_key_columns)
        .into_iter();
    meta.enforced_unique_tuples()
        .chain(single_column_primary_key)
        .any(|tuple| {
            tuple
                .iter()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>()
                == decl_cols
        })
}

/// Whether `shape` names exactly one, non-null value. `Equality` is the only
/// shape that does: an `IN` list can name more than one value, a range can
/// match every value it spans, and `IS NULL`/`IS NOT NULL` name a whole
/// partition, not one value. This is what a UNIQUE index's whole-key match
/// actually needs, on every matched column, before it can honestly claim to
/// read at most one row -- `whole_key` alone only counts matched columns,
/// not what kind of match each one is.
fn shape_is_single_value_equality(shape: &IndexPredicateShape) -> bool {
    matches!(shape, IndexPredicateShape::Equality(value) if !matches!(value, Value::Null))
}

/// Whether `decl`'s declared order, past whatever leading columns `filter`
/// pins to a single equality, already answers `sort_keys` without a sort --
/// the same question [`sort_keys_match_index_prefix_with_direction`] asks of
/// an already-chosen index, asked here of a candidate still being weighed
/// against the others. `allow_reverse` is the CALLER's own true capability,
/// not an assumption made here: the bounded read kernel really can walk an
/// index backward and passes `true`; the eager executor never reverses a
/// walk in `execute_index_scan` and must pass `false`, or candidate
/// selection would promise a route the eager executor cannot actually take.
fn candidate_elides_sort(
    decl: &contextdb_core::IndexDecl,
    filter: &Expr,
    sort_keys: &[contextdb_planner::SortKey],
    allow_reverse: bool,
    automatic_ordered: bool,
) -> bool {
    if sort_keys.is_empty() || automatic_index_is_exact_only(decl, automatic_ordered) {
        return false;
    }
    if let Some(leading_col) = decl.columns.first().map(|(c, _)| c.as_str())
        && leading_filter_has_fragmented_order_shape(filter, leading_col)
    {
        return false;
    }
    if declared_columns_answer_sort_keys(&decl.columns, sort_keys, allow_reverse) {
        return true;
    }
    let pinned = count_equality_prefix(Some(filter), &decl.columns);
    declared_columns_answer_sort_keys(&decl.columns[pinned..], sort_keys, allow_reverse)
}

/// Top-level decision: did we rewrite to IndexScan?
/// Carries index pick + the rejected candidates (for trace) + residual filter.
struct IndexAnalysis {
    pick: Option<IndexPick>,
    considered: Vec<crate::database::IndexCandidate>,
}

/// The relational route a plan would take, derived without touching rows.
///
/// This is deliberately distinct from [`QueryTrace`]: it is planning
/// metadata, not a receipt for work that happened. The analysis below reuses
/// the executor's index chooser and sort-elision predicates so passive
/// `.explain` and a later execution cannot drift into separate routing rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PassiveRelationalRoute {
    pub physical_plan: String,
    pub index_used: Option<String>,
    pub predicates_pushed: Vec<String>,
    pub indexes_considered: Vec<crate::database::IndexCandidate>,
    pub sort_elided: bool,
}

/// Analyze the ordinary relational route for `plan` without executing it.
///
/// `None` means the plan is not an ordinary relational read whose runtime
/// trace is represented by the scan/index fields (for example a write, graph
/// traversal, or vector search). Schema metadata and supplied parameter
/// values are the only inputs; no row, graph, vector, or plugin path is
/// reachable from this function.
pub(crate) fn analyze_passive_relational_route(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
) -> Option<PassiveRelationalRoute> {
    analyze_passive_relational_route_with_sort(db, plan, params, None)
}

fn analyze_passive_relational_route_with_sort(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    pending_sort_keys: Option<&[contextdb_planner::SortKey]>,
) -> Option<PassiveRelationalRoute> {
    match plan {
        PhysicalPlan::Scan { table, filter, .. } => {
            let meta = db.table_meta(table);
            let analysis = filter
                .as_ref()
                .filter(|_| meta.as_ref().is_some_and(|meta| !meta.indexes.is_empty()))
                .and_then(|filter| {
                    meta.as_ref().map(|meta| {
                        // Ordinary reads answer through the bounded read
                        // kernel, which really can walk a chosen index
                        // backward: allow_reverse=true here matches what
                        // execution actually does, so this passive route
                        // describes the truth.
                        analyze_filter_for_index_with_sort(
                            table,
                            meta,
                            filter,
                            params,
                            pending_sort_keys,
                            true,
                        )
                    })
                });

            match analysis {
                Some(IndexAnalysis {
                    pick: Some(pick),
                    considered,
                }) => {
                    let chosen_name = pick.name;
                    Some(PassiveRelationalRoute {
                        physical_plan: "IndexScan".to_owned(),
                        index_used: Some(chosen_name.clone()),
                        predicates_pushed: pick.pushed_columns,
                        indexes_considered: considered
                            .into_iter()
                            .filter(|candidate| candidate.name != chosen_name)
                            .collect(),
                        sort_elided: false,
                    })
                }
                Some(IndexAnalysis {
                    pick: None,
                    considered,
                }) => Some(PassiveRelationalRoute {
                    physical_plan: "Scan".to_owned(),
                    index_used: None,
                    predicates_pushed: Vec::new(),
                    indexes_considered: considered,
                    sort_elided: false,
                }),
                None => Some(PassiveRelationalRoute {
                    physical_plan: "Scan".to_owned(),
                    index_used: None,
                    predicates_pushed: Vec::new(),
                    indexes_considered: Vec::new(),
                    sort_elided: false,
                }),
            }
        }
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. } => {
            analyze_passive_relational_route_with_sort(db, input, params, pending_sort_keys)
        }
        PhysicalPlan::Sort { input, keys } => {
            // Runtime sort path A walks a matching index directly when the
            // underlying scan has no filter. Ask the same predicate here.
            if let Some(index) = sort_elision_index_decl(db, input, keys) {
                let pushed = index
                    .columns
                    .first()
                    .map(|(column, _)| vec![column.clone()])
                    .unwrap_or_default();
                return Some(PassiveRelationalRoute {
                    physical_plan: "IndexScan".to_owned(),
                    index_used: Some(index.name),
                    predicates_pushed: pushed,
                    indexes_considered: Vec::new(),
                    sort_elided: true,
                });
            }

            let mut route = analyze_passive_relational_route_with_sort(
                db,
                input,
                params,
                Some(keys.as_slice()),
            )?;
            // Runtime sort path B keeps an already-selected index when its
            // order satisfies the requested key prefix. Either direction:
            // this route describes the bounded read kernel, which really can
            // walk the chosen index backward, so it must agree with the
            // reverse-aware selection above instead of contradicting it.
            if route.physical_plan == "IndexScan"
                && route.index_used.as_deref().is_some_and(|index| {
                    sort_keys_match_index_prefix_in_either_direction(db, input, index, keys)
                })
            {
                route.sort_elided = true;
                return Some(route);
            }
            if !trace_label_survives_sort(&route.physical_plan) {
                route.physical_plan = "Sort".to_owned();
            }
            route.sort_elided = false;
            Some(route)
        }
        PhysicalPlan::IndexScan { index, .. } => Some(PassiveRelationalRoute {
            physical_plan: "IndexScan".to_owned(),
            index_used: Some(index.clone()),
            predicates_pushed: Vec::new(),
            indexes_considered: Vec::new(),
            sort_elided: false,
        }),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexScanAccessMode {
    Select,
    Predicate,
}

/// Coerce every literal value inside `pick.shape` to `pick.pushed_column`'s
/// declared type. B-tree walks use variant-exact comparisons by design, so a
/// SELECT `WHERE uuid_col = 'text-literal'` must arrive at the executor with
/// the text already converted to Uuid. Coercion failure propagates so callers
/// can fall back to zero-rows — matching the semantics a predicate-evaluating
/// scan would produce on an un-coercible literal.
fn coerce_pick_shape_to_column_type(
    db: &Database,
    table: &str,
    pick: &IndexPick,
) -> Result<IndexPick> {
    use std::ops::Bound;
    if pick.probe_values_normalized {
        return Ok(pick.clone());
    }
    let col = &pick.pushed_column;
    let coerce = |column: &str, v: Value| coerce_index_probe_value(db, table, column, v);
    let coerce_finite = |column: &str, shape: &IndexPredicateShape| {
        let normalized = normalize_finite_constraint(shape, |value| coerce(column, value))
            .expect("finite probe normalization receives a finite shape");
        let empty = normalized.values.is_empty();
        (normalized.into_shape(), empty)
    };
    let mut prefix_empty = pick.prefix_empty;
    let new_shape = match &pick.shape {
        IndexPredicateShape::Equality(_)
        | IndexPredicateShape::InList(_)
        | IndexPredicateShape::IsNull => {
            let (shape, empty) = coerce_finite(col, &pick.shape);
            prefix_empty |= empty;
            shape
        }
        IndexPredicateShape::Range { lower, upper } => {
            let lower = match lower {
                Bound::Included(v) => match coerce(col, v.clone()) {
                    Some(v) => Bound::Included(v),
                    None => {
                        prefix_empty = true;
                        Bound::Unbounded
                    }
                },
                Bound::Excluded(v) => match coerce(col, v.clone()) {
                    Some(v) => Bound::Excluded(v),
                    None => {
                        prefix_empty = true;
                        Bound::Unbounded
                    }
                },
                Bound::Unbounded => Bound::Unbounded,
            };
            let upper = match upper {
                Bound::Included(v) => match coerce(col, v.clone()) {
                    Some(v) => Bound::Included(v),
                    None => {
                        prefix_empty = true;
                        Bound::Unbounded
                    }
                },
                Bound::Excluded(v) => match coerce(col, v.clone()) {
                    Some(v) => Bound::Excluded(v),
                    None => {
                        prefix_empty = true;
                        Bound::Unbounded
                    }
                },
                Bound::Unbounded => Bound::Unbounded,
            };
            IndexPredicateShape::Range { lower, upper }
        }
        IndexPredicateShape::NotEqual(v) => match coerce(col, v.clone()) {
            Some(v) => IndexPredicateShape::NotEqual(v),
            None => {
                prefix_empty = true;
                IndexPredicateShape::NotEqual(Value::Null)
            }
        },
        IndexPredicateShape::IsNotNull => IndexPredicateShape::IsNotNull,
    };
    let mut suffix_shapes = Vec::with_capacity(pick.suffix_shapes.len());
    for ((column, _), shape) in pick.columns.iter().skip(1).zip(&pick.suffix_shapes) {
        let (shape, empty) = coerce_finite(column, shape);
        prefix_empty |= empty;
        suffix_shapes.push(shape);
    }
    Ok(IndexPick {
        name: pick.name.clone(),
        columns: pick.columns.clone(),
        shape: new_shape,
        pushed_column: pick.pushed_column.clone(),
        pushed_columns: pick.pushed_columns.clone(),
        suffix_shapes,
        trailing_range: pick.trailing_range.clone(),
        prefix_empty,
        probe_values_normalized: true,
        tuple_prefix: pick.tuple_prefix.clone(),
    })
}

fn usable_index_probe_value(value: &Value) -> bool {
    !value.is_null() && !matches!(value, Value::Float64(number) if number.is_nan())
}

fn coerce_index_probe_value(
    db: &Database,
    table: &str,
    column: &str,
    value: Value,
) -> Option<Value> {
    let coerced = coerce_value_for_column(db, table, column, value, None, None).ok()?;
    let Some(meta) = db.table_meta(table) else {
        return Some(coerced);
    };
    exact_index_probe_value(&meta, column, coerced)
}

fn coerce_index_probe_value_with_meta(
    table: &str,
    meta: &TableMeta,
    column: &str,
    value: Value,
) -> Option<Value> {
    let coerced = coerce_value_for_column_with_meta(table, meta, column, value, None, None).ok()?;
    exact_index_probe_value(meta, column, coerced)
}

fn exact_index_probe_value(meta: &TableMeta, column: &str, coerced: Value) -> Option<Value> {
    let Some(column_def) = meta
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
    else {
        return Some(coerced);
    };
    match (&column_def.column_type, coerced) {
        (_, Value::Null) => Some(Value::Null),
        (ColumnType::Integer, Value::Int64(v)) => Some(Value::Int64(v)),
        (ColumnType::Integer, Value::Float64(v)) => exact_i64_from_float(v).map(Value::Int64),
        (ColumnType::Real, Value::Float64(v)) => Some(Value::Float64(v)),
        (ColumnType::Real, Value::Int64(v)) => Some(Value::Float64(v as f64)),
        (ColumnType::Text, Value::Text(v)) => Some(Value::Text(v)),
        (ColumnType::Boolean, Value::Bool(v)) => Some(Value::Bool(v)),
        (ColumnType::Uuid, Value::Uuid(v)) => Some(Value::Uuid(v)),
        (ColumnType::Timestamp, Value::Timestamp(v)) => Some(Value::Timestamp(v)),
        (ColumnType::Timestamp, Value::Int64(v)) => Some(Value::Timestamp(v)),
        (ColumnType::TxId, Value::TxId(v)) => Some(Value::TxId(v)),
        _ => None,
    }
}

struct NormalizedFiniteConstraint {
    values: Arc<Vec<Value>>,
    equality: bool,
    null_test: bool,
}

impl NormalizedFiniteConstraint {
    fn into_shape(self) -> IndexPredicateShape {
        if self.null_test {
            IndexPredicateShape::IsNull
        } else if self.equality && self.values.len() == 1 {
            IndexPredicateShape::Equality(self.values[0].clone())
        } else {
            IndexPredicateShape::InList(self.values)
        }
    }
}

fn normalize_finite_constraint(
    shape: &IndexPredicateShape,
    mut coerce: impl FnMut(Value) -> Option<Value>,
) -> Option<NormalizedFiniteConstraint> {
    let (mut values, equality, null_test) = match shape {
        IndexPredicateShape::Equality(value) => (
            coerce(value.clone())
                .filter(usable_index_probe_value)
                .into_iter()
                .collect(),
            true,
            false,
        ),
        IndexPredicateShape::InList(values) => (
            values
                .iter()
                .cloned()
                .filter_map(&mut coerce)
                .filter(usable_index_probe_value)
                .collect(),
            false,
            false,
        ),
        IndexPredicateShape::IsNull => (vec![Value::Null], false, true),
        IndexPredicateShape::NotEqual(_)
        | IndexPredicateShape::Range { .. }
        | IndexPredicateShape::IsNotNull => return None,
    };
    values.sort_unstable_by(|left, right| {
        compare_finite_constraint_values(left, right)
            .unwrap_or_else(|| fallback_finite_constraint_order(left, right))
    });
    values.dedup_by(|right, left| {
        compare_finite_constraint_values(left, right) == Some(Ordering::Equal)
    });
    Some(NormalizedFiniteConstraint {
        values: Arc::new(values),
        equality,
        null_test,
    })
}

fn exact_i64_from_float(value: f64) -> Option<i64> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    let candidate = value as i64;
    (compare_values(&Value::Int64(candidate), &Value::Float64(value)) == Some(Ordering::Equal))
        .then_some(candidate)
}

/// Inspect `filter` looking for an eligible predicate on the first column of
/// any declared index. Returns the chosen pick + list of considered/rejected.
fn analyze_filter_for_index(
    table: &str,
    meta: &TableMeta,
    filter: &Expr,
    params: &HashMap<String, Value>,
) -> IndexAnalysis {
    analyze_filter_for_index_with_sort(table, meta, filter, params, None, false)
}

/// [`analyze_filter_for_index`], plus a still-needed `ORDER BY` the winner
/// comparator weighs against whole-key coverage. `sort_keys: None` reproduces
/// `analyze_filter_for_index`'s behavior exactly -- every caller with no sort
/// in scope keeps calling that function unchanged.
///
/// `allow_reverse` is the CALLING ROUTE's own true capability to walk a
/// chosen index backward, not a property of the sort itself -- the same
/// query can be asked through a route that can (the bounded read kernel) or
/// cannot (the eager executor). Candidate selection must never promise a
/// reverse elision the calling route cannot actually deliver.
fn analyze_filter_for_index_with_sort(
    table: &str,
    meta: &TableMeta,
    filter: &Expr,
    params: &HashMap<String, Value>,
    sort_keys: Option<&[contextdb_planner::SortKey]>,
    allow_reverse: bool,
) -> IndexAnalysis {
    analyze_filter_for_index_with_capability(
        table,
        meta,
        filter,
        params,
        sort_keys,
        allow_reverse,
        false,
    )
}

/// [`analyze_filter_for_index_with_sort`] for a route that walks durable
/// single-column automatic indexes as ordered trees (`automatic_ordered`):
/// such an index then answers ranges and orders like a declared one. Only the
/// bounded ordered route can take that walk; every other caller passes false.
fn analyze_filter_for_index_with_capability(
    table: &str,
    meta: &TableMeta,
    filter: &Expr,
    params: &HashMap<String, Value>,
    sort_keys: Option<&[contextdb_planner::SortKey]>,
    allow_reverse: bool,
    automatic_ordered: bool,
) -> IndexAnalysis {
    #[cfg(feature = "test-seams")]
    let _finite_planner_observation = FinitePlannerObservationScope::enter();

    use std::borrow::Cow;
    const AUTOMATIC_INDEX_REASON: &str = "automatic index supports equality/IN prefix probes only";
    const FEWER_COLUMNS_REASON: &str = "fewer predicate columns matched than chosen index";

    let mut considered: Vec<crate::database::IndexCandidate> = Vec::new();

    // Find each conjunct (split on AND) and map to (column, shape).
    let conjuncts = split_conjuncts(filter);
    let mut conjunct_shapes: Vec<(usize, String, IndexPredicateShape)> = Vec::new();
    let mut row_value_lists: Vec<RowValueList> = Vec::new();
    for (order, conjunct) in conjuncts.iter().enumerate() {
        if let Some((col, shape)) = classify_index_predicate(conjunct, params) {
            conjunct_shapes.push((order, col, shape));
        } else if let Some(list) = classify_row_value_list(table, meta, conjunct, params) {
            // Each column of a row-value list is bounded by the values it
            // takes across the listed rows. That bound is what chooses the
            // index; the tuples themselves are attached to the winner below.
            for (position, column) in list.columns.iter().enumerate() {
                let mut values = list
                    .tuples
                    .iter()
                    .map(|tuple| tuple[position].clone())
                    .collect::<Vec<_>>();
                values.sort_unstable_by(|left, right| {
                    compare_finite_constraint_values(left, right)
                        .unwrap_or_else(|| fallback_finite_constraint_order(left, right))
                });
                values.dedup_by(|right, left| {
                    compare_finite_constraint_values(left, right) == Some(Ordering::Equal)
                });
                conjunct_shapes.push((
                    order,
                    column.clone(),
                    IndexPredicateShape::InList(Arc::new(values)),
                ));
            }
            row_value_lists.push(list);
        }
    }
    let normalized_finite = normalize_finite_columns(table, meta, &conjunct_shapes);

    // Annotate rejections on indexes that can't apply, for the trace.
    let mut candidates: Vec<IndexCandidatePlan> = Vec::new();
    let mut deferred_exact_auto_rejections: Vec<IndexCandidatePlan> = Vec::new();
    for (i_idx, decl) in meta.indexes.iter().enumerate() {
        let first_col = match decl.columns.first() {
            Some((c, _)) => c.clone(),
            None => continue,
        };
        // Find the most-selective matching conjunct on the first column.
        let matching: Vec<&(usize, String, IndexPredicateShape)> = conjunct_shapes
            .iter()
            .filter(|(_, c, _)| c == &first_col)
            .collect();
        if matching.is_empty() {
            // Check whether the filter mentions first_col in an un-usable way
            // (function call / arithmetic / col-to-col / subquery) to produce
            // a useful rejection reason.
            let reason = classify_rejection_reason(filter, &first_col);
            considered.push(crate::database::IndexCandidate {
                name: decl.name.clone(),
                rejected_reason: Cow::Borrowed(reason),
            });
            continue;
        }
        let shape = choose_driving_shape(&matching, normalized_finite.get(&first_col));
        let tier = shape.selectivity_tier();
        let probe_values_normalized = matches!(
            shape,
            IndexPredicateShape::Equality(_)
                | IndexPredicateShape::InList(_)
                | IndexPredicateShape::IsNull
        );
        let mut pushed_columns = vec![first_col.clone()];
        let mut suffix_shapes = Vec::new();
        let mut prefix_empty =
            matches!(&shape, IndexPredicateShape::InList(values) if values.is_empty());
        if matches!(
            shape,
            IndexPredicateShape::Equality(_)
                | IndexPredicateShape::InList(_)
                | IndexPredicateShape::IsNull
        ) {
            for (column, _) in decl.columns.iter().skip(1) {
                match suffix_finite_shape(&normalized_finite, column) {
                    SuffixFinite::Absent => break,
                    SuffixFinite::Shape(shape) => {
                        pushed_columns.push(column.clone());
                        prefix_empty |= matches!(
                            &shape,
                            IndexPredicateShape::InList(values) if values.is_empty()
                        );
                        suffix_shapes.push(shape);
                    }
                }
            }
        }
        let trailing_range = if shape_is_single_value_equality(&shape)
            && suffix_shapes.iter().all(shape_is_single_value_equality)
        {
            let position = 1 + suffix_shapes.len();
            decl.columns.get(position).and_then(|(column, _)| {
                let ranges = conjunct_shapes
                    .iter()
                    .filter_map(|(_, candidate, shape)| {
                        (candidate == column && matches!(shape, IndexPredicateShape::Range { .. }))
                            .then_some(shape.clone())
                    })
                    .collect::<Vec<_>>();
                if ranges.is_empty() {
                    return None;
                }
                let ranges = ranges
                    .iter()
                    .map(|shape| normalize_index_range(table, meta, column, shape))
                    .collect::<Option<Vec<_>>>();
                let Some(ranges) = ranges else {
                    prefix_empty = true;
                    return None;
                };
                let range = combine_shapes(ranges);
                let bounds = trailing_range_edges(meta, &decl.columns, position, &range);
                if bounds.is_none() {
                    prefix_empty = true;
                }
                bounds
            })
        } else {
            None
        };
        let match_count = pushed_columns.len() + usize::from(trailing_range.is_some());
        let automatic_supported =
            automatic_index_supports_pick(decl, &shape, 1 + suffix_shapes.len(), automatic_ordered);
        let single_row_probe = trailing_range.is_none()
            && shape_is_single_value_equality(&shape)
            && suffix_shapes.iter().all(shape_is_single_value_equality);
        let plan = IndexCandidatePlan {
            pick: IndexPick {
                name: decl.name.clone(),
                columns: decl.columns.clone(),
                shape,
                pushed_column: first_col.clone(),
                pushed_columns,
                suffix_shapes,
                trailing_range,
                prefix_empty,
                probe_values_normalized,
                tuple_prefix: None,
            },
            match_count,
            tier,
            whole_key: match_count == decl.columns.len(),
            column_count: decl.columns.len(),
            creation_index: i_idx,
            unique: index_is_unique(decl, meta),
            single_row_probe,
            sort_elides: sort_keys.is_some_and(|keys| {
                candidate_elides_sort(decl, filter, keys, allow_reverse, automatic_ordered)
            }),
        };
        if !automatic_supported {
            deferred_exact_auto_rejections.push(plan);
            continue;
        }
        candidates.push(plan);
    }

    // One deterministic decision for execution and EXPLAIN: deepest pushed
    // prefix, lower selectivity tier, then one placement rank (below) that
    // orders a still-needed ORDER BY elided over whole-key coverage (unless
    // the whole-key side is UNIQUE, so its sort is moot), then shorter
    // declaration, then declaration/creation order.
    //
    // The placement rank is a single scalar per candidate, not a pairwise
    // comparison, on purpose: three or more candidates whose whole-key,
    // unique, and sort-eliding facts differ pairwise (e.g. a composite that
    // elides the sort, a whole-key index that does not, and an unrelated
    // index that does neither) cannot be given a consistent order by asking
    // "does A beat B" once per pair -- that produces a preference cycle
    // whose winner depends on iteration order. A scalar rank orders every
    // candidate against the same fixed scale, so the winner is the same
    // regardless of how many candidates there are or what order they were
    // declared in.
    let order_by_pending = sort_keys.is_some_and(|keys| !keys.is_empty());
    let winner = candidates
        .iter()
        .min_by_key(|c| {
            (
                // A fully bound, non-NULL unique equality matches at most one row; prefer it
                // over any prefix that is not one, however many more columns that prefix binds.
                !(c.whole_key && c.unique && c.single_row_probe),
                std::cmp::Reverse(c.match_count),
                c.tier,
                candidate_placement_rank(c, order_by_pending),
                c.column_count,
                c.creation_index,
            )
        })
        .cloned();

    if let Some(winner) = &winner {
        for candidate in &candidates {
            if candidate.pick.name == winner.pick.name {
                continue;
            }
            let winner_implies_one_row =
                order_by_pending && winner.whole_key && winner.unique && winner.single_row_probe;
            let candidate_implies_one_row = order_by_pending
                && candidate.whole_key
                && candidate.unique
                && candidate.single_row_probe;
            let winner_is_unique_point =
                winner.whole_key && winner.unique && winner.single_row_probe;
            let candidate_is_unique_point =
                candidate.whole_key && candidate.unique && candidate.single_row_probe;
            let reason = if winner_is_unique_point
                && !candidate_is_unique_point
                && candidate.match_count > winner.match_count
            {
                // Only where the unique-point preference overturned the old order; every other
                // loser keeps its existing reason.
                "chosen index is UNIQUE and fully bound, so it matches at most one row"
            } else if candidate.match_count < winner.match_count {
                "fewer predicate columns matched than chosen index"
            } else if candidate.tier > winner.tier {
                "lower selectivity than chosen index"
            } else if winner_implies_one_row && !candidate_implies_one_row {
                "chosen index is UNIQUE and reads at most one row, so no sort is needed"
            } else if winner.sort_elides && !candidate.sort_elides {
                "chosen index already answers the requested order without a sort"
            } else if !candidate.whole_key
                && winner.whole_key
                && winner.pick.trailing_range.is_none()
            {
                "chosen index has whole-key coverage"
            } else if candidate.column_count > winner.column_count {
                "more indexed columns than chosen index"
            } else if !candidate.whole_key && winner.whole_key {
                "chosen index has whole-key coverage"
            } else {
                "tied with chosen index; lost by creation order"
            };
            considered.push(crate::database::IndexCandidate {
                name: candidate.pick.name.clone(),
                rejected_reason: Cow::Borrowed(reason),
            });
        }
        for candidate in deferred_exact_auto_rejections {
            let reason = if candidate.match_count < winner.match_count {
                FEWER_COLUMNS_REASON
            } else {
                AUTOMATIC_INDEX_REASON
            };
            considered.push(crate::database::IndexCandidate {
                name: candidate.pick.name,
                rejected_reason: Cow::Borrowed(reason),
            });
        }
    } else {
        for candidate in deferred_exact_auto_rejections {
            considered.push(crate::database::IndexCandidate {
                name: candidate.pick.name,
                rejected_reason: Cow::Borrowed(AUTOMATIC_INDEX_REASON),
            });
        }
    }

    IndexAnalysis {
        pick: winner.map(|winner| attach_row_value_tuples(winner.pick, &row_value_lists)),
        considered,
    }
}

/// A row-value `IN` over plain columns with bindable values, coerced to the
/// columns' declared types. A listed row holding a value no stored key can
/// equal (NULL, NaN, or a value the column type cannot hold) can never make
/// the comparison true, so it names no key and is dropped.
struct RowValueList {
    columns: Vec<String>,
    tuples: Vec<Vec<Value>>,
}

fn classify_row_value_list(
    table: &str,
    meta: &TableMeta,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Option<RowValueList> {
    let Expr::InList {
        expr,
        list,
        negated: false,
    } = expr
    else {
        return None;
    };
    let Expr::Row(elements) = expr.as_ref() else {
        return None;
    };
    let columns = elements
        .iter()
        .map(extract_simple_col_ref)
        .collect::<Option<Vec<_>>>()?;
    let distinct = columns.iter().collect::<HashSet<_>>();
    if distinct.len() != columns.len() {
        return None;
    }
    let mut tuples = Vec::with_capacity(list.len());
    'rows: for item in list {
        let Expr::Row(values) = item else {
            return None;
        };
        if values.len() != columns.len() {
            return None;
        }
        let mut tuple = Vec::with_capacity(values.len());
        for (column, value) in columns.iter().zip(values) {
            if !is_literal_or_param(value) {
                return None;
            }
            let raw = resolve_simple_rhs(value, params)?;
            match coerce_index_probe_value_with_meta(table, meta, column, raw)
                .filter(usable_index_probe_value)
            {
                Some(value) => tuple.push(value),
                None => continue 'rows,
            }
        }
        tuples.push(tuple);
    }
    Some(RowValueList { columns, tuples })
}

/// Give the chosen pick the whole tuples of a row-value list whose columns
/// are exactly its leading pushed columns, reordered to key order and kept
/// only where every value also satisfies the other finite terms on its column.
fn attach_row_value_tuples(mut pick: IndexPick, lists: &[RowValueList]) -> IndexPick {
    if pick.prefix_empty {
        return pick;
    }
    let Some(list) = lists.iter().find(|list| {
        let arity = list.columns.len();
        arity <= pick.pushed_columns.len()
            && pick.pushed_columns[..arity]
                .iter()
                .all(|column| list.columns.contains(column))
    }) else {
        return pick;
    };
    let arity = list.columns.len();
    let order = pick.pushed_columns[..arity]
        .iter()
        .map(|column| {
            list.columns
                .iter()
                .position(|listed| listed == column)
                .expect("every leading pushed column is listed")
        })
        .collect::<Vec<_>>();
    let shapes = std::iter::once(&pick.shape)
        .chain(&pick.suffix_shapes)
        .take(arity)
        .collect::<Vec<_>>();
    let mut tuples = Vec::with_capacity(list.tuples.len());
    for listed in &list.tuples {
        let tuple = order
            .iter()
            .map(|position| listed[*position].clone())
            .collect::<Vec<_>>();
        let admitted = tuple.iter().zip(&shapes).all(|(value, shape)| {
            finite_shape_values(shape).is_some_and(|values| {
                values.iter().any(|allowed| {
                    compare_finite_constraint_values(allowed, value) == Some(Ordering::Equal)
                })
            })
        });
        if admitted {
            tuples.push(tuple);
        }
    }
    let directions = pick.columns[..arity]
        .iter()
        .map(|(_, direction)| *direction)
        .collect::<Vec<_>>();
    let directed = |tuple: &Vec<Value>| -> IndexKey {
        tuple
            .iter()
            .zip(&directions)
            .map(|(value, direction)| match direction {
                contextdb_core::SortDirection::Asc => {
                    DirectedValue::Asc(TotalOrdAsc(value.clone()))
                }
                contextdb_core::SortDirection::Desc => {
                    DirectedValue::Desc(TotalOrdDesc(value.clone()))
                }
            })
            .collect()
    };
    tuples.sort_by_cached_key(directed);
    tuples.dedup_by(|right, left| directed(left) == directed(right));
    pick.prefix_empty = tuples.is_empty();
    pick.tuple_prefix = Some(Arc::new(tuples));
    pick
}

/// A finite physical minimum exists for every declared scalar index type.
/// NULL is the maximum ascending value and the minimum descending value.
fn index_column_minimum(meta: &TableMeta, column: &str) -> Option<Value> {
    Some(
        match &meta
            .columns
            .iter()
            .find(|def| def.name == column)?
            .column_type
        {
            ColumnType::Integer => Value::Int64(i64::MIN),
            ColumnType::Real => Value::Float64(f64::from_bits(0xffff_ffff_ffff_ffff)),
            ColumnType::Text => Value::Text(String::new()),
            ColumnType::Boolean => Value::Bool(false),
            ColumnType::Uuid => Value::Uuid(uuid::Uuid::nil()),
            ColumnType::Timestamp => Value::Timestamp(i64::MIN),
            ColumnType::TxId => Value::TxId(TxId(0)),
            _ => return None,
        },
    )
}

fn normalize_index_range(
    table: &str,
    meta: &TableMeta,
    column: &str,
    shape: &IndexPredicateShape,
) -> Option<IndexPredicateShape> {
    use std::ops::Bound;
    let IndexPredicateShape::Range { lower, upper } = shape else {
        return None;
    };
    let normalize = |bound: &Bound<Value>| {
        let value = |value: &Value| {
            coerce_index_probe_value_with_meta(table, meta, column, value.clone())
                .filter(usable_index_probe_value)
        };
        Some(match bound {
            Bound::Included(v) => Bound::Included(value(v)?),
            Bound::Excluded(v) => Bound::Excluded(value(v)?),
            Bound::Unbounded => Bound::Unbounded,
        })
    };
    Some(IndexPredicateShape::Range {
        lower: normalize(lower)?,
        upper: normalize(upper)?,
    })
}

fn trailing_range_edges(
    meta: &TableMeta,
    columns: &[(String, contextdb_core::SortDirection)],
    position: usize,
    shape: &IndexPredicateShape,
) -> Option<(std::ops::Bound<IndexKey>, std::ops::Bound<IndexKey>)> {
    use std::ops::Bound;
    let IndexPredicateShape::Range { lower, upper } = shape else {
        return None;
    };
    let (column, direction) = &columns[position];
    let directed = |value, direction| match direction {
        contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value)),
        contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value)),
    };
    let high_suffix = columns
        .iter()
        .skip(position + 1)
        .map(|(column, direction)| {
            let value = match direction {
                contextdb_core::SortDirection::Asc => Value::Null,
                contextdb_core::SortDirection::Desc => index_column_minimum(meta, column)?,
            };
            Some(directed(value, *direction))
        })
        .collect::<Option<IndexKey>>()?;
    let edge = |bound: &Bound<Value>, high: bool| -> Option<Bound<IndexKey>> {
        let (value, included) = match bound {
            Bound::Included(value) => (value.clone(), true),
            Bound::Excluded(value) => (value.clone(), false),
            Bound::Unbounded => {
                let value = if high == (*direction == contextdb_core::SortDirection::Asc) {
                    Value::Null
                } else {
                    index_column_minimum(meta, column)?
                };
                let included = !value.is_null();
                (value, included)
            }
        };
        if !matches!(bound, Bound::Unbounded) && !usable_index_probe_value(&value) {
            return None;
        }
        let mut key = vec![directed(value, *direction)];
        // An exclusive lower edge skips the entire equal-component group;
        // an inclusive upper edge includes it. Other edges stay short.
        if high == included {
            key.extend(high_suffix.iter().cloned());
        }
        Some(if included {
            Bound::Included(key)
        } else {
            Bound::Excluded(key)
        })
    };
    let (low, high) = match direction {
        contextdb_core::SortDirection::Asc => (lower, upper),
        contextdb_core::SortDirection::Desc => (upper, lower),
    };
    let bounds = (edge(low, false)?, edge(high, true)?);
    if let (
        Bound::Included(low) | Bound::Excluded(low),
        Bound::Included(high) | Bound::Excluded(high),
    ) = (&bounds.0, &bounds.1)
    {
        match low.cmp(high) {
            Ordering::Greater => return None,
            Ordering::Equal
                if matches!(bounds.0, Bound::Excluded(_))
                    || matches!(bounds.1, Bound::Excluded(_)) =>
            {
                return None;
            }
            _ => {}
        }
    }
    Some(bounds)
}

fn choose_driving_shape(
    matching: &[&(usize, String, IndexPredicateShape)],
    normalized: Option<&NormalizedFiniteColumn>,
) -> IndexPredicateShape {
    let Some((first_order, _, first_shape)) = matching
        .iter()
        .min_by_key(|(order, _, shape)| (shape.selectivity_tier(), *order))
        .copied()
    else {
        unreachable!("choose_driving_shape requires at least one match");
    };
    if first_shape.selectivity_tier() == 0 {
        if let Some(shape) = normalized.and_then(|column| column.shape.as_ref()) {
            return shape.clone();
        }
    } else if matches!(first_shape, IndexPredicateShape::Range { .. }) {
        let mut range_shapes: Vec<IndexPredicateShape> = matching
            .iter()
            .filter_map(|(_, _, shape)| {
                if matches!(shape, IndexPredicateShape::Range { .. }) {
                    Some(shape.clone())
                } else {
                    None
                }
            })
            .collect();
        if !range_shapes.is_empty() {
            return combine_shapes(std::mem::take(&mut range_shapes));
        }
    }
    let _ = first_order;
    first_shape.clone()
}

enum SuffixFinite {
    Absent,
    Shape(IndexPredicateShape),
}

fn suffix_finite_shape(
    normalized: &HashMap<String, NormalizedFiniteColumn>,
    column: &str,
) -> SuffixFinite {
    let Some(column) = normalized.get(column) else {
        return SuffixFinite::Absent;
    };
    if !column.all_constraints_finite {
        return SuffixFinite::Absent;
    }
    column
        .shape
        .clone()
        .map_or(SuffixFinite::Absent, SuffixFinite::Shape)
}

#[derive(Clone)]
struct NormalizedFiniteColumn {
    shape: Option<IndexPredicateShape>,
    all_constraints_finite: bool,
}

fn normalize_finite_columns(
    table: &str,
    meta: &TableMeta,
    conjunct_shapes: &[(usize, String, IndexPredicateShape)],
) -> HashMap<String, NormalizedFiniteColumn> {
    let indexed_columns = meta
        .indexes
        .iter()
        .flat_map(|index| index.columns.iter().map(|(column, _)| column.as_str()))
        .collect::<HashSet<_>>();
    let mut grouped: HashMap<String, Vec<&IndexPredicateShape>> = HashMap::new();
    for (_, column, shape) in conjunct_shapes {
        if indexed_columns.contains(column.as_str()) {
            grouped.entry(column.clone()).or_default().push(shape);
        }
    }
    grouped
        .into_iter()
        .map(|(column, shapes)| {
            let all_constraints_finite = shapes.iter().all(|shape| {
                matches!(
                    shape,
                    IndexPredicateShape::Equality(_)
                        | IndexPredicateShape::InList(_)
                        | IndexPredicateShape::IsNull
                )
            });
            let constraints = shapes.iter().filter_map(|shape| {
                normalize_finite_constraint(shape, |value| {
                    coerce_index_probe_value_with_meta(table, meta, &column, value)
                })
            });
            let shape = intersect_normalized_finite_constraints(constraints);
            (
                column,
                NormalizedFiniteColumn {
                    shape,
                    all_constraints_finite,
                },
            )
        })
        .collect()
}

fn intersect_normalized_finite_constraints(
    constraints: impl IntoIterator<Item = NormalizedFiniteConstraint>,
) -> Option<IndexPredicateShape> {
    let mut constraints = constraints.into_iter();
    let first = constraints.next()?;
    let mut values = first.values;
    let mut saw_equality = first.equality;
    let mut saw_null_test = first.null_test;
    let mut saw_non_null_constraint = !first.null_test;
    for constraint in constraints {
        saw_equality |= constraint.equality;
        saw_null_test |= constraint.null_test;
        saw_non_null_constraint |= !constraint.null_test;
        values = if saw_null_test && saw_non_null_constraint {
            // `IS NULL` can never be true beside `=` or `IN` on the same
            // column. Resolve that contradiction before asking an ordering
            // comparator to relate NULL to a finite value.
            Arc::new(Vec::new())
        } else {
            Arc::new(intersect_sorted_finite_values(&values, &constraint.values))
        };
    }
    Some(
        NormalizedFiniteConstraint {
            values,
            equality: saw_equality,
            null_test: saw_null_test && !saw_non_null_constraint,
        }
        .into_shape(),
    )
}

fn intersect_sorted_finite_values(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut intersection = Vec::with_capacity(left.len().min(right.len()));
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        let Some(ordering) =
            compare_finite_constraint_values(&left[left_index], &right[right_index])
        else {
            // Normalized index probes have one declared type. If an
            // unexpected pair still cannot be ordered, preserve the SQL `=`
            // behavior used by row predicates rather than guessing that the
            // intersection is empty or panicking on caller input.
            return intersect_finite_values_by_sql_equality(left, right);
        };
        match ordering {
            Ordering::Less => left_index += 1,
            Ordering::Greater => right_index += 1,
            Ordering::Equal => {
                intersection.push(left[left_index].clone());
                left_index += 1;
                right_index += 1;
            }
        }
    }
    intersection
}

fn intersect_finite_values_by_sql_equality(left: &[Value], right: &[Value]) -> Vec<Value> {
    left.iter()
        .filter(|value| {
            right.iter().any(|candidate| {
                compare_finite_constraint_values(value, candidate) == Some(Ordering::Equal)
            })
        })
        .cloned()
        .collect()
}

fn compare_finite_constraint_values(left: &Value, right: &Value) -> Option<Ordering> {
    if values_identical(left, right) {
        Some(Ordering::Equal)
    } else {
        match (left, right) {
            // SQL equality remains false for a NULL/non-NULL pair, but a
            // deterministic order lets finite-set normalization establish
            // that the sets are disjoint without a panic.
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            _ => compare_values(left, right),
        }
    }
}

fn fallback_finite_constraint_order(left: &Value, right: &Value) -> Ordering {
    fn kind(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int64(_) => 2,
            Value::Float64(_) => 3,
            Value::Text(_) => 4,
            Value::Uuid(_) => 5,
            Value::Timestamp(_) => 6,
            Value::Json(_) => 7,
            Value::Vector(_) => 8,
            Value::TxId(_) => 9,
        }
    }

    kind(left)
        .cmp(&kind(right))
        .then_with(|| format!("{left:?}").cmp(&format!("{right:?}")))
}

#[cfg(feature = "test-seams")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct FinitePlannerValueOperations {
    comparisons: u64,
    equality_checks: u64,
}

#[cfg(feature = "test-seams")]
impl FinitePlannerValueOperations {
    #[doc(hidden)]
    pub fn comparisons(self) -> u64 {
        self.comparisons
    }

    #[doc(hidden)]
    pub fn equality_checks(self) -> u64 {
        self.equality_checks
    }

    #[doc(hidden)]
    pub fn total(self) -> u64 {
        self.comparisons.saturating_add(self.equality_checks)
    }
}

#[cfg(feature = "test-seams")]
#[derive(Clone, Copy)]
struct FinitePlannerOperationState {
    operations: FinitePlannerValueOperations,
    limit: Option<u64>,
}

#[cfg(feature = "test-seams")]
thread_local! {
    static FINITE_PLANNER_OPERATIONS: std::cell::Cell<FinitePlannerOperationState> =
        const { std::cell::Cell::new(FinitePlannerOperationState {
            operations: FinitePlannerValueOperations {
                comparisons: 0,
                equality_checks: 0,
            },
            limit: None,
        }) };
    static FINITE_PLANNER_OBSERVATION_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

#[cfg(feature = "test-seams")]
struct FinitePlannerObservationScope {
    previous_depth: u32,
}

#[cfg(feature = "test-seams")]
impl FinitePlannerObservationScope {
    fn enter() -> Self {
        let previous_depth = FINITE_PLANNER_OBSERVATION_DEPTH
            .with(|depth| depth.replace(depth.get().saturating_add(1)));
        Self { previous_depth }
    }
}

#[cfg(feature = "test-seams")]
impl Drop for FinitePlannerObservationScope {
    fn drop(&mut self) {
        FINITE_PLANNER_OBSERVATION_DEPTH.with(|depth| depth.set(self.previous_depth));
    }
}

/// A thread-local deterministic ceiling for finite-list planner value work.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub struct FinitePlannerOperationGuard {
    previous: FinitePlannerOperationState,
}

#[cfg(feature = "test-seams")]
impl FinitePlannerOperationGuard {
    #[doc(hidden)]
    pub fn operations(&self) -> FinitePlannerValueOperations {
        FINITE_PLANNER_OPERATIONS.with(|state| state.get().operations)
    }
}

#[cfg(feature = "test-seams")]
impl Drop for FinitePlannerOperationGuard {
    fn drop(&mut self) {
        FINITE_PLANNER_OPERATIONS.with(|state| state.set(self.previous));
    }
}

/// Refuse a test execution as soon as finite-list planning exceeds `limit`
/// shared value comparisons plus equality shortcuts. The count is active only
/// inside index analysis, so per-key execution work is measured separately by
/// the query trace. Production builds carry no counter or ceiling.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn finite_planner_operation_guard(limit: u64) -> FinitePlannerOperationGuard {
    let previous = FINITE_PLANNER_OPERATIONS.with(|state| {
        state.replace(FinitePlannerOperationState {
            operations: FinitePlannerValueOperations::default(),
            limit: Some(limit),
        })
    });
    FinitePlannerOperationGuard { previous }
}

/// Exercise the shared Rust-equality shortcut under planner observation.
/// This test-only door lets a negative control prove that work outside
/// `compare_finite_constraint_values` is still subject to the same ceiling.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn finite_planner_shared_equality_for_test(left: &Value, right: &Value) -> bool {
    let _observation = FinitePlannerObservationScope::enter();
    values_identical(left, right)
}

/// Execute one read through the eager executor implementation. This keeps
/// its shared index-planning behavior covered after public reads converged on
/// the bounded pull kernel.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn execute_eager_for_test(
    db: &Database,
    sql: &str,
    params: &HashMap<String, Value>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<QueryResult> {
    let statement = contextdb_parser::parse(sql)?;
    let plan = contextdb_planner::plan(&statement)?;
    validate_plan_columns(db, &plan)?;
    let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
    execute_plan(
        db,
        &plan,
        params,
        db.active_read_transaction(),
        &mut vector_copy_owner,
        read_memory.clone(),
    )
    .map(OwnedQueryResult::into_public)
}

#[cfg(feature = "test-seams")]
#[derive(Clone, Copy)]
enum FinitePlannerValueOperation {
    Comparison,
    EqualityCheck,
}

#[cfg(feature = "test-seams")]
fn observe_finite_planner_value_operation(operation: FinitePlannerValueOperation) {
    if FINITE_PLANNER_OBSERVATION_DEPTH.with(|depth| depth.get() == 0) {
        return;
    }
    FINITE_PLANNER_OPERATIONS.with(|state| {
        let mut current = state.get();
        let Some(limit) = current.limit else {
            return;
        };
        match operation {
            FinitePlannerValueOperation::Comparison => {
                current.operations.comparisons = current.operations.comparisons.saturating_add(1);
            }
            FinitePlannerValueOperation::EqualityCheck => {
                current.operations.equality_checks =
                    current.operations.equality_checks.saturating_add(1);
            }
        }
        state.set(current);
        assert!(
            current.operations.total() <= limit,
            "finite-list planning exceeded its deterministic value-operation ceiling of {limit}"
        );
    });
}

/// Whether this pick PINS the row it asks about: an equality on columns that
/// identify at most one row. This is the whole of the question when the pick
/// is all a reader has -- the pick's own shape says the predicate is an
/// equality, and the columns say what it is an equality on.
pub(crate) fn pick_names_one_row(
    db: &Database,
    table: &str,
    pick: &IndexPick,
) -> Option<Vec<String>> {
    if !matches!(pick.shape, IndexPredicateShape::Equality(_))
        || pick
            .suffix_shapes
            .iter()
            .any(|shape| !matches!(shape, IndexPredicateShape::Equality(_)))
        || pick.pushed_columns.len() != pick.columns.len()
    {
        return None;
    }
    let meta = db.table_meta(table)?;
    let pick_cols: Vec<&str> = pick.columns.iter().map(|(col, _)| col.as_str()).collect();
    unique_anchor_columns_for_pick(&meta, &pick_cols)
}

/// Whether a statement names at most one row by itself: its conjunctive filter binds every column
/// of a declared key -- a primary key of any width, a single-column `UNIQUE`, or a composite
/// `UNIQUE` -- with a single-value, non-NULL equality. This is decided from the statement and the
/// table's declared keys, never from the index a planner chose, so a hidden row the statement names
/// is refused, and any other hidden row is hidden, the same way on every index and every route
/// (docs/query-language.md, "Explicit-anchor `SELECT` shapes").
pub(crate) fn statement_names_one_row(
    meta: &TableMeta,
    filter: &Expr,
    params: &HashMap<String, Value>,
) -> bool {
    let mut bound = HashSet::new();
    for conjunct in split_conjuncts(filter) {
        if let Some((column, shape)) = classify_index_predicate(&conjunct, params)
            && shape_is_single_value_equality(&shape)
        {
            bound.insert(column);
        }
    }
    if bound.is_empty() {
        return false;
    }
    let covers = |columns: &[String]| {
        !columns.is_empty() && columns.iter().all(|column| bound.contains(column))
    };
    meta.columns
        .iter()
        .any(|column| (column.primary_key || column.unique) && bound.contains(&column.name))
        || covers(&meta.primary_key_columns)
        || meta.enforced_unique_tuples().any(|tuple| covers(tuple))
}

fn unique_anchor_columns_for_pick(meta: &TableMeta, pick_cols: &[&str]) -> Option<Vec<String>> {
    if pick_cols.len() == 1 {
        let column = meta
            .columns
            .iter()
            .find(|column| column.name == pick_cols[0])?;
        if column.primary_key || column.unique {
            return Some(vec![column.name.clone()]);
        }
    }
    for unique in &meta.unique_constraints {
        if unique.len() == pick_cols.len()
            && unique.iter().zip(pick_cols.iter()).all(|(a, b)| a == b)
        {
            return Some(unique.clone());
        }
    }
    None
}

/// Combine multiple index-shapes on the same column into the most-selective
/// composite form. Used by BETWEEN (which becomes `col >= X AND col <= Y`).
fn combine_shapes(mut shapes: Vec<IndexPredicateShape>) -> IndexPredicateShape {
    // Find the single best (most selective) shape.
    shapes.sort_by_key(|s| s.selectivity_tier());
    let head = shapes.remove(0);
    // If the head is a Range, try to merge subsequent Range conjuncts into it.
    if let IndexPredicateShape::Range {
        mut lower,
        mut upper,
    } = head.clone()
    {
        for s in shapes {
            if let IndexPredicateShape::Range { lower: l, upper: u } = s {
                // Merge lower: more restrictive is higher.
                lower = tighter_lower(&lower, &l);
                upper = tighter_upper(&upper, &u);
            }
        }
        return IndexPredicateShape::Range { lower, upper };
    }
    head
}

fn tighter_lower(a: &std::ops::Bound<Value>, b: &std::ops::Bound<Value>) -> std::ops::Bound<Value> {
    use std::ops::Bound;
    match (a, b) {
        (Bound::Unbounded, _) => b.clone(),
        (_, Bound::Unbounded) => a.clone(),
        (Bound::Included(va), Bound::Included(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Greater) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Excluded(va), Bound::Excluded(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Greater) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Included(va), Bound::Excluded(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Greater) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Excluded(va), Bound::Included(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Less) {
                b.clone()
            } else {
                a.clone()
            }
        }
    }
}

fn tighter_upper(a: &std::ops::Bound<Value>, b: &std::ops::Bound<Value>) -> std::ops::Bound<Value> {
    use std::ops::Bound;
    match (a, b) {
        (Bound::Unbounded, _) => b.clone(),
        (_, Bound::Unbounded) => a.clone(),
        (Bound::Included(va), Bound::Included(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Less) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Excluded(va), Bound::Excluded(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Less) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Included(va), Bound::Excluded(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Less) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Excluded(va), Bound::Included(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Greater) {
                b.clone()
            } else {
                a.clone()
            }
        }
    }
}

/// Split a boolean expression on top-level AND.
fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut out = split_conjuncts(left);
            out.extend(split_conjuncts(right));
            out
        }
        other => vec![other.clone()],
    }
}

/// Look at a predicate of the form `<col-ref> <op> <rhs>` where both sides are
/// simple. Return Some((column, shape)) if it's index-eligible, None otherwise.
fn classify_index_predicate(
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Option<(String, IndexPredicateShape)> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Equality is symmetric: `$v = column` is the same predicate as `column = $v`, so it
            // classifies, picks and names rows exactly the same way.
            let (left, right) = if *op == BinOp::Eq
                && extract_simple_col_ref(left).is_none()
                && extract_simple_col_ref(right).is_some()
            {
                (right, left)
            } else {
                (left, right)
            };
            let col = extract_simple_col_ref(left)?;
            // Reject function / arithmetic / column-ref RHS / subquery.
            if !is_literal_or_param(right) {
                return None;
            }
            let rhs = resolve_simple_rhs(right, params)?;
            let shape = match op {
                BinOp::Eq => IndexPredicateShape::Equality(rhs),
                BinOp::Neq => IndexPredicateShape::NotEqual(rhs),
                BinOp::Lt => IndexPredicateShape::Range {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Excluded(rhs),
                },
                BinOp::Lte => IndexPredicateShape::Range {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Included(rhs),
                },
                BinOp::Gt => IndexPredicateShape::Range {
                    lower: std::ops::Bound::Excluded(rhs),
                    upper: std::ops::Bound::Unbounded,
                },
                BinOp::Gte => IndexPredicateShape::Range {
                    lower: std::ops::Bound::Included(rhs),
                    upper: std::ops::Bound::Unbounded,
                },
                _ => return None,
            };
            Some((col, shape))
        }
        Expr::InList {
            expr: e,
            list,
            negated: false,
        } => {
            let col = extract_simple_col_ref(e)?;
            let mut values = Vec::with_capacity(list.len());
            for v in list {
                if !is_literal_or_param(v) {
                    return None;
                }
                values.push(resolve_simple_rhs(v, params)?);
            }
            Some((col, IndexPredicateShape::InList(Arc::new(values))))
        }
        Expr::IsNull { expr: e, negated } => {
            let col = extract_simple_col_ref(e)?;
            Some((
                col,
                if *negated {
                    IndexPredicateShape::IsNotNull
                } else {
                    IndexPredicateShape::IsNull
                },
            ))
        }
        _ => None,
    }
}

/// Classify why `filter` rejected `column` for IndexScan. Returns a static
/// reason string matching the plan's trace-reason vocabulary.
fn classify_rejection_reason(filter: &Expr, column: &str) -> &'static str {
    // Walk the expression tree and find a predicate mentioning `column`; report
    // the first structural reason we detect.
    fn walk(expr: &Expr, column: &str) -> Option<&'static str> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::And | BinOp::Or,
                right,
            } => walk(left, column).or_else(|| walk(right, column)),
            Expr::BinaryOp { left, op, right } => {
                // Detect arithmetic-on-column specifically (parser lowers
                // `a + 1` to FunctionCall { name: "__add", .. }).
                if expr_uses_arithmetic_on(left, column) || expr_uses_arithmetic_on(right, column) {
                    return Some("arithmetic in predicate");
                }
                // Generic function call (UPPER(col) etc.)
                if expr_uses_function_on(left, column) || expr_uses_function_on(right, column) {
                    return Some("function call in predicate");
                }
                // Column-ref RHS?
                if mentions_column_ref(left, column) || mentions_column_ref(right, column) {
                    let left_is_col = extract_simple_col_ref(left).as_deref() == Some(column);
                    let right_is_col_ref = matches!(right.as_ref(), Expr::Column(_));
                    if left_is_col && right_is_col_ref {
                        return Some("non-literal rhs");
                    }
                }
                let _ = op;
                None
            }
            Expr::Like { expr: e, .. } => {
                if mentions_column_ref(e, column) {
                    Some("LIKE is residual-only")
                } else {
                    None
                }
            }
            Expr::InSubquery { expr: e, .. } => {
                if mentions_column_ref(e, column) {
                    Some("non-literal rhs")
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    walk(filter, column).unwrap_or("first column not in WHERE")
}

fn extract_simple_col_ref(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(r) => Some(r.column.clone()),
        _ => None,
    }
}

fn is_literal_or_param(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::UnaryOp {
            op: UnaryOp::Neg,
            operand,
        } => is_literal_or_param(operand),
        Expr::FunctionCall { name, args } => {
            // Arithmetic-of-literals (e.g., `0.0 / 0.0`, `1 + 2`) counts as
            // a const RHS for planning purposes; we evaluate at execute time.
            matches!(name.as_str(), "__add" | "__sub" | "__mul" | "__div")
                && args.iter().all(is_literal_or_param)
        }
        _ => false,
    }
}

fn resolve_simple_rhs(expr: &Expr, params: &HashMap<String, Value>) -> Option<Value> {
    match expr {
        Expr::Literal(lit) => Some(match lit {
            Literal::Null => Value::Null,
            Literal::Bool(b) => Value::Bool(*b),
            Literal::Integer(i) => Value::Int64(*i),
            Literal::Real(f) => Value::Float64(*f),
            Literal::Text(s) => Value::Text(s.clone()),
            Literal::Vector(_) => return None,
        }),
        Expr::Parameter(name) => params.get(name).cloned(),
        Expr::UnaryOp {
            op: UnaryOp::Neg,
            operand,
        } => match resolve_simple_rhs(operand, params)? {
            Value::Int64(value) => value.checked_neg().map(Value::Int64),
            Value::Float64(value) => Some(Value::Float64(-value)),
            _ => None,
        },
        Expr::FunctionCall { name, args }
            if matches!(name.as_str(), "__add" | "__sub" | "__mul" | "__div") =>
        {
            if args.len() != 2 {
                return None;
            }
            let a = resolve_simple_rhs(&args[0], params)?;
            let b = resolve_simple_rhs(&args[1], params)?;
            match (a, b, name.as_str()) {
                (Value::Int64(x), Value::Int64(y), "__add") => {
                    // Use checked_add to avoid overflow. Return None on error, which causes
                    // the index predicate classification to fail and the predicate to
                    // fall back to residual filtering (same pattern as __div).
                    x.checked_add(y).map(Value::Int64)
                }
                (Value::Int64(x), Value::Int64(y), "__sub") => {
                    // Use checked_sub to avoid overflow. Return None on error, which causes
                    // the index predicate classification to fail and the predicate to
                    // fall back to residual filtering (same pattern as __div).
                    x.checked_sub(y).map(Value::Int64)
                }
                (Value::Int64(x), Value::Int64(y), "__mul") => {
                    // Use checked_mul to avoid overflow. Return None on error, which causes
                    // the index predicate classification to fail and the predicate to
                    // fall back to residual filtering (same pattern as __div).
                    x.checked_mul(y).map(Value::Int64)
                }
                (Value::Int64(x), Value::Int64(y), "__div") => {
                    // Use checked_div to avoid panic on overflow (e.g., i64::MIN / -1)
                    // or division by zero. Return None on error, which causes the
                    // index predicate classification to fail and the predicate to
                    // fall back to residual filtering.
                    x.checked_div(y).map(Value::Int64)
                }
                (Value::Float64(x), Value::Float64(y), "__add") => Some(Value::Float64(x + y)),
                (Value::Float64(x), Value::Float64(y), "__sub") => Some(Value::Float64(x - y)),
                (Value::Float64(x), Value::Float64(y), "__mul") => Some(Value::Float64(x * y)),
                (Value::Float64(x), Value::Float64(y), "__div") => {
                    // Avoid silently producing inf/NaN on division by zero; return None
                    // to cause the index predicate classification to fail.
                    if y == 0.0 {
                        None
                    } else {
                        Some(Value::Float64(x / y))
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn expr_uses_function_on(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::FunctionCall { name, args } => {
            // Skip known arithmetic-lowering function-call names; those are
            // classified separately as "arithmetic in predicate".
            if matches!(name.as_str(), "__add" | "__sub" | "__mul" | "__div") {
                return false;
            }
            args.iter().any(|a| mentions_column_ref(a, column))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_uses_function_on(left, column) || expr_uses_function_on(right, column)
        }
        _ => false,
    }
}

fn expr_uses_arithmetic_on(expr: &Expr, column: &str) -> bool {
    // The parser lowers `a + 1`, `a - 1`, etc. into FunctionCall with reserved
    // names `__add` / `__sub` / `__mul` / `__div`. We detect that shape here.
    match expr {
        Expr::FunctionCall { name, args } => {
            matches!(name.as_str(), "__add" | "__sub" | "__mul" | "__div")
                && args.iter().any(|a| mentions_column_ref(a, column))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_uses_arithmetic_on(left, column) || expr_uses_arithmetic_on(right, column)
        }
        _ => false,
    }
}

fn mentions_column_ref(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Column(r) => r.column == column,
        Expr::FunctionCall { args, .. } => args.iter().any(|a| mentions_column_ref(a, column)),
        Expr::BinaryOp { left, right, .. } => {
            mentions_column_ref(left, column) || mentions_column_ref(right, column)
        }
        Expr::UnaryOp { operand, .. } => mentions_column_ref(operand, column),
        Expr::IsNull { expr: e, .. } => mentions_column_ref(e, column),
        Expr::Like { expr: e, .. } => mentions_column_ref(e, column),
        Expr::InList { expr: e, .. } => mentions_column_ref(e, column),
        Expr::InSubquery { expr: e, .. } => mentions_column_ref(e, column),
        _ => false,
    }
}

/// Walk the index's B-tree per the picked shape, fetch matching rows by
/// row_id, apply residual filter, return VersionedRow list.
#[allow(clippy::too_many_arguments)]
/// The complete keys an exact-lookup index is probed with for this pick, or
/// nothing when the pick cannot be answered that way.
///
/// An index built for exact lookup has no order, so only a shape that names
/// WHOLE keys can use it -- an equality, an in-list, or a null test -- and
/// only when the pushed prefix covers every indexed column. A range, a
/// negation, or a partial prefix has to fall back to a scan. Both the
/// ordinary executor and the bounded kernel ask this one question, so the two
/// can never disagree about which index answers a query.
pub(crate) fn exact_probe_keys(indexed_columns: usize, pick: &IndexPick) -> Option<Vec<IndexKey>> {
    if pick.prefix_empty || 1 + pick.suffix_shapes.len() != indexed_columns {
        return None;
    }
    expanded_index_prefixes(pick).ok()
}

static NULL_INDEX_PROBE_VALUE: Value = Value::Null;

fn finite_shape_values(shape: &IndexPredicateShape) -> Option<&[Value]> {
    match shape {
        IndexPredicateShape::Equality(value) => Some(std::slice::from_ref(value)),
        IndexPredicateShape::InList(values) => Some(values.as_slice()),
        IndexPredicateShape::IsNull => Some(std::slice::from_ref(&NULL_INDEX_PROBE_VALUE)),
        IndexPredicateShape::NotEqual(_)
        | IndexPredicateShape::Range { .. }
        | IndexPredicateShape::IsNotNull => None,
    }
}

fn index_run_size_error(operation: &str) -> Error {
    Error::Other(format!("{operation} exceeds the native address space"))
}

fn checked_index_run_add(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| index_run_size_error(operation))
}

fn checked_index_run_mul(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_mul(right)
        .ok_or_else(|| index_run_size_error(operation))
}

fn finite_prefix_run_count(pick: &IndexPick) -> Result<Option<usize>> {
    if pick.prefix_empty {
        return Ok(Some(0));
    }
    let arity = pick.tuple_arity();
    let mut count = pick.tuple_prefix.as_ref().map_or(1, |tuples| tuples.len());
    for shape in std::iter::once(&pick.shape)
        .chain(&pick.suffix_shapes)
        .skip(arity)
    {
        let Some(values) = finite_shape_values(shape) else {
            return Ok(None);
        };
        count = checked_index_run_mul(count, values.len(), "index tuple-run count")?;
    }
    Ok(Some(count))
}

/// Bytes retained by every tuple run after the current run. This mirrors
/// `BoundedOrderedRowCursor::pending_run_bytes`: each pending pair owns both
/// key bounds, every directed value, and every cloned value payload.
pub(crate) fn planned_pending_run_bytes(pick: &IndexPick) -> Result<usize> {
    planned_index_run_bytes(pick, true)
}

/// Bytes needed while all tuple runs are constructed. The bounded cursor
/// consumes the first immediately and retains the rest; the eager executor
/// keeps this complete vector for the duration of its index walk.
pub(crate) fn planned_index_run_construction_bytes(pick: &IndexPick) -> Result<usize> {
    let runs = planned_index_run_bytes(pick, false)?;
    let prefix_len = 1 + pick.suffix_shapes.len();
    let shape_refs = checked_index_run_mul(
        prefix_len,
        std::mem::size_of::<&IndexPredicateShape>(),
        "index tuple-run workspace",
    )?;
    let directed_slots = checked_index_run_mul(
        prefix_len,
        std::mem::size_of::<DirectedValue>(),
        "index tuple-run workspace",
    )?;
    let arity = pick.tuple_arity();
    let mut largest_prefix = pick.tuple_prefix.as_ref().map_or(0, |tuples| {
        tuples
            .iter()
            .map(|tuple| tuple.iter().map(Value::estimated_bytes).sum::<usize>())
            .max()
            .unwrap_or(0)
    });
    for shape in std::iter::once(&pick.shape)
        .chain(&pick.suffix_shapes)
        .skip(arity)
    {
        let Some(values) = finite_shape_values(shape) else {
            return Ok(runs);
        };
        let largest = values.iter().map(Value::estimated_bytes).max().unwrap_or(0);
        largest_prefix =
            checked_index_run_add(largest_prefix, largest, "index tuple-run workspace")?;
    }
    [runs, shape_refs, directed_slots, largest_prefix]
        .into_iter()
        .try_fold(0usize, |bytes, part| {
            checked_index_run_add(bytes, part, "index tuple-run construction")
        })
}

fn planned_index_run_bytes(pick: &IndexPick, skip_first: bool) -> Result<usize> {
    use std::ops::Bound;
    let Some(run_count) = finite_prefix_run_count(pick)? else {
        return Ok(0);
    };
    if run_count == 0 || (skip_first && run_count == 1) {
        return Ok(0);
    }
    let retained_runs = run_count - usize::from(skip_first);
    let trailing_bytes = if let Some((lower, upper)) = &pick.trailing_range {
        let edge_bytes = |bound: &Bound<IndexKey>| -> Result<usize> {
            let (Bound::Included(key) | Bound::Excluded(key)) = bound else {
                return Ok(0);
            };
            key.iter().try_fold(
                checked_index_run_mul(
                    key.len(),
                    std::mem::size_of::<DirectedValue>(),
                    "index range edges",
                )?,
                |bytes, value| {
                    let value = match value {
                        DirectedValue::Asc(TotalOrdAsc(value))
                        | DirectedValue::Desc(TotalOrdDesc(value)) => value,
                    };
                    checked_index_run_add(bytes, value.estimated_bytes(), "index range edges")
                },
            )
        };
        checked_index_run_mul(
            retained_runs,
            checked_index_run_add(edge_bytes(lower)?, edge_bytes(upper)?, "index range edges")?,
            "index range edges",
        )?
    } else {
        0
    };
    let prefix_len = 1 + pick.suffix_shapes.len();
    let pad_len = if pick.trailing_range.is_none()
        && pick
            .columns
            .iter()
            .skip(prefix_len)
            .all(|(_, direction)| *direction == contextdb_core::SortDirection::Asc)
    {
        pick.columns.len().saturating_sub(prefix_len)
    } else {
        0
    };
    let pair_slots = checked_index_run_mul(
        retained_runs,
        std::mem::size_of::<(Bound<IndexKey>, Bound<IndexKey>)>(),
        "index tuple-run bounds",
    )?;
    let key_headers = checked_index_run_mul(
        retained_runs,
        checked_index_run_mul(2, std::mem::size_of::<IndexKey>(), "index tuple-run keys")?,
        "index tuple-run keys",
    )?;
    let directed_per_run = checked_index_run_add(
        checked_index_run_mul(prefix_len, 2, "index tuple-run directed values")?,
        pad_len,
        "index tuple-run directed values",
    )?;
    let directed_slots = checked_index_run_mul(
        retained_runs,
        checked_index_run_mul(
            directed_per_run,
            std::mem::size_of::<DirectedValue>(),
            "index tuple-run directed values",
        )?,
        "index tuple-run directed values",
    )?;
    let padding_values = checked_index_run_mul(
        retained_runs,
        checked_index_run_mul(
            pad_len,
            NULL_INDEX_PROBE_VALUE.estimated_bytes(),
            "index tuple-run padding",
        )?,
        "index tuple-run padding",
    )?;

    let mut all_prefix_value_bytes = 0usize;
    let mut first_prefix_value_bytes = 0usize;
    let arity = pick.tuple_arity();
    if let Some(tuples) = pick.tuple_prefix.as_ref() {
        let repetitions = run_count / tuples.len().max(1);
        let tuple_bytes = |tuple: &Vec<Value>| {
            tuple.iter().try_fold(0usize, |bytes, value| {
                checked_index_run_add(bytes, value.estimated_bytes(), "index tuple-run values")
            })
        };
        let listed_bytes = tuples.iter().try_fold(0usize, |bytes, tuple| {
            checked_index_run_add(bytes, tuple_bytes(tuple)?, "index tuple-run values")
        })?;
        all_prefix_value_bytes =
            checked_index_run_mul(listed_bytes, repetitions, "index tuple-run values")?;
        if skip_first && let Some(first) = tuples.first() {
            first_prefix_value_bytes = tuple_bytes(first)?;
        }
    }
    for shape in std::iter::once(&pick.shape)
        .chain(&pick.suffix_shapes)
        .skip(arity)
    {
        let values = finite_shape_values(shape).expect("finite run count checked every shape");
        let repetitions = run_count / values.len();
        let dimension_bytes = values.iter().try_fold(0usize, |bytes, value| {
            checked_index_run_add(bytes, value.estimated_bytes(), "index tuple-run values")
        })?;
        all_prefix_value_bytes = checked_index_run_add(
            all_prefix_value_bytes,
            checked_index_run_mul(dimension_bytes, repetitions, "index tuple-run values")?,
            "index tuple-run values",
        )?;
        if skip_first {
            first_prefix_value_bytes = checked_index_run_add(
                first_prefix_value_bytes,
                values[0].estimated_bytes(),
                "index tuple-run values",
            )?;
        }
    }
    let retained_prefix_value_bytes =
        all_prefix_value_bytes.saturating_sub(first_prefix_value_bytes);
    let both_edges =
        checked_index_run_mul(retained_prefix_value_bytes, 2, "index tuple-run values")?;

    [
        pair_slots,
        key_headers,
        directed_slots,
        padding_values,
        both_edges,
        trailing_bytes,
    ]
    .into_iter()
    .try_fold(0usize, |bytes, part| {
        checked_index_run_add(bytes, part, "index tuple-run bounds")
    })
}

fn visit_index_prefixes(
    pick: &IndexPick,
    mut visit: impl FnMut(&IndexKey) -> Result<()>,
) -> Result<()> {
    let Some(_) = finite_prefix_run_count(pick)? else {
        return Err(Error::Other(
            "non-finite predicate cannot form index tuple runs".to_string(),
        ));
    };
    let shapes = std::iter::once(&pick.shape)
        .chain(&pick.suffix_shapes)
        .collect::<Vec<_>>();
    let mut prefix = Vec::new();
    prefix
        .try_reserve_exact(shapes.len())
        .map_err(|_| Error::Other("index tuple-run key allocation failed".to_string()))?;
    let directed = |value: &Value, direction: contextdb_core::SortDirection| match direction {
        contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value.clone())),
        contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value.clone())),
    };

    fn expand<F>(
        dimension: usize,
        shapes: &[&IndexPredicateShape],
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &mut IndexKey,
        visit: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&IndexKey) -> Result<()>,
    {
        if dimension == shapes.len() {
            return visit(prefix);
        }
        let values = finite_shape_values(shapes[dimension])
            .expect("tuple-run expansion receives finite shapes");
        let direction = columns[dimension].1;
        for value in values {
            prefix.push(match direction {
                contextdb_core::SortDirection::Asc => {
                    DirectedValue::Asc(TotalOrdAsc(value.clone()))
                }
                contextdb_core::SortDirection::Desc => {
                    DirectedValue::Desc(TotalOrdDesc(value.clone()))
                }
            });
            expand(dimension + 1, shapes, columns, prefix, visit)?;
            prefix.pop();
        }
        Ok(())
    }
    // A row-value list names whole leading tuples: each is visited once and
    // only the columns after it expand, so k named pairs are k runs.
    if let Some(tuples) = pick.tuple_prefix.as_ref() {
        let arity = pick.tuple_arity();
        for tuple in tuples.iter() {
            prefix.clear();
            for (value, (_, direction)) in tuple.iter().zip(&pick.columns) {
                prefix.push(directed(value, *direction));
            }
            expand(arity, &shapes, &pick.columns, &mut prefix, &mut visit)?;
        }
        return Ok(());
    }
    expand(0, &shapes, &pick.columns, &mut prefix, &mut visit)
}

fn expanded_index_prefixes(pick: &IndexPick) -> Result<Vec<IndexKey>> {
    let run_count = finite_prefix_run_count(pick)?.unwrap_or(0);
    let mut prefixes = Vec::new();
    prefixes
        .try_reserve_exact(run_count)
        .map_err(|_| Error::Other("index tuple-run prefix allocation failed".to_string()))?;
    visit_index_prefixes(pick, |prefix| {
        prefixes.push(prefix.clone());
        Ok(())
    })?;
    Ok(prefixes)
}

/// Every finite equality/IN tuple prefix as one ordered run. Both eager and
/// bounded execution consume this representation, preserving identical run
/// order and residual semantics.
pub(crate) fn expanded_index_runs(
    pick: &IndexPick,
) -> Result<Vec<(std::ops::Bound<IndexKey>, std::ops::Bound<IndexKey>)>> {
    use std::ops::Bound;
    let run_count = finite_prefix_run_count(pick)?.unwrap_or(0);
    let mut runs = Vec::new();
    runs.try_reserve_exact(run_count)
        .map_err(|_| Error::Other("index tuple-run allocation failed".to_string()))?;
    visit_index_prefixes(pick, |prefix| {
        if let Some((lower, upper)) = &pick.trailing_range {
            let append = |bound: &Bound<IndexKey>| match bound {
                Bound::Included(edge) => {
                    Bound::Included(prefix.iter().chain(edge).cloned().collect())
                }
                Bound::Excluded(edge) => {
                    Bound::Excluded(prefix.iter().chain(edge).cloned().collect())
                }
                Bound::Unbounded => Bound::Unbounded,
            };
            runs.push((append(lower), append(upper)));
            return Ok(());
        }
        let mut reverse_seek_end = prefix.clone();
        if pick
            .columns
            .iter()
            .skip(prefix.len())
            .all(|(_, direction)| *direction == contextdb_core::SortDirection::Asc)
        {
            reverse_seek_end.extend(
                pick.columns
                    .iter()
                    .skip(prefix.len())
                    .map(|_| DirectedValue::Asc(TotalOrdAsc(Value::Null))),
            );
        }
        runs.push((
            Bound::Included(prefix.clone()),
            Bound::Included(reverse_seek_end),
        ));
        Ok(())
    })?;
    runs.sort_unstable_by(|(left, _), (right, _)| match (left, right) {
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => left.cmp(right),
        _ => unreachable!("finite tuple runs always have bounded starts"),
    });
    contextdb_core::find_attribution::count("index_runs_expanded", runs.len() as u64);
    Ok(runs)
}

#[allow(clippy::too_many_arguments)]
fn execute_index_scan(
    db: &Database,
    table: &str,
    pick: &IndexPick,
    snapshot: contextdb_core::SnapshotId,
    tx: Option<TxId>,
    access_mode: IndexScanAccessMode,
    materialized_columns: Option<&[String]>,
    residual_filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<(Vec<VersionedRow>, u64)> {
    use contextdb_core::{DirectedValue, SortDirection, TotalOrdAsc, TotalOrdDesc};
    use std::ops::Bound;

    if pick_matches_nothing(pick) {
        return Ok((Vec::new(), 0));
    }

    if db.writer_read_root()?.is_some() {
        return execute_writer_index_scan(
            db,
            table,
            pick,
            snapshot,
            tx,
            access_mode,
            materialized_columns,
            residual_filter,
            params,
            vector_copy_owner,
            read_memory.clone(),
        );
    }

    // A plan can outlive a concurrent DROP INDEX. Do not turn that race into
    // an empty result: there is no physical posting list to trust, so use the
    // normal visibility-aware table scan and let the caller's predicate path
    // apply its residual filter.
    let indexes = db.relational_store().indexes.read();
    if indexes
        .get(table)
        .and_then(|table_indexes| table_indexes.get(&pick.name))
        .is_none()
    {
        drop(indexes);
        let rows = scan_rows_for_select_statement(
            db,
            table,
            snapshot,
            tx,
            (access_mode == IndexScanAccessMode::Select)
                .then_some(residual_filter)
                .flatten(),
            params,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let rows_examined = rows.len() as u64;
        return Ok((rows, rows_examined));
    }
    drop(indexes);

    // Coerce pick.shape's literal values to the pushed column's declared type.
    // A SELECT WHERE uuid_col = 'uuid-string' arrives here with Text(..) even
    // though the indexed column stores Uuid(..). B-tree walks use variant-exact
    // comparisons (value_total_cmp panics on mismatched variants by design),
    // so we must match the stored key-type before walking. Coercion failure
    // (e.g. Text that is not a valid UUID) is treated as zero rows matched —
    // same semantics a full-scan predicate would produce.
    let pick = match coerce_pick_shape_to_column_type(db, table, pick) {
        Ok(coerced) => coerced,
        Err(_) => return Ok((Vec::new(), 0)),
    };
    let pick = &pick;
    if pick.prefix_empty {
        return Ok((Vec::new(), 0));
    }

    let indexes = db.relational_store().indexes.read();
    let storage = match indexes
        .get(table)
        .and_then(|table_indexes| table_indexes.get(&pick.name))
    {
        Some(s) => s,
        None => {
            drop(indexes);
            let rows = scan_rows_for_select_statement(
                db,
                table,
                snapshot,
                tx,
                (access_mode == IndexScanAccessMode::Select)
                    .then_some(residual_filter)
                    .flatten(),
                params,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let rows_examined = rows.len() as u64;
            return Ok((rows, rows_examined));
        }
    };
    let exact_only = storage.exact_only();

    let first_dir = pick
        .columns
        .first()
        .map(|(_, d)| *d)
        .unwrap_or(SortDirection::Asc);

    let wrap = |v: Value| -> DirectedValue {
        match first_dir {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v)),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v)),
        }
    };

    // Collect matching postings then filter by MVCC visibility.
    let mut postings: Vec<contextdb_relational::IndexEntry> = Vec::new();
    let mut rows_examined: u64 = 0;

    // `BTreeMap::range` panics if the supplied bound pair is inverted (start
    // > end) or degenerately empty (start == end with both ends excluded).
    // A normal query never constructs an inverted pair, but a NaN bound does:
    // the index's total order (`DirectedValue`/`value_total_cmp`, built on
    // `f64::total_cmp`) sorts the canonical NaN constant AFTER every other
    // value, so `NaN` as a lower bound can sort above a perfectly ordinary
    // upper bound. Rather than special-case NaN, guard the whole class: if
    // the bound pair can't form a non-empty range under this order, the
    // answer is the empty set (matching a non-indexed scan's IEEE-754
    // behavior, where every comparison against NaN is false), not a panic.
    let range_is_empty = |lower: &Bound<Vec<DirectedValue>>, upper: &Bound<Vec<DirectedValue>>| {
        let (lower_key, lower_excluded) = match lower {
            Bound::Included(v) => (Some(v), false),
            Bound::Excluded(v) => (Some(v), true),
            Bound::Unbounded => (None, false),
        };
        let (upper_key, upper_excluded) = match upper {
            Bound::Included(v) => (Some(v), false),
            Bound::Excluded(v) => (Some(v), true),
            Bound::Unbounded => (None, false),
        };
        match (lower_key, upper_key) {
            (Some(l), Some(u)) => match l.cmp(u) {
                Ordering::Greater => true,
                Ordering::Equal => lower_excluded || upper_excluded,
                Ordering::Less => false,
            },
            _ => false,
        }
    };

    let collect_range = |postings: &mut Vec<contextdb_relational::IndexEntry>,
                         examined: &mut u64,
                         lower: Bound<Vec<DirectedValue>>,
                         upper: Bound<Vec<DirectedValue>>| {
        if range_is_empty(&lower, &upper) {
            return;
        }
        for (_k, entries) in storage.read_range(
            snapshot,
            lower.as_ref().map(|key| key.as_slice()),
            upper.as_ref().map(|key| key.as_slice()),
        ) {
            for e in entries {
                *examined += 1;
                if e.visible_at(snapshot) {
                    postings.push(e.clone());
                }
            }
        }
    };

    let is_composite = pick.columns.len() > 1;
    let collect_prefix = |postings: &mut Vec<contextdb_relational::IndexEntry>,
                          examined: &mut u64,
                          prefix: &[DirectedValue]| {
        for (key, entries) in
            storage.read_range(snapshot, Bound::Included(prefix), Bound::Unbounded)
        {
            if !key.starts_with(prefix) {
                break;
            }
            for e in entries {
                *examined += 1;
                if e.visible_at(snapshot) {
                    postings.push(e.clone());
                }
            }
        }
    };
    if exact_only {
        // One rule for which keys an exact index is probed with, shared with
        // the bounded kernel so the two paths cannot disagree about which
        // index answers a query.
        let planned_runs = planned_index_run_construction_bytes(pick)?;
        let _run_memory = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            planned_runs,
            "query",
            "expand_index_tuple_runs",
            "Reduce the equality/IN tuple product or raise MEMORY_LIMIT.",
        )?;
        let Some(keys) = exact_probe_keys(storage.columns.len(), pick) else {
            drop(indexes);
            let rows = scan_rows_for_select_statement(
                db,
                table,
                snapshot,
                tx,
                (access_mode == IndexScanAccessMode::Select)
                    .then_some(residual_filter)
                    .flatten(),
                params,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let rows_examined = rows.len() as u64;
            return Ok((rows, rows_examined));
        };
        for key in keys {
            if let Some(entries) = storage.exact_postings(&key) {
                for e in entries {
                    rows_examined += 1;
                    if e.visible_at(snapshot) {
                        postings.push(e.clone());
                    }
                }
            }
        }
    } else {
        match &pick.shape {
            IndexPredicateShape::Equality(_)
            | IndexPredicateShape::InList(_)
            | IndexPredicateShape::IsNull => {
                let planned_runs = planned_index_run_construction_bytes(pick)?;
                let _run_memory = contextdb_core::read_memory::ReadCredit::try_new(
                    read_memory.clone(),
                    planned_runs,
                    "query",
                    "expand_index_tuple_runs",
                    "Reduce the equality/IN tuple product or raise MEMORY_LIMIT.",
                )?;
                for (start, end) in expanded_index_runs(pick)? {
                    if pick.trailing_range.is_some() {
                        collect_range(&mut postings, &mut rows_examined, start, end);
                        continue;
                    }
                    let Bound::Included(prefix) = start else {
                        unreachable!("finite tuple run always has an included prefix")
                    };
                    collect_prefix(&mut postings, &mut rows_examined, &prefix);
                }
            }
            IndexPredicateShape::Range { lower, upper } => {
                if is_composite {
                    // A DESCENDING leading column stores keys in the OPPOSITE
                    // physical order from a plain value comparison
                    // (`TotalOrdDesc::cmp` reverses `value_total_cmp`), so the
                    // SQL-lower bound wraps to the PHYSICALLY GREATER key and
                    // the SQL-upper bound wraps to the PHYSICALLY LESSER one.
                    // Swap which SQL bound plays the "physical lower" (walk
                    // start / `in_lower` test) vs "physical upper" (`in_upper`
                    // test / early-exit) role before using either -- the
                    // single-column arm below does the identical swap.
                    let (phys_lower, phys_upper) = if first_dir == SortDirection::Desc {
                        (upper, lower)
                    } else {
                        (lower, upper)
                    };
                    let lower_key = match phys_lower {
                        Bound::Included(v) => Bound::Included(vec![wrap(v.clone())]),
                        Bound::Excluded(v) => Bound::Excluded(vec![wrap(v.clone())]),
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    // Composite + range on the leading column cannot push suffix
                    // equalities; walk the ordered leading range and stop once
                    // the first component is beyond the upper bound.
                    for (key, entries) in storage.read_range(
                        snapshot,
                        lower_key.as_ref().map(|key| key.as_slice()),
                        Bound::Unbounded,
                    ) {
                        let Some(first) = key.first() else { continue };
                        let in_lower = match phys_lower {
                            Bound::Unbounded => true,
                            Bound::Included(v) => first >= &wrap(v.clone()),
                            Bound::Excluded(v) => first > &wrap(v.clone()),
                        };
                        if !in_lower {
                            continue;
                        }
                        let in_upper = match phys_upper {
                            Bound::Unbounded => true,
                            Bound::Included(v) => first <= &wrap(v.clone()),
                            Bound::Excluded(v) => first < &wrap(v.clone()),
                        };
                        if !in_upper {
                            break;
                        }
                        for e in entries {
                            rows_examined += 1;
                            if e.visible_at(snapshot) {
                                postings.push(e.clone());
                            }
                        }
                    }
                } else {
                    let l = match lower {
                        Bound::Included(v) => Bound::Included(vec![wrap(v.clone())]),
                        Bound::Excluded(v) => Bound::Excluded(vec![wrap(v.clone())]),
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    let u = match upper {
                        Bound::Included(v) => Bound::Included(vec![wrap(v.clone())]),
                        Bound::Excluded(v) => Bound::Excluded(vec![wrap(v.clone())]),
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    // Same physical-order inversion as the composite arm
                    // above: swap which wrapped bound plays the physically
                    // lesser vs greater role for a DESC leading column,
                    // BEFORE handing the pair to `collect_range` (and its
                    // `range_is_empty` guard) -- so an ordinary DESC-index
                    // range never looks inverted, and the guard still fires
                    // only on a genuinely inverted pair (the NaN class).
                    let (l, u) = if first_dir == SortDirection::Desc {
                        (u, l)
                    } else {
                        (l, u)
                    };
                    collect_range(&mut postings, &mut rows_examined, l, u);
                }
            }
            IndexPredicateShape::NotEqual(v) => {
                // Full walk; skip exact key. For IndexScan-trace we still attribute
                // all postings touched to __rows_examined (trace counts postings).
                let except_key = vec![wrap(v.clone())];
                for (k, entries) in storage.read_range(snapshot, Bound::Unbounded, Bound::Unbounded)
                {
                    if *k == except_key {
                        continue;
                    }
                    for e in entries {
                        rows_examined += 1;
                        if e.visible_at(snapshot) {
                            postings.push(e.clone());
                        }
                    }
                }
            }
            IndexPredicateShape::IsNotNull => {
                // Everything except NULL partition.
                let null_key = vec![wrap(Value::Null)];
                for (k, entries) in storage.read_range(snapshot, Bound::Unbounded, Bound::Unbounded)
                {
                    if *k == null_key {
                        continue;
                    }
                    for e in entries {
                        rows_examined += 1;
                        if e.visible_at(snapshot) {
                            postings.push(e.clone());
                        }
                    }
                }
            }
        }
    }

    // Now fetch base rows by row_id while preserving index-order. The index
    // already enumerates postings in index sort order; rows[] preserve it.
    let row_ids: Vec<RowId> = postings.iter().map(|p| p.row_id).collect();
    let mut out: Vec<VersionedRow> = Vec::with_capacity(row_ids.len());
    if !row_ids.is_empty() {
        for rid in &row_ids {
            if let Some(row) = db.committed_row_at(table, *rid, snapshot, read_memory.clone())? {
                out.push(row);
            }
        }
    }
    // Keep the physical-index read lock through the base-row fetch. A local
    // DDL publisher takes the matching write lock while swapping its prepared
    // rows, metadata and postings, so an old plan cannot join old postings to
    // a new row shape.
    drop(indexes);
    // Layer tx-scoped inserts / deletes on top, matching the semantics of
    // scan_with_tx.
    if let Some(tx_id) = tx {
        let overlay = db.index_scan_tx_overlay(
            tx_id,
            table,
            &pick.pushed_column,
            &pick.shape,
            read_memory.clone(),
        )?;
        let deleted_row_ids = overlay.deleted_row_ids;
        out.retain(|row| !deleted_row_ids.contains(&row.row_id));
        if exact_only {
            out.extend(overlay.matching_inserts);
        } else {
            out = merge_staged_rows_in_index_order(out, overlay.matching_inserts, &pick.columns);
        }
    }
    // An indexed posting identifies a row but is not its complete value. In a
    // retained file-backed store the point source supplies the long
    // relational body, while quantized vectors remain in their declared
    // vector store. Fill those exact-snapshot vector values before residual
    // predicates or an UPDATE replacement can observe a false NULL.
    db.supplement_selected_vector_columns(
        table,
        snapshot,
        tx,
        materialized_columns,
        &mut out,
        vector_copy_owner,
        read_memory.clone(),
    )?;
    let anchor_shape = access_mode == IndexScanAccessMode::Select
        && residual_filter.is_some_and(|filter| {
            db.table_meta(table)
                .is_some_and(|meta| statement_names_one_row(&meta, filter, params))
        });
    if anchor_shape {
        out = filter_rows_by_predicate(out, residual_filter, params)?;
        out =
            db.filter_rows_for_anchor_read_in_tx(tx, table, out, snapshot, read_memory.clone())?;
    } else {
        out = db.filter_rows_for_read_in_tx(tx, table, out, snapshot, read_memory.clone())?;
    }
    Ok((out, rows_examined))
}

#[allow(clippy::too_many_arguments)]
fn execute_writer_index_scan(
    db: &Database,
    table: &str,
    pick: &IndexPick,
    snapshot: contextdb_core::SnapshotId,
    tx: Option<TxId>,
    access_mode: IndexScanAccessMode,
    materialized_columns: Option<&[String]>,
    residual_filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<(Vec<VersionedRow>, u64)> {
    let pick = match coerce_pick_shape_to_column_type(db, table, pick) {
        Ok(pick) => pick,
        Err(_) => return Ok((Vec::new(), 0)),
    };
    if pick_matches_nothing(&pick) {
        return Ok((Vec::new(), 0));
    }
    let Some(meta) = db.table_meta(table) else {
        return Err(Error::TableNotFound(table.into()));
    };
    if !meta.indexes.iter().any(|decl| decl.name == pick.name) {
        let rows = scan_rows_for_select_statement(
            db,
            table,
            snapshot,
            tx,
            (access_mode == IndexScanAccessMode::Select)
                .then_some(residual_filter)
                .flatten(),
            params,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let examined = rows.len() as u64;
        return Ok((rows, examined));
    }
    let (column, direction) = pick
        .columns
        .first()
        .cloned()
        .ok_or_else(|| Error::Other("index has no leading column".into()))?;
    let mut admission = contextdb_core::read_memory::ReadCredit::try_new(
        read_memory.clone(),
        planned_index_run_construction_bytes(&pick)?,
        "query",
        "eager_index_output",
        "Reduce query output or raise MEMORY_LIMIT.",
    )?;
    let cursor =
        contextdb_relational::mem::BoundedOrderedRowCursor::for_index(pick.name.clone(), false);
    let mut cursor = if matches!(
        pick.shape,
        IndexPredicateShape::Equality(_)
            | IndexPredicateShape::InList(_)
            | IndexPredicateShape::IsNull
    ) {
        cursor.seeking_runs(expanded_index_runs(&pick)?)
    } else {
        let (lower, upper, prefix) = bounded::declared_index_run(&pick, direction, false);
        cursor.seeking_run_with_prefix_len(lower, upper, prefix)
    };
    cursor.source_configure(pick.columns.len());
    let mut examined = 0u64;
    let mut out = Vec::new();
    while let Some(row) = db.bounded_ordered_table_row_next(
        read_memory.clone(),
        table,
        &column,
        direction,
        snapshot,
        &mut cursor,
        || Ok::<_, Error>(()),
        || {
            examined = examined.saturating_add(1);
            Ok::<_, Error>(())
        },
        |bytes| {
            bytes.retain_into(
                &mut admission,
                "query",
                "eager_index_output",
                "Reduce query output or raise MEMORY_LIMIT.",
            )?;
            admission.try_grow_for(
                std::mem::size_of::<VersionedRow>() * 4,
                "query",
                "eager_index_output",
                "Reduce query output or raise MEMORY_LIMIT.",
            )
        },
    )? {
        out.push(row);
    }
    if let Some(tx) = tx {
        let overlay = db.index_scan_tx_overlay(
            tx,
            table,
            &pick.pushed_column,
            &pick.shape,
            read_memory.clone(),
        )?;
        out.retain(|row| !overlay.deleted_row_ids.contains(&row.row_id));
        out = merge_staged_rows_in_index_order(out, overlay.matching_inserts, &pick.columns);
    }
    db.supplement_selected_vector_columns(
        table,
        snapshot,
        tx,
        materialized_columns,
        &mut out,
        vector_copy_owner,
        read_memory.clone(),
    )?;
    let anchor = access_mode == IndexScanAccessMode::Select
        && residual_filter.is_some_and(|filter| {
            db.table_meta(table)
                .is_some_and(|meta| statement_names_one_row(&meta, filter, params))
        });
    if anchor {
        out = filter_rows_by_predicate(out, residual_filter, params)?;
        out =
            db.filter_rows_for_anchor_read_in_tx(tx, table, out, snapshot, read_memory.clone())?;
    } else {
        out = db.filter_rows_for_read_in_tx(tx, table, out, snapshot, read_memory.clone())?;
    }
    db.retain_writer_output(admission);
    Ok((out, examined))
}

fn compare_rows_by_index_columns(
    left: &VersionedRow,
    right: &VersionedRow,
    columns: &[(String, contextdb_core::SortDirection)],
) -> Ordering {
    for (column, direction) in columns {
        let left = left.values.get(column).cloned().unwrap_or(Value::Null);
        let right = right.values.get(column).cloned().unwrap_or(Value::Null);
        let ordering = match direction {
            contextdb_core::SortDirection::Asc => {
                DirectedValue::Asc(TotalOrdAsc(left)).cmp(&DirectedValue::Asc(TotalOrdAsc(right)))
            }
            contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(left))
                .cmp(&DirectedValue::Desc(TotalOrdDesc(right))),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn merge_staged_rows_in_index_order(
    committed: Vec<VersionedRow>,
    mut staged: Vec<VersionedRow>,
    columns: &[(String, contextdb_core::SortDirection)],
) -> Vec<VersionedRow> {
    #[cfg(feature = "in-process-test-seams")]
    crate::writer_query_test_seams::note_staged_sort(staged.len());
    staged.sort_unstable_by(|left, right| compare_rows_by_index_columns(left, right, columns));
    let mut committed = committed.into_iter().peekable();
    let mut staged = staged.into_iter().peekable();
    let mut merged = Vec::with_capacity(committed.len().saturating_add(staged.len()));
    while let (Some(committed_row), Some(staged_row)) = (committed.peek(), staged.peek()) {
        if compare_rows_by_index_columns(staged_row, committed_row, columns).is_lt() {
            merged.push(staged.next().expect("peeked staged index row"));
        } else {
            merged.push(committed.next().expect("peeked committed index row"));
        }
    }
    merged.extend(committed);
    merged.extend(staged);
    merged
}

pub(crate) fn range_includes(
    v: &Value,
    lower: &std::ops::Bound<Value>,
    upper: &std::ops::Bound<Value>,
) -> bool {
    use std::ops::Bound;
    let ok_lower = match lower {
        Bound::Unbounded => true,
        Bound::Included(b) => compare_values(v, b).is_some_and(|o| o != std::cmp::Ordering::Less),
        Bound::Excluded(b) => {
            compare_values(v, b).is_some_and(|o| o == std::cmp::Ordering::Greater)
        }
    };
    let ok_upper = match upper {
        Bound::Unbounded => true,
        Bound::Included(b) => {
            compare_values(v, b).is_some_and(|o| o != std::cmp::Ordering::Greater)
        }
        Bound::Excluded(b) => compare_values(v, b).is_some_and(|o| o == std::cmp::Ordering::Less),
    };
    ok_lower && ok_upper
}

/// Try to elide the `Sort` node when the child's Scan can be rewritten as an
/// IndexScan whose ordering matches `keys`. The common case with no WHERE
/// filter uses a full-range index walk. If the Scan has a WHERE filter that
/// does NOT match this specific index's first column, we refuse to elide
/// (the Scan arm will still pick the best-matching index for the filter and
/// the Sort arm's path-B check handles the elision).
fn try_elide_sort(
    db: &Database,
    input: &PhysicalPlan,
    keys: &[contextdb_planner::SortKey],
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    materialized_columns: Option<&[String]>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<OwnedQueryResult>> {
    fn find_scan(plan: &PhysicalPlan) -> Option<(&String, &Option<String>, &Option<Expr>)> {
        match plan {
            PhysicalPlan::Scan {
                table,
                alias,
                filter,
            } => Some((table, alias, filter)),
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Limit { input, .. } => find_scan(input),
            _ => None,
        }
    }
    let Some((table, _alias, filter)) = find_scan(input) else {
        return Ok(None);
    };
    let Some(matching) = sort_elision_index_decl(db, input, keys) else {
        return Ok(None);
    };
    run_index_scan_with_order(
        db,
        table,
        &matching,
        filter.as_ref(),
        params,
        tx,
        materialized_columns,
        vector_copy_owner,
        read_memory.clone(),
    )
}

/// The index whose own order lets an ORDER BY be answered without sorting
/// anything, for a scan that carries no predicate.
///
/// One rule, asked by both doors. A read that decides this for itself ends up
/// sorting where the other door does not, or claiming an order the rows it
/// returns are not in.
pub(crate) fn sort_elision_index_decl(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    keys: &[contextdb_planner::SortKey],
) -> Option<contextdb_core::IndexDecl> {
    sort_elision_index_decl_with_capability(db, input, keys, false)
}

/// [`sort_elision_index_decl`] for the bounded ordered route, which walks a
/// durable store's single-column automatic index as an ordered tree.
pub(crate) fn bounded_sort_elision_index_decl(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    keys: &[contextdb_planner::SortKey],
) -> Option<contextdb_core::IndexDecl> {
    sort_elision_index_decl_with_capability(db, input, keys, db.automatic_indexes_are_ordered())
}

fn sort_elision_index_decl_with_capability(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    keys: &[contextdb_planner::SortKey],
    automatic_ordered: bool,
) -> Option<contextdb_core::IndexDecl> {
    fn find_scan(plan: &PhysicalPlan) -> Option<(&String, &Option<Expr>)> {
        match plan {
            PhysicalPlan::Scan { table, filter, .. } => Some((table, filter)),
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Limit { input, .. } => find_scan(input),
            _ => None,
        }
    }
    let (table, filter) = find_scan(input)?;
    // A scan that carries a WHERE reads one run of the index its predicate
    // names, and whether THAT order answers the sort is a different question,
    // asked by `sort_keys_match_index_prefix` once the run is known.
    if filter.is_some() {
        return None;
    }
    // Keys must all be simple column references.
    let key_cols: Vec<(&str, &contextdb_parser::ast::SortDirection)> = keys
        .iter()
        .map(|k| match &k.expr {
            Expr::Column(r) => Some((r.column.as_str(), &k.direction)),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    let meta = db.table_meta(table)?;
    meta.indexes
        .iter()
        .find(|decl| {
            if automatic_index_is_exact_only(decl, automatic_ordered) {
                return false;
            }
            if decl.columns.len() < key_cols.len() {
                return false;
            }
            decl.columns
                .iter()
                .zip(key_cols.iter())
                .all(|((col, dir), (kcol, kdir))| col == kcol && core_dir_matches_ast(*dir, **kdir))
        })
        .cloned()
}

/// Execute an IndexScan over `table` with `index`, applying the optional
/// residual filter. Constructs predicates_pushed / indexes_considered the
/// same way the Scan arm does.
fn run_index_scan_with_order(
    db: &Database,
    table: &str,
    decl: &contextdb_core::IndexDecl,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    materialized_columns: Option<&[String]>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<OwnedQueryResult>> {
    use std::borrow::Cow;
    let snapshot = db.snapshot_for_read();
    let schema_columns = db.table_meta(table).map(|meta| {
        meta.columns
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>()
    });
    let resolved_filter = filter
        .map(|expr| {
            resolve_in_subqueries(db, expr, params, tx, vector_copy_owner, read_memory.clone())
        })
        .transpose()?;
    let ordered_materialized_columns =
        merge_expression_columns(materialized_columns, resolved_filter.iter());

    // Pick: full-range walk for ORDER BY elision. Shape is "unbounded range"
    // so we walk every posting. Residual filter applies.
    let pick = IndexPick {
        name: decl.name.clone(),
        columns: decl.columns.clone(),
        shape: IndexPredicateShape::Range {
            lower: std::ops::Bound::Unbounded,
            upper: std::ops::Bound::Unbounded,
        },
        pushed_column: decl.columns[0].0.clone(),
        pushed_columns: vec![decl.columns[0].0.clone()],
        suffix_shapes: Vec::new(),
        trailing_range: None,
        tuple_prefix: None,
        prefix_empty: false,
        probe_values_normalized: false,
    };
    let (rows, examined) = execute_index_scan(
        db,
        table,
        &pick,
        snapshot,
        tx,
        IndexScanAccessMode::Select,
        ordered_materialized_columns.as_deref(),
        resolved_filter.as_ref(),
        params,
        vector_copy_owner,
        read_memory.clone(),
    )?;
    db.__bump_rows_examined(examined);
    let mut result = materialize_rows(
        rows,
        resolved_filter.as_ref(),
        params,
        schema_columns.as_deref(),
    )?;
    let mut pushed: smallvec::SmallVec<[Cow<'static, str>; 4]> = smallvec::SmallVec::new();
    pushed.push(Cow::Owned(decl.columns[0].0.clone()));
    result.trace = crate::database::QueryTrace {
        physical_plan: "IndexScan",
        index_used: Some(decl.name.clone()),
        predicates_pushed: pushed,
        indexes_considered: Default::default(),
        sort_elided: true,
        query_vector_source: None,
        vector_search: None,
        rows_examined: 0,
        maintained_count: None,
        groups: Box::new([]),
    };
    Ok(Some(result))
}

/// Whether this predicate can match no row at all, so the answer is zero rows
/// having read none.
///
/// `col = NaN` never matches -- NaN equals nothing, itself included -- and
/// `col = NULL` is UNKNOWN rather than true, so neither reaches the store.
/// Both doors ask this one function; a read that decides it for itself returns
/// a row the other door excludes.
pub(crate) fn pick_matches_nothing(pick: &IndexPick) -> bool {
    match &pick.shape {
        IndexPredicateShape::Equality(Value::Float64(f)) if f.is_nan() => true,
        IndexPredicateShape::Equality(Value::Null) => true,
        _ => pick.prefix_empty,
    }
}

pub(crate) fn sort_keys_match_index_prefix(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    index_name: &str,
    keys: &[contextdb_planner::SortKey],
) -> bool {
    sort_keys_match_index_prefix_with_direction(db, input, index_name, keys, false, false)
}

/// Whether the selected index supplies the requested order when a bounded
/// cursor may walk the index in either physical direction.
pub(crate) fn sort_keys_match_index_prefix_in_either_direction(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    index_name: &str,
    keys: &[contextdb_planner::SortKey],
) -> bool {
    sort_keys_match_index_prefix_with_direction(
        db,
        input,
        index_name,
        keys,
        true,
        db.automatic_indexes_are_ordered(),
    )
}

fn sort_keys_match_index_prefix_with_direction(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    index_name: &str,
    keys: &[contextdb_planner::SortKey],
    allow_reverse: bool,
    automatic_ordered: bool,
) -> bool {
    fn find_scan_and_filter(plan: &PhysicalPlan) -> Option<(&String, &Option<Expr>)> {
        match plan {
            PhysicalPlan::Scan { table, filter, .. } => Some((table, filter)),
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Limit { input, .. } => find_scan_and_filter(input),
            _ => None,
        }
    }
    let (table, filter) = match find_scan_and_filter(input) {
        Some(t) => t,
        None => return false,
    };
    let meta = match db.table_meta(table) {
        Some(m) => m,
        None => return false,
    };
    let decl = meta.indexes.iter().find(|i| i.name == index_name);
    let Some(decl) = decl else {
        return false;
    };
    if automatic_index_is_exact_only(decl, automatic_ordered) {
        return false;
    }
    // Shape guard: IndexScan with InList or NotEqual shape on the leading
    // indexed column walks fragmented posting-list ranges, so rows are
    // emitted per-value, not globally sorted. Refuse sort elision for those
    // shapes even when values are bound parameters.
    if let Some(filter_expr) = filter.as_ref()
        && let Some(leading_col) = decl.columns.first().map(|(c, _)| c.as_str())
        && leading_filter_has_fragmented_order_shape(filter_expr, leading_col)
    {
        return false;
    }
    // Determine how many leading index columns the WHERE filter pins to a
    // single equality. Those columns are effectively "used up" by the
    // IndexScan's range; subsequent ORDER BY keys matching the remaining
    // index columns still elide the Sort.
    let pinned_prefix_len = count_equality_prefix(filter.as_ref(), &decl.columns);
    if declared_columns_answer_sort_keys(&decl.columns, keys, allow_reverse) {
        return true;
    }
    let remaining_index_cols = &decl.columns[pinned_prefix_len..];
    declared_columns_answer_sort_keys(remaining_index_cols, keys, allow_reverse)
}

/// Whether `columns` (an index's full declared order, or the remainder past
/// an equality-pinned prefix) answers `sort_keys` -- forward always, and
/// backward too when `allow_reverse` says a bounded cursor may walk this
/// index in either physical direction. The one rule both
/// [`sort_keys_match_index_prefix_with_direction`] (an already-chosen index)
/// and [`candidate_elides_sort`] (a candidate still being weighed) ask, so a
/// read that decides this for itself can never disagree with the selection
/// that chose its index.
fn declared_columns_answer_sort_keys(
    columns: &[(String, contextdb_core::SortDirection)],
    sort_keys: &[contextdb_planner::SortKey],
    allow_reverse: bool,
) -> bool {
    let same_columns = columns.len() >= sort_keys.len()
        && columns
            .iter()
            .zip(sort_keys.iter())
            .all(|((col, _dir), key)| match &key.expr {
                Expr::Column(r) => r.column == *col,
                _ => false,
            });
    same_columns
        && (columns
            .iter()
            .zip(sort_keys.iter())
            .all(|((_, dir), key)| core_dir_matches_ast(*dir, key.direction))
            || (allow_reverse
                && columns.iter().zip(sort_keys.iter()).all(|((_, dir), key)| {
                    matches!(
                        (*dir, key.direction),
                        (
                            contextdb_core::SortDirection::Asc,
                            contextdb_parser::ast::SortDirection::Desc,
                        ) | (
                            contextdb_core::SortDirection::Desc,
                            contextdb_parser::ast::SortDirection::Asc,
                        )
                    )
                })))
}

/// Single-column automatic indexes back point probes only in a memory store.
/// Composite automatic indexes have their ordered tree and can supply the same
/// prefix ordering as an equivalent declared index; so does every automatic
/// index of a durable store, whose postings live in one ordered family, when
/// the calling route can walk it (`automatic_ordered`).
fn automatic_index_is_exact_only(
    decl: &contextdb_core::IndexDecl,
    automatic_ordered: bool,
) -> bool {
    !automatic_ordered && decl.is_exact_only()
}

fn leading_filter_has_fragmented_order_shape(filter: &Expr, leading_col: &str) -> bool {
    split_conjuncts(filter)
        .iter()
        .any(|conjunct| match conjunct {
            Expr::InList {
                expr,
                negated: false,
                ..
            } => extract_simple_col_ref(expr).as_deref() == Some(leading_col),
            Expr::BinaryOp {
                left,
                op: BinOp::Neq,
                ..
            } => extract_simple_col_ref(left).as_deref() == Some(leading_col),
            _ => false,
        })
}

fn count_equality_prefix(
    filter: Option<&Expr>,
    columns: &[(String, contextdb_core::SortDirection)],
) -> usize {
    let Some(filter) = filter else {
        return 0;
    };
    let conjuncts = split_conjuncts(filter);
    let mut pinned = 0usize;
    for (col, _) in columns {
        let has_eq = conjuncts.iter().any(|c| match c {
            Expr::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } => {
                let left_is_col = matches!(left.as_ref(), Expr::Column(r) if r.column == *col);
                let right_is_simple =
                    matches!(right.as_ref(), Expr::Literal(_) | Expr::Parameter(_));
                left_is_col && right_is_simple
            }
            Expr::IsNull {
                expr,
                negated: false,
            } => matches!(expr.as_ref(), Expr::Column(reference) if reference.column == *col),
            _ => false,
        });
        if has_eq {
            pinned += 1;
        } else {
            break;
        }
    }
    pinned
}

fn core_dir_matches_ast(
    core: contextdb_core::SortDirection,
    ast: contextdb_parser::ast::SortDirection,
) -> bool {
    matches!(
        (core, ast),
        (
            contextdb_core::SortDirection::Asc,
            contextdb_parser::ast::SortDirection::Asc
        ) | (
            contextdb_core::SortDirection::Desc,
            contextdb_parser::ast::SortDirection::Desc
        )
    )
}

// ========================= End of index scan planning =========================

fn validate_update_state_transition(
    db: &Database,
    table: &str,
    existing: &VersionedRow,
    next_values: &HashMap<String, Value>,
) -> Result<()> {
    let Some(meta) = db.table_meta(table) else {
        return Ok(());
    };
    let Some(state_machine) = meta.state_machine else {
        return Ok(());
    };

    let old_state = existing
        .values
        .get(&state_machine.column)
        .and_then(Value::as_text);
    let new_state = next_values
        .get(&state_machine.column)
        .and_then(Value::as_text);

    let (Some(old_state), Some(new_state)) = (old_state, new_state) else {
        return Ok(());
    };

    if db.relational_store().validate_state_transition(
        table,
        &state_machine.column,
        old_state,
        new_state,
    ) {
        return Ok(());
    }

    Err(Error::InvalidStateTransition(format!(
        "{old_state} -> {new_state}"
    )))
}

pub(crate) fn estimate_vector_search_bytes(dimension: usize, k: usize) -> usize {
    k.saturating_mul(3)
        .saturating_mul(dimension)
        .saturating_mul(std::mem::size_of::<f32>())
}

pub(crate) fn estimate_drop_table_bytes(
    db: &Database,
    table: &str,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<usize> {
    let meta = db.table_meta(table);
    let metadata_bytes = meta
        .as_ref()
        .map(|meta| crate::database::retained_table_metadata_bytes(db.relational_store(), meta))
        .unwrap_or(0);
    let snapshot = db.snapshot();
    // Only a live version still owns a charge: a superseded or deleted
    // version handed its bytes back at the commit that stamped its
    // `deleted_tx`, so releasing it again here would credit the same bytes
    // twice and leave the ceiling below what the surviving tables still
    // hold. This fold mirrors the live-set charge exactly (the same
    // `deleted_tx.is_none()` filter and estimate `account_loaded_state`
    // uses), so dropping a table gives back exactly what holding it took.
    let row_bytes = db
        .relational_store()
        .tables
        .read()
        .get(table)
        .map_or(0usize, |versions| {
            versions
                .iter()
                .filter(|row| row.deleted_tx.is_none())
                .fold(0usize, |acc, row| {
                    acc.saturating_add(crate::database::retained_row_bytes_with_optional_meta(
                        table,
                        row,
                        meta.as_ref(),
                    ))
                })
        });
    let rows = db.scan_with_read_memory(table, snapshot, vector_copy_owner, read_memory.clone())?;
    let edge_bytes = if meta.as_ref().is_some_and(has_edge_columns) {
        rows.iter().fold(0usize, |acc, row| {
            match (
                row.values.get("source_id").and_then(Value::as_uuid),
                row.values.get("target_id").and_then(Value::as_uuid),
                row.values.get("edge_type").and_then(Value::as_text),
            ) {
                // Released through the same estimate the insert charged, so
                // dropping a table gives back exactly what holding it took.
                (Some(source), Some(target), Some(edge_type)) => acc.saturating_add(
                    AdjEntry {
                        source: *source,
                        target: *target,
                        edge_type: edge_type.to_string(),
                        properties: HashMap::new(),
                        created_tx: TxId(0),
                        deleted_tx: None,
                        lsn: Lsn(0),
                    }
                    .estimated_bytes(),
                ),
                _ => acc,
            }
        })
    } else {
        0
    };
    Ok(metadata_bytes
        .saturating_add(row_bytes)
        .saturating_add(edge_bytes))
}

fn materialize_rows(
    rows: Vec<VersionedRow>,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    schema_columns: Option<&[String]>,
) -> Result<OwnedQueryResult> {
    let filtered = filter_rows_by_predicate(rows, filter, params)?;

    let keys = if let Some(schema_columns) = schema_columns {
        schema_columns.to_vec()
    } else {
        let mut keys = BTreeSet::new();
        for r in &filtered {
            for k in r.values.keys() {
                keys.insert(k.clone());
            }
        }
        keys.into_iter().collect::<Vec<_>>()
    };

    let mut columns = vec!["row_id".to_string()];
    columns.extend(keys.iter().cloned());

    let rows = filtered
        .into_iter()
        .map(|r| {
            let mut out = vec![Value::Int64(r.row_id.0 as i64)];
            for k in &keys {
                out.push(r.values.get(k).cloned().unwrap_or(Value::Null));
            }
            out
        })
        .collect();

    Ok(OwnedQueryResult::new(QueryResult {
        columns,
        rows,
        rows_affected: 0,
        trace: crate::database::QueryTrace::scan(),
        cascade: None,
    }))
}

fn scan_rows_for_select(
    db: &Database,
    table: &str,
    snapshot: SnapshotId,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<VersionedRow>> {
    let mut rows = if let Some(tx) = tx {
        let rows =
            db.scan_in_tx_raw(tx, table, snapshot, vector_copy_owner, read_memory.clone())?;
        db.filter_rows_for_read_in_tx(Some(tx), table, rows, snapshot, read_memory.clone())?
    } else {
        db.scan_with_read_memory(table, snapshot, vector_copy_owner, read_memory.clone())?
    };
    db.supplement_quantized_vectors(
        table,
        snapshot,
        &mut rows,
        vector_copy_owner,
        read_memory.clone(),
    )?;
    Ok(rows)
}

/// The table-scan answer for a statement whose index vanished under it. A statement that names one
/// row is served as it would be on its index: the complete predicate first, then the gate that
/// refuses a hidden row it names. Any other statement scans with the gate that hides.
#[allow(clippy::too_many_arguments)]
fn scan_rows_for_select_statement(
    db: &Database,
    table: &str,
    snapshot: SnapshotId,
    tx: Option<TxId>,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<VersionedRow>> {
    let Some(filter) = filter.filter(|filter| {
        db.table_meta(table)
            .is_some_and(|meta| statement_names_one_row(&meta, filter, params))
    }) else {
        return scan_rows_for_select(db, table, snapshot, tx, vector_copy_owner, read_memory);
    };
    let rows = db.scan_rows_ungated_for_select(
        tx,
        table,
        snapshot,
        vector_copy_owner,
        read_memory.clone(),
    )?;
    let rows = filter_rows_by_predicate(rows, Some(filter), params)?;
    db.filter_rows_for_anchor_read_in_tx(tx, table, rows, snapshot, read_memory)
}

fn rows_by_row_id(
    db: &Database,
    table: &str,
    row_ids: &[RowId],
    snapshot: SnapshotId,
    tx: Option<TxId>,
    materialized_columns: Option<&[String]>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<VersionedRow>> {
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::with_capacity(row_ids.len());
    for row_id in row_ids {
        let row = db.find_row_by_id_in_tx(tx, table, *row_id, snapshot, read_memory.clone())?;
        rows.push(row);
    }
    db.supplement_selected_vector_columns(
        table,
        snapshot,
        tx,
        materialized_columns,
        &mut rows,
        vector_copy_owner,
        read_memory.clone(),
    )?;
    Ok(rows)
}

fn uuid_to_row_id_map(
    db: &Database,
    table: &str,
    snapshot: SnapshotId,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<HashMap<uuid::Uuid, RowId>> {
    let rows = scan_rows_for_select(
        db,
        table,
        snapshot,
        tx,
        vector_copy_owner,
        read_memory.clone(),
    )?;
    Ok(rows
        .iter()
        .filter_map(|row| match row.values.get("id") {
            Some(Value::Uuid(uuid)) => Some((*uuid, row.row_id)),
            _ => None,
        })
        .collect())
}

fn vector_search_trace(operator: &'static str, candidate_trace: Option<QueryTrace>) -> QueryTrace {
    let Some(mut trace) = candidate_trace else {
        return QueryTrace {
            physical_plan: operator,
            ..Default::default()
        };
    };

    trace.physical_plan = match (trace.physical_plan, operator) {
        ("IndexScan", "HNSWSearch") => "IndexScan -> HNSWSearch",
        ("IndexScan", _) => "IndexScan -> VectorSearch",
        ("Scan", "HNSWSearch") => "Scan -> HNSWSearch",
        ("Scan", _) => "Scan -> VectorSearch",
        (_, "HNSWSearch") => "HNSWSearch",
        _ => "VectorSearch",
    };
    trace.sort_elided = false;
    trace
}

fn trace_label_survives_sort(physical_plan: &str) -> bool {
    matches!(
        physical_plan,
        "IndexScan" | "AdjacencyProbe" | "EdgesScan" | "GraphBfs"
    )
}

fn vector_search_trace_with_source(
    operator: &'static str,
    candidate_trace: Option<QueryTrace>,
    query_vector_source: Option<contextdb_core::VectorIndexRef>,
) -> QueryTrace {
    let mut trace = vector_search_trace(operator, candidate_trace);
    trace.query_vector_source = query_vector_source;
    trace
}

fn mark_filtered_exact_route(trace: &mut crate::database::RetainedVectorSearchTrace) {
    trace.mark_filtered_exact_route();
}

fn row_vector_source_ref(expr: &Expr) -> Option<contextdb_core::VectorIndexRef> {
    match expr {
        Expr::RowVectorSource { table, column, .. } => Some(contextdb_core::VectorIndexRef::new(
            table.clone(),
            column.clone(),
        )),
        _ => None,
    }
}

fn is_unrestricted_scan_for_table(plan: &PhysicalPlan, table: &str) -> bool {
    matches!(
        plan,
        PhysicalPlan::Scan {
            table: scan_table,
            filter: None,
            ..
        } if scan_table == table
    )
}

/// A finite set of typed partition prefixes derived without reading a table
/// row. `covers_filter` means the candidate scan is redundant: the vector
/// reader's selected states are exactly the SQL predicate's possible states.
/// It is structural: true whenever the partition key covers the predicate,
/// whether or not every key identity is bound yet, so a passive explain of a
/// prepared statement reports the same residual and route shape as the same
/// statement with its literal inlined. Whether a reader may actually skip
/// the row filter is `covers_filter_for_execution`.
#[derive(Debug, Clone)]
pub(crate) struct VectorPartitionScope {
    prefixes: Vec<VectorPartitionKey>,
    /// Combinations whose every key component is constrained by equality but
    /// whose identity is not yet bound. Non-zero only on the passive explain
    /// path; every executing door binds its parameters first.
    unbound_prefixes: usize,
    pub(crate) covers_filter: bool,
}

impl VectorPartitionScope {
    /// Prefixes a reader may narrow by. `None` when an identity is unbound:
    /// the shape is known, the identity is not, so no reader may narrow.
    pub(crate) fn resolved_prefixes(&self) -> Option<&[VectorPartitionKey]> {
        (self.unbound_prefixes == 0).then_some(self.prefixes.as_slice())
    }

    /// How many partitions this predicate names, bound or not.
    pub(crate) fn selected_partitions(&self) -> usize {
        self.prefixes.len() + self.unbound_prefixes
    }

    /// Whether a reader may skip the row filter: covering AND fully bound.
    pub(crate) fn covers_filter_for_execution(&self) -> bool {
        self.covers_filter && self.unbound_prefixes == 0
    }
}

#[derive(Clone, Default)]
struct PartitionPredicateTerm {
    values: BTreeMap<String, Vec<PartitionKeyLiteral>>,
    residual: bool,
}

fn partition_column_name(expr: &Expr, table: &str, keys: &[String]) -> Option<String> {
    let Expr::Column(column) = expr else {
        return None;
    };
    if column.table.as_deref().is_some_and(|named| named != table)
        || !keys.iter().any(|key| key == &column.column)
    {
        return None;
    }
    Some(column.column.clone())
}

/// Intersect an AND's two literal lists for one key column. Both sides are
/// already canonical, so `Exact` meets `Exact` under the same `=` the row
/// filter applies. An unknown identity on either side (`Unresolved`,
/// `Unbound`) is the whole answer: an AND cannot shrink an unknown identity
/// below one. `NoPartition` equals nothing, so it drops out.
fn merge_partition_values(
    left: Vec<PartitionKeyLiteral>,
    right: Vec<PartitionKeyLiteral>,
) -> Vec<PartitionKeyLiteral> {
    let has = |predicate: fn(&PartitionKeyLiteral) -> bool| {
        left.iter().chain(right.iter()).any(predicate)
    };
    if has(|literal| matches!(literal, PartitionKeyLiteral::Unresolved)) {
        return vec![PartitionKeyLiteral::Unresolved];
    }
    if has(|literal| matches!(literal, PartitionKeyLiteral::Unbound)) {
        return vec![PartitionKeyLiteral::Unbound];
    }
    left.into_iter()
        .filter(|literal| {
            let PartitionKeyLiteral::Exact(value) = literal else {
                return false;
            };
            right.iter().any(|other| {
                matches!(
                    other,
                    PartitionKeyLiteral::Exact(other)
                        if compare_values(value, other) == Some(Ordering::Equal)
                )
            })
        })
        .collect()
}

/// One side of a key predicate, resolved against the declared key type. A
/// bare placeholder with no binding is an unknown identity, never an error;
/// any other expression that fails to resolve is a real planning error.
fn partition_predicate_literal(
    expr: &Expr,
    declared: &ColumnType,
    params: &HashMap<String, Value>,
) -> Result<PartitionKeyLiteral> {
    if let Expr::Parameter(name) = expr
        && !params.contains_key(name)
    {
        return Ok(PartitionKeyLiteral::Unbound);
    }
    let value = resolve_expr(expr, params)?;
    Ok(partition_key_literal(declared, &value))
}

fn partition_predicate_terms(
    expr: &Expr,
    table: &str,
    keys: &[String],
    key_types: &BTreeMap<String, ColumnType>,
    params: &HashMap<String, Value>,
) -> Result<Vec<PartitionPredicateTerm>> {
    let residual_term = || PartitionPredicateTerm {
        residual: true,
        ..Default::default()
    };
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let mut terms = partition_predicate_terms(left, table, keys, key_types, params)?;
            terms.extend(partition_predicate_terms(
                right, table, keys, key_types, params,
            )?);
            Ok(terms)
        }
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let left_terms = partition_predicate_terms(left, table, keys, key_types, params)?;
            let right_terms = partition_predicate_terms(right, table, keys, key_types, params)?;
            let mut combined = Vec::new();
            for left in left_terms {
                for right in &right_terms {
                    let mut term = left.clone();
                    term.residual |= right.residual;
                    for (column, values) in &right.values {
                        match term.values.remove(column) {
                            Some(existing) => {
                                term.values.insert(
                                    column.clone(),
                                    merge_partition_values(existing, values.clone()),
                                );
                            }
                            None => {
                                term.values.insert(column.clone(), values.clone());
                            }
                        }
                    }
                    combined.push(term);
                }
            }
            Ok(combined)
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let (column, other) = if let Some(column) = partition_column_name(left, table, keys) {
                (column, right)
            } else if let Some(column) = partition_column_name(right, table, keys) {
                (column, left)
            } else {
                return Ok(vec![residual_term()]);
            };
            let Some(declared) = key_types.get(&column) else {
                return Ok(vec![residual_term()]);
            };
            let literal = partition_predicate_literal(other, declared, params)?;
            Ok(vec![PartitionPredicateTerm {
                values: BTreeMap::from([(column, vec![literal])]),
                residual: false,
            }])
        }
        // A row-value list names whole key tuples: one term per listed row,
        // never the cross product of its columns' values.
        Expr::InList {
            expr,
            list,
            negated: false,
        } if matches!(expr.as_ref(), Expr::Row(_)) => {
            let Expr::Row(columns) = expr.as_ref() else {
                unreachable!("guarded as a row value");
            };
            let mut terms = Vec::with_capacity(list.len());
            for item in list {
                let Expr::Row(values) = item else {
                    return Ok(vec![residual_term()]);
                };
                let mut term = PartitionPredicateTerm::default();
                for (column, value) in columns.iter().zip(values) {
                    let Some(name) = partition_column_name(column, table, keys) else {
                        term.residual = true;
                        continue;
                    };
                    let Some(declared) = key_types.get(&name) else {
                        term.residual = true;
                        continue;
                    };
                    let literal = partition_predicate_literal(value, declared, params)?;
                    match term.values.remove(&name) {
                        Some(existing) => {
                            term.values
                                .insert(name, merge_partition_values(existing, vec![literal]));
                        }
                        None => {
                            term.values.insert(name, vec![literal]);
                        }
                    }
                }
                terms.push(term);
            }
            if terms.is_empty() {
                return Ok(vec![residual_term()]);
            }
            Ok(terms)
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let Some(column) = partition_column_name(expr, table, keys) else {
                return Ok(vec![residual_term()]);
            };
            let Some(declared) = key_types.get(&column) else {
                return Ok(vec![residual_term()]);
            };
            let values = list
                .iter()
                .map(|value| partition_predicate_literal(value, declared, params))
                .collect::<Result<Vec<_>>>()?;
            Ok(vec![PartitionPredicateTerm {
                values: BTreeMap::from([(column, values)]),
                residual: false,
            }])
        }
        _ => Ok(vec![residual_term()]),
    }
}

/// Bind every placeholder a vector search names in its query expression and
/// candidate predicate before any partition scope or route availability is
/// judged. Partition scope resolves after parameters are bound, so a
/// forgotten binding is the ordinary missing-parameter refusal, never a route
/// refusal that sends the caller to add a relational index or change modes.
/// Every executing reader calls this first; the passive explain path does
/// not, because it binds nothing and reports the shape.
pub(crate) fn bind_vector_search_parameters(
    query_expr: &Expr,
    candidates: Option<&PhysicalPlan>,
    params: &HashMap<String, Value>,
) -> Result<()> {
    match first_unbound_parameter(query_expr, params)
        .or_else(|| candidates.and_then(|plan| first_unbound_plan_parameter(plan, params)))
    {
        Some(name) => Err(Error::NotFound(format!("missing parameter: {name}"))),
        None => Ok(()),
    }
}

/// The first placeholder in a candidate plan's predicates with no binding.
/// The planner places a table scan carrying the statement's WHERE clause, or
/// a filter over one, below a vector search; an index scan carries already
/// resolved values, and any other shape reports its own missing parameter
/// when it runs.
fn first_unbound_plan_parameter<'a>(
    plan: &'a PhysicalPlan,
    params: &HashMap<String, Value>,
) -> Option<&'a str> {
    match plan {
        PhysicalPlan::Scan {
            filter: Some(filter),
            ..
        } => first_unbound_parameter(filter, params),
        PhysicalPlan::Filter { input, predicate } => first_unbound_parameter(predicate, params)
            .or_else(|| first_unbound_plan_parameter(input, params)),
        _ => None,
    }
}

/// The first placeholder in an expression with no binding, in evaluation
/// order. A subquery body is left to its own execution.
fn first_unbound_parameter<'a>(expr: &'a Expr, params: &HashMap<String, Value>) -> Option<&'a str> {
    let unbound = |expr: &'a Expr| first_unbound_parameter(expr, params);
    match expr {
        Expr::RowNumber(_) | Expr::WindowRank(_) => None,
        Expr::Parameter(name) => (!params.contains_key(name)).then_some(name.as_str()),
        Expr::Row(elements) => elements.iter().find_map(unbound),
        Expr::Column(_) | Expr::Literal(_) => None,
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            unbound(left).or_else(|| unbound(right))
        }
        Expr::UnaryOp { operand, .. } => unbound(operand),
        Expr::FunctionCall { args, .. } => args.iter().find_map(unbound),
        Expr::RowVectorSource { key, .. } => unbound(key),
        Expr::InList { expr, list, .. } => unbound(expr).or_else(|| list.iter().find_map(unbound)),
        Expr::Like { expr, pattern, .. } => unbound(expr).or_else(|| unbound(pattern)),
        Expr::InSubquery { expr, .. } | Expr::IsNull { expr, .. } => unbound(expr),
    }
}

/// Recovery names columns, never predicate values or bound parameters.
pub(crate) fn vector_filter_columns(candidates: Option<&PhysicalPlan>) -> Vec<String> {
    let Some(mut plan) = candidates else {
        return Vec::new();
    };
    while let PhysicalPlan::Project { input, .. } = plan {
        plan = input;
    }
    let mut columns = std::collections::BTreeSet::new();
    if let PhysicalPlan::Scan {
        filter: Some(filter),
        ..
    } = plan
    {
        let _ = walk_predicate_columns(filter, &mut |column| {
            columns.insert(column.column.clone());
            Ok(())
        });
    }
    columns.into_iter().collect()
}

/// The candidate scan's own filter, if it has one.
fn vector_candidate_scan_filter(candidates: Option<&PhysicalPlan>) -> Option<&Expr> {
    let mut plan = candidates?;
    while let PhysicalPlan::Project { input, .. } = plan {
        plan = input;
    }
    match plan {
        PhysicalPlan::Scan { filter, .. } => filter.as_ref(),
        _ => None,
    }
}

/// Every value a conjunct of `filter` pins `column` to, canonicalized to the
/// column's declared type, when the filter pins it to a finite set at all.
fn finite_column_values(
    table: &str,
    meta: &TableMeta,
    column: &str,
    filter: &Expr,
    params: &HashMap<String, Value>,
) -> Option<Vec<Value>> {
    let resolve = |expr: &Expr| -> Option<Value> {
        let raw = match expr {
            Expr::Literal(Literal::Text(text)) => Value::Text(text.clone()),
            Expr::Literal(Literal::Integer(value)) => Value::Int64(*value),
            Expr::Literal(Literal::Bool(value)) => Value::Bool(*value),
            Expr::Literal(Literal::Real(value)) if value.is_finite() => Value::Float64(*value),
            Expr::Parameter(name) => params.get(name)?.clone(),
            _ => return None,
        };
        coerce_index_probe_value_with_meta(table, meta, column, raw)
    };
    match filter {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let left = finite_column_values(table, meta, column, left, params);
            let right = finite_column_values(table, meta, column, right, params);
            match (left, right) {
                // Two finite terms on one column admit only their intersection.
                (Some(left), Some(right)) => Some(
                    left.into_iter()
                        .filter(|value| right.iter().any(|other| other == value))
                        .collect(),
                ),
                (Some(only), None) | (None, Some(only)) => Some(only),
                (None, None) => None,
            }
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => match left.as_ref() {
            Expr::Column(named) if named.column == column => Some(vec![resolve(right)?]),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => match expr.as_ref() {
            Expr::Column(named) if named.column == column => {
                list.iter().map(resolve).collect::<Option<Vec<_>>>()
            }
            // A row value pins each of its columns to that column's values
            // across the listed rows: every row the list admits carries one
            // of them.
            Expr::Row(columns) => {
                let position = columns.iter().position(
                    |element| matches!(element, Expr::Column(named) if named.column == column),
                )?;
                list.iter()
                    .map(|item| match item {
                        Expr::Row(values) => values.get(position).and_then(resolve),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
            }
            _ => None,
        },
        _ => None,
    }
}

/// Whether the statement's own predicate already keeps this read inside every
/// row restriction its reader carries.
///
/// Repeating a restriction the statement already enforces is not free: a column
/// the statement pins to a finite set, pinned a second time, stops being one
/// finite index term and becomes two, and the candidate route refuses to serve
/// two terms on one column from the index — so the read falls back to fetching
/// and re-checking every candidate row, which is the whole cost the index was
/// there to avoid.
///
/// The test is deliberately one-sided and total. Every restricted column must
/// be pinned by the statement to a finite set that is a subset of what the
/// reader may see, compared against the very values the row gate compares
/// against. A column the statement leaves open, pins through an unbound
/// parameter, negates, spreads across an `OR`, or constrains by a range is not
/// provably inside anything, and one such column is enough to answer no for the
/// whole read. Answering no costs a slower route and nothing else; answering
/// yes wrongly would widen a candidate set, so this never widens: the rows the
/// statement's own term selects are a subset of the rows the repeated pair
/// would select, and every row published still passes the real row gate.
fn statement_enforces_every_access_restriction(
    table: &str,
    meta: &TableMeta,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    restrictions: &[(String, Vec<Value>)],
) -> bool {
    if restrictions.is_empty() {
        return false;
    }
    let Some(filter) = filter else {
        return false;
    };
    restrictions.iter().all(|(column, allowed)| {
        let Some(requested) = finite_column_values(table, meta, column, filter, params) else {
            return false;
        };
        let allowed = allowed
            .iter()
            .filter_map(|value| {
                coerce_index_probe_value_with_meta(table, meta, column, value.clone())
            })
            .collect::<Vec<_>>();
        !requested.is_empty()
            && requested
                .iter()
                .all(|value| allowed.iter().any(|permitted| permitted == value))
    })
}

pub(crate) fn vector_candidates_with_access(
    db: &dyn ReadExecutionTarget,
    table: &str,
    candidates: Option<&PhysicalPlan>,
    params: &HashMap<String, Value>,
) -> Result<Option<PhysicalPlan>> {
    let Some(access) = db.vector_access_predicate(table)? else {
        return Ok(candidates.cloned());
    };
    if let Some(meta) = db.table_meta(table)
        && statement_enforces_every_access_restriction(
            table,
            &meta,
            vector_candidate_scan_filter(candidates),
            params,
            &db.vector_access_restrictions(table)?,
        )
    {
        return Ok(candidates.cloned());
    }
    let mut candidate = candidates.cloned().unwrap_or_else(|| PhysicalPlan::Scan {
        table: table.to_owned(),
        alias: None,
        filter: None,
    });
    let mut source = &mut candidate;
    while let PhysicalPlan::Project { input, .. } = source {
        source = input;
    }
    if let PhysicalPlan::Scan { filter, .. } = source {
        *filter = Some(match filter.take() {
            Some(left) => Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(access),
            },
            None => access,
        });
    }
    Ok(Some(candidate))
}

pub(crate) fn vector_partition_scope_for_candidates(
    db: &dyn ReadExecutionTarget,
    index: &VectorIndexRef,
    candidates: Option<&PhysicalPlan>,
    params: &HashMap<String, Value>,
) -> Result<Option<VectorPartitionScope>> {
    let candidates = candidates.map(|mut plan| {
        while let PhysicalPlan::Project { input, .. } = plan {
            plan = input;
        }
        plan
    });
    let Some(PhysicalPlan::Scan {
        table,
        filter: Some(filter),
        ..
    }) = candidates
    else {
        return Ok(None);
    };
    if table != &index.table {
        return Ok(None);
    }
    let Some(meta) = db.table_meta(&index.table) else {
        return Ok(None);
    };
    let Some(column) = meta
        .columns
        .iter()
        .find(|column| column.name == index.column)
    else {
        return Ok(None);
    };
    let Some(keys) = column.partition_key_columns.as_ref() else {
        return Ok(None);
    };
    // Every literal is canonicalized to the declared key type where it is
    // collected, so a prefix below is always a component the write side
    // stores.
    let mut key_types = BTreeMap::new();
    for key in keys {
        let Some(declared) = meta.columns.iter().find(|column| &column.name == key) else {
            return Ok(None);
        };
        key_types.insert(key.clone(), declared.column_type.clone());
    }
    let terms = partition_predicate_terms(filter, &index.table, keys, &key_types, params)?;
    let mut prefixes = BTreeSet::new();
    let mut unbound_prefixes = 0usize;
    let mut covers_filter = true;
    for term in terms {
        if term.residual {
            covers_filter = false;
        }
        let mut combinations: Vec<Vec<&PartitionKeyLiteral>> = vec![Vec::new()];
        let mut gap = false;
        for key in keys {
            let Some(values) = term.values.get(key) else {
                gap = true;
                continue;
            };
            if gap {
                covers_filter = false;
                continue;
            }
            let mut next = Vec::new();
            for combination in &combinations {
                for value in values {
                    let mut candidate = combination.clone();
                    candidate.push(value);
                    next.push(candidate);
                }
            }
            combinations = next;
        }
        if combinations.first().is_some_and(Vec::is_empty) {
            return Ok(None);
        }
        for literals in combinations {
            // An identity that names more than one component cannot be
            // narrowed by any prefix: the row filter only sees candidates
            // fetched from selected partitions, so the only non-lossy answer
            // is no narrowing at all.
            if literals
                .iter()
                .any(|literal| matches!(literal, PartitionKeyLiteral::Unresolved))
            {
                return Ok(None);
            }
            // A component no key of the declared type can equal names a
            // provably empty scope: it contributes no prefix and the
            // predicate is still covered, so the reader searches nothing and
            // plain `=` returns nothing.
            if literals
                .iter()
                .any(|literal| matches!(literal, PartitionKeyLiteral::NoPartition))
            {
                continue;
            }
            if literals
                .iter()
                .any(|literal| matches!(literal, PartitionKeyLiteral::Unbound))
            {
                unbound_prefixes += 1;
                continue;
            }
            let values = literals
                .into_iter()
                .filter_map(|literal| match literal {
                    PartitionKeyLiteral::Exact(value) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let key = VectorPartitionKey::from_values(&values).ok_or_else(|| {
                Error::PlanError(
                    "partition key literal canonicalized to a component the key encoder cannot represent"
                        .to_string(),
                )
            })?;
            prefixes.insert(key);
        }
    }
    Ok(
        (!prefixes.is_empty() || unbound_prefixes > 0 || covers_filter).then_some(
            VectorPartitionScope {
                prefixes: prefixes.into_iter().collect(),
                unbound_prefixes,
                covers_filter,
            },
        ),
    )
}

/// Whether a vector search can obtain every row its relational side admits
/// without first walking the table.  INDEXED promises that an unavailable
/// maintained vector route is a refusal, not an excuse to do unbounded work;
/// the same has to be true of the relational side that narrows that route.
///
/// This deliberately asks the existing relational index chooser rather than
/// making a second idea of which predicates are usable.  The candidate plan
/// the planner places below a vector search is the table scan carrying the
/// statement's WHERE clause.  An unrestricted scan needs no candidate set
/// unless the read target requires one for authorization.  A filtered scan is
/// complete only when that chooser can supply an index walk (or can prove the
/// predicate empty) before any table row is read.
pub(crate) struct VectorCandidateRoute {
    pub candidates: Option<PhysicalPlan>,
    pub scope: Option<VectorPartitionScope>,
    pub access_filter: bool,
    pub directory_route: bool,
    pub complete: bool,
}

pub(crate) fn vector_candidate_route(
    db: &dyn ReadExecutionTarget,
    index: &VectorIndexRef,
    candidates: Option<&PhysicalPlan>,
    params: &HashMap<String, Value>,
) -> Result<VectorCandidateRoute> {
    let access_filter = db.bounded_read_requires_candidate_filter(&index.table)?;
    let sql_scope = vector_partition_scope_for_candidates(db, index, candidates, params)?;
    let candidates = vector_candidates_with_access(db, &index.table, candidates, params)?;
    let scope = vector_partition_scope_for_candidates(db, index, candidates.as_ref(), params)?
        .or(sql_scope.clone());
    let directory_route = sql_scope
        .as_ref()
        .is_some_and(VectorPartitionScope::covers_filter_for_execution)
        || scope
            .as_ref()
            .is_some_and(VectorPartitionScope::covers_filter_for_execution);
    let indexed = vector_candidate_plan_has_complete_relational_route(
        db,
        candidates.as_ref(),
        index,
        params,
        true,
    )?;
    // Partition keys can supply a bounded identity directory. Every identity
    // still passes the real row gate; selecting a tuple never grants access.
    let complete = (!access_filter
        && candidates
            .as_ref()
            .is_none_or(|plan| is_unrestricted_scan_for_table(plan, &index.table)))
        || indexed
        || (scope
            .as_ref()
            .is_some_and(VectorPartitionScope::covers_filter_for_execution)
            && (!access_filter
                || !db
                    .table_meta(&index.table)
                    .is_some_and(|meta| meta.columns.iter().any(|c| c.acl_ref.is_some()))));
    let mut source = candidates.as_ref();
    while let Some(PhysicalPlan::Project { input, .. }) = source {
        source = Some(input);
    }
    let directory_route = !indexed
        && (directory_route
            || (access_filter && matches!(source, Some(PhysicalPlan::Scan { .. }))));
    Ok(VectorCandidateRoute {
        candidates,
        scope,
        access_filter,
        directory_route,
        complete,
    })
}

pub(crate) fn vector_candidate_refusal_columns(
    db: &dyn ReadExecutionTarget,
    index: &VectorIndexRef,
    candidates: Option<&PhysicalPlan>,
    access_filter: bool,
) -> Vec<String> {
    let mut columns = vector_filter_columns(candidates);
    if access_filter && let Some(meta) = db.table_meta(&index.table) {
        columns.extend(
            meta.columns
                .iter()
                .filter(|c| c.context_id || c.scope_label.is_some() || c.acl_ref.is_some())
                .map(|c| c.name.clone()),
        );
    }
    columns.sort();
    columns.dedup();
    columns
}

pub(crate) fn vector_candidate_plan_has_complete_relational_route(
    db: &dyn ReadExecutionTarget,
    candidates: Option<&PhysicalPlan>,
    index: &VectorIndexRef,
    params: &HashMap<String, Value>,
    access_filter: bool,
) -> Result<bool> {
    let table = &index.table;
    if !access_filter
        && vector_partition_scope_for_candidates(db, index, candidates, params)?
            .is_some_and(|scope| scope.covers_filter_for_execution())
    {
        return Ok(true);
    }
    let Some(candidates) = candidates else {
        return Ok(!access_filter);
    };
    if is_unrestricted_scan_for_table(candidates, table) {
        return Ok(!access_filter);
    }
    match candidates {
        // Projection changes values, not the source's candidate enumeration.
        PhysicalPlan::Project { input, .. } => vector_candidate_plan_has_complete_relational_route(
            db,
            Some(input),
            index,
            params,
            access_filter,
        ),
        // The planner already produced a direct index source.  It is a
        // complete relational answer by construction; executing it is still
        // left to the ordinary kernel so authorization is intersected there.
        PhysicalPlan::IndexScan {
            table: candidate_table,
            ..
        } if candidate_table == table => Ok(true),
        PhysicalPlan::Scan {
            table: candidate_table,
            filter: Some(filter),
            ..
        } if candidate_table == table => {
            let Some(meta) = db.table_meta(table) else {
                return Ok(false);
            };
            let Some(_) = analyze_filter_for_index(table, &meta, filter, params).pick else {
                return Ok(false);
            };
            // The scan kernel coerces this same pick when it opens the index
            // cursor.  If a bound value cannot be coerced, that kernel proves
            // an empty answer rather than reading the table, which is still a
            // complete candidate route here.
            Ok(true)
        }
        // A join, CTE, or another compound plan may be a valid relational
        // query, but this vector operator has no proof that it can enumerate
        // its complete allowed-row set without a broad scan.  Do not guess:
        // INDEXED reports the typed route refusal before any source runs.
        _ => Ok(false),
    }
}

pub(crate) fn row_matches(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<bool> {
    Ok(eval_bool_expr(expr, &|inner| eval_expr_value(row, inner, params))?.unwrap_or(false))
}

/// Filter `rows` by an optional predicate, propagating a genuine evaluation
/// error (e.g. negating a non-numeric column) as a statement error instead
/// of discarding it as "row excluded". `row_matches` already returns
/// `Result<bool>` correctly — NULL-as-false is folded to `Ok(false)` inside
/// `eval_bool_expr`, which is correct SQL — every call site here used to
/// throw the `Err` away via `.unwrap_or(false)`, so a predicate that failed
/// to evaluate silently excluded every row it touched instead of failing the
/// statement.
pub(crate) fn filter_rows_by_predicate(
    rows: Vec<VersionedRow>,
    predicate: Option<&Expr>,
    params: &HashMap<String, Value>,
) -> Result<Vec<VersionedRow>> {
    let Some(predicate) = predicate else {
        return Ok(rows);
    };
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        if row_matches(&row, predicate, params)? {
            kept.push(row);
        }
    }
    Ok(kept)
}

fn eval_expr_value(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Value> {
    match expr {
        Expr::Column(c) => {
            if c.column == "row_id" {
                Ok(Value::Int64(row.row_id.0 as i64))
            } else {
                Ok(row.values.get(&c.column).cloned().unwrap_or(Value::Null))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left = eval_expr_value(row, left, params)?;
            let right = eval_expr_value(row, right, params)?;
            eval_binary_op(op, &left, &right)
        }
        Expr::UnaryOp { op, operand } => {
            let value = eval_expr_value(row, operand, params)?;
            match op {
                UnaryOp::Not => Ok(if value == Value::Null {
                    Value::Null
                } else {
                    Value::Bool(!value_to_bool(&value))
                }),
                UnaryOp::Neg => match value {
                    Value::Int64(v) => checked_neg_i64(v),
                    Value::Float64(v) => Ok(Value::Float64(-v)),
                    _ => Err(Error::PlanError(
                        "cannot negate non-numeric value".to_string(),
                    )),
                },
            }
        }
        Expr::FunctionCall { name, args } => eval_function_in_row_context(row, name, args, params),
        Expr::IsNull { expr, negated } => {
            let is_null = eval_expr_value(row, expr, params)? == Value::Null;
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } if matches!(expr.as_ref(), Expr::Row(_)) => Ok(
            match row_value_in_list(expr, list, &|inner| eval_expr_value(row, inner, params))? {
                Some(matched) => Value::Bool(matched != *negated),
                None => Value::Null,
            },
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = eval_expr_value(row, expr, params)?;
            if needle == Value::Null {
                return Ok(Value::Null);
            }
            let mut saw_unknown = false;
            for item in list {
                let candidate = eval_expr_value(row, item, params)?;
                if candidate == Value::Null {
                    saw_unknown = true;
                } else if matches!(compare_values(&needle, &candidate), Some(Ordering::Equal))
                    || needle == candidate
                {
                    return Ok(Value::Bool(!*negated));
                }
            }
            Ok(if saw_unknown {
                Value::Null
            } else {
                Value::Bool(*negated)
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let matches = match (
                eval_expr_value(row, expr, params)?,
                eval_expr_value(row, pattern, params)?,
            ) {
                (Value::Null, _) | (_, Value::Null) => return Ok(Value::Null),
                (Value::Text(value), Value::Text(pattern)) => like_matches(&value, &pattern),
                _ => false,
            };
            Ok(Value::Bool(if *negated { !matches } else { matches }))
        }
        _ => resolve_expr(expr, params),
    }
}

pub fn resolve_expr(expr: &Expr, params: &HashMap<String, Value>) -> Result<Value> {
    match expr {
        Expr::RowNumber(_) | Expr::WindowRank(_) => Err(Error::WindowFunctionNotSupported {
            form: "ROW_NUMBER execution pending".to_string(),
        }),
        Expr::Literal(l) => Ok(match l {
            Literal::Null => Value::Null,
            Literal::Bool(v) => Value::Bool(*v),
            Literal::Integer(v) => Value::Int64(*v),
            Literal::Real(v) => Value::Float64(*v),
            Literal::Text(v) => Value::Text(v.clone()),
            Literal::Vector(v) => Value::Vector(v.clone()),
        }),
        Expr::Parameter(p) => params
            .get(p)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {}", p))),
        Expr::Column(c) => Ok(Value::Text(c.column.clone())),
        Expr::UnaryOp { op, operand } => match op {
            UnaryOp::Neg => match resolve_expr(operand, params)? {
                Value::Int64(v) => checked_neg_i64(v),
                Value::Float64(v) => Ok(Value::Float64(-v)),
                _ => Err(Error::PlanError(
                    "cannot negate non-numeric value".to_string(),
                )),
            },
            UnaryOp::Not => Err(Error::PlanError(
                "boolean NOT requires row context".to_string(),
            )),
        },
        Expr::FunctionCall { name, args } => {
            let values = args
                .iter()
                .map(|arg| resolve_expr(arg, params))
                .collect::<Result<Vec<_>>>()?;
            eval_function(name, &values)
        }
        Expr::CosineDistance { right, .. } => resolve_expr(right, params),
        _ => Err(Error::PlanError("unsupported expression".to_string())),
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<Ordering> {
    #[cfg(feature = "test-seams")]
    observe_finite_planner_value_operation(FinitePlannerValueOperation::Comparison);

    match (a, b) {
        (Value::Int64(left), Value::Int64(right)) => Some(left.cmp(right)),
        (Value::Float64(left), Value::Float64(right)) => Some(left.total_cmp(right)),
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right)) => Some(left.cmp(right)),
        (Value::Int64(left), Value::Float64(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float64(left), Value::Int64(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::Timestamp(left), Value::Int64(right)) => Some(left.cmp(right)),
        (Value::Int64(left), Value::Timestamp(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Some(left.cmp(right)),
        (Value::Uuid(u), Value::Text(t)) => {
            if let Ok(parsed) = t.parse::<uuid::Uuid>() {
                Some(u.cmp(&parsed))
            } else {
                None
            }
        }
        (Value::Text(t), Value::Uuid(u)) => {
            if let Ok(parsed) = t.parse::<uuid::Uuid>() {
                Some(parsed.cmp(u))
            } else {
                None
            }
        }
        (Value::TxId(a), Value::TxId(b)) => Some(a.0.cmp(&b.0)),
        (Value::TxId(a), Value::Int64(b)) => {
            if *b < 0 {
                Some(Ordering::Greater)
            } else {
                Some(a.0.cmp(&(*b as u64)))
            }
        }
        (Value::Int64(a), Value::TxId(b)) => {
            if *a < 0 {
                Some(Ordering::Less)
            } else {
                Some((*a as u64).cmp(&b.0))
            }
        }
        (Value::TxId(_), Value::Timestamp(_)) | (Value::Timestamp(_), Value::TxId(_)) => None,
        (Value::Null, _) | (_, Value::Null) => None,
        _ => None,
    }
}

fn values_identical(left: &Value, right: &Value) -> bool {
    #[cfg(feature = "test-seams")]
    observe_finite_planner_value_operation(FinitePlannerValueOperation::EqualityCheck);

    left == right
}

fn values_sql_equal(left: &Value, right: &Value) -> bool {
    compare_values(left, right) == Some(Ordering::Equal) || values_identical(left, right)
}

/// `(a, b, ...) IN ((x, y, ...), ...)` under SQL's three-valued row
/// comparison. Two rows are equal when every pair of elements is equal,
/// unequal as soon as one pair is known to differ, and unknown otherwise. The
/// list is true when one row is equal, unknown when none is equal but one is
/// unknown, and false only when every row is known to differ.
pub(crate) fn row_value_in_list(
    row: &Expr,
    list: &[Expr],
    eval_value: &impl Fn(&Expr) -> Result<Value>,
) -> Result<Option<bool>> {
    let Expr::Row(columns) = row else {
        return Err(Error::PlanError(
            "a row-value IN needs a row on its left".to_string(),
        ));
    };
    let needles = columns.iter().map(eval_value).collect::<Result<Vec<_>>>()?;
    let mut unknown = false;
    for item in list {
        let Expr::Row(values) = item else {
            return Err(Error::PlanError(
                "IN compares a row value against a single value".to_string(),
            ));
        };
        if values.len() != needles.len() {
            return Err(Error::PlanError(format!(
                "IN compares a row of {} columns against a row of {} values",
                needles.len(),
                values.len()
            )));
        }
        let mut row_unknown = false;
        let mut row_differs = false;
        for (needle, value) in needles.iter().zip(values) {
            let value = eval_value(value)?;
            if *needle == Value::Null || value == Value::Null {
                row_unknown = true;
            } else if !values_sql_equal(needle, &value) {
                row_differs = true;
                break;
            }
        }
        if row_differs {
            continue;
        }
        if !row_unknown {
            return Ok(Some(true));
        }
        unknown = true;
    }
    Ok((!unknown).then_some(false))
}

/// How a value resolves against a declared partition-key column type.
#[derive(Debug, Clone)]
pub(crate) enum PartitionKeyLiteral {
    /// The one component of the declared type this value is `=`-equal to.
    Exact(Value),
    /// No component of the declared type can be `=`-equal to this value:
    /// a provably empty scope, not an error.
    NoPartition,
    /// More than one distinct component of the declared type is `=`-equal to
    /// this value, so the predicate names no finite key set. No narrowing.
    Unresolved,
    /// A placeholder with no binding. The shape is known, the identity is
    /// not.
    Unbound,
}

/// Resolve a value to the one partition component of the declared key type
/// it is `=`-equal to, so a stored key and a predicate literal meet under the
/// same equality the row filter applies. Every arm below verifies itself by
/// calling `compare_values`; no arm restates an equality in its own words. An
/// arm only forms a candidate component and, where the declared type has
/// neighbouring components that could also be equal, names them, and
/// `unique_component` proves equality and uniqueness by asking
/// `compare_values`.
pub(crate) fn partition_key_literal(declared: &ColumnType, value: &Value) -> PartitionKeyLiteral {
    match (declared, value) {
        (ColumnType::Uuid, Value::Uuid(_))
        | (ColumnType::Integer, Value::Int64(_))
        | (ColumnType::Timestamp, Value::Timestamp(_))
        | (ColumnType::TxId, Value::TxId(_))
        | (ColumnType::Boolean, Value::Bool(_))
        | (ColumnType::Text, Value::Text(_)) => unique_component(value.clone(), value, &[]),
        (ColumnType::Uuid, Value::Text(text)) => match text.parse::<uuid::Uuid>() {
            Ok(parsed) => unique_component(Value::Uuid(parsed), value, &[]),
            Err(_) => PartitionKeyLiteral::NoPartition,
        },
        (ColumnType::Integer, Value::Float64(float)) => {
            if !float.is_finite() {
                return PartitionKeyLiteral::NoPartition;
            }
            // `as` saturates, so an out-of-range float forms a candidate that
            // `compare_values` then refuses. Above 2^53 several integers are
            // `=`-equal to one float; the neighbours make that visible.
            let candidate = *float as i64;
            let neighbours = [candidate.checked_sub(1), candidate.checked_add(1)]
                .into_iter()
                .flatten()
                .map(Value::Int64)
                .collect::<Vec<_>>();
            unique_component(Value::Int64(candidate), value, &neighbours)
        }
        (ColumnType::Timestamp, Value::Int64(n)) => {
            unique_component(Value::Timestamp(*n), value, &[])
        }
        (ColumnType::TxId, Value::Int64(n)) => {
            // A negative integer forms a candidate `compare_values` refuses.
            unique_component(Value::TxId(contextdb_core::TxId(*n as u64)), value, &[])
        }
        // Many text spellings are `=`-equal to one UUID while unequal to each
        // other, so no single text component names this value.
        (ColumnType::Text, Value::Uuid(_)) => PartitionKeyLiteral::Unresolved,
        _ => PartitionKeyLiteral::NoPartition,
    }
}

/// `candidate` is the declared-type component for `value` exactly when
/// `compare_values` says they are equal and says no neighbouring component is.
fn unique_component(candidate: Value, value: &Value, neighbours: &[Value]) -> PartitionKeyLiteral {
    if compare_values(&candidate, value) != Some(Ordering::Equal) {
        return PartitionKeyLiteral::NoPartition;
    }
    if neighbours
        .iter()
        .any(|neighbour| compare_values(neighbour, value) == Some(Ordering::Equal))
    {
        return PartitionKeyLiteral::Unresolved;
    }
    PartitionKeyLiteral::Exact(candidate)
}

fn eval_bool_expr(
    expr: &Expr,
    eval_value: &impl Fn(&Expr) -> Result<Value>,
) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                let left = eval_value(left)?;
                let right = eval_value(right)?;
                if left == Value::Null || right == Value::Null {
                    return Ok(None);
                }

                let result = match op {
                    BinOp::Eq => values_sql_equal(&left, &right),
                    BinOp::Neq => !values_sql_equal(&left, &right),
                    BinOp::Lt => compare_values(&left, &right) == Some(Ordering::Less),
                    BinOp::Lte => matches!(
                        compare_values(&left, &right),
                        Some(Ordering::Less | Ordering::Equal)
                    ),
                    BinOp::Gt => compare_values(&left, &right) == Some(Ordering::Greater),
                    BinOp::Gte => matches!(
                        compare_values(&left, &right),
                        Some(Ordering::Greater | Ordering::Equal)
                    ),
                    BinOp::And | BinOp::Or => unreachable!(),
                };
                Ok(Some(result))
            }
            BinOp::And => {
                let left = eval_bool_expr(left, eval_value)?;
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = eval_bool_expr(right, eval_value)?;
                Ok(match (left, right) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(true), other) => other,
                    (None, Some(false)) => Some(false),
                    (None, Some(true)) | (None, None) => None,
                    (Some(false), _) => Some(false),
                })
            }
            BinOp::Or => {
                let left = eval_bool_expr(left, eval_value)?;
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = eval_bool_expr(right, eval_value)?;
                Ok(match (left, right) {
                    (Some(false), Some(false)) => Some(false),
                    (Some(false), other) => other,
                    (None, Some(true)) => Some(true),
                    (None, Some(false)) | (None, None) => None,
                    (Some(true), _) => Some(true),
                })
            }
        },
        Expr::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => Ok(eval_bool_expr(operand, eval_value)?.map(|value| !value)),
        Expr::InList {
            expr,
            list,
            negated,
        } if matches!(expr.as_ref(), Expr::Row(_)) => {
            Ok(row_value_in_list(expr, list, eval_value)?.map(|matched| matched != *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = eval_value(expr)?;
            if needle == Value::Null {
                return Ok(None);
            }

            let mut saw_unknown = false;
            for item in list {
                let candidate = eval_value(item)?;
                if candidate == Value::Null {
                    saw_unknown = true;
                } else if values_sql_equal(&needle, &candidate) {
                    return Ok(Some(!*negated));
                }
            }
            Ok(if saw_unknown { None } else { Some(*negated) })
        }
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before execution".to_string(),
        )),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let left = eval_value(expr)?;
            let right = eval_value(pattern)?;
            let matched = match (left, right) {
                (Value::Null, _) | (_, Value::Null) => None,
                (Value::Text(value), Value::Text(pattern)) => Some(like_matches(&value, &pattern)),
                _ => Some(false),
            };
            Ok(matched.map(|matched| matched != *negated))
        }
        Expr::IsNull { expr, negated } => {
            let is_null = eval_value(expr)? == Value::Null;
            Ok(Some(if *negated { !is_null } else { is_null }))
        }
        // A boolean literal is a legal predicate anywhere a predicate is
        // legal (`ON TRUE`, `WHERE FALSE`), per standard SQL.
        Expr::Literal(Literal::Bool(value)) => Ok(Some(*value)),
        // A bare boolean column (`WHERE flag` means `WHERE flag = TRUE`), a
        // boolean `$param`, a literal `NULL` predicate (`WHERE NULL` is
        // standard SQL for "match nothing"), and a boolean-returning function
        // call all reduce to a value first. A NULL value excludes the row —
        // the same NULL-as-false semantics every other shape in this match
        // already uses — and a genuinely non-boolean value is an evaluation
        // error, never a silent exclusion.
        Expr::Column(_)
        | Expr::Parameter(_)
        | Expr::Literal(Literal::Null)
        | Expr::FunctionCall { .. } => match eval_value(expr)? {
            Value::Bool(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            other => Err(Error::PlanError(format!(
                "WHERE expression must be boolean, got {:?}: {:?}",
                other, expr
            ))),
        },
        _ => Err(Error::PlanError(format!(
            "unsupported WHERE expression: {:?}",
            expr
        ))),
    }
}

fn eval_binary_op(op: &BinOp, left: &Value, right: &Value) -> Result<Value> {
    if matches!(op, BinOp::And | BinOp::Or) {
        let left = if left == &Value::Null {
            None
        } else {
            Some(value_to_bool(left))
        };
        let right = if right == &Value::Null {
            None
        } else {
            Some(value_to_bool(right))
        };
        let truth = match op {
            BinOp::And => match (left, right) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            BinOp::Or => match (left, right) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
            _ => unreachable!(),
        };
        return Ok(truth.map(Value::Bool).unwrap_or(Value::Null));
    }
    if left == &Value::Null || right == &Value::Null {
        return Ok(Value::Null);
    }
    let bool_value = match op {
        BinOp::Eq => {
            if left == &Value::Null || right == &Value::Null {
                false
            } else {
                values_sql_equal(left, right)
            }
        }
        BinOp::Neq => {
            if left == &Value::Null || right == &Value::Null {
                false
            } else {
                !values_sql_equal(left, right)
            }
        }
        BinOp::Lt => compare_values(left, right) == Some(Ordering::Less),
        BinOp::Lte => matches!(
            compare_values(left, right),
            Some(Ordering::Less | Ordering::Equal)
        ),
        BinOp::Gt => compare_values(left, right) == Some(Ordering::Greater),
        BinOp::Gte => matches!(
            compare_values(left, right),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        BinOp::And => value_to_bool(left) && value_to_bool(right),
        BinOp::Or => value_to_bool(left) || value_to_bool(right),
    };
    Ok(Value::Bool(bool_value))
}

fn value_to_bool(value: &Value) -> bool {
    matches!(value, Value::Bool(true))
}

fn compare_sort_values(left: &Value, right: &Value, direction: SortDirection) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => match direction {
            SortDirection::Asc => Ordering::Greater,
            SortDirection::Desc => Ordering::Less,
            SortDirection::CosineDistance => Ordering::Equal,
        },
        (_, Value::Null) => match direction {
            SortDirection::Asc => Ordering::Less,
            SortDirection::Desc => Ordering::Greater,
            SortDirection::CosineDistance => Ordering::Equal,
        },
        _ => {
            let ordering = compare_values(left, right).unwrap_or(Ordering::Equal);
            match direction {
                SortDirection::Asc => ordering,
                SortDirection::Desc => ordering.reverse(),
                SortDirection::CosineDistance => ordering,
            }
        }
    }
}

fn eval_assignment_expr(
    expr: &Expr,
    row_values: &HashMap<String, Value>,
    params: &HashMap<String, Value>,
) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => literal_to_value(lit),
        Expr::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Other(format!("unknown parameter: {}", name))),
        Expr::Column(col_ref) => row_values
            .get(&col_ref.column)
            .cloned()
            .ok_or_else(|| Error::Other(format!("column not found: {}", col_ref.column))),
        Expr::BinaryOp { left, op, right } => {
            let left = eval_assignment_expr(left, row_values, params)?;
            let right = eval_assignment_expr(right, row_values, params)?;
            eval_binary_op(op, &left, &right)
        }
        Expr::UnaryOp { op, operand } => match op {
            UnaryOp::Neg => match eval_assignment_expr(operand, row_values, params)? {
                Value::Int64(value) => checked_neg_i64(value),
                Value::Float64(value) => Ok(Value::Float64(-value)),
                _ => Err(Error::Other(format!(
                    "unsupported expression in UPDATE SET: {:?}",
                    expr
                ))),
            },
            UnaryOp::Not => Err(Error::Other(format!(
                "unsupported expression in UPDATE SET: {:?}",
                expr
            ))),
        },
        Expr::FunctionCall { name, args } => {
            let evaluated = args
                .iter()
                .map(|arg| eval_assignment_expr(arg, row_values, params))
                .collect::<Result<Vec<_>>>()?;
            eval_function(name, &evaluated)
        }
        _ => Err(Error::Other(format!(
            "unsupported expression in UPDATE SET: {:?}",
            expr
        ))),
    }
}

pub(crate) fn apply_on_conflict_updates(
    db: &Database,
    table: &str,
    mut insert_values: HashMap<String, Value>,
    existing_row: &VersionedRow,
    on_conflict: &OnConflictPlan,
    params: &HashMap<String, Value>,
    active_tx: Option<TxId>,
) -> Result<HashMap<String, Value>> {
    if on_conflict.update_columns.is_empty() {
        return Ok(insert_values);
    }

    if db.table_meta(table).is_some_and(|meta| meta.immutable) {
        return Err(Error::ImmutableTable(table.to_string()));
    }

    // Reject column-level IMMUTABLE updates at the ON CONFLICT DO UPDATE merge
    // point. First flagged column in update-list order wins. Rejection returns
    // Err here; the caller (exec_insert) is responsible for releasing any
    // allocator bytes and restoring the write-set checkpoint.
    if let Some(meta) = db.table_meta(table) {
        for (column, _) in &on_conflict.update_columns {
            if let Some(col_def) = meta.columns.iter().find(|c| c.name == *column)
                && col_def.immutable
            {
                return Err(Error::ImmutableColumn {
                    table: table.to_string(),
                    column: column.clone(),
                });
            }
        }
    }

    let current_tx_max = Some(db.committed_watermark());

    let mut merged = existing_row.values.clone();
    for (column, expr) in &on_conflict.update_columns {
        let value = eval_assignment_expr(expr, &existing_row.values, params)?;
        merged.insert(
            column.clone(),
            coerce_value_for_column(db, table, column, value, current_tx_max, active_tx)?,
        );
    }

    for (column, value) in insert_values.drain() {
        merged.entry(column).or_insert(value);
    }

    Ok(merged)
}

fn literal_to_value(lit: &Literal) -> Result<Value> {
    Ok(match lit {
        Literal::Null => Value::Null,
        Literal::Bool(v) => Value::Bool(*v),
        Literal::Integer(v) => Value::Int64(*v),
        Literal::Real(v) => Value::Float64(*v),
        Literal::Text(v) => Value::Text(v.clone()),
        Literal::Vector(v) => Value::Vector(v.clone()),
    })
}

fn eval_arithmetic(name: &str, args: &[Value]) -> Result<Value> {
    let [left, right] = args else {
        return Err(Error::PlanError(format!(
            "function {} expects 2 arguments",
            name
        )));
    };

    // Standard SQL NULL propagation: a NULL operand makes the whole
    // arithmetic expression NULL, for every one of the four operators —
    // never the generic "expects numeric arguments" refusal the type-pair
    // match would otherwise produce, and (for division specifically) never
    // the division-by-zero error below.
    if *left == Value::Null || *right == Value::Null {
        return Ok(Value::Null);
    }

    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => match name {
            "__add" => checked_add_i64(*left, *right),
            "__sub" => checked_sub_i64(*left, *right),
            "__mul" => checked_mul_i64(*left, *right),
            "__div" => checked_div_i64(*left, *right),
            _ => Err(Error::PlanError(format!("unknown function: {}", name))),
        },
        (Value::Float64(left), Value::Float64(right)) => match name {
            "__add" => Ok(Value::Float64(left + right)),
            "__sub" => Ok(Value::Float64(left - right)),
            "__mul" => Ok(Value::Float64(left * right)),
            "__div" => checked_div_f64(*left, *right),
            _ => Err(Error::PlanError(format!("unknown function: {}", name))),
        },
        (Value::Int64(left), Value::Float64(right)) => match name {
            "__add" => Ok(Value::Float64(*left as f64 + right)),
            "__sub" => Ok(Value::Float64(*left as f64 - right)),
            "__mul" => Ok(Value::Float64(*left as f64 * right)),
            "__div" => checked_div_f64(*left as f64, *right),
            _ => Err(Error::PlanError(format!("unknown function: {}", name))),
        },
        (Value::Float64(left), Value::Int64(right)) => match name {
            "__add" => Ok(Value::Float64(left + *right as f64)),
            "__sub" => Ok(Value::Float64(left - *right as f64)),
            "__mul" => Ok(Value::Float64(left * *right as f64)),
            "__div" => checked_div_f64(*left, *right as f64),
            _ => Err(Error::PlanError(format!("unknown function: {}", name))),
        },
        _ => Err(Error::PlanError(format!(
            "function {} expects numeric arguments",
            name
        ))),
    }
}

/// Integer addition, raising the standard-SQL out-of-range error on overflow
/// instead of panicking the process or silently wrapping.
fn checked_add_i64(left: i64, right: i64) -> Result<Value> {
    left.checked_add(right)
        .map(Value::Int64)
        .ok_or_else(|| Error::PlanError("integer out of range".to_string()))
}

/// Integer negation, raising the standard-SQL out-of-range error on overflow.
fn checked_neg_i64(value: i64) -> Result<Value> {
    value
        .checked_neg()
        .map(Value::Int64)
        .ok_or_else(|| Error::PlanError("integer out of range".to_string()))
}

/// Integer subtraction, raising the standard-SQL out-of-range error on overflow
/// instead of panicking the process or silently wrapping.
fn checked_sub_i64(left: i64, right: i64) -> Result<Value> {
    left.checked_sub(right)
        .map(Value::Int64)
        .ok_or_else(|| Error::PlanError("integer out of range".to_string()))
}

/// Integer multiplication, raising the standard-SQL out-of-range error on overflow
/// instead of panicking the process or silently wrapping.
fn checked_mul_i64(left: i64, right: i64) -> Result<Value> {
    left.checked_mul(right)
        .map(Value::Int64)
        .ok_or_else(|| Error::PlanError("integer out of range".to_string()))
}

/// Integer division, refusing a zero divisor with the standard-SQL wording
/// instead of panicking the process (`i64::MIN / -1` is refused the same
/// way, as the one other case integer division cannot represent).
fn checked_div_i64(left: i64, right: i64) -> Result<Value> {
    if right == 0 {
        return Err(Error::PlanError("division by zero".to_string()));
    }
    left.checked_div(right)
        .map(Value::Int64)
        .ok_or_else(|| Error::PlanError(format!("integer overflow: {left} / {right}")))
}

/// Float division, refusing a zero divisor with the standard-SQL wording
/// rather than silently producing `inf`/`NaN`.
fn checked_div_f64(left: f64, right: f64) -> Result<Value> {
    if right == 0.0 {
        return Err(Error::PlanError("division by zero".to_string()));
    }
    Ok(Value::Float64(left / right))
}

fn eval_function_in_row_context(
    row: &VersionedRow,
    name: &str,
    args: &[Expr],
    params: &HashMap<String, Value>,
) -> Result<Value> {
    let values = args
        .iter()
        .map(|arg| eval_expr_value(row, arg, params))
        .collect::<Result<Vec<_>>>()?;
    eval_function(name, &values)
}

fn eval_function(name: &str, args: &[Value]) -> Result<Value> {
    match name.to_ascii_lowercase().as_str() {
        "__add" | "__sub" | "__mul" | "__div" => eval_arithmetic(name, args),
        "coalesce" => Ok(args
            .iter()
            .find(|value| **value != Value::Null)
            .cloned()
            .unwrap_or(Value::Null)),
        "now" => Ok(Value::Timestamp(now_timestamp_seconds()?)),
        _ => Err(Error::PlanError(format!("unknown function: {}", name))),
    }
}

/// Seconds since the epoch for `NOW()`. Reaching the clock through
/// `eval_function` first normalizes the function name into a fresh `String`;
/// a caller that has already identified the function calls this instead, so
/// no scratch is allocated outside whatever budget governs it.
fn now_timestamp_seconds() -> Result<i64> {
    Ok((Wallclock::now().0 / 1_000) as i64)
}

fn like_matches(value: &str, pattern: &str) -> bool {
    let value_chars = value.chars().collect::<Vec<_>>();
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let (mut vi, mut pi) = (0usize, 0usize);
    let (mut star_idx, mut match_idx) = (None, 0usize);

    while vi < value_chars.len() {
        if pi < pattern_chars.len()
            && (pattern_chars[pi] == '_' || pattern_chars[pi] == value_chars[vi])
        {
            vi += 1;
            pi += 1;
        } else if pi < pattern_chars.len() && pattern_chars[pi] == '%' {
            star_idx = Some(pi);
            match_idx = vi;
            pi += 1;
        } else if let Some(star) = star_idx {
            pi = star + 1;
            match_idx += 1;
            vi = match_idx;
        } else {
            return false;
        }
    }

    while pi < pattern_chars.len() && pattern_chars[pi] == '%' {
        pi += 1;
    }

    pi == pattern_chars.len()
}

fn resolve_in_subqueries(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Expr> {
    resolve_in_subqueries_with_ctes(
        db,
        expr,
        params,
        tx,
        &[],
        vector_copy_owner,
        read_memory.clone(),
    )
}

fn resolve_graph_filter_at_snapshot(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: SnapshotId,
    ctes: &[Cte],
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Expr> {
    db.with_snapshot_override(snapshot, || {
        resolve_in_subqueries_with_ctes(
            db,
            expr,
            params,
            tx,
            ctes,
            vector_copy_owner,
            read_memory.clone(),
        )
    })
}

#[inline(never)]
pub(crate) fn resolve_in_subqueries_with_ctes(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    ctes: &[Cte],
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Expr> {
    // The eager door reads the inner answer the eager way: the whole result at
    // once, with no ceiling over it, which is exactly what it promises its
    // callers.
    let mut drain = |plan: &PhysicalPlan, select: &Expr| -> Result<Vec<Value>> {
        let result = execute_plan(db, plan, params, tx, vector_copy_owner, read_memory.clone())?;
        result
            .rows
            .iter()
            .map(|row| eval_project_expr(select, row, &result.columns, params))
            .collect()
    };
    resolve_in_subqueries_with_drain(db, expr, params, ctes, &mut drain)
}

/// The shape of `IN (subquery)` resolution that both doors share: the same
/// correlation refusal, the same planning, the same literal conversion, the
/// same walk over the surrounding expression. What differs is how the inner
/// answer's rows are READ, which is the only part that has to be bounded, so
/// that part is the caller's.
#[allow(clippy::type_complexity)]
#[allow(clippy::only_used_in_recursion)]
pub(crate) fn resolve_in_subqueries_with_drain<E>(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    ctes: &[Cte],
    drain: &mut dyn FnMut(&PhysicalPlan, &Expr) -> std::result::Result<Vec<Value>, E>,
) -> std::result::Result<Expr, E>
where
    E: From<Error>,
{
    match expr {
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            // Detect correlated subqueries: WHERE references to outer tables
            let mut subquery_tables: std::collections::HashSet<String> = subquery
                .from
                .iter()
                .filter_map(|item| match item {
                    contextdb_parser::ast::FromItem::Table { name, alias } => {
                        Some(alias.clone().unwrap_or_else(|| name.clone()))
                    }
                    _ => None,
                })
                .collect();
            // CTE names are valid table references within the subquery
            for cte in ctes {
                match cte {
                    Cte::SqlCte { name, .. } | Cte::MatchCte { name, .. } => {
                        subquery_tables.insert(name.clone());
                    }
                }
            }
            if let Some(where_clause) = &subquery.where_clause
                && has_outer_table_ref(where_clause, &subquery_tables)
            {
                return Err(E::from(Error::Other(
                    "correlated subqueries are not supported".to_string(),
                )));
            }

            let query_plan = plan(&Statement::Select(SelectStatement {
                ctes: ctes.to_vec(),
                body: (**subquery).clone(),
            }))
            .map_err(E::from)?;
            // A subquery is planned here at execution time rather than being a
            // subtree of the root plan, so it needs the plan-time pass applied
            // to it directly — otherwise it would be the one path where an
            // unknown column is still only caught while rows are being read.
            validate_plan_columns(db, &query_plan).map_err(E::from)?;
            let select_expr = subquery
                .columns
                .first()
                .map(|column| column.expr.clone())
                .ok_or_else(|| {
                    E::from(Error::PlanError(
                        "subquery must select one column".to_string(),
                    ))
                })?;
            let list = drain(&query_plan, &select_expr)?
                .into_iter()
                .map(value_to_literal)
                .collect::<Result<Vec<_>>>()
                .map_err(E::from)?;
            Ok(Expr::InList {
                expr: Box::new(resolve_in_subqueries_with_drain(
                    db, expr, params, ctes, drain,
                )?),
                list,
                negated: *negated,
            })
        }
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(resolve_in_subqueries_with_drain(
                db, left, params, ctes, drain,
            )?),
            op: *op,
            right: Box::new(resolve_in_subqueries_with_drain(
                db, right, params, ctes, drain,
            )?),
        }),
        Expr::UnaryOp { op, operand } => Ok(Expr::UnaryOp {
            op: *op,
            operand: Box::new(resolve_in_subqueries_with_drain(
                db, operand, params, ctes, drain,
            )?),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(resolve_in_subqueries_with_drain(
                db, expr, params, ctes, drain,
            )?),
            list: list
                .iter()
                .map(|item| resolve_in_subqueries_with_drain(db, item, params, ctes, drain))
                .collect::<std::result::Result<Vec<_>, E>>()?,
            negated: *negated,
        }),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Ok(Expr::Like {
            expr: Box::new(resolve_in_subqueries_with_drain(
                db, expr, params, ctes, drain,
            )?),
            pattern: Box::new(resolve_in_subqueries_with_drain(
                db, pattern, params, ctes, drain,
            )?),
            negated: *negated,
        }),
        Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
            expr: Box::new(resolve_in_subqueries_with_drain(
                db, expr, params, ctes, drain,
            )?),
            negated: *negated,
        }),
        Expr::FunctionCall { name, args } => Ok(Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_in_subqueries_with_drain(db, arg, params, ctes, drain))
                .collect::<std::result::Result<Vec<_>, E>>()?,
        }),
        _ => Ok(expr.clone()),
    }
}

fn has_outer_table_ref(expr: &Expr, subquery_tables: &std::collections::HashSet<String>) -> bool {
    match expr {
        Expr::Column(ColumnRef {
            table: Some(table), ..
        }) => !subquery_tables.contains(table),
        Expr::BinaryOp { left, right, .. } => {
            has_outer_table_ref(left, subquery_tables)
                || has_outer_table_ref(right, subquery_tables)
        }
        Expr::UnaryOp { operand, .. } => has_outer_table_ref(operand, subquery_tables),
        Expr::InList { expr, list, .. } => {
            has_outer_table_ref(expr, subquery_tables)
                || list
                    .iter()
                    .any(|item| has_outer_table_ref(item, subquery_tables))
        }
        Expr::IsNull { expr, .. } => has_outer_table_ref(expr, subquery_tables),
        Expr::Like { expr, pattern, .. } => {
            has_outer_table_ref(expr, subquery_tables)
                || has_outer_table_ref(pattern, subquery_tables)
        }
        Expr::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| has_outer_table_ref(arg, subquery_tables)),
        _ => false,
    }
}

fn value_to_literal(value: Value) -> Result<Expr> {
    Ok(Expr::Literal(match value {
        Value::Null => Literal::Null,
        Value::Bool(v) => Literal::Bool(v),
        Value::Int64(v) => Literal::Integer(v),
        Value::Float64(v) => Literal::Real(v),
        Value::Text(v) => Literal::Text(v),
        Value::Uuid(v) => Literal::Text(v.to_string()),
        Value::Timestamp(v) => Literal::Integer(v),
        other => {
            return Err(Error::PlanError(format!(
                "unsupported subquery result value: {:?}",
                other
            )));
        }
    }))
}

fn query_result_row_matches(
    row: &[Value],
    columns: &[String],
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<bool> {
    Ok(eval_bool_expr(expr, &|inner| {
        eval_query_result_expr(inner, row, columns, params)
    })?
    .unwrap_or(false))
}

/// How a column reference resolves against a query result's column list.
///
/// Kept distinct from the formatted error so callers that need to react to the
/// two failures differently — the sort path reports an unknown ORDER BY column
/// as `ColumnNotFound` — can do so without matching on message text.
enum QueryResultColumn {
    Found(usize),
    NotFound,
    Ambiguous,
}

fn resolve_query_result_column(
    input_columns: &[String],
    column_ref: &ColumnRef,
) -> QueryResultColumn {
    if let Some(table) = &column_ref.table {
        let qualified = format!("{table}.{}", column_ref.column);
        // Prioritize qualified match (e.g., "e.id") over unqualified (e.g., "id")
        // to avoid picking the wrong table's column in JOINs.
        return input_columns
            .iter()
            .position(|name| name == &qualified)
            .or_else(|| {
                input_columns
                    .iter()
                    .position(|name| name == &column_ref.column)
            })
            .map_or(QueryResultColumn::NotFound, QueryResultColumn::Found);
    }

    let matches = input_columns
        .iter()
        .enumerate()
        .filter_map(|(idx, name)| {
            (name == &column_ref.column
                || name.rsplit('.').next() == Some(column_ref.column.as_str()))
            .then_some(idx)
        })
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [] => QueryResultColumn::NotFound,
        [idx] => QueryResultColumn::Found(*idx),
        _ => QueryResultColumn::Ambiguous,
    }
}

/// `resolve_query_result_column`, additionally requiring an explicit
/// qualifier to name a real relation in `known_relations` before it is
/// allowed to fall back to a bare-name match — checked BEFORE the
/// exact-qualified-name lookup so a bogus qualifier can never coincidentally
/// resolve. Used by the plan-time validators; the runtime evaluators call
/// `resolve_query_result_column` directly and keep its historical bare-name
/// fallback for the handful of plan shapes (see `plan_contains_graph_bfs`)
/// the static validators deliberately do not check at all.
fn resolve_query_result_column_scoped(
    input_columns: &[String],
    column_ref: &ColumnRef,
    known_relations: &[String],
) -> QueryResultColumn {
    if let Some(table) = &column_ref.table {
        let qualified = format!("{table}.{}", column_ref.column);
        if let Some(idx) = input_columns.iter().position(|name| name == &qualified) {
            return QueryResultColumn::Found(idx);
        }
        if !known_relations.iter().any(|name| name == table) {
            return QueryResultColumn::NotFound;
        }
    }
    resolve_query_result_column(input_columns, column_ref)
}

// Kept as `PlanError`, NOT `ColumnNotFound`: this runtime fallback is shared
// with GRAPH_TABLE `COLUMNS (...)` projections (a graph node property, not a
// table column), whose existing "project column not found" wording is
// pinned elsewhere and out of scope here. An ordinary table SELECT-list's
// unknown column is caught earlier, at plan time, by the dedicated
// `ColumnNotFound` check in `validate_plan_columns`'s `Project` arm — this
// fallback is unreachable for that shape.
fn query_result_column_error(column_ref: &ColumnRef, resolution: &QueryResultColumn) -> Error {
    match resolution {
        QueryResultColumn::Ambiguous => {
            Error::PlanError(format!("ambiguous column reference: {}", column_ref.column))
        }
        _ => Error::PlanError(format!(
            "project column not found: {}",
            column_ref.table.as_ref().map_or_else(
                || column_ref.column.clone(),
                |table| format!("{table}.{}", column_ref.column)
            )
        )),
    }
}

fn lookup_query_result_column(
    row: &[Value],
    input_columns: &[String],
    column_ref: &ColumnRef,
) -> Result<Value> {
    match resolve_query_result_column(input_columns, column_ref) {
        QueryResultColumn::Found(idx) => Ok(row.get(idx).cloned().unwrap_or(Value::Null)),
        other => Err(query_result_column_error(column_ref, &other)),
    }
}

fn concatenate_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    combined
}

fn duplicate_column_names(left: &[String], right: &[String]) -> BTreeSet<String> {
    let left_names = left
        .iter()
        .map(|column| column.rsplit('.').next().unwrap_or(column.as_str()))
        .collect::<BTreeSet<_>>();
    right
        .iter()
        .filter_map(|column| {
            let bare = column.rsplit('.').next().unwrap_or(column.as_str());
            left_names.contains(bare).then(|| bare.to_string())
        })
        .collect()
}

fn qualify_join_columns(
    columns: &[String],
    left_columns: &[String],
    right_columns: &[String],
    left_alias: &Option<String>,
    right_prefix: &str,
) -> Vec<String> {
    let left_prefix = left_alias.as_deref();
    columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            if idx < left_columns.len() {
                if left_columns[idx].contains('.') {
                    left_columns[idx].clone()
                } else if let Some(prefix) = left_prefix {
                    format!("{prefix}.{}", left_columns[idx])
                } else {
                    left_columns[idx].clone()
                }
            } else {
                let right_idx = idx - left_columns.len();
                let bare = right_columns[right_idx]
                    .rsplit('.')
                    .next()
                    .unwrap_or(right_columns[right_idx].as_str());
                if column == bare {
                    format!("{right_prefix}.{bare}")
                } else {
                    column.clone()
                }
            }
        })
        .collect()
}

fn right_table_name(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Scan { table, alias, .. } => alias.clone().unwrap_or_else(|| table.clone()),
        _ => "right".to_string(),
    }
}

fn distinct_row_key(row: &[Value]) -> Vec<u8> {
    bincode::serde::encode_to_vec(row, bincode::config::standard())
        .expect("query rows should serialize for DISTINCT")
}

fn resolve_uuid(expr: &Expr, params: &HashMap<String, Value>) -> Result<uuid::Uuid> {
    match resolve_expr(expr, params)? {
        Value::Uuid(u) => Ok(u),
        Value::Text(t) => uuid::Uuid::parse_str(&t)
            .map_err(|e| Error::PlanError(format!("invalid uuid '{}': {}", t, e))),
        _ => Err(Error::PlanError(
            "graph start node must be UUID".to_string(),
        )),
    }
}

#[derive(Clone, Copy)]
enum GraphTraceShape {
    AdjacencyProbe { index: &'static str },
    EdgesScan { rejected_index: &'static str },
    GraphBfs,
}

pub(crate) struct GraphStartResolution {
    pub(crate) ids: Vec<uuid::Uuid>,
    pub(crate) predicates_pushed: smallvec::SmallVec<[Cow<'static, str>; 4]>,
    pub(crate) pinned: bool,
    /// Rows this resolution read to find the starts. The eager route reports
    /// them through the query-wide counter; a bounded read has no such counter
    /// and puts them in its own trace, so the figure is returned as well as
    /// bumped.
    #[allow(dead_code)]
    pub(crate) examined: u64,
}

fn graph_trace_shape(
    single_step: bool,
    unpinned_start: bool,
    direction: Option<Direction>,
) -> GraphTraceShape {
    if !single_step {
        return GraphTraceShape::GraphBfs;
    }
    let index = graph_adjacency_index_label(direction.unwrap_or(Direction::Outgoing));
    if unpinned_start {
        GraphTraceShape::EdgesScan {
            rejected_index: index,
        }
    } else {
        GraphTraceShape::AdjacencyProbe { index }
    }
}

fn graph_adjacency_index_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Outgoing | Direction::Both => "forward_adj",
        Direction::Incoming => "reverse_adj",
    }
}

fn reverse_graph_probe_direction(direction: Direction) -> Direction {
    match direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

fn graph_query_trace(
    shape: GraphTraceShape,
    predicates_pushed: smallvec::SmallVec<[Cow<'static, str>; 4]>,
) -> QueryTrace {
    match shape {
        GraphTraceShape::AdjacencyProbe { index } => QueryTrace {
            physical_plan: "AdjacencyProbe",
            index_used: Some(index.to_string()),
            predicates_pushed,
            ..Default::default()
        },
        GraphTraceShape::EdgesScan { rejected_index } => {
            let mut indexes_considered: smallvec::SmallVec<[crate::database::IndexCandidate; 4]> =
                smallvec::SmallVec::new();
            indexes_considered.push(crate::database::IndexCandidate {
                name: rejected_index.to_string(),
                rejected_reason: Cow::Borrowed("no pinned vertex"),
            });
            QueryTrace {
                physical_plan: "EdgesScan",
                indexes_considered,
                ..Default::default()
            }
        }
        GraphTraceShape::GraphBfs => QueryTrace {
            physical_plan: "GraphBfs",
            predicates_pushed,
            ..Default::default()
        },
    }
}

/// Resolve start nodes for a graph traversal from a WHERE filter like
/// `a.name = 'entity-0'`. Uses a matching relational index when one exists and
/// otherwise reports the full row scan needed to resolve the start vertices.
pub(crate) fn resolve_graph_start_nodes_from_filter(
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    start_alias: &str,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<GraphStartResolution> {
    let mut graph_materialized_columns = expression_columns(std::iter::once(filter));
    if !graph_materialized_columns
        .iter()
        .any(|column| column == "id")
    {
        graph_materialized_columns.push("id".to_string());
    }
    // The eager door reads the candidate rows the eager way: whole result sets,
    // no ceiling, which is what it promises its callers.
    let mut read = |table: &str, pick: Option<&IndexPick>| -> Result<(Vec<uuid::Uuid>, u64)> {
        let (rows, scanned) = match pick {
            Some(pick) => execute_index_scan(
                db,
                table,
                pick,
                snapshot,
                tx,
                IndexScanAccessMode::Select,
                Some(&graph_materialized_columns),
                None,
                params,
                vector_copy_owner,
                read_memory.clone(),
            )?,
            None => {
                let rows = scan_rows_for_select(
                    db,
                    table,
                    snapshot,
                    tx,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                let scanned = rows.len() as u64;
                (rows, scanned)
            }
        };
        db.__bump_rows_examined(scanned);
        Ok((
            rows.into_iter()
                .filter_map(|row| match row.values.get("id") {
                    Some(Value::Uuid(id)) => Some(*id),
                    _ => None,
                })
                .collect(),
            scanned,
        ))
    };
    resolve_graph_start_nodes_with_reader(
        db,
        filter,
        params,
        tx,
        snapshot,
        start_alias,
        &mut read,
        read_memory.clone(),
    )
}

/// The shape of start resolution both doors share: which filter names a start,
/// which tables can describe one, which predicates were pushed, and the final
/// per-candidate confirmation. What differs is how the candidate ROWS are read,
/// which is the only part that has to be bounded.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub(crate) fn resolve_graph_start_nodes_with_reader<E>(
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    start_alias: &str,
    read: &mut dyn FnMut(
        &str,
        Option<&IndexPick>,
    ) -> std::result::Result<(Vec<uuid::Uuid>, u64), E>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> std::result::Result<GraphStartResolution, E>
where
    E: From<Error>,
{
    let mut predicates_pushed: smallvec::SmallVec<[Cow<'static, str>; 4]> =
        smallvec::SmallVec::new();
    let mut examined = 0_u64;

    let Some(start_filter) = graph_start_resolution_filter(filter, start_alias) else {
        return Ok(GraphStartResolution {
            ids: Vec::new(),
            predicates_pushed,
            pinned: false,
            examined,
        });
    };
    if let Some(ids) = resolve_graph_start_ids_from_filter(
        db,
        &start_filter,
        params,
        tx,
        snapshot,
        start_alias,
        read_memory.clone(),
    )
    .map_err(E::from)?
    {
        predicates_pushed.push(Cow::Owned(format!("{start_alias}.id")));
        return Ok(GraphStartResolution {
            ids,
            predicates_pushed,
            pinned: true,
            examined,
        });
    }

    let start_columns = graph_start_columns(&start_filter, start_alias);
    if start_columns.is_empty() {
        return Ok(GraphStartResolution {
            ids: Vec::new(),
            predicates_pushed,
            pinned: false,
            examined,
        });
    }
    if graph_start_filter_needs_unpinned_null_semantics(&start_filter, start_alias) {
        return Ok(GraphStartResolution {
            ids: Vec::new(),
            predicates_pushed,
            pinned: false,
            examined,
        });
    }
    for column in &start_columns {
        predicates_pushed.push(Cow::Owned(format!("{start_alias}.{column}")));
    }

    let mut candidate_ids = BTreeSet::new();
    for table_name in db.table_names() {
        let meta = match db.table_meta(&table_name) {
            Some(m) => m,
            None => continue,
        };
        let has_start_col = meta.columns.iter().any(|c| start_columns.contains(&c.name));
        let has_id = has_exact_column_type(&meta, "id", &ColumnType::Uuid);
        if !has_start_col || !has_id {
            continue;
        }

        let analysis = analyze_filter_for_index(&table_name, &meta, &start_filter, params);
        let (ids, scanned) = read(&table_name, analysis.pick.as_ref())?;
        examined = examined.saturating_add(scanned);
        candidate_ids.extend(ids);
    }
    let mut ids = Vec::new();
    for id in candidate_ids {
        let bindings = HashMap::from([(start_alias.to_string(), id)]);
        if graph_filter_matches_bindings(
            db,
            &start_filter,
            params,
            tx,
            snapshot,
            &bindings,
            read_memory.clone(),
        )
        .map_err(E::from)?
        {
            ids.push(id);
        }
    }
    db.assert_graph_anchor_nodes_readable_in_tx(tx, &ids, snapshot, read_memory.clone())
        .map_err(E::from)?;
    Ok(GraphStartResolution {
        ids,
        predicates_pushed,
        pinned: true,
        examined,
    })
}

fn resolve_graph_start_nodes_from_plan(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<uuid::Uuid>> {
    let result = db.with_snapshot_override(snapshot, || {
        execute_plan(db, plan, params, tx, vector_copy_owner, read_memory.clone())
    })?;
    result
        .result
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .map(|value| match value {
            Value::Uuid(id) => Ok(id),
            Value::Text(text) => uuid::Uuid::parse_str(&text)
                .map_err(|_| Error::PlanError(format!("invalid UUID in graph start plan: {text}"))),
            other => Err(Error::PlanError(format!(
                "invalid graph start identifier from plan: {other:?}"
            ))),
        })
        .collect()
}

fn resolve_graph_start_ids_from_filter(
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    start_alias: &str,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<Vec<uuid::Uuid>>> {
    match filter {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } if is_graph_start_column_ref(left, start_alias, "id")
            || is_graph_start_column_ref(right, start_alias, "id") =>
        {
            let id = if is_graph_start_column_ref(left, start_alias, "id") {
                resolve_graph_static_uuid_expr(right, params, "graph start identifier in filter")?
            } else {
                resolve_graph_static_uuid_expr(left, params, "graph start identifier in filter")?
            };
            let Some(id) = id else {
                return Ok(None);
            };
            let ids = vec![id];
            db.assert_graph_anchor_nodes_readable_in_tx(tx, &ids, snapshot, read_memory.clone())?;
            Ok(Some(ids))
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_graph_start_column_ref(expr, start_alias, "id") => {
            let mut ids = Vec::with_capacity(list.len());
            for item in list {
                let Some(id) = resolve_graph_static_uuid_expr(
                    item,
                    params,
                    "graph start identifier in filter",
                )?
                else {
                    return Ok(None);
                };
                ids.push(id);
            }
            db.assert_graph_anchor_nodes_readable_in_tx(tx, &ids, snapshot, read_memory.clone())?;
            Ok(Some(ids))
        }
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let left_ids = resolve_graph_start_ids_from_filter(
                db,
                left,
                params,
                tx,
                snapshot,
                start_alias,
                read_memory.clone(),
            )?;
            let right_ids = resolve_graph_start_ids_from_filter(
                db,
                right,
                params,
                tx,
                snapshot,
                start_alias,
                read_memory.clone(),
            )?;
            match (left_ids, right_ids) {
                (Some(left_ids), Some(right_ids)) => {
                    let right_ids = right_ids.into_iter().collect::<BTreeSet<_>>();
                    let ids = left_ids
                        .into_iter()
                        .filter(|id| right_ids.contains(id))
                        .collect::<Vec<_>>();
                    db.assert_graph_anchor_nodes_readable_in_tx(
                        tx,
                        &ids,
                        snapshot,
                        read_memory.clone(),
                    )?;
                    Ok(Some(ids))
                }
                (Some(_), None) | (None, Some(_)) | (None, None) => Ok(None),
            }
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let left_ids = resolve_graph_start_ids_from_filter(
                db,
                left,
                params,
                tx,
                snapshot,
                start_alias,
                read_memory.clone(),
            )?;
            let right_ids = resolve_graph_start_ids_from_filter(
                db,
                right,
                params,
                tx,
                snapshot,
                start_alias,
                read_memory.clone(),
            )?;
            match (left_ids, right_ids) {
                (Some(left_ids), Some(right_ids)) => {
                    let ids = left_ids
                        .into_iter()
                        .chain(right_ids)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>();
                    db.assert_graph_anchor_nodes_readable_in_tx(
                        tx,
                        &ids,
                        snapshot,
                        read_memory.clone(),
                    )?;
                    Ok(Some(ids))
                }
                (Some(_), None) | (None, Some(_)) | (None, None) => Ok(None),
            }
        }
        Expr::UnaryOp { .. } => Ok(None),
        _ => Ok(None),
    }
}

fn resolve_graph_target_id_residual(
    filter: &Expr,
    params: &HashMap<String, Value>,
    target_alias: &str,
) -> Result<Option<uuid::Uuid>> {
    for conjunct in split_conjuncts(filter) {
        if let Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = conjunct
        {
            if is_graph_alias_column_ref(&left, target_alias, "id") {
                return resolve_graph_static_uuid_expr(
                    &right,
                    params,
                    "graph target identifier in filter",
                );
            }
            if is_graph_alias_column_ref(&right, target_alias, "id") {
                return resolve_graph_static_uuid_expr(
                    &left,
                    params,
                    "graph target identifier in filter",
                );
            }
        }
    }
    Ok(None)
}

#[derive(Clone, Copy)]
pub(crate) enum GraphPredicateWork {
    Checkpoint,
    AccessRow(usize),
}

struct GraphValueCache<'a> {
    values: HashMap<(uuid::Uuid, String), Vec<Value>>,
    memory: contextdb_core::read_memory::ReadCredit,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    work: &'a mut dyn FnMut(GraphPredicateWork) -> Result<()>,
}
impl<'a> GraphValueCache<'a> {
    fn new(
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        work: &'a mut dyn FnMut(GraphPredicateWork) -> Result<()>,
    ) -> Self {
        Self {
            values: HashMap::new(),
            memory: contextdb_core::read_memory::ReadCredit::new(read_memory.clone()),
            read_memory,
            work,
        }
    }
}

fn graph_filter_matches_bindings(
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<bool> {
    graph_filter_matches_bindings_with_work(
        db,
        filter,
        params,
        tx,
        snapshot,
        bindings,
        &mut |_| Ok(()),
        read_memory.clone(),
    )
}

#[allow(clippy::too_many_arguments)]
fn graph_filter_matches_bindings_with_work(
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,
    work: &mut dyn FnMut(GraphPredicateWork) -> Result<()>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<bool> {
    let mut cache = GraphValueCache::new(read_memory.clone(), work);
    Ok(graph_eval_bool_expr(
        db,
        filter,
        params,
        tx,
        snapshot,
        bindings,
        &mut cache,
        read_memory.clone(),
    )?
    .unwrap_or(false))
}

fn graph_eval_bool_expr(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,
    cache: &mut GraphValueCache<'_>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<bool>> {
    (cache.work)(GraphPredicateWork::Checkpoint)?;
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                let left = graph_eval_values(
                    db,
                    left,
                    params,
                    tx,
                    snapshot,
                    bindings,
                    cache,
                    read_memory.clone(),
                )?;
                let right = graph_eval_values(
                    db,
                    right,
                    params,
                    tx,
                    snapshot,
                    bindings,
                    cache,
                    read_memory.clone(),
                )?;
                graph_compare_any(&left, &right, *op, cache.work)
            }
            BinOp::And => {
                let left = graph_eval_bool_expr(
                    db,
                    left,
                    params,
                    tx,
                    snapshot,
                    bindings,
                    cache,
                    read_memory.clone(),
                )?;
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = graph_eval_bool_expr(
                    db,
                    right,
                    params,
                    tx,
                    snapshot,
                    bindings,
                    cache,
                    read_memory.clone(),
                )?;
                Ok(match (left, right) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(true), other) => other,
                    (None, Some(false)) => Some(false),
                    (None, Some(true)) | (None, None) => None,
                    (Some(false), _) => Some(false),
                })
            }
            BinOp::Or => {
                let left = graph_eval_bool_expr(
                    db,
                    left,
                    params,
                    tx,
                    snapshot,
                    bindings,
                    cache,
                    read_memory.clone(),
                )?;
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = graph_eval_bool_expr(
                    db,
                    right,
                    params,
                    tx,
                    snapshot,
                    bindings,
                    cache,
                    read_memory.clone(),
                )?;
                Ok(match (left, right) {
                    (Some(false), Some(false)) => Some(false),
                    (Some(false), other) => other,
                    (None, Some(true)) => Some(true),
                    (None, Some(false)) | (None, None) => None,
                    (Some(true), _) => Some(true),
                })
            }
        },
        Expr::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => Ok(graph_eval_bool_expr(
            db,
            operand,
            params,
            tx,
            snapshot,
            bindings,
            cache,
            read_memory.clone(),
        )?
        .map(|value| !value)),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needles = graph_eval_values(
                db,
                expr,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )?;
            let mut candidates = Vec::new();
            for item in list {
                candidates.extend(graph_eval_values(
                    db,
                    item,
                    params,
                    tx,
                    snapshot,
                    bindings,
                    cache,
                    read_memory.clone(),
                )?);
            }
            Ok(
                graph_compare_any(&needles, &candidates, BinOp::Eq, cache.work)?
                    .map(|matched| matched != *negated),
            )
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let values = graph_eval_values(
                db,
                expr,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )?;
            let patterns = graph_eval_values(
                db,
                pattern,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )?;
            let mut saw_unknown = false;
            for value in &values {
                for pattern in &patterns {
                    (cache.work)(GraphPredicateWork::Checkpoint)?;
                    match (value, pattern) {
                        (Value::Null, _) | (_, Value::Null) => saw_unknown = true,
                        (Value::Text(value), Value::Text(pattern))
                            if like_matches(value, pattern) =>
                        {
                            return Ok(Some(!*negated));
                        }
                        _ => {}
                    }
                }
            }
            Ok(if saw_unknown { None } else { Some(*negated) })
        }
        Expr::IsNull { expr, negated } => {
            let values = graph_eval_values(
                db,
                expr,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )?;
            let is_null = values.iter().all(|value| *value == Value::Null);
            Ok(Some(if *negated { !is_null } else { is_null }))
        }
        Expr::FunctionCall { .. } => {
            match graph_eval_values(
                db,
                expr,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )?
            .into_iter()
            .next()
            .unwrap_or(Value::Null)
            {
                Value::Bool(value) => Ok(Some(value)),
                Value::Null => Ok(None),
                _ => Err(Error::PlanError(format!(
                    "unsupported graph WHERE expression: {:?}",
                    expr
                ))),
            }
        }
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before graph execution".to_string(),
        )),
        _ => Err(Error::PlanError(format!(
            "unsupported graph WHERE expression: {:?}",
            expr
        ))),
    }
}

fn graph_eval_values(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,
    cache: &mut GraphValueCache<'_>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<Value>> {
    (cache.work)(GraphPredicateWork::Checkpoint)?;
    match expr {
        Expr::RowNumber(_) | Expr::WindowRank(_) => Err(Error::WindowFunctionNotSupported {
            form: "window in graph predicate".to_string(),
        }),
        Expr::Literal(_) | Expr::Parameter(_) => Ok(vec![resolve_expr(expr, params)?]),
        Expr::Column(column) => graph_column_values(
            db,
            column,
            tx,
            snapshot,
            bindings,
            cache,
            read_memory.clone(),
        ),
        Expr::BinaryOp { left, op, right } => {
            let left = graph_eval_values(
                db,
                left,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )?;
            let right = graph_eval_values(
                db,
                right,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )?;
            let mut values = Vec::new();
            for left in &left {
                for right in &right {
                    (cache.work)(GraphPredicateWork::Checkpoint)?;
                    values.push(eval_binary_op(op, left, right)?);
                }
            }
            Ok(if values.is_empty() {
                vec![Value::Null]
            } else {
                values
            })
        }
        Expr::UnaryOp { op, operand } => {
            let values = graph_eval_values(
                db,
                operand,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )?;
            values
                .into_iter()
                .map(|value| match op {
                    UnaryOp::Not => Ok(if value == Value::Null {
                        Value::Null
                    } else {
                        Value::Bool(!value_to_bool(&value))
                    }),
                    UnaryOp::Neg => match value {
                        Value::Int64(v) => checked_neg_i64(v),
                        Value::Float64(v) => Ok(Value::Float64(-v)),
                        _ => Err(Error::PlanError(
                            "cannot negate non-numeric graph value".to_string(),
                        )),
                    },
                })
                .collect()
        }
        Expr::FunctionCall { name, args } => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                let mut arg_values = graph_eval_values(
                    db,
                    arg,
                    params,
                    tx,
                    snapshot,
                    bindings,
                    cache,
                    read_memory.clone(),
                )?;
                values.push(arg_values.pop().unwrap_or(Value::Null));
            }
            Ok(vec![eval_function(name, &values)?])
        }
        Expr::InList { .. } | Expr::Like { .. } | Expr::IsNull { .. } => {
            Ok(vec![match graph_eval_bool_expr(
                db,
                expr,
                params,
                tx,
                snapshot,
                bindings,
                cache,
                read_memory.clone(),
            )? {
                Some(value) => Value::Bool(value),
                None => Value::Null,
            }])
        }
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before graph execution".to_string(),
        )),
        Expr::Row(_) => Err(Error::PlanError(
            "a row value is not a scalar a graph pattern can evaluate".to_string(),
        )),
        Expr::CosineDistance { .. } | Expr::RowVectorSource { .. } => Err(Error::PlanError(
            format!("unsupported graph WHERE value expression: {:?}", expr),
        )),
    }
}

fn graph_column_values(
    db: &Database,
    column: &ColumnRef,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,
    cache: &mut GraphValueCache<'_>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<Value>> {
    if let Some(alias) = column.table.as_ref() {
        let Some(node) = bindings.get(alias) else {
            return Ok(vec![Value::Null]);
        };
        return graph_node_column_values(
            db,
            *node,
            &column.column,
            tx,
            snapshot,
            cache,
            read_memory.clone(),
        );
    }

    if column.column == "id" {
        let values = bindings
            .values()
            .copied()
            .map(Value::Uuid)
            .collect::<Vec<_>>();
        return Ok(if values.is_empty() {
            vec![Value::Null]
        } else {
            values
        });
    }

    if bindings.len() == 1 {
        let node = *bindings.values().next().expect("length checked above");
        return graph_node_column_values(
            db,
            node,
            &column.column,
            tx,
            snapshot,
            cache,
            read_memory.clone(),
        );
    }

    Ok(vec![Value::Null])
}

fn graph_node_column_values(
    db: &Database,
    node: uuid::Uuid,
    column: &str,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    cache: &mut GraphValueCache<'_>,

    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<Value>> {
    if column == "id" {
        return Ok(vec![Value::Uuid(node)]);
    }

    let key = (node, column.to_string());
    if let Some(values) = cache.values.get(&key) {
        return Ok(values.clone());
    }

    let values = db.readable_graph_node_column_values_within(
        tx,
        node,
        column,
        snapshot,
        cache.work,
        &mut cache.memory,
        cache.read_memory.clone(),
    )?;
    let values = if values.is_empty() {
        vec![Value::Null]
    } else {
        values
    };
    cache.values.insert(key, values.clone());
    Ok(values)
}

fn graph_compare_any(
    left: &[Value],
    right: &[Value],
    op: BinOp,
    work: &mut dyn FnMut(GraphPredicateWork) -> Result<()>,
) -> Result<Option<bool>> {
    let mut saw_unknown = false;
    for left in left {
        for right in right {
            work(GraphPredicateWork::Checkpoint)?;
            if *left == Value::Null || *right == Value::Null {
                saw_unknown = true;
                continue;
            }
            let matched = match op {
                BinOp::Eq => compare_values(left, right) == Some(Ordering::Equal) || left == right,
                BinOp::Neq => {
                    !(compare_values(left, right) == Some(Ordering::Equal) || left == right)
                }
                BinOp::Lt => compare_values(left, right) == Some(Ordering::Less),
                BinOp::Lte => matches!(
                    compare_values(left, right),
                    Some(Ordering::Less | Ordering::Equal)
                ),
                BinOp::Gt => compare_values(left, right) == Some(Ordering::Greater),
                BinOp::Gte => matches!(
                    compare_values(left, right),
                    Some(Ordering::Greater | Ordering::Equal)
                ),
                BinOp::And | BinOp::Or => unreachable!(),
            };
            if matched {
                return Ok(Some(true));
            }
        }
    }
    Ok(if saw_unknown { None } else { Some(false) })
}

fn graph_uuid_from_value(value: Value, context: &str) -> Result<uuid::Uuid> {
    match value {
        Value::Uuid(id) => Ok(id),
        Value::Text(text) => uuid::Uuid::parse_str(&text)
            .map_err(|_| Error::PlanError(format!("invalid UUID in {context}: {text}"))),
        other => Err(Error::PlanError(format!("invalid {context}: {other:?}"))),
    }
}

fn resolve_graph_static_uuid_expr(
    expr: &Expr,
    params: &HashMap<String, Value>,
    context: &str,
) -> Result<Option<uuid::Uuid>> {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => {
            graph_uuid_from_value(resolve_expr(expr, params)?, context).map(Some)
        }
        _ => Ok(None),
    }
}

fn is_graph_alias_column_ref(expr: &Expr, alias: &str, column: &str) -> bool {
    matches!(
        expr,
        Expr::Column(contextdb_parser::ast::ColumnRef {
            table: Some(table),
            column: col,
        }) if table == alias && col == column
    )
}

fn is_graph_start_column_ref(expr: &Expr, alias: &str, column: &str) -> bool {
    matches!(
        expr,
        Expr::Column(contextdb_parser::ast::ColumnRef {
            table: Some(table),
            column: col,
        }) if table == alias && col == column
    ) || matches!(
        expr,
        Expr::Column(contextdb_parser::ast::ColumnRef {
            table: None,
            column: col,
        }) if col == column
    )
}

fn graph_start_resolution_filter(filter: &Expr, start_alias: &str) -> Option<Expr> {
    let conjuncts = split_conjuncts(filter)
        .into_iter()
        .filter(|conjunct| graph_expr_refs_only_start_alias(conjunct, start_alias))
        .collect::<Vec<_>>();
    combine_conjuncts(conjuncts)
}

fn graph_start_columns(expr: &Expr, start_alias: &str) -> BTreeSet<String> {
    fn walk(expr: &Expr, start_alias: &str, columns: &mut BTreeSet<String>) {
        match expr {
            Expr::RowNumber(_) | Expr::WindowRank(_) => {}
            Expr::Column(contextdb_parser::ast::ColumnRef {
                table: Some(table),
                column,
            }) if table == start_alias => {
                columns.insert(column.clone());
            }
            Expr::Column(contextdb_parser::ast::ColumnRef {
                table: None,
                column,
            }) if column == "id" => {
                columns.insert(column.clone());
            }
            Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
                walk(left, start_alias, columns);
                walk(right, start_alias, columns);
            }
            Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
                walk(operand, start_alias, columns);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    walk(arg, start_alias, columns);
                }
            }
            Expr::InList { expr, list, .. } => {
                walk(expr, start_alias, columns);
                for item in list {
                    walk(item, start_alias, columns);
                }
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, start_alias, columns);
                walk(pattern, start_alias, columns);
            }
            Expr::InSubquery { expr, .. } => walk(expr, start_alias, columns),
            Expr::Row(elements) => {
                for element in elements {
                    walk(element, start_alias, columns);
                }
            }
            Expr::Column(_)
            | Expr::Literal(_)
            | Expr::Parameter(_)
            | Expr::RowVectorSource { .. } => {}
        }
    }

    let mut columns = BTreeSet::new();
    walk(expr, start_alias, &mut columns);
    columns
}

fn graph_start_filter_needs_unpinned_null_semantics(expr: &Expr, start_alias: &str) -> bool {
    fn walk(expr: &Expr, start_alias: &str, not_parity: bool) -> bool {
        match expr {
            Expr::RowNumber(_) | Expr::WindowRank(_) => false,
            Expr::IsNull { expr, negated } => {
                let matches_missing_metadata = !(*negated ^ not_parity);
                matches_missing_metadata
                    && graph_start_columns(expr, start_alias)
                        .into_iter()
                        .any(|column| column != "id")
            }
            Expr::UnaryOp {
                op: UnaryOp::Not,
                operand,
            } => walk(operand, start_alias, !not_parity),
            Expr::UnaryOp { operand, .. } => walk(operand, start_alias, not_parity),
            Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
                walk(left, start_alias, not_parity) || walk(right, start_alias, not_parity)
            }
            Expr::FunctionCall { args, .. } => {
                args.iter().any(|arg| walk(arg, start_alias, not_parity))
            }
            Expr::InList { expr, list, .. } => {
                walk(expr, start_alias, not_parity)
                    || list.iter().any(|item| walk(item, start_alias, not_parity))
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, start_alias, not_parity) || walk(pattern, start_alias, not_parity)
            }
            Expr::InSubquery { expr, .. } => walk(expr, start_alias, not_parity),
            Expr::Row(_)
            | Expr::Column(_)
            | Expr::Literal(_)
            | Expr::Parameter(_)
            | Expr::RowVectorSource { .. } => false,
        }
    }

    walk(expr, start_alias, false)
}

fn graph_expr_refs_only_start_alias(expr: &Expr, start_alias: &str) -> bool {
    fn walk(expr: &Expr, start_alias: &str, saw_start: &mut bool, saw_other: &mut bool) {
        match expr {
            Expr::RowNumber(_) | Expr::WindowRank(_) => *saw_other = true,
            Expr::Column(contextdb_parser::ast::ColumnRef { table, .. }) => {
                match table.as_deref() {
                    Some(alias) if alias == start_alias => *saw_start = true,
                    None => {
                        if let Expr::Column(contextdb_parser::ast::ColumnRef { column, .. }) = expr
                            && column == "id"
                        {
                            *saw_start = true;
                        } else {
                            *saw_other = true;
                        }
                    }
                    _ => *saw_other = true,
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                walk(left, start_alias, saw_start, saw_other);
                walk(right, start_alias, saw_start, saw_other);
            }
            Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
                walk(operand, start_alias, saw_start, saw_other);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    walk(arg, start_alias, saw_start, saw_other);
                }
            }
            Expr::InList { expr, list, .. } => {
                walk(expr, start_alias, saw_start, saw_other);
                for item in list {
                    walk(item, start_alias, saw_start, saw_other);
                }
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, start_alias, saw_start, saw_other);
                walk(pattern, start_alias, saw_start, saw_other);
            }
            Expr::InSubquery { expr, .. } | Expr::CosineDistance { left: expr, .. } => {
                walk(expr, start_alias, saw_start, saw_other);
                *saw_other = true;
            }
            Expr::Row(elements) => {
                for element in elements {
                    walk(element, start_alias, saw_start, saw_other);
                }
            }
            Expr::RowVectorSource { .. } => *saw_other = true,
            Expr::Literal(_) | Expr::Parameter(_) => {}
        }
    }

    let mut saw_start = false;
    let mut saw_other = false;
    walk(expr, start_alias, &mut saw_start, &mut saw_other);
    saw_start && !saw_other
}

fn combine_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut iter = conjuncts.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    }))
}

fn resolve_vector_from_expr(expr: &Expr, params: &HashMap<String, Value>) -> Result<Vec<f32>> {
    match resolve_expr(expr, params)? {
        Value::Vector(v) => Ok(v),
        Value::Text(text) if text.trim_start().starts_with('[') => parse_text_vector_literal(&text),
        Value::Text(name) => match params.get(&name) {
            Some(Value::Vector(v)) => Ok(v.clone()),
            _ => Err(Error::PlanError("vector parameter missing".to_string())),
        },
        _ => Err(Error::PlanError(
            "invalid vector query expression".to_string(),
        )),
    }
}

fn resolve_query_vector_from_expr(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: SnapshotId,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<(Vec<f32>, Option<contextdb_core::VectorIndexRef>)> {
    match expr {
        Expr::RowVectorSource { table, column, key } => {
            let index = contextdb_core::VectorIndexRef::new(table.clone(), column.clone());
            let vector = resolve_row_vector_source(
                db,
                &index,
                key,
                params,
                tx,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            Ok((vector, Some(index)))
        }
        _ => Ok((resolve_vector_from_expr(expr, params)?, None)),
    }
}

fn resolve_row_vector_source(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    key_expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: SnapshotId,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<f32>> {
    let meta = db
        .table_meta(&index.table)
        .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
    db.assert_table_read_allowed(&index.table)?;
    if !meta.columns.iter().any(|column| {
        column.name == index.column
            && matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
    }) {
        return Err(Error::UnknownVectorIndex {
            index: index.clone(),
        });
    }
    db.assert_vector_index_exists_under_schema_read(index)?;

    let raw_key = resolve_expr(key_expr, params)?;
    if matches!(raw_key, Value::Null) {
        return Err(Error::PlanError(
            "ROW_VECTOR key cannot be NULL".to_string(),
        ));
    }
    if matches!(raw_key, Value::Vector(_)) {
        return Err(Error::PlanError(
            "ROW_VECTOR key cannot be a vector".to_string(),
        ));
    }

    let key_column = db.natural_key_column_for_table(&index.table)?;
    let key =
        coerce_into_column(db, &index.table, &key_column, raw_key, None, tx).map_err(|err| {
            Error::PlanError(format!(
                "ROW_VECTOR argument 3 key cannot be coerced to `{}`.`{}` natural key: {err}",
                index.table, key_column
            ))
        })?;
    if matches!(key, Value::Null) {
        return Err(Error::PlanError(
            "ROW_VECTOR key cannot be NULL".to_string(),
        ));
    }
    let key_label = row_vector_key_label(&key);
    let row_id = db
        .row_id_for_natural_key_in_tx(
            tx,
            &index.table,
            &key_column,
            &key,
            snapshot,
            read_memory.clone(),
        )?
        .ok_or_else(|| Error::PersistedRowVectorRowMissing {
            index: index.clone(),
            key: key_label.clone(),
        })?;
    let vector = db
        .with_row_vector_source(
            index,
            row_id,
            snapshot,
            tx,
            |_| Ok::<(), Error>(()),
            |vector| Ok::<Vec<f32>, Error>(vector),
            vector_copy_owner,
            read_memory.clone(),
        )?
        .ok_or_else(|| Error::PersistedRowVectorCellNull {
            index: index.clone(),
            key: key_label,
        })?;
    db.validate_vector_under_schema_read(index, vector.len())?;
    Ok(vector)
}

fn row_vector_key_label(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Uuid(value) => value.to_string(),
        Value::Timestamp(value) => value.to_string(),
        Value::Json(value) => value.to_string(),
        Value::Vector(_) => "<vector>".to_string(),
        Value::TxId(value) => value.to_string(),
    }
}

fn validate_vector_columns(
    db: &Database,
    table: &str,
    values: &HashMap<String, Value>,
) -> Result<()> {
    let Some(meta) = db.table_meta(table) else {
        return Ok(());
    };

    for column in &meta.columns {
        if let contextdb_core::ColumnType::Vector(expected) = column.column_type
            && let Some(Value::Vector(vector)) = values.get(&column.name)
        {
            let got = vector.len();
            if got != expected {
                return Err(vector_dimension_error(table, &column.name, expected, got));
            }
        }
    }

    Ok(())
}

fn vector_columns_for_meta(meta: &TableMeta) -> Vec<String> {
    meta.columns
        .iter()
        .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)))
        .map(|column| column.name.clone())
        .collect()
}

fn vector_refs_for_meta(table: &str, meta: &TableMeta) -> Vec<contextdb_core::VectorIndexRef> {
    meta.columns
        .iter()
        .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)))
        .map(|column| contextdb_core::VectorIndexRef::new(table, column.name.clone()))
        .collect()
}

fn has_edge_columns(meta: &TableMeta) -> bool {
    [
        ("source_id", ColumnType::Uuid),
        ("target_id", ColumnType::Uuid),
        ("edge_type", ColumnType::Text),
    ]
    .into_iter()
    .all(|(name, column_type)| has_exact_column_type(meta, name, &column_type))
}

fn has_exact_column_type(meta: &TableMeta, name: &str, column_type: &ColumnType) -> bool {
    let mut columns = meta.columns.iter().filter(|column| column.name == name);
    matches!(columns.next(), Some(column) if &column.column_type == column_type)
        && columns.next().is_none()
}

fn vector_indexes_for_table(db: &Database, table: &str) -> Vec<contextdb_core::VectorIndexRef> {
    db.table_meta(table)
        .map(|meta| {
            meta.columns
                .iter()
                .filter(|column| {
                    matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                })
                .map(|column| contextdb_core::VectorIndexRef::new(table, column.name.clone()))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn coerce_into_column(
    db: &Database,
    table: &str,
    col: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    coerce_value_for_column(db, table, col, v, current_tx_max, active_tx)
}

pub(crate) fn coerce_into_column_with_meta(
    table: &str,
    meta: &TableMeta,
    col: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    coerce_value_for_column_with_meta(table, meta, col, v, current_tx_max, active_tx)
}

fn coerce_value_for_column(
    db: &Database,
    table: &str,
    col_name: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    let Some(meta) = db.table_meta(table) else {
        // Non-TxId variant: pass through with lenient id-name coercion.
        if let Value::TxId(_) = &v {
            return Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "UNKNOWN".to_owned(),
                actual: "TxId".to_owned(),
            });
        }
        return Ok(coerce_uuid_if_needed(col_name, v));
    };
    coerce_value_for_column_with_meta(table, &meta, col_name, v, current_tx_max, active_tx)
}

fn coerce_value_for_column_with_meta(
    table: &str,
    meta: &TableMeta,
    col_name: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    let Some(col) = meta.columns.iter().find(|c| c.name == col_name) else {
        if let Value::TxId(_) = &v {
            return Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "UNKNOWN".to_owned(),
                actual: "TxId".to_owned(),
            });
        }
        return Ok(coerce_uuid_if_needed(col_name, v));
    };

    match col.column_type {
        contextdb_core::ColumnType::Uuid => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "UUID".to_owned(),
                actual: "TxId".to_owned(),
            }),
            other => coerce_uuid_value(other),
        },
        contextdb_core::ColumnType::Timestamp => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "TIMESTAMP".to_owned(),
                actual: "TxId".to_owned(),
            }),
            other => coerce_timestamp_value(other),
        },
        contextdb_core::ColumnType::Vector(dim) => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: format_vector_type(dim).to_owned(),
                actual: "TxId".to_owned(),
            }),
            other => coerce_vector_value(table, col_name, other, dim, col.quantization),
        },
        contextdb_core::ColumnType::Integer => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "INTEGER".to_owned(),
                actual: "TxId".to_owned(),
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::Real => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "REAL".to_owned(),
                actual: "TxId".to_owned(),
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::Text => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "TEXT".to_owned(),
                actual: "TxId".to_owned(),
            }),
            // A column DECLARED `TEXT` preserves its text — including a value that happens to parse
            // as a UUID. The id-name heuristic in `coerce_uuid_if_needed` is for untyped/UUID
            // columns; applying it to a declared-TEXT column would silently convert a TEXT id (e.g.
            // a `cg_skill_firing_trace.id` string) into a `Value::Uuid`, which then mismatches the
            // `(Text, Text)` probe arm and reads back as the wrong value type.
            other => Ok(other),
        },
        contextdb_core::ColumnType::Boolean => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "BOOLEAN".to_owned(),
                actual: "TxId".to_owned(),
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::Json => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "JSON".to_owned(),
                actual: "TxId".to_owned(),
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::TxId => {
            coerce_txid_value(table, col_name, v, col.nullable, current_tx_max, active_tx)
        }
    }
}

fn coerce_insert_value_for_column_with_meta(
    table: &str,
    meta: &TableMeta,
    col_name: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    let should_auto_stamp_null = meta
        .columns
        .iter()
        .find(|column| column.name == col_name)
        .is_some_and(|column| {
            !column.nullable
                && matches!(column.column_type, contextdb_core::ColumnType::TxId)
                && matches!(&v, Value::Null)
        });
    if should_auto_stamp_null {
        let tx = active_tx.ok_or_else(|| Error::Other("missing active tx".to_string()))?;
        return Ok(Value::TxId(tx));
    }

    coerce_value_for_column_with_meta(table, meta, col_name, v, current_tx_max, active_tx)
}

fn format_vector_type(dim: usize) -> &'static str {
    // We need &'static str for the error variant. Fall back to a lookup for common dims.
    match dim {
        1 => "VECTOR(1)",
        2 => "VECTOR(2)",
        3 => "VECTOR(3)",
        4 => "VECTOR(4)",
        8 => "VECTOR(8)",
        16 => "VECTOR(16)",
        32 => "VECTOR(32)",
        64 => "VECTOR(64)",
        128 => "VECTOR(128)",
        256 => "VECTOR(256)",
        512 => "VECTOR(512)",
        768 => "VECTOR(768)",
        1024 => "VECTOR(1024)",
        1536 => "VECTOR(1536)",
        3072 => "VECTOR(3072)",
        _ => "VECTOR",
    }
}

fn coerce_txid_value(
    table: &str,
    col: &str,
    v: Value,
    nullable: bool,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    match v {
        Value::Null => {
            if nullable {
                Ok(Value::Null)
            } else {
                Err(Error::ColumnNotNullable {
                    table: table.to_string(),
                    column: col.to_string(),
                })
            }
        }
        Value::TxId(tx) => {
            // Plan B7: `Value::TxId(n)` into a TXID column requires
            // `n <= max(committed_watermark, active_tx)`. The watermark is the
            // statement-scoped `current_tx_max` snapshot from
            // `TransactionManager::current_tx_max()`; `active_tx` is the in-flight
            // transaction that allocated the caller's TxId, which is permitted
            // as a self-reference. The error reports the watermark so callers
            // see what their edge has committed. Non-SQL callers pass `None`
            // for `current_tx_max` and skip the check.
            if let Some(max) = current_tx_max {
                let ceiling = max.0.max(active_tx.map(|t| t.0).unwrap_or(0));
                if tx.0 > ceiling {
                    return Err(Error::TxIdOutOfRange {
                        table: table.to_string(),
                        column: col.to_string(),
                        value: tx.0,
                        max: max.0,
                    });
                }
            }
            Ok(Value::TxId(tx))
        }
        other => Err(Error::ColumnTypeMismatch {
            table: table.to_string(),
            column: col.to_string(),
            expected: "TXID".to_owned(),
            actual: value_variant_name(&other).to_owned(),
        }),
    }
}

fn value_variant_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int64(_) => "Int64",
        Value::Float64(_) => "Float64",
        Value::Text(_) => "Text",
        Value::Uuid(_) => "Uuid",
        Value::Timestamp(_) => "Timestamp",
        Value::Json(_) => "Json",
        Value::Vector(_) => "Vector",
        Value::TxId(_) => "TxId",
    }
}

fn coerce_uuid_value(v: Value) -> Result<Value> {
    match v {
        Value::Null => Ok(Value::Null),
        Value::Uuid(id) => Ok(Value::Uuid(id)),
        Value::Text(text) => uuid::Uuid::parse_str(&text)
            .map(Value::Uuid)
            .map_err(|err| Error::Other(format!("invalid UUID literal '{text}': {err}"))),
        other => Err(Error::Other(format!(
            "UUID column requires UUID or text literal, got {other:?}"
        ))),
    }
}

fn coerce_uuid_if_needed(col: &str, v: Value) -> Value {
    if (col == "id" || col.ends_with("_id"))
        && let Value::Text(s) = &v
        && let Ok(u) = uuid::Uuid::parse_str(s)
    {
        return Value::Uuid(u);
    }
    v
}

fn coerce_timestamp_value(v: Value) -> Result<Value> {
    match v {
        Value::Null => Ok(Value::Null),
        Value::Text(text) if text.eq_ignore_ascii_case("infinity") => {
            Ok(Value::Timestamp(i64::MAX))
        }
        Value::Text(text) => {
            let parsed = OffsetDateTime::parse(&text, &Rfc3339).map_err(|err| {
                Error::Other(format!("invalid TIMESTAMP literal '{text}': {err}"))
            })?;
            Ok(Value::Timestamp(
                parsed.unix_timestamp_nanos() as i64 / 1_000_000,
            ))
        }
        other => Ok(other),
    }
}

fn coerce_vector_value(
    table: &str,
    column: &str,
    v: Value,
    expected_dim: usize,
    quantization: contextdb_core::VectorQuantization,
) -> Result<Value> {
    // A vector column holds embeddings and nothing else. Anything that is not
    // an absent embedding, an embedding, or the text spelling of one is
    // refused here -- at the door, before the statement stages a row or a
    // searchable vector -- so a caller that binds a scalar by mistake is told
    // which column rejected it instead of having the store accept a value the
    // column cannot hold and surfacing it only when the file is read back.
    let vector = match v {
        Value::Null => return Ok(Value::Null),
        Value::Vector(vector) => vector,
        Value::Text(text) => parse_text_vector_literal(&text)?,
        other => {
            return Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: column.to_string(),
                expected: format_vector_type(expected_dim).to_owned(),
                actual: value_variant_name(&other).to_owned(),
            });
        }
    };

    if vector.len() != expected_dim {
        return Err(vector_dimension_error(
            table,
            column,
            expected_dim,
            vector.len(),
        ));
    }

    // A column declared with space-saving storage keeps an approximation, so
    // the value the row carries from here on is the value that will be
    // stored. Taking the loss once, at the boundary where the value enters
    // the column, is what makes the same query answer the same way while the
    // writer is still warm, after a restart, and through a reader of the
    // closed file -- instead of the writer alone answering from a precision
    // that no longer exists anywhere else.
    Ok(Value::Vector(contextdb_vector::stored_vector_value(
        &vector,
        quantization,
    )))
}

fn vector_dimension_error(table: &str, column: &str, expected: usize, got: usize) -> Error {
    Error::VectorIndexDimensionMismatch {
        index: contextdb_core::VectorIndexRef::new(table, column),
        expected,
        actual: got,
    }
}

fn parse_text_vector_literal(text: &str) -> Result<Vec<f32>> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| Error::Other(format!("invalid VECTOR literal '{text}'")))?;

    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    inner
        .split(',')
        .map(|part| {
            part.trim().parse::<f32>().map_err(|err| {
                Error::Other(format!("invalid VECTOR component '{}': {err}", part.trim()))
            })
        })
        .collect()
}

fn apply_missing_column_defaults(
    db: &Database,
    table: &str,
    values: &mut HashMap<String, Value>,
    active_tx: Option<TxId>,
) -> Result<()> {
    let Some(meta) = db.table_meta(table) else {
        return Ok(());
    };

    let current_tx_max = Some(db.committed_watermark());

    for column in &meta.columns {
        if !column.nullable && matches!(column.column_type, ColumnType::TxId) {
            if matches!(values.get(&column.name), None | Some(Value::Null)) {
                let tx = active_tx.ok_or_else(|| Error::Other("missing active tx".to_string()))?;
                values.insert(column.name.clone(), Value::TxId(tx));
            }
            continue;
        }

        if values.contains_key(&column.name) {
            continue;
        }
        let Some(default) = &column.default else {
            continue;
        };
        let value = evaluate_stored_default_expr(default)?;
        // A bare numeric default ("0", "5") is ambiguous between INTEGER and
        // REAL at the string level -- `REAL DEFAULT 0.0` and `INTEGER DEFAULT
        // 0` both stringify to "0". `evaluate_stored_default_expr` returns
        // the natural parse (Int64), so widen to Float64 here where the
        // column's declared type is known.
        let value = match (&column.column_type, &value) {
            (ColumnType::Real, Value::Int64(n)) => Value::Float64(*n as f64),
            _ => value,
        };
        values.insert(
            column.name.clone(),
            coerce_value_for_column(db, table, &column.name, value, current_tx_max, active_tx)?,
        );
    }

    Ok(())
}

fn evaluate_stored_default_expr(default: &str) -> Result<Value> {
    if default.eq_ignore_ascii_case("NOW()") {
        return eval_function("now", &[]);
    }
    if default.contains("FunctionCall") && default.contains("name: \"NOW\"") {
        return eval_function("now", &[]);
    }
    if default == "Literal(Null)" || default.eq_ignore_ascii_case("NULL") {
        return Ok(Value::Null);
    }
    if default.eq_ignore_ascii_case("TRUE") {
        return Ok(Value::Bool(true));
    }
    if default.eq_ignore_ascii_case("FALSE") {
        return Ok(Value::Bool(false));
    }
    if default.starts_with('\'') && default.ends_with('\'') && default.len() >= 2 {
        return Ok(Value::Text(
            default[1..default.len() - 1].replace("''", "'"),
        ));
    }
    if let Some(text) = default
        .strip_prefix("Literal(Text(\"")
        .and_then(|value| value.strip_suffix("\"))"))
    {
        return Ok(Value::Text(text.to_string()));
    }
    if let Some(value) = default
        .strip_prefix("Literal(Integer(")
        .and_then(|value| value.strip_suffix("))"))
    {
        let parsed = value.parse::<i64>().map_err(|err| {
            Error::Other(format!("invalid stored integer default '{value}': {err}"))
        })?;
        return Ok(Value::Int64(parsed));
    }
    if let Some(value) = default
        .strip_prefix("Literal(Real(")
        .and_then(|value| value.strip_suffix("))"))
    {
        let parsed = value
            .parse::<f64>()
            .map_err(|err| Error::Other(format!("invalid stored real default '{value}': {err}")))?;
        return Ok(Value::Float64(parsed));
    }
    if let Some(value) = default
        .strip_prefix("Literal(Bool(")
        .and_then(|value| value.strip_suffix("))"))
    {
        let parsed = value
            .parse::<bool>()
            .map_err(|err| Error::Other(format!("invalid stored bool default '{value}': {err}")))?;
        return Ok(Value::Bool(parsed));
    }
    // `stored_default_expr` serializes INTEGER and REAL defaults as a bare
    // number ("5", "0", "3.5"), not the wrapped `Literal(Integer(..))` /
    // `Literal(Real(..))` shape the branches above accept -- so this is the
    // shape produced by this engine's stored-default encoder. Try an
    // integer parse first so a
    // round-numbered value keeps its exact integer representation; a
    // decimal or exponent falls through to the float parse.
    if let Ok(parsed) = default.parse::<i64>() {
        return Ok(Value::Int64(parsed));
    }
    if let Ok(parsed) = default.parse::<f64>() {
        return Ok(Value::Float64(parsed));
    }

    Err(Error::Other(format!(
        "unsupported stored DEFAULT expression: {default}"
    )))
}

pub(crate) fn stored_default_expr(expr: &Expr) -> String {
    match expr {
        Expr::Literal(Literal::Null) => "NULL".to_string(),
        Expr::Literal(Literal::Bool(value)) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Expr::Literal(Literal::Integer(value)) => value.to_string(),
        Expr::Literal(Literal::Real(value)) => value.to_string(),
        Expr::Literal(Literal::Text(value)) => format!("'{}'", value.replace('\'', "''")),
        Expr::FunctionCall { name, args }
            if name.eq_ignore_ascii_case("NOW") && args.is_empty() =>
        {
            "NOW()".to_string()
        }
        _ => format!("{expr:?}"),
    }
}

fn validate_expires_column(col: &contextdb_parser::ast::ColumnDef) -> Result<()> {
    if col.expires && !matches!(col.data_type, DataType::Timestamp) {
        return Err(Error::Other(
            "EXPIRES is only valid on TIMESTAMP columns".to_string(),
        ));
    }
    Ok(())
}

/// The direction a table actually has: the one it declared, or the engine
/// default when it declared none.
pub(crate) fn effective_sync_direction(meta: &TableMeta) -> SyncDirection {
    meta.sync_direction.unwrap_or(DEFAULT_SYNC_DIRECTION)
}

/// `SYNC SAFE` promises that a row is not expired locally until the
/// destination confirms receipt. A direction that never sends the table
/// anywhere makes that promise unkeepable — the rows would simply never
/// expire. So the contradiction is refused at the moment it is written, at
/// every door: CREATE, ALTER, and DDL arriving from another machine.
///
/// Plain retention with no delivery promise is untouched: a colocated
/// installation that keeps one copy declares `RETAIN … SYNC OFF` and is
/// entirely legal.
pub(crate) fn refuse_promise_with_no_delivery(
    table: &str,
    sync_safe: bool,
    direction: Option<SyncDirection>,
) -> Result<()> {
    if !sync_safe {
        return Ok(());
    }
    let direction = direction.unwrap_or(DEFAULT_SYNC_DIRECTION);
    if direction.delivers() {
        return Ok(());
    }
    Err(Error::SchemaInvalid {
        reason: format!(
            "table '{table}' declares SYNC SAFE, which promises that a row is not expired here \
             until the destination confirms receipt — but {} never delivers this table \
             anywhere, so the promise could never be kept and the rows would never expire. \
             Declare SYNC PUSH ONLY or SYNC TWO WAY, or drop SYNC SAFE and keep plain RETAIN.",
            direction.sql()
        ),
    })
}

/// `SYNC SAFE` promises delete-only-AFTER-DELIVERY, and delivery needs a key
/// the hub can identify the row by: `changes_since` builds a changeset row from
/// the table's natural key, so a table with none never gets its rows onto the
/// wire at all — while the push still reports success and advances the
/// watermark past their LSNs. The gate would then open on rows the hub never
/// received and retention would delete them. So the promise is refused wherever
/// it is declared on a table that cannot keep it. Plain `RETAIN` is untouched:
/// it prunes locally and promises no delivery.
pub(crate) fn refuse_sync_safe_without_key(table: &str, meta: &TableMeta) -> Result<()> {
    if !meta.sync_safe {
        return Ok(());
    }
    refuse_sync_safe_without_key_for(table, meta)
}

/// The key check itself, for callers that know `SYNC SAFE` is being declared.
pub(crate) fn refuse_sync_safe_without_key_for(table: &str, meta: &TableMeta) -> Result<()> {
    if crate::sync_types::natural_key_column_for_meta(meta).is_some() {
        return Ok(());
    }
    Err(Error::SchemaInvalid {
        reason: format!(
            "SYNC SAFE on table '{table}' requires a PRIMARY KEY: a row with no key never \
             enters a pushed changeset, so the delete-after-delivery gate would open on rows \
             the hub never received and retention would delete them undelivered. Declare a \
             PRIMARY KEY whose values are unique across every NODE that writes this table — \
             include an origin identifier, or use globally unique ids — since two edges \
             pushing the same key collide at the hub. Plain RETAIN without SYNC SAFE needs \
             no key."
        ),
    })
}

/// `HISTORY CURRENT ONLY` declares that only a row's CURRENT version has
/// consumer value, so superseded versions may be reclaimed. That is only
/// safe when this table cannot hand a peer a stale "first" value: a machine
/// that PULLS this table under `SYNC CONFLICT KEEP FIRST` (declared or
/// defaulted — [`DEFAULT_CONFLICT_POLICY`] is keep-first) files whatever
/// value it receives as the FIRST value it has ever seen for that key. Once
/// this table has reclaimed history, the only value left to send IS the
/// newest one — so a puller under keep-first arbitration silently accepts a
/// stale, wrong "first" value.
///
/// The hazard is conditioned on `SyncDirection::delivers()`, not on the
/// policy alone: a table that never sends anywhere (`SYNC OFF` / `SYNC PULL
/// ONLY`) has no puller and so no hazard, whatever its conflict policy reads.
///
/// Takes the POST-change [`TableMeta`] so every door that can produce the
/// combination — local `CREATE TABLE`, the `SET HISTORY` / `SET SYNC
/// CONFLICT` / `SET SYNC ...` ALTER arms, an arriving `CreateTable` (fresh or
/// adopting an existing table), and an arriving `AlterTable` — asks the same
/// question about the same projected shape.
pub(crate) fn refuse_reclaimed_history_under_keep_first(
    table: &str,
    meta: &TableMeta,
) -> Result<()> {
    if meta.history_policy != Some(HistoryPolicy::CurrentOnly) {
        return Ok(());
    }
    if !effective_sync_direction(meta).delivers() {
        return Ok(());
    }
    let conflict = meta.conflict_policy.unwrap_or(DEFAULT_CONFLICT_POLICY);
    if conflict != contextdb_core::ConflictPolicy::KEEP_FIRST {
        return Ok(());
    }
    Err(Error::SchemaInvalid {
        reason: format!(
            "table '{table}': HISTORY CURRENT ONLY needs SYNC CONFLICT KEEP LATEST. \
             Reclaiming superseded versions leaves only the newest value, and a machine that \
             pulls this table under SYNC CONFLICT KEEP FIRST (the default when no conflict \
             clause is declared) would file that newest value as the FIRST value for its key. \
             Declare SYNC CONFLICT KEEP LATEST if the table carries current truth, or SYNC OFF \
             if it never leaves this machine."
        ),
    })
}

/// The four built-in ledger tables whose `RETAIN` / `HISTORY` / `SYNC
/// CONFLICT` clauses are declared exactly once, by the engine itself, in
/// their own `CREATE TABLE` text -- never by an operator. The engine's own
/// bookkeeping depends on each staying at its declared shape: `work_inputs`'
/// `RETAIN` window is what keeps ledger-carried input copies bounded (a
/// dropped or widened window reopens the unbounded-debris class
/// `install_work_ledger_schema` exists to close -- see
/// [`crate::work_ledger::run_input_retention`]'s doc comment); the three
/// built-in CURRENCY tables' `HISTORY CURRENT ONLY` plus their `SYNC
/// CONFLICT KEEP LATEST` (or `SYNC OFF`) is what makes
/// `Database::compact_currency_versions` safe to reclaim their superseded
/// versions at all (see `database.rs`'s "three built-in currency tables"
/// note). See [`engine_owned_ledger_policy`] for the declared shape each one
/// is held to, and [`refuse_engine_owned_policy_mutation`] for the door.
pub(crate) const ENGINE_OWNED_LEDGER_TABLES: [&str; 4] = [
    "work_inputs",
    "work_capabilities",
    "peer_directory",
    "work_node_contacts",
];

/// The `RETAIN` / `HISTORY` / `SYNC CONFLICT` / `SYNC ...` / `SYNC SAFE`
/// shape the engine itself declares for one of [`ENGINE_OWNED_LEDGER_TABLES`],
/// mirrored by hand from each table's own `CREATE TABLE` text
/// (`work_ledger::CREATE_WORK_INPUTS`, `CREATE_WORK_CAPABILITIES`;
/// `peer_directory::CREATE_PEER_DIRECTORY`;
/// `contextdb_server::work_ledger::CREATE_WORK_NODE_CONTACTS` -- one crate
/// up from here and so not importable as a constant; keep these two literal
/// shapes in agreement by hand if either DDL string ever changes). None of
/// the four declares `SYNC SAFE`, so `sync_safe` is `false` for all of them;
/// `work_inputs`, `work_capabilities`, and `peer_directory` explicitly
/// declare `SYNC TWO WAY`; `work_node_contacts` explicitly declares `SYNC
/// OFF`.
pub(crate) struct EngineOwnedLedgerPolicy {
    pub(crate) retain: Option<(u64, RetainUnit)>,
    pub(crate) history: Option<HistoryPolicy>,
    pub(crate) conflict: Option<contextdb_core::ConflictPolicy>,
    pub(crate) direction: Option<SyncDirection>,
    pub(crate) sync_safe: bool,
}

/// `None` for any table name outside [`ENGINE_OWNED_LEDGER_TABLES`].
pub(crate) fn engine_owned_ledger_policy(table: &str) -> Option<EngineOwnedLedgerPolicy> {
    if !ENGINE_OWNED_LEDGER_TABLES.contains(&table) {
        return None;
    }
    match table {
        "work_inputs" => Some(EngineOwnedLedgerPolicy {
            retain: Some((7 * 24 * 60 * 60, RetainUnit::Days)),
            history: None,
            conflict: Some(contextdb_core::ConflictPolicy::KEEP_FIRST),
            direction: Some(SyncDirection::Both),
            sync_safe: false,
        }),
        "work_capabilities" | "peer_directory" => Some(EngineOwnedLedgerPolicy {
            retain: None,
            history: Some(HistoryPolicy::CurrentOnly),
            conflict: Some(contextdb_core::ConflictPolicy::KEEP_LATEST),
            direction: Some(SyncDirection::Both),
            sync_safe: false,
        }),
        "work_node_contacts" => Some(EngineOwnedLedgerPolicy {
            retain: None,
            history: Some(HistoryPolicy::CurrentOnly),
            conflict: None,
            direction: Some(SyncDirection::None),
            sync_safe: false,
        }),
        _ => unreachable!("ENGINE_OWNED_LEDGER_TABLES and this match must name the same tables"),
    }
}

/// Refuses an EXPLICIT engine-owned policy axis the arriving shape carries
/// that differs from [`engine_owned_ledger_policy`]'s canonical value for
/// `table` -- `Ok` for any table outside [`ENGINE_OWNED_LEDGER_TABLES`], `Ok`
/// for an axis the incoming shape is silent on (silence is never judged
/// here -- see [`engine_owned_merged_policy`] for why), and `Ok` for an axis
/// that verbatim-restates the canonical value (a half-healed peer's own
/// in-progress reconcile, or a fully healed peer's real push, must
/// interoperate). Shared by THREE doors that can produce the mutation the
/// local ALTER door ([`refuse_engine_owned_policy_mutation`]) refuses: the
/// arriving `AlterTable` mirror and the arriving `CreateTable` ADOPT arm for a
/// table that already exists locally (`database.rs`'s adopt branch overwrites
/// the SAME fields an `AlterTable` does, so it must ask the identical
/// question), plus the LOCAL fresh `CREATE TABLE` door
/// ([`refuse_engine_owned_reserved_name_shape`]'s policy half, in
/// `execute_plan`'s `CreateTable` arm) for a consumer typing one of the four
/// reserved names directly.
pub(crate) fn refuse_engine_owned_policy_axes(table: &str, incoming: &TableMeta) -> Result<()> {
    let Some(canonical) = engine_owned_ledger_policy(table) else {
        return Ok(());
    };
    if let Some(retain) = incoming
        .default_ttl_seconds
        .zip(incoming.retain_declared_unit)
        && Some(retain) != canonical.retain
    {
        return Err(engine_owned_policy_refusal(table, "RETAIN"));
    }
    if let Some(history) = incoming.history_policy
        && Some(history) != canonical.history
    {
        return Err(engine_owned_policy_refusal(table, "HISTORY"));
    }
    if let Some(conflict) = incoming.conflict_policy
        && Some(conflict) != canonical.conflict
    {
        return Err(engine_owned_policy_refusal(table, "SYNC CONFLICT"));
    }
    if let Some(direction) = incoming.sync_direction
        && Some(direction) != canonical.direction
    {
        return Err(engine_owned_policy_refusal(table, "SYNC"));
    }
    if incoming.sync_safe && !canonical.sync_safe {
        return Err(engine_owned_policy_refusal(table, "SYNC SAFE"));
    }
    Ok(())
}

/// The installer's own `CREATE TABLE` text for one of the nine reserved
/// names -- the SINGLE SOURCE every shape door below parses to get the
/// canonical column shape, replacing an earlier hand-mirrored
/// `(name, data type, is primary key, is nullable)` tuple table (a checklist
/// subset that silently admitted `IMMUTABLE` / `EXPIRES`, since neither was
/// ever a column the tuple had room for). Changing one of the seven
/// `CREATE_WORK_*` constants or `CREATE_PEER_DIRECTORY` now changes every
/// door's canonical shape with it -- there is nothing left here to fall out
/// of sync with the installer's real DDL by hand. `work_node_contacts` is
/// `contextdb-server`'s table (one crate up from here, so its DDL constant
/// is not importable) -- its text is duplicated below; keep it in agreement
/// with `contextdb_server::work_ledger::CREATE_WORK_NODE_CONTACTS` if that
/// string ever changes. `None` for any table name outside the nine -- an
/// operator table name may declare any column shape at all.
fn engine_owned_reserved_table_create_ddl(table: &str) -> Option<&'static str> {
    match table {
        "work_jobs" => Some(crate::work_ledger::CREATE_WORK_JOBS),
        "work_claims" => Some(crate::work_ledger::CREATE_WORK_CLAIMS),
        "work_results" => Some(crate::work_ledger::CREATE_WORK_RESULTS),
        "work_failures" => Some(crate::work_ledger::CREATE_WORK_FAILURES),
        "work_cancellations" => Some(crate::work_ledger::CREATE_WORK_CANCELLATIONS),
        "work_inputs" => Some(crate::work_ledger::CREATE_WORK_INPUTS),
        "work_capabilities" => Some(crate::work_ledger::CREATE_WORK_CAPABILITIES),
        "peer_directory" => Some(crate::peer_directory::CREATE_PEER_DIRECTORY),
        "work_node_contacts" => Some(ENGINE_OWNED_WORK_NODE_CONTACTS_CREATE_DDL),
        _ => None,
    }
}

/// The engine's own hand-duplicated copy of `contextdb-server`'s
/// `work_node_contacts` DDL text (`contextdb_server::work_ledger::
/// CREATE_WORK_NODE_CONTACTS`) -- duplicated, not imported, because that
/// table is `contextdb-server`'s (one crate up from here) and this crate
/// cannot depend on it without an inversion. `pub` (not `pub(crate)`) so a
/// downstream drift test (in `contextdb-server`, which already depends on
/// this crate) can assert the two texts stay byte-identical rather than
/// trusting a doc-comment promise alone.
pub const ENGINE_OWNED_WORK_NODE_CONTACTS_CREATE_DDL: &str = "CREATE TABLE work_node_contacts (\
     node_id TEXT PRIMARY KEY, \
     last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF";

/// The canonical PARSED `CreateTable` statement for one of the nine
/// reserved names -- [`engine_owned_reserved_table_create_ddl`]'s own text,
/// run through the REAL parser (the identical parse every operator-typed
/// `CREATE TABLE` goes through), not a hand-typed restatement of it. The
/// WHOLE statement, not just `.columns`: an earlier revision of this door
/// held only the column list here, so the table-LEVEL options (`IMMUTABLE`
/// / `STATE_MACHINE` / `DAG` / `PROPAGATE`) had no canonical value to
/// compare against and repeated the same failure one level up (a later
/// review's finding -- see
/// [`refuse_reserved_name_table_shape`]). `None` for any table name outside
/// the nine.
fn engine_owned_reserved_table_canonical_create(
    table: &str,
) -> Option<contextdb_parser::ast::CreateTable> {
    let ddl = engine_owned_reserved_table_create_ddl(table)?;
    match contextdb_parser::parse(ddl) {
        Ok(contextdb_parser::ast::Statement::CreateTable(create)) => Some(create),
        other => unreachable!(
            "engine_owned_reserved_table_create_ddl's own text for {table} must parse as a \
             CreateTable statement: {other:?}"
        ),
    }
}

/// Whether `candidate` structurally matches `canonical` on EVERY field
/// `ColumnDef` carries -- an EXHAUSTIVE destructure of `candidate`, never a
/// `..` pattern: if a future AST change adds a field to `ColumnDef`, this
/// fails to COMPILE until this door is told how the new field
/// participates, instead of silently ignoring it the way an earlier
/// count/name/type/primary-key/nullable subset let `IMMUTABLE` and
/// `EXPIRES` slip through unseen.
///
/// None of the nine reserved tables' own `CREATE TABLE` text ever declares
/// a `DEFAULT`, `REFERENCES`, `RANK_POLICY`, a context-id column, a scope
/// label, or an ACL reference -- so `canonical` never carries one either;
/// asserted on both sides below (never just assumed of `canonical`) so a
/// future installer DDL that DID start declaring one of these would fail
/// the comparison loudly rather than being silently exempted.
fn column_def_matches_canonical(
    candidate: &contextdb_parser::ast::ColumnDef,
    canonical: &contextdb_parser::ast::ColumnDef,
) -> bool {
    let contextdb_parser::ast::ColumnDef {
        name,
        data_type,
        nullable,
        primary_key,
        unique,
        default,
        references,
        expires,
        immutable,
        quantization,
        partition_key_columns,
        max_partitions,
        search_mode,
        auto_index_at,
        hnsw,
        consolidation,
        rank_policy,
        context_id,
        scope_label,
        acl_ref,
    } = candidate;
    *name == canonical.name
        && *data_type == canonical.data_type
        && *nullable == canonical.nullable
        && *primary_key == canonical.primary_key
        && *unique == canonical.unique
        && default.is_none()
        && canonical.default.is_none()
        && references.is_none()
        && canonical.references.is_none()
        && *expires == canonical.expires
        && *immutable == canonical.immutable
        && *quantization == canonical.quantization
        && *partition_key_columns == canonical.partition_key_columns
        && *max_partitions == canonical.max_partitions
        && *search_mode == canonical.search_mode
        && *auto_index_at == canonical.auto_index_at
        && *hnsw == canonical.hnsw
        && *consolidation == canonical.consolidation
        && rank_policy.is_none()
        && canonical.rank_policy.is_none()
        && *context_id == canonical.context_id
        && scope_label.is_none()
        && canonical.scope_label.is_none()
        && acl_ref.is_none()
        && canonical.acl_ref.is_none()
}

/// What about a reserved-name `CREATE TABLE` / column-shape `ALTER TABLE`
/// mismatched, named in [`engine_owned_reserved_shape_refusal`]'s message so
/// a dissatisfied operator has something to diagnose with -- the fix for
/// a later review's finding that the old message's "name, type, and primary key"
/// claim was false for every hidden-attribute mismatch (UNIQUE / DEFAULT /
/// nullability / IMMUTABLE / EXPIRES) since all of those satisfy
/// name/type/primary-key exactly.
enum ReservedShapeMismatch<'a> {
    /// A table-level UNIQUE, table-level composite PRIMARY KEY, or composite
    /// FOREIGN KEY -- none of the nine reserved tables' own DDL declares one.
    TableLevelConstraint,
    /// A different number of columns than the canonical shape declares.
    ColumnCount,
    /// One column, named here, differs on some field the canonical shape
    /// pins (see [`column_def_matches_canonical`] for the full field list).
    Column(&'a str),
    /// The arriving wire column text does not even parse as a column shape.
    Unparseable,
    /// A table-level option the canonical shape does not declare -- named
    /// here (`"IMMUTABLE"` / `"STATE_MACHINE"` / `"DAG"` / `"PROPAGATE"`).
    TableOption(&'static str),
}

/// The shared refusal for a structurally-mismatched `CREATE TABLE` /
/// column-shape `ALTER TABLE` of one of the nine reserved names, local or
/// wire. States the REAL rule: columns must structurally match the
/// canonical shape the owning installer declares -- NOT that only the
/// installer may ever type the name. A canonical-shape `CREATE TABLE` of
/// any of these nine names, typed by a consumer directly, is legal (see
/// `create_table_work_capabilities_with_canonical_columns_silent_on_policy_still_works`
/// and its five hub-refereed siblings in `engine_owned_ledger_policy_tests.rs`);
/// only a structural mismatch refuses.
fn engine_owned_reserved_shape_refusal(table: &str, mismatch: ReservedShapeMismatch<'_>) -> Error {
    let detail = match mismatch {
        ReservedShapeMismatch::TableLevelConstraint => {
            "a table-level UNIQUE, composite PRIMARY KEY, or composite FOREIGN KEY -- none of \
             which the canonical shape declares"
                .to_string()
        }
        ReservedShapeMismatch::ColumnCount => {
            "a different number of columns than the canonical shape declares".to_string()
        }
        ReservedShapeMismatch::Column(column) => {
            format!("its '{column}' column")
        }
        ReservedShapeMismatch::Unparseable => {
            "columns whose declared type text does not even parse as a valid column shape"
                .to_string()
        }
        ReservedShapeMismatch::TableOption(option) => {
            format!("a table-level {option} option -- the canonical shape declares none")
        }
    };
    Error::SchemaInvalid {
        reason: format!(
            "table '{table}' is engine-owned infrastructure, not an operator table: its shape \
             does not structurally match the canonical shape {} declares for {table} -- the \
             mismatch is {detail}. A CREATE TABLE or column-shape ALTER TABLE naming {table} is \
             accepted only when every column matches the canonical shape across name, type, \
             nullability, primary key, uniqueness, default, references, EXPIRES, and IMMUTABLE, \
             and no table-level IMMUTABLE / STATE_MACHINE / DAG / PROPAGATE option is added. \
             Choose a different name for a table of your own, or match {table}'s canonical \
             shape exactly.",
            engine_owned_reserved_name_installer(table)
        ),
    }
}

/// The function name of the installer that owns one of the nine reserved
/// names, named in [`engine_owned_reserved_shape_refusal`]'s refusal
/// message.
fn engine_owned_reserved_name_installer(table: &str) -> &'static str {
    match table {
        "work_jobs" | "work_claims" | "work_results" | "work_failures" | "work_cancellations"
        | "work_inputs" | "work_capabilities" => "install_work_ledger_schema",
        "peer_directory" => "install_peer_directory_schema",
        "work_node_contacts" => "install_node_contacts_schema",
        _ => "the owning installer",
    }
}

/// The candidate shape a reserved-name `CREATE TABLE` (local or wire) is
/// judged on -- the type-agnostic bridge [`refuse_reserved_name_table_shape`]
/// accepts so both the local door (whose input is a planner
/// `CreateTablePlan`, carrying `propagation_rules: Vec<contextdb_core::
/// PropagationRule>`) and the wire door (whose input, after reparse, is an
/// `ast::CreateTable`, carrying `propagation_rules: Vec<AstPropagationRule>`)
/// can feed it their own already-typed answer to "is this axis present at
/// all" without either door needing to know the other's type.
struct ReservedShapeCandidate<'a> {
    columns: &'a [contextdb_parser::ast::ColumnDef],
    unique_constraints: &'a [Vec<String>],
    primary_key_columns: &'a [String],
    has_composite_foreign_keys: bool,
    immutable: bool,
    has_state_machine: bool,
    has_dag_edge_types: bool,
    has_propagation_rules: bool,
}

/// The shared judge for a reserved-name `CREATE TABLE` / column-shape
/// `ALTER TABLE`, local or wire: EVERY structural axis a `CreateTable`
/// carries, not columns alone. A later review found that an earlier
/// revision closed the
/// per-COLUMN subset-checklist class (`IMMUTABLE` / `EXPIRES` hiding on one
/// column), but the door still compared only `columns` /
/// `unique_constraints` / `primary_key_columns` / `composite_foreign_keys`
/// -- the SAME subset-checklist failure one level up, on the table's own
/// options: `CreateTablePlan.immutable` / `.state_machine` /
/// `.dag_edge_types` / `.propagation_rules` never reached any door, so a
/// table-level `IMMUTABLE` on `work_claims` armed `Error::ImmutableTable` on
/// its own lease-renewal UPDATE while `SHOW` still reported it governed.
///
/// EXPLICIT EXEMPTION (deliberately deferred -- do not fold these in
/// without revisiting that deferral): `RETAIN` / `SYNC` direction / `HISTORY` are judged
/// by [`refuse_engine_owned_policy_axes`] once a projected `TableMeta`
/// exists, not here; `SYNC CONFLICT` is judged by that door and
/// [`refuse_hub_refereed_ledger_sync_conflict_mismatch`]. This door judges
/// only the axes that were NEVER anyone's job before that later review:
/// table-level `IMMUTABLE` / `STATE_MACHINE` / `DAG` / `PROPAGATE`, plus the
/// earlier column/table-level-constraint checks it already owned.
fn refuse_reserved_name_table_shape(
    table: &str,
    candidate: ReservedShapeCandidate<'_>,
) -> Result<()> {
    let Some(canonical) = engine_owned_reserved_table_canonical_create(table) else {
        return Ok(());
    };
    // None of the nine reserved tables' own CREATE TABLE text declares a
    // table-level constraint -- no composite UNIQUE, no table-level
    // PRIMARY KEY (a, b, ...), no composite FOREIGN KEY. Refuse any of the
    // three outright before the per-column comparison below: cheap (three
    // emptiness checks), and it closes the same smuggling shape as a
    // hidden per-column UNIQUE without needing a canonical value to vary
    // per table.
    if !candidate.unique_constraints.is_empty()
        || !candidate.primary_key_columns.is_empty()
        || candidate.has_composite_foreign_keys
    {
        return Err(engine_owned_reserved_shape_refusal(
            table,
            ReservedShapeMismatch::TableLevelConstraint,
        ));
    }
    // None of the nine reserved tables' own CREATE TABLE text declares a
    // table-level IMMUTABLE / STATE_MACHINE / DAG / PROPAGATE option --
    // asserted of `canonical` below (never just assumed), so an installer
    // DDL that DID start declaring one of these would fail loudly rather
    // than being silently exempted, mirroring `column_def_matches_canonical`'s
    // own two-sided assertions.
    if candidate.immutable {
        debug_assert!(!canonical.immutable);
        return Err(engine_owned_reserved_shape_refusal(
            table,
            ReservedShapeMismatch::TableOption("IMMUTABLE"),
        ));
    }
    if candidate.has_state_machine {
        debug_assert!(canonical.state_machine.is_none());
        return Err(engine_owned_reserved_shape_refusal(
            table,
            ReservedShapeMismatch::TableOption("STATE_MACHINE"),
        ));
    }
    if candidate.has_dag_edge_types {
        debug_assert!(canonical.dag_edge_types.is_empty());
        return Err(engine_owned_reserved_shape_refusal(
            table,
            ReservedShapeMismatch::TableOption("DAG"),
        ));
    }
    if candidate.has_propagation_rules {
        debug_assert!(canonical.propagation_rules.is_empty());
        return Err(engine_owned_reserved_shape_refusal(
            table,
            ReservedShapeMismatch::TableOption("PROPAGATE"),
        ));
    }
    if candidate.columns.len() != canonical.columns.len() {
        return Err(engine_owned_reserved_shape_refusal(
            table,
            ReservedShapeMismatch::ColumnCount,
        ));
    }
    if let Some((column, _)) = candidate
        .columns
        .iter()
        .zip(canonical.columns.iter())
        .find(|(column, canon)| !column_def_matches_canonical(column, canon))
    {
        return Err(engine_owned_reserved_shape_refusal(
            table,
            ReservedShapeMismatch::Column(&column.name),
        ));
    }
    Ok(())
}

/// Validates that an arriving wire-format `CREATE TABLE` / column-shape-
/// restating `ALTER TABLE` (see `database.rs`'s
/// `refuse_engine_owned_policy_sync_ddl`, which calls this from both arms)
/// of one of the nine engine-owned reserved tables match the FULL canonical
/// shape -- the wire-format mirror of
/// [`refuse_engine_owned_reserved_name_shape`], sharing its judge
/// ([`refuse_reserved_name_table_shape`]). `Ok` for any other table name,
/// `Ok` for a canonical match, `Err` for a structural mismatch.
///
/// DESIGN CHOICE (an earlier revision replaced token-matching with this
/// approach; a later revision widened the reconstruction to carry
/// `constraints` too, closing the table-level-options gap on this side the
/// same way): the wire format
/// carries each column as one flat `(name, type-and-constraints-string)`
/// pair and table options as a flat `Vec<String>`, neither a structured
/// `ast::CreateTable`. This reconstructs both into real `CREATE TABLE` SQL
/// text (`database.rs`'s own `sync_create_table_sql`, the SAME builder its
/// `validate_sync_table_shape_ddl` / `validate_sync_alter_table_shape_ddl`
/// already use to pre-validate arriving DDL) and runs it through the REAL
/// parser, then judges the WHOLE parsed statement with the identical
/// [`refuse_reserved_name_table_shape`] the local door uses. There is no
/// keyword list left to fall out of sync with the AST, on the column axis
/// or the table-option axis.
///
/// `composite_unique` / `composite_foreign_keys` arrive already structured
/// (the wire protocol never flattens table-level constraints into a
/// string), so that half of the table-level check costs nothing beyond an
/// emptiness test -- refused up front, same as the local AST door, and
/// passed as empty into the reconstruction below (composite table elements
/// never legally appear in a reserved-name `constraints` string either).
pub(crate) fn refuse_engine_owned_reserved_name_shape_wire(
    table: &str,
    columns: &[(String, String)],
    constraints: &[String],
    foreign_keys: &[contextdb_core::SingleColumnForeignKey],
    composite_unique: &[Vec<String>],
    composite_foreign_keys: &[contextdb_core::CompositeForeignKey],
) -> Result<()> {
    if engine_owned_reserved_table_create_ddl(table).is_none() {
        return Ok(());
    }
    if !composite_unique.is_empty() || !composite_foreign_keys.is_empty() {
        return Err(engine_owned_reserved_shape_refusal(
            table,
            ReservedShapeMismatch::TableLevelConstraint,
        ));
    }

    let candidate_ddl = crate::database::sync_create_table_sql(
        table,
        columns,
        constraints,
        foreign_keys,
        composite_foreign_keys,
        composite_unique,
    );
    let candidate = match contextdb_parser::parse(&candidate_ddl) {
        Ok(contextdb_parser::ast::Statement::CreateTable(create)) => create,
        _ => {
            return Err(engine_owned_reserved_shape_refusal(
                table,
                ReservedShapeMismatch::Unparseable,
            ));
        }
    };

    refuse_reserved_name_table_shape(
        table,
        ReservedShapeCandidate {
            columns: &candidate.columns,
            unique_constraints: &candidate.unique_constraints,
            primary_key_columns: &candidate.primary_key_columns,
            has_composite_foreign_keys: !candidate.composite_foreign_keys.is_empty(),
            immutable: candidate.immutable,
            has_state_machine: candidate.state_machine.is_some(),
            has_dag_edge_types: !candidate.dag_edge_types.is_empty(),
            has_propagation_rules: !candidate.propagation_rules.is_empty(),
        },
    )
}

/// The column-and-table-option-shape half of the reserved-name door:
/// refuses a LOCAL `CREATE TABLE` of one of the nine reserved names whose
/// shape does not structurally match
/// ([`refuse_reserved_name_table_shape`]) the shape the owning installer's
/// own `CREATE TABLE` text declares
/// ([`engine_owned_reserved_table_canonical_create`]). `Ok` for any other
/// table name.
///
/// This is deliberately independent of [`refuse_engine_owned_policy_axes`]
/// (the RETAIN/HISTORY/SYNC-direction/SYNC-CONFLICT policy half, run
/// separately once the projected `TableMeta` exists -- see C1-1 in
/// [`refuse_reserved_name_table_shape`]'s doc comment): a create that
/// restates the exact canonical shape but is silent on those axes still
/// passes here. Reconciliation tests construct this undeclared shape
/// directly through the same local `CREATE TABLE` path.
///
/// `plan` is destructured EXHAUSTIVELY (no `..`), the same structural
/// guarantee an earlier revision gave `ColumnDef`: a future `CreateTablePlan` field is a
/// COMPILE ERROR here until this door is told how the new field
/// participates -- either compared, or explicitly exempted with a comment
/// naming why (as `retain` / `sync_direction` / `conflict_policy` /
/// `history` are below) -- instead of silently smuggling through the way
/// `immutable` / `state_machine` / `dag_edge_types` / `propagation_rules`
/// did before that later review.
fn refuse_engine_owned_reserved_name_shape(
    plan: &contextdb_planner::CreateTablePlan,
) -> Result<()> {
    let contextdb_planner::CreateTablePlan {
        name,
        columns,
        unique_constraints,
        primary_key_columns,
        composite_foreign_keys,
        // Deliberately exempt: `IF NOT EXISTS` is not part of a table's
        // declared shape. It only decides what a collision on an existing
        // name does, and the collision door in the `CreateTable` arm answers
        // that before this shape door ever runs -- so a reserved name reaches
        // here only when no such table exists yet, where the flag has nothing
        // to say.
        if_not_exists: _,
        immutable,
        state_machine,
        dag_edge_types,
        propagation_rules,
        // Deliberately deferred: RETAIN / SYNC direction / HISTORY are
        // judged by `refuse_engine_owned_policy_axes` once a projected
        // `TableMeta` exists, not by this shape-only door -- do not fold
        // them in here without revisiting that deferral; that would silently
        // widen it.
        retain: _,
        sync_direction: _,
        // Judged by `refuse_engine_owned_policy_axes` /
        // `refuse_hub_refereed_ledger_sync_conflict_mismatch`, not here.
        conflict_policy: _,
        history: _,
        // Custody policy axes are judged with the other policy fields after
        // projection; they are not part of the reserved table's column shape.
        delivery_manifest_tables: _,
        edge_discard: _,
        // A maintained-count declaration is a write-cost choice over an
        // existing index, not part of a reserved table's column shape; the
        // declared lists are judged by
        // `crate::maintained_counts::validate_maintained_counts_for_meta`.
        maintained_counts: _,
    } = plan;
    refuse_reserved_name_table_shape(
        name,
        ReservedShapeCandidate {
            columns,
            unique_constraints,
            primary_key_columns,
            has_composite_foreign_keys: !composite_foreign_keys.is_empty(),
            immutable: *immutable,
            has_state_machine: state_machine.is_some(),
            has_dag_edge_types: !dag_edge_types.is_empty(),
            has_propagation_rules: !propagation_rules.is_empty(),
        },
    )
}

/// The column-shape half of the reserved-name door for a LOCAL `ALTER
/// TABLE ... ADD COLUMN` / `DROP COLUMN` / `RENAME COLUMN` naming one of the
/// nine reserved names -- refuses outright, before any other ALTER
/// validation runs, so a rejected statement applies no part of itself. Column
/// shape on these nine names is declared exactly once, by the owning
/// installer's own `CREATE TABLE` text
/// ([`engine_owned_reserved_table_create_ddl`]); the engine's own
/// bookkeeping (`work_ledger.rs`, `peer_directory.rs`, and
/// `contextdb-server`'s node-contacts tracking) reads these tables' columns
/// by name and depends on them staying exactly as declared, the same reason
/// [`refuse_engine_owned_reserved_name_shape`] refuses a mismatched fresh
/// `CREATE TABLE` of one of these names. `Ok` for any other table name --
/// column-shape ALTERs on an operator table are entirely unaffected.
fn refuse_engine_owned_reserved_name_column_alter(table: &str) -> Result<()> {
    if engine_owned_reserved_table_create_ddl(table).is_none() {
        return Ok(());
    }
    Err(Error::SchemaInvalid {
        reason: format!(
            "table '{table}' is engine-owned infrastructure, not an operator table: its column \
             shape is declared once, in the engine's own CREATE TABLE text for {table} \
             ({}), and the engine's own bookkeeping depends on it staying exactly as declared \
             there -- an ALTER TABLE ADD COLUMN / DROP COLUMN / RENAME COLUMN naming {table} is \
             refused outright, never applied. Reshape a table of your own instead.",
            engine_owned_reserved_name_installer(table)
        ),
    })
}

/// The policy axes an engine-owned built-in ledger table
/// ([`ENGINE_OWNED_LEDGER_TABLES`]) should carry after an arriving sync DDL
/// that can legally touch its policy -- the `AlterTable` mirror and the
/// `CreateTable` ADOPT arm, both gated by [`refuse_engine_owned_policy_axes`]
/// before this merge ever runs, so `incoming`'s axes are at this point either
/// silent or already equal to canonical. An axis `incoming` is SILENT on
/// (`None`, or `false` for `sync_safe`, which has no explicit-off spelling)
/// PRESERVES `current`'s value rather than clearing it -- the fix for the
/// defect where the writer's full post-DDL shape, baked by every emitter
/// whether or not that specific axis changed, was taken verbatim and so a
/// silent/dropped axis landed as cleared. An axis `incoming` DOES carry
/// adopts verbatim, which is a no-op when `current` already matches (the
/// common case) and installs the declaration when `current` omits it,
/// just as a local reconciliation `ALTER` does. `None` for any table outside
/// [`ENGINE_OWNED_LEDGER_TABLES`] -- callers keep the existing full-overwrite
/// behavior there.
pub(crate) struct EngineOwnedMergedPolicy {
    pub(crate) default_ttl_seconds: Option<u64>,
    pub(crate) retain_declared_unit: Option<RetainUnit>,
    pub(crate) history_policy: Option<HistoryPolicy>,
    pub(crate) conflict_policy: Option<contextdb_core::ConflictPolicy>,
    pub(crate) sync_direction: Option<SyncDirection>,
    pub(crate) sync_safe: bool,
}

pub(crate) fn engine_owned_merged_policy(
    table: &str,
    current: &TableMeta,
    incoming: &TableMeta,
) -> Option<EngineOwnedMergedPolicy> {
    engine_owned_ledger_policy(table)?;
    Some(EngineOwnedMergedPolicy {
        default_ttl_seconds: incoming.default_ttl_seconds.or(current.default_ttl_seconds),
        retain_declared_unit: incoming
            .retain_declared_unit
            .or(current.retain_declared_unit),
        history_policy: incoming.history_policy.or(current.history_policy),
        conflict_policy: incoming.conflict_policy.or(current.conflict_policy),
        sync_direction: incoming.sync_direction.or(current.sync_direction),
        sync_safe: incoming.sync_safe || current.sync_safe,
    })
}

/// What an `ALTER TABLE` is asking to change, on the one axis
/// [`refuse_engine_owned_policy_mutation`] checks at a time. `Retain { window:
/// None, .. }` is `DROP RETAIN`; `Retain { window: Some(..), .. }` is `SET
/// RETAIN`. The `sync_safe` field of the mutation captures whether the
/// statement included the SYNC SAFE flag, so the refusal gate can check it
/// against the canonical `sync_safe` axis alongside the window. `SyncDirection`
/// is `SET SYNC ...` (the `AlterAction::SetSyncDirection` arm) -- for a table
/// whose canonical shape declares no direction at all (`None`, i.e. the
/// default governs; every engine-owned table except `work_node_contacts`),
/// ANY explicit `SET SYNC` differs from that canonical `None` and refuses;
/// `work_node_contacts` alone tolerates the one value it declares,
/// `SET SYNC OFF`, as a verbatim restatement.
pub(crate) enum EngineOwnedMutation {
    Retain {
        window: Option<(u64, RetainUnit)>,
        sync_safe: bool,
    },
    History(HistoryPolicy),
    SyncConflict(contextdb_core::ConflictPolicy),
    SyncDirection(SyncDirection),
}

/// Refuse an `ALTER TABLE` that would move one of the four engine-owned
/// built-in ledger tables ([`ENGINE_OWNED_LEDGER_TABLES`]) away from the
/// policy the engine declared for it. `Ok` for any other table, and `Ok` for
/// a mutation that only RESTATES the table's own declared shape verbatim --
/// the installers' own reconcile arms (`install_work_ledger_schema`,
/// `install_peer_directory_schema`,
/// `contextdb_server::work_ledger::install_node_contacts_schema`) heal a
/// table with missing declarations by issuing that restatement one axis
/// at a time, and must keep working underneath this door -- see those
/// functions' own doc comments for why `default_ttl_seconds.is_none()` (or
/// the equivalent gap on `history_policy` / `conflict_policy`) can now only
/// mean "this table lacks the declaration," never "an operator dropped
/// it," now that this door is shut.
///
/// This is the same door pattern as
/// [`refuse_reclaimed_history_under_keep_first`]: called before the write
/// lock is taken, so a refused ALTER applies no part of itself -- not even
/// the accompanying `SYNC SAFE` flag on a `SET RETAIN`.
pub(crate) fn refuse_engine_owned_policy_mutation(
    table: &str,
    mutation: EngineOwnedMutation,
) -> Result<()> {
    let Some(canonical) = engine_owned_ledger_policy(table) else {
        return Ok(());
    };
    let clause = match mutation {
        EngineOwnedMutation::Retain { window, sync_safe } => {
            if window != canonical.retain {
                "RETAIN"
            } else if sync_safe && !canonical.sync_safe {
                "SYNC SAFE"
            } else {
                return Ok(());
            }
        }
        EngineOwnedMutation::History(policy) if Some(policy) != canonical.history => "HISTORY",
        EngineOwnedMutation::SyncConflict(policy) if Some(policy) != canonical.conflict => {
            "SYNC CONFLICT"
        }
        EngineOwnedMutation::SyncDirection(direction) if Some(direction) != canonical.direction => {
            "SYNC"
        }
        _ => return Ok(()),
    };
    Err(engine_owned_policy_refusal(table, clause))
}

/// The message [`refuse_engine_owned_policy_mutation`] and its arriving-sync-DDL
/// mirror in `database.rs` both raise -- one place so the two doors never
/// drift onto different wording for the same refusal.
pub(crate) fn engine_owned_policy_refusal(table: &str, clause: &str) -> Error {
    let tunable_surface = if table == "work_inputs" {
        " Tune how promptly a job's ledger-carried input copies are treated as gone via \
          run_input_retention's grace-period argument, not this table's schema."
    } else {
        ""
    };
    // The five hub-refereed work-ledger tables outside `ENGINE_OWNED_LEDGER_TABLES`
    // (`work_jobs` / `work_claims` / `work_results` / `work_failures` /
    // `work_cancellations`) get an axis-scoped message, never the table-wide
    // "never by an ALTER an operator types" claim below: SYNC CONFLICT is the
    // ONLY axis any door judges for these five today (RETAIN / HISTORY / SYNC
    // direction are unguarded on them -- filed as C1-1, owner-deferred), so a
    // categorical "unalterable" claim would be false. `work_claims` /
    // `work_results` / `work_cancellations` additionally carry NO `SYNC
    // CONFLICT` clause in their own `CREATE TABLE` text at all (see
    // `CREATE_WORK_CLAIMS` / `CREATE_WORK_RESULTS` / `CREATE_WORK_CANCELLATIONS`
    // in `work_ledger.rs`) -- their real arbitration is a purely
    // engine-INTERNAL `ServerWins` override
    // (`work_ledger::apply_work_ledger_policy_overrides_inner`) that no DDL
    // text anywhere declares, so pointing a dissatisfied operator at "the
    // engine's own CREATE TABLE text" for these three would name a document
    // that does not exist; `work_jobs` / `work_failures` DO carry the clause
    // in their own DDL, so their message may cite it.
    if clause == "SYNC CONFLICT" && is_hub_refereed_sync_conflict_table(table) {
        let arbitration_source =
            if matches!(table, "work_claims" | "work_results" | "work_cancellations") {
                format!(
                    "its {clause} arbitration is decided entirely inside the engine -- a \
                     hub-refereed override applied at every sync chokepoint, not a clause \
                     {table}'s own CREATE TABLE text declares"
                )
            } else {
                format!(
                    "its {clause} declaration lives once, in the engine's own CREATE TABLE text \
                     for {table}, mirrored by the same hub-refereed override applied at every \
                     sync chokepoint"
                )
            };
        // Not installer-exclusive: a consumer CAN legally restate this
        // axis verbatim (the honest, canonical-matching clause) in their
        // own CREATE TABLE -- only a MISMATCHING value is refused. Say
        // that, not a categorical "only the installer may declare this".
        return Error::SchemaInvalid {
            reason: format!(
                "table '{table}' is engine-owned infrastructure, not an operator table: \
                 {arbitration_source}. A {clause} clause that restates this arbitration \
                 verbatim, or no {clause} clause at all, is accepted here; a value that \
                 mismatches it is refused."
            ),
        };
    }
    // The four ORIGINAL `ENGINE_OWNED_LEDGER_TABLES` (`work_inputs` /
    // `work_capabilities` / `peer_directory` / `work_node_contacts`) reach
    // here for a MISMATCHING SYNC CONFLICT declaration. RETAIN / HISTORY /
    // SYNC direction on these four are held to the identical mismatch-only
    // contract, not ALTER-immunity: an ALTER naming one of these tables is
    // refused only when the table already exists (the caller checks
    // existence before any of these doors run) AND the value it declares
    // mismatches the canonical one -- a verbatim restate, or silence on the
    // axis, is accepted on every one of them, RETAIN / HISTORY / SYNC
    // direction included, the same as SYNC CONFLICT here.
    // `refuse_engine_owned_policy_mutation` already tolerates an honest
    // VERBATIM restate of SYNC CONFLICT specifically
    // (`restating_the_declared_value_verbatim_is_not_refused`), so a
    // categorical "never alterable" claim would be false for this axis --
    // exactly the same falsehood already fixed on the five hub-refereed
    // tables above. Give SYNC CONFLICT its own mismatch-only wording here
    // too, before falling through to the shared mismatch-only message every
    // other axis reaches.
    if clause == "SYNC CONFLICT" {
        // `work_node_contacts` is the one member of `ENGINE_OWNED_LEDGER_TABLES`
        // whose own CREATE TABLE text (`CREATE_WORK_NODE_CONTACTS`) declares
        // NO SYNC CONFLICT clause at all -- `engine_owned_ledger_policy`'s
        // own canonical entry for it is `conflict: None`. The generic
        // wording below is false for it on both counts: there is no
        // declaration "living once" in its DDL text to point at, and there
        // is no canonical value for a caller to "restate verbatim" and be
        // accepted -- EVERY explicit SYNC CONFLICT value mismatches the
        // undeclared `None`, not just a wrong one.
        if engine_owned_ledger_policy(table).is_some_and(|policy| policy.conflict.is_none()) {
            return Error::SchemaInvalid {
                reason: format!(
                    "table '{table}' is engine-owned infrastructure, not an operator table: its \
                     own CREATE TABLE text declares no {clause} clause at all, and the engine's \
                     own bookkeeping depends on that staying undeclared -- an explicit {clause} \
                     value is refused here regardless of what it names.{tunable_surface}"
                ),
            };
        }
        return Error::SchemaInvalid {
            reason: format!(
                "table '{table}' is engine-owned infrastructure, not an operator table: its \
                 {clause} declaration lives once, in the engine's own CREATE TABLE text for \
                 {table}, and the engine's own bookkeeping depends on it staying exactly as \
                 declared there. A {clause} clause that restates this arbitration verbatim, \
                 or no {clause} clause at all, is accepted here; a value that mismatches it \
                 is refused.{tunable_surface}"
            ),
        };
    }
    Error::SchemaInvalid {
        reason: format!(
            "table '{table}' is engine-owned infrastructure, not an operator table: its \
             {clause} declaration lives once, in the engine's own CREATE TABLE text for \
             {table}, and the engine's own bookkeeping depends on it staying exactly as \
             declared there -- changing it here would leave the engine reasoning about a \
             shape the stored rows no longer match. A {clause} clause that restates this \
             declaration verbatim, or no {clause} clause at all, is accepted here; a value \
             that mismatches it is refused. Declare RETAIN / HISTORY / SYNC CONFLICT on \
             tables you create instead -- ALTER TABLE there is unrestricted.{tunable_surface}"
        ),
    }
}

/// The five hub-refereed work-ledger tables not in
/// [`ENGINE_OWNED_LEDGER_TABLES`] (`work_jobs`, `work_claims`,
/// `work_results`, `work_failures`, `work_cancellations`), paired with each
/// one's TRUE arbitration display word -- derived from the SAME live source
/// [`crate::work_ledger::work_ledger_conflict_policy_entries_inner`]'s
/// `SHOW SYNC_CONFLICT_POLICY` reads
/// ([`crate::work_ledger::engine_owned_work_ledger_conflict_policy_display`]),
/// filtered to the names outside `ENGINE_OWNED_LEDGER_TABLES` (which already
/// get full-axis coverage from [`refuse_engine_owned_policy_axes`] and so
/// are excluded here). One source of truth, so a future ledger-policy
/// change can never split SHOW's rendering from what these doors refuse.
fn hub_refereed_sync_conflict_tables() -> Vec<(&'static str, &'static str)> {
    crate::work_ledger::engine_owned_work_ledger_conflict_policy_display()
        .into_iter()
        .filter(|(table, _)| !ENGINE_OWNED_LEDGER_TABLES.contains(table))
        .collect()
}

/// `true` iff `table` is one of the five
/// [`hub_refereed_sync_conflict_tables`] names -- the membership half,
/// shared by [`engine_owned_policy_refusal`]'s message-wording branch and
/// `database.rs`'s fresh-CreateTable wire preflight (which widens its match
/// guard with this alongside [`ENGINE_OWNED_LEDGER_TABLES`]).
pub(crate) fn is_hub_refereed_sync_conflict_table(table: &str) -> bool {
    hub_refereed_sync_conflict_tables()
        .iter()
        .any(|(owned_table, _)| *owned_table == table)
}

/// Refuses an EXPLICIT `SYNC CONFLICT` value that mismatches
/// [`hub_refereed_sync_conflict_tables`]'s true arbitration word for one of
/// the five hub-refereed work-ledger tables: the local ALTER arm
/// (`AlterAction::SetSyncConflict`), fresh local `CREATE TABLE`, and the
/// three arriving-sync-DDL preflights in `database.rs` (`AlterTable`,
/// `CreateTable` adopting an existing table, and fresh `CreateTable` of a
/// reserved name) -- every door that can move this axis for these five
/// tables.
///
/// Deliberately narrower than [`refuse_engine_owned_policy_axes`] (the
/// full-axis door the four [`ENGINE_OWNED_LEDGER_TABLES`] get): it judges
/// ONLY the SYNC CONFLICT axis. RETAIN / HISTORY / SYNC direction on these
/// five carry no canonical value anywhere the way the four
/// `ENGINE_OWNED_LEDGER_TABLES` do (`engine_owned_ledger_policy` declares a
/// full canonical shape on every axis for those four, and its `match` would
/// need a real value for axes these five never declare) -- so this is a
/// separate, axis-scoped door rather than a widened
/// `ENGINE_OWNED_LEDGER_TABLES`. Widening the RETAIN/HISTORY/SYNC gap on
/// these five is filed as C1-1 (owner-deferred); do not fold it in here.
///
/// `Ok` for any other table, `Ok` when `incoming_conflict_policy` is `None`
/// (silence, e.g. the undeclared shape, or a table create/alter
/// that never mentions SYNC CONFLICT at all), `Ok` for a verbatim restate of
/// the table's true arbitration word, `Err` for any other explicit value.
pub(crate) fn refuse_hub_refereed_ledger_sync_conflict_mismatch(
    table: &str,
    incoming_conflict_policy: Option<contextdb_core::ConflictPolicy>,
) -> Result<()> {
    let Some(policy) = incoming_conflict_policy else {
        return Ok(());
    };
    let Some((_, true_word)) = hub_refereed_sync_conflict_tables()
        .into_iter()
        .find(|(owned_table, _)| *owned_table == table)
    else {
        return Ok(());
    };
    if declared_conflict_policy_to_string(policy) != true_word {
        return Err(engine_owned_policy_refusal(table, "SYNC CONFLICT"));
    }
    Ok(())
}

fn expires_column_name(columns: &[contextdb_parser::ast::ColumnDef]) -> Result<Option<String>> {
    let mut expires_column = None;
    for col in columns {
        validate_expires_column(col)?;
        if col.expires {
            if expires_column.is_some() {
                return Err(Error::Other(
                    "only one EXPIRES column is supported per table".to_string(),
                ));
            }
            expires_column = Some(col.name.clone());
        }
    }
    Ok(expires_column)
}

fn validate_projected_foreign_keys_after_drop_table(db: &Database, table: &str) -> Result<()> {
    let mut projected = db.relational_store().table_meta.read().clone();
    if projected.remove(table).is_some() {
        Database::validate_projected_foreign_key_schema(&projected)?;
    }
    Ok(())
}

fn validate_projected_foreign_keys_after_drop_column(
    db: &Database,
    table: &str,
    column: &str,
) -> Result<()> {
    let mut projected = db.relational_store().table_meta.read().clone();
    if let Some(meta) = projected.get_mut(table)
        && let Some(pos) = meta.columns.iter().position(|c| c.name == column)
    {
        meta.columns.remove(pos);
        if meta.expires_column.as_deref() == Some(column) {
            meta.expires_column = None;
        }
    }
    Database::validate_projected_foreign_key_schema(&projected)
}

fn validate_projected_foreign_keys_after_rename_column(
    db: &Database,
    table: &str,
    from: &str,
    to: &str,
) -> Result<()> {
    let mut projected = db.relational_store().table_meta.read().clone();
    if let Some(meta) = projected.get_mut(table) {
        if meta.columns.iter().any(|c| c.name == to) {
            return Ok(());
        }
        if let Some(column) = meta.columns.iter_mut().find(|c| c.name == from) {
            column.name = to.to_string();
            if meta.expires_column.as_deref() == Some(from) {
                meta.expires_column = Some(to.to_string());
            }
        }
    }
    Database::validate_projected_foreign_key_schema(&projected)
}

pub(crate) fn btree_indexable(column_type: &ColumnType) -> bool {
    !matches!(column_type, ColumnType::Json | ColumnType::Vector(_))
}

fn exact_constraint_key_indexable(column_type: &ColumnType) -> bool {
    !matches!(
        column_type,
        ColumnType::Real | ColumnType::Json | ColumnType::Vector(_)
    )
}

pub(crate) fn validate_exact_constraint_keys_for_meta(table: &str, meta: &TableMeta) -> Result<()> {
    for column in &meta.columns {
        if (column.primary_key || column.unique)
            && !exact_constraint_key_indexable(&column.column_type)
        {
            return Err(Error::ColumnNotIndexable {
                table: table.to_string(),
                column: column.name.clone(),
                column_type: column.column_type.clone(),
            });
        }
    }

    for unique_constraint in &meta.unique_constraints {
        if unique_constraint.is_empty() {
            return Err(Error::SchemaInvalid {
                reason: format!("UNIQUE constraint on {table} must include at least one column"),
            });
        }
        for column_name in unique_constraint {
            let Some(column) = meta
                .columns
                .iter()
                .find(|candidate| candidate.name == *column_name)
            else {
                return Err(Error::ColumnNotFound {
                    table: table.to_string(),
                    column: column_name.clone(),
                });
            };
            if !exact_constraint_key_indexable(&column.column_type) {
                return Err(Error::ColumnNotIndexable {
                    table: table.to_string(),
                    column: column_name.clone(),
                    column_type: column.column_type.clone(),
                });
            }
        }
    }

    // A multi-column PRIMARY KEY is the sync identity and needs a covering
    // index over exact-matchable columns; refuse a key column that cannot be
    // exact-indexed (REAL / JSON / VECTOR) at declaration rather than failing
    // the sync-apply probe later.
    for column_name in &meta.primary_key_columns {
        let Some(column) = meta
            .columns
            .iter()
            .find(|candidate| candidate.name == *column_name)
        else {
            return Err(Error::ColumnNotFound {
                table: table.to_string(),
                column: column_name.clone(),
            });
        };
        if !exact_constraint_key_indexable(&column.column_type) {
            return Err(Error::ColumnNotIndexable {
                table: table.to_string(),
                column: column_name.clone(),
                column_type: column.column_type.clone(),
            });
        }
    }

    Ok(())
}

pub(crate) fn auto_indexes_for_table_meta(meta: &TableMeta) -> Vec<contextdb_core::IndexDecl> {
    let mut meta_with_indexes = meta.clone();
    meta_with_indexes.indexes.clear();
    for column in &meta.columns {
        if column.primary_key && exact_constraint_key_indexable(&column.column_type) {
            let columns = vec![(column.name.clone(), contextdb_core::SortDirection::Asc)];
            if !auto_index_with_columns_exists(&meta_with_indexes, &columns) {
                let name =
                    unique_auto_index_name(&meta_with_indexes, format!("__pk_{}", column.name));
                meta_with_indexes.indexes.push(contextdb_core::IndexDecl {
                    name,
                    columns,
                    kind: contextdb_core::IndexKind::Auto,
                });
            }
        }
        if column.unique
            && !column.primary_key
            && exact_constraint_key_indexable(&column.column_type)
        {
            let columns = vec![(column.name.clone(), contextdb_core::SortDirection::Asc)];
            if !auto_index_with_columns_exists(&meta_with_indexes, &columns) {
                let name =
                    unique_auto_index_name(&meta_with_indexes, format!("__unique_{}", column.name));
                meta_with_indexes.indexes.push(contextdb_core::IndexDecl {
                    name,
                    columns,
                    kind: contextdb_core::IndexKind::Auto,
                });
            }
        }
    }
    for unique_constraint in &meta.unique_constraints {
        if unique_constraint.is_empty()
            || !unique_constraint.iter().all(|column_name| {
                meta.columns
                    .iter()
                    .find(|column| column.name == *column_name)
                    .is_some_and(|column| exact_constraint_key_indexable(&column.column_type))
            })
        {
            continue;
        }
        let columns = unique_constraint
            .iter()
            .map(|column| (column.clone(), contextdb_core::SortDirection::Asc))
            .collect::<Vec<_>>();
        if auto_index_with_columns_exists(&meta_with_indexes, &columns) {
            continue;
        }
        let name = unique_auto_index_name(
            &meta_with_indexes,
            format!("__unique_{}", unique_constraint.join("_")),
        );
        meta_with_indexes.indexes.push(contextdb_core::IndexDecl {
            name,
            columns,
            kind: contextdb_core::IndexKind::Auto,
        });
    }
    // The multi-column PRIMARY KEY is also the sync identity, and the exact
    // sync-key probe refuses to run without a covering index over
    // the whole key. Back it with an ordered auto-index over its columns, in
    // declared order, so an arriving row is matched by its full identity.
    if meta.primary_key_columns.len() >= 2
        && meta.primary_key_columns.iter().all(|column_name| {
            meta.columns
                .iter()
                .find(|column| column.name == *column_name)
                .is_some_and(|column| exact_constraint_key_indexable(&column.column_type))
        })
    {
        let columns = meta
            .primary_key_columns
            .iter()
            .map(|column| (column.clone(), contextdb_core::SortDirection::Asc))
            .collect::<Vec<_>>();
        if !auto_index_with_columns_exists(&meta_with_indexes, &columns) {
            let name = unique_auto_index_name(
                &meta_with_indexes,
                format!("__pk_{}", meta.primary_key_columns.join("_")),
            );
            meta_with_indexes.indexes.push(contextdb_core::IndexDecl {
                name,
                columns,
                kind: contextdb_core::IndexKind::Auto,
            });
        }
    }
    append_single_column_fk_auto_indexes(&mut meta_with_indexes);
    append_composite_fk_auto_indexes(&mut meta_with_indexes);
    append_graph_edge_auto_index(&mut meta_with_indexes);
    meta_with_indexes.indexes
}

fn auto_index_with_columns_exists(
    meta: &TableMeta,
    columns: &[(String, contextdb_core::SortDirection)],
) -> bool {
    meta.indexes
        .iter()
        .any(|index| index.kind == contextdb_core::IndexKind::Auto && index.columns == columns)
}

fn append_single_column_fk_auto_indexes(meta: &mut TableMeta) {
    for column in &meta.columns {
        let Some(reference) = &column.references else {
            continue;
        };
        if !exact_constraint_key_indexable(&column.column_type) {
            continue;
        }
        let columns = vec![(column.name.clone(), contextdb_core::SortDirection::Asc)];
        if meta.indexes.iter().any(|index| index.columns == columns) {
            continue;
        }
        let name = unique_auto_index_name(
            meta,
            single_column_fk_auto_index_name(&column.name, reference),
        );
        meta.indexes.push(contextdb_core::IndexDecl {
            name,
            columns,
            kind: contextdb_core::IndexKind::Auto,
        });
    }
}

pub(crate) fn append_composite_fk_auto_indexes(meta: &mut TableMeta) {
    for fk in &meta.composite_foreign_keys {
        if fk.child_columns.is_empty()
            || !fk.child_columns.iter().all(|column_name| {
                meta.columns
                    .iter()
                    .find(|column| column.name == *column_name)
                    .is_some_and(|column| exact_constraint_key_indexable(&column.column_type))
            })
        {
            continue;
        }
        let columns = fk
            .child_columns
            .iter()
            .map(|column| (column.clone(), contextdb_core::SortDirection::Asc))
            .collect::<Vec<_>>();
        if meta.indexes.iter().any(|index| index.columns == columns) {
            continue;
        }
        let name = unique_auto_index_name(meta, composite_fk_auto_index_name(fk));
        meta.indexes.push(contextdb_core::IndexDecl {
            name,
            columns,
            kind: contextdb_core::IndexKind::Auto,
        });
    }
}

fn append_graph_edge_auto_index(meta: &mut TableMeta) {
    let graph_edge_columns = [
        ("source_id", ColumnType::Uuid),
        ("target_id", ColumnType::Uuid),
        ("edge_type", ColumnType::Text),
    ];
    if !graph_edge_columns
        .iter()
        .all(|(column_name, column_type)| has_exact_column_type(meta, column_name, column_type))
    {
        return;
    }
    let name = "__graph_edge_source_target_type".to_string();
    if meta.indexes.iter().any(|index| index.name == name) {
        return;
    }
    meta.indexes.push(contextdb_core::IndexDecl {
        name,
        columns: graph_edge_columns
            .iter()
            .map(|(column, _)| ((*column).to_string(), contextdb_core::SortDirection::Asc))
            .collect(),
        kind: contextdb_core::IndexKind::Auto,
    });
}

fn single_column_fk_auto_index_name(
    column: &str,
    reference: &contextdb_core::ForeignKeyReference,
) -> String {
    encoded_auto_index_name("__fk", [column, &reference.table, &reference.column])
}

fn composite_fk_auto_index_name(fk: &contextdb_core::CompositeForeignKey) -> String {
    let mut parts = Vec::with_capacity(fk.child_columns.len() + fk.parent_columns.len() + 1);
    parts.extend(fk.child_columns.iter().map(String::as_str));
    parts.push(fk.parent_table.as_str());
    parts.extend(fk.parent_columns.iter().map(String::as_str));
    encoded_auto_index_name("__fk", parts)
}

fn encoded_auto_index_name<'a, I>(prefix: &str, parts: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let mut name = prefix.to_string();
    for part in parts {
        name.push('_');
        name.push_str(&part.len().to_string());
        name.push(':');
        name.push_str(part);
    }
    name
}

fn unique_auto_index_name(meta: &TableMeta, base: String) -> String {
    if meta.indexes.iter().all(|index| index.name != base) {
        return base;
    }

    for suffix in 1usize.. {
        let candidate = format!("{base}__{suffix}");
        if meta.indexes.iter().all(|index| index.name != candidate) {
            return candidate;
        }
    }

    unreachable!("unbounded suffix search must find a unique auto-index name")
}

pub(crate) fn validate_composite_foreign_keys_for_meta<F>(
    table: &str,
    meta: &TableMeta,
    mut parent_lookup: F,
) -> Result<()>
where
    F: FnMut(&str) -> Option<TableMeta>,
{
    let child_columns = meta
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<HashMap<_, _>>();

    for fk in &meta.composite_foreign_keys {
        if fk.child_columns.is_empty()
            || fk.child_columns.len() != fk.parent_columns.len()
            || fk.parent_table.is_empty()
        {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "composite foreign key on {table} must have matching non-empty child and parent columns"
                ),
            });
        }
        reject_duplicate_columns(table, &fk.child_columns, "FOREIGN KEY child columns")?;
        reject_duplicate_columns(
            &fk.parent_table,
            &fk.parent_columns,
            "FOREIGN KEY parent columns",
        )?;

        for column in &fk.child_columns {
            let Some(column_meta) = child_columns.get(column.as_str()) else {
                return Err(Error::ColumnNotFound {
                    table: table.to_string(),
                    column: column.clone(),
                });
            };
            if !exact_constraint_key_indexable(&column_meta.column_type) {
                return Err(Error::ColumnNotIndexable {
                    table: table.to_string(),
                    column: column.clone(),
                    column_type: column_meta.column_type.clone(),
                });
            }
        }

        let Some(parent_meta) = parent_lookup(&fk.parent_table) else {
            return Err(Error::TableNotFound(fk.parent_table.clone()));
        };
        for column in &fk.parent_columns {
            let Some(parent_column) = parent_meta.columns.iter().find(|c| c.name == *column) else {
                return Err(Error::ColumnNotFound {
                    table: fk.parent_table.clone(),
                    column: column.clone(),
                });
            };
            if !exact_constraint_key_indexable(&parent_column.column_type) {
                return Err(Error::ColumnNotIndexable {
                    table: fk.parent_table.clone(),
                    column: column.clone(),
                    column_type: parent_column.column_type.clone(),
                });
            }
        }
        if !parent_tuple_is_key_covered(&parent_meta, &fk.parent_columns) {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "composite foreign key on {table} references {}({}) without an ordered PRIMARY KEY or UNIQUE constraint",
                    fk.parent_table,
                    fk.parent_columns.join(", ")
                ),
            });
        }
    }

    Ok(())
}

pub(crate) fn validate_single_column_foreign_keys_for_meta<F>(
    table: &str,
    meta: &TableMeta,
    mut parent_lookup: F,
) -> Result<()>
where
    F: FnMut(&str) -> Option<TableMeta>,
{
    for column in &meta.columns {
        let Some(reference) = &column.references else {
            continue;
        };
        if !exact_constraint_key_indexable(&column.column_type) {
            return Err(Error::ColumnNotIndexable {
                table: table.to_string(),
                column: column.name.clone(),
                column_type: column.column_type.clone(),
            });
        }

        let Some(parent_meta) = parent_lookup(&reference.table) else {
            return Err(Error::TableNotFound(reference.table.clone()));
        };
        let Some(parent_column) = parent_meta
            .columns
            .iter()
            .find(|candidate| candidate.name == reference.column)
        else {
            return Err(Error::ColumnNotFound {
                table: reference.table.clone(),
                column: reference.column.clone(),
            });
        };
        if !exact_constraint_key_indexable(&parent_column.column_type) {
            return Err(Error::ColumnNotIndexable {
                table: reference.table.clone(),
                column: reference.column.clone(),
                column_type: parent_column.column_type.clone(),
            });
        }

        if !parent_tuple_is_key_covered(&parent_meta, std::slice::from_ref(&reference.column)) {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "foreign key on {table}.{} references {}({}) without a PRIMARY KEY or UNIQUE constraint",
                    column.name, reference.table, reference.column
                ),
            });
        }
    }

    Ok(())
}

fn reject_duplicate_columns(table: &str, columns: &[String], label: &str) -> Result<()> {
    let mut seen = HashSet::new();
    for column in columns {
        if !seen.insert(column) {
            return Err(Error::SchemaInvalid {
                reason: format!("{label} on {table} contain duplicate column '{column}'"),
            });
        }
    }
    Ok(())
}

pub(crate) fn parent_tuple_is_key_covered(
    parent_meta: &TableMeta,
    parent_columns: &[String],
) -> bool {
    if parent_columns.len() == 1
        && parent_meta
            .columns
            .iter()
            .any(|column| column.name == parent_columns[0] && (column.primary_key || column.unique))
    {
        return true;
    }

    parent_meta
        .unique_constraints
        .iter()
        .any(|unique| unique == parent_columns)
}

pub(crate) fn map_column_type(dtype: &DataType) -> contextdb_core::ColumnType {
    match dtype {
        DataType::Uuid => contextdb_core::ColumnType::Uuid,
        DataType::Text => contextdb_core::ColumnType::Text,
        DataType::Integer => contextdb_core::ColumnType::Integer,
        DataType::Real => contextdb_core::ColumnType::Real,
        DataType::Boolean => contextdb_core::ColumnType::Boolean,
        DataType::Timestamp => contextdb_core::ColumnType::Timestamp,
        DataType::Json => contextdb_core::ColumnType::Json,
        DataType::Vector(dim) => contextdb_core::ColumnType::Vector(*dim as usize),
        DataType::TxId => contextdb_core::ColumnType::TxId,
    }
}

pub(crate) fn map_rank_policy(
    policy: &contextdb_parser::ast::RankPolicyAst,
) -> contextdb_core::RankPolicy {
    contextdb_core::RankPolicy {
        joined_table: policy.joined_table.clone(),
        joined_column: policy.joined_column.clone(),
        anchor_column: String::new(),
        sort_key: policy.sort_key.clone(),
        formula: policy.formula.clone(),
        protected_index: String::new(),
    }
}

struct ResolvedRankPolicy {
    policy: contextdb_core::RankPolicy,
    formula: Arc<RankFormula>,
}

pub(crate) fn core_column_from_ast(
    table: &str,
    col: &contextdb_parser::ast::ColumnDef,
    rank_policy: Option<contextdb_core::RankPolicy>,
) -> Result<contextdb_core::ColumnDef> {
    let (
        partition_key_columns,
        max_partitions,
        search_mode,
        auto_index_at,
        hnsw_m,
        hnsw_ef_construction,
        hnsw_ef_search,
        consolidation_change_percent,
        consolidation_tombstone_percent,
        consolidation_disabled,
    ) = normalized_ast_vector_declaration(table, col)?;
    Ok(contextdb_core::ColumnDef {
        name: col.name.clone(),
        column_type: map_column_type(&col.data_type),
        // SQL primary-key columns are implicitly NOT NULL even when the
        // declaration does not repeat that constraint explicitly.
        nullable: col.nullable && !col.primary_key,
        primary_key: col.primary_key,
        unique: col.unique,
        default: col.default.as_ref().map(stored_default_expr),
        references: col
            .references
            .as_ref()
            .map(|reference| contextdb_core::ForeignKeyReference {
                table: reference.table.clone(),
                column: reference.column.clone(),
            }),
        expires: col.expires,
        immutable: col.immutable,
        quantization: map_vector_quantization(col.quantization),
        rank_policy,
        context_id: col.context_id,
        scope_label: col.scope_label.as_deref().map(|scope| match scope {
            contextdb_parser::ast::ScopeLabelConstraint::Simple { labels } => {
                contextdb_core::ScopeLabelKind::Simple {
                    write_labels: labels.clone(),
                }
            }
            contextdb_parser::ast::ScopeLabelConstraint::Split { read, write } => {
                contextdb_core::ScopeLabelKind::Split {
                    read_labels: read.clone(),
                    write_labels: write.clone(),
                }
            }
        }),
        acl_ref: col.acl_ref.as_ref().map(|acl| contextdb_core::AclRef {
            ref_table: acl.ref_table.clone(),
            ref_column: acl.ref_column.clone(),
        }),
        partition_key_columns,
        max_partitions,
        search_mode,
        auto_index_at,
        hnsw_m,
        hnsw_ef_construction,
        hnsw_ef_search,
        vector_policy_revision: contextdb_core::DEFAULT_VECTOR_POLICY_REVISION,
        consolidation_change_percent,
        consolidation_tombstone_percent,
        consolidation_disabled,
    })
}

fn ast_column_from_core(col: contextdb_core::ColumnDef) -> contextdb_parser::ast::ColumnDef {
    let is_vector = matches!(&col.column_type, ColumnType::Vector(_));
    let effective_max_partitions = col.effective_max_partitions();
    contextdb_parser::ast::ColumnDef {
        name: col.name,
        data_type: match col.column_type {
            ColumnType::Uuid => DataType::Uuid,
            ColumnType::Text => DataType::Text,
            ColumnType::Integer => DataType::Integer,
            ColumnType::Real => DataType::Real,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Timestamp => DataType::Timestamp,
            ColumnType::Json => DataType::Json,
            ColumnType::Vector(dim) => DataType::Vector(dim as u32),
            ColumnType::TxId => DataType::TxId,
        },
        nullable: col.nullable,
        primary_key: col.primary_key,
        unique: col.unique,
        default: None,
        references: None,
        expires: col.expires,
        immutable: col.immutable,
        quantization: match col.quantization {
            contextdb_core::VectorQuantization::F32 => {
                contextdb_parser::ast::VectorQuantization::F32
            }
            contextdb_core::VectorQuantization::SQ8 => {
                contextdb_parser::ast::VectorQuantization::SQ8
            }
            contextdb_core::VectorQuantization::SQ4 => {
                contextdb_parser::ast::VectorQuantization::SQ4
            }
        },
        partition_key_columns: if is_vector {
            col.partition_key_columns.unwrap_or_default()
        } else {
            Vec::new()
        },
        max_partitions: is_vector
            .then_some(effective_max_partitions)
            .flatten()
            .map(|value| value.to_string()),
        search_mode: is_vector.then_some(map_core_vector_search_mode(col.search_mode)),
        auto_index_at: col.auto_index_at.map(|value| value.to_string()),
        hnsw: contextdb_parser::ast::VectorHnswOptions {
            m: col
                .hnsw_m
                .map(|value| contextdb_parser::ast::VectorPolicyValue::Value(value.to_string())),
            ef_construction: col
                .hnsw_ef_construction
                .map(|value| contextdb_parser::ast::VectorPolicyValue::Value(value.to_string())),
            ef_search: col
                .hnsw_ef_search
                .map(|value| contextdb_parser::ast::VectorPolicyValue::Value(value.to_string())),
        },
        consolidation: contextdb_parser::ast::VectorConsolidationOptions {
            change_percent: col
                .consolidation_change_percent
                .map(|value| contextdb_parser::ast::VectorPolicyValue::Value(value.to_string())),
            tombstone_percent: col
                .consolidation_tombstone_percent
                .map(|value| contextdb_parser::ast::VectorPolicyValue::Value(value.to_string())),
            disabled: col.consolidation_disabled,
        },
        rank_policy: None,
        context_id: col.context_id,
        scope_label: None,
        acl_ref: None,
    }
}

fn invalid_vector_partition_declaration(
    table: &str,
    column: &str,
    issue: VectorPartitionDeclarationIssue,
) -> Error {
    Error::InvalidVectorPartitionDeclaration {
        index: VectorIndexRef::new(table, column),
        issue,
    }
}

pub(crate) fn map_vector_search_mode(
    mode: contextdb_parser::ast::VectorSearchMode,
) -> contextdb_core::VectorSearchMode {
    match mode {
        contextdb_parser::ast::VectorSearchMode::Auto => contextdb_core::VectorSearchMode::Auto,
        contextdb_parser::ast::VectorSearchMode::Exact => contextdb_core::VectorSearchMode::Exact,
        contextdb_parser::ast::VectorSearchMode::Indexed => {
            contextdb_core::VectorSearchMode::Indexed
        }
    }
}

fn map_core_vector_search_mode(
    mode: contextdb_core::VectorSearchMode,
) -> contextdb_parser::ast::VectorSearchMode {
    match mode {
        contextdb_core::VectorSearchMode::Auto => contextdb_parser::ast::VectorSearchMode::Auto,
        contextdb_core::VectorSearchMode::Exact => contextdb_parser::ast::VectorSearchMode::Exact,
        contextdb_core::VectorSearchMode::Indexed => {
            contextdb_parser::ast::VectorSearchMode::Indexed
        }
    }
}

fn parse_vector_max_partitions(table: &str, column: &str, raw: &str) -> Result<u32> {
    let raw = raw.trim();
    if raw.starts_with('-') {
        return Err(invalid_vector_partition_declaration(
            table,
            column,
            VectorPartitionDeclarationIssue::MaxPartitionsNotPositive,
        ));
    }
    match raw.parse::<u128>() {
        Ok(0) => Err(invalid_vector_partition_declaration(
            table,
            column,
            VectorPartitionDeclarationIssue::MaxPartitionsNotPositive,
        )),
        Ok(value) => u32::try_from(value).map_err(|_| {
            invalid_vector_partition_declaration(
                table,
                column,
                VectorPartitionDeclarationIssue::MaxPartitionsOutOfRange,
            )
        }),
        Err(_) => Err(invalid_vector_partition_declaration(
            table,
            column,
            VectorPartitionDeclarationIssue::MaxPartitionsOutOfRange,
        )),
    }
}

fn invalid_vector_policy_declaration(
    table: &str,
    column: &str,
    issue: contextdb_core::VectorPolicyDeclarationIssue,
) -> Error {
    Error::InvalidVectorPolicyDeclaration {
        index: VectorIndexRef::new(table, column),
        issue,
    }
}

fn parse_vector_policy_u32(
    table: &str,
    column: &str,
    raw: &str,
    auto_index_at: bool,
) -> Result<u32> {
    use contextdb_core::VectorPolicyDeclarationIssue as Issue;

    let raw = raw.trim();
    let not_positive = if auto_index_at {
        Issue::AutoIndexAtNotPositive
    } else {
        Issue::HnswValueNotPositive
    };
    let out_of_range = if auto_index_at {
        Issue::AutoIndexAtOutOfRange
    } else {
        Issue::HnswValueOutOfRange
    };
    if raw.starts_with('-') {
        return Err(invalid_vector_policy_declaration(
            table,
            column,
            not_positive,
        ));
    }
    match raw.parse::<u128>() {
        Ok(0) => Err(invalid_vector_policy_declaration(
            table,
            column,
            not_positive,
        )),
        Ok(value) => u32::try_from(value)
            .map_err(|_| invalid_vector_policy_declaration(table, column, out_of_range)),
        Err(_) => Err(invalid_vector_policy_declaration(
            table,
            column,
            out_of_range,
        )),
    }
}

fn parsed_hnsw_member(
    table: &str,
    column: &str,
    member: Option<&contextdb_parser::ast::VectorPolicyValue>,
) -> Result<Option<u32>> {
    match member {
        None | Some(contextdb_parser::ast::VectorPolicyValue::Default) => Ok(None),
        Some(contextdb_parser::ast::VectorPolicyValue::Value(raw)) => {
            parse_vector_policy_u32(table, column, raw, false).map(Some)
        }
    }
}

fn parsed_consolidation_member(
    table: &str,
    column: &str,
    member: Option<&contextdb_parser::ast::VectorPolicyValue>,
) -> Result<Option<u32>> {
    match member {
        None | Some(contextdb_parser::ast::VectorPolicyValue::Default) => Ok(None),
        Some(contextdb_parser::ast::VectorPolicyValue::Value(raw)) => {
            let parsed = raw.trim().parse::<u32>().ok();
            match parsed.filter(|value| (1..=100).contains(value)) {
                Some(value) => Ok(Some(value)),
                None => Err(invalid_vector_policy_declaration(
                    table,
                    column,
                    contextdb_core::VectorPolicyDeclarationIssue::ConsolidationPercentOutOfRange,
                )),
            }
        }
    }
}

fn validate_resolved_hnsw_policy(
    table: &str,
    column: &str,
    quantization: contextdb_core::VectorQuantization,
    hnsw_m: Option<u32>,
    hnsw_ef_construction: Option<u32>,
    hnsw_ef_search: Option<u32>,
) -> Result<()> {
    let layout = contextdb_vector::store::VectorIndexLayout::new(
        1,
        quantization,
        Vec::new(),
        None,
        contextdb_core::VectorSearchMode::Auto,
    )
    .with_policy(
        None,
        hnsw_m,
        hnsw_ef_construction,
        hnsw_ef_search,
        contextdb_core::DEFAULT_VECTOR_POLICY_REVISION,
    );
    if layout.hnsw_policy_is_valid() {
        Ok(())
    } else {
        Err(invalid_vector_policy_declaration(
            table,
            column,
            contextdb_core::VectorPolicyDeclarationIssue::EfConstructionBelowM,
        ))
    }
}

type NormalizedAstVectorDeclaration = (
    Option<Vec<String>>,
    Option<u32>,
    contextdb_core::VectorSearchMode,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    bool,
);

fn normalized_ast_vector_declaration(
    table: &str,
    col: &contextdb_parser::ast::ColumnDef,
) -> Result<NormalizedAstVectorDeclaration> {
    let has_partition_clause = !col.partition_key_columns.is_empty()
        || col.max_partitions.is_some()
        || col.search_mode.is_some();
    let has_policy_clause = col.auto_index_at.is_some()
        || col.hnsw != contextdb_parser::ast::VectorHnswOptions::default()
        || col.consolidation != contextdb_parser::ast::VectorConsolidationOptions::default();
    if !matches!(col.data_type, DataType::Vector(_)) {
        if has_policy_clause {
            return Err(invalid_vector_policy_declaration(
                table,
                &col.name,
                contextdb_core::VectorPolicyDeclarationIssue::RequiresVectorColumn,
            ));
        }
        if has_partition_clause {
            return Err(invalid_vector_partition_declaration(
                table,
                &col.name,
                VectorPartitionDeclarationIssue::RequiresVectorColumn,
            ));
        }
        return Ok((
            None,
            None,
            contextdb_core::VectorSearchMode::Auto,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        ));
    }

    let partition_key_columns =
        (!col.partition_key_columns.is_empty()).then(|| col.partition_key_columns.clone());
    if partition_key_columns.is_none() && col.max_partitions.is_some() {
        return Err(invalid_vector_partition_declaration(
            table,
            &col.name,
            VectorPartitionDeclarationIssue::MaxPartitionsWithoutPartitionKey,
        ));
    }
    let max_partitions = if partition_key_columns.is_some() {
        Some(match col.max_partitions.as_deref() {
            Some(raw) => parse_vector_max_partitions(table, &col.name, raw)?,
            None => DEFAULT_VECTOR_MAX_PARTITIONS,
        })
    } else {
        None
    };
    let search_mode = col
        .search_mode
        .map(map_vector_search_mode)
        .unwrap_or(contextdb_core::VectorSearchMode::Auto);
    let auto_index_at = col
        .auto_index_at
        .as_deref()
        .map(|raw| parse_vector_policy_u32(table, &col.name, raw, true))
        .transpose()?;
    let hnsw_m = parsed_hnsw_member(table, &col.name, col.hnsw.m.as_ref())?;
    let hnsw_ef_construction =
        parsed_hnsw_member(table, &col.name, col.hnsw.ef_construction.as_ref())?;
    let hnsw_ef_search = parsed_hnsw_member(table, &col.name, col.hnsw.ef_search.as_ref())?;
    let consolidation_change_percent =
        parsed_consolidation_member(table, &col.name, col.consolidation.change_percent.as_ref())?;
    let consolidation_tombstone_percent = parsed_consolidation_member(
        table,
        &col.name,
        col.consolidation.tombstone_percent.as_ref(),
    )?;
    let consolidation_disabled = col.consolidation.disabled;
    validate_resolved_hnsw_policy(
        table,
        &col.name,
        map_vector_quantization(col.quantization),
        hnsw_m,
        hnsw_ef_construction,
        hnsw_ef_search,
    )?;
    Ok((
        partition_key_columns,
        max_partitions,
        search_mode,
        auto_index_at,
        hnsw_m,
        hnsw_ef_construction,
        hnsw_ef_search,
        consolidation_change_percent,
        consolidation_tombstone_percent,
        consolidation_disabled,
    ))
}

pub(crate) fn validate_create_table_vector_declarations(
    table: &str,
    create: &contextdb_parser::ast::CreateTable,
) -> Result<()> {
    let mut columns = create
        .columns
        .iter()
        .map(|column| core_column_from_ast(table, column, None))
        .collect::<Result<Vec<_>>>()?;
    for column in &mut columns {
        if create.primary_key_columns.contains(&column.name) {
            column.nullable = false;
        }
    }
    validate_vector_partition_declarations(
        table,
        &TableMeta {
            columns,
            ..TableMeta::default()
        },
    )
}

pub(crate) fn validate_vector_partition_declarations(table: &str, meta: &TableMeta) -> Result<()> {
    for vector in &meta.columns {
        if !matches!(vector.column_type, ColumnType::Vector(_)) {
            if vector.partition_key_columns.is_some()
                || vector.max_partitions.is_some()
                || vector.search_mode != contextdb_core::VectorSearchMode::Auto
                || vector.auto_index_at.is_some()
                || vector.hnsw_m.is_some()
                || vector.hnsw_ef_construction.is_some()
                || vector.hnsw_ef_search.is_some()
                || vector.consolidation_change_percent.is_some()
                || vector.consolidation_tombstone_percent.is_some()
                || vector.consolidation_disabled
            {
                return Err(invalid_vector_policy_declaration(
                    table,
                    &vector.name,
                    contextdb_core::VectorPolicyDeclarationIssue::RequiresVectorColumn,
                ));
            }
            continue;
        }

        if vector.auto_index_at == Some(0) {
            return Err(invalid_vector_policy_declaration(
                table,
                &vector.name,
                contextdb_core::VectorPolicyDeclarationIssue::AutoIndexAtNotPositive,
            ));
        }
        if vector.hnsw_m == Some(0)
            || vector.hnsw_ef_construction == Some(0)
            || vector.hnsw_ef_search == Some(0)
        {
            return Err(invalid_vector_policy_declaration(
                table,
                &vector.name,
                contextdb_core::VectorPolicyDeclarationIssue::HnswValueNotPositive,
            ));
        }
        validate_resolved_hnsw_policy(
            table,
            &vector.name,
            vector.quantization,
            vector.hnsw_m,
            vector.hnsw_ef_construction,
            vector.hnsw_ef_search,
        )?;
        if vector
            .consolidation_change_percent
            .is_some_and(|value| !(1..=100).contains(&value))
            || vector
                .consolidation_tombstone_percent
                .is_some_and(|value| !(1..=100).contains(&value))
        {
            return Err(invalid_vector_policy_declaration(
                table,
                &vector.name,
                contextdb_core::VectorPolicyDeclarationIssue::ConsolidationPercentOutOfRange,
            ));
        }

        let Some(partition_key_columns) = vector.partition_key_columns.as_ref() else {
            if vector.max_partitions.is_some() {
                return Err(invalid_vector_partition_declaration(
                    table,
                    &vector.name,
                    VectorPartitionDeclarationIssue::MaxPartitionsWithoutPartitionKey,
                ));
            }
            continue;
        };
        if partition_key_columns.is_empty() {
            return Err(invalid_vector_partition_declaration(
                table,
                &vector.name,
                VectorPartitionDeclarationIssue::EmptyPartitionKey,
            ));
        }
        if vector.max_partitions == Some(0) {
            return Err(invalid_vector_partition_declaration(
                table,
                &vector.name,
                VectorPartitionDeclarationIssue::MaxPartitionsNotPositive,
            ));
        }

        let mut seen = HashSet::new();
        for component_name in partition_key_columns {
            if !seen.insert(component_name.as_str()) {
                return Err(invalid_vector_partition_declaration(
                    table,
                    &vector.name,
                    VectorPartitionDeclarationIssue::DuplicatePartitionKeyColumn,
                ));
            }
            if component_name == &vector.name {
                return Err(invalid_vector_partition_declaration(
                    table,
                    &vector.name,
                    VectorPartitionDeclarationIssue::VectorColumnInPartitionKey,
                ));
            }
            let Some(component) = meta
                .columns
                .iter()
                .find(|column| column.name == *component_name)
            else {
                return Err(invalid_vector_partition_declaration(
                    table,
                    &vector.name,
                    VectorPartitionDeclarationIssue::UnknownPartitionKeyColumn,
                ));
            };
            if component.nullable {
                return Err(invalid_vector_partition_declaration(
                    table,
                    &vector.name,
                    VectorPartitionDeclarationIssue::NullablePartitionKeyColumn,
                ));
            }
            if !component.column_type.is_vector_partition_key_type() {
                return Err(invalid_vector_partition_declaration(
                    table,
                    &vector.name,
                    VectorPartitionDeclarationIssue::UnsupportedPartitionKeyColumnType,
                ));
            }
        }
    }
    Ok(())
}

fn vector_partition_key_component_blocker(
    db: &Database,
    table: &str,
    component: &str,
) -> Option<Error> {
    let meta = db.table_meta(table)?;
    let vector = meta.columns.iter().find(|column| {
        matches!(column.column_type, ColumnType::Vector(_))
            && column
                .partition_key_columns
                .as_ref()
                .is_some_and(|key| key.iter().any(|name| name == component))
    })?;
    Some(Error::VectorPartitionKeyInUse {
        table: table.to_owned(),
        column: component.to_owned(),
        index: vector.name.clone(),
    })
}

pub(crate) fn set_vector_max_partitions(
    table: &str,
    column: &str,
    raw: &str,
    meta: &mut TableMeta,
) -> Result<()> {
    let vector = meta
        .columns
        .iter_mut()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| Error::ColumnNotFound {
            table: table.to_owned(),
            column: column.to_owned(),
        })?;
    if !matches!(vector.column_type, ColumnType::Vector(_)) {
        return Err(invalid_vector_partition_declaration(
            table,
            column,
            VectorPartitionDeclarationIssue::RequiresVectorColumn,
        ));
    }
    if vector.partition_key_columns.is_none() {
        return Err(invalid_vector_partition_declaration(
            table,
            column,
            VectorPartitionDeclarationIssue::MaxPartitionsWithoutPartitionKey,
        ));
    }
    let max_partitions = parse_vector_max_partitions(table, column, raw)?;
    vector.max_partitions = Some(max_partitions);
    Ok(())
}

pub(crate) fn set_vector_search_mode(
    table: &str,
    column: &str,
    mode: contextdb_parser::ast::VectorSearchMode,
    meta: &mut TableMeta,
) -> Result<()> {
    let vector = meta
        .columns
        .iter_mut()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| Error::ColumnNotFound {
            table: table.to_owned(),
            column: column.to_owned(),
        })?;
    if !matches!(vector.column_type, ColumnType::Vector(_)) {
        return Err(invalid_vector_partition_declaration(
            table,
            column,
            VectorPartitionDeclarationIssue::RequiresVectorColumn,
        ));
    }
    vector.search_mode = map_vector_search_mode(mode);
    Ok(())
}

pub(crate) fn set_vector_auto_index_at(
    table: &str,
    column: &str,
    raw: Option<&str>,
    meta: &mut TableMeta,
) -> Result<()> {
    let vector = meta
        .columns
        .iter_mut()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| Error::ColumnNotFound {
            table: table.to_owned(),
            column: column.to_owned(),
        })?;
    if !matches!(vector.column_type, ColumnType::Vector(_)) {
        return Err(invalid_vector_policy_declaration(
            table,
            column,
            contextdb_core::VectorPolicyDeclarationIssue::RequiresVectorColumn,
        ));
    }
    vector.auto_index_at = raw
        .map(|raw| parse_vector_policy_u32(table, column, raw, true))
        .transpose()?;
    Ok(())
}

pub(crate) fn set_vector_hnsw(
    table: &str,
    column: &str,
    changes: Option<&contextdb_parser::ast::VectorHnswOptions>,
    meta: &mut TableMeta,
) -> Result<()> {
    let vector = meta
        .columns
        .iter_mut()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| Error::ColumnNotFound {
            table: table.to_owned(),
            column: column.to_owned(),
        })?;
    if !matches!(vector.column_type, ColumnType::Vector(_)) {
        return Err(invalid_vector_policy_declaration(
            table,
            column,
            contextdb_core::VectorPolicyDeclarationIssue::RequiresVectorColumn,
        ));
    }

    let old_topology = (vector.hnsw_m, vector.hnsw_ef_construction);
    match changes {
        None => {
            vector.hnsw_m = None;
            vector.hnsw_ef_construction = None;
            vector.hnsw_ef_search = None;
        }
        Some(changes) => {
            if let Some(member) = changes.m.as_ref() {
                vector.hnsw_m = parsed_hnsw_member(table, column, Some(member))?;
            }
            if let Some(member) = changes.ef_construction.as_ref() {
                vector.hnsw_ef_construction = parsed_hnsw_member(table, column, Some(member))?;
            }
            if let Some(member) = changes.ef_search.as_ref() {
                vector.hnsw_ef_search = parsed_hnsw_member(table, column, Some(member))?;
            }
        }
    }
    validate_resolved_hnsw_policy(
        table,
        column,
        vector.quantization,
        vector.hnsw_m,
        vector.hnsw_ef_construction,
        vector.hnsw_ef_search,
    )?;
    if old_topology != (vector.hnsw_m, vector.hnsw_ef_construction) {
        vector.vector_policy_revision = next_vector_policy_revision(vector.vector_policy_revision)
            .ok_or_else(|| {
                invalid_vector_policy_declaration(
                    table,
                    column,
                    contextdb_core::VectorPolicyDeclarationIssue::HnswValueOutOfRange,
                )
            })?;
    }
    Ok(())
}

pub(crate) fn set_vector_consolidation(
    table: &str,
    column: &str,
    changes: Option<&contextdb_parser::ast::VectorConsolidationOptions>,
    meta: &mut TableMeta,
) -> Result<()> {
    let vector = meta
        .columns
        .iter_mut()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| Error::ColumnNotFound {
            table: table.to_owned(),
            column: column.to_owned(),
        })?;
    if !matches!(vector.column_type, ColumnType::Vector(_)) {
        return Err(invalid_vector_policy_declaration(
            table,
            column,
            contextdb_core::VectorPolicyDeclarationIssue::RequiresVectorColumn,
        ));
    }
    match changes {
        None => {
            vector.consolidation_change_percent = None;
            vector.consolidation_tombstone_percent = None;
            vector.consolidation_disabled = false;
        }
        Some(changes) => {
            if changes.disabled {
                vector.consolidation_change_percent = None;
                vector.consolidation_tombstone_percent = None;
                vector.consolidation_disabled = true;
                return Ok(());
            }
            vector.consolidation_disabled = false;
            if let Some(member) = changes.change_percent.as_ref() {
                vector.consolidation_change_percent =
                    parsed_consolidation_member(table, column, Some(member))?;
            }
            if let Some(member) = changes.tombstone_percent.as_ref() {
                vector.consolidation_tombstone_percent =
                    parsed_consolidation_member(table, column, Some(member))?;
            }
        }
    }
    Ok(())
}

/// The next desired topology revision after `M` or `EF_CONSTRUCTION` changed,
/// shared by the local ALTER door and the synced-schema receiver so both
/// advance on exactly the same axes. `None` only on u64 overflow.
pub(crate) fn next_vector_policy_revision(current: u64) -> Option<u64> {
    current
        .max(contextdb_core::DEFAULT_VECTOR_POLICY_REVISION)
        .checked_add(1)
}

fn validate_rank_policy_for_column(
    db: &Database,
    table: &str,
    column: &contextdb_parser::ast::ColumnDef,
    all_columns: &[contextdb_parser::ast::ColumnDef],
) -> Result<Option<ResolvedRankPolicy>> {
    let Some(policy_ast) = column.rank_policy.as_deref() else {
        return Ok(None);
    };
    let index = rank_index_name(table, &column.name);
    if !matches!(column.data_type, DataType::Vector(_)) {
        return Err(Error::RankPolicyColumnType {
            index,
            column: column.name.clone(),
            expected: "VECTOR(N)".to_string(),
            actual: data_type_name(&column.data_type).to_string(),
        });
    }
    let joined_meta = db.table_meta(&policy_ast.joined_table).ok_or_else(|| {
        Error::RankPolicyJoinTableUnknown {
            index: index.clone(),
            table: policy_ast.joined_table.clone(),
        }
    })?;
    if !joined_meta
        .columns
        .iter()
        .any(|col| col.name == policy_ast.joined_column)
    {
        return Err(Error::RankPolicyJoinColumnUnknown {
            index: index.clone(),
            table: policy_ast.joined_table.clone(),
            column: policy_ast.joined_column.clone(),
        });
    }
    let protected_index = protected_rank_policy_index(&joined_meta, &policy_ast.joined_column)
        .ok_or_else(|| Error::RankPolicyJoinColumnUnindexed {
            index: index.clone(),
            joined_table: policy_ast.joined_table.clone(),
            column: policy_ast.joined_column.clone(),
        })?;
    let anchor_column =
        resolve_rank_policy_anchor_column(&index, policy_ast, all_columns, &joined_meta)?;
    let formula = Arc::new(RankFormula::compile_for_index(&index, &policy_ast.formula)?);
    validate_rank_formula_columns(
        &index,
        &column.name,
        all_columns,
        &joined_meta,
        formula.column_refs(),
    )?;
    Ok(Some(ResolvedRankPolicy {
        policy: contextdb_core::RankPolicy {
            joined_table: policy_ast.joined_table.clone(),
            joined_column: policy_ast.joined_column.clone(),
            anchor_column,
            sort_key: policy_ast.sort_key.clone(),
            formula: policy_ast.formula.clone(),
            protected_index,
        },
        formula,
    }))
}

fn protected_rank_policy_index(meta: &TableMeta, joined_column: &str) -> Option<String> {
    meta.indexes
        .iter()
        .filter(|index| index.kind == contextdb_core::IndexKind::UserDeclared)
        .chain(meta.indexes.iter())
        .find(|index| {
            index
                .columns
                .first()
                .is_some_and(|(column, _)| column == joined_column)
        })
        .map(|index| index.name.clone())
}

fn resolve_rank_policy_anchor_column(
    index: &str,
    policy: &contextdb_parser::ast::RankPolicyAst,
    anchor_columns: &[contextdb_parser::ast::ColumnDef],
    joined_meta: &TableMeta,
) -> Result<String> {
    let joined_column = joined_meta
        .columns
        .iter()
        .find(|col| col.name == policy.joined_column)
        .ok_or_else(|| Error::RankPolicyJoinColumnUnknown {
            index: index.to_string(),
            table: policy.joined_table.clone(),
            column: policy.joined_column.clone(),
        })?;

    let anchor_by_name = |name: &str| anchor_columns.iter().find(|col| col.name == name);
    let mut candidates = Vec::new();
    if joined_column.primary_key {
        let singular = singular_table_name(&policy.joined_table);
        for name in [
            format!("{singular}_id"),
            format!("{}_id", policy.joined_table),
        ] {
            if anchor_by_name(&name).is_some() && !candidates.contains(&name) {
                candidates.push(name);
            }
        }
    }
    if candidates.is_empty() && anchor_by_name(&policy.joined_column).is_some() {
        candidates.push(policy.joined_column.clone());
    }
    if candidates.is_empty()
        && let Some(primary_key) = anchor_columns.iter().find(|col| col.primary_key)
    {
        candidates.push(primary_key.name.clone());
    }
    if candidates.is_empty() && anchor_by_name("id").is_some() {
        candidates.push("id".to_string());
    }

    let anchor_column = match candidates.as_slice() {
        [single] => single.clone(),
        [] => {
            return Err(Error::RankPolicyColumnUnknown {
                index: index.to_string(),
                column: policy.joined_column.clone(),
            });
        }
        _ => {
            return Err(Error::RankPolicyColumnAmbiguous {
                index: index.to_string(),
                column: candidates.join(","),
            });
        }
    };
    let anchor_def =
        anchor_by_name(&anchor_column).ok_or_else(|| Error::RankPolicyColumnUnknown {
            index: index.to_string(),
            column: anchor_column.clone(),
        })?;
    let anchor_type = map_column_type(&anchor_def.data_type);
    if anchor_type != joined_column.column_type {
        return Err(Error::RankPolicyColumnType {
            index: index.to_string(),
            column: anchor_column,
            expected: column_type_name(&joined_column.column_type).to_string(),
            actual: data_type_name(&anchor_def.data_type).to_string(),
        });
    }
    Ok(anchor_column)
}

fn singular_table_name(table: &str) -> String {
    if let Some(stem) = table.strip_suffix("ies") {
        format!("{stem}y")
    } else if let Some(stem) = table.strip_suffix('s') {
        stem.to_string()
    } else {
        table.to_string()
    }
}

fn validate_rank_formula_columns(
    index: &str,
    anchor_vector_column: &str,
    anchor_columns: &[contextdb_parser::ast::ColumnDef],
    joined_meta: &TableMeta,
    refs: &[String],
) -> Result<()> {
    let vector_score_column_exists = anchor_columns.iter().any(|col| col.name == "vector_score")
        || joined_meta
            .columns
            .iter()
            .any(|col| col.name == "vector_score");
    for column in refs {
        if column == "vector_score" {
            if vector_score_column_exists {
                return Err(Error::RankPolicyColumnAmbiguous {
                    index: index.to_string(),
                    column: column.clone(),
                });
            }
            continue;
        }
        let anchor = anchor_columns.iter().find(|col| col.name == *column);
        let joined = joined_meta.columns.iter().find(|col| col.name == *column);
        if anchor.is_none() && joined.is_none() {
            return Err(Error::RankPolicyColumnUnknown {
                index: index.to_string(),
                column: column.clone(),
            });
        }
        if column == "id" && anchor.is_some() && joined.is_some() {
            return Err(Error::RankPolicyColumnAmbiguous {
                index: index.to_string(),
                column: column.clone(),
            });
        }
        if let Some(anchor) = anchor {
            validate_rank_formula_type(
                index,
                column,
                data_type_name(&anchor.data_type),
                &map_column_type(&anchor.data_type),
            )?;
        } else if let Some(joined) = joined {
            validate_rank_formula_type(
                index,
                column,
                column_type_name(&joined.column_type),
                &joined.column_type,
            )?;
        }
    }
    if refs.iter().any(|column| column == anchor_vector_column) {
        return Err(Error::RankPolicyColumnType {
            index: index.to_string(),
            column: anchor_vector_column.to_string(),
            expected: "number-or-bool".to_string(),
            actual: "VECTOR".to_string(),
        });
    }
    Ok(())
}

fn validate_rank_formula_type(
    index: &str,
    column: &str,
    actual_name: &str,
    column_type: &ColumnType,
) -> Result<()> {
    if matches!(
        column_type,
        ColumnType::Real | ColumnType::Integer | ColumnType::Boolean
    ) {
        return Ok(());
    }
    Err(Error::RankPolicyColumnType {
        index: index.to_string(),
        column: column.to_string(),
        expected: "number-or-bool".to_string(),
        actual: actual_name.to_string(),
    })
}

fn data_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Uuid => "UUID",
        DataType::Text => "TEXT",
        DataType::Integer => "INTEGER",
        DataType::Real => "REAL",
        DataType::Boolean => "BOOLEAN",
        DataType::Timestamp => "TIMESTAMP",
        DataType::Json => "JSON",
        DataType::Vector(_) => "VECTOR",
        DataType::TxId => "TXID",
    }
}

fn column_type_name(column_type: &ColumnType) -> &'static str {
    match column_type {
        ColumnType::Uuid => "UUID",
        ColumnType::Text => "TEXT",
        ColumnType::Integer => "INTEGER",
        ColumnType::Real => "REAL",
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Timestamp => "TIMESTAMP",
        ColumnType::Json => "JSON",
        ColumnType::Vector(_) => "VECTOR",
        ColumnType::TxId => "TXID",
    }
}

pub(crate) fn rank_policy_drop_table_blocker(db: &Database, table: &str) -> Option<Error> {
    for (policy_table, policy_column, policy) in all_rank_policies(db) {
        if policy.joined_table == table && policy_table != table {
            return Some(Error::DropBlockedByRankPolicy {
                table: table.into(),
                column: None,
                dropped_index: None,
                policy_table: policy_table.into_boxed_str(),
                policy_column: policy_column.into_boxed_str(),
                sort_key: policy.sort_key.into_boxed_str(),
            });
        }
    }
    None
}

pub(crate) fn rank_policy_drop_index_blocker(
    db: &Database,
    table: &str,
    index: &str,
) -> Option<Error> {
    for (policy_table, policy_column, policy) in all_rank_policies(db) {
        if policy.joined_table == table && policy.protected_index == index {
            return Some(Error::DropBlockedByRankPolicy {
                table: table.into(),
                column: None,
                dropped_index: Some(index.into()),
                policy_table: policy_table.into_boxed_str(),
                policy_column: policy_column.into_boxed_str(),
                sort_key: policy.sort_key.into_boxed_str(),
            });
        }
    }
    None
}

fn rank_policy_drop_column_blocker(db: &Database, table: &str, column: &str) -> Option<Error> {
    let metas = db
        .table_names()
        .into_iter()
        .filter_map(|name| db.table_meta(&name).map(|meta| (name, meta)))
        .collect::<HashMap<_, _>>();
    for (policy_table, meta) in &metas {
        for policy_col in &meta.columns {
            let Some(policy) = &policy_col.rank_policy else {
                continue;
            };
            if policy_table == table && policy_col.name == column {
                return Some(drop_column_rank_error(
                    table,
                    column,
                    policy_table,
                    &policy_col.name,
                    policy,
                ));
            }
            if policy.joined_table == table && policy.joined_column == column {
                return Some(drop_column_rank_error(
                    table,
                    column,
                    policy_table,
                    &policy_col.name,
                    policy,
                ));
            }
            if policy_table == table && policy.anchor_column == column {
                return Some(drop_column_rank_error(
                    table,
                    column,
                    policy_table,
                    &policy_col.name,
                    policy,
                ));
            }
            let Ok(formula) = RankFormula::compile_for_index(
                &rank_index_name(policy_table, &policy_col.name),
                &policy.formula,
            ) else {
                continue;
            };
            let joined_meta = metas.get(&policy.joined_table);
            for reference in formula.column_refs() {
                if reference == "vector_score" {
                    continue;
                }
                let anchor_has = meta.columns.iter().any(|col| col.name == *reference);
                let joined_has = joined_meta
                    .is_some_and(|joined| joined.columns.iter().any(|col| col.name == *reference));
                if anchor_has && policy_table == table && reference == column {
                    return Some(drop_column_rank_error(
                        table,
                        column,
                        policy_table,
                        &policy_col.name,
                        policy,
                    ));
                }
                if !anchor_has && joined_has && policy.joined_table == table && reference == column
                {
                    return Some(drop_column_rank_error(
                        table,
                        column,
                        policy_table,
                        &policy_col.name,
                        policy,
                    ));
                }
            }
        }
    }
    None
}

fn drop_column_rank_error(
    table: &str,
    column: &str,
    policy_table: &str,
    policy_column: &str,
    policy: &contextdb_core::RankPolicy,
) -> Error {
    Error::DropBlockedByRankPolicy {
        table: table.into(),
        column: Some(column.into()),
        dropped_index: None,
        policy_table: policy_table.into(),
        policy_column: policy_column.into(),
        sort_key: policy.sort_key.clone().into_boxed_str(),
    }
}

fn all_rank_policies(db: &Database) -> Vec<(String, String, contextdb_core::RankPolicy)> {
    db.table_names()
        .into_iter()
        .filter_map(|table| db.table_meta(&table).map(|meta| (table, meta)))
        .flat_map(|(table, meta)| {
            meta.columns.into_iter().filter_map(move |column| {
                column
                    .rank_policy
                    .map(|policy| (table.clone(), column.name, policy))
            })
        })
        .collect()
}

fn map_vector_quantization(
    quantization: contextdb_parser::ast::VectorQuantization,
) -> contextdb_core::VectorQuantization {
    match quantization {
        contextdb_parser::ast::VectorQuantization::F32 => contextdb_core::VectorQuantization::F32,
        contextdb_parser::ast::VectorQuantization::SQ8 => contextdb_core::VectorQuantization::SQ8,
        contextdb_parser::ast::VectorQuantization::SQ4 => contextdb_core::VectorQuantization::SQ4,
    }
}

fn declared_conflict_policy_to_string(p: contextdb_core::ConflictPolicy) -> String {
    if p == contextdb_core::ConflictPolicy::KEEP_LATEST {
        "keep_latest".to_string()
    } else {
        "keep_first".to_string()
    }
}

#[cfg(test)]
fn project_graph_frontier_rows(
    frontier: Vec<(HashMap<String, uuid::Uuid>, uuid::Uuid, u32)>,
    start_alias: &str,
    steps: &[GraphStepPlan],
) -> Result<Vec<Vec<Value>>> {
    frontier
        .into_iter()
        .map(|(bindings, id, depth)| {
            let mut row = Vec::with_capacity(steps.len() + 3);
            let start_id = bindings.get(start_alias).ok_or_else(|| {
                Error::PlanError(format!(
                    "graph frontier missing required start alias binding '{start_alias}'"
                ))
            })?;
            row.push(Value::Uuid(*start_id));
            for step in steps {
                let target_id = bindings.get(&step.target_alias).ok_or_else(|| {
                    Error::PlanError(format!(
                        "graph frontier missing required target alias binding '{}'",
                        step.target_alias
                    ))
                })?;
                row.push(Value::Uuid(*target_id));
            }
            row.push(Value::Uuid(id));
            row.push(Value::Int64(depth as i64));
            Ok(row)
        })
        .collect()
}

impl ReadExecutionTarget for BoundedCursorTarget {
    #[cfg(feature = "test-seams")]
    fn resident_history_database_for_test(&self) -> Result<&Database> {
        self.store.resident_history_database_for_test()
    }

    fn load_row_vector_for_column(
        &self,
        tx: Option<TxId>,
        table: &str,
        column: &str,
        row_id: RowId,
        lsn: Lsn,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Vec<f32>>> {
        self.store.load_row_vector_for_column(
            tx,
            table,
            column,
            row_id,
            lsn,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_score_committed_vector_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        before_checkpoint: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<f32>, BoundedExecutionError> {
        self.store.bounded_score_committed_vector_row(
            index,
            row_id,
            snapshot,
            query,
            before_checkpoint,
            before_distance,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn quantized_vector_columns(&self, table: &str) -> Vec<String> {
        self.store.quantized_vector_columns(table)
    }

    fn delivery_metadata(
        &self,
        request: crate::direct_file_reader::DirectMetadataRequest,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::direct_file_reader::DirectMetadataBody> {
        self.store.delivery_metadata(request, read_memory)
    }

    fn committed_change_state(
        &self,
        checkpoint: &dyn Fn() -> Result<()>,
        charge: &Arc<crate::read_image_memory::ImageMemory>,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectChangeState,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        self.store.committed_change_state(checkpoint, charge)
    }

    fn committed_sync_state(
        &self,
        checkpoint: &dyn Fn() -> Result<()>,
        charge: &Arc<crate::read_image_memory::ImageMemory>,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectSyncState,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        self.store.committed_sync_state(checkpoint, charge)
    }

    fn content_size_answer(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_table: &mut dyn FnMut() -> Result<()>,
    ) -> Result<OwnedQueryResult> {
        self.store.content_size_answer(read_memory, before_table)
    }

    fn store_state_answer(
        &self,
        plan: &PhysicalPlan,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<OwnedQueryResult> {
        self.store
            .store_state_answer(plan, vector_copy_owner, read_memory.clone())
    }

    fn explain_plan_without_running_it(
        &self,
        sql: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<String> {
        self.store
            .explain_plan_without_running_it(sql, vector_copy_owner, read_memory)
    }

    fn explain_output_without_running_it(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::database::ExplainOutput> {
        self.store
            .explain_output_without_running_it(sql, params, vector_copy_owner, read_memory)
    }

    fn plugin(&self) -> &dyn crate::plugin::DatabasePlugin {
        self.store.plugin()
    }

    fn table_meta(&self, table: &str) -> Option<TableMeta> {
        self.store.table_meta(table)
    }

    fn natural_key_column_for_table(&self, table: &str) -> Result<String> {
        self.store.natural_key_column_for_table(table)
    }

    fn row_id_for_natural_key_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        key_col: &str,
        key_value: &Value,
        snapshot: SnapshotId,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<RowId>> {
        self.store.row_id_for_natural_key_in_tx(
            tx,
            table,
            key_col,
            key_value,
            snapshot,
            read_memory.clone(),
        )
    }

    fn prepare_bounded_read_plan(
        &self,
        parsed: &Statement,
        params: &HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(Statement, PhysicalPlan)> {
        self.store
            .prepare_bounded_read_plan(parsed, params, vector_copy_owner, read_memory.clone())
    }

    fn declared_read_request_provider(
        &self,
    ) -> Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider> {
        self.store.declared_read_request_provider()
    }

    fn bounded_read_accountant(&self) -> Arc<crate::memory_accounting::MemoryAccountant> {
        self.store.bounded_read_accountant()
    }

    fn automatic_indexes_are_ordered(&self) -> bool {
        self.store.automatic_indexes_are_ordered()
    }

    fn bounded_store_path(&self) -> String {
        self.store.bounded_store_path()
    }

    #[cfg(feature = "test-seams")]
    fn note_vector_candidate_materialized(&self) {
        self.store.note_vector_candidate_materialized();
    }

    #[cfg(feature = "test-seams")]
    fn vector_passive_activity_for_test(&self) -> contextdb_vector::VectorPassiveActivityCounters {
        self.store.vector_passive_activity_for_test()
    }

    #[cfg(feature = "test-seams")]
    fn force_history_log_gap_for_test(&self) {
        self.store.force_history_log_gap_for_test()
    }

    #[cfg(feature = "test-seams")]
    fn force_change_log_gap_for_test(&self) {
        self.store.force_change_log_gap_for_test()
    }

    #[cfg(feature = "test-seams")]
    fn vector_store_for_test(&self) -> Arc<contextdb_vector::VectorStore> {
        self.store.vector_store_for_test()
    }

    fn capture_read_schema(&self) -> crate::database::SchemaPublicationGuard<'_> {
        self.store.capture_read_schema()
    }

    fn relational_membership_store(&self) -> &contextdb_relational::RelationalStore {
        self.store.relational_membership_store()
    }

    fn register_writer_cursor(&self, cursor: &Arc<BoundedCursorExecutionState>) -> Result<()> {
        self.store.register_writer_cursor(cursor)
    }

    fn enter_writer_read(
        &self,
        root: Option<Arc<crate::persistence::WriterSnapshot>>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Option<crate::database::writer_read::WriterReadScope> {
        self.store.enter_writer_read(root, read_memory)
    }

    fn retained_relational_membership_source(&self) -> bool {
        self.store.retained_relational_membership_source()
    }

    fn release_statement_proof_reuse(&self) {
        self.store.release_statement_proof_reuse();
    }

    fn authenticated_relational_index_prefix_count(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &IndexKey,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<u64>> {
        self.store
            .authenticated_relational_index_prefix_count(table, columns, prefix, read_memory)
    }

    fn bounded_read_snapshot_registration(
        &self,
        needs_vector_graph: bool,
        budget: &crate::database::ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<BoundedReadSnapshotPin>> {
        self.store.bounded_read_snapshot_registration(
            needs_vector_graph,
            budget,
            withdrawn,
            read_memory,
        )
    }

    fn bounded_cursor_snapshot_registration(
        &self,
        needs_vector_graph: bool,
        clock: Arc<dyn DeadlineClock>,
        idle_ms: u64,
        budget: &crate::database::ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<BoundedCursorSnapshotPin>> {
        self.store.bounded_cursor_snapshot_registration(
            needs_vector_graph,
            clock,
            idle_ms,
            budget,
            withdrawn,
            read_memory,
        )
    }

    fn assert_table_read_allowed(&self, table: &str) -> Result<()> {
        self.store.assert_table_read_allowed(table)
    }

    fn bounded_read_requires_candidate_filter(&self, table: &str) -> Result<bool> {
        self.store.bounded_read_requires_candidate_filter(table)
    }

    fn vector_access_predicate(&self, table: &str) -> Result<Option<Expr>> {
        self.store.vector_access_predicate(table)
    }

    fn vector_access_restrictions(&self, table: &str) -> Result<Vec<(String, Vec<Value>)>> {
        self.store.vector_access_restrictions(table)
    }

    fn bounded_vector_visit_visible_ids(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        before_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),
        visit: &mut dyn FnMut(RowId) -> std::result::Result<(), BoundedExecutionError>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.store.bounded_vector_visit_visible_ids(
            index,
            snapshot,
            before_entry,
            before_retain,
            release_retained,
            visit,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_read_allowed_for_row(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        self.store.bounded_read_allowed_for_row(
            tx,
            table,
            meta,
            row,
            snapshot,
            before_access_row,
            read_memory.clone(),
        )
    }

    fn bounded_read_denial_for_row(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<Error>, BoundedExecutionError> {
        self.store.bounded_read_denial_for_row(
            tx,
            table,
            meta,
            row,
            snapshot,
            before_access_row,
            read_memory.clone(),
        )
    }

    fn vector_read_preflight(
        &self,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        copies: &mut contextdb_vector::VectorCopyAdmission,
        checkpoint: Option<bounded::ReadOpenContext>,
    ) -> Result<Option<crate::persistence::vector_membership_source::VectorReadScope>> {
        self.store.vector_read_preflight(snapshot, read_memory, copies, checkpoint)
    }

    fn preflight_historical_graph(
        &self, snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.store.preflight_historical_graph(snapshot, read_memory, before)
    }

    fn preflight_historical_index(
        &self, table: &str, index: &str, snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.store.preflight_historical_index(table, index, snapshot, read_memory, before)
    }

    fn bounded_row_by_identity(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError> {
        self.store.bounded_row_by_identity(
            read_memory,
            tx,
            table,
            row_id,
            snapshot,
            before_touch,
            before_clone,
        )
    }

    fn bounded_physical_table_cursor(
        &self,
        table: &str,
    ) -> Result<contextdb_relational::mem::BoundedPhysicalCursor> {
        self.store.bounded_physical_table_cursor(table)
    }

    fn bounded_physical_table_row_next(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedPhysicalCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        observed: &mut dyn FnMut(&VersionedRow) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError> {
        self.store.bounded_physical_table_row_next(
            read_memory,
            table,
            snapshot,
            cursor,
            before_touch,
            observed,
            before_clone,
        )
    }

    fn bounded_exact_index_row_next(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedExactIndexCursor,
        before_storage_step: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(
            usize) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError> {
        self.store.bounded_exact_index_row_next(
            read_memory,
            table,
            index,
            snapshot,
            cursor,
            before_storage_step,
            before_touch,
            before_retain,
            release_retained,
            before_clone,
        )
    }

    fn ordered_prefix_snapshot_is_current(
        &self,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        self.store
            .ordered_prefix_snapshot_is_current(snapshot, read_memory)
    }

    fn bounded_ordered_table_row_next_observed(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        column: &str,
        direction: contextdb_core::SortDirection,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        observed: &mut dyn FnMut(
            &IndexKey,
            RowId,
        ) -> std::result::Result<(), BoundedExecutionError>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<Option<VersionedRow>, BoundedExecutionError> {
        self.store.bounded_ordered_table_row_next_observed(
            read_memory,
            table,
            column,
            direction,
            snapshot,
            cursor,
            check_boundary,
            before_touch,
            observed,
            before_clone,
        )
    }

    fn bounded_visit_verified_ordered_identities(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        visit: &mut dyn FnMut(RowId) -> std::result::Result<(), BoundedExecutionError>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        self.store.bounded_visit_verified_ordered_identities(
            table,
            index,
            snapshot,
            cursor,
            before_touch,
            visit,
            read_memory,
        )
    }

    fn bounded_verified_index_prefixes_are_empty(
        &self,
        table: &str,
        index: &str,
        prefixes: &[IndexKey],
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<bool>, BoundedExecutionError> {
        self.store.bounded_verified_index_prefixes_are_empty(
            table,
            index,
            prefixes,
            check_boundary,
            before_touch,
            read_memory,
        )
    }

    fn bounded_vector_schema_read_many(
        &self,
        indexes: Vec<VectorIndexRef>,
    ) -> crate::database::VectorSchemaReadGuard {
        self.store.bounded_vector_schema_read_many(indexes)
    }

    fn assert_vector_index_exists_under_schema_read(&self, index: &VectorIndexRef) -> Result<()> {
        self.store
            .assert_vector_index_exists_under_schema_read(index)
    }

    fn validate_vector_under_schema_read(
        &self,
        index: &VectorIndexRef,
        actual: usize,
    ) -> Result<()> {
        self.store.validate_vector_under_schema_read(index, actual)
    }

    fn bounded_vector_candidate_groups(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        before_checkpoint: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_placement: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        contextdb_vector::mem::BoundedVectorCandidateGroups,
        BoundedExecutionError,
    > {
        self.store.bounded_vector_candidate_groups(
            index,
            candidates,
            snapshot,
            before_checkpoint,
            before_placement,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_vector_candidate_k(
        &self,
        index: &VectorIndexRef,
        requested: usize,
        sort_key: Option<&str>,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        before_source: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<usize, BoundedExecutionError> {
        self.store.bounded_vector_candidate_k(
            index,
            requested,
            sort_key,
            groups,
            before_source,
            vector_copy_owner,
            read_memory,
        )
    }

    fn bounded_vector_entry_count(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<usize> {
        self.store
            .bounded_vector_entry_count(index, snapshot, vector_copy_owner)
    }

    fn completed_vector_search_disclosure(
        &self,
        index: VectorIndexRef,
        k: usize,
        restricted_candidates: bool,
        candidates: Option<&PhysicalPlan>,
        search_mode: Option<contextdb_parser::ast::VectorSearchMode>,
        query_expr: &Expr,
        snapshot: SnapshotId,
        params: &HashMap<String, Value>,
        aggregate_allowed_vectors: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        partition_routes: &[contextdb_vector::VectorPartitionSearchTrace],
        used_hnsw: bool,
        fallback_reason: Option<&'static str>,
        budget_receipt: Option<contextdb_vector::mem::VectorSearchBudgetReceipt>,

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<VectorSearchDisclosure> {
        self.store.completed_vector_search_disclosure(
            index,
            k,
            restricted_candidates,
            candidates,
            search_mode,
            query_expr,
            snapshot,
            params,
            aggregate_allowed_vectors,
            groups,
            partition_routes,
            used_hnsw,
            fallback_reason,
            budget_receipt,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn publish_vector_search_trace(&self, trace: contextdb_vector::OwnedVectorSearchTrace) {
        self.store.publish_vector_search_trace(trace);
    }

    fn with_bounded_row_vector(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        before_access_row: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        use_vector: &mut dyn FnMut(
            &[f32],
        ) -> std::result::Result<
            BoundedRowVectorClone,
            BoundedExecutionError,
        >,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<BoundedRowVectorClone>, BoundedExecutionError> {
        self.store.with_bounded_row_vector(
            index,
            row_id,
            snapshot,
            tx,
            before_access_row,
            use_vector,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_brute_force_vector_cursor(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: contextdb_vector::mem::BoundedVectorCandidateGroups,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedBruteForceCursor, BoundedExecutionError>
    {
        self.store.bounded_brute_force_vector_cursor(
            index,
            query,
            k,
            groups,
            before_source_entry,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_brute_force_vector_step(
        &self,
        cursor: &mut contextdb_vector::mem::BoundedBruteForceCursor,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_precharged_candidate: &mut dyn FnMut() -> std::result::Result<
            (),
            BoundedExecutionError,
        >,
        before_exhaustive_candidate: &mut dyn FnMut() -> std::result::Result<
            (),
            BoundedExecutionError,
        >,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<contextdb_vector::mem::BoundedVectorStep, BoundedExecutionError> {
        self.store.bounded_brute_force_vector_step(
            cursor,
            before_source_entry,
            before_precharged_candidate,
            before_exhaustive_candidate,
            before_distance,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn bounded_hnsw_vector_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        mode: VectorSearchMode,
        is_reader_memory_refusal: &dyn Fn(&BoundedExecutionError) -> bool,
        allowed_search_limits: contextdb_vector::hnsw::HnswAllowedSearchLimits,
        before_source_entry: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_exact_candidate: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        load_transfer: crate::memory_accounting::VectorLoadReservationTransfer,
    ) -> std::result::Result<BoundedVectorSearchOutcome, BoundedExecutionError> {
        self.store.bounded_hnsw_vector_search(
            index,
            query,
            k,
            groups,
            mode,
            is_reader_memory_refusal,
            allowed_search_limits,
            before_source_entry,
            before_exact_candidate,
            before_distance,
            before_retain,
            release_retained,
            vector_copy_owner,
            read_memory.clone(),
            load_transfer,
        )
    }

    fn bounded_rank_candidate(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        index: &VectorIndexRef,
        sort_key: &str,
        anchor_row_id: RowId,
        anchor_values: &HashMap<String, Value>,
        vector_score: f32,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        before_candidate: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_joined_clone: &mut dyn FnMut(
            usize,
        )
            -> std::result::Result<(), BoundedExecutionError>,
        release_joined_clone: &mut dyn FnMut(
            usize,
        )
            -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<(Option<f32>, usize), BoundedExecutionError> {
        self.store.bounded_rank_candidate(
            read_memory,
            index,
            sort_key,
            anchor_row_id,
            anchor_values,
            vector_score,
            snapshot,
            tx,
            before_candidate,
            before_access,
            before_joined_clone,
            release_joined_clone,
        )
    }

    fn bounded_graph_edge_cursor(
        &self,
        direction: contextdb_core::Direction,
        snapshot: SnapshotId,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
    ) -> std::result::Result<crate::database::gate::BoundedGraphEdgeCursor, BoundedExecutionError>
    {
        self.store
            .bounded_graph_edge_cursor(direction, snapshot, before_retain, release_retained)
    }

    fn bounded_graph_edge_next(
        &self,
        cursor: &mut crate::database::gate::BoundedGraphEdgeCursor,
        edge_types: Option<&[EdgeType]>,
        tx: Option<TxId>,
        before_edge: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        entry_read: &mut dyn FnMut(),

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        Option<crate::database::gate::CountedGraphScanEdge>,
        BoundedExecutionError,
    > {
        self.store.bounded_graph_edge_next(
            cursor,
            edge_types,
            tx,
            before_edge,
            before_access,
            before_retain,
            release_retained,
            entry_read,
            read_memory.clone(),
        )
    }

    fn bounded_graph_bfs_cursor(
        &self,
        tx: Option<TxId>,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: contextdb_core::Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Option<contextdb_graph::mem::BoundedBfsCursor>, BoundedExecutionError>
    {
        self.store.bounded_graph_bfs_cursor(
            tx,
            start,
            edge_types,
            direction,
            min_depth,
            max_depth,
            snapshot,
            before_access,
            before_retain,
            release_retained,
            read_memory.clone(),
        )
    }

    fn bounded_graph_bfs_next(
        &self,
        cursor: &mut contextdb_graph::mem::BoundedBfsCursor,
        before_edge: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_path_element: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        before_access: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        release_retained: &mut dyn FnMut(usize),
        staged_touch: &mut dyn FnMut() -> std::result::Result<(), BoundedExecutionError>,
        staged_retain: &mut dyn FnMut(usize) -> std::result::Result<(), BoundedExecutionError>,
        staged_hand_back: &mut dyn FnMut(usize),
        entry_read: &mut dyn FnMut(),
        visited_cap: Option<usize>,
        tx: Option<TxId>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<
        Option<contextdb_graph::mem::BoundedTraversalNode>,
        BoundedExecutionError,
    > {
        self.store.bounded_graph_bfs_next(
            cursor,
            before_edge,
            before_path_element,
            before_access,
            before_retain,
            release_retained,
            staged_touch,
            staged_retain,
            staged_hand_back,
            entry_read,
            visited_cap,
            tx,
            read_memory.clone(),
        )
    }

    fn resolve_graph_filter_at_snapshot(
        &self,
        expr: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        ctes: &[Cte],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Expr> {
        self.store.resolve_graph_filter_at_snapshot(
            expr,
            params,
            tx,
            snapshot,
            ctes,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn as_database_for_subquery_plan(&self) -> Option<&Database> {
        self.store.as_database_for_subquery_plan()
    }

    fn resolve_graph_start_nodes_from_filter(
        &self,
        filter: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        start_alias: &str,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<GraphStartResolution> {
        self.store.resolve_graph_start_nodes_from_filter(
            filter,
            params,
            tx,
            snapshot,
            start_alias,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    fn graph_filter_matches_bindings_with_work(
        &self,
        filter: &Expr,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        snapshot: SnapshotId,
        bindings: &HashMap<String, uuid::Uuid>,
        work: &mut dyn FnMut(GraphPredicateWork) -> Result<()>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        self.store.graph_filter_matches_bindings_with_work(
            filter,
            params,
            tx,
            snapshot,
            bindings,
            work,
            read_memory.clone(),
        )
    }

    fn coerce_into_column(
        &self,
        table: &str,
        col: &str,
        value: Value,
        current_tx_max: Option<TxId>,
        active_tx: Option<TxId>,
    ) -> Result<Value> {
        self.store
            .coerce_into_column(table, col, value, current_tx_max, active_tx)
    }

    fn coerce_pick_shape_to_column_type(&self, table: &str, pick: &IndexPick) -> Result<IndexPick> {
        self.store.coerce_pick_shape_to_column_type(table, pick)
    }

    fn read_query(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        (OwnedQueryResult, Vec<u8>, ReadPublicationScope),
        crate::direct_file_reader::DirectFileReaderError,
    > {
        self.store
            .read_query(sql, params, read_memory, limits, clock, cancellation)
    }

    fn open_read_cursor(
        self: Arc<Self>,
        sql: &str,
        params: &HashMap<String, Value>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: &OwnerReadCancellation,
        snapshot: crate::direct_file_reader::DirectSnapshot,
    ) -> std::result::Result<
        (
            Arc<dyn ReadExecutionTarget>,
            crate::direct_file_reader::DirectCursorPage,
            Option<crate::direct_file_reader::DirectCursorPage>,
            ReadPublicationScope,
        ),
        crate::direct_file_reader::DirectFileReaderError,
    > {
        Arc::clone(&self.store).open_read_cursor(
            sql,
            params,
            read_memory,
            limits,
            clock,
            cancellation,
            snapshot,
        )
    }

    fn read_cursor_pages(
        &self,
        rows: Option<NonZeroUsize>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        (
            crate::direct_file_reader::DirectCursorPage,
            Option<crate::direct_file_reader::DirectCursorPage>,
            ReadPublicationScope,
        ),
        crate::direct_file_reader::DirectFileReaderError,
    > {
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_cancellation(
            crate::read_probe::CANCELLATION_CURSOR_FETCH,
            cancellation,
        );
        let fetched = self.with_cursor(|cursor| {
            fetch_bounded_cursor(
                cursor,
                rows,
                cancellation.clone(),
                #[cfg(feature = "test-seams")]
                crate::read_session::session_kernel_probe_for_test(),
            )
        });
        let fetched = match fetched {
            Ok(fetched) => fetched,
            Err(crate::direct_file_reader::DirectFileReaderError::Cancelled) => {
                return Err(crate::direct_file_reader::DirectFileReaderError::Cancelled);
            }
            // A refusal the kernel answered without disturbing the suspended
            // read leaves the cursor open, so there is nothing to give back and
            // the caller keeps the escape the refusal just named.
            Err(error) if self.read_cursor_retains_continuation() => {
                return Err(error);
            }
            Err(error) => {
                // A refused read is over: it returns its bytes here rather
                // than waiting for a caller to remember to close it.
                #[cfg(feature = "test-seams")]
                crate::read_probe::note_cursor_refusal();
                self.release()?;
                return Err(error);
            }
        };
        let constructed = (|| {
            let published = published_page(
                fetched.page,
                fetched.observation,
                self.snapshot,
                self.read_memory.clone(),
            )?;
            let drained = drained_page(&published, self.read_memory.clone())?;
            Ok((published, drained))
        })();
        let (published, drained) = match constructed {
            Ok(pages) => pages,
            Err(error) => {
                self.release()?;
                return Err(error);
            }
        };
        if drained.is_some() {
            self.release()?;
        }
        Ok((published, drained, fetched.publication))
    }

    fn read_cursor_observation(
        &self,
    ) -> std::result::Result<
        Option<CursorObservation>,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        let observation = self.with_cursor(bounded::cursor_observation);
        if observation.is_err() || matches!(&observation, Ok(None)) {
            self.release()?;
        }
        observation
    }

    fn read_cursor_retains_continuation(&self) -> bool {
        let cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        bounded::cursor_retains_continuation(&cursor)
    }

    fn close_read_cursor(
        &self,
    ) -> std::result::Result<(), crate::direct_file_reader::DirectFileReaderError> {
        self.release()
    }

    fn read_metadata(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        request: crate::direct_file_reader::DirectMetadataRequest,
        image: &crate::direct_file_reader::DirectOwnedImage,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        continuation: Option<&str>,
        cancellation: &OwnerReadCancellation,
    ) -> std::result::Result<
        crate::direct_file_reader::DirectMetadataResponse,
        crate::direct_file_reader::DirectFileReaderError,
    > {
        self.store.read_metadata(
            read_memory,
            request,
            image,
            limits,
            clock,
            continuation,
            cancellation,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_planner::GraphStepPlan;
    use uuid::Uuid;

    #[test]
    fn eager_and_pull_vector_routes_share_restricted_refusal_and_recovery() {
        use crate::database::{VectorSearchResidual, VectorSearchRoute, VectorSearchRouteEvidence};
        use contextdb_core::{ContextId, VectorSearchMode};
        use std::collections::BTreeSet;
        let db = Database::open_memory();
        db.set_maintenance_policy(crate::database::MaintenancePolicy::CallerDriven);
        db.execute("CREATE TABLE readable_vectors (id INT PRIMARY KEY, context UUID CONTEXT_ID, embedding VECTOR(2) AUTO_INDEX_AT 1)", &HashMap::new()).unwrap();
        for (id, context, vector) in [(1, 1, vec![0.8, 0.6]), (2, 2, vec![1.0, 0.0])] {
            db.execute(
                "INSERT INTO readable_vectors VALUES ($id,$context,$vector)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("context".into(), Value::Uuid(Uuid::from_u128(context))),
                    ("vector".into(), Value::Vector(vector)),
                ]),
            )
            .unwrap();
        }
        for _ in 0..8 {
            db.run_maintenance_cycle().unwrap();
        }
        let scoped = db.scoped_with_contexts(BTreeSet::from([ContextId::new(Uuid::from_u128(1))]));
        let sql = "SELECT id FROM readable_vectors ORDER BY embedding <=> [1.0,0.0] USE VECTOR INDEXED LIMIT 1";
        let passive = |sql: &str| {
            let memory = db.accountant().usage().used;
            let scans = db.__relational_scan_rows_touched();
            let activity = db.__vector_passive_activity_counters_for_test();
            let explained = scoped.explain_output(sql).unwrap();
            assert_eq!(db.accountant().usage().used, memory);
            assert_eq!(db.__relational_scan_rows_touched(), scans);
            assert_eq!(db.__vector_passive_activity_counters_for_test(), activity);
            let rendered = crate::cli_render::render_explain_output(&explained);
            for context in [1, 2] {
                assert!(!rendered.contains(&Uuid::from_u128(context).to_string()));
            }
            let disclosure = explained.vector_search.unwrap();
            assert_eq!(disclosure.aggregate_allowed_vectors, Some(1));
            assert!(disclosure.partition_hnsw.is_empty());
            disclosure
        };
        let refused = passive(sql);
        assert_eq!(refused.route, Some(VectorSearchRoute::FilteredIndexed));
        assert_eq!(refused.residual, VectorSearchResidual::Unsupported);
        assert_eq!(
            refused.refusal.as_deref(),
            Some("filtered_route_unavailable")
        );
        assert_eq!(
            refused.recovery.as_deref(),
            Some("CREATE INDEX readable_vectors_context_idx ON readable_vectors (context)")
        );
        let plan = contextdb_planner::plan(&contextdb_parser::parse(sql).unwrap()).unwrap();
        let executor_fixture_reader_1 = scoped
            .declared_reader_memory()
            .expect("admit the fixture executor reader");
        let mut executor_fixture_copy_1 =
            crate::vector_read_memory::admission(executor_fixture_reader_1.clone());

        let eager = execute_plan(
            &scoped,
            &plan,
            &HashMap::new(),
            None,
            &mut executor_fixture_copy_1,
            executor_fixture_reader_1,
        )
        .unwrap_err();
        let pull = scoped.execute(sql, &HashMap::new()).unwrap_err();
        assert!(matches!(
            eager,
            Error::VectorFilteredRouteUnavailable { .. }
        ));
        assert_eq!(eager.to_string(), pull.to_string());
        db.execute(
            "CREATE INDEX readable_context ON readable_vectors(context)",
            &HashMap::new(),
        )
        .unwrap();
        let executor_fixture_reader_2 = scoped
            .declared_reader_memory()
            .expect("admit the fixture executor reader");
        let mut executor_fixture_copy_2 =
            crate::vector_read_memory::admission(executor_fixture_reader_2.clone());

        let eager = execute_plan(
            &scoped,
            &plan,
            &HashMap::new(),
            None,
            &mut executor_fixture_copy_2,
            executor_fixture_reader_2,
        )
        .unwrap();
        let pull = scoped.execute(sql, &HashMap::new()).unwrap();
        assert_eq!(eager.rows, pull.rows);
        assert_eq!(pull.rows, vec![vec![Value::Int64(1)]]);
        assert!(scoped.__debug_last_query_vector_used_hnsw_for_test());
        let ready = passive(sql);
        assert_eq!(ready.route, Some(VectorSearchRoute::FilteredIndexed));
        assert_eq!(ready.residual, VectorSearchResidual::Bounded);
        assert_eq!(ready.refusal, None);
        assert_eq!(ready.recovery, None);
        let executed_ready = {
            let mut disclosure = ready.clone();
            disclosure.route_evidence = VectorSearchRouteEvidence::Executed;
            let actual = eager.trace.vector_search.as_ref().unwrap();
            assert!(actual.global_candidate_budget.is_some());
            assert_eq!(actual.budget_terminal.as_deref(), Some("answer_complete"));
            disclosure.global_candidate_budget = actual.global_candidate_budget;
            disclosure.admission_evaluations = actual.admission_evaluations;
            disclosure.expansion_evaluations = actual.expansion_evaluations;
            disclosure.layers_admitted = actual.layers_admitted;
            disclosure.budget_terminal = actual.budget_terminal.clone();
            disclosure
        };
        for result in [&eager, &pull] {
            let disclosure = result.trace.vector_search.as_ref().unwrap();
            assert_eq!(
                disclosure.route_evidence,
                VectorSearchRouteEvidence::Executed
            );
            assert_eq!(disclosure, &executed_ready);
        }
        let auto = sql.replace("INDEXED", "AUTO");
        let auto_ready = passive(&auto);
        assert_eq!(auto_ready.refusal, None);
        assert_eq!(auto_ready.recovery, None);
        let auto_plan = contextdb_planner::plan(&contextdb_parser::parse(&auto).unwrap()).unwrap();
        let executor_fixture_reader_3 = scoped
            .declared_reader_memory()
            .expect("admit the fixture executor reader");
        let mut executor_fixture_copy_3 =
            crate::vector_read_memory::admission(executor_fixture_reader_3.clone());

        let auto_eager = execute_plan(
            &scoped,
            &auto_plan,
            &HashMap::new(),
            None,
            &mut executor_fixture_copy_3,
            executor_fixture_reader_3,
        )
        .unwrap();
        let auto_pull = scoped.execute(&auto, &HashMap::new()).unwrap();
        assert_eq!(auto_eager.rows, pull.rows);
        assert_eq!(auto_eager.rows, auto_pull.rows);
        let auto_executed = auto_eager.trace.vector_search.as_ref().unwrap();
        assert_eq!(
            auto_executed.route_evidence,
            VectorSearchRouteEvidence::Executed
        );
        assert_eq!(auto_executed.refusal, auto_ready.refusal);
        assert_eq!(auto_executed.recovery, auto_ready.recovery);
        assert_eq!(auto_pull.trace.vector_search.as_ref(), Some(auto_executed));

        db.execute(
            "DROP INDEX readable_context ON readable_vectors",
            &HashMap::new(),
        )
        .unwrap();
        let auto = sql.replace("INDEXED", "AUTO");
        let plan = contextdb_planner::plan(&contextdb_parser::parse(&auto).unwrap()).unwrap();
        let auto_exact = passive(&auto);
        assert_eq!(auto_exact.resolved_mode, VectorSearchMode::Exact);
        assert_eq!(auto_exact.route, Some(VectorSearchRoute::Exact));
        assert_eq!(auto_exact.fallback.as_deref(), Some("filtered_route_exact"));
        assert_eq!(passive(sql).refusal, refused.refusal);
        let executor_fixture_reader_4 = scoped
            .declared_reader_memory()
            .expect("admit the fixture executor reader");
        let mut executor_fixture_copy_4 =
            crate::vector_read_memory::admission(executor_fixture_reader_4.clone());

        let eager = execute_plan(
            &scoped,
            &plan,
            &HashMap::new(),
            None,
            &mut executor_fixture_copy_4,
            executor_fixture_reader_4,
        )
        .unwrap();
        assert!(!scoped.__debug_last_query_vector_used_hnsw_for_test());
        let pull = scoped.execute(&auto, &HashMap::new()).unwrap();
        assert_eq!(eager.rows, pull.rows);
        let eager_disc = eager.trace.vector_search.as_ref().unwrap();
        assert_eq!(
            eager_disc.route_evidence,
            VectorSearchRouteEvidence::Executed
        );
        assert_eq!(eager_disc.refusal, auto_exact.refusal);
        assert_eq!(eager_disc.recovery, auto_exact.recovery);
        assert_eq!(pull.trace.vector_search.as_ref(), Some(eager_disc));
    }

    #[test]
    fn graph_01_frontier_projection_requires_complete_bindings() {
        let steps = vec![GraphStepPlan {
            edge_types: vec!["EDGE".to_string()],
            direction: Direction::Outgoing,
            min_depth: 1,
            max_depth: 1,
            target_alias: "b".to_string(),
        }];

        let missing_start = vec![(HashMap::new(), Uuid::new_v4(), 0)];
        let missing_target = vec![(
            HashMap::from([("a".to_string(), Uuid::new_v4())]),
            Uuid::new_v4(),
            0,
        )];

        let start_result = project_graph_frontier_rows(missing_start, "a", &steps);
        assert!(
            matches!(start_result, Err(Error::PlanError(_))),
            "graph frontier projection should return a plan error on missing start alias binding, got {start_result:?}"
        );

        let target_result = project_graph_frontier_rows(missing_target, "a", &steps);
        assert!(
            matches!(target_result, Err(Error::PlanError(_))),
            "graph frontier projection should return a plan error on missing target alias binding, got {target_result:?}"
        );
    }

    #[test]
    fn graph_02_filter_subqueries_resolve_at_graph_snapshot_without_outer_override() {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE seeds (id UUID PRIMARY KEY, node_id UUID)",
            &HashMap::new(),
        )
        .unwrap();

        let pre_pin_seed = Uuid::from_u128(1);
        let post_pin_seed = Uuid::from_u128(2);
        db.execute(
            "INSERT INTO seeds (id, node_id) VALUES ($id, $node_id)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::from_u128(10))),
                ("node_id".to_string(), Value::Uuid(pre_pin_seed)),
            ]),
        )
        .unwrap();
        let snapshot = db.snapshot();
        db.execute(
            "INSERT INTO seeds (id, node_id) VALUES ($id, $node_id)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::from_u128(11))),
                ("node_id".to_string(), Value::Uuid(post_pin_seed)),
            ]),
        )
        .unwrap();

        let stmt =
            contextdb_parser::parse("SELECT id FROM nodes WHERE id IN (SELECT node_id FROM seeds)")
                .unwrap();
        let Statement::Select(select) = stmt else {
            panic!("expected SELECT");
        };
        let filter = select.body.where_clause.expect("expected WHERE filter");
        let executor_fixture_reader_5 = db
            .declared_reader_memory()
            .expect("admit the fixture executor reader");
        let mut executor_fixture_copy_5 =
            crate::vector_read_memory::admission(executor_fixture_reader_5.clone());

        let resolved = resolve_graph_filter_at_snapshot(
            &db,
            &filter,
            &HashMap::new(),
            None,
            snapshot,
            &[],
            &mut executor_fixture_copy_5,
            executor_fixture_reader_5,
        )
        .unwrap();
        let Expr::InList { list, .. } = resolved else {
            panic!("expected resolved IN list");
        };
        let resolved_ids = list
            .into_iter()
            .map(|expr| match expr {
                Expr::Literal(Literal::Text(value)) => Uuid::parse_str(&value).unwrap(),
                other => panic!("expected UUID text literal, got {other:?}"),
            })
            .collect::<BTreeSet<_>>();

        assert_eq!(resolved_ids, BTreeSet::from([pre_pin_seed]));
        assert!(
            !resolved_ids.contains(&post_pin_seed),
            "post-pin seed leaked into graph filter subquery resolution"
        );
    }

    #[test]
    fn default_01_writer_and_evaluator_agree_on_integer_shape() {
        let stored = stored_default_expr(&Expr::Literal(Literal::Integer(5)));
        assert_eq!(
            stored, "5",
            "writer must keep serializing INTEGER defaults as a bare number"
        );
        let value = evaluate_stored_default_expr(&stored)
            .expect("evaluator must read back exactly what the writer stores");
        assert_eq!(value, Value::Int64(5));
    }

    #[test]
    fn default_02_writer_and_evaluator_agree_on_real_shape() {
        let stored = stored_default_expr(&Expr::Literal(Literal::Real(3.5)));
        assert_eq!(
            stored, "3.5",
            "writer must keep serializing REAL defaults as a bare number"
        );
        let value = evaluate_stored_default_expr(&stored)
            .expect("evaluator must read back exactly what the writer stores");
        assert_eq!(value, Value::Float64(3.5));
    }

    #[test]
    fn default_02b_whole_number_real_default_widens_to_float_for_the_column() {
        // `REAL DEFAULT 0.0` (docs/query-language.md's own canonical `tasks`
        // example) round-trips through the writer as the bare string "0" --
        // indistinguishable at the string level from an INTEGER default of 0.
        // evaluate_stored_default_expr alone can only return the natural
        // numeric parse (Int64); it takes the column's declared type, applied
        // by the caller that fills defaults, to land on Float64. That full
        // path is covered end to end by sql_01c/sql_01d in
        // sql_surface_tests.rs; here we just pin the raw evaluator's honest
        // ambiguous-shape behavior so a future edit cannot silently start
        // guessing REAL instead.
        let stored = stored_default_expr(&Expr::Literal(Literal::Real(0.0)));
        assert_eq!(stored, "0");
        let value = evaluate_stored_default_expr(&stored)
            .expect("evaluator must read back exactly what the writer stores");
        assert_eq!(value, Value::Int64(0));
    }

    #[test]
    fn default_03_evaluator_also_tolerates_the_legacy_wrapped_shape() {
        // The evaluator accepts wrapped Literal(Integer(..)) and Literal(Real(..))
        // inputs as well as the bare numbers emitted by the stored-default encoder.
        let int_value = evaluate_stored_default_expr("Literal(Integer(5))")
            .expect("wrapped integer shape must still be readable");
        assert_eq!(int_value, Value::Int64(5));

        let real_value = evaluate_stored_default_expr("Literal(Real(0.5))")
            .expect("wrapped real shape must still be readable");
        assert_eq!(real_value, Value::Float64(0.5));
    }
}

#[cfg(test)]
mod composite_resume_range_tests {
    use super::*;

    #[test]
    fn composite_resume_ranges_preserve_directions_and_exclusive_duplicates() {
        let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
        for ordinal_direction in ["ASC", "DESC"] {
            for id_direction in ["ASC", "DESC"] {
                let db = Database::open_memory();
                db.execute("CREATE TABLE resume_items (id INTEGER PRIMARY KEY, context INTEGER, stream INTEGER, ordinal INTEGER)", &HashMap::new()).unwrap();
                db.execute(&format!("CREATE INDEX resume_position ON resume_items (context, stream, ordinal {ordinal_direction}, id {id_direction})"), &HashMap::new()).unwrap();
                let tx = db.begin_or_panic();
                for (id, stream, ordinal) in [
                    (1, 6, 1),
                    (2, 7, 1),
                    (3, 7, 2),
                    (4, 7, 2),
                    (5, 7, 3),
                    (6, 7, 4),
                    (7, 8, 3),
                ] {
                    db.execute_in_tx(
                        tx,
                        &format!("INSERT INTO resume_items VALUES ({id}, 9, {stream}, {ordinal})"),
                        &HashMap::new(),
                    )
                    .unwrap();
                }
                db.execute_in_tx(
                    tx,
                    "INSERT INTO resume_items VALUES (8, 9, 7, NULL)",
                    &HashMap::new(),
                )
                .unwrap();
                db.execute_in_tx(
                    tx,
                    "INSERT INTO resume_items VALUES (9, 8, 7, 3)",
                    &HashMap::new(),
                )
                .unwrap();
                db.execute_in_tx(
                    tx,
                    "INSERT INTO resume_items VALUES (10, 10, 7, 3)",
                    &HashMap::new(),
                )
                .unwrap();
                db.commit(tx).unwrap();
                for (predicate, ascending) in [
                    ("ordinal > 2", vec![5, 6]),
                    ("ordinal < 3", vec![2, 3]),
                    ("ordinal > 2 AND ordinal < 4", vec![5]),
                    ("ordinal >= 2 AND ordinal <= 3", vec![3, 4]),
                    ("ordinal > 3 AND ordinal < 3", vec![]),
                ] {
                    for direction in ["ASC", "DESC"] {
                        let answer = db.execute(&format!("SELECT id FROM resume_items WHERE context = 9 AND stream = 7 AND {predicate} ORDER BY ordinal {direction}, id {direction} LIMIT 2"), &HashMap::new()).unwrap();
                        let expected = match (predicate, direction) {
                            ("ordinal > 2", "DESC") => vec![6, 5],
                            ("ordinal < 3", "DESC") => vec![4, 3],
                            ("ordinal >= 2 AND ordinal <= 3", "DESC") => vec![5, 4],
                            _ => ascending.clone(),
                        };
                        assert_eq!(
                            answer.rows,
                            expected
                                .into_iter()
                                .map(|id| vec![Value::Int64(id)])
                                .collect::<Vec<_>>(),
                            "{ordinal_direction}/{id_direction}/{predicate}/{direction}"
                        );
                        // Only matching entries contribute, including when the
                        // seek skips duplicate ordinals at an exclusive edge.
                        if ordinal_direction == id_direction {
                            assert_eq!(answer.trace.rows_examined, answer.rows.len() as u64);
                        }
                        // Mixed physical directions may need the ordinary SQL
                        // sort; agreeing directions must walk the index directly.
                        if ordinal_direction == id_direction {
                            assert!(answer.trace.sort_elided);
                        }
                    }
                }
            }
        }
    }
}
#[cfg(all(test, feature = "test-seams"))]
#[path = "executor/writecost_missing_index_tests.rs"]
mod writecost_missing_index_tests;

