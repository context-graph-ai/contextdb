//! Test-feature-only adapter for bounded-read production paths.
//!
//! This module owns no execution behavior.  It is a thin conversion layer to
//! the crate-private bounded-kernel entrance in the parent module.

pub use super::bounded::{ContinuationBytes, PENDING_INDEX_RUNS};
use super::{
    BoundedCursorHandle, BoundedExecutionError, BoundedExecutionProbe, BoundedExecutionTelemetry,
    BoundedSourceTouch, BoundedWorkSource, close_bounded_cursor, execute_bounded,
    fetch_bounded_cursor, open_bounded_cursor,
};
use crate::{Database, QueryResult};
use contextdb_core::Value;
use contextdb_core::read_contract::{
    CursorPage, DeadlineClock, OwnerReadCancellation, ReadFailure, ReadLimits,
};
use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// The test-side request maps one-for-one to the bounded-kernel context.
pub struct BoundedReadRequest {
    pub sql: String,
    pub params: HashMap<String, Value>,
    pub limits: ReadLimits,
    pub clock: Arc<dyn DeadlineClock>,
    pub cancellation: OwnerReadCancellation,
    pub probe: Option<Arc<dyn ExecutionProbe>>,
}

impl BoundedReadRequest {
    pub fn new(
        sql: impl Into<String>,
        params: HashMap<String, Value>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
    ) -> Self {
        Self {
            sql: sql.into(),
            params,
            limits,
            clock,
            cancellation: OwnerReadCancellation::new(),
            probe: None,
        }
    }
}

/// Hook used by deterministic production-path tests.  Implementations advance
/// the injected clock or cancel the supplied token without a blocking wait.
pub trait ExecutionProbe: Send + Sync {
    fn before_work(&self, source: TestWorkSource, completed_work: u64);
    fn before_source_touch(&self, _touch: TestSourceTouch, _completed_items: u64) {}
    fn before_temporary_reservation(
        &self,
        _source: TestWorkSource,
        _requested_bytes: u64,
        _held_temporary_bytes: u64,
    ) {
    }
    fn after_temporary_reservation(
        &self,
        _source: TestWorkSource,
        _reserved_bytes: u64,
        _held_temporary_bytes: u64,
    ) {
    }
    fn cancellation_observed(&self, completed_work: u64);
}

struct ProbeAdapter(Arc<dyn ExecutionProbe>);

impl BoundedExecutionProbe for ProbeAdapter {
    fn before_work(&self, source: BoundedWorkSource, completed_work: u64) {
        self.0.before_work(source.into(), completed_work);
    }

    fn before_source_touch(&self, touch: BoundedSourceTouch, completed_items: u64) {
        assert_ne!(
            touch,
            BoundedSourceTouch::HnswCandidate,
            "HNSW touches must come from the sealed vector distance-loop handoff"
        );
        self.0.before_source_touch(touch.into(), completed_items);
    }

    fn before_hnsw_candidate_distance(
        &self,
        event: contextdb_vector::hnsw::HnswCandidateDistanceEvent,
    ) {
        self.0
            .before_source_touch(TestSourceTouch::HnswCandidate, event.completed_candidates());
    }

    fn before_temporary_reservation(
        &self,
        source: BoundedWorkSource,
        requested_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        self.0
            .before_temporary_reservation(source.into(), requested_bytes, held_temporary_bytes);
    }

    fn after_temporary_reservation(
        &self,
        source: BoundedWorkSource,
        reserved_bytes: u64,
        held_temporary_bytes: u64,
    ) {
        self.0
            .after_temporary_reservation(source.into(), reserved_bytes, held_temporary_bytes);
    }

    fn cancellation_observed(&self, completed_work: u64) {
        self.0.cancellation_observed(completed_work);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TestWorkSource {
    TableScan,
    IndexRange,
    UnindexedSort,
    GraphTraversal,
    VectorCandidates,
    RankCandidates,
    AccessControl,
}

impl From<BoundedWorkSource> for TestWorkSource {
    fn from(value: BoundedWorkSource) -> Self {
        match value {
            BoundedWorkSource::TableScan => Self::TableScan,
            BoundedWorkSource::IndexRange => Self::IndexRange,
            BoundedWorkSource::UnindexedSort => Self::UnindexedSort,
            BoundedWorkSource::GraphTraversal => Self::GraphTraversal,
            BoundedWorkSource::VectorCandidates => Self::VectorCandidates,
            BoundedWorkSource::RankCandidates => Self::RankCandidates,
            BoundedWorkSource::AccessControl => Self::AccessControl,
        }
    }
}

/// The actual source-loop item reached by the bounded pull kernel.  Tests use
/// these values as poison points, rather than accepting counters supplied by
/// the kernel after an eager collection has already happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TestSourceTouch {
    TableRow,
    IndexEntry,
    SortCandidate,
    GraphEdge,
    AdjacencyEntry,
    BruteForceVectorCandidate,
    HnswCandidate,
    RankCandidate,
    AccessRow,
}

impl From<BoundedSourceTouch> for TestSourceTouch {
    fn from(value: BoundedSourceTouch) -> Self {
        match value {
            BoundedSourceTouch::TableRow => Self::TableRow,
            BoundedSourceTouch::IndexEntry => Self::IndexEntry,
            BoundedSourceTouch::SortCandidate => Self::SortCandidate,
            BoundedSourceTouch::GraphEdge => Self::GraphEdge,
            BoundedSourceTouch::AdjacencyEntry => Self::AdjacencyEntry,
            BoundedSourceTouch::BruteForceVectorCandidate => Self::BruteForceVectorCandidate,
            BoundedSourceTouch::HnswCandidate => Self::HnswCandidate,
            BoundedSourceTouch::RankCandidate => Self::RankCandidate,
            BoundedSourceTouch::AccessRow => Self::AccessRow,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TestTelemetry {
    pub work_units: u64,
    pub source_work: BTreeMap<TestWorkSource, u64>,
    pub source_peak_temporary_bytes: BTreeMap<TestWorkSource, u64>,
    pub encoded_bytes: u64,
    pub peak_temporary_bytes: u64,
    pub cancellation_observed: bool,
}

impl From<BoundedExecutionTelemetry> for TestTelemetry {
    fn from(value: BoundedExecutionTelemetry) -> Self {
        Self {
            work_units: value.work_units,
            source_work: value
                .source_work
                .into_iter()
                .map(|(source, count)| (source.into(), count))
                .collect(),
            source_peak_temporary_bytes: value
                .source_peak_temporary_bytes
                .into_iter()
                .map(|(source, bytes)| (source.into(), bytes))
                .collect(),
            encoded_bytes: value.encoded_bytes,
            peak_temporary_bytes: value.peak_temporary_bytes,
            cancellation_observed: value.cancellation_observed,
        }
    }
}

#[derive(Debug)]
pub struct TestResult {
    pub result: QueryResult,
    pub telemetry: TestTelemetry,
}

pub struct TestCursorOpen {
    pub cursor: TestCursor,
    pub first_page: CursorPage,
    pub telemetry: TestTelemetry,
}

#[derive(Debug)]
pub struct TestCursorFetch {
    pub page: CursorPage,
    pub telemetry: TestTelemetry,
}

pub struct TestCursor {
    handle: BoundedCursorHandle,
    probe: Option<Arc<dyn ExecutionProbe>>,
}

#[derive(Debug)]
pub enum TestError {
    Unimplemented,
    Refused(ReadFailure),
    Cancelled,
    Engine(String),
}

fn test_error(error: BoundedExecutionError) -> TestError {
    match error {
        BoundedExecutionError::Unimplemented => TestError::Unimplemented,
        BoundedExecutionError::Refused(refusal) => TestError::Refused(refusal),
        BoundedExecutionError::Cancelled => TestError::Cancelled,
        BoundedExecutionError::Engine(error) => TestError::Engine(error.to_string()),
    }
}

/// Present a session-level observer to the bounded kernel as the kernel's own
/// probe. Route code cannot construct a kernel callback any other way, so an
/// event a session reports about a source item is one the kernel really made.
pub(crate) fn kernel_probe(probe: Arc<dyn ExecutionProbe>) -> Arc<dyn BoundedExecutionProbe> {
    Arc::new(ProbeAdapter(probe))
}

fn adapted_probe(request: &BoundedReadRequest) -> Option<Arc<dyn BoundedExecutionProbe>> {
    request
        .probe
        .as_ref()
        .map(|probe| Arc::new(ProbeAdapter(Arc::clone(probe))) as Arc<dyn BoundedExecutionProbe>)
}

/// Run one complete bounded ordinary read through the production kernel seam.
pub fn execute(db: &Database, request: &BoundedReadRequest) -> Result<TestResult, TestError> {
    execute_bounded(
        db,
        &request.sql,
        &request.params,
        request.limits,
        Arc::clone(&request.clock),
        request.cancellation.clone(),
        adapted_probe(request),
    )
    .map(|outcome| TestResult {
        result: outcome.result,
        telemetry: outcome.telemetry.into(),
    })
    .map_err(test_error)
}

/// Open a cursor and receive its required first page.
pub fn open_cursor(
    db: Arc<Database>,
    request: &BoundedReadRequest,
) -> Result<TestCursorOpen, TestError> {
    open_bounded_cursor(
        db,
        &request.sql,
        &request.params,
        request.limits,
        Arc::clone(&request.clock),
        request.cancellation.clone(),
        adapted_probe(request),
    )
    .map(|outcome| TestCursorOpen {
        cursor: TestCursor {
            handle: outcome.cursor,
            probe: request.probe.as_ref().map(Arc::clone),
        },
        first_page: outcome.first_page,
        telemetry: outcome.telemetry.into(),
    })
    .map_err(test_error)
}

impl TestCursor {
    /// Fetch a bounded page without rerunning the SQL statement.
    pub fn fetch(
        &mut self,
        rows: Option<NonZeroUsize>,
        cancellation: OwnerReadCancellation,
    ) -> Result<TestCursorFetch, TestError> {
        let probe = self.probe.as_ref().map(|probe| {
            Arc::new(ProbeAdapter(Arc::clone(probe))) as Arc<dyn BoundedExecutionProbe>
        });
        fetch_bounded_cursor(&mut self.handle, rows, cancellation, probe)
            .map(|outcome| TestCursorFetch {
                page: outcome.page,
                telemetry: outcome.telemetry.into(),
            })
            .map_err(test_error)
    }

    /// Replace only the proof observer used for subsequent fetches.  Cursor
    /// lifecycle and continuation remain entirely production-owned.
    pub fn set_probe(&mut self, probe: Option<Arc<dyn ExecutionProbe>>) {
        self.probe = probe;
    }

    /// What this open cursor is holding between fetches, part by part -- the
    /// figure its settlement is compared against, and the name of each part
    /// that makes it up. A part the read does not have reads as zero.
    pub fn continuation_bytes(&self) -> Result<ContinuationBytes, TestError> {
        self.handle.continuation_bytes().map_err(test_error)
    }

    /// Closing must release the kernel's snapshot and retained memory.
    pub fn close(&mut self) -> Result<(), TestError> {
        close_bounded_cursor(&mut self.handle).map_err(test_error)
    }
}
