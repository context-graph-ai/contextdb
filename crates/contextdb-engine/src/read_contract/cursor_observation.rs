//! Borrowed canonical cursor trace encoding; no intermediate owned disclosure is built.
use super::*;
use crate::executor::CursorObservation;
use crate::owned_cursor_page::OwnedCursorPage as CursorPage;

#[derive(Serialize)]
struct BorrowedTrace<'a> {
    physical_plan: &'a str,
    index_used: &'a Option<String>,
    predicates_pushed: Predicates<'a>,
    indexes_considered: Candidates<'a>,
    sort_elided: bool,
    query_vector_source: &'a Option<VectorIndexRef>,
    vector_search: Option<BorrowedVector<'a>>,
    rows_examined: u64,
    maintained_count: Option<BorrowedCount<'a>>,
    groups: &'a [crate::database::GroupDisclosure],
}
struct Predicates<'a>(&'a [std::borrow::Cow<'static, str>]);
impl Serialize for Predicates<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|value| value.as_ref()))
    }
}
struct Candidates<'a>(&'a [crate::database::IndexCandidate]);
impl Serialize for Candidates<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            self.0
                .iter()
                .map(|value| (&value.name, value.rejected_reason.as_ref())),
        )
    }
}
struct Columns<'a>(&'a [(String, contextdb_core::SortDirection)]);
impl Serialize for Columns<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|(name, direction)| {
            (
                name,
                matches!(direction, contextdb_core::SortDirection::Desc),
            )
        }))
    }
}
struct ColumnLists<'a>(&'a [Vec<(String, contextdb_core::SortDirection)>]);
impl Serialize for ColumnLists<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|columns| Columns(columns)))
    }
}
#[derive(Serialize)]
struct BorrowedCount<'a> {
    used: bool,
    declared: bool,
    table: &'a str,
    columns: Columns<'a>,
    already_declared: ColumnLists<'a>,
}
impl<'a> From<&'a crate::maintained_counts::MaintainedCountDisclosure> for BorrowedCount<'a> {
    fn from(value: &'a crate::maintained_counts::MaintainedCountDisclosure) -> Self {
        use crate::maintained_counts::MaintainedCountDisclosure::*;
        let (used, declared, table, columns, already_declared) = match value {
            Used { table, columns } => (true, true, table, columns, &[][..]),
            DeclaredNotServed { table, columns } => (false, true, table, columns, &[][..]),
            NotDeclared {
                table,
                columns,
                already_declared,
            } => (false, false, table, columns, already_declared.as_slice()),
        };
        Self {
            used,
            declared,
            table,
            columns: Columns(columns),
            already_declared: ColumnLists(already_declared),
        }
    }
}
#[derive(Serialize)]
struct BorrowedVector<'a> {
    route_evidence: &'a str,
    requested_mode: &'a str,
    resolved_mode: &'a str,
    aggregate_allowed_vectors: Option<u64>,
    effective_auto_index_at: u64,
    auto_index_at_source: &'a str,
    partition_key_columns: &'a [String],
    scope: &'a str,
    route: Option<&'a str>,
    base: &'a str,
    change: &'a str,
    tail: &'a str,
    residual: &'a str,
    fallback: &'a Option<String>,
    refusal: &'a Option<String>,
    recovery: &'a Option<String>,
    query_source: &'a str,
    partition_hnsw: Partitions<'a>,
    global_candidate_budget: Option<u64>,
    admission_evaluations: Option<u64>,
    expansion_evaluations: Option<u64>,
    layers_admitted: Option<u64>,
    budget_terminal: &'a Option<String>,
}
struct Partitions<'a>(&'a [VectorPartitionHnswDisclosure]);
impl Serialize for Partitions<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|p| {
            (
                &p.partition,
                p.route.map(|r| r.as_str()),
                &p.reason,
                p.hnsw_m as u64,
                p.hnsw_ef_construction as u64,
                p.hnsw_ef_search as u64,
                &p.ef_search_source,
                p.policy_revision,
            )
        }))
    }
}
impl<'a> From<&'a VectorSearchDisclosure> for BorrowedVector<'a> {
    fn from(d: &'a VectorSearchDisclosure) -> Self {
        Self {
            route_evidence: d.route_evidence.as_str(),
            requested_mode: d.requested_mode.as_str(),
            resolved_mode: d.resolved_mode.as_str(),
            aggregate_allowed_vectors: d.aggregate_allowed_vectors.map(|n| n as u64),
            effective_auto_index_at: d.effective_auto_index_at as u64,
            auto_index_at_source: &d.auto_index_at_source,
            partition_key_columns: &d.partition_key_columns,
            scope: d.scope.as_str(),
            route: d.route.map(|r| r.as_str()),
            base: d.base.as_str(),
            change: d.change.as_str(),
            tail: d.tail.as_str(),
            residual: d.residual.as_str(),
            fallback: &d.fallback,
            refusal: &d.refusal,
            recovery: &d.recovery,
            query_source: d.query_source.as_str(),
            partition_hnsw: Partitions(&d.partition_hnsw),
            global_candidate_budget: d.global_candidate_budget.map(|n| n as u64),
            admission_evaluations: d.admission_evaluations.map(|n| n as u64),
            expansion_evaluations: d.expansion_evaluations.map(|n| n as u64),
            layers_admitted: d.layers_admitted.map(|n| n as u64),
            budget_terminal: &d.budget_terminal,
        }
    }
}
impl<'a> BorrowedTrace<'a> {
    fn new(trace: &'a QueryTrace, rows_examined: u64) -> Self {
        Self {
            physical_plan: trace.physical_plan,
            index_used: &trace.index_used,
            predicates_pushed: Predicates(&trace.predicates_pushed),
            indexes_considered: Candidates(&trace.indexes_considered),
            sort_elided: trace.sort_elided,
            query_vector_source: &trace.query_vector_source,
            vector_search: trace.vector_search.as_ref().map(Into::into),
            rows_examined,
            maintained_count: trace.maintained_count.as_deref().map(Into::into),
            groups: &trace.groups,
        }
    }
}
#[derive(Serialize)]
pub(crate) struct BorrowedObservedPage<'a> {
    page: &'a CursorPage,
    trace: BorrowedTrace<'a>,
}
impl<'a> BorrowedObservedPage<'a> {
    pub(crate) fn new(page: &'a CursorPage, observation: &'a CursorObservation) -> Self {
        Self {
            page,
            trace: BorrowedTrace::new(&observation.trace, observation.rows_examined),
        }
    }
}
#[derive(Deserialize)]
pub(crate) struct CanonicalObservedCursorPage {
    pub(crate) page: contextdb_core::read_contract::CursorPage,
    pub(crate) trace: CanonicalQueryTrace,
    // Standalone validation owns its native payload until this DTO drops.
    #[serde(skip)]
    memory: Option<std::sync::Arc<crate::read_image_memory::ImageMemory>>,
}

pub(crate) fn encode_observed_cursor_page(
    page: &CursorPage,
    observation: &CursorObservation,
) -> Result<Vec<u8>, ReadEncodingError> {
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    encode(&BorrowedObservedPage::new(page, observation))
}
pub(crate) fn decode_observed_cursor_page(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<CanonicalObservedCursorPage, ReadEncodingError> {
    let memory = decoder_memory(memory_ceiling)
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let mut observed = decode_observed_cursor_page_with_memory(bytes, &memory, memory_ceiling)?;
    observed.memory = Some(memory);
    Ok(observed)
}

pub(crate) fn decode_observed_cursor_page_with_memory(
    bytes: &[u8],
    memory: &std::sync::Arc<crate::read_image_memory::ImageMemory>,
    memory_ceiling: u64,
) -> Result<CanonicalObservedCursorPage, ReadEncodingError> {
    crate::local_transport::preflight_observed_cursor_page_payload(bytes, memory_ceiling)?;
    let observed: CanonicalObservedCursorPage =
        decode_exact_with_memory(bytes, memory, memory_ceiling)?;
    observed
        .page
        .validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    Ok(observed)
}
