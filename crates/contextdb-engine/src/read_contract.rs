//! Canonical bounded-read payload boundary.
//!
//! The encoder is intentionally route-neutral: it accepts query and page values, never a file
//! handle, owner channel, or route. The local protocol layer owns framing and byte-level fixtures.

pub(crate) mod cursor_observation;
mod encoded_bytes;
pub(crate) use cursor_observation::{decode_observed_cursor_page, encode_observed_cursor_page};
pub use encoded_bytes::EncodedReadBytes;

use crate::local_transport::{
    preflight_canonical_query_result, preflight_cursor_page_payload,
    preflight_metadata_page_payload,
};
use crate::{
    QueryResult, QueryTrace, VectorPartitionDisclosure, VectorQuerySourceDisclosure,
    VectorSearchDisclosure, VectorSearchLayerPresence, VectorSearchResidual, VectorSearchRoute,
    VectorSearchRouteEvidence, VectorSearchScopeShape, VectorSearchTailState,
};
use bincode::config::standard;
use contextdb_core::read_contract::{CursorPage, MetadataPage, ReadLimits};
use contextdb_core::{Value, VectorIndexRef, VectorSearchMode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalIndexCandidate {
    pub name: String,
    pub rejected_reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalQueryTrace {
    pub physical_plan: String,
    pub index_used: Option<String>,
    pub predicates_pushed: Vec<String>,
    pub indexes_considered: Vec<CanonicalIndexCandidate>,
    pub sort_elided: bool,
    pub query_vector_source: Option<VectorIndexRef>,
    pub vector_search: Option<CanonicalVectorSearchDisclosure>,
    pub rows_examined: u64,
    /// The maintained-index-count route disclosure, carried across the owner
    /// channel so a reader of someone else's store sees the same line an
    /// in-process `.explain` prints.
    #[serde(default)]
    pub maintained_count: Option<CanonicalMaintainedCountDisclosure>,
    pub groups: Vec<crate::database::GroupDisclosure>,
    pub cache_resident_fraction: Option<f64>,
}

/// `used` is the maintained count serving the answer; `false` with `declared`
/// is a count the table keeps current that this route counted from the index
/// range instead; `false` with neither is a count nothing declares, and then
/// `columns` and `already_declared` are what the advised statement is built
/// from.
///
/// Each column's direction rides as `true` for `DESC`, so the hand-written
/// preflight walker in `local_transport::framing` needs only the primitives it
/// already has to admit this shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalMaintainedCountDisclosure {
    pub used: bool,
    /// Whether the table DECLARES this count, which is separate from whether
    /// this route served the answer from it.
    pub declared: bool,
    pub table: String,
    pub columns: Vec<(String, bool)>,
    /// The lists the table already declares, carried so the advised statement
    /// a reader of someone else's store is shown keeps them.
    pub already_declared: Vec<Vec<(String, bool)>>,
}

fn canonical_count_columns(
    columns: &[(String, contextdb_core::SortDirection)],
) -> Vec<(String, bool)> {
    columns
        .iter()
        .map(|(column, direction)| {
            (
                column.clone(),
                matches!(direction, contextdb_core::SortDirection::Desc),
            )
        })
        .collect()
}

fn restored_count_columns(
    columns: Vec<(String, bool)>,
) -> Vec<(String, contextdb_core::SortDirection)> {
    let mut restored = Vec::with_capacity(columns.len());
    for (column, descending) in columns {
        restored.push((
            column,
            if descending {
                contextdb_core::SortDirection::Desc
            } else {
                contextdb_core::SortDirection::Asc
            },
        ));
    }
    restored
}

impl CanonicalMaintainedCountDisclosure {
    /// The engine-side disclosure this wire shape stands for.
    pub fn restored(self) -> crate::maintained_counts::MaintainedCountDisclosure {
        use crate::maintained_counts::MaintainedCountDisclosure;
        let columns = restored_count_columns(self.columns);
        match (self.used, self.declared) {
            (true, _) => MaintainedCountDisclosure::Used {
                table: self.table,
                columns,
            },
            (false, true) => MaintainedCountDisclosure::DeclaredNotServed {
                table: self.table,
                columns,
            },
            (false, false) => MaintainedCountDisclosure::NotDeclared {
                table: self.table,
                columns,
                already_declared: {
                    let mut lists = Vec::with_capacity(self.already_declared.len());
                    for columns in self.already_declared {
                        lists.push(restored_count_columns(columns));
                    }
                    lists
                },
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalVectorPartitionDisclosure {
    pub partition: String,
    pub route: Option<String>,
    pub reason: Option<String>,
    pub lists: Option<u32>,
    pub lists_source: Option<String>,
    pub probes: Option<u32>,
    pub probes_source: Option<String>,
    pub refine_factor_hundredths: Option<u32>,
    pub refine_factor_source: Option<String>,
    pub policy_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalVectorSearchDisclosure {
    pub route_evidence: String,
    pub requested_mode: String,
    pub resolved_mode: String,
    pub aggregate_allowed_vectors: Option<u64>,
    pub effective_auto_index_at: u64,
    pub auto_index_at_source: String,
    pub partition_key_columns: Vec<String>,
    pub scope: String,
    pub route: Option<String>,
    pub base: String,
    pub change: String,
    pub tail: String,
    pub residual: String,
    pub fallback: Option<String>,
    pub refusal: Option<String>,
    pub recovery: Option<String>,
    pub query_source: String,
    pub partitions: Vec<CanonicalVectorPartitionDisclosure>,
    #[serde(default)]
    pub global_candidate_budget: Option<u64>,
    #[serde(default)]
    pub admission_evaluations: Option<u64>,
    #[serde(default)]
    pub expansion_evaluations: Option<u64>,
    #[serde(default)]
    pub rescore_evaluations: Option<u64>,
    #[serde(default)]
    pub layers_admitted: Option<u64>,
    #[serde(default)]
    pub budget_terminal: Option<String>,
}

impl From<&VectorSearchDisclosure> for CanonicalVectorSearchDisclosure {
    fn from(disclosure: &VectorSearchDisclosure) -> Self {
        Self {
            route_evidence: disclosure.route_evidence.as_str().to_owned(),
            requested_mode: disclosure.requested_mode.as_str().to_owned(),
            resolved_mode: disclosure.resolved_mode.as_str().to_owned(),
            aggregate_allowed_vectors: disclosure
                .aggregate_allowed_vectors
                .map(|count| count as u64),
            effective_auto_index_at: disclosure.effective_auto_index_at as u64,
            auto_index_at_source: disclosure.auto_index_at_source.clone(),
            partition_key_columns: disclosure.partition_key_columns.clone(),
            scope: disclosure.scope.as_str().to_owned(),
            route: disclosure.route.map(|route| route.as_str().to_owned()),
            base: disclosure.base.as_str().to_owned(),
            change: disclosure.change.as_str().to_owned(),
            tail: disclosure.tail.as_str().to_owned(),
            residual: disclosure.residual.as_str().to_owned(),
            fallback: disclosure.fallback.clone(),
            refusal: disclosure.refusal.clone(),
            recovery: disclosure.recovery.clone(),
            query_source: disclosure.query_source.as_str().to_owned(),
            global_candidate_budget: disclosure.global_candidate_budget.map(|count| count as u64),
            admission_evaluations: disclosure.admission_evaluations.map(|count| count as u64),
            expansion_evaluations: disclosure.expansion_evaluations.map(|count| count as u64),
            rescore_evaluations: disclosure.rescore_evaluations.map(|count| count as u64),
            layers_admitted: disclosure.layers_admitted.map(|count| count as u64),
            budget_terminal: disclosure.budget_terminal.clone(),
            partitions: disclosure
                .partitions
                .iter()
                .map(|partition| CanonicalVectorPartitionDisclosure {
                    partition: partition.partition.clone(),
                    route: partition.route.map(|route| route.as_str().to_owned()),
                    reason: partition.reason.clone(),
                    lists: partition.lists,
                    lists_source: partition.lists_source.clone(),
                    probes: partition.probes,
                    probes_source: partition.probes_source.clone(),
                    refine_factor_hundredths: partition.refine_factor_hundredths,
                    refine_factor_source: partition.refine_factor_source.clone(),
                    policy_revision: partition.policy_revision,
                })
                .collect(),
        }
    }
}

impl TryFrom<CanonicalVectorSearchDisclosure> for VectorSearchDisclosure {
    type Error = ();

    fn try_from(disclosure: CanonicalVectorSearchDisclosure) -> Result<Self, Self::Error> {
        let mode = |mode: &str| match mode {
            "AUTO" => Ok(VectorSearchMode::Auto),
            "EXACT" => Ok(VectorSearchMode::Exact),
            "INDEXED" => Ok(VectorSearchMode::Indexed),
            _ => Err(()),
        };
        let route_evidence = match disclosure.route_evidence.as_str() {
            "passive-prediction" => VectorSearchRouteEvidence::PassivePrediction,
            "executed" => VectorSearchRouteEvidence::Executed,
            _ => return Err(()),
        };
        let scope = match disclosure.scope.as_str() {
            "one" => VectorSearchScopeShape::One,
            "several" => VectorSearchScopeShape::Few,
            "all" => VectorSearchScopeShape::All,
            _ => return Err(()),
        };
        let route = disclosure
            .route
            .as_deref()
            .map(|route| match route {
                "exact" => Ok(VectorSearchRoute::Exact),
                "indexed" => Ok(VectorSearchRoute::Indexed),
                "filtered-indexed" => Ok(VectorSearchRoute::FilteredIndexed),
                _ => Err(()),
            })
            .transpose()?;
        let layer = |layer: &str| match layer {
            "present" => Ok(VectorSearchLayerPresence::Present),
            "absent" => Ok(VectorSearchLayerPresence::Absent),
            _ => Err(()),
        };
        let tail = match disclosure.tail.as_str() {
            "present" => VectorSearchTailState::Present,
            "empty" => VectorSearchTailState::Empty,
            _ => return Err(()),
        };
        let residual = match disclosure.residual.as_str() {
            "none" => VectorSearchResidual::None,
            "bounded" => VectorSearchResidual::Bounded,
            "unsupported" => VectorSearchResidual::Unsupported,
            _ => return Err(()),
        };
        let query_source = match disclosure.query_source.as_str() {
            "<vector>" => VectorQuerySourceDisclosure::Vector,
            "<redacted>" => VectorQuerySourceDisclosure::RedactedRowKey,
            "unknown" => VectorQuerySourceDisclosure::Unknown,
            _ => return Err(()),
        };
        Ok(Self {
            route_evidence,
            requested_mode: mode(&disclosure.requested_mode)?,
            resolved_mode: mode(&disclosure.resolved_mode)?,
            aggregate_allowed_vectors: disclosure
                .aggregate_allowed_vectors
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ())?,
            effective_auto_index_at: usize::try_from(disclosure.effective_auto_index_at)
                .map_err(|_| ())?,
            auto_index_at_source: disclosure.auto_index_at_source,
            partition_key_columns: disclosure.partition_key_columns,
            scope,
            route,
            base: layer(&disclosure.base)?,
            change: layer(&disclosure.change)?,
            tail,
            residual,
            fallback: disclosure.fallback,
            refusal: disclosure.refusal,
            recovery: disclosure.recovery,
            query_source,
            global_candidate_budget: disclosure
                .global_candidate_budget
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ())?,
            admission_evaluations: disclosure
                .admission_evaluations
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ())?,
            expansion_evaluations: disclosure
                .expansion_evaluations
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ())?,
            rescore_evaluations: disclosure
                .rescore_evaluations
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ())?,
            layers_admitted: disclosure
                .layers_admitted
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ())?,
            budget_terminal: disclosure.budget_terminal,
            partitions: {
                let mut partitions = Vec::with_capacity(disclosure.partitions.len());
                for partition in disclosure.partitions {
                    partitions.push(VectorPartitionDisclosure {
                        partition: partition.partition,
                        route: partition
                            .route
                            .as_deref()
                            .map(|value| match value {
                                "exact" => Ok(VectorSearchRoute::Exact),
                                "indexed" => Ok(VectorSearchRoute::Indexed),
                                "filtered-indexed" => Ok(VectorSearchRoute::FilteredIndexed),
                                _ => Err(()),
                            })
                            .transpose()?,
                        reason: partition.reason,
                        lists: partition.lists,
                        lists_source: partition.lists_source,
                        probes: partition.probes,
                        probes_source: partition.probes_source,
                        refine_factor_hundredths: partition.refine_factor_hundredths,
                        refine_factor_source: partition.refine_factor_source,
                        policy_revision: partition.policy_revision,
                    });
                }
                partitions
            },
        })
    }
}

impl CanonicalQueryTrace {
    /// Empty inline trace for measuring a moved result's row buffers through
    /// the existing shared native-capacity walker.
    pub(crate) fn empty() -> Self {
        Self {
            physical_plan: String::new(),
            index_used: None,
            predicates_pushed: Vec::new(),
            indexes_considered: Vec::new(),
            sort_elided: false,
            query_vector_source: None,
            vector_search: None,
            rows_examined: 0,
            maintained_count: None,
            groups: Vec::new(),
            cache_resident_fraction: None,
        }
    }
}

impl From<&QueryTrace> for CanonicalQueryTrace {
    fn from(trace: &QueryTrace) -> Self {
        Self {
            physical_plan: trace.physical_plan.to_owned(),
            index_used: trace.index_used.clone(),
            predicates_pushed: trace
                .predicates_pushed
                .iter()
                .map(|predicate| predicate.to_string())
                .collect(),
            indexes_considered: trace
                .indexes_considered
                .iter()
                .map(|candidate| CanonicalIndexCandidate {
                    name: candidate.name.clone(),
                    rejected_reason: candidate.rejected_reason.to_string(),
                })
                .collect(),
            sort_elided: trace.sort_elided,
            query_vector_source: trace.query_vector_source.clone(),
            vector_search: trace.vector_search.as_deref().map(Into::into),
            rows_examined: trace.rows_examined,
            groups: trace.groups.to_vec(),
            cache_resident_fraction: trace.cache_resident_fraction,
            maintained_count: trace.maintained_count.as_deref().map(|disclosure| {
                use crate::maintained_counts::MaintainedCountDisclosure;
                let (used, declared, table, columns, already_declared) = match disclosure {
                    MaintainedCountDisclosure::Used { table, columns } => {
                        (true, true, table, columns, Vec::new())
                    }
                    MaintainedCountDisclosure::DeclaredNotServed { table, columns } => {
                        (false, true, table, columns, Vec::new())
                    }
                    MaintainedCountDisclosure::NotDeclared {
                        table,
                        columns,
                        already_declared,
                    } => (false, false, table, columns, already_declared.clone()),
                };
                CanonicalMaintainedCountDisclosure {
                    used,
                    declared,
                    table: table.clone(),
                    columns: canonical_count_columns(columns),
                    already_declared: already_declared
                        .iter()
                        .map(|list| canonical_count_columns(list))
                        .collect(),
                }
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalCascadeReport {
    pub dropped_indexes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
    pub trace: CanonicalQueryTrace,
    pub cascade: Option<CanonicalCascadeReport>,
}

/// The canonical result WITHOUT copying it, for the one thing that only needs
/// to look: measuring how many bytes it would take on the wire.
///
/// Measuring encodes into a counter that keeps nothing, so the copy the owned
/// form made existed purely to be handed to the encoder -- and it was made
/// again after every row appended, which is what put a second and a third full
/// copy of a large row under the memory ceiling at the same time. Borrowed,
/// the measurement costs the rows nothing.
///
/// The fields are the owned form's, in the owned form's order, and bincode
/// writes a borrowed slice and a `Vec` identically, so the number this
/// measures is the number the published encoding produces.
#[derive(Debug, Serialize)]
pub struct CanonicalQueryResultView<'result> {
    pub columns: &'result [String],
    #[serde(serialize_with = "serialize_query_result_rows")]
    pub rows: &'result [Vec<Value>],
    pub rows_affected: u64,
    #[serde(serialize_with = "serialize_query_result_trace")]
    pub trace: &'result QueryTrace,
    #[serde(serialize_with = "serialize_query_result_cascade")]
    pub cascade: Option<&'result crate::database::CascadeReport>,
}

impl<'result> From<&'result QueryResult> for CanonicalQueryResultView<'result> {
    fn from(result: &'result QueryResult) -> Self {
        Self {
            columns: result.columns(),
            rows: result.rows(),
            rows_affected: result.rows_affected(),
            trace: result.trace(),
            cascade: result.cascade(),
        }
    }
}
impl<'result> From<&'result crate::database::QueryResultData>
    for CanonicalQueryResultView<'result>
{
    fn from(result: &'result crate::database::QueryResultData) -> Self {
        Self {
            columns: &result.columns,
            rows: &result.rows,
            rows_affected: result.rows_affected,
            trace: &result.trace,
            cascade: result.cascade.as_ref(),
        }
    }
}

// This explicit application DTO conversion preserves the consumer copy
// boundary. Engine execution and serializers use the borrowed view above.
impl From<&QueryResult> for CanonicalQueryResult {
    fn from(result: &QueryResult) -> Self {
        Self {
            columns: result.columns().to_vec(),
            rows: result.rows().to_vec(),
            rows_affected: result.rows_affected(),
            trace: CanonicalQueryTrace::from(result.trace()),
            cascade: result.cascade().map(|cascade| CanonicalCascadeReport {
                dropped_indexes: cascade.dropped_indexes.clone(),
            }),
        }
    }
}
fn serialize_query_result_rows<S: serde::Serializer>(
    rows: &[Vec<Value>],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    QueryRows(rows).serialize(serializer)
}

fn serialize_query_result_trace<S: serde::Serializer>(
    trace: &QueryTrace,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    QueryTraceView::from(trace).serialize(serializer)
}
fn serialize_query_result_cascade<S: serde::Serializer>(
    cascade: &Option<&crate::database::CascadeReport>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    cascade
        .map(|cascade| QueryCascadeView {
            dropped_indexes: &cascade.dropped_indexes,
        })
        .serialize(serializer)
}

#[derive(Debug, thiserror::Error)]
pub enum ReadEncodingError {
    /// Original admission error, moved intact through output encoding.
    #[error("canonical output admission failed: {0}")]
    Admission(contextdb_core::Error),
    #[error("canonical read encoding is not implemented")]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from bounded.rs"
        )
    )]
    Unimplemented,
    #[error("canonical read payload is invalid")]
    InvalidPayload,
    /// The payload is exactly what it claims to be; holding its decoded form
    /// would cross the memory ceiling in force for this decode. That is a
    /// budget answer a caller can act on by raising the ceiling, so it is
    /// kept apart from bytes that are not a payload at all.
    #[error("canonical read payload would exceed the {ceiling} byte memory ceiling")]
    MemoryCeilingExceeded { ceiling: u64 },
}

/// Keep a wire preflight's ONE budget answer as a budget answer while it
/// crosses into the encoding vocabulary. Everything else the preflight
/// refuses is content this decoder could not read.
fn encoding_error_from_transport(
    error: crate::local_transport::LocalTransportError,
) -> ReadEncodingError {
    match error {
        crate::local_transport::LocalTransportError::Payload(
            crate::local_transport::PayloadViolation::MemoryCeilingExceeded { ceiling, .. },
        ) => ReadEncodingError::MemoryCeilingExceeded { ceiling },
        _ => ReadEncodingError::InvalidPayload,
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from local_transport_framing.rs"
    )
)]
pub fn encode_query_result(
    result: &QueryResult,
    caller_owner: contextdb_core::read_memory::ReadMemoryOwner,
) -> Result<EncodedReadBytes, ReadEncodingError> {
    let owner = result.encoding_owner().unwrap_or(caller_owner);
    let view = CanonicalQueryResultView::from(result);
    validate_query_view(&view)?;
    let bytes = encode_transport_funded(&view, Some(owner), u64::MAX)?;
    EncodedReadBytes::from_funded(bytes, u64::MAX)
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature"
    )
)]
fn validate_query_view(view: &CanonicalQueryResultView<'_>) -> Result<(), ReadEncodingError> {
    for row in view.rows {
        if row.len() != view.columns.len() {
            return Err(ReadEncodingError::InvalidPayload);
        }
    }
    Ok(())
}

// Stack-only views of the existing canonical fields, in the identical
// order. Encoding never constructs an owned trace/cascade merely to measure
// and serialize it. These types are private and are not a new wire contract.
#[derive(Serialize)]
struct QueryPartitionView<'a> {
    partition: &'a str,
    route: Option<&'a str>,
    reason: Option<&'a str>,
    lists: Option<u32>,
    lists_source: Option<&'a str>,
    probes: Option<u32>,
    probes_source: Option<&'a str>,
    refine_factor_hundredths: Option<u32>,
    refine_factor_source: Option<&'a str>,
    policy_revision: u64,
}
#[derive(Serialize)]
struct QueryVectorView<'a> {
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
    fallback: Option<&'a str>,
    refusal: Option<&'a str>,
    recovery: Option<&'a str>,
    query_source: &'a str,
    partitions: QueryPartitionSequence<'a>,
    global_candidate_budget: Option<u64>,
    admission_evaluations: Option<u64>,
    expansion_evaluations: Option<u64>,
    rescore_evaluations: Option<u64>,
    layers_admitted: Option<u64>,
    budget_terminal: Option<&'a str>,
}
#[derive(Serialize)]
struct QueryTraceView<'a> {
    physical_plan: &'a str,
    index_used: Option<&'a str>,
    predicates_pushed: &'a [std::borrow::Cow<'static, str>],
    indexes_considered: QueryCandidateSequence<'a>,
    sort_elided: bool,
    query_vector_source: Option<&'a VectorIndexRef>,
    vector_search: Option<QueryVectorView<'a>>,
    rows_examined: u64,
    maintained_count: Option<QueryCountView<'a>>,
    groups: &'a [crate::database::GroupDisclosure],
    cache_resident_fraction: Option<f64>,
}
#[derive(Serialize)]
struct QueryCountView<'a> {
    used: bool,
    declared: bool,
    table: &'a str,
    columns: QueryCountColumns<'a>,
    already_declared: QueryCountLists<'a>,
}
#[derive(Serialize)]
struct QueryCascadeView<'a> {
    dropped_indexes: &'a [String],
}
#[derive(Serialize)]
struct QueryResultView<'a> {
    columns: &'a [String],
    rows: QueryRows<'a>,
    rows_affected: u64,
    trace: QueryTraceView<'a>,
    cascade: Option<QueryCascadeView<'a>>,
}
// The core JSON serializer builds a temporary String. This private binary
// view writes that SAME length-prefixed UTF-8 directly, without a second
// engine-held copy. Other Value variants keep their existing serializer.
struct QueryRows<'a>(&'a [Vec<Value>]);
struct QueryRow<'a>(&'a [Value]);
struct QueryValue<'a>(&'a Value);
impl Serialize for QueryRows<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            sequence.serialize_element(&QueryRow(row))?;
        }
        sequence.end()
    }
}
impl Serialize for QueryRow<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            sequence.serialize_element(&QueryValue(value))?;
        }
        sequence.end()
    }
}
impl Serialize for QueryValue<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Json(value) => {
                serializer.serialize_newtype_variant("Value", 7, "Json", &QueryJson(value))
            }
            value => value.serialize(serializer),
        }
    }
}
struct QueryJson<'a>(&'a serde_json::Value);
struct QueryJsonSize(usize);
impl std::io::Write for QueryJsonSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or(std::io::ErrorKind::InvalidData)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct QueryJsonSequence<S: serde::ser::SerializeSeq> {
    sequence: S,
    failure: Option<S::Error>,
}
impl<S: serde::ser::SerializeSeq> std::io::Write for QueryJsonSequence<S> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for byte in bytes {
            if let Err(error) = self.sequence.serialize_element(byte) {
                self.failure = Some(error);
                return Err(std::io::ErrorKind::InvalidData.into());
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Serialize for QueryJson<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeSeq};
        let mut size = QueryJsonSize(0);
        serde_json::to_writer(&mut size, self.0).map_err(S::Error::custom)?;
        let mut output = QueryJsonSequence {
            sequence: serializer.serialize_seq(Some(size.0))?,
            failure: None,
        };
        let encoded = serde_json::to_writer(&mut output, self.0);
        if let Some(error) = output.failure {
            return Err(error);
        }
        encoded.map_err(S::Error::custom)?;
        output.sequence.end()
    }
}
struct QueryCandidateSequence<'a>(&'a [crate::database::IndexCandidate]);
impl Serialize for QueryCandidateSequence<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        #[derive(Serialize)]
        struct Candidate<'a> {
            name: &'a str,
            rejected_reason: &'a str,
        }
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for candidate in self.0 {
            sequence.serialize_element(&Candidate {
                name: &candidate.name,
                rejected_reason: candidate.rejected_reason.as_ref(),
            })?;
        }
        sequence.end()
    }
}
struct QueryPartitionSequence<'a>(&'a [VectorPartitionDisclosure]);
impl Serialize for QueryPartitionSequence<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for partition in self.0 {
            sequence.serialize_element(&QueryPartitionView {
                partition: &partition.partition,
                route: partition.route.map(|route| route.as_str()),
                reason: partition.reason.as_deref(),
                lists: partition.lists,
                lists_source: partition.lists_source.as_deref(),
                probes: partition.probes,
                probes_source: partition.probes_source.as_deref(),
                refine_factor_hundredths: partition.refine_factor_hundredths,
                refine_factor_source: partition.refine_factor_source.as_deref(),
                policy_revision: partition.policy_revision,
            })?;
        }
        sequence.end()
    }
}
struct QueryCountColumns<'a>(&'a [(String, contextdb_core::SortDirection)]);
impl Serialize for QueryCountColumns<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for (name, direction) in self.0 {
            sequence.serialize_element(&(
                name,
                matches!(direction, contextdb_core::SortDirection::Desc),
            ))?;
        }
        sequence.end()
    }
}
struct QueryCountLists<'a>(&'a [Vec<(String, contextdb_core::SortDirection)>]);
impl Serialize for QueryCountLists<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for columns in self.0 {
            sequence.serialize_element(&QueryCountColumns(columns))?;
        }
        sequence.end()
    }
}
impl<'a> From<&'a crate::maintained_counts::MaintainedCountDisclosure> for QueryCountView<'a> {
    fn from(value: &'a crate::maintained_counts::MaintainedCountDisclosure) -> Self {
        use crate::maintained_counts::MaintainedCountDisclosure;
        let (used, declared, table, columns, lists) = match value {
            MaintainedCountDisclosure::Used { table, columns } => {
                (true, true, table, columns, &[][..])
            }
            MaintainedCountDisclosure::DeclaredNotServed { table, columns } => {
                (false, true, table, columns, &[][..])
            }
            MaintainedCountDisclosure::NotDeclared {
                table,
                columns,
                already_declared,
            } => (false, false, table, columns, already_declared.as_slice()),
        };
        Self {
            used,
            declared,
            table,
            columns: QueryCountColumns(columns),
            already_declared: QueryCountLists(lists),
        }
    }
}
impl<'a> From<&'a QueryTrace> for QueryTraceView<'a> {
    fn from(trace: &'a QueryTrace) -> Self {
        Self {
            physical_plan: trace.physical_plan,
            index_used: trace.index_used.as_deref(),
            predicates_pushed: &trace.predicates_pushed,
            indexes_considered: QueryCandidateSequence(&trace.indexes_considered),
            sort_elided: trace.sort_elided,
            query_vector_source: trace.query_vector_source.as_ref(),
            vector_search: trace.vector_search.as_deref().map(Into::into),
            rows_examined: trace.rows_examined,
            maintained_count: trace.maintained_count.as_deref().map(Into::into),
            groups: &trace.groups,
            cache_resident_fraction: trace.cache_resident_fraction,
        }
    }
}
impl<'a> From<&'a VectorSearchDisclosure> for QueryVectorView<'a> {
    fn from(disclosure: &'a VectorSearchDisclosure) -> Self {
        Self {
            route_evidence: disclosure.route_evidence.as_str(),
            requested_mode: disclosure.requested_mode.as_str(),
            resolved_mode: disclosure.resolved_mode.as_str(),
            aggregate_allowed_vectors: disclosure
                .aggregate_allowed_vectors
                .map(|count| count as u64),
            effective_auto_index_at: disclosure.effective_auto_index_at as u64,
            auto_index_at_source: disclosure.auto_index_at_source.as_str(),
            partition_key_columns: disclosure.partition_key_columns.as_slice(),
            scope: disclosure.scope.as_str(),
            route: disclosure.route.map(|route| route.as_str()),
            base: disclosure.base.as_str(),
            change: disclosure.change.as_str(),
            tail: disclosure.tail.as_str(),
            residual: disclosure.residual.as_str(),
            fallback: disclosure.fallback.as_deref(),
            refusal: disclosure.refusal.as_deref(),
            recovery: disclosure.recovery.as_deref(),
            query_source: disclosure.query_source.as_str(),
            global_candidate_budget: disclosure.global_candidate_budget.map(|count| count as u64),
            admission_evaluations: disclosure.admission_evaluations.map(|count| count as u64),
            expansion_evaluations: disclosure.expansion_evaluations.map(|count| count as u64),
            rescore_evaluations: disclosure.rescore_evaluations.map(|count| count as u64),
            layers_admitted: disclosure.layers_admitted.map(|count| count as u64),
            budget_terminal: disclosure.budget_terminal.as_deref(),
            partitions: QueryPartitionSequence(&disclosure.partitions),
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn encode_query_result_funded(
    result: &crate::database::QueryResultData,
    owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    memory_ceiling: u64,
) -> Result<crate::local_transport::query_funding::Funded<Vec<u8>>, ReadEncodingError> {
    let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

    for row in &result.rows {
        if row.len() != result.columns.len() {
            return Err(ReadEncodingError::InvalidPayload);
        }
    }
    let view = QueryResultView {
        columns: &result.columns,
        rows: QueryRows(&result.rows),
        rows_affected: result.rows_affected,
        trace: QueryTraceView::from(&result.trace),
        cascade: result.cascade.as_ref().map(|cascade| QueryCascadeView {
            dropped_indexes: &cascade.dropped_indexes,
        }),
    };
    encode_transport_funded(&view, Some(owner), memory_ceiling)
}

/// The Query variant has the same tag, field order and map ordering as the
/// owned public DTO. Only JSON values use the existing streaming wire view.
#[derive(Serialize)]
struct QueryRequestEnvelopeView<'a> {
    limits: ReadLimits,
    request: QueryRequestView<'a>,
}
#[derive(Serialize)]
enum QueryRequestView<'a> {
    Query {
        statement: &'a str,
        params: QueryParamsView<'a>,
    },
}
struct QueryParamsView<'a>(&'a std::collections::BTreeMap<String, Value>);
impl Serialize for QueryParamsView<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(key, &QueryValue(value))?;
        }
        map.end()
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn encode_request_funded(
    request: &crate::local_transport::LocalRequestEnvelope,
    owner: Option<contextdb_core::read_memory::ReadMemoryOwner>,
    memory_ceiling: u64,
) -> Result<crate::local_transport::query_funding::Funded<Vec<u8>>, ReadEncodingError> {
    if let crate::local_transport::LocalRequest::Query { statement, params } = &request.request {
        encode_transport_funded(
            &QueryRequestEnvelopeView {
                limits: request.limits,
                request: QueryRequestView::Query {
                    statement,
                    params: QueryParamsView(params),
                },
            },
            owner,
            memory_ceiling,
        )
    } else {
        encode_transport_funded(request, owner, memory_ceiling)
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn encode_transport_funded<T: Serialize>(
    value: &T,
    owner: Option<contextdb_core::read_memory::ReadMemoryOwner>,
    memory_ceiling: u64,
) -> Result<crate::local_transport::query_funding::Funded<Vec<u8>>, ReadEncodingError> {
    let len = encoded_size(value)?;
    let mut output =
        crate::local_transport::query_funding::Funded::allocate_owned(owner, len, || vec![0; len])
            .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    // A fixed slice cannot silently grow beyond the admitted allocation if a
    // serializer writes a different size on its second visit.
    let written = contextdb_core::types::with_borrowed_json_wire(|| {
        bincode::serde::encode_into_slice(value, &mut output.value, standard())
    })
    .map_err(|_| ReadEncodingError::InvalidPayload)?;
    if written != len {
        return Err(ReadEncodingError::InvalidPayload);
    }
    Ok(output)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn decode_query_result_funded(
    bytes: &[u8],
    memory: &crate::read_image_memory::ImageMemoryHandle,
    memory_ceiling: u64,
) -> Result<crate::local_transport::query_funding::Funded<CanonicalQueryResult>, ReadEncodingError>
{
    #[cfg(feature = "test-seams")]
    crate::local_transport::OrdinaryResultReceiver::observe_actual_canonical_decode_entry_for_test(
        bytes,
    );
    preflight_canonical_query_result(bytes, memory_ceiling)?;
    let result = crate::local_transport::query_funding::decode(bytes, memory)
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    validate_query_result(&result.value)?;
    Ok(result)
}

/// Decode with no caller ceiling in hand. The shipped default stands in as
/// the backstop, which is what a caller that never declared one gets.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from read_contract_encoding.rs"
    )
)]
pub fn decode_query_result(bytes: &[u8]) -> Result<CanonicalQueryResult, ReadEncodingError> {
    decode_query_result_under_memory_ceiling(bytes, ReadLimits::SHIPPED_MEMORY)
}

/// Decode against the memory ceiling actually in force for THIS exchange.
/// A caller that declared a tighter or wider envelope is held to its own
/// number rather than to the shipped backstop, so a decode refused here names
/// the ceiling that caller set and can raise.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn decode_query_result_under_memory_ceiling(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<CanonicalQueryResult, ReadEncodingError> {
    #[cfg(feature = "test-seams")]
    crate::local_transport::OrdinaryResultReceiver::observe_actual_canonical_decode_entry_for_test(
        bytes,
    );
    preflight_canonical_query_result(bytes, memory_ceiling)?;
    let memory = decoder_memory(memory_ceiling)
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let result = decode_exact_with_memory(bytes, &memory, memory_ceiling)?;
    validate_query_result(&result)?;
    Ok(result)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from read_contract_encoding.rs"
    )
)]
pub fn query_result_encoded_size(result: &QueryResult) -> Result<usize, ReadEncodingError> {
    let view = CanonicalQueryResultView::from(result);
    validate_query_view(&view)?;
    encoded_size(&view)
}

/// Canonical bytes for one metadata answer.
///
/// Metadata travels the same way ordinary results do, so it needs the same
/// property: two routes answering the same question publish the same bytes.
/// The reading vocabulary carries no serialization derives -- its module is
/// closed over a small set of dependencies on purpose -- so the shape is
/// written out here, field by field, length-prefixed so no two shapes can
/// render alike.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from metadata_body_round_trip.rs"
    )
)]
pub fn encode_metadata_body(
    body: &crate::direct_file_reader::DirectMetadataBody,
    owner: contextdb_core::read_memory::ReadMemoryOwner,
) -> Result<EncodedReadBytes, ReadEncodingError> {
    EncodedReadBytes::from_funded(
        encode_metadata_body_funded(body, owner, u64::MAX)?,
        u64::MAX,
    )
}

/// How many bytes [`encode_metadata_body`] produces for `body`, measured
/// without building them, so a limit can refuse the encoding before it is
/// allocated.
pub(crate) fn encoded_metadata_body_len(
    body: &crate::direct_file_reader::DirectMetadataBody,
) -> usize {
    let mut counter = CanonicalWriter::counting();
    crate::read_image::write_metadata_body(&mut counter, body);
    counter.len()
}

/// Encode on the actual request owner; the returned allocation keeps its
/// original credit through payload destruction, including on failed callers.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn encode_metadata_body_funded(
    body: &crate::direct_file_reader::DirectMetadataBody,
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    memory_ceiling: u64,
) -> Result<crate::local_transport::query_funding::Funded<Vec<u8>>, ReadEncodingError> {
    let len = encoded_metadata_body_len(body);
    let mut output =
        crate::local_transport::query_funding::Funded::allocate_owned(Some(owner), len, || {
            Vec::with_capacity(len)
        })
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let mut writer = CanonicalWriter {
        bytes: std::mem::take(&mut output.value),
        len: 0,
        counting: false,
        credit: None,
        overflowed: false,
    };
    crate::read_image::write_metadata_body(&mut writer, body);
    if writer.len() != len {
        return Err(ReadEncodingError::InvalidPayload);
    }
    output.value = writer.finish_raw()?;
    #[cfg(any(test, feature = "test-seams"))]
    crate::read_image::answer_encoded_for_test();
    Ok(output)
}

/// A deterministic, self-delimiting byte writer for canonical documents.
pub struct CanonicalWriter {
    bytes: Vec<u8>,
    len: usize,
    /// Measure only: count what would be written and keep none of it.
    counting: bool,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    credit: Option<contextdb_core::read_memory::ReadCredit>,
    overflowed: bool,
}

impl CanonicalWriter {
    /// Admit the exact output capacity on the original caller owner. Writing
    /// beyond this capacity refuses at finish instead of growing uncharged.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub fn try_new(
        owner: contextdb_core::read_memory::ReadMemoryOwner,
        capacity: usize,
    ) -> Result<Self, ReadEncodingError> {
        let bytes = crate::local_transport::query_funding::Funded::allocate_owned(
            Some(owner),
            capacity,
            || Vec::with_capacity(capacity),
        )
        .map_err(ReadEncodingError::Admission)?;
        Ok(Self {
            bytes: bytes.value,
            len: 0,
            counting: false,
            credit: bytes.credit,
            overflowed: false,
        })
    }

    /// A writer that measures a document's encoded length without keeping
    /// its bytes.
    pub fn counting() -> Self {
        Self {
            bytes: Vec::new(),
            len: 0,
            counting: true,
            credit: None,
            overflowed: false,
        }
    }

    /// The bytes written, or counted, so far.
    pub fn len(&self) -> usize {
        self.len
    }

    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn put(&mut self, bytes: &[u8]) {
        let Some(len) = self.len.checked_add(bytes.len()) else {
            self.overflowed = true;
            return;
        };
        self.len = len;
        if !self.counting && !self.overflowed {
            if len > self.bytes.capacity() {
                self.overflowed = true;
            } else {
                self.bytes.extend_from_slice(bytes);
            }
        }
    }

    pub fn tag(&mut self, tag: u8) -> &mut Self {
        self.put(&[tag]);
        self
    }

    pub fn count(&mut self, value: u64) -> &mut Self {
        self.put(&value.to_le_bytes());
        self
    }

    pub fn flag(&mut self, value: bool) -> &mut Self {
        self.tag(u8::from(value))
    }

    pub fn text(&mut self, text: &str) -> &mut Self {
        self.count(text.len() as u64);
        self.put(text.as_bytes());
        self
    }

    /// Preserve the length-prefixed JSON text without allocating a temporary
    /// String, including when this writer only measures an answer.
    pub(crate) fn json_text<T: Serialize>(&mut self, value: &T) -> &mut Self {
        let mut size = QueryJsonSize(0);
        serde_json::to_writer(&mut size, value).expect("canonical metadata JSON serializes");
        self.count(size.0 as u64);
        if self.counting {
            self.len = self.len.saturating_add(size.0);
        } else {
            struct JsonSink<'a>(&'a mut CanonicalWriter);
            impl std::io::Write for JsonSink<'_> {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    self.0.put(bytes);
                    Ok(bytes.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            serde_json::to_writer(JsonSink(&mut *self), value)
                .expect("canonical metadata JSON serializes");
        }
        self
    }

    pub fn optional_text(&mut self, text: Option<&str>) -> &mut Self {
        match text {
            Some(text) => {
                self.tag(1);
                self.text(text)
            }
            None => self.tag(0),
        }
    }

    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.count(bytes.len() as u64);
        self.put(bytes);
        self
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn finish_raw(self) -> Result<Vec<u8>, ReadEncodingError> {
        if self.overflowed {
            return Err(ReadEncodingError::InvalidPayload);
        }
        Ok(self.bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub fn finish(self) -> Result<EncodedReadBytes, ReadEncodingError> {
        if self.counting || self.overflowed {
            return Err(ReadEncodingError::InvalidPayload);
        }
        EncodedReadBytes::from_funded(
            crate::local_transport::query_funding::Funded {
                value: self.bytes,
                credit: self.credit,
            },
            u64::MAX,
        )
    }
}

/// Read one metadata answer back out of canonical bytes.
///
/// The inverse of [`encode_metadata_body`], field for field: the same document
/// order, the same length prefixes, the same tags. Bytes that do not spell a
/// complete body -- a tag no kind uses, a length that runs past the end, text
/// that is not UTF-8, trailing bytes after a complete body -- are not a
/// metadata answer and are refused as an invalid payload rather than guessed
/// at.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from metadata_body_round_trip.rs"
    )
)]
pub fn decode_metadata_body(
    bytes: &[u8],
) -> Result<crate::direct_file_reader::DirectMetadataBody, ReadEncodingError> {
    let memory = decoder_memory(ReadLimits::SHIPPED_MEMORY)
        .map_err(|error| encoding_error_from_core(error, ReadLimits::SHIPPED_MEMORY))?;
    decode_metadata_body_with_memory(bytes, &memory, ReadLimits::SHIPPED_MEMORY)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn decode_metadata_body_with_memory(
    bytes: &[u8],
    memory: &crate::read_image_memory::ImageMemoryHandle,
    ceiling: u64,
) -> Result<crate::direct_file_reader::DirectMetadataBody, ReadEncodingError> {
    // Canonical fields now admit their exact text/vector destinations before
    // allocation. There is no separate wire-length-multiple container bank.
    let result = memory.scope(|| {
        let mut reader = CanonicalReader::with_memory(bytes, memory.clone());
        let body = crate::read_image::read_metadata_body(&mut reader)?;
        reader.finish()?;
        Ok(body)
    });
    result.map_err(|error| match error {
        ReadEncodingError::MemoryCeilingExceeded { .. } => {
            ReadEncodingError::MemoryCeilingExceeded { ceiling }
        }
        ReadEncodingError::Admission(error) => encoding_error_from_core(error, ceiling),
        error => error,
    })
}

/// A cursor over canonical bytes, reading exactly what [`CanonicalWriter`]
/// wrote.
///
/// Every read is bounds-checked against what is actually left, so a length
/// prefix taken from the payload can never be trusted into an allocation or a
/// slice: a claim the bytes do not back is refused here, not acted on.
pub struct CanonicalReader<'a> {
    bytes: &'a [u8],
    at: usize,
    memory: Option<crate::read_image_memory::ImageMemoryHandle>,
}

impl<'a> CanonicalReader<'a> {
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            at: 0,
            memory: None,
        }
    }
    pub(crate) fn with_memory(
        bytes: &'a [u8],
        memory: crate::read_image_memory::ImageMemoryHandle,
    ) -> Self {
        Self {
            bytes,
            at: 0,
            memory: Some(memory),
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector<T>(&self, count: usize) -> Result<Vec<T>, ReadEncodingError> {
        let bytes = count
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(ReadEncodingError::InvalidPayload)?;
        self.admit(bytes)?;
        Ok(Vec::with_capacity(count))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn admit(&self, bytes: usize) -> Result<(), ReadEncodingError> {
        if let Some(memory) = &self.memory {
            memory
                .reserve(bytes)
                .map_err(ReadEncodingError::Admission)?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn take(&mut self, length: usize) -> Result<&'a [u8], ReadEncodingError> {
        let end = self
            .at
            .checked_add(length)
            .ok_or(ReadEncodingError::InvalidPayload)?;
        let taken = self
            .bytes
            .get(self.at..end)
            .ok_or(ReadEncodingError::InvalidPayload)?;
        self.at = end;
        Ok(taken)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn tag(&mut self) -> Result<u8, ReadEncodingError> {
        Ok(self.take(1)?[0])
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn count(&mut self) -> Result<u64, ReadEncodingError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| ReadEncodingError::InvalidPayload)?;
        Ok(u64::from_le_bytes(bytes))
    }

    /// A count that is about to become a number of elements to read. The
    /// bytes claim it; the payload has to be long enough to mean it, and the
    /// smallest element any of these documents holds is one byte.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn element_count(&mut self) -> Result<usize, ReadEncodingError> {
        let claimed =
            usize::try_from(self.count()?).map_err(|_| ReadEncodingError::InvalidPayload)?;
        if claimed > self.bytes.len().saturating_sub(self.at) {
            return Err(ReadEncodingError::InvalidPayload);
        }
        Ok(claimed)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn flag(&mut self) -> Result<bool, ReadEncodingError> {
        match self.tag()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ReadEncodingError::InvalidPayload),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn text(&mut self) -> Result<String, ReadEncodingError> {
        let length =
            usize::try_from(self.count()?).map_err(|_| ReadEncodingError::InvalidPayload)?;
        let bytes = self.take(length)?;
        let text = std::str::from_utf8(bytes).map_err(|_| ReadEncodingError::InvalidPayload)?;
        self.admit(bytes.len())?;
        Ok(text.to_owned())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn optional_text(&mut self) -> Result<Option<String>, ReadEncodingError> {
        match self.tag()? {
            0 => Ok(None),
            1 => Ok(Some(self.text()?)),
            _ => Err(ReadEncodingError::InvalidPayload),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn raw(&mut self) -> Result<&'a [u8], ReadEncodingError> {
        let length =
            usize::try_from(self.count()?).map_err(|_| ReadEncodingError::InvalidPayload)?;
        self.take(length)
    }

    /// A complete document ends where the bytes end: anything after it was
    /// never part of the answer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn finish(self) -> Result<(), ReadEncodingError> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(ReadEncodingError::InvalidPayload)
        }
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from direct_file_reader.rs"
    )
)]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(crate) fn encode_owned_cursor_page_for_test(
    page: &crate::owned_cursor_page::OwnedCursorPage,
) -> Result<Vec<u8>, ReadEncodingError> {
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    encode(page)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from read_contract_encoding.rs"
    )
)]
pub fn encode_cursor_page(
    page: &CursorPage,
    owner: contextdb_core::read_memory::ReadMemoryOwner,
) -> Result<EncodedReadBytes, ReadEncodingError> {
    let owner = page.encoding_owner().unwrap_or(owner);
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    EncodedReadBytes::from_funded(
        encode_transport_funded(page, Some(owner), u64::MAX)?,
        u64::MAX,
    )
}

/// See [`decode_query_result`]: the shipped default is the backstop for a
/// caller with no declared ceiling.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from owner_read_service_contract.rs"
    )
)]
pub fn decode_cursor_page(bytes: &[u8]) -> Result<CursorPage, ReadEncodingError> {
    decode_cursor_page_under_memory_ceiling(bytes, ReadLimits::SHIPPED_MEMORY)
}

/// Decode a cursor page against the memory ceiling in force for THIS
/// exchange.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from every_public_route_installs_its_own_memory_owner.rs"
    )
)]
pub fn decode_cursor_page_under_memory_ceiling(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<CursorPage, ReadEncodingError> {
    preflight_cursor_page_payload(bytes, memory_ceiling).map_err(encoding_error_from_transport)?;
    let (owner, memory) = crate::local_transport::query_funding::QueryMemory::new(memory_ceiling)
        .and_then(|memory| memory.into_cursor_owner())
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let prepared = crate::owned_cursor_page::OwnedCursorPage::try_prepare_public(owner.clone())
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let before = memory.held();
    let page: crate::owned_cursor_page::OwnedCursorPage =
        decode_exact_with_memory(bytes, &memory, memory_ceiling)?;
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    let credit = memory
        .take_retained_credit(memory.held() - before)
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let credit = crate::local_transport::query_funding::retain_cursor_credit(credit, owner.clone());
    let mut page = crate::local_transport::query_funding::Funded {
        value: page,
        credit: Some(credit),
    };
    if page.value.rows.capacity() != 0 {
        page.credit
            .as_mut()
            .expect("decoded page owns credit")
            .try_grow_for(
                crate::owned_cursor_page::SharedRows::allocation_header_bytes(),
                "reader",
                "publish_cursor_root",
                "Reduce retained pages or raise the reader memory limit.",
            )
            .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    }
    page.value
        .with_credit(page.credit.take().expect("decoded page owns credit"))
        .and_then(|mut page| {
            page.attach_publication(prepared);
            page.into_public()
        })
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from bounded_pull_execution_contract.rs"
    )
)]
pub fn cursor_page_encoded_size(page: &CursorPage) -> Result<usize, ReadEncodingError> {
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    encoded_size(page)
}

// Match MetadataPage/MetadataItem field and variant order while borrowing
// values through the same streaming JSON view as ordinary query answers.
#[derive(Serialize)]
struct MetadataPageView<'a> {
    vocabulary: contextdb_core::read_contract::MetadataPageVocabulary,
    items: MetadataItemsView<'a>,
    has_more: bool,
    continuation: Option<&'a str>,
}
struct MetadataItemsView<'a>(&'a [contextdb_core::read_contract::MetadataItem]);
impl Serialize for MetadataItemsView<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use contextdb_core::read_contract::MetadataItem;
        use serde::ser::SerializeSeq;
        #[derive(Serialize)]
        enum Item<'a> {
            Table(&'a str),
            EventStatus(QueryParamsView<'a>),
        }
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for item in self.0 {
            let view = match item {
                MetadataItem::Table(name) => Item::Table(name),
                MetadataItem::EventStatus(status) => Item::EventStatus(QueryParamsView(status)),
            };
            sequence.serialize_element(&view)?;
        }
        sequence.end()
    }
}
impl<'a> From<&'a MetadataPage> for MetadataPageView<'a> {
    fn from(page: &'a MetadataPage) -> Self {
        Self {
            vocabulary: page.vocabulary,
            items: MetadataItemsView(&page.items),
            has_more: page.has_more,
            continuation: page.continuation.as_deref(),
        }
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn encode_metadata_page_funded(
    page: &MetadataPage,
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    memory_ceiling: u64,
) -> Result<crate::local_transport::query_funding::Funded<Vec<u8>>, ReadEncodingError> {
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    encode_transport_funded(&MetadataPageView::from(page), Some(owner), memory_ceiling)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from read_contract_encoding.rs"
    )
)]
pub fn encode_metadata_page(
    page: &MetadataPage,
    owner: contextdb_core::read_memory::ReadMemoryOwner,
) -> Result<EncodedReadBytes, ReadEncodingError> {
    EncodedReadBytes::from_funded(
        encode_metadata_page_funded(page, owner, u64::MAX)?,
        u64::MAX,
    )
}

/// See [`decode_query_result`]: the shipped default is the backstop for a
/// caller with no declared ceiling.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn decode_metadata_page(bytes: &[u8]) -> Result<MetadataPage, ReadEncodingError> {
    decode_metadata_page_under_memory_ceiling(bytes, ReadLimits::SHIPPED_MEMORY)
}

/// Decode a metadata page against the memory ceiling in force for THIS
/// exchange.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn decode_metadata_page_under_memory_ceiling(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<MetadataPage, ReadEncodingError> {
    let memory = decoder_memory(memory_ceiling)
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    decode_metadata_page_with_memory(bytes, &memory, memory_ceiling)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn decode_metadata_page_with_memory(
    bytes: &[u8],
    memory: &crate::read_image_memory::ImageMemoryHandle,
    memory_ceiling: u64,
) -> Result<MetadataPage, ReadEncodingError> {
    preflight_metadata_page_payload(bytes, memory_ceiling)
        .map_err(encoding_error_from_transport)?;
    let page: MetadataPage = decode_exact_with_memory(bytes, memory, memory_ceiling)?;
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    Ok(page)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from read_contract_encoding.rs"
    )
)]
pub fn metadata_page_encoded_size(page: &MetadataPage) -> Result<usize, ReadEncodingError> {
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    encoded_size(&MetadataPageView::from(page))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn validate_query_result(result: &CanonicalQueryResult) -> Result<(), ReadEncodingError> {
    for row in &result.rows {
        if row.len() != result.columns.len() {
            return Err(ReadEncodingError::InvalidPayload);
        }
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn encoded_size<T: Serialize>(value: &T) -> Result<usize, ReadEncodingError> {
    let mut measured = bincode::enc::write::SizeWriter::default();
    contextdb_core::types::with_borrowed_json_wire(|| {
        bincode::serde::encode_into_writer(value, &mut measured, standard())
    })
    .map_err(|_| ReadEncodingError::InvalidPayload)?;
    Ok(measured.bytes_written)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature"
    )
)]
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ReadEncodingError> {
    // Measured first and written once into a buffer of exactly that length.
    // Growing the output by doubling would reserve up to twice a large answer
    // and briefly hold the old and new buffers together while it copies.
    let mut measured = bincode::enc::write::SizeWriter::default();
    contextdb_core::types::with_borrowed_json_wire(|| {
        bincode::serde::encode_into_writer(value, &mut measured, standard())
    })
    .map_err(|_| ReadEncodingError::InvalidPayload)?;
    let mut encoded = Vec::with_capacity(measured.bytes_written);
    contextdb_core::types::with_borrowed_json_wire(|| {
        bincode::serde::encode_into_std_write(value, &mut encoded, standard())
    })
    .map_err(|_| ReadEncodingError::InvalidPayload)?;
    Ok(encoded)
}

/// The standalone codec retains its original factory in its typed image
/// capability, including through escaping page and decoded-payload aliases.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn decoder_memory(
    memory_ceiling: u64,
) -> contextdb_core::Result<crate::read_image_memory::ImageMemoryHandle> {
    Ok(crate::local_transport::query_funding::QueryMemory::new(memory_ceiling)?.image())
}

pub(crate) fn encoding_error_from_core(
    error: contextdb_core::Error,
    memory_ceiling: u64,
) -> ReadEncodingError {
    match error {
        error if crate::read_image_memory::is_memory_refusal(&error) => {
            ReadEncodingError::MemoryCeilingExceeded {
                ceiling: if memory_ceiling == u64::MAX {
                    crate::local_transport::query_funding::memory_refusal_ceiling(&error)
                        .unwrap_or(memory_ceiling)
                } else {
                    memory_ceiling
                },
            }
        }
        _ => ReadEncodingError::InvalidPayload,
    }
}

/// Borrowed wire input coexists with all decoded buffers. This credit is
/// temporary; payload credit stays on the same owner until its DTO boundary.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn decode_exact_with_memory<T: DeserializeOwned>(
    bytes: &[u8],
    memory: &crate::read_image_memory::ImageMemoryHandle,
    memory_ceiling: u64,
) -> Result<T, ReadEncodingError> {
    let input = contextdb_core::read_memory::ReadCredit::try_new_owned(
        memory.owner(),
        bytes.len(),
        "reader",
        "decode_input",
        "Reduce the payload or raise the declared reader memory limit.",
    )
    .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let before = memory.held();
    let (value, consumed) = memory
        .scope(|| {
            crate::read_image_memory::decode(
                &crate::read_image_memory::ImageMemory::enter_decode_call()?,
                bytes,
            )
        })
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    if consumed != bytes.len() {
        drop(value);
        memory.release(memory.held() - before);
        return Err(ReadEncodingError::InvalidPayload);
    }
    drop(input);
    Ok(value)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn decode_query_result_with_memory(
    bytes: &[u8],
    memory: &crate::read_image_memory::ImageMemoryHandle,
    memory_ceiling: u64,
) -> Result<CanonicalQueryResult, ReadEncodingError> {
    #[cfg(feature = "test-seams")]
    crate::local_transport::OrdinaryResultReceiver::observe_actual_canonical_decode_entry_for_test(
        bytes,
    );
    preflight_canonical_query_result(bytes, memory_ceiling)?;
    let result = decode_exact_with_memory(bytes, memory, memory_ceiling)?;
    validate_query_result(&result)?;
    Ok(result)
}

#[cfg(test)]
#[path = "read_contract/response_encoding_proofs.rs"]
mod response_encoding_proofs;
