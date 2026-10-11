//! Canonical bounded-read payload boundary.
//!
//! The encoder is intentionally route-neutral: it accepts query and page values, never a file
//! handle, owner channel, or route. The local protocol layer owns framing and byte-level fixtures.

pub(crate) mod cursor_observation;
pub(crate) use cursor_observation::{decode_observed_cursor_page, encode_observed_cursor_page};

use crate::local_transport::{
    preflight_canonical_query_result, preflight_cursor_page_payload,
    preflight_metadata_page_payload,
};
use crate::{
    QueryResult, QueryTrace, VectorPartitionHnswDisclosure, VectorQuerySourceDisclosure,
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
pub struct CanonicalVectorPartitionHnswDisclosure {
    pub partition: String,
    pub route: Option<String>,
    pub reason: Option<String>,
    pub hnsw_m: u64,
    pub hnsw_ef_construction: u64,
    pub hnsw_ef_search: u64,
    pub ef_search_source: String,
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
    pub partition_hnsw: Vec<CanonicalVectorPartitionHnswDisclosure>,
    #[serde(default)]
    pub global_candidate_budget: Option<u64>,
    #[serde(default)]
    pub admission_evaluations: Option<u64>,
    #[serde(default)]
    pub expansion_evaluations: Option<u64>,
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
            layers_admitted: disclosure.layers_admitted.map(|count| count as u64),
            budget_terminal: disclosure.budget_terminal.clone(),
            partition_hnsw: disclosure
                .partition_hnsw
                .iter()
                .map(|partition| CanonicalVectorPartitionHnswDisclosure {
                    partition: partition.partition.clone(),
                    route: partition.route.map(|route| route.as_str().to_owned()),
                    reason: partition.reason.clone(),
                    hnsw_m: partition.hnsw_m as u64,
                    hnsw_ef_construction: partition.hnsw_ef_construction as u64,
                    hnsw_ef_search: partition.hnsw_ef_search as u64,
                    ef_search_source: partition.ef_search_source.clone(),
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
            layers_admitted: disclosure
                .layers_admitted
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ())?,
            budget_terminal: disclosure.budget_terminal,
            partition_hnsw: {
                let mut partitions = Vec::with_capacity(disclosure.partition_hnsw.len());
                for partition in disclosure.partition_hnsw {
                    partitions.push(VectorPartitionHnswDisclosure {
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
                        hnsw_m: usize::try_from(partition.hnsw_m).map_err(|_| ())?,
                        hnsw_ef_construction: usize::try_from(partition.hnsw_ef_construction)
                            .map_err(|_| ())?,
                        hnsw_ef_search: usize::try_from(partition.hnsw_ef_search)
                            .map_err(|_| ())?,
                        ef_search_source: partition.ef_search_source,
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
            vector_search: trace.vector_search.as_ref().map(Into::into),
            rows_examined: trace.rows_examined,
            groups: trace.groups.to_vec(),
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
    pub rows: &'result [Vec<Value>],
    pub rows_affected: u64,
    pub trace: CanonicalQueryTrace,
    pub cascade: Option<CanonicalCascadeReport>,
}

impl<'result> From<&'result QueryResult> for CanonicalQueryResultView<'result> {
    fn from(result: &'result QueryResult) -> Self {
        Self {
            columns: &result.columns,
            rows: &result.rows,
            rows_affected: result.rows_affected,
            trace: CanonicalQueryTrace::from(&result.trace),
            cascade: result
                .cascade
                .as_ref()
                .map(|cascade| CanonicalCascadeReport {
                    dropped_indexes: cascade.dropped_indexes.clone(),
                }),
        }
    }
}

impl From<&QueryResult> for CanonicalQueryResult {
    fn from(result: &QueryResult) -> Self {
        Self {
            columns: result.columns.clone(),
            rows: result.rows.clone(),
            rows_affected: result.rows_affected,
            trace: CanonicalQueryTrace::from(&result.trace),
            cascade: result
                .cascade
                .as_ref()
                .map(|cascade| CanonicalCascadeReport {
                    dropped_indexes: cascade.dropped_indexes.clone(),
                }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadEncodingError {
    #[error("canonical read encoding is not implemented")]
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

pub fn encode_query_result(result: &QueryResult) -> Result<Vec<u8>, ReadEncodingError> {
    // Encoded from the borrowed view, which writes the same bytes as the owned
    // canonical form: building the owned form first would copy every row of
    // the answer only to hand the copy to the encoder.
    for row in &result.rows {
        if row.len() != result.columns.len() {
            return Err(ReadEncodingError::InvalidPayload);
        }
    }
    encode(&CanonicalQueryResultView::from(result))
}

/// Decode with no caller ceiling in hand. The shipped default stands in as
/// the backstop, which is what a caller that never declared one gets.
pub fn decode_query_result(bytes: &[u8]) -> Result<CanonicalQueryResult, ReadEncodingError> {
    decode_query_result_under_memory_ceiling(bytes, ReadLimits::SHIPPED_MEMORY)
}

/// Decode against the memory ceiling actually in force for THIS exchange.
/// A caller that declared a tighter or wider envelope is held to its own
/// number rather than to the shipped backstop, so a decode refused here names
/// the ceiling that caller set and can raise.
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

pub fn query_result_encoded_size(result: &QueryResult) -> Result<usize, ReadEncodingError> {
    Ok(encode_query_result(result)?.len())
}

/// Canonical bytes for one metadata answer.
///
/// Metadata travels the same way ordinary results do, so it needs the same
/// property: two routes answering the same question publish the same bytes.
/// The reading vocabulary carries no serialization derives -- its module is
/// closed over a small set of dependencies on purpose -- so the shape is
/// written out here, field by field, length-prefixed so no two shapes can
/// render alike.
pub fn encode_metadata_body(
    body: &crate::direct_file_reader::DirectMetadataBody,
) -> Result<Vec<u8>, ReadEncodingError> {
    let mut bytes = CanonicalWriter::default();
    crate::read_image::write_metadata_body(&mut bytes, body);
    Ok(bytes.finish())
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

/// [`encode_metadata_body`] into a buffer allocated once at `len`, the size
/// [`encoded_metadata_body_len`] measured and the caller admitted.
pub(crate) fn encode_metadata_body_measured(
    body: &crate::direct_file_reader::DirectMetadataBody,
    len: usize,
) -> Vec<u8> {
    let mut bytes = CanonicalWriter {
        bytes: Vec::with_capacity(len),
        len: 0,
        counting: false,
    };
    crate::read_image::write_metadata_body(&mut bytes, body);
    #[cfg(feature = "test-seams")]
    crate::read_image::answer_encoded_for_test();
    bytes.finish()
}

/// A deterministic, self-delimiting byte writer for canonical documents.
#[derive(Default)]
pub struct CanonicalWriter {
    bytes: Vec<u8>,
    len: usize,
    /// Measure only: count what would be written and keep none of it.
    counting: bool,
}

impl CanonicalWriter {
    /// A writer that measures a document's encoded length without keeping
    /// its bytes.
    pub fn counting() -> Self {
        Self {
            bytes: Vec::new(),
            len: 0,
            counting: true,
        }
    }

    /// The bytes written, or counted, so far.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn put(&mut self, bytes: &[u8]) {
        self.len = self.len.saturating_add(bytes.len());
        if !self.counting {
            self.bytes.extend_from_slice(bytes);
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

    pub fn optional_text(&mut self, text: Option<&str>) -> &mut Self {
        match text {
            Some(text) => {
                self.tag(1);
                self.text(text)
            }
            None => self.tag(0),
        }
    }

    pub fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.count(bytes.len() as u64);
        self.put(bytes);
        self
    }

    pub fn finish(self) -> Vec<u8> {
        self.bytes
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
pub fn decode_metadata_body(
    bytes: &[u8],
) -> Result<crate::direct_file_reader::DirectMetadataBody, ReadEncodingError> {
    let memory = decoder_memory(ReadLimits::SHIPPED_MEMORY)
        .map_err(|error| encoding_error_from_core(error, ReadLimits::SHIPPED_MEMORY))?;
    decode_metadata_body_with_memory(bytes, &memory, ReadLimits::SHIPPED_MEMORY)
}

pub(crate) fn decode_metadata_body_with_memory(
    bytes: &[u8],
    memory: &std::sync::Arc<crate::read_image_memory::ImageMemory>,
    ceiling: u64,
) -> Result<crate::direct_file_reader::DirectMetadataBody, ReadEncodingError> {
    let bound = bytes
        .len()
        .checked_mul(4)
        .and_then(|n| n.checked_add(4096))
        .ok_or(ReadEncodingError::MemoryCeilingExceeded { ceiling })?;
    let transient = contextdb_core::read_memory::ReadCredit::try_new(
        memory.clone(),
        bound,
        "reader",
        "metadata_body",
        "Raise reader memory to decode this metadata body.",
    )
    .map_err(|error| encoding_error_from_core(error, ceiling))?;
    let result = memory.scope(|| {
        let mut reader = CanonicalReader::new(bytes);
        let body = crate::read_image::read_metadata_body(&mut reader)?;
        reader.finish()?;
        Ok(body)
    });
    // The body keeps its admitted JSON payload on this same owner; its
    // construction-only input/container upper bound ends here.
    drop(transient);
    result.map_err(|error| match error {
        ReadEncodingError::MemoryCeilingExceeded { .. } => {
            ReadEncodingError::MemoryCeilingExceeded { ceiling }
        }
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
}

impl<'a> CanonicalReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

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

    pub fn tag(&mut self) -> Result<u8, ReadEncodingError> {
        Ok(self.take(1)?[0])
    }

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
    pub fn element_count(&mut self) -> Result<usize, ReadEncodingError> {
        let claimed =
            usize::try_from(self.count()?).map_err(|_| ReadEncodingError::InvalidPayload)?;
        if claimed > self.bytes.len().saturating_sub(self.at) {
            return Err(ReadEncodingError::InvalidPayload);
        }
        Ok(claimed)
    }

    pub fn flag(&mut self) -> Result<bool, ReadEncodingError> {
        match self.tag()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ReadEncodingError::InvalidPayload),
        }
    }

    pub fn text(&mut self) -> Result<String, ReadEncodingError> {
        let length =
            usize::try_from(self.count()?).map_err(|_| ReadEncodingError::InvalidPayload)?;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| ReadEncodingError::InvalidPayload)
    }

    pub fn optional_text(&mut self) -> Result<Option<String>, ReadEncodingError> {
        match self.tag()? {
            0 => Ok(None),
            1 => Ok(Some(self.text()?)),
            _ => Err(ReadEncodingError::InvalidPayload),
        }
    }

    pub fn raw(&mut self) -> Result<&'a [u8], ReadEncodingError> {
        let length =
            usize::try_from(self.count()?).map_err(|_| ReadEncodingError::InvalidPayload)?;
        self.take(length)
    }

    /// A complete document ends where the bytes end: anything after it was
    /// never part of the answer.
    pub fn finish(self) -> Result<(), ReadEncodingError> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(ReadEncodingError::InvalidPayload)
        }
    }
}

pub fn encode_cursor_page(page: &CursorPage) -> Result<Vec<u8>, ReadEncodingError> {
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    encode(page)
}

/// See [`decode_query_result`]: the shipped default is the backstop for a
/// caller with no declared ceiling.
pub fn decode_cursor_page(bytes: &[u8]) -> Result<CursorPage, ReadEncodingError> {
    decode_cursor_page_under_memory_ceiling(bytes, ReadLimits::SHIPPED_MEMORY)
}

/// Decode a cursor page against the memory ceiling in force for THIS
/// exchange.
pub fn decode_cursor_page_under_memory_ceiling(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<CursorPage, ReadEncodingError> {
    preflight_cursor_page_payload(bytes, memory_ceiling).map_err(encoding_error_from_transport)?;
    let memory = decoder_memory(memory_ceiling)
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let page: CursorPage = decode_exact_with_memory(bytes, &memory, memory_ceiling)?;
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    Ok(page)
}

pub fn cursor_page_encoded_size(page: &CursorPage) -> Result<usize, ReadEncodingError> {
    Ok(encode_cursor_page(page)?.len())
}

pub fn encode_metadata_page(page: &MetadataPage) -> Result<Vec<u8>, ReadEncodingError> {
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    encode(page)
}

/// See [`decode_query_result`]: the shipped default is the backstop for a
/// caller with no declared ceiling.
pub fn decode_metadata_page(bytes: &[u8]) -> Result<MetadataPage, ReadEncodingError> {
    decode_metadata_page_under_memory_ceiling(bytes, ReadLimits::SHIPPED_MEMORY)
}

/// Decode a metadata page against the memory ceiling in force for THIS
/// exchange.
pub fn decode_metadata_page_under_memory_ceiling(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<MetadataPage, ReadEncodingError> {
    preflight_metadata_page_payload(bytes, memory_ceiling)
        .map_err(encoding_error_from_transport)?;
    let memory = decoder_memory(memory_ceiling)
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let page: MetadataPage = decode_exact_with_memory(bytes, &memory, memory_ceiling)?;
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    Ok(page)
}

pub fn metadata_page_encoded_size(page: &MetadataPage) -> Result<usize, ReadEncodingError> {
    Ok(encode_metadata_page(page)?.len())
}

fn validate_query_result(result: &CanonicalQueryResult) -> Result<(), ReadEncodingError> {
    for row in &result.rows {
        if row.len() != result.columns.len() {
            return Err(ReadEncodingError::InvalidPayload);
        }
    }
    Ok(())
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ReadEncodingError> {
    // Measured first and written once into a buffer of exactly that length.
    // Growing the output by doubling would reserve up to twice a large answer
    // and briefly hold the old and new buffers together while it copies.
    let mut measured = bincode::enc::write::SizeWriter::default();
    bincode::serde::encode_into_writer(value, &mut measured, standard())
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    let mut encoded = Vec::with_capacity(measured.bytes_written);
    bincode::serde::encode_into_std_write(value, &mut encoded, standard())
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    Ok(encoded)
}

/// Use the existing declared reader root as the standalone codec's host
/// capability boundary. Every engine-created native control is admitted to
/// its operation child before the corresponding Arc allocations.
pub(crate) fn decoder_memory(
    memory_ceiling: u64,
) -> contextdb_core::Result<std::sync::Arc<crate::read_image_memory::ImageMemory>> {
    let root = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(memory_ceiling);
    let child = root.child()?;
    let controls = std::mem::size_of::<crate::memory_accounting::MemoryAccountant>()
        + 2 * std::mem::size_of::<usize>()
        + 2 * (std::mem::size_of::<std::sync::atomic::AtomicUsize>()
            + 2 * std::mem::size_of::<usize>());
    child.try_reserve(
        controls,
        "reader",
        "decode_accountant",
        "Reduce the payload or raise the declared reader memory limit.",
    )?;
    let accountant = std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        usize::try_from(memory_ceiling).unwrap_or(usize::MAX),
    ));
    let memory =
        match crate::read_image_memory::ImageMemory::operation_reader(accountant.into(), child.clone()) {
            Ok(memory) => memory,
            Err(error) => {
                child.release(controls);
                return Err(error);
            }
        };
    memory.adopt_reader_credit(&child, controls)?;
    Ok(memory)
}

pub(crate) fn encoding_error_from_core(
    error: contextdb_core::Error,
    memory_ceiling: u64,
) -> ReadEncodingError {
    match error {
        error if crate::read_image_memory::is_memory_refusal(&error) => {
            ReadEncodingError::MemoryCeilingExceeded {
                ceiling: memory_ceiling,
            }
        }
        _ => ReadEncodingError::InvalidPayload,
    }
}

/// Borrowed wire input coexists with all decoded buffers. This credit is
/// temporary; payload credit stays on the same owner until its DTO boundary.
pub(crate) fn decode_exact_with_memory<T: DeserializeOwned>(
    bytes: &[u8],
    memory: &std::sync::Arc<crate::read_image_memory::ImageMemory>,
    memory_ceiling: u64,
) -> Result<T, ReadEncodingError> {
    let input = contextdb_core::read_memory::ReadCredit::try_new(
        memory.clone(),
        bytes.len(),
        "reader",
        "decode_input",
        "Reduce the payload or raise the declared reader memory limit.",
    )
    .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    let before = memory.held();
    let (value, consumed) = memory
        .scope(|| crate::read_image_memory::decode(bytes))
        .map_err(|error| encoding_error_from_core(error, memory_ceiling))?;
    if consumed != bytes.len() {
        drop(value);
        memory.release(memory.held() - before);
        return Err(ReadEncodingError::InvalidPayload);
    }
    drop(input);
    Ok(value)
}

pub(crate) fn decode_query_result_with_memory(
    bytes: &[u8],
    memory: &std::sync::Arc<crate::read_image_memory::ImageMemory>,
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
