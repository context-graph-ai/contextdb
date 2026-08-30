//! Canonical bounded-read payload boundary.
//!
//! The encoder is intentionally route-neutral: it accepts query and page values, never a file
//! handle, owner channel, or route. The local protocol layer owns framing and byte-level fixtures.

use crate::local_transport::{
    preflight_canonical_query_result, preflight_cursor_page_payload,
    preflight_metadata_page_payload,
};
use crate::{QueryResult, QueryTrace};
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use contextdb_core::read_contract::{CursorPage, MetadataPage, ReadLimits};
use contextdb_core::{Value, VectorIndexRef};
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
    pub rows_examined: u64,
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
            rows_examined: trace.rows_examined,
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
            crate::local_transport::PayloadViolation::MemoryCeilingExceeded { ceiling },
        ) => ReadEncodingError::MemoryCeilingExceeded { ceiling },
        _ => ReadEncodingError::InvalidPayload,
    }
}

pub fn encode_query_result(result: &QueryResult) -> Result<Vec<u8>, ReadEncodingError> {
    let canonical = CanonicalQueryResult::from(result);
    validate_query_result(&canonical)?;
    encode(&canonical)
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
    let result = decode_exact(bytes)?;
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

/// A deterministic, self-delimiting byte writer for canonical documents.
#[derive(Default)]
pub struct CanonicalWriter {
    bytes: Vec<u8>,
}

impl CanonicalWriter {
    pub fn tag(&mut self, tag: u8) -> &mut Self {
        self.bytes.push(tag);
        self
    }

    pub fn count(&mut self, value: u64) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub fn flag(&mut self, value: bool) -> &mut Self {
        self.tag(u8::from(value))
    }

    pub fn text(&mut self, text: &str) -> &mut Self {
        self.count(text.len() as u64);
        self.bytes.extend_from_slice(text.as_bytes());
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
        self.bytes.extend_from_slice(bytes);
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
    let mut reader = CanonicalReader::new(bytes);
    let body = crate::read_image::read_metadata_body(&mut reader)?;
    reader.finish()?;
    Ok(body)
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
    let page: CursorPage = decode_exact(bytes)?;
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
    let page: MetadataPage = decode_exact(bytes)?;
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
    encode_to_vec(value, standard()).map_err(|_| ReadEncodingError::InvalidPayload)
}

/// The decoder's own limit counts wire reads, and counts each integer field at
/// its fixed width rather than the shorter form actually consumed, so it is
/// stated at the decoder's worst case per admitted wire byte. What the decoded
/// form costs in live memory is charged by the preflight each caller runs
/// above, before the decoder is entered.
const DECODE_WIRE_CLAIM_CEILING: usize = crate::local_transport::MAX_FRAME_BYTES * 8;

fn decode_exact<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ReadEncodingError> {
    let configuration = standard().with_limit::<DECODE_WIRE_CLAIM_CEILING>();
    let (value, consumed) =
        decode_from_slice(bytes, configuration).map_err(|_| ReadEncodingError::InvalidPayload)?;
    if consumed != bytes.len() {
        return Err(ReadEncodingError::InvalidPayload);
    }
    Ok(value)
}
