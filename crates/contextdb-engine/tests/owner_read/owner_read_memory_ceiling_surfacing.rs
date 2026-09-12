//! A caller that crosses their OWN declared memory ceiling must be told so.
//!
//! The wire decoder already separates the two answers: a payload that is
//! exactly what it claims to be and simply too large to decode inside the
//! declared ceiling is a ceiling crossing, and bytes that are not a payload at
//! all are invalid channel content. The owner boundary is where a caller
//! actually reads that answer, and a ceiling crossing that arrives there as
//! invalid channel data with nothing said tells an operator the owner is
//! sending garbage when the ceiling they themselves declared is the thing that
//! was reached -- so they cannot act on it, and cannot raise it.

use contextdb_core::read_contract::{
    ReadFailure, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_engine::local_transport::{
    FrameBufferAllocator, FrameReader, LocalTransportError, MAX_FRAME_BYTES,
};
use contextdb_engine::owner_read::{OwnerClient, OwnerReadScaffoldError};

/// Room for the fixed request preamble ahead of the parameter text.
const REQUEST_PREAMBLE_RESERVE: usize = 512;

/// A reader that hands out one already-framed payload exactly as the carrier
/// delivers it: the fixed length prefix first, then the payload bytes.
struct FramedSlice {
    bytes: Vec<u8>,
    offset: usize,
}

impl FramedSlice {
    fn new(payload: &[u8]) -> Self {
        let mut bytes = u32::try_from(payload.len())
            .expect("a framed payload length fits the prefix")
            .to_be_bytes()
            .to_vec();
        bytes.extend_from_slice(payload);
        Self { bytes, offset: 0 }
    }
}

impl FrameReader for FramedSlice {
    fn read_exact(&mut self, destination: &mut [u8]) -> Result<(), LocalTransportError> {
        let end = self.offset.saturating_add(destination.len());
        let source = self
            .bytes
            .get(self.offset..end)
            .expect("the fixture delivers every byte the boundary asks for");
        destination.copy_from_slice(source);
        self.offset = end;
        Ok(())
    }
}

struct PlainAllocator;

impl FrameBufferAllocator for PlainAllocator {
    fn allocate(&mut self, length: usize) -> Result<Vec<u8>, LocalTransportError> {
        Ok(vec![0; length])
    }
}

fn push_unsigned(bytes: &mut Vec<u8>, value: u64) {
    if value <= 250 {
        bytes.push(u8::try_from(value).expect("a small length is one byte"));
    } else if let Ok(narrow) = u16::try_from(value) {
        bytes.push(251);
        bytes.extend_from_slice(&narrow.to_le_bytes());
    } else if let Ok(narrow) = u32::try_from(value) {
        bytes.push(252);
        bytes.extend_from_slice(&narrow.to_le_bytes());
    } else {
        bytes.push(253);
        bytes.extend_from_slice(&value.to_le_bytes());
    }
}

fn push_length(bytes: &mut Vec<u8>, value: usize) {
    push_unsigned(
        bytes,
        u64::try_from(value).expect("a length fits an unsigned word"),
    );
}

/// A query request carrying one well-formed JSON parameter whose text is
/// supplied verbatim, exactly as a peer would put it on the wire.
fn json_parameter_request_bytes(json_text: &[u8]) -> Vec<u8> {
    let limits = ReadLimits::default();
    let mut bytes = Vec::new();
    for field in [
        limits.result_rows,
        limits.result_bytes,
        limits.work,
        limits.active_ms,
        limits.memory,
        limits.cursor_page_rows,
        limits.cursor_page_bytes,
        limits.cursor_idle_ms,
        limits.cursor_lifetime_ms,
    ] {
        push_unsigned(&mut bytes, field);
    }
    bytes.push(0);
    push_length(&mut bytes, 0);
    push_length(&mut bytes, 1);
    push_length(&mut bytes, 1);
    bytes.push(b'p');
    bytes.push(7);
    push_length(&mut bytes, json_text.len());
    bytes.extend_from_slice(json_text);
    bytes
}

fn json_zero_array_text(elements: usize) -> Vec<u8> {
    let mut text = Vec::with_capacity(elements * 2 + 1);
    text.push(b'[');
    for element in 0..elements {
        if element > 0 {
            text.push(b',');
        }
        text.push(b'0');
    }
    text.push(b']');
    text
}

/// The refusal a caller actually receives at the owner boundary.
fn surfaced_refusal(payload: &[u8], expectation: &str) -> ReadFailure {
    let mut reader = FramedSlice::new(payload);
    let mut allocator = PlainAllocator;
    match OwnerClient::read_envelope(&mut reader, &mut allocator) {
        Err(OwnerReadScaffoldError::Refused(failure)) => failure,
        other => panic!("{expectation}, got {other:?}"),
    }
}

/// Whether this refusal says the memory ceiling is what was reached, and
/// -- when it says so with the typed budget document -- names the exact
/// ceiling actually in force rather than some other number. Either shape
/// counts as "says so": the typed budget document that names the memory
/// limit, or prose that says so in words. What does NOT count is a refusal
/// that says nothing at all (what a caller was handed before this refusal
/// was surfaced), or a typed document that names the memory limit but the
/// wrong number: a caller cannot raise a ceiling that was misreported.
fn names_the_memory_ceiling(failure: &ReadFailure, ceiling_in_force: u64) -> bool {
    match failure.detail() {
        ReadFailureDetail::OwnerLimitExceeded(detail) => {
            detail.limit == ReadFailureLimit::Memory && detail.value == ceiling_in_force
        }
        ReadFailureDetail::Reason { reason } => {
            let reason = reason.to_lowercase();
            reason.contains("memory") && (reason.contains("ceiling") || reason.contains("limit"))
        }
        _ => false,
    }
}

/// The two answers must not arrive as the same document, and the ceiling
/// crossing must name the ceiling that was crossed.
#[test]
fn a_decode_that_crosses_the_memory_ceiling_surfaces_a_refusal_naming_that_ceiling() {
    let elements = (MAX_FRAME_BYTES - REQUEST_PREAMBLE_RESERVE) / 2;
    let oversized = json_parameter_request_bytes(&json_zero_array_text(elements));
    assert!(
        oversized.len() <= MAX_FRAME_BYTES,
        "the payload must sit inside the wire ceiling to be admitted at all, so the memory \
         ceiling is the only thing it crosses"
    );

    let ceiling_crossing = surfaced_refusal(
        &oversized,
        "a payload whose decoded form crosses the declared memory ceiling is refused at the \
         owner boundary",
    );
    let not_a_payload = surfaced_refusal(
        &[0xFF_u8; 24],
        "bytes that are not a payload at all are refused at the owner boundary",
    );

    assert_ne!(
        ceiling_crossing, not_a_payload,
        "a caller who crossed their own declared memory ceiling and a caller whose owner sent \
         unreadable bytes receive the same refusal, {ceiling_crossing:?}, so the caller cannot \
         tell a ceiling they set from a peer that is broken"
    );
    assert!(
        names_the_memory_ceiling(&ceiling_crossing, ReadLimits::default().memory),
        "the refusal a caller receives for crossing the declared memory ceiling says nothing \
         about that ceiling, or names the wrong number: {ceiling_crossing:?}. A budget refusal \
         names the ceiling it exceeded, so the caller can raise it"
    );
    assert_eq!(
        (not_a_payload.kind(), not_a_payload.detail().clone()),
        (ReadFailureKind::InvalidChannelData, ReadFailureDetail::None),
        "guard: bytes that are not a payload stay the empty invalid-channel-data refusal; if \
         that changed, the comparison above no longer proves what it claims"
    );
}
