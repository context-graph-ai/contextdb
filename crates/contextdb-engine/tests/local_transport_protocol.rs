use contextdb_core::read_contract::{
    DatabaseIdentity, LocalUserIdentity, OwnerReadLimits, OwnerReadStatus, OwnerServiceTimeouts,
    OwnerServingReason, OwnerServingState, ReadFailure, ReadFailureDetail, ReadFailureKind,
    ReadLimits, WriterRunNumber,
};
use contextdb_core::{TxId, Value};
use contextdb_engine::local_transport::{
    CursorCloseAcknowledgement, CursorOpenedResponse, CursorPageResponse, FrameBufferAllocator,
    FrameReader, FrameViolation, LocalConfigurationSource, LocalEffectiveLimits, LocalHandshake,
    LocalInboundKind, LocalInboundMessage, LocalMetadataRequest, LocalOutboundMessage,
    LocalOwnerStatusResponse, LocalOwnerTimeouts, LocalProtocolBoundary, LocalRequest,
    LocalRequestEnvelope, LocalResponse, LocalResponseExpectation, LocalTransportError,
    MAX_FRAME_BYTES, MetadataResponse, OwnerAdmissionCounters, OwnerMemoryCounters,
    PayloadViolation, ResultChunk, TerminalSuccess, decode_handshake_exact, decode_message_exact,
    encode_message,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::num::NonZeroU64;

const HANDSHAKE_FIXTURE: &[u8] = &[
    0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x64, 0x62, 0x2d, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x2d,
    0x72, 0x65, 0x61, 0x64, 0x02, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
    0x11, 0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
    0x22, 0x22, 0x22, 0x22, 0x22, 0x07, 0x00,
];

// These bytes were transcribed from bincode 2's standard integer grammar,
// independently of the transport encoder. Every page ceiling is legal with
// respect to its corresponding result ceiling, and every field exercises the
// multi-byte integer form.
const LIMITS_FIXTURE: &[u8] = &[
    0xfb, 0xf4, 0x01, 0xfb, 0x00, 0x10, 0xfb, 0x88, 0x13, 0xfb, 0xe8, 0x03, 0xfb, 0x00, 0x20, 0xfb,
    0x2c, 0x01, 0xfb, 0x00, 0x04, 0xfb, 0x58, 0x02, 0xfb, 0xb0, 0x04,
];
const QUERY_BODY: &[u8] = &[0, 1, b'Q', 1, 1, b'p', 9, 42];
const CURSOR_OPEN_BODY: &[u8] = &[1, 1, b'O', 1, 1, b'b', 1, 1];
const CURSOR_FETCH_DEFAULT_BODY: &[u8] = &[
    2, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    0x44, 0,
];
const CURSOR_FETCH_ROWS_BODY: &[u8] = &[
    2, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    0x44, 1, 0xfb, 0x2c, 0x01,
];
const CURSOR_FETCH_ZERO_ROWS_BODY: &[u8] = &[
    2, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    0x44, 1, 0,
];
const CURSOR_CLOSE_BODY: &[u8] = &[
    3, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    0x44,
];
const TABLES_NONE_BODY: &[u8] = &[4, 0, 0];
const TABLES_SOME_BODY: &[u8] = &[4, 0, 1, 1, b'c'];
const SCHEMA_BODY: &[u8] = &[4, 1, 1, b't'];
const EVENTS_NONE_BODY: &[u8] = &[4, 2, 0];
const EVENTS_SOME_BODY: &[u8] = &[4, 2, 1, 1, b'c'];
const MAINTENANCE_BODY: &[u8] = &[4, 3];
const EXPLAIN_BODY: &[u8] = &[5, 1, b'E', 1, 1, b'n', 4, 1, b'v'];
const OWNER_STATUS_REQUEST_BODY: &[u8] = &[6];
const CUSTOM_BODY: &[u8] = &[7, 1, b'n', 2, 0x5a, 0x5b];

const RESULT_CHUNK_FIXTURE: &[u8] = &[0, 2, 0xc1, 0xc2];
const TERMINAL_SUCCESS_FIXTURE: &[u8] = &[1, 1, 0xc3];
const CANONICAL_CURSOR_PAGE: &[u8] = &[2, 1, b'i', 1, b't', 1, 2, 2, 2, 9, 42, 1];
const CURSOR_OPENED_FIXTURE: &[u8] = &[
    2, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    0x44, 12, 2, 1, b'i', 1, b't', 1, 2, 2, 2, 9, 42, 1,
];
const CURSOR_PAGE_FIXTURE: &[u8] = &[3, 12, 2, 1, b'i', 1, b't', 1, 2, 2, 2, 9, 42, 1];
const CURSOR_CLOSED_TRUE_FIXTURE: &[u8] = &[4, 1];
const CURSOR_CLOSED_FALSE_FIXTURE: &[u8] = &[4, 0];
const TABLE_METADATA_PAYLOAD: &[u8] = &[0, 1, 0, 1, b't', 1, 1, 1, b'n'];
const EVENTS_METADATA_PAYLOAD: &[u8] = &[1, 1, 1, 1, 2, b'o', b'k', 1, 1, 0, 0];
const METADATA_RESPONSE_FIXTURE: &[u8] = &[5, 9, 0, 1, 0, 1, b't', 1, 1, 1, b'n'];
const FINAL_TABLE_METADATA_PAYLOAD: &[u8] = &[0, 1, 0, 1, b't', 0, 0];
const FINAL_TABLE_METADATA_RESPONSE_FIXTURE: &[u8] = &[5, 7, 0, 1, 0, 1, b't', 0, 0];
const EVENTS_METADATA_RESPONSE_FIXTURE: &[u8] = &[5, 11, 1, 1, 1, 1, 2, b'o', b'k', 1, 1, 0, 0];
const SCHEMA_METADATA_RESPONSE_FIXTURE: &[u8] = &[5, 2, b'{', b'}'];
const MAINTENANCE_METADATA_RESPONSE_FIXTURE: &[u8] = &[5, 2, b'[', b']'];
const EXPLAIN_RESPONSE_FIXTURE: &[u8] = &[6, 2, 0xca, 0xcb];
const CUSTOM_RESPONSE_FIXTURE: &[u8] = &[8, 2, 0xcc, 0xcd];
const INVALID_CHANNEL_FAILURE_FIXTURE: &[u8] = &[9, 11, 0];
const OPERATION_ALREADY_COMPLETED_FAILURE_FIXTURE: &[u8] = &[9, 21, 0];

const OWNER_STATUS_DEFAULT_FIXTURE: &[u8] = &[
    7, 0, 0, 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 0, 0xfb, 0x88, 0x13, 0, 0xfb, 0xe8, 0x03, 0,
    0xfb, 0x00, 0x20, 0, 0xfb, 0x2c, 0x01, 0, 0xfb, 0x00, 0x04, 0, 0xfb, 0x58, 0x02, 0, 0xfb, 0xb0,
    0x04, 0, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 0, 4, 3, 15, 1, 16,
];
// Three of this fixture's "overrides" are the shipped defaults -- result_rows
// 500, concurrency 4, request_ms 10_000 -- so their source bytes (offsets 6,
// 40 and 44) read Default. A source describes ITS OWN setting: configuring one
// ceiling does not make the ones left alone into choices.
const OWNER_STATUS_OVERRIDE_FIXTURE: &[u8] = &[
    7, 0, 0, 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 1, 0xfb, 0x88, 0x13, 1, 0xfb, 0xe8, 0x03, 1,
    0xfb, 0x00, 0x20, 1, 0xfb, 0x2c, 0x01, 1, 0xfb, 0x00, 0x04, 1, 0xfb, 0x58, 0x02, 1, 0xfb, 0xb0,
    0x04, 1, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 1, 4, 3, 15, 1, 16,
];
const OWNER_STATUS_NO_AVAILABLE_MEMORY_FIXTURE: &[u8] = &[
    7, 0, 0, 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 0, 0xfb, 0x88, 0x13, 0, 0xfb, 0xe8, 0x03, 0,
    0xfb, 0x00, 0x20, 0, 0xfb, 0x2c, 0x01, 0, 0xfb, 0x00, 0x04, 0, 0xfb, 0x58, 0x02, 0, 0xfb, 0xb0,
    0x04, 0, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 0, 4, 3, 15, 0,
];
const OWNER_STATUS_AT_CAPACITY_FIXTURE: &[u8] = &[
    7, 0, 0, 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 0, 0xfb, 0x88, 0x13, 0, 0xfb, 0xe8, 0x03, 0,
    0xfb, 0x00, 0x20, 0, 0xfb, 0x2c, 0x01, 0, 0xfb, 0x00, 0x04, 0, 0xfb, 0x58, 0x02, 0, 0xfb, 0xb0,
    0x04, 0, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 0, 4, 4, 15, 1, 16,
];
const OWNER_STATUS_SERVING_DISABLED_FIXTURE: &[u8] = &[
    7, 1, 1, 0, 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 0, 0xfb, 0x88, 0x13, 0, 0xfb, 0xe8, 0x03, 0,
    0xfb, 0x00, 0x20, 0, 0xfb, 0x2c, 0x01, 0, 0xfb, 0x00, 0x04, 0, 0xfb, 0x58, 0x02, 0, 0xfb, 0xb0,
    0x04, 0, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 0, 4, 3, 15, 1, 16,
];
const OWNER_STATUS_STARTUP_FAILURE_FIXTURE: &[u8] = &[
    7, 2, 1, 1, 3, b'b', b'a', b'd', 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 0, 0xfb, 0x88, 0x13, 0,
    0xfb, 0xe8, 0x03, 0, 0xfb, 0x00, 0x20, 0, 0xfb, 0x2c, 0x01, 0, 0xfb, 0x00, 0x04, 0, 0xfb, 0x58,
    0x02, 0, 0xfb, 0xb0, 0x04, 0, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 0, 4, 3, 15, 1, 16,
];
const OWNER_STATUS_SHUTDOWN_DRAINING_FIXTURE: &[u8] = &[
    7, 2, 1, 3, 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 0, 0xfb, 0x88, 0x13, 0, 0xfb, 0xe8, 0x03, 0,
    0xfb, 0x00, 0x20, 0, 0xfb, 0x2c, 0x01, 0, 0xfb, 0x00, 0x04, 0, 0xfb, 0x58, 0x02, 0, 0xfb, 0xb0,
    0x04, 0, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 0, 4, 3, 15, 1, 16,
];
const OWNER_STATUS_NOT_APPLICABLE_FIXTURE: &[u8] = &[
    7, 3, 0, 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 0, 0xfb, 0x88, 0x13, 0, 0xfb, 0xe8, 0x03, 0,
    0xfb, 0x00, 0x20, 0, 0xfb, 0x2c, 0x01, 0, 0xfb, 0x00, 0x04, 0, 0xfb, 0x58, 0x02, 0, 0xfb, 0xb0,
    0x04, 0, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 0, 4, 3, 15, 1, 16,
];
const OWNER_STATUS_PLATFORM_UNSUPPORTED_FIXTURE: &[u8] = &[
    7, 3, 1, 2, 0xfb, 0xf4, 0x01, 0, 0xfb, 0x00, 0x10, 0, 0xfb, 0x88, 0x13, 0, 0xfb, 0xe8, 0x03, 0,
    0xfb, 0x00, 0x20, 0, 0xfb, 0x2c, 0x01, 0, 0xfb, 0x00, 0x04, 0, 0xfb, 0x58, 0x02, 0, 0xfb, 0xb0,
    0x04, 0, 4, 0, 0xfb, 0x10, 0x27, 0, 0xfb, 0xe0, 0x2e, 0, 4, 3, 15, 1, 16,
];
const OWNER_STATUS_SOURCE_OFFSETS: [usize; 12] = [6, 10, 14, 18, 22, 26, 30, 34, 38, 40, 44, 48];

fn fixture_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        result_bytes: 4_096,
        work: 5_000,
        active_ms: 1_000,
        memory: 8_192,
        cursor_page_rows: 300,
        cursor_page_bytes: 1_024,
        cursor_idle_ms: 600,
        cursor_lifetime_ms: 1_200,
    }
}

fn envelope(request: LocalRequest) -> LocalRequestEnvelope {
    LocalRequestEnvelope {
        limits: fixture_limits(),
        request,
    }
}

fn request_fixture(body: &[u8]) -> Vec<u8> {
    let mut fixture = LIMITS_FIXTURE.to_vec();
    fixture.extend_from_slice(body);
    fixture
}

fn handshake() -> LocalHandshake {
    LocalHandshake::current(
        DatabaseIdentity([0x11; 16]),
        WriterRunNumber([0x22; 16]),
        LocalUserIdentity(7),
    )
}

fn owner_status(
    source: LocalConfigurationSource,
    available_bytes: Option<u64>,
    status: OwnerReadStatus,
) -> LocalOwnerStatusResponse {
    let effective_limits = LocalEffectiveLimits::from_owner_limits(
        OwnerReadLimits {
            limits: fixture_limits(),
            concurrency: 4,
        },
        source,
    )
    .expect("convert owner limits through the production transport DTO");
    let timeouts = LocalOwnerTimeouts::from_owner_timeouts(
        OwnerServiceTimeouts {
            request_ms: 10_000,
            shutdown_drain_ms: 12_000,
        },
        source,
    )
    .expect("convert owner timeouts through the production transport DTO");
    LocalOwnerStatusResponse {
        status,
        effective_limits,
        timeouts,
        admission: OwnerAdmissionCounters {
            capacity: 4,
            active_readers: 3,
        },
        memory: OwnerMemoryCounters {
            used_bytes: 15,
            available_bytes,
        },
    }
}

fn serving_status() -> OwnerReadStatus {
    OwnerReadStatus {
        state: OwnerServingState::Serving,
        reason: None,
    }
}

fn failure(kind: ReadFailureKind, detail: ReadFailureDetail) -> ReadFailure {
    ReadFailure::new(kind, detail).expect("the fixture detail belongs to its failure kind")
}

fn assert_codec<T>(value: T, fixture: &[u8])
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    assert_eq!(
        encode_message(&value).expect("encode checked-in local bytes"),
        fixture
    );
    assert_eq!(
        decode_message_exact::<T>(fixture).expect("decode checked-in local bytes"),
        value
    );
}

struct LiteralFrameReader {
    bytes: Vec<u8>,
    offset: usize,
}

impl LiteralFrameReader {
    fn new(payload: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(payload.len() + 4);
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(payload);
        Self { bytes, offset: 0 }
    }
}

impl FrameReader for LiteralFrameReader {
    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), LocalTransportError> {
        let end = self.offset.saturating_add(output.len());
        if end > self.bytes.len() {
            return Err(LocalTransportError::Frame(FrameViolation::TruncatedPayload));
        }
        output.copy_from_slice(&self.bytes[self.offset..end]);
        self.offset = end;
        Ok(())
    }
}

#[derive(Default)]
struct RecordingAllocator {
    requested: Vec<usize>,
    allocation_limit: usize,
}

impl RecordingAllocator {
    fn bounded(allocation_limit: usize) -> Self {
        Self {
            requested: Vec::new(),
            allocation_limit,
        }
    }
}

impl FrameBufferAllocator for RecordingAllocator {
    fn allocate(&mut self, length: usize) -> Result<Vec<u8>, LocalTransportError> {
        self.requested.push(length);
        if length > self.allocation_limit {
            return Err(LocalTransportError::Frame(
                FrameViolation::LengthExceedsMaximum,
            ));
        }
        Ok(vec![0; length])
    }
}

fn literal_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn protocol_boundary() -> LocalProtocolBoundary {
    LocalProtocolBoundary::with_effective_limits(fixture_limits())
}

fn assert_handshake_boundary(value: &LocalHandshake, fixture: &[u8]) {
    let boundary = protocol_boundary();
    assert_eq!(
        boundary
            .encode_frame(LocalOutboundMessage::Handshake(value))
            .expect("encode handshake through the production boundary"),
        literal_frame(fixture)
    );
    let mut reader = LiteralFrameReader::new(fixture);
    let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
    assert_eq!(
        boundary
            .receive_frame(LocalInboundKind::Handshake, &mut reader, &mut allocator)
            .expect("decode handshake through the production boundary"),
        LocalInboundMessage::Handshake(value.clone())
    );
}

fn assert_request_boundary(value: &LocalRequestEnvelope, fixture: &[u8]) {
    let boundary = protocol_boundary();
    assert_eq!(
        boundary
            .encode_frame(LocalOutboundMessage::Request(value))
            .expect("encode request through the production boundary"),
        literal_frame(fixture)
    );
    let mut reader = LiteralFrameReader::new(fixture);
    let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
    assert_eq!(
        boundary
            .receive_frame(LocalInboundKind::Request, &mut reader, &mut allocator)
            .expect("decode request through the production boundary"),
        LocalInboundMessage::Request(value.clone())
    );
}

fn assert_response_boundary(
    value: &LocalResponse,
    expectation: LocalResponseExpectation,
    fixture: &[u8],
) {
    let boundary = protocol_boundary();
    assert_eq!(
        boundary
            .encode_frame(LocalOutboundMessage::Response {
                response: value,
                expectation: &expectation,
            })
            .expect("encode response through the production boundary"),
        literal_frame(fixture)
    );
    let mut reader = LiteralFrameReader::new(fixture);
    let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
    assert_eq!(
        boundary
            .receive_frame(
                LocalInboundKind::Response(expectation),
                &mut reader,
                &mut allocator,
            )
            .expect("decode response through the production boundary"),
        LocalInboundMessage::Response(value.clone())
    );
}

fn invalid_channel_data() -> LocalTransportError {
    LocalTransportError::Refusal(
        ReadFailure::new(ReadFailureKind::InvalidChannelData, ReadFailureDetail::None)
            .expect("invalid channel data accepts an empty detail"),
    )
}

fn assert_complete_payload_damage_is_invalid(kind: LocalInboundKind, fixture: &[u8]) {
    for damaged in [fixture[..fixture.len() - 1].to_vec(), {
        let mut trailing = fixture.to_vec();
        trailing.push(0);
        trailing
    }] {
        let mut reader = LiteralFrameReader::new(&damaged);
        let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
        assert_eq!(
            protocol_boundary().receive_frame(kind.clone(), &mut reader, &mut allocator,),
            Err(invalid_channel_data())
        );
    }
}

fn assert_truncated_and_trailing_are_rejected<T>(fixture: &[u8])
where
    T: DeserializeOwned + PartialEq + std::fmt::Debug,
{
    assert!(!fixture.is_empty());
    assert_eq!(
        decode_message_exact::<T>(&fixture[..fixture.len() - 1]),
        Err(
            contextdb_engine::local_transport::LocalTransportError::Payload(
                PayloadViolation::Malformed,
            )
        )
    );
    let mut trailing = fixture.to_vec();
    trailing.push(0);
    assert_eq!(
        decode_message_exact::<T>(&trailing),
        Err(
            contextdb_engine::local_transport::LocalTransportError::Payload(
                PayloadViolation::TrailingBytes,
            )
        )
    );
}

#[test]
fn handshake_has_one_literal_encoding_and_rejects_partial_or_trailing_bytes() {
    assert_codec(handshake(), HANDSHAKE_FIXTURE);
    assert_handshake_boundary(&handshake(), HANDSHAKE_FIXTURE);
    assert_eq!(
        decode_handshake_exact(HANDSHAKE_FIXTURE).expect("decode fixed handshake"),
        handshake()
    );
    assert_truncated_and_trailing_are_rejected::<LocalHandshake>(HANDSHAKE_FIXTURE);
    assert_complete_payload_damage_is_invalid(LocalInboundKind::Handshake, HANDSHAKE_FIXTURE);
}

#[test]
fn every_request_carries_all_caller_limits_and_uses_checked_in_bytes() {
    let cursor_id = [0x44; 16];
    let mut query_params = BTreeMap::new();
    query_params.insert("p".to_owned(), Value::TxId(TxId(42)));
    let mut cursor_params = BTreeMap::new();
    cursor_params.insert("b".to_owned(), Value::Bool(true));
    let mut explain_params = BTreeMap::new();
    explain_params.insert("n".to_owned(), Value::Text("v".to_owned()));

    assert_codec(fixture_limits(), LIMITS_FIXTURE);

    let cases = [
        (
            envelope(LocalRequest::Query {
                statement: "Q".to_owned(),
                params: query_params,
            }),
            QUERY_BODY,
        ),
        (
            envelope(LocalRequest::CursorOpen {
                statement: "O".to_owned(),
                params: cursor_params,
            }),
            CURSOR_OPEN_BODY,
        ),
        (
            envelope(LocalRequest::CursorFetch {
                cursor_id,
                rows: None,
            }),
            CURSOR_FETCH_DEFAULT_BODY,
        ),
        (
            envelope(LocalRequest::CursorFetch {
                cursor_id,
                rows: NonZeroU64::new(300),
            }),
            CURSOR_FETCH_ROWS_BODY,
        ),
        (
            envelope(LocalRequest::CursorClose { cursor_id }),
            CURSOR_CLOSE_BODY,
        ),
        (
            envelope(LocalRequest::Metadata {
                request: LocalMetadataRequest::Tables { continuation: None },
            }),
            TABLES_NONE_BODY,
        ),
        (
            envelope(LocalRequest::Metadata {
                request: LocalMetadataRequest::Tables {
                    continuation: Some("c".to_owned()),
                },
            }),
            TABLES_SOME_BODY,
        ),
        (
            envelope(LocalRequest::Metadata {
                request: LocalMetadataRequest::Schema {
                    table: "t".to_owned(),
                },
            }),
            SCHEMA_BODY,
        ),
        (
            envelope(LocalRequest::Metadata {
                request: LocalMetadataRequest::EventsStatus { continuation: None },
            }),
            EVENTS_NONE_BODY,
        ),
        (
            envelope(LocalRequest::Metadata {
                request: LocalMetadataRequest::EventsStatus {
                    continuation: Some("c".to_owned()),
                },
            }),
            EVENTS_SOME_BODY,
        ),
        (
            envelope(LocalRequest::Metadata {
                request: LocalMetadataRequest::MaintenanceStatus,
            }),
            MAINTENANCE_BODY,
        ),
        (
            envelope(LocalRequest::Explain {
                statement: "E".to_owned(),
                params: explain_params,
            }),
            EXPLAIN_BODY,
        ),
        (
            envelope(LocalRequest::OwnerStatus),
            OWNER_STATUS_REQUEST_BODY,
        ),
        (
            envelope(LocalRequest::Custom {
                namespace: "n".to_owned(),
                payload: vec![0x5a, 0x5b],
            }),
            CUSTOM_BODY,
        ),
    ];

    for (request, body) in cases {
        let fixture = request_fixture(body);
        assert_request_boundary(&request, &fixture);
        assert_truncated_and_trailing_are_rejected::<LocalRequestEnvelope>(&fixture);
        assert_complete_payload_damage_is_invalid(LocalInboundKind::Request, &fixture);
    }

    let zero_rows = request_fixture(CURSOR_FETCH_ZERO_ROWS_BODY);
    assert_eq!(
        decode_message_exact::<LocalRequestEnvelope>(&zero_rows),
        Err(
            contextdb_engine::local_transport::LocalTransportError::Payload(
                PayloadViolation::Malformed,
            )
        ),
        "a cursor fetch cannot request zero rows"
    );

    let mut schema_with_continuation = request_fixture(SCHEMA_BODY);
    schema_with_continuation.extend_from_slice(&[1, 1, b'c']);
    let mut maintenance_with_continuation = request_fixture(MAINTENANCE_BODY);
    maintenance_with_continuation.extend_from_slice(&[1, 1, b'c']);
    for malformed in [schema_with_continuation, maintenance_with_continuation] {
        assert_eq!(
            decode_message_exact::<LocalRequestEnvelope>(&malformed),
            Err(
                contextdb_engine::local_transport::LocalTransportError::Payload(
                    PayloadViolation::TrailingBytes,
                )
            ),
            "only tables and event status accept continuation tokens"
        );
    }

    let mut unknown_request = LIMITS_FIXTURE.to_vec();
    unknown_request.push(250);
    assert_eq!(
        decode_message_exact::<LocalRequestEnvelope>(&unknown_request),
        Err(
            contextdb_engine::local_transport::LocalTransportError::Payload(
                PayloadViolation::Malformed,
            )
        )
    );
}

fn assert_request_mutation(
    request: LocalRequestEnvelope,
    base_body: &[u8],
    fixture_offset: usize,
    expected_byte: u8,
) {
    let mut expected = request_fixture(base_body);
    expected[fixture_offset] = expected_byte;
    assert_request_boundary(&request, &expected);
}

#[test]
fn every_request_field_and_caller_limit_changes_its_independent_wire_position() {
    let mut base_params = BTreeMap::new();
    base_params.insert("p".to_owned(), Value::TxId(TxId(42)));
    let base_query = LocalRequest::Query {
        statement: "Q".to_owned(),
        params: base_params.clone(),
    };
    let limit_mutations = [
        (0, 1, 0xf5),
        (1, 4, 0x01),
        (2, 7, 0x89),
        (3, 10, 0xe9),
        (4, 13, 0x01),
        (5, 16, 0x2d),
        (6, 19, 0x01),
        (7, 22, 0x59),
        (8, 25, 0xb1),
    ];
    for (field, fixture_offset, expected_byte) in limit_mutations {
        let mut limits = fixture_limits();
        match field {
            0 => limits.result_rows += 1,
            1 => limits.result_bytes += 1,
            2 => limits.work += 1,
            3 => limits.active_ms += 1,
            4 => limits.memory += 1,
            5 => limits.cursor_page_rows += 1,
            6 => limits.cursor_page_bytes += 1,
            7 => limits.cursor_idle_ms += 1,
            8 => limits.cursor_lifetime_ms += 1,
            _ => unreachable!(),
        }
        let request = LocalRequestEnvelope {
            limits,
            request: base_query.clone(),
        };
        let mut expected = request_fixture(QUERY_BODY);
        expected[fixture_offset] = expected_byte;
        assert_request_boundary(&request, &expected);
    }

    let mut params = base_params.clone();
    assert_request_mutation(
        envelope(LocalRequest::Query {
            statement: "R".to_owned(),
            params: params.clone(),
        }),
        QUERY_BODY,
        29,
        b'R',
    );
    let value = params.remove("p").expect("base parameter");
    params.insert("q".to_owned(), value);
    assert_request_mutation(
        envelope(LocalRequest::Query {
            statement: "Q".to_owned(),
            params,
        }),
        QUERY_BODY,
        32,
        b'q',
    );
    let mut params = BTreeMap::new();
    params.insert("p".to_owned(), Value::TxId(TxId(43)));
    assert_request_mutation(
        envelope(LocalRequest::Query {
            statement: "Q".to_owned(),
            params,
        }),
        QUERY_BODY,
        34,
        43,
    );

    let mut params = BTreeMap::new();
    params.insert("b".to_owned(), Value::Bool(true));
    assert_request_mutation(
        envelope(LocalRequest::CursorOpen {
            statement: "P".to_owned(),
            params: params.clone(),
        }),
        CURSOR_OPEN_BODY,
        29,
        b'P',
    );
    let value = params.remove("b").expect("cursor parameter");
    params.insert("c".to_owned(), value);
    assert_request_mutation(
        envelope(LocalRequest::CursorOpen {
            statement: "O".to_owned(),
            params,
        }),
        CURSOR_OPEN_BODY,
        32,
        b'c',
    );
    let mut params = BTreeMap::new();
    params.insert("b".to_owned(), Value::Bool(false));
    assert_request_mutation(
        envelope(LocalRequest::CursorOpen {
            statement: "O".to_owned(),
            params,
        }),
        CURSOR_OPEN_BODY,
        34,
        0,
    );

    let mut cursor_id = [0x44; 16];
    cursor_id[0] = 0x45;
    assert_request_mutation(
        envelope(LocalRequest::CursorFetch {
            cursor_id,
            rows: NonZeroU64::new(300),
        }),
        CURSOR_FETCH_ROWS_BODY,
        28,
        0x45,
    );
    assert_request_mutation(
        envelope(LocalRequest::CursorFetch {
            cursor_id: [0x44; 16],
            rows: NonZeroU64::new(301),
        }),
        CURSOR_FETCH_ROWS_BODY,
        46,
        0x2d,
    );
    assert_request_mutation(
        envelope(LocalRequest::CursorClose { cursor_id }),
        CURSOR_CLOSE_BODY,
        28,
        0x45,
    );
    assert_request_mutation(
        envelope(LocalRequest::Metadata {
            request: LocalMetadataRequest::Tables {
                continuation: Some("d".to_owned()),
            },
        }),
        TABLES_SOME_BODY,
        31,
        b'd',
    );
    assert_request_mutation(
        envelope(LocalRequest::Metadata {
            request: LocalMetadataRequest::Schema {
                table: "u".to_owned(),
            },
        }),
        SCHEMA_BODY,
        30,
        b'u',
    );
    assert_request_mutation(
        envelope(LocalRequest::Metadata {
            request: LocalMetadataRequest::EventsStatus {
                continuation: Some("d".to_owned()),
            },
        }),
        EVENTS_SOME_BODY,
        31,
        b'd',
    );

    let mut explain_params = BTreeMap::new();
    explain_params.insert("n".to_owned(), Value::Text("v".to_owned()));
    assert_request_mutation(
        envelope(LocalRequest::Explain {
            statement: "F".to_owned(),
            params: explain_params.clone(),
        }),
        EXPLAIN_BODY,
        29,
        b'F',
    );
    let value = explain_params.remove("n").expect("explain parameter");
    explain_params.insert("o".to_owned(), value);
    assert_request_mutation(
        envelope(LocalRequest::Explain {
            statement: "E".to_owned(),
            params: explain_params,
        }),
        EXPLAIN_BODY,
        32,
        b'o',
    );
    let mut explain_params = BTreeMap::new();
    explain_params.insert("n".to_owned(), Value::Text("w".to_owned()));
    assert_request_mutation(
        envelope(LocalRequest::Explain {
            statement: "E".to_owned(),
            params: explain_params,
        }),
        EXPLAIN_BODY,
        35,
        b'w',
    );
    assert_request_mutation(
        envelope(LocalRequest::Custom {
            namespace: "o".to_owned(),
            payload: vec![0x5a, 0x5b],
        }),
        CUSTOM_BODY,
        29,
        b'o',
    );
    assert_request_mutation(
        envelope(LocalRequest::Custom {
            namespace: "n".to_owned(),
            payload: vec![0x5c, 0x5b],
        }),
        CUSTOM_BODY,
        31,
        0x5c,
    );
}

#[test]
fn illegal_caller_limit_relationships_are_rejected_at_the_typed_boundary() {
    let base_request = LocalRequest::OwnerStatus;
    let mut illegal_limits = Vec::new();
    let mut limits = fixture_limits();
    limits.result_rows = 0;
    illegal_limits.push(limits);
    let mut limits = fixture_limits();
    limits.cursor_page_rows = limits.result_rows + 1;
    illegal_limits.push(limits);
    let mut limits = fixture_limits();
    limits.cursor_page_bytes = limits.result_bytes + 1;
    illegal_limits.push(limits);
    let mut limits = fixture_limits();
    limits.cursor_idle_ms = limits.cursor_lifetime_ms + 1;
    illegal_limits.push(limits);

    for limits in illegal_limits {
        let request = LocalRequestEnvelope {
            limits,
            request: base_request.clone(),
        };
        assert_eq!(
            protocol_boundary().encode_frame(LocalOutboundMessage::Request(&request)),
            Err(invalid_channel_data())
        );
    }

    let complete_illegal_payloads = [
        {
            let mut bytes = vec![0];
            bytes.extend_from_slice(&LIMITS_FIXTURE[3..]);
            bytes.extend_from_slice(OWNER_STATUS_REQUEST_BODY);
            bytes
        },
        {
            let mut bytes = LIMITS_FIXTURE.to_vec();
            bytes[16] = 0xf5;
            bytes.extend_from_slice(OWNER_STATUS_REQUEST_BODY);
            bytes
        },
        {
            let mut bytes = LIMITS_FIXTURE.to_vec();
            bytes[19] = 0x01;
            bytes[20] = 0x10;
            bytes.extend_from_slice(OWNER_STATUS_REQUEST_BODY);
            bytes
        },
        {
            let mut bytes = LIMITS_FIXTURE.to_vec();
            bytes[22] = 0xb1;
            bytes[23] = 0x04;
            bytes.extend_from_slice(OWNER_STATUS_REQUEST_BODY);
            bytes
        },
    ];
    for payload in complete_illegal_payloads {
        let mut reader = LiteralFrameReader::new(&payload);
        let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
        assert_eq!(
            protocol_boundary().receive_frame(
                LocalInboundKind::Request,
                &mut reader,
                &mut allocator,
            ),
            Err(invalid_channel_data())
        );
    }
}

#[test]
fn every_response_envelope_has_checked_in_bytes() {
    let cursor_id = [0x44; 16];
    let invalid_channel = failure(ReadFailureKind::InvalidChannelData, ReadFailureDetail::None);
    let operation_already_completed = failure(
        ReadFailureKind::OperationAlreadyCompleted,
        ReadFailureDetail::None,
    );
    let cases = [
        (
            LocalResponse::ResultChunk {
                chunk: ResultChunk {
                    bytes: vec![0xc1, 0xc2],
                },
            },
            LocalResponseExpectation::OrdinaryResult,
            RESULT_CHUNK_FIXTURE,
        ),
        (
            LocalResponse::TerminalSuccess {
                success: TerminalSuccess {
                    final_bytes: vec![0xc3],
                },
            },
            LocalResponseExpectation::OrdinaryResult,
            TERMINAL_SUCCESS_FIXTURE,
        ),
        (
            LocalResponse::CursorOpened {
                opened: CursorOpenedResponse {
                    cursor_id,
                    payload: CANONICAL_CURSOR_PAGE.to_vec(),
                },
            },
            LocalResponseExpectation::CursorOpen,
            CURSOR_OPENED_FIXTURE,
        ),
        (
            LocalResponse::CursorPage {
                page: CursorPageResponse {
                    payload: CANONICAL_CURSOR_PAGE.to_vec(),
                },
            },
            LocalResponseExpectation::CursorFetch,
            CURSOR_PAGE_FIXTURE,
        ),
        (
            LocalResponse::CursorClosed {
                acknowledgement: CursorCloseAcknowledgement { closed: true },
            },
            LocalResponseExpectation::CursorClose,
            CURSOR_CLOSED_TRUE_FIXTURE,
        ),
        (
            LocalResponse::CursorClosed {
                acknowledgement: CursorCloseAcknowledgement { closed: false },
            },
            LocalResponseExpectation::CursorClose,
            CURSOR_CLOSED_FALSE_FIXTURE,
        ),
        (
            LocalResponse::Metadata {
                metadata: MetadataResponse {
                    payload: TABLE_METADATA_PAYLOAD.to_vec(),
                },
            },
            LocalResponseExpectation::Metadata(LocalMetadataRequest::Tables { continuation: None }),
            METADATA_RESPONSE_FIXTURE,
        ),
        (
            LocalResponse::Explain {
                payload: vec![0xca, 0xcb],
            },
            LocalResponseExpectation::Explain,
            EXPLAIN_RESPONSE_FIXTURE,
        ),
        (
            LocalResponse::Custom {
                payload: vec![0xcc, 0xcd],
            },
            LocalResponseExpectation::Custom,
            CUSTOM_RESPONSE_FIXTURE,
        ),
        (
            LocalResponse::Failure {
                failure: invalid_channel,
            },
            LocalResponseExpectation::OrdinaryResult,
            INVALID_CHANNEL_FAILURE_FIXTURE,
        ),
        (
            LocalResponse::Failure {
                failure: operation_already_completed,
            },
            LocalResponseExpectation::OrdinaryResult,
            OPERATION_ALREADY_COMPLETED_FAILURE_FIXTURE,
        ),
    ];

    for (response, expectation, fixture) in cases {
        assert_response_boundary(&response, expectation.clone(), fixture);
        assert_truncated_and_trailing_are_rejected::<LocalResponse>(fixture);
        assert_complete_payload_damage_is_invalid(LocalInboundKind::Response(expectation), fixture);
    }

    assert_response_boundary(
        &LocalResponse::Metadata {
            metadata: MetadataResponse {
                payload: EVENTS_METADATA_PAYLOAD.to_vec(),
            },
        },
        LocalResponseExpectation::Metadata(LocalMetadataRequest::EventsStatus {
            continuation: None,
        }),
        EVENTS_METADATA_RESPONSE_FIXTURE,
    );
    assert_response_boundary(
        &LocalResponse::Metadata {
            metadata: MetadataResponse {
                payload: b"{}".to_vec(),
            },
        },
        LocalResponseExpectation::Metadata(LocalMetadataRequest::Schema {
            table: "t".to_owned(),
        }),
        SCHEMA_METADATA_RESPONSE_FIXTURE,
    );
    assert_response_boundary(
        &LocalResponse::Metadata {
            metadata: MetadataResponse {
                payload: b"[]".to_vec(),
            },
        },
        LocalResponseExpectation::Metadata(LocalMetadataRequest::MaintenanceStatus),
        MAINTENANCE_METADATA_RESPONSE_FIXTURE,
    );
    assert_eq!(
        decode_message_exact::<LocalResponse>(&[250]),
        Err(
            contextdb_engine::local_transport::LocalTransportError::Payload(
                PayloadViolation::Malformed,
            )
        )
    );
}

fn assert_response_mutation(
    response: LocalResponse,
    expectation: LocalResponseExpectation,
    base_fixture: &[u8],
    fixture_offset: usize,
    expected_byte: u8,
) {
    let mut expected = base_fixture.to_vec();
    expected[fixture_offset] = expected_byte;
    assert_response_boundary(&response, expectation, &expected);
}

#[test]
fn every_success_response_field_changes_independent_literal_bytes() {
    assert_response_mutation(
        LocalResponse::ResultChunk {
            chunk: ResultChunk {
                bytes: vec![0xc3, 0xc2],
            },
        },
        LocalResponseExpectation::OrdinaryResult,
        RESULT_CHUNK_FIXTURE,
        2,
        0xc3,
    );
    assert_response_mutation(
        LocalResponse::TerminalSuccess {
            success: TerminalSuccess {
                final_bytes: vec![0xc4],
            },
        },
        LocalResponseExpectation::OrdinaryResult,
        TERMINAL_SUCCESS_FIXTURE,
        2,
        0xc4,
    );

    let mut cursor_id = [0x44; 16];
    cursor_id[0] = 0x45;
    assert_response_mutation(
        LocalResponse::CursorOpened {
            opened: CursorOpenedResponse {
                cursor_id,
                payload: CANONICAL_CURSOR_PAGE.to_vec(),
            },
        },
        LocalResponseExpectation::CursorOpen,
        CURSOR_OPENED_FIXTURE,
        1,
        0x45,
    );
    for (payload_offset, fixture_offset, byte) in
        [(2, 20, b'j'), (8, 26, 4), (10, 28, 43), (11, 29, 0)]
    {
        let mut payload = CANONICAL_CURSOR_PAGE.to_vec();
        payload[payload_offset] = byte;
        assert_response_mutation(
            LocalResponse::CursorOpened {
                opened: CursorOpenedResponse {
                    cursor_id: [0x44; 16],
                    payload,
                },
            },
            LocalResponseExpectation::CursorOpen,
            CURSOR_OPENED_FIXTURE,
            fixture_offset,
            byte,
        );
    }
    for (payload_offset, fixture_offset, byte) in
        [(2, 4, b'j'), (8, 10, 4), (10, 12, 43), (11, 13, 0)]
    {
        let mut payload = CANONICAL_CURSOR_PAGE.to_vec();
        payload[payload_offset] = byte;
        assert_response_mutation(
            LocalResponse::CursorPage {
                page: CursorPageResponse { payload },
            },
            LocalResponseExpectation::CursorFetch,
            CURSOR_PAGE_FIXTURE,
            fixture_offset,
            byte,
        );
    }

    assert_response_boundary(
        &LocalResponse::CursorClosed {
            acknowledgement: CursorCloseAcknowledgement { closed: false },
        },
        LocalResponseExpectation::CursorClose,
        CURSOR_CLOSED_FALSE_FIXTURE,
    );
    assert_response_mutation(
        LocalResponse::Metadata {
            metadata: MetadataResponse {
                payload: {
                    let mut payload = TABLE_METADATA_PAYLOAD.to_vec();
                    payload[4] = b'u';
                    payload
                },
            },
        },
        LocalResponseExpectation::Metadata(LocalMetadataRequest::Tables { continuation: None }),
        METADATA_RESPONSE_FIXTURE,
        6,
        b'u',
    );
    assert_response_mutation(
        LocalResponse::Metadata {
            metadata: MetadataResponse {
                payload: {
                    let mut payload = TABLE_METADATA_PAYLOAD.to_vec();
                    payload[8] = b'o';
                    payload
                },
            },
        },
        LocalResponseExpectation::Metadata(LocalMetadataRequest::Tables {
            continuation: Some("c".to_owned()),
        }),
        METADATA_RESPONSE_FIXTURE,
        10,
        b'o',
    );
    assert_response_boundary(
        &LocalResponse::Metadata {
            metadata: MetadataResponse {
                payload: FINAL_TABLE_METADATA_PAYLOAD.to_vec(),
            },
        },
        LocalResponseExpectation::Metadata(LocalMetadataRequest::Tables { continuation: None }),
        FINAL_TABLE_METADATA_RESPONSE_FIXTURE,
    );
    assert_response_mutation(
        LocalResponse::Explain {
            payload: vec![0xcb, 0xcb],
        },
        LocalResponseExpectation::Explain,
        EXPLAIN_RESPONSE_FIXTURE,
        2,
        0xcb,
    );
    assert_response_mutation(
        LocalResponse::Custom {
            payload: vec![0xcd, 0xcd],
        },
        LocalResponseExpectation::Custom,
        CUSTOM_RESPONSE_FIXTURE,
        2,
        0xcd,
    );
}

#[test]
fn hostile_nested_lengths_and_cursor_page_ceiling_are_rejected_before_nested_allocation() {
    let boundary = protocol_boundary();
    let hostile_declared_cursor_payload = [3, 0xfc, 0x01, 0x00, 0x40, 0x00];
    let mut reader = LiteralFrameReader::new(&hostile_declared_cursor_payload);
    let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
    assert_eq!(
        boundary.receive_frame(
            LocalInboundKind::Response(LocalResponseExpectation::CursorFetch),
            &mut reader,
            &mut allocator,
        ),
        Err(invalid_channel_data())
    );
    assert_eq!(
        allocator.requested,
        vec![hostile_declared_cursor_payload.len()],
        "a hostile inner length must not request an inner allocation"
    );

    let mut over_page_payload = vec![1, 0xfb, 0xfb, 0x03];
    over_page_payload.extend(std::iter::repeat_n(b'x', 1_019));
    over_page_payload.extend_from_slice(&[0, 0]);
    assert_eq!(over_page_payload.len(), 1_025);
    let mut response = vec![3, 0xfb, 0x01, 0x04];
    response.extend_from_slice(&over_page_payload);
    let mut reader = LiteralFrameReader::new(&response);
    let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
    assert_eq!(
        boundary.receive_frame(
            LocalInboundKind::Response(LocalResponseExpectation::CursorFetch),
            &mut reader,
            &mut allocator,
        ),
        Err(invalid_channel_data())
    );
    assert_eq!(allocator.requested, vec![response.len()]);
}

#[test]
fn every_dynamic_protocol_family_preflights_hostile_inner_lengths() {
    const HOSTILE: &[u8] = &[0xfc, 0x01, 0x00, 0x40, 0x00];
    let mut request_cases = Vec::new();
    for prefix in [&[0][..], &[1][..], &[4, 1][..], &[5][..], &[7][..]] {
        let mut payload = LIMITS_FIXTURE.to_vec();
        payload.extend_from_slice(prefix);
        payload.extend_from_slice(HOSTILE);
        request_cases.push(payload);
    }
    for prefix in [&[4, 0, 1][..], &[4, 2, 1][..]] {
        let mut payload = LIMITS_FIXTURE.to_vec();
        payload.extend_from_slice(prefix);
        payload.extend_from_slice(HOSTILE);
        request_cases.push(payload);
    }
    for payload in request_cases {
        let mut reader = LiteralFrameReader::new(&payload);
        let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
        assert_eq!(
            protocol_boundary().receive_frame(
                LocalInboundKind::Request,
                &mut reader,
                &mut allocator,
            ),
            Err(invalid_channel_data())
        );
        assert_eq!(allocator.requested, vec![payload.len()]);
    }

    let mut response_cases = Vec::new();
    for (prefix, expectation) in [
        (&[0][..], LocalResponseExpectation::OrdinaryResult),
        (&[1][..], LocalResponseExpectation::OrdinaryResult),
        (&[3][..], LocalResponseExpectation::CursorFetch),
        (
            &[5][..],
            LocalResponseExpectation::Metadata(LocalMetadataRequest::Tables { continuation: None }),
        ),
        (&[6][..], LocalResponseExpectation::Explain),
        (&[8][..], LocalResponseExpectation::Custom),
        (&[9, 4, 4][..], LocalResponseExpectation::Custom),
        (&[7, 2, 1, 1][..], LocalResponseExpectation::OwnerStatus),
    ] {
        let mut payload = prefix.to_vec();
        payload.extend_from_slice(HOSTILE);
        response_cases.push((payload, expectation));
    }
    let mut cursor_open = vec![2];
    cursor_open.extend_from_slice(&[0x44; 16]);
    cursor_open.extend_from_slice(HOSTILE);
    response_cases.push((cursor_open, LocalResponseExpectation::CursorOpen));

    for (payload, expectation) in response_cases {
        let mut reader = LiteralFrameReader::new(&payload);
        let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
        assert_eq!(
            protocol_boundary().receive_frame(
                LocalInboundKind::Response(expectation),
                &mut reader,
                &mut allocator,
            ),
            Err(invalid_channel_data())
        );
        assert_eq!(allocator.requested, vec![payload.len()]);
    }
}

#[test]
fn typed_boundary_admits_exactly_four_mebibytes_and_refuses_one_more() {
    const CUSTOM_RESPONSE_OVERHEAD: usize = 6;
    let expectation = LocalResponseExpectation::Custom;
    let exact = LocalResponse::Custom {
        payload: vec![0x5a; MAX_FRAME_BYTES - CUSTOM_RESPONSE_OVERHEAD],
    };
    let exact_frame = protocol_boundary()
        .encode_frame(LocalOutboundMessage::Response {
            response: &exact,
            expectation: &expectation,
        })
        .expect("the exact outer payload ceiling is admitted");
    assert_eq!(exact_frame.len(), MAX_FRAME_BYTES + 4);

    let oversized = LocalResponse::Custom {
        payload: vec![0x5a; MAX_FRAME_BYTES - CUSTOM_RESPONSE_OVERHEAD + 1],
    };
    assert_eq!(
        protocol_boundary().encode_frame(LocalOutboundMessage::Response {
            response: &oversized,
            expectation: &expectation,
        }),
        Err(LocalTransportError::Frame(
            FrameViolation::LengthExceedsMaximum,
        ))
    );
}

#[test]
fn successful_response_variants_cannot_cross_operation_expectations() {
    let cursor_page = CANONICAL_CURSOR_PAGE.to_vec();
    let mismatches = [
        (
            LocalResponse::ResultChunk {
                chunk: ResultChunk { bytes: vec![1] },
            },
            LocalResponseExpectation::CursorFetch,
        ),
        (
            LocalResponse::TerminalSuccess {
                success: TerminalSuccess {
                    final_bytes: vec![1],
                },
            },
            LocalResponseExpectation::Custom,
        ),
        (
            LocalResponse::CursorOpened {
                opened: CursorOpenedResponse {
                    cursor_id: [0x44; 16],
                    payload: cursor_page.clone(),
                },
            },
            LocalResponseExpectation::CursorFetch,
        ),
        (
            LocalResponse::CursorPage {
                page: CursorPageResponse {
                    payload: cursor_page,
                },
            },
            LocalResponseExpectation::CursorOpen,
        ),
        (
            LocalResponse::CursorClosed {
                acknowledgement: CursorCloseAcknowledgement { closed: true },
            },
            LocalResponseExpectation::Explain,
        ),
        (
            LocalResponse::Metadata {
                metadata: MetadataResponse {
                    payload: TABLE_METADATA_PAYLOAD.to_vec(),
                },
            },
            LocalResponseExpectation::OwnerStatus,
        ),
        (
            LocalResponse::Explain { payload: vec![1] },
            LocalResponseExpectation::Custom,
        ),
        (
            LocalResponse::OwnerStatus {
                status: owner_status(
                    LocalConfigurationSource::Default,
                    Some(16),
                    serving_status(),
                ),
            },
            LocalResponseExpectation::CursorClose,
        ),
        (
            LocalResponse::Custom { payload: vec![1] },
            LocalResponseExpectation::Explain,
        ),
    ];
    for (response, expectation) in mismatches {
        assert_eq!(
            protocol_boundary().encode_frame(LocalOutboundMessage::Response {
                response: &response,
                expectation: &expectation,
            }),
            Err(invalid_channel_data())
        );
    }
}

#[test]
fn owner_status_carries_each_limit_and_timeout_source_plus_capacity_and_memory_states() {
    let default_status = owner_status(
        LocalConfigurationSource::Default,
        Some(16),
        serving_status(),
    );
    let override_status = owner_status(
        LocalConfigurationSource::Override,
        Some(16),
        serving_status(),
    );
    assert_response_boundary(
        &LocalResponse::OwnerStatus {
            status: default_status.clone(),
        },
        LocalResponseExpectation::OwnerStatus,
        OWNER_STATUS_DEFAULT_FIXTURE,
    );
    assert_response_boundary(
        &LocalResponse::OwnerStatus {
            status: override_status,
        },
        LocalResponseExpectation::OwnerStatus,
        OWNER_STATUS_OVERRIDE_FIXTURE,
    );

    let mut source_mutations = Vec::new();
    let mut status = default_status.clone();
    status.effective_limits.result_rows.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.result_bytes.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.work.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.active_ms.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.memory.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.cursor_page_rows.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.cursor_page_bytes.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.cursor_idle_ms.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.cursor_lifetime_ms.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.effective_limits.concurrency.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.timeouts.request_ms.source = LocalConfigurationSource::Override;
    source_mutations.push(status);
    let mut status = default_status.clone();
    status.timeouts.shutdown_drain_ms.source = LocalConfigurationSource::Override;
    source_mutations.push(status);

    for (status, source_offset) in source_mutations
        .into_iter()
        .zip(OWNER_STATUS_SOURCE_OFFSETS)
    {
        let mut expected = OWNER_STATUS_DEFAULT_FIXTURE.to_vec();
        expected[source_offset] = 1;
        assert_response_boundary(
            &LocalResponse::OwnerStatus { status },
            LocalResponseExpectation::OwnerStatus,
            &expected,
        );
    }

    let no_available = owner_status(LocalConfigurationSource::Default, None, serving_status());
    assert_response_boundary(
        &LocalResponse::OwnerStatus {
            status: no_available,
        },
        LocalResponseExpectation::OwnerStatus,
        OWNER_STATUS_NO_AVAILABLE_MEMORY_FIXTURE,
    );
    let mut at_capacity = default_status;
    at_capacity.admission.active_readers = at_capacity.admission.capacity;
    assert_response_boundary(
        &LocalResponse::OwnerStatus {
            status: at_capacity,
        },
        LocalResponseExpectation::OwnerStatus,
        OWNER_STATUS_AT_CAPACITY_FIXTURE,
    );

    let base = owner_status(
        LocalConfigurationSource::Default,
        Some(16),
        serving_status(),
    );
    // Each entry names every wire offset a mutation is expected to move.
    // `admission.capacity` and `effective_limits.concurrency.value` are not
    // independent: `OwnerAdmission::counters()` (owner_read/admission.rs)
    // derives `capacity` from the identical `limits.concurrency` the
    // effective-limits conversion also reads, so production can never emit a
    // status where the two disagree, and `validate_owner_status` refuses one
    // that does. Moving either field alone therefore stages a shape
    // production never emits; they are moved together here into one
    // co-varying mutation that still proves each field owns its own,
    // non-overlapping wire byte.
    let mut value_mutations = Vec::new();
    let mut status = base.clone();
    status.effective_limits.result_rows.value = 501;
    value_mutations.push((status, vec![(4, 0xf5)]));
    let mut status = base.clone();
    status.effective_limits.result_bytes.value = 4_097;
    value_mutations.push((status, vec![(8, 0x01)]));
    let mut status = base.clone();
    status.effective_limits.work.value = 5_001;
    value_mutations.push((status, vec![(12, 0x89)]));
    let mut status = base.clone();
    status.effective_limits.active_ms.value = 1_001;
    value_mutations.push((status, vec![(16, 0xe9)]));
    let mut status = base.clone();
    status.effective_limits.memory.value = 8_193;
    value_mutations.push((status, vec![(20, 0x01)]));
    let mut status = base.clone();
    status.effective_limits.cursor_page_rows.value = 301;
    value_mutations.push((status, vec![(24, 0x2d)]));
    let mut status = base.clone();
    status.effective_limits.cursor_page_bytes.value = 1_025;
    value_mutations.push((status, vec![(28, 0x01)]));
    let mut status = base.clone();
    status.effective_limits.cursor_idle_ms.value = 601;
    value_mutations.push((status, vec![(32, 0x59)]));
    let mut status = base.clone();
    status.effective_limits.cursor_lifetime_ms.value = 1_201;
    value_mutations.push((status, vec![(36, 0xb1)]));
    let mut status = base.clone();
    status.effective_limits.concurrency.value = 5;
    status.admission.capacity = 5;
    value_mutations.push((status, vec![(39, 5), (49, 5)]));
    let mut status = base.clone();
    status.timeouts.request_ms.value = 10_001;
    value_mutations.push((status, vec![(42, 0x11)]));
    let mut status = base.clone();
    status.timeouts.shutdown_drain_ms.value = 12_001;
    value_mutations.push((status, vec![(46, 0xe1)]));
    let mut status = base.clone();
    status.admission.active_readers = 2;
    value_mutations.push((status, vec![(50, 2)]));
    let mut status = base.clone();
    status.memory.used_bytes = 16;
    value_mutations.push((status, vec![(51, 16)]));
    let mut status = base;
    status.memory.available_bytes = Some(17);
    value_mutations.push((status, vec![(53, 17)]));

    for (status, offsets) in value_mutations {
        let mut expected = OWNER_STATUS_DEFAULT_FIXTURE.to_vec();
        for (offset, byte) in offsets {
            expected[offset] = byte;
        }
        assert_response_boundary(
            &LocalResponse::OwnerStatus { status },
            LocalResponseExpectation::OwnerStatus,
            &expected,
        );
    }
}

#[test]
fn every_legal_owner_serving_state_and_reason_has_independent_literal_bytes() {
    let cases = [
        (serving_status(), OWNER_STATUS_DEFAULT_FIXTURE),
        (
            OwnerReadStatus {
                state: OwnerServingState::ServingDisabled,
                reason: Some(OwnerServingReason::DisabledByConfiguration),
            },
            OWNER_STATUS_SERVING_DISABLED_FIXTURE,
        ),
        (
            OwnerReadStatus {
                state: OwnerServingState::NotServing,
                reason: Some(OwnerServingReason::StartupFailure("bad".to_owned())),
            },
            OWNER_STATUS_STARTUP_FAILURE_FIXTURE,
        ),
        (
            OwnerReadStatus {
                state: OwnerServingState::NotServing,
                reason: Some(OwnerServingReason::ShutdownDraining),
            },
            OWNER_STATUS_SHUTDOWN_DRAINING_FIXTURE,
        ),
        (
            OwnerReadStatus {
                state: OwnerServingState::NotApplicable,
                reason: None,
            },
            OWNER_STATUS_NOT_APPLICABLE_FIXTURE,
        ),
        (
            OwnerReadStatus {
                state: OwnerServingState::NotApplicable,
                reason: Some(OwnerServingReason::PlatformUnsupported),
            },
            OWNER_STATUS_PLATFORM_UNSUPPORTED_FIXTURE,
        ),
    ];

    for (serving, fixture) in cases {
        let status = owner_status(LocalConfigurationSource::Default, Some(16), serving);
        assert_response_boundary(
            &LocalResponse::OwnerStatus { status },
            LocalResponseExpectation::OwnerStatus,
            fixture,
        );
    }
}

#[test]
fn contradictory_owner_status_is_rejected_on_encode_and_complete_decode() {
    let illegal = OwnerReadStatus {
        state: OwnerServingState::Serving,
        reason: Some(OwnerServingReason::ShutdownDraining),
    };
    let response = LocalResponse::OwnerStatus {
        status: owner_status(LocalConfigurationSource::Default, Some(16), illegal),
    };
    let boundary = protocol_boundary();
    assert_eq!(
        boundary.encode_frame(LocalOutboundMessage::Response {
            response: &response,
            expectation: &LocalResponseExpectation::OwnerStatus,
        }),
        Err(invalid_channel_data())
    );

    let mut illegal_fixture = OWNER_STATUS_DEFAULT_FIXTURE.to_vec();
    illegal_fixture[1] = 1;
    let mut reader = LiteralFrameReader::new(&illegal_fixture);
    let mut allocator = RecordingAllocator::bounded(4 * 1024 * 1024);
    assert_eq!(
        boundary.receive_frame(
            LocalInboundKind::Response(LocalResponseExpectation::OwnerStatus),
            &mut reader,
            &mut allocator,
        ),
        Err(invalid_channel_data())
    );
}
