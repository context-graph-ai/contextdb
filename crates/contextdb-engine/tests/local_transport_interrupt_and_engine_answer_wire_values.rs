//! Golden bytes for the two frames the interactive-cancellation journey added.
//!
//! Every other frame kind in this protocol has its encoding checked in against
//! bytes transcribed by hand, so that a change to a Rust type cannot quietly
//! rewrite what a peer reads. These two arrived later and are pinned the same
//! way, in their own file because the files that pin the rest are frozen.
//!
//! The bytes below were written from bincode 2's standard grammar — a variant
//! index and a length are each a single byte below 251 — not by printing what
//! the encoder produced.

use contextdb_core::read_contract::{ReadFailure, ReadFailureDetail, ReadFailureKind, ReadLimits};
use contextdb_core::{Error, Value};
use contextdb_engine::local_transport::{
    FrameBufferAllocator, FrameReader, LocalEngineFailure, LocalInboundKind, LocalInboundMessage,
    LocalOutboundMessage, LocalProtocolBoundary, LocalRequest, LocalRequestEnvelope, LocalResponse,
    LocalResponseExpectation, LocalTransportError, decode_message_exact, encode_message,
};

/// The caller limits every request carries, ahead of the request itself.
const LIMITS_FIXTURE: &[u8] = &[
    0xfb, 0xf4, 0x01, 0xfb, 0x00, 0x10, 0xfb, 0x88, 0x13, 0xfb, 0xe8, 0x03, 0xfb, 0x00, 0x20, 0xfb,
    0x2c, 0x01, 0xfb, 0x00, 0x04, 0xfb, 0x58, 0x02, 0xfb, 0xb0, 0x04,
];

/// Request kind 8, naming the seventh request on this connection.
const CANCEL_IN_FLIGHT_BODY: &[u8] = &[8, 7];
/// The same, naming a request far enough along to need the multi-byte form.
const CANCEL_IN_FLIGHT_LARGE_BODY: &[u8] = &[8, 0xfb, 0x2c, 0x01];

/// Response kind 10, carrying a one-byte engine document: the read-channel
/// error tag for `ReadCancelled`, and no body.
const ENGINE_READ_CANCELLED: &[u8] = &[10, 1, 3];
/// The same frame carrying the tag for `TableNotFound` and its one word.
const ENGINE_TABLE_NOT_FOUND: &[u8] = &[10, 3, 8, 1, b't'];

fn fixture_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        result_bytes: 4096,
        work: 5_000,
        active_ms: 1000,
        memory: 8192,
        cursor_page_rows: 300,
        cursor_page_bytes: 1024,
        cursor_idle_ms: 600,
        cursor_lifetime_ms: 1200,
    }
}

fn request_fixture(body: &[u8]) -> Vec<u8> {
    let mut fixture = LIMITS_FIXTURE.to_vec();
    fixture.extend_from_slice(body);
    fixture
}

struct LiteralFrameReader {
    payload: Vec<u8>,
    position: usize,
}

impl LiteralFrameReader {
    fn new(payload: &[u8]) -> Self {
        let mut framed = u32::try_from(payload.len())
            .expect("fixture length fits the frame prefix")
            .to_be_bytes()
            .to_vec();
        framed.extend_from_slice(payload);
        Self {
            payload: framed,
            position: 0,
        }
    }
}

impl FrameReader for LiteralFrameReader {
    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), LocalTransportError> {
        let end = self.position + output.len();
        if end > self.payload.len() {
            return Err(LocalTransportError::CredentialsUnavailable);
        }
        output.copy_from_slice(&self.payload[self.position..end]);
        self.position = end;
        Ok(())
    }
}

struct PlainAllocator;

impl FrameBufferAllocator for PlainAllocator {
    fn allocate(&mut self, length: usize) -> Result<Vec<u8>, LocalTransportError> {
        Ok(vec![0; length])
    }
}

fn boundary() -> LocalProtocolBoundary {
    LocalProtocolBoundary::with_effective_limits(fixture_limits())
}

fn assert_request_boundary(envelope: &LocalRequestEnvelope, fixture: &[u8]) {
    let boundary = boundary();
    let mut framed = u32::try_from(fixture.len())
        .expect("fixture length fits the frame prefix")
        .to_be_bytes()
        .to_vec();
    framed.extend_from_slice(fixture);
    assert_eq!(
        boundary
            .encode_frame(LocalOutboundMessage::Request(envelope))
            .expect("encode the interrupt through the production boundary"),
        framed,
    );
    let mut reader = LiteralFrameReader::new(fixture);
    let mut allocator = PlainAllocator;
    assert_eq!(
        boundary
            .receive_frame(LocalInboundKind::Request, &mut reader, &mut allocator)
            .expect("decode the interrupt through the production boundary"),
        LocalInboundMessage::Request(envelope.clone()),
    );
}

fn assert_response_boundary(response: &LocalResponse, fixture: &[u8]) {
    let boundary = boundary();
    let mut framed = u32::try_from(fixture.len())
        .expect("fixture length fits the frame prefix")
        .to_be_bytes()
        .to_vec();
    framed.extend_from_slice(fixture);
    assert_eq!(
        boundary
            .encode_frame(LocalOutboundMessage::Response {
                response,
                expectation: &LocalResponseExpectation::Custom,
            })
            .expect("encode the engine answer through the production boundary"),
        framed,
    );
    let mut reader = LiteralFrameReader::new(fixture);
    let mut allocator = PlainAllocator;
    assert_eq!(
        boundary
            .receive_frame(
                LocalInboundKind::Response(LocalResponseExpectation::Custom),
                &mut reader,
                &mut allocator,
            )
            .expect("decode the engine answer through the production boundary"),
        LocalInboundMessage::Response(response.clone()),
    );
}

fn engine_answer(error: Error) -> LocalResponse {
    LocalResponse::EngineFailure {
        failure: LocalEngineFailure::from_error(&error),
    }
}

#[test]
fn the_interrupt_frame_has_one_literal_encoding_and_names_its_request() {
    assert_request_boundary(
        &LocalRequestEnvelope {
            limits: fixture_limits(),
            request: LocalRequest::CancelInFlight { request_ordinal: 7 },
        },
        &request_fixture(CANCEL_IN_FLIGHT_BODY),
    );
    assert_request_boundary(
        &LocalRequestEnvelope {
            limits: fixture_limits(),
            request: LocalRequest::CancelInFlight {
                request_ordinal: 300,
            },
        },
        &request_fixture(CANCEL_IN_FLIGHT_LARGE_BODY),
    );
    // The ordinal is what keeps a late interrupt off the next request, so it
    // occupies its own wire position rather than being implied.
    let mut renamed = request_fixture(CANCEL_IN_FLIGHT_BODY);
    let ordinal = renamed.len() - 1;
    renamed[ordinal] = 8;
    assert_eq!(
        decode_message_exact::<LocalRequestEnvelope>(&renamed).expect("decode the renamed frame"),
        LocalRequestEnvelope {
            limits: fixture_limits(),
            request: LocalRequest::CancelInFlight { request_ordinal: 8 },
        },
    );
}

#[test]
fn the_engine_answer_frame_has_literal_bytes_for_the_answers_a_read_meets() {
    assert_response_boundary(&engine_answer(Error::ReadCancelled), ENGINE_READ_CANCELLED);
    assert_response_boundary(
        &engine_answer(Error::TableNotFound("t".to_owned())),
        ENGINE_TABLE_NOT_FOUND,
    );
}

/// The point of the read channel's error document is that the answer a caller
/// reads back is the answer the engine gave — the same class, the same fields
/// — for every class a read can meet, not just the few that would have been
/// named by hand. The channel's full class list is walked in
/// `local_transport_read_channel_error_document`; these are the ones this
/// frame's own fixtures are built around.
#[test]
fn every_class_a_read_can_meet_survives_the_round_trip_unchanged() {
    let answers = [
        // The statement itself is wrong.
        Error::ParseError("unexpected token at 3".to_owned()),
        Error::PlanError("no plan for this shape".to_owned()),
        Error::SubqueryNotSupported,
        Error::OrderByExpressionNotSupported,
        // The data model is wrong.
        Error::TableNotFound("widgets".to_owned()),
        Error::ColumnNotFound {
            table: "widgets".to_owned(),
            column: "label".to_owned(),
        },
        Error::ColumnTypeMismatch {
            table: "widgets".to_owned(),
            column: "label".to_owned(),
            expected: "TEXT".to_owned(),
            actual: "TxId".to_owned(),
        },
        Error::IndexNotFound {
            table: "widgets".to_owned(),
            index: "widgets_label_idx".to_owned(),
        },
        // The read went past what it was allowed.
        Error::BfsDepthExceeded(7),
        Error::BfsVisitedExceeded(4096),
        Error::UnboundedTraversal,
        Error::UnboundedVectorSearch,
        Error::MemoryBudgetExceeded {
            subsystem: "vector".to_owned(),
            operation: "candidate materialization".to_owned(),
            requested_bytes: 4096,
            available_bytes: 128,
            budget_limit_bytes: 8192,
            hint: "raise --read-memory".to_owned(),
        },
        // The caller may not see it.
        Error::PrincipalRequired {
            table: "widgets".to_owned(),
        },
        // The vector surface disagreed with the query.
        Error::UnknownVectorIndex {
            index: contextdb_core::types::VectorIndexRef::new("widgets", "embedding".to_owned()),
        },
        Error::UseRankRequiresLimit,
        // The read was stopped, or refused with typed detail.
        Error::ReadCancelled,
        Error::ReadFailure(
            ReadFailure::new(ReadFailureKind::OwnerTimeout, ReadFailureDetail::None)
                .expect("a canonical empty-detail refusal"),
        ),
        // The store underneath is the problem, not the query.
        Error::StoreCorrupted {
            path: "/store/widgets.db".to_owned(),
            reason: "commit index missing".to_owned(),
        },
        Error::DatabaseLocked {
            holder_pid: 4321,
            path: std::path::PathBuf::from("/store/widgets.db"),
        },
        // Prose, the last resort, still arrives as prose.
        Error::Other("the owner could not say more".to_owned()),
    ];

    for answer in answers {
        let expected = format!("{answer:?}");
        let frame = engine_answer(answer);
        let bytes = encode_message(&frame).expect("encode the engine answer");
        let decoded =
            decode_message_exact::<LocalResponse>(&bytes).expect("decode the engine answer");
        let LocalResponse::EngineFailure { failure } = decoded else {
            panic!("an engine answer decodes as an engine answer, got {decoded:?}");
        };
        let rebuilt = failure.into_error(ReadLimits::default().memory);
        assert_eq!(
            format!("{rebuilt:?}"),
            expected,
            "the answer a caller reads back must be the answer the engine gave",
        );
    }
}

/// An answer whose class this channel has no tag for -- a write-path or
/// sync-path answer a read never produces -- is not lost and not disguised: it
/// arrives as prose that names the class and repeats the engine's words.
#[test]
fn an_answer_outside_the_read_surface_arrives_named_rather_than_lost() {
    let outside = Error::SyncReplayOfAcceptedDelete {
        table: "widgets".to_owned(),
        key: vec![("id".to_owned(), Value::Int64(9))],
    };
    let words = outside.to_string();
    let frame = engine_answer(outside);
    let bytes = encode_message(&frame).expect("encode the engine answer");
    let decoded = decode_message_exact::<LocalResponse>(&bytes).expect("decode the engine answer");
    let LocalResponse::EngineFailure { failure } = decoded else {
        panic!("an engine answer decodes as an engine answer, got {decoded:?}");
    };
    let rebuilt = failure.into_error(ReadLimits::default().memory);
    let Error::Other(prose) = &rebuilt else {
        panic!("an unnamed class arrives as prose, got {rebuilt:?}");
    };
    assert!(
        prose.starts_with("SyncReplayOfAcceptedDelete: ") && prose.ends_with(&words),
        "the prose names the class and repeats the words: {prose}",
    );
}

/// An engine answer is admitted by its length against the ceiling the caller
/// declared, before the buffer that would hold it exists. A caller that asked
/// to be kept inside a small budget is not handed an unbounded decode because
/// the thing being decoded happens to be an error.
#[test]
fn an_engine_answer_is_admitted_against_the_ceiling_the_caller_declared() {
    let long = Error::Other("w".repeat(4_096));
    let frame = engine_answer(long);
    let bytes = encode_message(&frame).expect("encode the long engine answer");
    let mut framed = u32::try_from(bytes.len())
        .expect("fixture length fits the frame prefix")
        .to_be_bytes()
        .to_vec();
    framed.extend_from_slice(&bytes);

    let roomy = boundary();
    let mut reader = LiteralFrameReader::new(&bytes);
    let mut allocator = PlainAllocator;
    roomy
        .receive_frame(
            LocalInboundKind::Response(LocalResponseExpectation::Custom),
            &mut reader,
            &mut allocator,
        )
        .expect("a declared ceiling with room for the answer admits it");

    let mut narrow = fixture_limits();
    narrow.memory = 64;
    let cramped = LocalProtocolBoundary::with_effective_limits(narrow);
    let mut reader = LiteralFrameReader::new(&bytes);
    let mut allocator = PlainAllocator;
    assert!(
        cramped
            .receive_frame(
                LocalInboundKind::Response(LocalResponseExpectation::Custom),
                &mut reader,
                &mut allocator,
            )
            .is_err(),
        "an answer larger than the declared ceiling is refused before it is held",
    );
}
