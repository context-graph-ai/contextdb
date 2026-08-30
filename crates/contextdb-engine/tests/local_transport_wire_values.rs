use contextdb_core::read_contract::{
    CursorExpiryKind, CursorPage, HeldByReadersDetail, HeldByWriterDetail, MetadataItem,
    MetadataPage, MetadataPageVocabulary, OwnerLimitExceededDetail, OwnerRouteUnsupportedDetail,
    ProcessStartIdentity, ReadFailure, ReadFailureDetail, ReadFailureKind, ReadFailureLimit,
    ReaderBreadcrumb, RequiredBytesSetting, StatementRemedy,
};
use contextdb_core::{TxId, Value, VectorIndexRef};
use contextdb_engine::local_transport::{
    CursorOpenedResponse, CursorPageResponse, FrameBufferAllocator, FrameReader, FrameViolation,
    LocalInboundKind, LocalInboundMessage, LocalOutboundMessage, LocalProtocolBoundary,
    LocalResponse, LocalResponseExpectation, LocalTransportError, MetadataResponse,
    PayloadViolation, decode_message_exact, encode_message,
};
use contextdb_engine::read_contract::{
    CanonicalCascadeReport, CanonicalIndexCandidate, CanonicalQueryResult, CanonicalQueryTrace,
    ReadEncodingError, decode_cursor_page, decode_metadata_page, decode_query_result,
    encode_cursor_page, encode_metadata_page, encode_query_result,
};
use contextdb_engine::{CascadeReport, IndexCandidate, QueryResult, QueryTrace};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use uuid::Uuid;

const NULL_VALUE: &[u8] = &[0];
const BOOL_VALUE: &[u8] = &[1, 1];
const INT_VALUE: &[u8] = &[2, 13];
const FLOAT_VALUE: &[u8] = &[3, 0, 0, 0, 0, 0, 0, 0x0c, 0x40];
const TEXT_VALUE: &[u8] = &[4, 1, b'v'];
const UUID_VALUE: &[u8] = &[
    5, 16, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
    0x55, 0x55,
];
const TIMESTAMP_VALUE: &[u8] = &[6, 3];
const JSON_VALUE: &[u8] = &[7, 7, b'{', b'"', b'a', b'"', b':', b'1', b'}'];
const VECTOR_VALUE: &[u8] = &[8, 2, 0, 0, 0x80, 0x3f, 0, 0, 0, 0xc0];
const TX_ID_VALUE: &[u8] = &[9, 42];
const BOOL_FALSE_VALUE: &[u8] = &[1, 0];
const TEXT_MUTATION_VALUE: &[u8] = &[4, 1, b'w'];
const TX_ID_MUTATION_VALUE: &[u8] = &[9, 43];

const CANONICAL_QUERY_RESULT_BYTES: &[u8] = &[
    10, 1, b'n', 1, b'b', 1, b'i', 1, b'f', 1, b's', 1, b'u', 1, b't', 1, b'j', 1, b'v', 1, b'x',
    1, 10, 0, 1, 1, 2, 13, 3, 0, 0, 0, 0, 0, 0, 0x0c, 0x40, 4, 1, b'v', 5, 16, 0x55, 0x55, 0x55,
    0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 6, 3, 7, 7, b'{',
    b'"', b'a', b'"', b':', b'1', b'}', 8, 2, 0, 0, 0x80, 0x3f, 0, 0, 0, 0xc0, 9, 42, 2, 1, b'P',
    1, 1, b'I', 1, 1, b'p', 1, 1, b'i', 1, b'r', 1, 1, 1, b't', 1, b'v', 3, 1, 1, 1, b'd',
];

const HELD_BY_READERS_FAILURE: &[u8] = &[9, 2, 1, 2, 1, 7, 1, b'r', 9];
// The writer-contention detail is the newest member of the detail vocabulary,
// so it takes the position after every detail that was already encoded and
// none of the checked-in fixtures above move.
const HELD_BY_WRITER_FAILURE: &[u8] = &[9, 1, 5, 1, 7, 1, b'w'];
const HELD_BY_WRITER_UNPUBLISHED_FAILURE: &[u8] = &[9, 1, 5, 0, 1, b'w'];
// The unsupported-inspection detail is newer still, so it takes the position
// after the writer-contention detail and nothing already encoded moves.
const OWNER_ROUTE_UNSUPPORTED_FAILURE: &[u8] = &[9, 22, 6, 1, b'i'];
const REASON_FAILURE: &[u8] = &[9, 4, 4, 3, b'w', b'h', b'y'];
const RESULT_ROWS_LIMIT_FAILURE: &[u8] = &[9, 8, 2, 0, 10, 0, 0];
const RESULT_BYTES_LIMIT_FAILURE: &[u8] = &[9, 8, 2, 1, 11, 1, 12, 1, b'r', 0];
const WORK_LIMIT_FAILURE: &[u8] = &[9, 8, 2, 2, 13, 0, 1, 1, b's', 1, b'm'];
const ACTIVE_TIME_LIMIT_FAILURE: &[u8] = &[9, 8, 2, 3, 14, 1, 15, 1, b'r', 1, 1, b's', 1, b'm'];
const MEMORY_LIMIT_FAILURE: &[u8] = &[9, 8, 2, 4, 16, 1, 17, 2, b'm', b'b', 0];
const CURSOR_PAGE_BYTES_LIMIT_FAILURE: &[u8] = &[9, 8, 2, 5, 18, 0, 1, 1, b'p', 1, b'c'];
const CURSOR_IDLE_FAILURE: &[u8] = &[9, 13, 3, 0];
const CURSOR_LIFETIME_FAILURE: &[u8] = &[9, 13, 3, 1];

const CURSOR_PAGE_BYTES: &[u8] = &[2, 1, b'i', 1, b't', 1, 2, 2, 2, 9, 42, 1];
const CURSOR_OPENED_RESPONSE: &[u8] = &[
    2, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    0x44, 12, 2, 1, b'i', 1, b't', 1, 2, 2, 2, 9, 42, 1,
];
const CURSOR_PAGE_RESPONSE: &[u8] = &[3, 12, 2, 1, b'i', 1, b't', 1, 2, 2, 2, 9, 42, 1];
const TABLE_METADATA_BYTES: &[u8] = &[0, 1, 0, 1, b't', 1, 1, 1, b'n'];
const TABLE_METADATA_RESPONSE: &[u8] = &[5, 9, 0, 1, 0, 1, b't', 1, 1, 1, b'n'];
const EVENTS_METADATA_BYTES: &[u8] = &[1, 1, 1, 1, 2, b'o', b'k', 1, 1, 0, 0];
const EVENTS_METADATA_RESPONSE: &[u8] = &[5, 11, 1, 1, 1, 1, 2, b'o', b'k', 1, 1, 0, 0];

fn assert_codec<T>(value: T, fixture: &[u8])
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    assert_eq!(
        encode_message(&value).expect("encode literal fixture"),
        fixture
    );
    assert_eq!(
        decode_message_exact::<T>(fixture).expect("decode literal fixture"),
        value
    );
}

struct ResponseReader {
    bytes: Vec<u8>,
    offset: usize,
}

impl FrameReader for ResponseReader {
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

struct ResponseAllocator;

impl FrameBufferAllocator for ResponseAllocator {
    fn allocate(&mut self, length: usize) -> Result<Vec<u8>, LocalTransportError> {
        Ok(vec![0; length])
    }
}

fn response_boundary() -> LocalProtocolBoundary {
    LocalProtocolBoundary::with_effective_limits(contextdb_core::read_contract::ReadLimits {
        result_rows: 500,
        result_bytes: 4_096,
        work: 5_000,
        active_ms: 1_000,
        memory: 8_192,
        cursor_page_rows: 300,
        cursor_page_bytes: 1_024,
        cursor_idle_ms: 600,
        cursor_lifetime_ms: 1_200,
    })
}

fn assert_response_codec(
    value: LocalResponse,
    expectation: LocalResponseExpectation,
    fixture: &[u8],
) {
    let boundary = response_boundary();
    let mut frame = Vec::with_capacity(fixture.len() + 4);
    frame.extend_from_slice(&(fixture.len() as u32).to_be_bytes());
    frame.extend_from_slice(fixture);
    assert_eq!(
        boundary
            .encode_frame(LocalOutboundMessage::Response {
                response: &value,
                expectation: &expectation,
            })
            .expect("encode response through the production boundary"),
        frame
    );
    let mut bytes = Vec::with_capacity(fixture.len() + 4);
    bytes.extend_from_slice(&(fixture.len() as u32).to_be_bytes());
    bytes.extend_from_slice(fixture);
    let mut reader = ResponseReader { bytes, offset: 0 };
    let mut allocator = ResponseAllocator;
    assert_eq!(
        boundary
            .receive_frame(
                LocalInboundKind::Response(expectation),
                &mut reader,
                &mut allocator,
            )
            .expect("decode response through the production boundary"),
        LocalInboundMessage::Response(value)
    );
}

fn assert_wire_damage_is_rejected<T>(fixture: &[u8])
where
    T: DeserializeOwned + PartialEq + std::fmt::Debug,
{
    assert_eq!(
        decode_message_exact::<T>(&fixture[..fixture.len() - 1]),
        Err(LocalTransportError::Payload(PayloadViolation::Malformed))
    );
    let mut trailing = fixture.to_vec();
    trailing.push(0);
    assert_eq!(
        decode_message_exact::<T>(&trailing),
        Err(LocalTransportError::Payload(
            PayloadViolation::TrailingBytes
        ))
    );
}

fn failure(kind: ReadFailureKind, detail: ReadFailureDetail) -> LocalResponse {
    LocalResponse::Failure {
        failure: ReadFailure::new(kind, detail)
            .expect("the fixture detail belongs to the failure kind"),
    }
}

fn required(bytes: u64, setting: &str) -> RequiredBytesSetting {
    RequiredBytesSetting {
        required_bytes: bytes,
        required_setting: setting.to_owned(),
    }
}

fn statement(statement: &str, remedy: &str) -> StatementRemedy {
    StatementRemedy {
        statement: statement.to_owned(),
        remedy_command: remedy.to_owned(),
    }
}

fn query_result() -> QueryResult {
    QueryResult {
        columns: ["n", "b", "i", "f", "s", "u", "t", "j", "v", "x"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        rows: vec![vec![
            Value::Null,
            Value::Bool(true),
            Value::Int64(-7),
            Value::Float64(3.5),
            Value::Text("v".to_owned()),
            Value::Uuid(Uuid::from_bytes([0x55; 16])),
            Value::Timestamp(-2),
            Value::Json(serde_json::json!({"a": 1})),
            Value::Vector(vec![1.0, -2.0]),
            Value::TxId(TxId(42)),
        ]],
        rows_affected: 2,
        trace: QueryTrace {
            physical_plan: "P",
            index_used: Some("I".to_owned()),
            predicates_pushed: smallvec::smallvec![std::borrow::Cow::Borrowed("p")],
            indexes_considered: smallvec::smallvec![IndexCandidate {
                name: "i".to_owned(),
                rejected_reason: std::borrow::Cow::Borrowed("r"),
            }],
            sort_elided: true,
            query_vector_source: Some(VectorIndexRef::new("t", "v")),
            rows_examined: 3,
        },
        cascade: Some(CascadeReport {
            dropped_indexes: vec!["d".to_owned()],
        }),
    }
}

fn canonical_query_result() -> CanonicalQueryResult {
    CanonicalQueryResult {
        columns: ["n", "b", "i", "f", "s", "u", "t", "j", "v", "x"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        rows: query_result().rows,
        rows_affected: 2,
        trace: CanonicalQueryTrace {
            physical_plan: "P".to_owned(),
            index_used: Some("I".to_owned()),
            predicates_pushed: vec!["p".to_owned()],
            indexes_considered: vec![CanonicalIndexCandidate {
                name: "i".to_owned(),
                rejected_reason: "r".to_owned(),
            }],
            sort_elided: true,
            query_vector_source: Some(VectorIndexRef::new("t", "v")),
            rows_examined: 3,
        },
        cascade: Some(CanonicalCascadeReport {
            dropped_indexes: vec!["d".to_owned()],
        }),
    }
}

fn assert_query_mutation(
    result: QueryResult,
    expected: CanonicalQueryResult,
    byte_offset: usize,
    expected_byte: u8,
) {
    let mut expected_bytes = CANONICAL_QUERY_RESULT_BYTES.to_vec();
    expected_bytes[byte_offset] = expected_byte;
    assert_eq!(
        encode_query_result(&result).expect("encode canonical query mutation"),
        expected_bytes
    );
    assert_eq!(
        decode_query_result(&expected_bytes).expect("decode canonical query mutation"),
        expected
    );
}

#[test]
fn all_ten_value_variants_have_independent_literal_bytes() {
    let cases = [
        (Value::Null, NULL_VALUE),
        (Value::Bool(true), BOOL_VALUE),
        (Value::Int64(-7), INT_VALUE),
        (Value::Float64(3.5), FLOAT_VALUE),
        (Value::Text("v".to_owned()), TEXT_VALUE),
        (Value::Uuid(Uuid::from_bytes([0x55; 16])), UUID_VALUE),
        (Value::Timestamp(-2), TIMESTAMP_VALUE),
        (Value::Json(serde_json::json!({"a": 1})), JSON_VALUE),
        (Value::Vector(vec![1.0, -2.0]), VECTOR_VALUE),
        (Value::TxId(TxId(42)), TX_ID_VALUE),
    ];
    for (value, fixture) in cases {
        assert_codec(value, fixture);
        assert_wire_damage_is_rejected::<Value>(fixture);
    }

    assert_codec(Value::Bool(false), BOOL_FALSE_VALUE);
    assert_codec(Value::Text("w".to_owned()), TEXT_MUTATION_VALUE);
    assert_codec(Value::TxId(TxId(43)), TX_ID_MUTATION_VALUE);
    assert_eq!(
        decode_message_exact::<Value>(&[250]),
        Err(LocalTransportError::Payload(PayloadViolation::Malformed))
    );
}

#[test]
fn canonical_query_result_has_one_independent_literal_and_field_mutation_matrix() {
    let base = query_result();
    let expected = canonical_query_result();
    assert_eq!(
        encode_query_result(&base).expect("encode canonical query result"),
        CANONICAL_QUERY_RESULT_BYTES
    );
    assert_eq!(
        decode_query_result(CANONICAL_QUERY_RESULT_BYTES)
            .expect("decode independent canonical query bytes"),
        expected
    );
    let mut terminal_fixture = vec![1, 106];
    terminal_fixture.extend_from_slice(CANONICAL_QUERY_RESULT_BYTES);
    assert_response_codec(
        LocalResponse::TerminalSuccess {
            success: contextdb_engine::local_transport::TerminalSuccess {
                final_bytes: CANONICAL_QUERY_RESULT_BYTES.to_vec(),
            },
        },
        LocalResponseExpectation::OrdinaryResult,
        &terminal_fixture,
    );

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.columns[0] = "N".to_owned();
    canonical.columns[0] = "N".to_owned();
    assert_query_mutation(result, canonical, 2, b'N');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][1] = Value::Bool(false);
    canonical.rows[0][1] = Value::Bool(false);
    assert_query_mutation(result, canonical, 25, 0);

    let mut invalid_null = CANONICAL_QUERY_RESULT_BYTES.to_vec();
    invalid_null[23] = 250;
    assert!(matches!(
        decode_query_result(&invalid_null),
        Err(ReadEncodingError::InvalidPayload)
    ));

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][2] = Value::Int64(-8);
    canonical.rows[0][2] = Value::Int64(-8);
    assert_query_mutation(result, canonical, 27, 15);

    let adjacent_float = f64::from_bits(3.5_f64.to_bits() + 1);
    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][3] = Value::Float64(adjacent_float);
    canonical.rows[0][3] = Value::Float64(adjacent_float);
    assert_query_mutation(result, canonical, 29, 1);

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][4] = Value::Text("w".to_owned());
    canonical.rows[0][4] = Value::Text("w".to_owned());
    assert_query_mutation(result, canonical, 39, b'w');

    let mut changed_uuid = [0x55; 16];
    changed_uuid[0] = 0x54;
    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][5] = Value::Uuid(Uuid::from_bytes(changed_uuid));
    canonical.rows[0][5] = Value::Uuid(Uuid::from_bytes(changed_uuid));
    assert_query_mutation(result, canonical, 42, 0x54);

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][6] = Value::Timestamp(-3);
    canonical.rows[0][6] = Value::Timestamp(-3);
    assert_query_mutation(result, canonical, 59, 5);

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][7] = Value::Json(serde_json::json!({"a": 2}));
    canonical.rows[0][7] = Value::Json(serde_json::json!({"a": 2}));
    assert_query_mutation(result, canonical, 67, b'2');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][8] = Value::Vector(vec![0.5, -2.0]);
    canonical.rows[0][8] = Value::Vector(vec![0.5, -2.0]);
    assert_query_mutation(result, canonical, 73, 0);

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows[0][9] = Value::TxId(TxId(43));
    canonical.rows[0][9] = Value::TxId(TxId(43));
    assert_query_mutation(result, canonical, 80, 43);

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.rows_affected = 3;
    canonical.rows_affected = 3;
    assert_query_mutation(result, canonical, 81, 3);

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.physical_plan = "Q";
    canonical.trace.physical_plan = "Q".to_owned();
    assert_query_mutation(result, canonical, 83, b'Q');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.index_used = Some("J".to_owned());
    canonical.trace.index_used = Some("J".to_owned());
    assert_query_mutation(result, canonical, 86, b'J');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.predicates_pushed[0] = std::borrow::Cow::Borrowed("q");
    canonical.trace.predicates_pushed[0] = "q".to_owned();
    assert_query_mutation(result, canonical, 89, b'q');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.indexes_considered[0].name = "k".to_owned();
    canonical.trace.indexes_considered[0].name = "k".to_owned();
    assert_query_mutation(result, canonical, 92, b'k');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.indexes_considered[0].rejected_reason = std::borrow::Cow::Borrowed("s");
    canonical.trace.indexes_considered[0].rejected_reason = "s".to_owned();
    assert_query_mutation(result, canonical, 94, b's');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.sort_elided = false;
    canonical.trace.sort_elided = false;
    assert_query_mutation(result, canonical, 95, 0);

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.query_vector_source = Some(VectorIndexRef::new("u", "v"));
    canonical.trace.query_vector_source = Some(VectorIndexRef::new("u", "v"));
    assert_query_mutation(result, canonical, 98, b'u');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.query_vector_source = Some(VectorIndexRef::new("t", "w"));
    canonical.trace.query_vector_source = Some(VectorIndexRef::new("t", "w"));
    assert_query_mutation(result, canonical, 100, b'w');

    let mut result = base.clone();
    let mut canonical = expected.clone();
    result.trace.rows_examined = 4;
    canonical.trace.rows_examined = 4;
    assert_query_mutation(result, canonical, 101, 4);

    let mut result = base;
    let mut canonical = expected;
    result.cascade.as_mut().expect("cascade").dropped_indexes[0] = "e".to_owned();
    canonical
        .cascade
        .as_mut()
        .expect("canonical cascade")
        .dropped_indexes[0] = "e".to_owned();
    assert_query_mutation(result, canonical, 105, b'e');

    assert!(matches!(
        decode_query_result(
            &CANONICAL_QUERY_RESULT_BYTES[..CANONICAL_QUERY_RESULT_BYTES.len() - 1]
        ),
        Err(ReadEncodingError::InvalidPayload)
    ));
    let mut trailing = CANONICAL_QUERY_RESULT_BYTES.to_vec();
    trailing.push(0);
    assert!(matches!(
        decode_query_result(&trailing),
        Err(ReadEncodingError::InvalidPayload)
    ));
}

#[test]
fn every_failure_kind_and_specialized_detail_has_literal_bytes() {
    let empty_details = [
        (ReadFailureKind::WriteRequiresFlag, &[9, 0, 0][..]),
        (ReadFailureKind::HeldByWriter, &[9, 1, 0][..]),
        (ReadFailureKind::OwnerNotRunning, &[9, 3, 0][..]),
        (ReadFailureKind::OwnerNotServing, &[9, 4, 0][..]),
        (ReadFailureKind::OwnerUserMismatch, &[9, 5, 0][..]),
        (ReadFailureKind::OwnerMismatch, &[9, 6, 0][..]),
        (ReadFailureKind::OwnerAtCapacity, &[9, 7, 0][..]),
        (ReadFailureKind::OwnerTimeout, &[9, 9, 0][..]),
        (ReadFailureKind::OwnerDisconnected, &[9, 10, 0][..]),
        (ReadFailureKind::InvalidChannelData, &[9, 11, 0][..]),
        (ReadFailureKind::LocalProtocolMismatch, &[9, 12, 0][..]),
        (ReadFailureKind::CursorNotFound, &[9, 14, 0][..]),
        (ReadFailureKind::DirectReadRequiresWriter, &[9, 15, 0][..]),
        (ReadFailureKind::StoreNotFound, &[9, 16, 0][..]),
        (ReadFailureKind::InvalidContinuation, &[9, 17, 0][..]),
        (ReadFailureKind::CursorAlreadyOpen, &[9, 18, 0][..]),
        (ReadFailureKind::CursorTransactionActive, &[9, 19, 0][..]),
        (ReadFailureKind::CursorInvalidStatement, &[9, 20, 0][..]),
        (ReadFailureKind::OperationAlreadyCompleted, &[9, 21, 0][..]),
    ];
    for (kind, fixture) in empty_details {
        assert_response_codec(
            failure(kind, ReadFailureDetail::None),
            LocalResponseExpectation::Custom,
            fixture,
        );
        assert_wire_damage_is_rejected::<LocalResponse>(fixture);
    }

    let held = ReadFailure::held_by_readers(HeldByReadersDetail {
        observed_direct_readers: 2,
        verified_readers: vec![ReaderBreadcrumb {
            process_id: 7,
            process_name: "r".to_owned(),
            process_start: ProcessStartIdentity(9),
        }],
    })
    .expect("valid concrete direct-reader evidence");
    assert_response_codec(
        LocalResponse::Failure { failure: held },
        LocalResponseExpectation::Custom,
        HELD_BY_READERS_FAILURE,
    );
    assert_response_codec(
        failure(
            ReadFailureKind::OwnerNotServing,
            ReadFailureDetail::Reason {
                reason: "why".to_owned(),
            },
        ),
        LocalResponseExpectation::Custom,
        REASON_FAILURE,
    );
    assert_response_codec(
        failure(
            ReadFailureKind::HeldByWriter,
            ReadFailureDetail::HeldByWriter(HeldByWriterDetail {
                process_id: Some(7),
                store_path: "w".to_owned(),
            }),
        ),
        LocalResponseExpectation::Custom,
        HELD_BY_WRITER_FAILURE,
    );
    assert_wire_damage_is_rejected::<LocalResponse>(HELD_BY_WRITER_FAILURE);
    assert_response_codec(
        failure(
            ReadFailureKind::HeldByWriter,
            ReadFailureDetail::HeldByWriter(HeldByWriterDetail {
                process_id: None,
                store_path: "w".to_owned(),
            }),
        ),
        LocalResponseExpectation::Custom,
        HELD_BY_WRITER_UNPUBLISHED_FAILURE,
    );
    assert_wire_damage_is_rejected::<LocalResponse>(HELD_BY_WRITER_UNPUBLISHED_FAILURE);
    assert_response_codec(
        failure(
            ReadFailureKind::OwnerRouteUnsupported,
            ReadFailureDetail::OwnerRouteUnsupported(OwnerRouteUnsupportedDetail {
                inspection: "i".to_owned(),
            }),
        ),
        LocalResponseExpectation::Custom,
        OWNER_ROUTE_UNSUPPORTED_FAILURE,
    );
    assert_wire_damage_is_rejected::<LocalResponse>(OWNER_ROUTE_UNSUPPORTED_FAILURE);
    assert_response_codec(
        LocalResponse::Failure {
            failure: ReadFailure::cursor_expired(CursorExpiryKind::Idle),
        },
        LocalResponseExpectation::CursorFetch,
        CURSOR_IDLE_FAILURE,
    );
    assert_response_codec(
        LocalResponse::Failure {
            failure: ReadFailure::cursor_expired(CursorExpiryKind::Lifetime),
        },
        LocalResponseExpectation::CursorFetch,
        CURSOR_LIFETIME_FAILURE,
    );

    assert_eq!(
        decode_message_exact::<LocalResponse>(&[9, 2, 0]),
        Err(LocalTransportError::Payload(PayloadViolation::Malformed)),
        "reader contention cannot carry an empty detail"
    );
    assert_eq!(
        decode_message_exact::<LocalResponse>(&[9, 22, 0]),
        Err(LocalTransportError::Payload(PayloadViolation::Malformed)),
        "a refusal that exists to name an inspection cannot arrive without one"
    );
    assert_eq!(
        decode_message_exact::<LocalResponse>(&[9, 22, 4, 1, b'r']),
        Err(LocalTransportError::Payload(PayloadViolation::Malformed)),
        "an ordinary sentence is not the named inspection this refusal owes"
    );
    assert_eq!(
        decode_message_exact::<LocalResponse>(&[9, 8, 1, 0, 10, 0, 0]),
        Err(LocalTransportError::Payload(PayloadViolation::Malformed)),
        "the former owner-limit detail discriminant must not decode after reader evidence was inserted"
    );
    assert_eq!(
        decode_message_exact::<LocalResponse>(&[9, 2, 1, 0, 0]),
        Err(LocalTransportError::Payload(PayloadViolation::Malformed)),
        "zero observed readers is malformed specialized evidence"
    );
}

#[test]
fn reader_evidence_identity_is_process_and_start_not_mutable_display_name() {
    let duplicate_identity = HeldByReadersDetail {
        observed_direct_readers: 2,
        verified_readers: vec![
            ReaderBreadcrumb {
                process_id: 7,
                process_name: "before".to_owned(),
                process_start: ProcessStartIdentity(9),
            },
            ReaderBreadcrumb {
                process_id: 7,
                process_name: "after".to_owned(),
                process_start: ProcessStartIdentity(9),
            },
        ],
    };
    assert!(
        duplicate_identity.validate().is_err(),
        "changing a process display name cannot make one stable reader identity unique"
    );

    let distinct_start = HeldByReadersDetail {
        observed_direct_readers: 2,
        verified_readers: vec![
            ReaderBreadcrumb {
                process_id: 7,
                process_name: "same".to_owned(),
                process_start: ProcessStartIdentity(9),
            },
            ReaderBreadcrumb {
                process_id: 7,
                process_name: "same".to_owned(),
                process_start: ProcessStartIdentity(10),
            },
        ],
    };
    assert!(distinct_start.validate().is_ok());
}

#[test]
fn every_limit_detail_and_optional_remedy_shape_has_literal_bytes() {
    let cases = [
        (
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::ResultRows,
                value: 10,
                required: None,
                statement: None,
            },
            RESULT_ROWS_LIMIT_FAILURE,
        ),
        (
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::ResultBytes,
                value: 11,
                required: Some(required(12, "r")),
                statement: None,
            },
            RESULT_BYTES_LIMIT_FAILURE,
        ),
        (
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::Work,
                value: 13,
                required: None,
                statement: Some(statement("s", "m")),
            },
            WORK_LIMIT_FAILURE,
        ),
        (
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::ActiveMs,
                value: 14,
                required: Some(required(15, "r")),
                statement: Some(statement("s", "m")),
            },
            ACTIVE_TIME_LIMIT_FAILURE,
        ),
        (
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::Memory,
                value: 16,
                required: Some(required(17, "mb")),
                statement: None,
            },
            MEMORY_LIMIT_FAILURE,
        ),
        (
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::CursorPageBytes,
                value: 18,
                required: None,
                statement: Some(statement("p", "c")),
            },
            CURSOR_PAGE_BYTES_LIMIT_FAILURE,
        ),
    ];
    for (detail, fixture) in cases {
        assert_response_codec(
            LocalResponse::Failure {
                failure: ReadFailure::owner_limit_exceeded(detail),
            },
            LocalResponseExpectation::Custom,
            fixture,
        );
        assert_wire_damage_is_rejected::<LocalResponse>(fixture);
    }
}

fn assert_failure_mutation(response: LocalResponse, fixture: &[u8], offset: usize, byte: u8) {
    let mut expected = fixture.to_vec();
    expected[offset] = byte;
    assert_response_codec(response, LocalResponseExpectation::Custom, &expected);
}

#[test]
fn every_failure_detail_field_changes_an_independent_literal_position() {
    let held = |observed, process_id, process_name: &str, process_start| LocalResponse::Failure {
        failure: ReadFailure::held_by_readers(HeldByReadersDetail {
            observed_direct_readers: observed,
            verified_readers: vec![ReaderBreadcrumb {
                process_id,
                process_name: process_name.to_owned(),
                process_start: ProcessStartIdentity(process_start),
            }],
        })
        .expect("valid mutated reader evidence"),
    };
    assert_failure_mutation(held(3, 7, "r", 9), HELD_BY_READERS_FAILURE, 3, 3);
    assert_failure_mutation(held(2, 8, "r", 9), HELD_BY_READERS_FAILURE, 5, 8);
    assert_failure_mutation(held(2, 7, "s", 9), HELD_BY_READERS_FAILURE, 7, b's');
    assert_failure_mutation(held(2, 7, "r", 10), HELD_BY_READERS_FAILURE, 8, 10);

    assert_failure_mutation(
        failure(
            ReadFailureKind::OwnerNotServing,
            ReadFailureDetail::Reason {
                reason: "whz".to_owned(),
            },
        ),
        REASON_FAILURE,
        6,
        b'z',
    );

    let held_by_writer = |process_id, store_path: &str| {
        failure(
            ReadFailureKind::HeldByWriter,
            ReadFailureDetail::HeldByWriter(HeldByWriterDetail {
                process_id,
                store_path: store_path.to_owned(),
            }),
        )
    };
    assert_failure_mutation(held_by_writer(Some(8), "w"), HELD_BY_WRITER_FAILURE, 4, 8);
    assert_failure_mutation(
        held_by_writer(Some(7), "x"),
        HELD_BY_WRITER_FAILURE,
        6,
        b'x',
    );
    assert_failure_mutation(
        held_by_writer(None, "x"),
        HELD_BY_WRITER_UNPUBLISHED_FAILURE,
        5,
        b'x',
    );

    let detail = |limit, value, required_bytes, setting: &str, sql: &str, remedy: &str| {
        LocalResponse::Failure {
            failure: ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
                limit,
                value,
                required: Some(required(required_bytes, setting)),
                statement: Some(statement(sql, remedy)),
            }),
        }
    };
    assert_failure_mutation(
        detail(ReadFailureLimit::Work, 14, 15, "r", "s", "m"),
        ACTIVE_TIME_LIMIT_FAILURE,
        3,
        2,
    );
    assert_failure_mutation(
        detail(ReadFailureLimit::ActiveMs, 15, 15, "r", "s", "m"),
        ACTIVE_TIME_LIMIT_FAILURE,
        4,
        15,
    );
    assert_failure_mutation(
        detail(ReadFailureLimit::ActiveMs, 14, 16, "r", "s", "m"),
        ACTIVE_TIME_LIMIT_FAILURE,
        6,
        16,
    );
    assert_failure_mutation(
        detail(ReadFailureLimit::ActiveMs, 14, 15, "q", "s", "m"),
        ACTIVE_TIME_LIMIT_FAILURE,
        8,
        b'q',
    );
    assert_failure_mutation(
        detail(ReadFailureLimit::ActiveMs, 14, 15, "r", "t", "m"),
        ACTIVE_TIME_LIMIT_FAILURE,
        11,
        b't',
    );
    assert_failure_mutation(
        detail(ReadFailureLimit::ActiveMs, 14, 15, "r", "s", "n"),
        ACTIVE_TIME_LIMIT_FAILURE,
        13,
        b'n',
    );
    assert_failure_mutation(
        LocalResponse::Failure {
            failure: ReadFailure::cursor_expired(CursorExpiryKind::Lifetime),
        },
        CURSOR_IDLE_FAILURE,
        3,
        1,
    );
}

fn cursor_page() -> CursorPage {
    CursorPage {
        columns: vec!["i".to_owned(), "t".to_owned()],
        rows: vec![vec![Value::Int64(1), Value::TxId(TxId(42))]],
        has_more: true,
    }
}

fn table_metadata() -> MetadataPage {
    MetadataPage {
        vocabulary: MetadataPageVocabulary::Tables,
        items: vec![MetadataItem::Table("t".to_owned())],
        has_more: true,
        continuation: Some("n".to_owned()),
    }
}

fn event_metadata() -> MetadataPage {
    let mut item = BTreeMap::new();
    item.insert("ok".to_owned(), Value::Bool(true));
    MetadataPage {
        vocabulary: MetadataPageVocabulary::EventsStatus,
        items: vec![MetadataItem::EventStatus(item)],
        has_more: false,
        continuation: None,
    }
}

#[test]
fn canonical_cursor_and_metadata_payloads_keep_their_literal_bytes_inside_responses() {
    assert_eq!(
        encode_cursor_page(&cursor_page()).expect("encode canonical cursor page"),
        CURSOR_PAGE_BYTES
    );
    assert_eq!(
        decode_cursor_page(CURSOR_PAGE_BYTES).expect("decode canonical cursor page"),
        cursor_page()
    );
    assert_response_codec(
        LocalResponse::CursorOpened {
            opened: CursorOpenedResponse {
                cursor_id: [0x44; 16],
                payload: CURSOR_PAGE_BYTES.to_vec(),
            },
        },
        LocalResponseExpectation::CursorOpen,
        CURSOR_OPENED_RESPONSE,
    );
    assert_response_codec(
        LocalResponse::CursorPage {
            page: CursorPageResponse {
                payload: CURSOR_PAGE_BYTES.to_vec(),
            },
        },
        LocalResponseExpectation::CursorFetch,
        CURSOR_PAGE_RESPONSE,
    );

    assert_eq!(
        encode_metadata_page(&table_metadata()).expect("encode resumable table metadata"),
        TABLE_METADATA_BYTES
    );
    assert_eq!(
        decode_metadata_page(TABLE_METADATA_BYTES).expect("decode resumable table metadata"),
        table_metadata()
    );
    assert_response_codec(
        LocalResponse::Metadata {
            metadata: MetadataResponse {
                payload: TABLE_METADATA_BYTES.to_vec(),
            },
        },
        LocalResponseExpectation::Metadata(
            contextdb_engine::local_transport::LocalMetadataRequest::Tables { continuation: None },
        ),
        TABLE_METADATA_RESPONSE,
    );

    assert_eq!(
        encode_metadata_page(&event_metadata()).expect("encode final event metadata"),
        EVENTS_METADATA_BYTES
    );
    assert_eq!(
        decode_metadata_page(EVENTS_METADATA_BYTES).expect("decode final event metadata"),
        event_metadata()
    );
    assert_response_codec(
        LocalResponse::Metadata {
            metadata: MetadataResponse {
                payload: EVENTS_METADATA_BYTES.to_vec(),
            },
        },
        LocalResponseExpectation::Metadata(
            contextdb_engine::local_transport::LocalMetadataRequest::EventsStatus {
                continuation: None,
            },
        ),
        EVENTS_METADATA_RESPONSE,
    );
}
