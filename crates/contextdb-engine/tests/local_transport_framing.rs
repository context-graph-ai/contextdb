use contextdb_core::Value;
use contextdb_core::read_contract::{ReadFailure, ReadFailureDetail, ReadFailureKind, ReadLimits};
use contextdb_engine::local_transport::{
    FrameBufferAllocator, FrameReader, FrameViolation, LocalOutboundMessage, LocalProtocolBoundary,
    LocalResponse, LocalResponseExpectation, LocalTransportError, MAX_FRAME_BYTES,
    OrdinaryResultReceiver, ResultChunk, ResultReceiveOutcome, ResultReceiveState, TerminalSuccess,
    decode_frame_exact, encode_message, encode_payload_frame, frame_length_prefix,
    read_payload_with_admission, split_canonical_result,
};
use contextdb_engine::read_contract::{
    CanonicalIndexCandidate, CanonicalQueryResult, decode_query_result, encode_query_result,
};
use contextdb_engine::{CascadeReport, IndexCandidate, QueryResult, QueryTrace};

struct InstrumentedReader {
    prefix: [u8; 4],
    fill: u8,
    reads: usize,
}

impl FrameReader for InstrumentedReader {
    fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), LocalTransportError> {
        self.reads += 1;
        if self.reads == 1 {
            assert_eq!(
                bytes.len(),
                4,
                "the prefix is read into a fixed stack buffer"
            );
            bytes.copy_from_slice(&self.prefix);
        } else {
            bytes.fill(self.fill);
        }
        Ok(())
    }
}

struct AllocationLimit {
    maximum: usize,
    calls: usize,
}

struct SliceReader {
    bytes: Vec<u8>,
    offset: usize,
}

impl SliceReader {
    fn new(bytes: Vec<u8>) -> Self {
        Self { bytes, offset: 0 }
    }
}

impl FrameReader for SliceReader {
    fn read_exact(&mut self, destination: &mut [u8]) -> Result<(), LocalTransportError> {
        let end = self.offset.saturating_add(destination.len());
        let Some(source) = self.bytes.get(self.offset..end) else {
            return Err(LocalTransportError::Frame(FrameViolation::TruncatedPayload));
        };
        destination.copy_from_slice(source);
        self.offset = end;
        Ok(())
    }
}

impl FrameBufferAllocator for AllocationLimit {
    fn allocate(&mut self, length: usize) -> Result<Vec<u8>, LocalTransportError> {
        self.calls += 1;
        if length > self.maximum {
            return Err(LocalTransportError::Frame(
                FrameViolation::LengthExceedsMaximum,
            ));
        }
        Ok(vec![0; length])
    }
}

#[test]
fn local_frame_fixture_encodes_and_decodes_exactly() {
    let payload = [0x73, 0x74];
    let fixture = [0x00, 0x00, 0x00, 0x02, 0x73, 0x74];
    assert_eq!(
        encode_payload_frame(&payload).expect("encode fixed frame fixture"),
        fixture
    );
    assert_eq!(
        decode_frame_exact(&fixture).expect("decode fixed frame fixture"),
        payload
    );
}

#[test]
fn local_frame_payload_matrix_is_exact_and_never_accepts_partial_or_trailing_data() {
    assert_eq!(
        decode_frame_exact(&[0, 0, 0]),
        Err(LocalTransportError::Frame(FrameViolation::TruncatedLength))
    );
    assert_eq!(
        decode_frame_exact(&[0, 0, 0, 3, 0x73]),
        Err(LocalTransportError::Frame(FrameViolation::TruncatedPayload))
    );
    assert_eq!(
        decode_frame_exact(&[0, 0, 0, 1, 0x73, 0x74]),
        Err(LocalTransportError::Frame(FrameViolation::TrailingBytes))
    );

    let mut mutated = [0, 0, 0, 2, 0x73, 0x74];
    mutated[3] = 3;
    assert_eq!(
        decode_frame_exact(&mutated),
        Err(LocalTransportError::Frame(FrameViolation::TruncatedPayload))
    );
}

#[test]
fn declared_frame_length_is_refused_before_the_payload_allocator_is_called() {
    let mut reader = InstrumentedReader {
        prefix: (MAX_FRAME_BYTES as u32 + 1).to_be_bytes(),
        fill: 0xa7,
        reads: 0,
    };
    let mut allocator = AllocationLimit {
        maximum: MAX_FRAME_BYTES,
        calls: 0,
    };

    let result = read_payload_with_admission(&mut reader, &mut allocator);
    assert_eq!(reader.reads, 1, "only the fixed prefix may be read first");
    assert_eq!(
        allocator.calls, 0,
        "the oversized declaration must allocate nothing"
    );
    assert_eq!(
        result,
        Err(LocalTransportError::Frame(
            FrameViolation::LengthExceedsMaximum
        ))
    );
}

#[test]
fn four_mebibytes_is_admitted_through_the_allocation_limited_reader_seam() {
    let mut reader = InstrumentedReader {
        prefix: (MAX_FRAME_BYTES as u32).to_be_bytes(),
        fill: 0xa7,
        reads: 0,
    };
    let mut allocator = AllocationLimit {
        maximum: MAX_FRAME_BYTES,
        calls: 0,
    };

    let payload = read_payload_with_admission(&mut reader, &mut allocator)
        .expect("the exact frame ceiling is admitted");
    assert_eq!(reader.reads, 2, "the payload is read only after admission");
    assert_eq!(
        allocator.calls, 1,
        "one admitted payload buffer is allocated"
    );
    assert_eq!(payload, vec![0xa7; MAX_FRAME_BYTES]);
}

#[test]
fn ordinary_result_chunks_stay_unpublished_until_terminal_success() {
    let canonical = encode_query_result(&QueryResult {
        columns: vec!["value".to_owned()],
        rows: vec![vec![Value::Null]],
        rows_affected: 0,
        trace: QueryTrace::scan(),
        cascade: None,
    })
    .expect("encode a valid canonical ordinary result");
    assert!(canonical.len() >= 3);
    let first_end = canonical.len() / 3;
    let second_end = canonical.len() * 2 / 3;
    let first = canonical[..first_end].to_vec();
    let second = canonical[first_end..second_end].to_vec();
    let terminal = TerminalSuccess {
        final_bytes: canonical[second_end..].to_vec(),
    };
    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    let mut receiver = OrdinaryResultReceiver::new();
    assert!(first.len() <= MAX_FRAME_BYTES);
    assert!(second.len() <= MAX_FRAME_BYTES);

    assert_eq!(
        receiver
            .receive(LocalResponse::ResultChunk {
                chunk: ResultChunk { bytes: first },
            })
            .expect("accept first ordinary-result chunk"),
        ResultReceiveOutcome::Pending
    );
    assert_eq!(receiver.state(), ResultReceiveState::Receiving);
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        0
    );
    assert_eq!(
        receiver
            .receive(LocalResponse::ResultChunk {
                chunk: ResultChunk { bytes: second },
            })
            .expect("accept second ordinary-result chunk"),
        ResultReceiveOutcome::Pending
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        0
    );

    assert_eq!(
        receiver
            .receive(LocalResponse::TerminalSuccess {
                success: terminal.clone(),
            })
            .expect("publish only at terminal success"),
        ResultReceiveOutcome::Published(
            contextdb_engine::local_transport::AssembledOrdinaryResult {
                bytes: canonical.clone(),
                terminal,
            }
        )
    );
    assert_eq!(receiver.state(), ResultReceiveState::Published);
    assert_eq!(receiver.buffered_byte_count(), 0);
    assert!(
        OrdinaryResultReceiver::canonical_preflight_matches_for_test(
            &canonical,
            ReadLimits::SHIPPED_MEMORY,
            true,
        )
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        1
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_receive_events_for_test(),
        vec![
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_PASSED,
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_AUTHORIZED,
            OrdinaryResultReceiver::TEST_EVENT_PUBLISHED,
            OrdinaryResultReceiver::TEST_EVENT_ASSEMBLED_RESULT,
        ]
    );
}

#[test]
fn canonical_result_crosses_frames_and_publishes_only_the_exact_terminal_assembly() {
    let result = QueryResult {
        columns: vec!["payload".to_owned()],
        rows: vec![vec![Value::Text("x".repeat(MAX_FRAME_BYTES + 512))]],
        rows_affected: 0,
        trace: QueryTrace::scan(),
        cascade: None,
    };
    let canonical = encode_query_result(&result).expect("encode the route-neutral result");
    assert!(canonical.len() > MAX_FRAME_BYTES);
    let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits {
        result_rows: 2,
        result_bytes: canonical.len() as u64,
        work: 10_000,
        active_ms: 10_000,
        memory: (canonical.len() * 2) as u64,
        cursor_page_rows: 1,
        cursor_page_bytes: 1_024,
        cursor_idle_ms: 1_000,
        cursor_lifetime_ms: 2_000,
    });

    let responses = split_canonical_result(&canonical).expect("split the canonical result");
    assert!(
        responses.len() >= 2,
        "the result must cross at least two envelopes"
    );
    assert!(matches!(
        responses.last(),
        Some(LocalResponse::TerminalSuccess { .. })
    ));
    let frames = responses
        .iter()
        .map(|response| {
            let expectation = LocalResponseExpectation::OrdinaryResult;
            let frame = boundary
                .encode_frame(LocalOutboundMessage::Response {
                    response,
                    expectation: &expectation,
                })
                .expect("encode complete response through the bounded boundary");
            assert!(
                frame.len() - 4 <= MAX_FRAME_BYTES,
                "the complete response envelope must fit the payload ceiling"
            );
            frame
        })
        .collect::<Vec<_>>();

    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    let mut receiver = OrdinaryResultReceiver::new();
    for (index, frame) in frames.into_iter().enumerate() {
        let mut reader = SliceReader::new(frame);
        let mut allocator = AllocationLimit {
            maximum: MAX_FRAME_BYTES,
            calls: 0,
        };
        let outcome = boundary
            .receive_ordinary_result_frame(&mut receiver, &mut reader, &mut allocator)
            .expect("read and publish through the production receiver");
        assert_eq!(allocator.calls, 1);
        assert_eq!(reader.offset, reader.bytes.len());

        if index + 1 == responses.len() {
            let ResultReceiveOutcome::Published(assembled) = outcome else {
                panic!("terminal success must publish the assembled result");
            };
            assert_eq!(assembled.bytes, canonical);
            assert_eq!(receiver.state(), ResultReceiveState::Published);
            assert_eq!(receiver.buffered_byte_count(), 0);
            assert!(
                OrdinaryResultReceiver::canonical_preflight_matches_for_test(
                    &canonical,
                    (canonical.len() * 2) as u64,
                    true,
                )
            );
            assert_eq!(
                OrdinaryResultReceiver::canonical_decode_entries_for_test(),
                1
            );
            assert_eq!(
                OrdinaryResultReceiver::canonical_receive_events_for_test(),
                vec![
                    OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_PASSED,
                    OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_AUTHORIZED,
                    OrdinaryResultReceiver::TEST_EVENT_PUBLISHED,
                    OrdinaryResultReceiver::TEST_EVENT_ASSEMBLED_RESULT,
                ],
                "the real terminal receiver must complete allocation-free preflight before entering the canonical decoder and publication boundaries"
            );
        } else {
            assert_eq!(outcome, ResultReceiveOutcome::Pending);
            assert_eq!(receiver.state(), ResultReceiveState::Receiving);
            assert_eq!(
                OrdinaryResultReceiver::canonical_decode_entries_for_test(),
                0
            );
        }
    }
}

#[test]
fn splitter_accounts_for_the_response_envelope_at_both_chunk_boundaries() {
    let canonical = vec![0x5a; MAX_FRAME_BYTES + 257];
    let responses = split_canonical_result(&canonical).expect("split synthetic canonical bytes");
    let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits {
        result_rows: 1,
        result_bytes: canonical.len() as u64,
        work: 1,
        active_ms: 1,
        memory: canonical.len() as u64,
        cursor_page_rows: 1,
        cursor_page_bytes: 1,
        cursor_idle_ms: 1,
        cursor_lifetime_ms: 1,
    });
    assert!(responses.len() >= 2);
    let mut assembled = Vec::new();
    for response in &responses {
        let expectation = LocalResponseExpectation::OrdinaryResult;
        let encoded = boundary
            .encode_frame(LocalOutboundMessage::Response {
                response,
                expectation: &expectation,
            })
            .expect("encode response through the bounded boundary");
        assert!(encoded.len() - 4 <= MAX_FRAME_BYTES);
        match response {
            LocalResponse::ResultChunk { chunk } => assembled.extend_from_slice(&chunk.bytes),
            LocalResponse::TerminalSuccess { success } => {
                assembled.extend_from_slice(&success.final_bytes)
            }
            other => panic!("splitter emitted a non-result response: {other:?}"),
        }
    }
    assert!(matches!(
        responses.last(),
        Some(LocalResponse::TerminalSuccess { .. })
    ));
    assert_eq!(assembled, canonical);
}

#[test]
fn failed_or_disconnected_ordinary_results_discard_every_received_chunk() {
    let failure = ReadFailure::new(ReadFailureKind::OwnerAtCapacity, ReadFailureDetail::None)
        .expect("owner capacity refusal accepts an empty detail");
    let mut failed_receiver = OrdinaryResultReceiver::new();
    failed_receiver
        .receive(LocalResponse::ResultChunk {
            chunk: ResultChunk { bytes: vec![0x51] },
        })
        .expect("accept chunk before a failure");
    assert_eq!(failed_receiver.buffered_byte_count(), 1);
    assert_eq!(
        failed_receiver
            .receive(LocalResponse::Failure {
                failure: failure.clone(),
            })
            .expect("discard failed ordinary result"),
        ResultReceiveOutcome::Failed(failure)
    );
    assert_eq!(failed_receiver.state(), ResultReceiveState::Discarded);
    assert_eq!(failed_receiver.buffered_byte_count(), 0);

    let mut disconnected_receiver = OrdinaryResultReceiver::new();
    disconnected_receiver
        .receive(LocalResponse::ResultChunk {
            chunk: ResultChunk { bytes: vec![0x52] },
        })
        .expect("accept chunk before disconnect");
    assert_eq!(disconnected_receiver.buffered_byte_count(), 1);
    assert_eq!(
        disconnected_receiver
            .disconnect()
            .expect("discard disconnected result"),
        ResultReceiveOutcome::Disconnected
    );
    assert_eq!(disconnected_receiver.state(), ResultReceiveState::Discarded);
    assert_eq!(disconnected_receiver.buffered_byte_count(), 0);
}

#[test]
fn frame_prefix_boundary_is_stable_for_exact_and_oversized_lengths() {
    assert_eq!(
        frame_length_prefix(MAX_FRAME_BYTES + 1),
        Err(LocalTransportError::Frame(
            FrameViolation::LengthExceedsMaximum
        ))
    );
    assert_eq!(
        frame_length_prefix(MAX_FRAME_BYTES).expect("exact frame prefix"),
        (MAX_FRAME_BYTES as u32).to_be_bytes()
    );
}

const COMPACT_NESTED_ITEMS: usize = 4_096;
const EFFECTIVE_MEMORY_CEILING: u64 = 32 * 1024;

#[derive(Clone, Copy)]
enum CompactCanonicalContainer {
    ColumnsAndValues,
    Rows,
    TracePredicates,
    TraceCandidates,
    CascadeIndexes,
}

impl CompactCanonicalContainer {
    const fn label(self) -> &'static str {
        match self {
            Self::ColumnsAndValues => "columns and row values",
            Self::Rows => "rows",
            Self::TracePredicates => "trace predicates",
            Self::TraceCandidates => "trace index candidates",
            Self::CascadeIndexes => "cascade indexes",
        }
    }

    fn minimum_native_allocation(self, decoded: &CanonicalQueryResult) -> usize {
        match self {
            Self::ColumnsAndValues => {
                decoded.columns.len() * std::mem::size_of::<String>()
                    + decoded.rows[0].len() * std::mem::size_of::<Value>()
            }
            Self::Rows => decoded.rows.len() * std::mem::size_of::<Vec<Value>>(),
            Self::TracePredicates => {
                decoded.trace.predicates_pushed.len() * std::mem::size_of::<String>()
            }
            Self::TraceCandidates => {
                decoded.trace.indexes_considered.len()
                    * std::mem::size_of::<CanonicalIndexCandidate>()
            }
            Self::CascadeIndexes => {
                decoded
                    .cascade
                    .as_ref()
                    .expect("cascade fixture survives canonical conversion")
                    .dropped_indexes
                    .len()
                    * std::mem::size_of::<String>()
            }
        }
    }
}

fn compact_canonical_result(
    container: CompactCanonicalContainer,
) -> (Vec<u8>, CanonicalQueryResult) {
    let mut result = QueryResult {
        columns: vec!["value".to_owned()],
        rows: vec![vec![Value::Null]],
        rows_affected: 0,
        trace: QueryTrace::scan(),
        cascade: None,
    };
    match container {
        CompactCanonicalContainer::ColumnsAndValues => {
            result.columns = vec![String::new(); COMPACT_NESTED_ITEMS];
            result.rows = vec![vec![Value::Null; COMPACT_NESTED_ITEMS]];
        }
        CompactCanonicalContainer::Rows => {
            result.columns = vec![String::new()];
            result.rows = vec![vec![Value::Null]; COMPACT_NESTED_ITEMS];
        }
        CompactCanonicalContainer::TracePredicates => {
            let mut predicates = smallvec::SmallVec::new();
            for _ in 0..COMPACT_NESTED_ITEMS {
                predicates.push(std::borrow::Cow::Borrowed(""));
            }
            result.trace.predicates_pushed = predicates;
        }
        CompactCanonicalContainer::TraceCandidates => {
            let mut candidates = smallvec::SmallVec::new();
            for _ in 0..COMPACT_NESTED_ITEMS {
                candidates.push(IndexCandidate {
                    name: String::new(),
                    rejected_reason: std::borrow::Cow::Borrowed(""),
                });
            }
            result.trace.indexes_considered = candidates;
        }
        CompactCanonicalContainer::CascadeIndexes => {
            result.cascade = Some(CascadeReport {
                dropped_indexes: vec![String::new(); COMPACT_NESTED_ITEMS],
            });
        }
    }
    let canonical = encode_query_result(&result).expect("encode bounded canonical fixture");
    let decoded = decode_query_result(&canonical).expect("fixture is a valid canonical result");
    match container {
        CompactCanonicalContainer::ColumnsAndValues => {
            assert_eq!(decoded.columns.len(), COMPACT_NESTED_ITEMS);
            assert_eq!(decoded.rows.len(), 1);
            assert_eq!(decoded.rows[0].len(), COMPACT_NESTED_ITEMS);
        }
        CompactCanonicalContainer::Rows => {
            assert_eq!(decoded.columns.len(), 1);
            assert_eq!(decoded.rows.len(), COMPACT_NESTED_ITEMS);
            assert!(decoded.rows.iter().all(|row| row.len() == 1));
        }
        CompactCanonicalContainer::TracePredicates => {
            assert_eq!(decoded.trace.predicates_pushed.len(), COMPACT_NESTED_ITEMS);
        }
        CompactCanonicalContainer::TraceCandidates => {
            assert_eq!(decoded.trace.indexes_considered.len(), COMPACT_NESTED_ITEMS);
        }
        CompactCanonicalContainer::CascadeIndexes => {
            assert_eq!(
                decoded
                    .cascade
                    .as_ref()
                    .expect("cascade fixture survives canonical conversion")
                    .dropped_indexes
                    .len(),
                COMPACT_NESTED_ITEMS
            );
        }
    }
    (canonical, decoded)
}

fn frame_for_ordinary_response(response: &LocalResponse) -> Vec<u8> {
    let payload = encode_message(response).expect("encode ordinary response envelope");
    encode_payload_frame(&payload).expect("frame ordinary response envelope")
}

#[test]
fn canonical_preflight_capability_is_exact_one_shot_and_consumed_at_actual_decode_entry() {
    let canonical = encode_query_result(&QueryResult {
        columns: vec!["value".to_owned()],
        rows: vec![vec![Value::Null]],
        rows_affected: 0,
        trace: QueryTrace::scan(),
        cascade: None,
    })
    .expect("encode valid canonical causal fixture");
    let different_canonical = encode_query_result(&QueryResult {
        columns: vec!["value".to_owned()],
        rows: vec![vec![Value::Null]],
        rows_affected: 1,
        trace: QueryTrace::scan(),
        cascade: None,
    })
    .expect("encode distinct valid canonical causal fixture");
    let same_bytes_different_storage = canonical.clone();
    assert_ne!(canonical.as_ptr(), same_bytes_different_storage.as_ptr());

    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    decode_query_result(&canonical).expect("direct canonical decode remains valid");
    assert_eq!(
        OrdinaryResultReceiver::canonical_receive_events_for_test(),
        vec![OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_UNAUTHORIZED]
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        1
    );

    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    decode_query_result(&canonical).expect("first direct decode remains valid");
    OrdinaryResultReceiver::preflight_canonical_result_for_test(
        &canonical,
        EFFECTIVE_MEMORY_CEILING,
    )
    .expect("real allocation-free preflight admits the valid canonical bytes");
    OrdinaryResultReceiver::decode_preflighted_canonical_result_for_test(
        &canonical,
        EFFECTIVE_MEMORY_CEILING,
    )
    .expect("the one authorized decode remains valid");
    assert_eq!(
        OrdinaryResultReceiver::canonical_receive_events_for_test(),
        vec![
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_UNAUTHORIZED,
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_PASSED,
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_AUTHORIZED,
        ],
        "a decode before preflight remains visible even if a valid causal sequence follows"
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        2
    );

    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    OrdinaryResultReceiver::preflight_canonical_result_for_test(
        &canonical,
        EFFECTIVE_MEMORY_CEILING,
    )
    .expect("preflight exact bytes before the one-shot decode");
    OrdinaryResultReceiver::decode_preflighted_canonical_result_for_test(
        &canonical,
        EFFECTIVE_MEMORY_CEILING,
    )
    .expect("authorized canonical decode");
    decode_query_result(&canonical).expect("second direct decode remains structurally valid");
    assert_eq!(
        OrdinaryResultReceiver::canonical_receive_events_for_test(),
        vec![
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_PASSED,
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_AUTHORIZED,
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_UNAUTHORIZED,
        ],
        "the preflight capability authorizes exactly one actual decoder entry"
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        2
    );

    for wrong_bytes in [&same_bytes_different_storage, &different_canonical] {
        OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
        OrdinaryResultReceiver::preflight_canonical_result_for_test(
            &canonical,
            EFFECTIVE_MEMORY_CEILING,
        )
        .expect("preflight the original canonical storage");
        OrdinaryResultReceiver::decode_preflighted_canonical_result_for_test(
            wrong_bytes,
            EFFECTIVE_MEMORY_CEILING,
        )
        .expect("the distinct fixture is independently valid canonical data");
        assert_eq!(
            OrdinaryResultReceiver::canonical_receive_events_for_test(),
            vec![
                OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_PASSED,
                OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_UNAUTHORIZED,
            ],
            "a capability minted for different assembled bytes cannot authorize decode"
        );
        assert_eq!(
            OrdinaryResultReceiver::canonical_decode_entries_for_test(),
            1
        );
    }

    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    OrdinaryResultReceiver::preflight_canonical_result_for_test(
        &canonical,
        EFFECTIVE_MEMORY_CEILING,
    )
    .expect("preflight under the intended effective memory ceiling");
    OrdinaryResultReceiver::decode_preflighted_canonical_result_for_test(
        &canonical,
        EFFECTIVE_MEMORY_CEILING + 1,
    )
    .expect("the canonical bytes remain structurally valid");
    assert_eq!(
        OrdinaryResultReceiver::canonical_receive_events_for_test(),
        vec![
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_PASSED,
            OrdinaryResultReceiver::TEST_EVENT_CANONICAL_DECODE_UNAUTHORIZED,
        ],
        "a capability minted for a different memory ceiling cannot authorize decode"
    );

    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    OrdinaryResultReceiver::preflight_canonical_result_for_test(
        &canonical,
        EFFECTIVE_MEMORY_CEILING,
    )
    .expect("preflight-only causal fixture");
    assert_eq!(
        OrdinaryResultReceiver::canonical_receive_events_for_test(),
        vec![OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_PASSED],
        "preflight completion alone cannot manufacture an actual decoder entry"
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        0
    );
}

#[test]
fn malformed_assembled_canonical_result_is_discarded_before_actual_decode() {
    let mut malformed = encode_query_result(&QueryResult {
        columns: vec!["value".to_owned()],
        rows: vec![vec![Value::Null]],
        rows_affected: 0,
        trace: QueryTrace::scan(),
        cascade: None,
    })
    .expect("encode canonical fixture before truncating it");
    malformed
        .pop()
        .expect("canonical fixture has a final field");
    assert!(
        decode_query_result(&malformed).is_err(),
        "the truncated fixture must not be a valid canonical result"
    );
    let split_at = malformed.len() / 2;
    assert!(split_at > 0 && split_at < malformed.len());
    let responses = [
        LocalResponse::ResultChunk {
            chunk: ResultChunk {
                bytes: malformed[..split_at].to_vec(),
            },
        },
        LocalResponse::TerminalSuccess {
            success: TerminalSuccess {
                final_bytes: malformed[split_at..].to_vec(),
            },
        },
    ];
    let frames = responses
        .iter()
        .map(frame_for_ordinary_response)
        .collect::<Vec<_>>();
    let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits {
        result_rows: 1,
        result_bytes: malformed.len() as u64,
        work: 1,
        active_ms: 1,
        memory: ReadLimits::SHIPPED_MEMORY,
        cursor_page_rows: 1,
        cursor_page_bytes: 1,
        cursor_idle_ms: 1,
        cursor_lifetime_ms: 1,
    });
    let expected_error = LocalTransportError::Refusal(
        ReadFailure::new(ReadFailureKind::InvalidChannelData, ReadFailureDetail::None)
            .expect("invalid channel data accepts an empty detail"),
    );
    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    assert!(
        OrdinaryResultReceiver::preflight_canonical_result_for_test(
            &malformed,
            ReadLimits::SHIPPED_MEMORY,
        )
        .is_err(),
        "the allocation-free canonical grammar must reject the malformed assembled bytes"
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_receive_events_for_test(),
        vec![OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_FAILED]
    );
    assert!(
        OrdinaryResultReceiver::canonical_preflight_matches_for_test(
            &malformed,
            ReadLimits::SHIPPED_MEMORY,
            false,
        )
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        0
    );

    let mut receiver = OrdinaryResultReceiver::new();
    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();

    for (index, frame) in frames.into_iter().enumerate() {
        let mut reader = SliceReader::new(frame);
        let mut allocator = AllocationLimit {
            maximum: MAX_FRAME_BYTES,
            calls: 0,
        };
        let received =
            boundary.receive_ordinary_result_frame(&mut receiver, &mut reader, &mut allocator);
        assert_eq!(reader.offset, reader.bytes.len());

        if index == 0 {
            assert_eq!(received, Ok(ResultReceiveOutcome::Pending));
            assert_eq!(receiver.state(), ResultReceiveState::Receiving);
            assert!(OrdinaryResultReceiver::canonical_receive_events_for_test().is_empty());
            assert_eq!(
                OrdinaryResultReceiver::canonical_decode_entries_for_test(),
                0
            );
        } else {
            assert_eq!(received, Err(expected_error.clone()));
            assert_eq!(receiver.state(), ResultReceiveState::Discarded);
            assert_eq!(receiver.buffered_byte_count(), 0);
            assert!(
                OrdinaryResultReceiver::canonical_preflight_matches_for_test(
                    &malformed,
                    ReadLimits::SHIPPED_MEMORY,
                    false,
                )
            );
            assert_eq!(
                OrdinaryResultReceiver::canonical_receive_events_for_test(),
                vec![OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_FAILED],
                "malformed canonical structure must be refused without decode, publication, or assembled-result construction"
            );
            assert_eq!(
                OrdinaryResultReceiver::canonical_decode_entries_for_test(),
                0
            );
        }
    }
}

fn assert_compact_canonical_container_is_preflighted(container: CompactCanonicalContainer) {
    let (canonical, decoded) = compact_canonical_result(container);
    assert!(
        canonical.len() as u64 <= EFFECTIVE_MEMORY_CEILING,
        "the {label} fixture fits the effective memory ceiling as wire bytes",
        label = container.label()
    );
    assert!(
        container.minimum_native_allocation(&decoded) > EFFECTIVE_MEMORY_CEILING as usize,
        "the {label} fixture's nested native container allocation exceeds the effective memory ceiling",
        label = container.label()
    );

    let split_at = canonical.len() / 2;
    let responses = [
        LocalResponse::ResultChunk {
            chunk: ResultChunk {
                bytes: canonical[..split_at].to_vec(),
            },
        },
        LocalResponse::TerminalSuccess {
            success: TerminalSuccess {
                final_bytes: canonical[split_at..].to_vec(),
            },
        },
    ];
    let frames = responses
        .iter()
        .map(|response| {
            let frame = frame_for_ordinary_response(response);
            assert!(
                frame.len() - 4 <= MAX_FRAME_BYTES,
                "the {label} envelope must fit the outer frame ceiling",
                label = container.label()
            );
            frame
        })
        .collect::<Vec<_>>();
    let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits {
        result_rows: u64::MAX,
        result_bytes: MAX_FRAME_BYTES as u64,
        work: 1,
        active_ms: 1,
        memory: EFFECTIVE_MEMORY_CEILING,
        cursor_page_rows: 1,
        cursor_page_bytes: 1,
        cursor_idle_ms: 1,
        cursor_lifetime_ms: 1,
    });
    let mut receiver = OrdinaryResultReceiver::new();
    let expected_error = LocalTransportError::Refusal(
        ReadFailure::new(ReadFailureKind::InvalidChannelData, ReadFailureDetail::None)
            .expect("invalid channel data accepts an empty detail"),
    );
    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();
    assert!(
        OrdinaryResultReceiver::preflight_canonical_result_for_test(
            &canonical,
            EFFECTIVE_MEMORY_CEILING,
        )
        .is_err(),
        "the real allocation-free canonical preflight must reject the over-memory {label} fixture",
        label = container.label()
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_receive_events_for_test(),
        vec![OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_FAILED]
    );
    assert!(
        OrdinaryResultReceiver::canonical_preflight_matches_for_test(
            &canonical,
            EFFECTIVE_MEMORY_CEILING,
            false,
        )
    );
    assert_eq!(
        OrdinaryResultReceiver::canonical_decode_entries_for_test(),
        0
    );
    OrdinaryResultReceiver::reset_canonical_receive_trace_for_test();

    for (index, frame) in frames.into_iter().enumerate() {
        let mut reader = SliceReader::new(frame);
        let mut allocator = AllocationLimit {
            maximum: MAX_FRAME_BYTES,
            calls: 0,
        };
        let received =
            boundary.receive_ordinary_result_frame(&mut receiver, &mut reader, &mut allocator);
        assert_eq!(reader.offset, reader.bytes.len());

        if index == 0 {
            assert_eq!(received, Ok(ResultReceiveOutcome::Pending));
            assert_eq!(receiver.state(), ResultReceiveState::Receiving);
            assert!(OrdinaryResultReceiver::canonical_receive_events_for_test().is_empty());
            assert_eq!(
                OrdinaryResultReceiver::canonical_decode_entries_for_test(),
                0
            );
        } else {
            assert_eq!(
                OrdinaryResultReceiver::canonical_receive_events_for_test(),
                vec![OrdinaryResultReceiver::TEST_EVENT_CANONICAL_PREFLIGHT_FAILED],
                "the over-ceiling {label} fixture must be refused after allocation-free preflight without entering canonical decode, publication, or assembled-result construction",
                label = container.label()
            );
            assert_eq!(
                received,
                Err(expected_error.clone()),
                "the valid assembled {label} result must be rejected before nested serde allocation",
                label = container.label()
            );
            assert_eq!(receiver.state(), ResultReceiveState::Discarded);
            assert_eq!(receiver.buffered_byte_count(), 0);
            assert!(
                OrdinaryResultReceiver::canonical_preflight_matches_for_test(
                    &canonical,
                    EFFECTIVE_MEMORY_CEILING,
                    false,
                )
            );
            assert_eq!(
                OrdinaryResultReceiver::canonical_decode_entries_for_test(),
                0,
                "the over-memory {label} fixture must be refused before the actual canonical decoder entrance",
                label = container.label()
            );
        }
    }
}

#[test]
fn assembled_canonical_results_preflight_compact_nested_containers_before_deserialization() {
    for container in [
        CompactCanonicalContainer::ColumnsAndValues,
        CompactCanonicalContainer::Rows,
        CompactCanonicalContainer::TracePredicates,
        CompactCanonicalContainer::TraceCandidates,
        CompactCanonicalContainer::CascadeIndexes,
    ] {
        assert_compact_canonical_container_is_preflighted(container);
    }
}
