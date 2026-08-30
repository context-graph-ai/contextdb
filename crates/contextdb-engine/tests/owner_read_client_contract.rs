use contextdb_core::Value;
use contextdb_core::read_contract::{ReadFailure, ReadFailureDetail, ReadFailureKind, ReadLimits};
use contextdb_engine::local_transport::{
    AssembledOrdinaryResult, FrameBufferAllocator, FrameReader, LocalRequest, LocalRequestEnvelope,
    LocalResponse, LocalTransportError, MAX_FRAME_BYTES, OrdinaryResultReceiver, ResultChunk,
    ResultReceiveOutcome, ResultReceiveState, split_canonical_result,
};
use contextdb_engine::owner_read::{OwnerClient, OwnerReadScaffoldError};
use contextdb_engine::read_contract::encode_query_result;
use contextdb_engine::{QueryResult, QueryTrace};
use std::collections::BTreeMap;

fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        result_bytes: 4 * 1024 * 1024,
        work: 50_000,
        active_ms: 5_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 100,
        cursor_page_bytes: 1024 * 1024,
        cursor_idle_ms: 300_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

struct PrefixReader {
    prefix: [u8; 4],
    reads: usize,
}

impl FrameReader for PrefixReader {
    fn read_exact(&mut self, destination: &mut [u8]) -> Result<(), LocalTransportError> {
        self.reads += 1;
        if self.reads == 1 {
            assert_eq!(destination.len(), 4, "only the fixed prefix is read first");
            destination.copy_from_slice(&self.prefix);
        } else {
            destination.fill(0xa7);
        }
        Ok(())
    }
}

#[derive(Default)]
struct CountingAllocator {
    calls: usize,
}

impl FrameBufferAllocator for CountingAllocator {
    fn allocate(&mut self, length: usize) -> Result<Vec<u8>, LocalTransportError> {
        self.calls += 1;
        Ok(vec![0; length])
    }
}

#[test]
fn hostile_length_prefix_is_refused_before_payload_allocation() {
    let mut reader = PrefixReader {
        prefix: (MAX_FRAME_BYTES as u32 + 1).to_be_bytes(),
        reads: 0,
    };
    let mut allocator = CountingAllocator::default();

    let error = OwnerClient::read_envelope(&mut reader, &mut allocator)
        .expect_err("an oversized declared payload must be refused before decode");
    assert!(matches!(
        error,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::InvalidChannelData
    ));
    assert_eq!(reader.reads, 1);
    assert_eq!(allocator.calls, 0);
}

#[test]
fn oversized_custom_request_is_refused_by_production_framing_before_carrier_write() {
    let envelope = LocalRequestEnvelope {
        limits: limits(),
        request: LocalRequest::Custom {
            namespace: "proof.oversized-request".to_owned(),
            payload: vec![0x51; MAX_FRAME_BYTES + 1],
        },
    };
    let error = OwnerClient::encode_envelope(&envelope)
        .expect_err("the complete encoded request cannot cross the frame ceiling");
    assert!(matches!(
        error,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::InvalidChannelData
    ));
}

#[test]
fn multiple_chunks_remain_private_until_terminal_success() {
    // The bytes staged here are the ones a real owner sends: one route-neutral
    // canonical result, cut into envelopes by the production splitter. Bytes
    // that are not a canonical result are refused at terminal assembly by the
    // same framing contract this journey lives beside, so staging them would
    // make the journey prove nothing about privacy-until-terminal-success.
    let result = QueryResult {
        columns: vec!["payload".to_owned()],
        rows: vec![vec![Value::Text("x".repeat(MAX_FRAME_BYTES + 512))]],
        rows_affected: 0,
        trace: QueryTrace::scan(),
        cascade: None,
    };
    let canonical = encode_query_result(&result).expect("encode the route-neutral result");
    let responses =
        split_canonical_result(&canonical).expect("frame the canonical result as the sender does");
    let last = responses.len() - 1;
    assert!(
        last >= 1,
        "the fixture must stage more than one chunk ahead of terminal success"
    );

    let mut receiver = OrdinaryResultReceiver::with_byte_limit(canonical.len() as u64)
        .expect("a receiver bound to this result's declared byte ceiling");
    let mut staged = 0_usize;
    for (index, response) in responses.into_iter().enumerate() {
        if index < last {
            let LocalResponse::ResultChunk { chunk } = &response else {
                panic!("every envelope ahead of the last is a result chunk, got {response:?}");
            };
            staged += chunk.bytes.len();
            assert_eq!(
                OwnerClient::receive_ordinary(&mut receiver, response.clone())
                    .expect("the shared receiver accepts a complete result chunk"),
                ResultReceiveOutcome::Pending,
            );
            assert_eq!(receiver.state(), ResultReceiveState::Receiving);
            assert_eq!(receiver.buffered_byte_count(), staged);
            continue;
        }
        let LocalResponse::TerminalSuccess { success } = response.clone() else {
            panic!("the splitter's last envelope is terminal success, got {response:?}");
        };
        assert_eq!(
            OwnerClient::receive_ordinary(&mut receiver, response)
                .expect("terminal success publishes the complete canonical byte stream"),
            ResultReceiveOutcome::Published(AssembledOrdinaryResult {
                bytes: canonical.clone(),
                terminal: success,
            }),
        );
        assert_eq!(receiver.state(), ResultReceiveState::Published);
        assert_eq!(receiver.buffered_byte_count(), 0);
    }
}

#[test]
fn terminal_failure_and_disconnect_discard_every_staged_chunk() {
    let failure = ReadFailure::new(ReadFailureKind::OwnerTimeout, ReadFailureDetail::None)
        .expect("owner timeout uses canonical empty detail");
    let mut failed = OrdinaryResultReceiver::new();
    OwnerClient::receive_ordinary(
        &mut failed,
        LocalResponse::ResultChunk {
            chunk: ResultChunk {
                bytes: vec![0x51, 0x52],
            },
        },
    )
    .expect("stage a chunk before terminal failure");
    assert_eq!(
        OwnerClient::receive_ordinary(
            &mut failed,
            LocalResponse::Failure {
                failure: failure.clone(),
            },
        )
        .expect("terminal failure discards staged result bytes"),
        ResultReceiveOutcome::Failed(failure),
    );
    assert_eq!(failed.state(), ResultReceiveState::Discarded);
    assert_eq!(failed.buffered_byte_count(), 0);

    let mut disconnected = OrdinaryResultReceiver::new();
    OwnerClient::receive_ordinary(
        &mut disconnected,
        LocalResponse::ResultChunk {
            chunk: ResultChunk {
                bytes: vec![0x61, 0x62],
            },
        },
    )
    .expect("stage a chunk before EOF/HUP");
    assert_eq!(
        disconnected
            .disconnect()
            .expect("the shared receiver discards on EOF/HUP"),
        ResultReceiveOutcome::Disconnected,
    );
    assert_eq!(disconnected.state(), ResultReceiveState::Discarded);
    assert_eq!(disconnected.buffered_byte_count(), 0);
}

#[test]
fn client_source_has_only_real_carrier_framing_and_named_deadline_paths() {
    // The typed-boundary containment audit
    // (`owner_channel_routes_cannot_bypass_the_typed_protocol_boundary`,
    // local_transport_containment.rs) forbids owner_read from calling the raw
    // codec/admission primitives (`encode_message`, `encode_payload_frame`,
    // `read_payload_with_admission`, `receive_framed_ordinary_result`)
    // directly, and requires the carrier's boundary-routed methods to be
    // observable as qualified paths rather than method calls. The client was
    // moved onto `LocalProtocolBoundary::receive_frame_preserving_refusal` /
    // `encode_frame` for its own envelope helpers, its now-unused
    // `receive_framed_ordinary` wrapper (dead code, no callers) was deleted,
    // and its carrier calls now use `UnixLocalCarrier::method(&carrier, ...)`
    // instead of `carrier.method(...)`; this list is updated to match.
    let source = include_str!("../src/owner_read/client.rs");
    for required in [
        "UnixLocalCarrier",
        ".connect(",
        "handshake_with_deadline",
        "LocalOutboundMessage::Handshake",
        "LocalProtocolBoundary",
        "UnixLocalCarrier::write_request(",
        "UnixLocalCarrier::receive_response(",
        "OrdinaryResultReceiver::with_effective_ceilings",
        ".receive(response.clone())",
        ".disconnect()",
        "receive_frame_preserving_refusal",
        "encode_frame",
    ] {
        assert!(
            source.contains(required),
            "the thin client must retain production journey seam {required}",
        );
    }
    for forbidden in [
        "OwnerReadService",
        "service.handle",
        "peer_user:",
        "Database::",
        "ReadSession",
        "direct_file_reader",
        "retry_route_with_deadline",
        "persistence::",
        "redb::",
    ] {
        assert!(
            !source.contains(forbidden),
            "the owner client must not bypass or reroute around its carrier: {forbidden}",
        );
    }

    let dynamic = [
        LocalRequest::Query {
            statement: "SELECT $value".to_owned(),
            params: BTreeMap::new(),
        },
        LocalRequest::OwnerStatus,
        LocalRequest::Custom {
            namespace: "proof.dynamic".to_owned(),
            payload: vec![0x71],
        },
    ];
    for request in dynamic {
        OwnerClient::encode_envelope(&LocalRequestEnvelope {
            limits: limits(),
            request,
        })
        .expect("every dynamic request uses the canonical production codec");
    }
}
