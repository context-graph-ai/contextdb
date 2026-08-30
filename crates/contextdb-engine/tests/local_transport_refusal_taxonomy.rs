#![cfg(feature = "test-seams")]
//! What the channel says went wrong is what the caller acts on.
//!
//! The refusal kinds on this channel are not decoration: a caller branches on
//! them. A peer that stops mid-reply is a disconnect, and the disconnect kind
//! is what governs whether a caller may fall back on the same connection.
//! Corrupt channel content is a different thing and means a different response.
//! A refusal decided about the peer before any waiting begins is a refusal
//! about the peer, not a missed deadline. A payload whose decoded form crosses
//! a declared ceiling is a ceiling crossing, not malformed content. And the
//! set of local filesystem kinds a runtime root may sit on is the same set
//! however wide the word the operating system reports it in.

use contextdb_core::read_contract::{
    DeadlineClock, ReadFailure, ReadFailureDetail, ReadFailureKind, ReadLimits,
};
use contextdb_engine::local_transport::{
    FrameBufferAllocator, FrameReader, FrameViolation, LocalInboundKind, LocalProtocolBoundary,
    LocalRequestEnvelope, LocalResponseExpectation, LocalTransportError, MAX_FRAME_BYTES,
    ManualDeadlineClock, OrdinaryResultReceiver, decode_message_exact, handshake_with_deadline,
    linux_filesystem_type_is_local, receive_framed_ordinary_result,
};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

// --- a peer that stops mid-reply -------------------------------------------

/// A stream that carries a complete length prefix and then ends, exactly as a
/// peer that goes away part-way through writing its reply leaves the channel.
struct TruncatedStream {
    bytes: Vec<u8>,
    offset: usize,
}

impl TruncatedStream {
    /// A frame that promises `declared` payload bytes and delivers `delivered`.
    fn new(declared: usize, delivered: usize) -> Self {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(
            &u32::try_from(declared)
                .expect("a declared frame length fits the prefix")
                .to_be_bytes(),
        );
        bytes.resize(bytes.len() + delivered, 0);
        Self { bytes, offset: 0 }
    }
}

impl FrameReader for TruncatedStream {
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

struct PlainAllocator;

impl FrameBufferAllocator for PlainAllocator {
    fn allocate(&mut self, length: usize) -> Result<Vec<u8>, LocalTransportError> {
        Ok(vec![0; length])
    }
}

fn refusal_kind(error: &LocalTransportError) -> Option<ReadFailureKind> {
    match error {
        LocalTransportError::Refusal(refusal) => Some(refusal.kind()),
        _ => None,
    }
}

/// Assembling an ordinary result from a peer that went away mid-reply is the
/// documented production assembly entrance. It must name the disconnect.
#[test]
fn an_owner_that_stops_mid_reply_is_reported_as_a_disconnect_during_result_assembly() {
    let mut receiver = OrdinaryResultReceiver::new();
    let mut reader = TruncatedStream::new(4_096, 12);
    let mut allocator = PlainAllocator;

    let error = receive_framed_ordinary_result(&mut receiver, &mut reader, &mut allocator)
        .expect_err("a reply that ends mid-frame cannot be assembled");

    assert_eq!(
        refusal_kind(&error),
        Some(ReadFailureKind::OwnerDisconnected),
        "a peer that goes away part-way through its reply has disconnected; the assembly \
         entrance reported {error:?}, which does not carry the disconnect the fallback rule \
         is decided on"
    );
}

/// The same end-of-stream through the protocol boundary. A disconnect is not
/// the channel telling the caller its content was corrupt.
#[test]
fn an_owner_that_stops_mid_reply_is_not_reported_as_corrupt_channel_content() {
    let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits::default());
    let mut reader = TruncatedStream::new(4_096, 12);
    let mut allocator = PlainAllocator;

    let error = boundary
        .receive_frame(
            LocalInboundKind::Response(LocalResponseExpectation::OrdinaryResult),
            &mut reader,
            &mut allocator,
        )
        .expect_err("a frame that ends mid-payload cannot be received");

    assert_ne!(
        refusal_kind(&error),
        Some(ReadFailureKind::InvalidChannelData),
        "the peer went away; nothing it sent was corrupt. Reporting corrupt channel content \
         for an end-of-stream tells the caller to distrust bytes it never received, and \
         withholds the disconnect the fallback rule is decided on"
    );
    assert_eq!(
        refusal_kind(&error),
        Some(ReadFailureKind::OwnerDisconnected),
        "an end-of-stream mid-frame is a disconnect, observed {error:?}"
    );
}

// --- a refusal decided before any waiting ----------------------------------

/// A handshake refused on the peer's identity is decided before the channel
/// waits for anything. An elapsed deadline must not overwrite that answer with
/// a timing story about a peer that was never going to be served.
#[test]
fn a_peer_refused_on_identity_keeps_that_refusal_when_the_deadline_has_elapsed() {
    let clock = ManualDeadlineClock::at(5_000);
    let identity_refusal = LocalTransportError::Refusal(
        ReadFailure::new(ReadFailureKind::OwnerUserMismatch, ReadFailureDetail::None)
            .expect("a fixed refusal carries no specialized detail"),
    );
    let operation: contextdb_engine::local_transport::LocalDeadlineOperation<'_, ()> =
        Box::pin(std::future::ready(Err(identity_refusal)));
    let mut wait = handshake_with_deadline(&clock, 1_000, operation);
    let mut context = Context::from_waker(Waker::noop());

    let Poll::Ready(Err(error)) = Pin::new(&mut wait).poll(&mut context) else {
        panic!("a handshake whose refusal is already decided answers on its first drive");
    };
    assert_eq!(
        refusal_kind(&error),
        Some(ReadFailureKind::OwnerUserMismatch),
        "the peer was refused on its identity before any waiting began; reporting {error:?} \
         instead hands the caller a timing story for a peer the channel had already refused, \
         and hides an identity refusal behind a deadline"
    );
    assert!(
        clock.now_ms() >= 1_000,
        "the deadline has genuinely elapsed"
    );
}

// --- an operation already delivered its outcome ----------------------------

/// A deadline operation that has already produced its outcome keeps no work
/// to drive. Nothing about the peer changed between the poll that delivered
/// the outcome and a later poll on the same, already-consumed wait -- so a
/// later poll must not tell the caller the connection to the owner ended.
/// That is a disconnect story about a peer that never disconnected; the
/// truthful answer is that this operation already answered.
#[test]
fn a_deadline_operation_polled_again_after_delivering_its_outcome_is_reported_as_already_completed_not_a_disconnect()
 {
    let clock = ManualDeadlineClock::at(5_000);
    let operation: contextdb_engine::local_transport::LocalDeadlineOperation<'_, ()> =
        Box::pin(std::future::ready(Ok(())));
    let mut wait = handshake_with_deadline(&clock, 10_000, operation);
    let mut context = Context::from_waker(Waker::noop());

    let Poll::Ready(Ok(())) = Pin::new(&mut wait).poll(&mut context) else {
        panic!("an operation that is already ready answers on its first drive");
    };

    let Poll::Ready(Err(error)) = Pin::new(&mut wait).poll(&mut context) else {
        panic!(
            "polling a deadline operation again after it already delivered its outcome must \
             still answer, not hang"
        );
    };
    assert_eq!(
        refusal_kind(&error),
        Some(ReadFailureKind::OperationAlreadyCompleted),
        "the operation already answered and its outcome was delivered on the previous poll; \
         reporting {error:?} instead of the already-completed kind hands the caller a \
         disconnect story about a peer that never disconnected"
    );
    assert_ne!(
        refusal_kind(&error),
        Some(ReadFailureKind::OwnerDisconnected),
        "nothing disconnected -- this wait had already delivered its outcome before this poll, \
         so reporting the disconnect kind here is a lie"
    );
}

// --- a decoded footprint that crosses the ceiling ---------------------------

/// Room for the fixed request preamble ahead of the parameter text.
const REQUEST_PREAMBLE_RESERVE: usize = 512;

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

/// A payload that is exactly what it claims to be, and simply too large to
/// decode inside the declared ceiling, is a ceiling crossing. Reporting it the
/// same way as content that is not a payload at all tells an operator their
/// peer is broken when their ceiling is the thing that was reached.
#[test]
fn an_oversized_payload_is_refused_differently_from_content_that_is_not_a_payload() {
    let elements = (MAX_FRAME_BYTES - REQUEST_PREAMBLE_RESERVE) / 2;
    let oversized = json_parameter_request_bytes(&json_zero_array_text(elements));
    assert!(
        oversized.len() <= MAX_FRAME_BYTES,
        "the oversized payload must sit inside the wire ceiling to be admitted at all"
    );

    let oversized_error = decode_message_exact::<LocalRequestEnvelope>(&oversized)
        .expect_err("a payload whose decoded form crosses the memory ceiling is refused");
    let malformed_error = decode_message_exact::<LocalRequestEnvelope>(&[0xFF_u8; 24])
        .expect_err("bytes that are not a payload are refused");

    assert_ne!(
        format!("{oversized_error:?}"),
        format!("{malformed_error:?}"),
        "a well-formed payload refused for crossing the declared memory ceiling and bytes \
         that are not a payload at all are two different answers; both are reported as \
         {oversized_error:?}, so an operator cannot tell a ceiling they set from a peer \
         that is sending garbage"
    );
}

// --- the local filesystem kinds a runtime root may sit on -------------------

/// The magic numbers the operating system reports for a filesystem kind are
/// fixed values. Reading one through a narrower signed word than the constant
/// it is compared against turns a local filesystem into a foreign one, and a
/// usable runtime root into a refused one.
#[test]
#[cfg(target_os = "linux")]
fn a_local_filesystem_kind_is_recognized_however_wide_the_reported_word() {
    const RAMFS: u32 = 0x8584_58f6;
    assert!(
        linux_filesystem_type_is_local(i64::from(RAMFS)),
        "the widened form of the filesystem kind is the one the check is written against"
    );

    let reported_through_a_narrow_word = i64::from(RAMFS as i32);
    assert!(
        linux_filesystem_type_is_local(reported_through_a_narrow_word),
        "the same filesystem kind reported through a narrower signed word arrives as \
         {reported_through_a_narrow_word} and is rejected, so a runtime root on that \
         filesystem is refused as though it were remote"
    );
}
