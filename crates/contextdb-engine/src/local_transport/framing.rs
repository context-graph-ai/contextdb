use super::engine_answer::{BodyField, ReadChannelError, body_grammar};
use super::{
    LOCAL_PROTOCOL_MARKER, LOCAL_PROTOCOL_VERSION, LocalTransportError, MAX_FRAME_BYTES,
    frame_read_refusal, invalid_channel_data,
};
use crate::read_contract::{decode_cursor_page, decode_metadata_page};
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use contextdb_core::read_contract::{
    DatabaseIdentity, LocalUserIdentity, MetadataPageVocabulary, OwnerReadLimits, OwnerReadStatus,
    OwnerServiceTimeouts, ReadFailure, ReadLimits, WriterRunNumber,
};
use contextdb_core::{ContextId, Principal, ScopeLabel, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameViolation {
    LengthExceedsMaximum,
    TruncatedLength,
    TruncatedPayload,
    TrailingBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadViolation {
    Malformed,
    /// The payload is exactly what it claims to be and the memory its decoded
    /// form would occupy crosses the ceiling in force for this exchange. An
    /// operator reading this reached a ceiling they set; they did not meet a
    /// peer sending content that is not a payload -- so the ceiling itself
    /// travels with the violation, and the refusal a caller finally reads can
    /// name the number they would have to raise.
    MemoryCeilingExceeded {
        ceiling: u64,
    },
    TrailingBytes,
}

/// What a reader declares, once, about the visibility its whole session reads
/// under: which contexts it is inside, which scope labels it reads, and who it
/// reads as.
///
/// It travels in the handshake because it belongs to the SESSION rather than
/// to any one statement -- a reader cannot widen partway through by asking a
/// different question -- and a reader can never see past what the writer
/// serving it may itself see. Contexts and scope labels are sets, so the
/// owner intersects those with its own access. Identities are not sets and
/// have no intersection: a writer that named no principal reads as the
/// declared one, a writer that named the same one serves it unchanged, and a
/// writer that named a DIFFERENT one refuses the session at admission rather
/// than serve its own wider view. Every field `None` is a reader that
/// declared nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalReadDeclaration {
    pub contexts: Option<BTreeSet<ContextId>>,
    pub scope_labels: Option<BTreeSet<ScopeLabel>>,
    pub principal: Option<Principal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalHandshake {
    pub marker: [u8; LOCAL_PROTOCOL_MARKER.len()],
    pub version: u16,
    pub database_identity: DatabaseIdentity,
    pub writer_run: WriterRunNumber,
    pub owner_user: LocalUserIdentity,
    /// The visibility this reader declared for its session, when it declared
    /// one. It is the reader speaking about itself, never an owner fact, so it
    /// takes no part in deciding whether this peer is who it claims to be.
    pub declared: Option<LocalReadDeclaration>,
}

#[cfg(feature = "test-seams")]
std::thread_local! {
    static HANDSHAKE_DECODE_ENTRIES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl LocalHandshake {
    pub const fn current(
        database_identity: DatabaseIdentity,
        writer_run: WriterRunNumber,
        owner_user: LocalUserIdentity,
    ) -> Self {
        Self {
            marker: LOCAL_PROTOCOL_MARKER,
            version: LOCAL_PROTOCOL_VERSION,
            database_identity,
            writer_run,
            owner_user,
            declared: None,
        }
    }

    /// The same handshake, carrying the visibility this reader's session
    /// declared. `None` leaves it the undeclared handshake `current` builds.
    #[must_use]
    pub fn declaring(mut self, declared: Option<LocalReadDeclaration>) -> Self {
        self.declared = declared;
        self
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn reset_decode_entries_for_test() {
        HANDSHAKE_DECODE_ENTRIES.with(|entries| entries.set(0));
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn decode_entries_for_test() -> usize {
        HANDSHAKE_DECODE_ENTRIES.with(std::cell::Cell::get)
    }
}

/// Validate the protocol marker, version, database identity, writer run,
/// and authenticated user before a request enters service admission.
pub fn validate_handshake(
    expected: &LocalHandshake,
    presented: &LocalHandshake,
    peer_user: LocalUserIdentity,
) -> Result<(), LocalTransportError> {
    if expected.marker != LOCAL_PROTOCOL_MARKER
        || expected.version != LOCAL_PROTOCOL_VERSION
        || presented.marker != LOCAL_PROTOCOL_MARKER
        || presented.version != LOCAL_PROTOCOL_VERSION
    {
        return Err(super::refusal(
            contextdb_core::read_contract::ReadFailureKind::LocalProtocolMismatch,
        ));
    }
    if presented.database_identity != expected.database_identity
        || presented.writer_run != expected.writer_run
    {
        return Err(super::refusal(
            contextdb_core::read_contract::ReadFailureKind::OwnerMismatch,
        ));
    }
    if peer_user != expected.owner_user || presented.owner_user != peer_user {
        return Err(super::refusal(
            contextdb_core::read_contract::ReadFailureKind::OwnerUserMismatch,
        ));
    }
    // `declared` is deliberately not compared. It is what the reader says
    // about its OWN visibility, not a recorded owner fact it must match, and
    // the owner decides what to do with it after admission: contexts and
    // scope labels are intersected with the owner's own access, which can
    // only take rows away, and a declared identity the owner will not read as
    // ends that session with a refusal of its own.
    Ok(())
}

/// Decode the fixed handshake shape and map malformed channel data to the
/// stable local-channel refusal before admission.
pub fn decode_handshake_exact(bytes: &[u8]) -> Result<LocalHandshake, LocalTransportError> {
    #[cfg(feature = "test-seams")]
    HANDSHAKE_DECODE_ENTRIES.with(|entries| entries.set(entries.get().saturating_add(1)));
    preflight_handshake(bytes, ReadLimits::SHIPPED_MEMORY)?;
    decode_message_exact(bytes)
}

/// Caller-controlled read ceilings accompany every request, including control
/// and metadata operations, so the owner can compute effective limits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalRequestEnvelope {
    pub limits: ReadLimits,
    pub request: LocalRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalMetadataRequest {
    Tables { continuation: Option<String> },
    Schema { table: String },
    EventsStatus { continuation: Option<String> },
    MaintenanceStatus,
}

/// Everything a reader can say. All of it is read-only, and all of it is the
/// reader speaking first: the owner answers and never initiates. The last
/// variant is the one exception to one-request-one-answer — it names the
/// request already in flight rather than asking for anything of its own, and
/// is answered by that request's own terminal reply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LocalRequest {
    Query {
        statement: String,
        params: BTreeMap<String, Value>,
    },
    CursorOpen {
        statement: String,
        params: BTreeMap<String, Value>,
    },
    CursorFetch {
        cursor_id: [u8; 16],
        rows: Option<NonZeroU64>,
    },
    CursorClose {
        cursor_id: [u8; 16],
    },
    Metadata {
        request: LocalMetadataRequest,
    },
    Explain {
        statement: String,
        params: BTreeMap<String, Value>,
    },
    OwnerStatus,
    Custom {
        namespace: String,
        payload: Vec<u8>,
    },
    /// Stop the request already in flight on this same connection. The
    /// reading thread is blocked awaiting its reply, so the cancel arrives
    /// on a second handle to the same carrier; naming the request by the
    /// ordinal both sides count keeps a late cancel from reaching the next
    /// request instead of the one the human interrupted.
    CancelInFlight {
        request_ordinal: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultChunk {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalSuccess {
    pub final_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorOpenedResponse {
    pub cursor_id: [u8; 16],
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorPageResponse {
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorCloseAcknowledgement {
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalConfigurationSource {
    Default,
    Override,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalConfiguredValue {
    pub value: u64,
    pub source: LocalConfigurationSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalEffectiveLimits {
    pub result_rows: LocalConfiguredValue,
    pub result_bytes: LocalConfiguredValue,
    pub work: LocalConfiguredValue,
    pub active_ms: LocalConfiguredValue,
    pub memory: LocalConfiguredValue,
    pub cursor_page_rows: LocalConfiguredValue,
    pub cursor_page_bytes: LocalConfiguredValue,
    pub cursor_idle_ms: LocalConfiguredValue,
    pub cursor_lifetime_ms: LocalConfiguredValue,
    pub concurrency: LocalConfiguredValue,
}

impl LocalEffectiveLimits {
    pub fn from_owner_limits(
        owner: OwnerReadLimits,
        source: LocalConfigurationSource,
    ) -> Result<Self, LocalTransportError> {
        owner.validate().map_err(|_| invalid_channel_data())?;
        let shipped = OwnerReadLimits::default();
        let configured = |value, shipped| configured_value(value, shipped, source);
        Ok(Self {
            result_rows: configured(owner.limits.result_rows, shipped.limits.result_rows),
            result_bytes: configured(owner.limits.result_bytes, shipped.limits.result_bytes),
            work: configured(owner.limits.work, shipped.limits.work),
            active_ms: configured(owner.limits.active_ms, shipped.limits.active_ms),
            memory: configured(owner.limits.memory, shipped.limits.memory),
            cursor_page_rows: configured(
                owner.limits.cursor_page_rows,
                shipped.limits.cursor_page_rows,
            ),
            cursor_page_bytes: configured(
                owner.limits.cursor_page_bytes,
                shipped.limits.cursor_page_bytes,
            ),
            cursor_idle_ms: configured(owner.limits.cursor_idle_ms, shipped.limits.cursor_idle_ms),
            cursor_lifetime_ms: configured(
                owner.limits.cursor_lifetime_ms,
                shipped.limits.cursor_lifetime_ms,
            ),
            concurrency: configured(owner.concurrency, shipped.concurrency),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOwnerTimeouts {
    pub request_ms: LocalConfiguredValue,
    pub shutdown_drain_ms: LocalConfiguredValue,
}

impl LocalOwnerTimeouts {
    pub fn from_owner_timeouts(
        timeouts: OwnerServiceTimeouts,
        source: LocalConfigurationSource,
    ) -> Result<Self, LocalTransportError> {
        timeouts.validate().map_err(|_| invalid_channel_data())?;
        let shipped = OwnerServiceTimeouts::default();
        Ok(Self {
            request_ms: configured_value(timeouts.request_ms, shipped.request_ms, source),
            shutdown_drain_ms: configured_value(
                timeouts.shutdown_drain_ms,
                shipped.shutdown_drain_ms,
                source,
            ),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerAdmissionCounters {
    pub capacity: u64,
    pub active_readers: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerMemoryCounters {
    pub used_bytes: u64,
    pub available_bytes: Option<u64>,
}

/// One setting, and whether this deployment actually chose it.
///
/// A caller reads `source` to answer "is this mine to change?", so it has to
/// describe THIS setting rather than the configuration it arrived in. A
/// deployment that narrows one ceiling has not chosen the eight others it left
/// alone, and telling an operator it did sends them looking for a setting that
/// does not exist. So a value equal to what ships is reported as the shipped
/// default whatever else was configured alongside it.
fn configured_value(
    value: u64,
    shipped: u64,
    source: LocalConfigurationSource,
) -> LocalConfiguredValue {
    let source = if source == LocalConfigurationSource::Override && value != shipped {
        LocalConfigurationSource::Override
    } else {
        LocalConfigurationSource::Default
    };
    LocalConfiguredValue { value, source }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOwnerStatusResponse {
    pub status: OwnerReadStatus,
    pub effective_limits: LocalEffectiveLimits,
    pub timeouts: LocalOwnerTimeouts,
    pub admission: OwnerAdmissionCounters,
    pub memory: OwnerMemoryCounters,
}

/// Everything an owner can answer. Result, page, and metadata payloads are
/// already canonical route-neutral bytes, and this layer frames them without
/// converting their contents. Two of these are terminal for any request: a
/// read refusal, and the engine's own answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LocalResponse {
    ResultChunk {
        chunk: ResultChunk,
    },
    TerminalSuccess {
        success: TerminalSuccess,
    },
    CursorOpened {
        opened: CursorOpenedResponse,
    },
    CursorPage {
        page: CursorPageResponse,
    },
    CursorClosed {
        acknowledgement: CursorCloseAcknowledgement,
    },
    Metadata {
        metadata: MetadataResponse,
    },
    Explain {
        payload: Vec<u8>,
    },
    OwnerStatus {
        status: LocalOwnerStatusResponse,
    },
    Custom {
        payload: Vec<u8>,
    },
    Failure {
        failure: ReadFailure,
    },
    /// The owner ran the work and the engine itself answered with something
    /// that is not a read refusal -- a cancelled read, a handler's own error.
    /// A caller is entitled to the same answer an in-process call would have
    /// produced, so the engine outcome travels rather than collapsing into a
    /// generic channel failure.
    EngineFailure {
        failure: LocalEngineFailure,
    },
    /// What the read the owner is running has done so far, sent while that
    /// read is still running.
    ///
    /// A reader waiting on one blocking call cannot otherwise tell a slow
    /// read from a wedged one, and the owner is the only party that knows.
    /// So this is a NONTERMINAL frame belonging to the request in flight: it
    /// carries no part of the answer, never ends the exchange, and the result
    /// itself still stays withheld until the terminal frame arrives. A reader
    /// that wants no progress simply ignores it.
    ///
    /// It appends last so every response variant before it keeps the bytes it
    /// already had on this wire.
    Progress {
        progress: crate::read_progress::ReadProgress,
    },
    /// The owner has APPLIED an interrupt to the request it names.
    ///
    /// Cancelling a read over the channel stops the owning process's
    /// execution of that statement, not merely the reader's wait -- so the
    /// caller that cancelled is entitled to know when that has actually
    /// happened, rather than only that its interrupt was written. Without
    /// this the two cancellations are two events a socket apart, and anything
    /// the caller does next races the owner's own thread.
    ///
    /// Nonterminal, like `Progress`: it carries no part of the answer and
    /// never ends the exchange. It appends last so every response variant
    /// before it keeps the bytes it already had on this wire.
    CancelApplied {
        request_ordinal: u64,
    },
}

/// An engine answer, carried in the read channel's own vocabulary.
///
/// A caller reading over the channel is entitled to the same answer an
/// in-process call would have produced: the same class, carrying the same
/// fields, classified the same way. Naming a handful of outcomes here and
/// flattening the rest into text would silently reclassify most real errors --
/// a parse error and a store fault would both arrive as unstructured prose,
/// and the store fault would lose the class that tells an operator to look at
/// the machine. So the class travels, as one canonical document inside this
/// frame, the same way results and pages already do.
///
/// What travels is [`ReadChannelError`], NOT [`contextdb_core::Error`]. The
/// engine's error enum is the whole engine's vocabulary; carrying it here
/// would have made every sync, purge, trigger, and plugin error a
/// read-protocol byte, because a positional encoder writes a variant's
/// POSITION. The read channel names the classes a read can return, under tags
/// written down in [`super::engine_answer`], and everything else arrives as
/// the fallback class with its name and its words intact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalEngineFailure {
    document: Vec<u8>,
}

impl LocalEngineFailure {
    pub fn from_error(error: &contextdb_core::Error) -> Self {
        let document = ReadChannelError::from(error);
        Self {
            // An engine answer that cannot be written down is itself an
            // engine answer: the caller still learns what happened, in the
            // one class that carries prose.
            document: encode_message(&document).unwrap_or_else(|_| {
                encode_message(&ReadChannelError::Other {
                    message: error.to_string(),
                })
                .unwrap_or_default()
            }),
        }
    }

    /// Rebuild the answer under the memory ceiling in force for this exchange.
    pub fn into_error(self, memory_ceiling: u64) -> contextdb_core::Error {
        decode_message_under_memory_ceiling::<ReadChannelError>(&self.document, memory_ceiling)
            .map(contextdb_core::Error::from)
            .unwrap_or_else(|_| {
                contextdb_core::Error::Other(
                    "the owner's engine answer did not arrive in a readable shape".to_owned(),
                )
            })
    }

    /// The class this answer travels as, for tests that pin the wire.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn document_bytes(&self) -> &[u8] {
        &self.document
    }
}

/// The operation that selected a response also selects the only legal
/// response shape. A failure is legal for every expectation; all successful
/// variants must match exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalResponseExpectation {
    OrdinaryResult,
    CursorOpen,
    CursorFetch,
    CursorClose,
    Metadata(LocalMetadataRequest),
    Explain,
    OwnerStatus,
    Custom,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LocalInboundMessage {
    Handshake(LocalHandshake),
    Request(LocalRequestEnvelope),
    Response(LocalResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalInboundKind {
    Handshake,
    Request,
    Response(LocalResponseExpectation),
}

#[derive(Debug, Clone, Copy)]
pub enum LocalOutboundMessage<'a> {
    Handshake(&'a LocalHandshake),
    Request(&'a LocalRequestEnvelope),
    Response {
        response: &'a LocalResponse,
        expectation: &'a LocalResponseExpectation,
    },
}

/// One production framing and codec boundary for every local protocol shape.
/// It admits the outer prefix before allocating, preflights every nested
/// bincode sequence length before any nested allocation, validates the
/// operation-specific payload, and rejects trailing bytes. Ordinary results
/// use the effective result ceiling, cursor pages use the stricter cursor-page
/// ceiling, and custom payloads use the four-mebibyte frame ceiling.
#[derive(Debug, Default)]
struct OrdinaryBoundaryBytes {
    inbound: u64,
    outbound: u64,
}

#[derive(Debug, Clone, Copy)]
enum OrdinaryBoundaryDirection {
    Inbound,
    Outbound,
}

/// The leading bytes of one payload-carrying answer that earlier frames of
/// this exchange already carried.
///
/// A cursor page or an inspection answer is ONE canonical payload, and a
/// legitimate one can be larger than the frame this channel moves bytes in.
/// Ordinary results have always cut themselves into as many frames as they
/// need; these do the same, and this boundary is where the pieces are put
/// back together, so nothing above the transport ever learns that a frame
/// ceiling exists. Inbound keeps the bytes because it rebuilds the answer;
/// outbound only counts them, because the sender already holds the whole
/// payload and is measuring it against the budget the session declared.
#[derive(Debug, Default)]
struct PayloadAssemblyBytes {
    inbound: Vec<u8>,
    outbound: u64,
    /// Set when the sender has said the answer it is about to write spans
    /// several frames. Without it a chunk offered for a cursor or metadata
    /// exchange is a crossed pairing -- a response variant that does not
    /// belong to the operation asked for -- and stays refused.
    outbound_spans_frames: bool,
}

/// Where one frame sits in a payload-carrying answer.
#[derive(Debug, Clone, Copy)]
enum PayloadPosition {
    /// This frame carries the whole answer, or no answer payload at all.
    Whole,
    /// One frame of an answer known to span several. Its payload is a piece
    /// rather than a decodable answer, and the declared budget is measured
    /// against the total rather than against this piece.
    AnswerFrame { earlier_bytes: u64 },
}

impl PayloadPosition {
    const fn earlier_bytes(self) -> u64 {
        match self {
            Self::Whole => 0,
            Self::AnswerFrame { earlier_bytes } => earlier_bytes,
        }
    }

    const fn is_whole(self) -> bool {
        matches!(self, Self::Whole)
    }
}

/// The declared byte budget one payload-carrying answer is measured against,
/// and `None` for an exchange whose frames carry no such payload. Ordinary
/// results are absent on purpose: they have their own assembly and their own
/// accounting.
const fn payload_answer_budget(
    expectation: &LocalResponseExpectation,
    limits: ReadLimits,
) -> Option<u64> {
    match expectation {
        LocalResponseExpectation::CursorOpen | LocalResponseExpectation::CursorFetch => {
            Some(limits.cursor_page_bytes)
        }
        LocalResponseExpectation::Metadata(_) | LocalResponseExpectation::Explain => {
            Some(limits.result_bytes)
        }
        _ => None,
    }
}

/// The payload this response carries as one canonical answer, if it carries
/// one at all.
fn answer_payload_mut(response: &mut LocalResponse) -> Option<&mut Vec<u8>> {
    match response {
        LocalResponse::CursorOpened { opened } => Some(&mut opened.payload),
        LocalResponse::CursorPage { page } => Some(&mut page.payload),
        LocalResponse::Metadata { metadata } => Some(&mut metadata.payload),
        LocalResponse::Explain { payload } => Some(payload),
        _ => None,
    }
}

/// Whether this frame ends the exchange it belongs to. A progress report and
/// an interrupt acknowledgement do not, so a payload half-assembled around
/// them survives them untouched.
const fn ends_the_exchange(response: &LocalResponse) -> bool {
    !matches!(
        response,
        LocalResponse::ResultChunk { .. }
            | LocalResponse::Progress { .. }
            | LocalResponse::CancelApplied { .. }
    )
}

#[derive(Debug)]
pub struct LocalProtocolBoundary {
    effective_limits: ReadLimits,
    ordinary_bytes: std::sync::Mutex<OrdinaryBoundaryBytes>,
    payload_assembly: std::sync::Mutex<PayloadAssemblyBytes>,
}

impl LocalProtocolBoundary {
    pub fn with_effective_limits(effective_limits: ReadLimits) -> Self {
        Self {
            effective_limits,
            ordinary_bytes: std::sync::Mutex::new(OrdinaryBoundaryBytes::default()),
            payload_assembly: std::sync::Mutex::new(PayloadAssemblyBytes::default()),
        }
    }

    pub const fn effective_limits(&self) -> ReadLimits {
        self.effective_limits
    }

    /// Validate and encode one complete length-prefixed message. Successful
    /// response payloads are decoded and structurally validated here before
    /// the containing response can be emitted.
    pub fn encode_frame(
        &self,
        message: LocalOutboundMessage<'_>,
    ) -> Result<Vec<u8>, LocalTransportError> {
        let ordinary_response = match message {
            LocalOutboundMessage::Response {
                response,
                expectation,
            } if *expectation == LocalResponseExpectation::OrdinaryResult => {
                Some((response, expectation))
            }
            _ => None,
        };
        let encoded = (|| {
            let payload = match message {
                LocalOutboundMessage::Handshake(handshake) => encode_message(handshake)?,
                LocalOutboundMessage::Request(request) => {
                    validate_request(request)?;
                    encode_message(request)?
                }
                LocalOutboundMessage::Response {
                    response,
                    expectation,
                } => {
                    let position = self.note_outbound_answer_frame(response, expectation);
                    validate_response(response, expectation, self.effective_limits, position)?;
                    encode_message(response)?
                }
            };
            frame_length_prefix(payload.len())?;
            if let Some((response, expectation)) = ordinary_response {
                self.admit_ordinary_response(
                    response,
                    expectation,
                    OrdinaryBoundaryDirection::Outbound,
                )?;
            }
            encode_payload_frame(&payload)
        })();
        if encoded.is_err() {
            if ordinary_response.is_some() {
                self.reset_ordinary_outbound();
            }
            self.reset_outbound_answer();
        }
        encoded
    }

    /// Admit, read, decode, and validate one complete message. A declared
    /// hostile nested length is rejected before asking the allocator for that
    /// nested buffer. Every malformed complete message is normalized to the
    /// stable invalid-channel-data refusal.
    pub fn receive_frame(
        &self,
        kind: LocalInboundKind,
        reader: &mut dyn FrameReader,
        allocator: &mut dyn FrameBufferAllocator,
    ) -> Result<LocalInboundMessage, LocalTransportError> {
        let ordinary_response = matches!(
            &kind,
            LocalInboundKind::Response(LocalResponseExpectation::OrdinaryResult)
        );
        let received = (|| {
            let payload =
                read_payload_with_admission(reader, allocator).map_err(frame_read_refusal)?;
            self.decode_payload(kind, &payload)
                .map_err(|_| invalid_channel_data())
        })();
        if received.is_err() && ordinary_response {
            self.reset_ordinary_inbound();
        }
        received
    }

    /// Read and decode one complete message through this same admission and
    /// decode path as [`Self::receive_frame`], but hand back the refusal
    /// decode itself produced instead of collapsing it to the generic
    /// invalid-channel-data document. `receive_frame` collapses on purpose:
    /// its caller is a live socket peer, and a connection that has not yet
    /// authenticated should not learn anything about why its bytes were
    /// unreadable. A caller building or reading a request/response payload
    /// directly -- not off a live, unauthenticated connection -- needs the
    /// typed refusal a decode failure actually carries, such as one naming
    /// the memory ceiling it crossed, to reach it.
    pub(crate) fn receive_frame_preserving_refusal(
        &self,
        kind: LocalInboundKind,
        reader: &mut dyn FrameReader,
        allocator: &mut dyn FrameBufferAllocator,
    ) -> Result<LocalInboundMessage, LocalTransportError> {
        let payload = read_payload_with_admission(reader, allocator).map_err(frame_read_refusal)?;
        self.decode_payload(kind, &payload)
    }

    /// Receive an ordinary result through this same boundary and feed only a
    /// validated response into the publication state machine.
    pub fn receive_ordinary_result_frame(
        &self,
        receiver: &mut OrdinaryResultReceiver,
        reader: &mut dyn FrameReader,
        allocator: &mut dyn FrameBufferAllocator,
    ) -> Result<ResultReceiveOutcome, LocalTransportError> {
        let inbound = match self.receive_frame(
            LocalInboundKind::Response(LocalResponseExpectation::OrdinaryResult),
            reader,
            allocator,
        ) {
            Ok(inbound) => inbound,
            Err(error) => {
                receiver.discard_if_receiving();
                self.reset_ordinary_inbound();
                return Err(error);
            }
        };
        let LocalInboundMessage::Response(response) = inbound else {
            receiver.discard_if_receiving();
            self.reset_ordinary_inbound();
            return Err(invalid_channel_data());
        };
        if let Err(error) = receiver.set_effective_ceilings(
            self.effective_limits.result_bytes,
            self.effective_limits.memory,
        ) {
            receiver.discard_if_receiving();
            self.reset_ordinary_inbound();
            return Err(error);
        }
        match receiver.receive(response) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                self.reset_ordinary_inbound();
                Err(error)
            }
        }
    }

    pub(crate) fn decode_payload(
        &self,
        kind: LocalInboundKind,
        payload: &[u8],
    ) -> Result<LocalInboundMessage, LocalTransportError> {
        match kind {
            LocalInboundKind::Handshake => Ok(LocalInboundMessage::Handshake(
                decode_handshake_exact(payload)?,
            )),
            LocalInboundKind::Request => {
                let memory_ceiling = self.effective_limits.memory;
                preflight_request(payload, memory_ceiling)?;
                let request = decode_message_under_memory_ceiling(payload, memory_ceiling)?;
                validate_request(&request)?;
                Ok(LocalInboundMessage::Request(request))
            }
            LocalInboundKind::Response(expectation) => {
                let memory_ceiling = self.effective_limits.memory;
                let continued = self.answer_assembly_in_flight();
                preflight_response(payload, &expectation, memory_ceiling, continued)?;
                let response: LocalResponse = if continued {
                    decode_preflighted_message(payload)?
                } else {
                    decode_message_under_memory_ceiling(payload, memory_ceiling)?
                };
                // The frame ceiling stops here: an answer that arrived in
                // pieces is whole again before anything validates or reads it.
                let (response, position) = self.assemble_inbound_answer(response, &expectation)?;
                validate_response(&response, &expectation, self.effective_limits, position)?;
                self.admit_ordinary_response(
                    &response,
                    &expectation,
                    OrdinaryBoundaryDirection::Inbound,
                )?;
                Ok(LocalInboundMessage::Response(response))
            }
        }
    }

    fn admit_ordinary_response(
        &self,
        response: &LocalResponse,
        expectation: &LocalResponseExpectation,
        direction: OrdinaryBoundaryDirection,
    ) -> Result<(), LocalTransportError> {
        if *expectation != LocalResponseExpectation::OrdinaryResult {
            return Ok(());
        }
        let mut tracked = self
            .ordinary_bytes
            .lock()
            .map_err(|_| invalid_channel_data())?;
        let count = match direction {
            OrdinaryBoundaryDirection::Inbound => &mut tracked.inbound,
            OrdinaryBoundaryDirection::Outbound => &mut tracked.outbound,
        };
        let (additional, terminal) = match response {
            LocalResponse::ResultChunk { chunk } => (chunk.bytes.len(), false),
            LocalResponse::TerminalSuccess { success } => (success.final_bytes.len(), true),
            LocalResponse::Failure { .. } => {
                *count = 0;
                return Ok(());
            }
            _ => return Ok(()),
        };
        let additional = u64::try_from(additional).map_err(|_| invalid_channel_data())?;
        let Some(total) = (*count).checked_add(additional) else {
            *count = 0;
            return Err(invalid_channel_data());
        };
        if total > self.effective_limits.result_bytes {
            *count = 0;
            return Err(invalid_channel_data());
        }
        *count = if terminal { 0 } else { total };
        Ok(())
    }

    pub(crate) fn reset_ordinary_inbound(&self) {
        if let Ok(mut tracked) = self.ordinary_bytes.lock() {
            tracked.inbound = 0;
        }
    }

    pub(crate) fn reset_ordinary_outbound(&self) {
        if let Ok(mut tracked) = self.ordinary_bytes.lock() {
            tracked.outbound = 0;
        }
    }

    fn answer_assembly_in_flight(&self) -> bool {
        self.payload_assembly
            .lock()
            .is_ok_and(|tracked| !tracked.inbound.is_empty())
    }

    fn reset_outbound_answer(&self) {
        if let Ok(mut tracked) = self.payload_assembly.lock() {
            tracked.outbound = 0;
            tracked.outbound_spans_frames = false;
        }
    }

    /// Account for one outbound frame of a payload-carrying answer and say
    /// where it sits in that answer, so the budget is measured against the
    /// whole thing and only a complete payload is asked to decode.
    /// Say that the answer about to be written spans several frames.
    ///
    /// Only a sender that has actually cut one answer into pieces says this,
    /// and it says so before writing the first piece. Everything else keeps
    /// the rule it always had: a chunk offered for a cursor or metadata
    /// exchange is a response variant crossing into an operation it does not
    /// belong to, and is refused.
    pub fn answer_spans_frames(&self) {
        if let Ok(mut tracked) = self.payload_assembly.lock() {
            tracked.outbound = 0;
            tracked.outbound_spans_frames = true;
        }
    }

    /// Account for one outbound frame of an answer that spans frames and say
    /// where it sits in that answer, so the budget is measured against the
    /// whole thing and only a complete payload is asked to decode.
    fn note_outbound_answer_frame(
        &self,
        response: &LocalResponse,
        expectation: &LocalResponseExpectation,
    ) -> PayloadPosition {
        if payload_answer_budget(expectation, self.effective_limits).is_none() {
            return PayloadPosition::Whole;
        }
        let Ok(mut tracked) = self.payload_assembly.lock() else {
            return PayloadPosition::Whole;
        };
        if !tracked.outbound_spans_frames {
            return PayloadPosition::Whole;
        }
        let earlier_bytes = tracked.outbound;
        match response {
            LocalResponse::ResultChunk { chunk } => {
                tracked.outbound =
                    earlier_bytes.saturating_add(u64::try_from(chunk.bytes.len()).unwrap_or(0));
            }
            response if ends_the_exchange(response) => {
                tracked.outbound = 0;
                tracked.outbound_spans_frames = false;
            }
            _ => return PayloadPosition::Whole,
        }
        PayloadPosition::AnswerFrame { earlier_bytes }
    }

    /// Put an answer that arrived in pieces back together.
    ///
    /// A leading piece is kept and travels on to the caller as the nonterminal
    /// frame it is; the frame that ends the exchange receives every byte that
    /// went before it, so what leaves this boundary is the one complete
    /// payload the owner produced.
    fn assemble_inbound_answer(
        &self,
        mut response: LocalResponse,
        expectation: &LocalResponseExpectation,
    ) -> Result<(LocalResponse, PayloadPosition), LocalTransportError> {
        let Some(budget) = payload_answer_budget(expectation, self.effective_limits) else {
            return Ok((response, PayloadPosition::Whole));
        };
        let mut tracked = self
            .payload_assembly
            .lock()
            .map_err(|_| invalid_channel_data())?;
        if let LocalResponse::ResultChunk { chunk } = &response {
            let earlier_bytes =
                u64::try_from(tracked.inbound.len()).map_err(|_| invalid_channel_data())?;
            let total = u64::try_from(chunk.bytes.len())
                .map_err(|_| invalid_channel_data())?
                .saturating_add(earlier_bytes);
            if total > budget {
                tracked.inbound = Vec::new();
                return Err(invalid_channel_data());
            }
            tracked.inbound.extend_from_slice(&chunk.bytes);
            return Ok((response, PayloadPosition::AnswerFrame { earlier_bytes }));
        }
        if !ends_the_exchange(&response) {
            return Ok((response, PayloadPosition::Whole));
        }
        let earlier = std::mem::take(&mut tracked.inbound);
        if earlier.is_empty() {
            return Ok((response, PayloadPosition::Whole));
        }
        if let Some(slot) = answer_payload_mut(&mut response) {
            let mut whole = earlier;
            whole.extend_from_slice(slot);
            if u64::try_from(whole.len()).map_err(|_| invalid_channel_data())? > budget {
                return Err(invalid_channel_data());
            }
            *slot = whole;
        }
        Ok((response, PayloadPosition::Whole))
    }
}

/// A received ordinary reply cannot be rendered until its terminal success
/// frame arrives. Failures and disconnects discard accumulated frame bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultReceiveState {
    Receiving,
    Published,
    Discarded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssembledOrdinaryResult {
    pub bytes: Vec<u8>,
    pub terminal: TerminalSuccess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultReceiveOutcome {
    Pending,
    Published(AssembledOrdinaryResult),
    Failed(ReadFailure),
    EngineFailed(LocalEngineFailure),
    Disconnected,
}

#[cfg(feature = "test-seams")]
const CANONICAL_RECEIVE_TRACE_CAPACITY: usize = 16;

/// What the assembled bytes contain, independent of where they are stored.
/// The observation an assertion reads back is content-addressed: the caller
/// holding an equal copy of the assembled result can confirm the preflight
/// ran over exactly those bytes under exactly that ceiling.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalBytesContent {
    length: usize,
    digest: u64,
}

/// The exact storage the preflight walked. A decode capability is bound to
/// this, so a second buffer holding equal bytes cannot consume it.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalBytesIdentity {
    address: usize,
    content: CanonicalBytesContent,
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy)]
struct CanonicalPreflightToken {
    bytes: CanonicalBytesIdentity,
    memory_ceiling: u64,
    generation: u64,
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy)]
struct CanonicalPreflightObservation {
    content: CanonicalBytesContent,
    memory_ceiling: u64,
    passed: bool,
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy)]
struct CanonicalDecodePermit {
    bytes: CanonicalBytesIdentity,
    generation: u64,
    authorized: bool,
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy)]
struct CanonicalReceiveTrace {
    events: [u8; CANONICAL_RECEIVE_TRACE_CAPACITY],
    event_count: usize,
    decode_entries: usize,
    next_generation: u64,
    last_preflight: Option<CanonicalPreflightObservation>,
    pending_preflight: Option<CanonicalPreflightToken>,
    decode_permit: Option<CanonicalDecodePermit>,
}

#[cfg(feature = "test-seams")]
impl CanonicalReceiveTrace {
    const fn empty() -> Self {
        Self {
            events: [0; CANONICAL_RECEIVE_TRACE_CAPACITY],
            event_count: 0,
            decode_entries: 0,
            next_generation: 0,
            last_preflight: None,
            pending_preflight: None,
            decode_permit: None,
        }
    }

    /// The trace is a fixed window over the events since the last reset. A
    /// caller that keeps receiving without resetting keeps its window intact
    /// rather than making the receive path fail on an observation buffer.
    fn record(&mut self, event: u8) {
        if let Some(slot) = self.events.get_mut(self.event_count) {
            *slot = event;
            self.event_count += 1;
        }
    }
}

#[cfg(feature = "test-seams")]
std::thread_local! {
    static CANONICAL_RECEIVE_TRACE: std::cell::Cell<CanonicalReceiveTrace> =
        const { std::cell::Cell::new(CanonicalReceiveTrace::empty()) };
}

#[cfg(feature = "test-seams")]
fn canonical_bytes_content(bytes: &[u8]) -> CanonicalBytesContent {
    let mut digest = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        digest ^= u64::from(*byte);
        digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
    }
    CanonicalBytesContent {
        length: bytes.len(),
        digest,
    }
}

#[cfg(feature = "test-seams")]
fn canonical_bytes_identity(bytes: &[u8]) -> CanonicalBytesIdentity {
    CanonicalBytesIdentity {
        address: bytes.as_ptr() as usize,
        content: canonical_bytes_content(bytes),
    }
}

/// An upper bound on the number of nodes the decoded form of this JSON text
/// can hold. Every value in a JSON document is either the whole document or
/// sits immediately after an opening bracket, a comma, or a colon, so counting
/// those bytes cannot undercount the tree. Text that only looks like structure
/// — those bytes inside a quoted string — inflates the count, which charges
/// more than the decode will use and never less.
fn json_node_ceiling(text: &[u8]) -> usize {
    let mut nodes = 1_usize;
    for byte in text {
        if matches!(byte, b'[' | b'{' | b',' | b':') {
            nodes = nodes.saturating_add(1);
        }
    }
    nodes
}

/// Executable canonical grammar that walks the assembled result bytes without
/// allocating anything: every nested container length is bounded by the bytes
/// that remain, and the native allocation each length would claim is charged
/// against the effective memory ceiling. A hostile or truncated assembly is
/// refused here, before the deserializer is asked to build one container.
struct CanonicalAllocationPreflight<'a> {
    bytes: &'a [u8],
    offset: usize,
    charged_bytes: u64,
    memory_ceiling: u64,
}

impl<'a> CanonicalAllocationPreflight<'a> {
    const fn new(bytes: &'a [u8], memory_ceiling: u64) -> Self {
        Self {
            bytes,
            offset: 0,
            charged_bytes: 0,
            memory_ceiling,
        }
    }

    fn malformed<T>() -> Result<T, crate::read_contract::ReadEncodingError> {
        Err(crate::read_contract::ReadEncodingError::InvalidPayload)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], crate::read_contract::ReadEncodingError> {
        let Some(end) = self.offset.checked_add(length) else {
            return Self::malformed();
        };
        let Some(value) = self.bytes.get(self.offset..end) else {
            return Self::malformed();
        };
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, crate::read_contract::ReadEncodingError> {
        Ok(self.take(1)?[0])
    }

    fn fixed(&mut self, length: usize) -> Result<(), crate::read_contract::ReadEncodingError> {
        self.take(length).map(|_| ())
    }

    fn unsigned(
        &mut self,
        maximum_width: usize,
    ) -> Result<u64, crate::read_contract::ReadEncodingError> {
        match self.byte()? {
            value @ 0..=250 => Ok(u64::from(value)),
            251 if maximum_width >= 2 => {
                let bytes: [u8; 2] = self
                    .take(2)?
                    .try_into()
                    .expect("two-byte canonical integer preflight");
                Ok(u64::from(u16::from_le_bytes(bytes)))
            }
            252 if maximum_width >= 4 => {
                let bytes: [u8; 4] = self
                    .take(4)?
                    .try_into()
                    .expect("four-byte canonical integer preflight");
                Ok(u64::from(u32::from_le_bytes(bytes)))
            }
            253 if maximum_width >= 8 => {
                let bytes: [u8; 8] = self
                    .take(8)?
                    .try_into()
                    .expect("eight-byte canonical integer preflight");
                Ok(u64::from_le_bytes(bytes))
            }
            _ => Self::malformed(),
        }
    }

    fn u32(&mut self) -> Result<u32, crate::read_contract::ReadEncodingError> {
        u32::try_from(self.unsigned(4)?)
            .map_err(|_| crate::read_contract::ReadEncodingError::InvalidPayload)
    }

    fn u64(&mut self) -> Result<u64, crate::read_contract::ReadEncodingError> {
        self.unsigned(8)
    }

    fn length(&mut self) -> Result<usize, crate::read_contract::ReadEncodingError> {
        usize::try_from(self.unsigned(8)?)
            .map_err(|_| crate::read_contract::ReadEncodingError::InvalidPayload)
    }

    fn enumeration(
        &mut self,
        maximum: u32,
    ) -> Result<u32, crate::read_contract::ReadEncodingError> {
        let value = self.u32()?;
        if value > maximum {
            return Self::malformed();
        }
        Ok(value)
    }

    fn boolean(&mut self) -> Result<bool, crate::read_contract::ReadEncodingError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Self::malformed(),
        }
    }

    fn option(&mut self) -> Result<bool, crate::read_contract::ReadEncodingError> {
        self.boolean()
    }

    fn charge<T>(&mut self, length: usize) -> Result<(), crate::read_contract::ReadEncodingError> {
        let element_bytes = u64::try_from(std::mem::size_of::<T>().max(1))
            .map_err(|_| crate::read_contract::ReadEncodingError::InvalidPayload)?;
        let length = u64::try_from(length)
            .map_err(|_| crate::read_contract::ReadEncodingError::InvalidPayload)?;
        let claimed = length
            .checked_mul(element_bytes)
            .and_then(|bytes| self.charged_bytes.checked_add(bytes))
            .ok_or(crate::read_contract::ReadEncodingError::InvalidPayload)?;
        if claimed > self.memory_ceiling {
            // A payload that is exactly what it claims to be and simply too
            // large to hold inside the ceiling in force is a budget answer,
            // not unreadable content.
            return Err(
                crate::read_contract::ReadEncodingError::MemoryCeilingExceeded {
                    ceiling: self.memory_ceiling,
                },
            );
        }
        self.charged_bytes = claimed;
        Ok(())
    }

    fn byte_vector(&mut self) -> Result<&'a [u8], crate::read_contract::ReadEncodingError> {
        let length = self.length()?;
        if length > self.remaining() {
            return Self::malformed();
        }
        self.charge::<u8>(length)?;
        self.take(length)
    }

    fn string(&mut self) -> Result<(), crate::read_contract::ReadEncodingError> {
        let bytes = self.byte_vector()?;
        std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|_| crate::read_contract::ReadEncodingError::InvalidPayload)
    }

    /// A JSON value travels as text and decodes into a tree of nodes plus
    /// copies of the text it holds. Both are charged from the text itself,
    /// before the tree is built.
    fn json_text(&mut self) -> Result<(), crate::read_contract::ReadEncodingError> {
        let text = self.byte_vector()?;
        if std::str::from_utf8(text).is_err() {
            return Self::malformed();
        }
        let nodes = json_node_ceiling(text);
        let text_length = text.len();
        self.charge::<serde_json::Value>(nodes)?;
        self.charge::<u8>(text_length)
    }

    fn strings(&mut self) -> Result<(), crate::read_contract::ReadEncodingError> {
        let length = self.length()?;
        if length > self.remaining() {
            return Self::malformed();
        }
        self.charge::<String>(length)?;
        for _ in 0..length {
            self.string()?;
        }
        Ok(())
    }

    fn value(&mut self) -> Result<(), crate::read_contract::ReadEncodingError> {
        match self.enumeration(9)? {
            0 => {}
            1 => {
                self.boolean()?;
            }
            2 | 6 => {
                self.u64()?;
            }
            3 => self.fixed(std::mem::size_of::<f64>())?,
            4 => self.string()?,
            7 => self.json_text()?,
            5 => {
                let uuid = self.byte_vector()?;
                if uuid.len() != 16 {
                    return Self::malformed();
                }
            }
            8 => {
                let length = self.length()?;
                self.charge::<f32>(length)?;
                let byte_length = length
                    .checked_mul(std::mem::size_of::<f32>())
                    .ok_or(crate::read_contract::ReadEncodingError::InvalidPayload)?;
                self.fixed(byte_length)?;
            }
            9 => {
                self.u64()?;
            }
            _ => unreachable!("bounded canonical value discriminant"),
        }
        Ok(())
    }

    fn rows(&mut self) -> Result<(), crate::read_contract::ReadEncodingError> {
        let rows = self.length()?;
        if rows > self.remaining() {
            return Self::malformed();
        }
        self.charge::<Vec<Value>>(rows)?;
        for _ in 0..rows {
            let values = self.length()?;
            if values > self.remaining() {
                return Self::malformed();
            }
            self.charge::<Value>(values)?;
            for _ in 0..values {
                self.value()?;
            }
        }
        Ok(())
    }

    fn query_trace(&mut self) -> Result<(), crate::read_contract::ReadEncodingError> {
        self.string()?;
        if self.option()? {
            self.string()?;
        }
        self.strings()?;

        let candidates = self.length()?;
        if candidates > self.remaining() {
            return Self::malformed();
        }
        self.charge::<crate::read_contract::CanonicalIndexCandidate>(candidates)?;
        for _ in 0..candidates {
            self.string()?;
            self.string()?;
        }

        self.boolean()?;
        if self.option()? {
            self.string()?;
            self.string()?;
        }
        self.u64()?;
        Ok(())
    }

    fn query_result(mut self) -> Result<(), crate::read_contract::ReadEncodingError> {
        self.charge::<crate::read_contract::CanonicalQueryResult>(1)?;
        self.strings()?;
        self.rows()?;
        self.u64()?;
        self.query_trace()?;
        if self.option()? {
            self.strings()?;
        }
        if self.offset != self.bytes.len() {
            return Self::malformed();
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct OrdinaryResultReceiver {
    chunks: Vec<Vec<u8>>,
    buffered_bytes: usize,
    byte_limit: u64,
    memory_ceiling: u64,
    state: Option<ResultReceiveState>,
}

/// Split route-neutral canonical bytes into result envelopes whose complete
/// encoded payload, including the `LocalResponse` variant and vector length,
/// fits the local frame ceiling. The final envelope is always terminal
/// success, including for an empty result.
pub fn split_canonical_result(
    canonical_bytes: &[u8],
) -> Result<Vec<LocalResponse>, LocalTransportError> {
    // Standard bincode encodes this response variant plus a four-mebibyte
    // vector length in six bytes: one enum discriminant and a five-byte
    // variable-length integer. The fixed ceiling guarantees this form.
    const RESULT_ENVELOPE_OVERHEAD: usize = 6;
    let chunk_capacity = MAX_FRAME_BYTES - RESULT_ENVELOPE_OVERHEAD;
    let mut responses = Vec::new();
    let mut remainder = canonical_bytes;
    while remainder.len() > chunk_capacity {
        let (chunk, rest) = remainder.split_at(chunk_capacity);
        responses.push(LocalResponse::ResultChunk {
            chunk: ResultChunk {
                bytes: chunk.to_vec(),
            },
        });
        remainder = rest;
    }
    responses.push(LocalResponse::TerminalSuccess {
        success: TerminalSuccess {
            final_bytes: remainder.to_vec(),
        },
    });
    Ok(responses)
}

/// Split one payload-carrying answer into the frames this channel can move,
/// with the response that ends the exchange carrying the last of them.
///
/// The four-mebibyte local frame is an internal fact about how the owner's
/// channel moves bytes; the byte budgets a session declares are what a caller
/// is refused by. So a cursor page or an inspection answer that is legitimately
/// larger than one frame is cut into as many as it needs, exactly as an
/// ordinary result already is. An answer that fits travels as the single
/// response it always was -- not one byte on the wire moves for it.
pub fn split_payload_answer(
    payload: Vec<u8>,
    terminal: impl Fn(Vec<u8>) -> LocalResponse,
) -> Result<Vec<LocalResponse>, LocalTransportError> {
    // Standard bincode encodes this response variant plus a four-mebibyte
    // vector length in six bytes: one enum discriminant and a five-byte
    // variable-length integer. The fixed ceiling guarantees this form.
    const RESULT_ENVELOPE_OVERHEAD: usize = 6;
    // What the terminal response costs around its payload, measured on the
    // real encoder rather than assumed, plus the four bytes a payload length
    // grows by once it needs the widest variable-length form.
    const LENGTH_GROWTH: usize = 4;
    let empty = encode_message(&terminal(Vec::new()))?.len();
    let terminal_capacity = MAX_FRAME_BYTES.saturating_sub(empty + LENGTH_GROWTH);
    if payload.len() <= terminal_capacity {
        return Ok(vec![terminal(payload)]);
    }
    let chunk_capacity = MAX_FRAME_BYTES - RESULT_ENVELOPE_OVERHEAD;
    let mut responses = Vec::new();
    let mut remainder = payload.as_slice();
    while remainder.len() > terminal_capacity {
        let take = chunk_capacity.min(remainder.len());
        let (chunk, rest) = remainder.split_at(take);
        responses.push(LocalResponse::ResultChunk {
            chunk: ResultChunk {
                bytes: chunk.to_vec(),
            },
        });
        remainder = rest;
    }
    responses.push(terminal(remainder.to_vec()));
    Ok(responses)
}

impl Default for OrdinaryResultReceiver {
    fn default() -> Self {
        Self::new()
    }
}

impl OrdinaryResultReceiver {
    /// A receiver bound to the request's effective ceilings from its first
    /// byte, so a deliberately raised result or memory limit governs terminal
    /// assembly instead of the shipped defaults.
    pub(crate) fn with_effective_ceilings(
        byte_limit: u64,
        memory_ceiling: u64,
    ) -> Result<Self, LocalTransportError> {
        let mut receiver = Self::new();
        receiver.set_effective_ceilings(byte_limit, memory_ceiling)?;
        Ok(receiver)
    }

    pub fn new() -> Self {
        Self {
            chunks: Vec::new(),
            buffered_bytes: 0,
            byte_limit: ReadLimits::SHIPPED_RESULT_BYTES,
            memory_ceiling: ReadLimits::SHIPPED_MEMORY,
            state: Some(ResultReceiveState::Receiving),
        }
    }

    pub fn with_byte_limit(byte_limit: u64) -> Result<Self, LocalTransportError> {
        if byte_limit == 0 {
            return Err(invalid_channel_data());
        }
        Ok(Self {
            chunks: Vec::new(),
            buffered_bytes: 0,
            byte_limit,
            memory_ceiling: ReadLimits::SHIPPED_MEMORY,
            state: Some(ResultReceiveState::Receiving),
        })
    }

    pub fn state(&self) -> ResultReceiveState {
        self.state.unwrap_or(ResultReceiveState::Receiving)
    }

    #[doc(hidden)]
    pub fn buffered_byte_count(&self) -> usize {
        self.buffered_bytes
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub const TEST_EVENT_CANONICAL_PREFLIGHT_PASSED: u8 = 1;

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub const TEST_EVENT_CANONICAL_PREFLIGHT_FAILED: u8 = 2;

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub const TEST_EVENT_CANONICAL_DECODE_AUTHORIZED: u8 = 3;

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub const TEST_EVENT_CANONICAL_DECODE_UNAUTHORIZED: u8 = 4;

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub const TEST_EVENT_PUBLISHED: u8 = 5;

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub const TEST_EVENT_ASSEMBLED_RESULT: u8 = 6;

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn reset_canonical_receive_trace_for_test() {
        CANONICAL_RECEIVE_TRACE.with(|trace| trace.set(CanonicalReceiveTrace::empty()));
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn canonical_receive_events_for_test() -> Vec<u8> {
        CANONICAL_RECEIVE_TRACE.with(|cell| {
            let trace = cell.get();
            trace.events[..trace.event_count].to_vec()
        })
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn canonical_decode_entries_for_test() -> usize {
        CANONICAL_RECEIVE_TRACE.with(|cell| cell.get().decode_entries)
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn canonical_preflight_matches_for_test(
        bytes: &[u8],
        memory_ceiling: u64,
        passed: bool,
    ) -> bool {
        let content = canonical_bytes_content(bytes);
        CANONICAL_RECEIVE_TRACE.with(|cell| {
            cell.get().last_preflight.is_some_and(|observed| {
                observed.content == content
                    && observed.memory_ceiling == memory_ceiling
                    && observed.passed == passed
            })
        })
    }

    /// Walk the assembled canonical bytes allocation-free before the
    /// deserializer sees them. Every nested container length is bounded by the
    /// remaining bytes and charged against the effective memory ceiling, so an
    /// assembly that fits the result-byte ceiling on the wire but would claim
    /// hostile native memory is refused here.
    fn preflight_canonical_result(
        bytes: &[u8],
        memory_ceiling: u64,
    ) -> Result<(), crate::read_contract::ReadEncodingError> {
        let admitted = CanonicalAllocationPreflight::new(bytes, memory_ceiling).query_result();
        #[cfg(feature = "test-seams")]
        Self::observe_canonical_preflight_for_test(bytes, memory_ceiling, admitted.is_ok());
        admitted
    }

    /// Decode the exact assembled bytes the preflight above just admitted.
    /// This is the only canonical decoder entrance on the receive path.
    fn decode_preflighted_canonical_result(
        bytes: &[u8],
        memory_ceiling: u64,
    ) -> Result<crate::read_contract::CanonicalQueryResult, crate::read_contract::ReadEncodingError>
    {
        #[cfg(feature = "test-seams")]
        Self::authorize_canonical_decode_for_test(bytes, memory_ceiling);
        crate::read_contract::decode_query_result_under_memory_ceiling(bytes, memory_ceiling)
    }

    #[cfg(feature = "test-seams")]
    fn observe_canonical_preflight_for_test(bytes: &[u8], memory_ceiling: u64, admitted: bool) {
        CANONICAL_RECEIVE_TRACE.with(|cell| {
            let mut trace = cell.get();
            trace.last_preflight = Some(CanonicalPreflightObservation {
                content: canonical_bytes_content(bytes),
                memory_ceiling,
                passed: admitted,
            });
            trace.pending_preflight = None;
            trace.decode_permit = None;
            if admitted {
                trace.next_generation = trace.next_generation.wrapping_add(1);
                trace.pending_preflight = Some(CanonicalPreflightToken {
                    bytes: canonical_bytes_identity(bytes),
                    memory_ceiling,
                    generation: trace.next_generation,
                });
                trace.record(Self::TEST_EVENT_CANONICAL_PREFLIGHT_PASSED);
            } else {
                trace.record(Self::TEST_EVENT_CANONICAL_PREFLIGHT_FAILED);
            }
            cell.set(trace);
        });
    }

    #[cfg(feature = "test-seams")]
    fn authorize_canonical_decode_for_test(bytes: &[u8], memory_ceiling: u64) {
        let bytes_identity = canonical_bytes_identity(bytes);
        CANONICAL_RECEIVE_TRACE.with(|cell| {
            let mut trace = cell.get();
            let token = trace.pending_preflight.take();
            trace.decode_permit = Some(CanonicalDecodePermit {
                bytes: bytes_identity,
                generation: token.map_or(0, |token| token.generation),
                authorized: token.is_some_and(|token| {
                    token.bytes == bytes_identity && token.memory_ceiling == memory_ceiling
                }),
            });
            cell.set(trace);
        });
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn preflight_canonical_result_for_test(
        bytes: &[u8],
        memory_ceiling: u64,
    ) -> Result<(), crate::read_contract::ReadEncodingError> {
        Self::preflight_canonical_result(bytes, memory_ceiling)
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn decode_preflighted_canonical_result_for_test(
        bytes: &[u8],
        memory_ceiling: u64,
    ) -> Result<crate::read_contract::CanonicalQueryResult, crate::read_contract::ReadEncodingError>
    {
        Self::decode_preflighted_canonical_result(bytes, memory_ceiling)
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub(crate) fn observe_actual_canonical_decode_entry_for_test(bytes: &[u8]) {
        let bytes_identity = canonical_bytes_identity(bytes);
        CANONICAL_RECEIVE_TRACE.with(|cell| {
            let mut trace = cell.get();
            trace.decode_entries = trace.decode_entries.saturating_add(1);
            let permit = trace.decode_permit.take();
            trace.pending_preflight = None;
            let authorized = permit.is_some_and(|permit| {
                permit.authorized && permit.bytes == bytes_identity && permit.generation != 0
            });
            trace.record(if authorized {
                Self::TEST_EVENT_CANONICAL_DECODE_AUTHORIZED
            } else {
                Self::TEST_EVENT_CANONICAL_DECODE_UNAUTHORIZED
            });
            cell.set(trace);
        });
    }

    #[cfg(feature = "test-seams")]
    fn observe_canonical_publication_for_test() {
        CANONICAL_RECEIVE_TRACE.with(|cell| {
            let mut trace = cell.get();
            trace.record(Self::TEST_EVENT_PUBLISHED);
            cell.set(trace);
        });
    }

    #[cfg(feature = "test-seams")]
    fn observe_canonical_assembly_for_test() {
        CANONICAL_RECEIVE_TRACE.with(|cell| {
            let mut trace = cell.get();
            trace.record(Self::TEST_EVENT_ASSEMBLED_RESULT);
            cell.set(trace);
        });
    }

    /// The effective result-byte ceiling bounds the assembled bytes; the
    /// effective memory ceiling travels beside it so terminal assembly can
    /// charge the native allocation the assembly would claim.
    fn set_effective_ceilings(
        &mut self,
        byte_limit: u64,
        memory_ceiling: u64,
    ) -> Result<(), LocalTransportError> {
        if byte_limit == 0
            || memory_ceiling == 0
            || self.state() != ResultReceiveState::Receiving
            || u64::try_from(self.buffered_bytes).unwrap_or(u64::MAX) > byte_limit
        {
            return Err(invalid_channel_data());
        }
        self.byte_limit = byte_limit;
        self.memory_ceiling = memory_ceiling;
        Ok(())
    }

    fn admit_bytes(&self, additional: usize) -> Result<usize, LocalTransportError> {
        let total = self
            .buffered_bytes
            .checked_add(additional)
            .ok_or_else(invalid_channel_data)?;
        if u64::try_from(total).unwrap_or(u64::MAX) > self.byte_limit {
            return Err(invalid_channel_data());
        }
        Ok(total)
    }

    fn discard_buffered(&mut self) {
        self.chunks.clear();
        self.buffered_bytes = 0;
        self.state = Some(ResultReceiveState::Discarded);
    }

    fn discard_if_receiving(&mut self) {
        if self.state() == ResultReceiveState::Receiving {
            self.discard_buffered();
        }
    }

    pub fn receive(
        &mut self,
        response: LocalResponse,
    ) -> Result<ResultReceiveOutcome, LocalTransportError> {
        if self.state() != ResultReceiveState::Receiving {
            return Err(invalid_channel_data());
        }
        match response {
            LocalResponse::ResultChunk { chunk } => {
                if chunk.bytes.len() > MAX_FRAME_BYTES {
                    self.discard_buffered();
                    return Err(invalid_channel_data());
                }
                let new_total = match self.admit_bytes(chunk.bytes.len()) {
                    Ok(total) => total,
                    Err(error) => {
                        self.discard_buffered();
                        return Err(error);
                    }
                };
                self.chunks.push(chunk.bytes);
                self.buffered_bytes = new_total;
                Ok(ResultReceiveOutcome::Pending)
            }
            LocalResponse::TerminalSuccess { success } => {
                if success.final_bytes.len() > MAX_FRAME_BYTES {
                    self.discard_buffered();
                    return Err(invalid_channel_data());
                }
                let final_length = match self.admit_bytes(success.final_bytes.len()) {
                    Ok(total) => total,
                    Err(error) => {
                        self.discard_buffered();
                        return Err(error);
                    }
                };
                let mut bytes = Vec::with_capacity(final_length);
                for chunk in std::mem::take(&mut self.chunks) {
                    bytes.extend_from_slice(&chunk);
                }
                bytes.extend_from_slice(&success.final_bytes);
                self.buffered_bytes = 0;
                // Only an assembly that survives the allocation-free walk and
                // then decodes as one exact canonical result is published; the
                // decoded value itself is released here, because the caller is
                // handed the assembled bytes.
                let admitted = Self::preflight_canonical_result(&bytes, self.memory_ceiling)
                    .and_then(|()| {
                        Self::decode_preflighted_canonical_result(&bytes, self.memory_ceiling)
                    })
                    .map(|_decoded| ());
                if admitted.is_err() {
                    self.discard_buffered();
                    return Err(invalid_channel_data());
                }
                #[cfg(feature = "test-seams")]
                Self::observe_canonical_publication_for_test();
                self.state = Some(ResultReceiveState::Published);
                #[cfg(feature = "test-seams")]
                Self::observe_canonical_assembly_for_test();
                Ok(ResultReceiveOutcome::Published(AssembledOrdinaryResult {
                    bytes,
                    terminal: success,
                }))
            }
            LocalResponse::Failure { failure } => {
                self.discard_buffered();
                Ok(ResultReceiveOutcome::Failed(failure))
            }
            LocalResponse::EngineFailure { failure } => {
                self.discard_buffered();
                Ok(ResultReceiveOutcome::EngineFailed(failure))
            }
            _ => {
                self.discard_buffered();
                Err(invalid_channel_data())
            }
        }
    }

    pub fn disconnect(&mut self) -> Result<ResultReceiveOutcome, LocalTransportError> {
        if self.state() != ResultReceiveState::Receiving {
            return Err(invalid_channel_data());
        }
        self.discard_buffered();
        Ok(ResultReceiveOutcome::Disconnected)
    }
}

/// Encode one local payload with bincode standard configuration.
pub fn encode_message<T: Serialize>(message: &T) -> Result<Vec<u8>, LocalTransportError> {
    encode_to_vec(message, standard())
        .map_err(|_| LocalTransportError::Payload(PayloadViolation::Malformed))
}

/// Prove one response can be encoded and framed under the shared wire-format
/// rules, without validating it against a caller's declared response-shape
/// expectation or effective limits. The owner service uses this to prove a
/// response it is about to commit internal state for -- a retained cursor, a
/// stored resource -- can actually be carried before committing that state;
/// the shape- and limit-aware validation a caller is entitled to
/// (`LocalProtocolBoundary::encode_frame`) still runs moments later, at the
/// real send.
pub(crate) fn response_is_encodable(response: &LocalResponse) -> Result<(), LocalTransportError> {
    let encoded = encode_message(response)?;
    encode_payload_frame(&encoded)?;
    Ok(())
}

/// The decoder's own limit counts the bytes it reads off the wire, and it
/// counts each integer field at its fixed width rather than the shorter
/// variable-length form actually consumed: one maximal result chunk claims
/// twelve units for a six-byte envelope. Stated in those units, this backstop
/// admits any payload the four-mebibyte wire ceiling already admits, at the
/// decoder's worst-case eight units per wire byte. It bounds wire reads only.
/// What the decoded form costs in live memory is bounded before this point, by
/// the allocation-free preflight below.
const DECODE_WIRE_CLAIM_CEILING: usize = MAX_FRAME_BYTES * 8;

/// Decode one complete bincode-standard payload and reject trailing bytes.
/// The decoded footprint is charged against the shipped memory ceiling; a
/// caller holding narrower or wider effective limits decodes through
/// [`decode_message_under_memory_ceiling`] instead.
pub fn decode_message_exact<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, LocalTransportError> {
    decode_message_under_memory_ceiling(bytes, ReadLimits::SHIPPED_MEMORY)
}

/// Decode one complete bincode-standard payload whose decoded footprint is
/// charged against the memory ceiling in force for this exchange.
pub(crate) fn decode_message_under_memory_ceiling<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<T, LocalTransportError> {
    preflight_known_message::<T>(bytes, memory_ceiling)?;
    decode_preflighted_message(bytes)
}

/// Decode a payload whose structural preflight has already run, with the
/// caller's own expectation in hand. A frame carrying the TAIL of an answer
/// that arrived in pieces is exactly that case: its payload is not a nested
/// answer of its own, so the shape pass that would insist it be one has
/// already been made to stand aside, and repeating it here by type name would
/// undo that.
fn decode_preflighted_message<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, LocalTransportError> {
    let configuration = standard().with_limit::<DECODE_WIRE_CLAIM_CEILING>();
    let (value, consumed) = decode_from_slice(bytes, configuration)
        .map_err(|_| LocalTransportError::Payload(PayloadViolation::Malformed))?;
    if consumed != bytes.len() {
        return Err(LocalTransportError::Payload(
            PayloadViolation::TrailingBytes,
        ));
    }
    Ok(value)
}

/// The live memory one payload would occupy once decoded, accumulated as the
/// grammar is walked. Charges are cumulative and are never given back: every
/// container a payload declares coexists with the containers enclosing it, so
/// what bounds the allocation is the total the whole payload would hold at
/// once. A payload nested inside another carries the enclosing total forward,
/// so nesting cannot buy a fresh allowance.
#[derive(Debug, Clone, Copy)]
struct WireBudget {
    charged_bytes: u64,
    memory_ceiling: u64,
}

/// Allocation-free structural pass over the exact standard-bincode grammar
/// used by the local channel. The serde compatibility decoder does not charge
/// sequence claims to bincode's configured limit, so dynamic lengths must be
/// admitted here before typed deserialization can allocate them.
struct WirePreflight<'a> {
    bytes: &'a [u8],
    offset: usize,
    budget: WireBudget,
    /// The largest buffer this pass will walk at all.
    ///
    /// For bytes read as ONE FRAME that is the frame ceiling, which is what
    /// the wire itself already admitted. For a complete canonical answer --
    /// a cursor page, an inspection body -- it is the length the caller has
    /// already admitted against the byte budget the session declared, because
    /// such an answer may legitimately be larger than one frame and arrive in
    /// pieces. Letting the frame ceiling govern there would make an internal
    /// fact about how bytes move into a ceiling a caller is refused by.
    byte_ceiling: usize,
}

impl<'a> WirePreflight<'a> {
    const fn with_memory_ceiling(bytes: &'a [u8], memory_ceiling: u64) -> Self {
        Self::with_budget(
            bytes,
            WireBudget {
                charged_bytes: 0,
                memory_ceiling,
            },
        )
    }

    /// A pass over one complete canonical answer the caller has already
    /// admitted against its declared byte budget -- see `byte_ceiling`.
    const fn over_admitted_answer(bytes: &'a [u8], memory_ceiling: u64) -> Self {
        let mut wire = Self::with_memory_ceiling(bytes, memory_ceiling);
        wire.byte_ceiling = bytes.len();
        wire
    }

    const fn with_budget(bytes: &'a [u8], budget: WireBudget) -> Self {
        Self {
            bytes,
            offset: 0,
            budget,
            byte_ceiling: MAX_FRAME_BYTES,
        }
    }

    fn malformed<T>() -> Result<T, LocalTransportError> {
        Err(LocalTransportError::Payload(PayloadViolation::Malformed))
    }

    fn finish(self) -> Result<(), LocalTransportError> {
        self.finish_into_budget().map(|_| ())
    }

    /// Complete this pass and hand back the running total, so a payload that
    /// was walked inside another keeps charging the enclosing budget.
    fn finish_into_budget(self) -> Result<WireBudget, LocalTransportError> {
        if self.bytes.len() > self.byte_ceiling || self.offset != self.bytes.len() {
            return Err(LocalTransportError::Payload(
                PayloadViolation::TrailingBytes,
            ));
        }
        Ok(self.budget)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], LocalTransportError> {
        if self.bytes.len() > self.byte_ceiling {
            return Self::malformed();
        }
        let Some(end) = self.offset.checked_add(length) else {
            return Self::malformed();
        };
        let Some(value) = self.bytes.get(self.offset..end) else {
            return Self::malformed();
        };
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, LocalTransportError> {
        Ok(self.take(1)?[0])
    }

    fn fixed(&mut self, length: usize) -> Result<(), LocalTransportError> {
        self.take(length).map(|_| ())
    }

    fn unsigned(&mut self, maximum_width: usize) -> Result<u64, LocalTransportError> {
        match self.byte()? {
            value @ 0..=250 => Ok(u64::from(value)),
            251 if maximum_width >= 2 => {
                let bytes: [u8; 2] = self
                    .take(2)?
                    .try_into()
                    .expect("two-byte integer preflight");
                Ok(u64::from(u16::from_le_bytes(bytes)))
            }
            252 if maximum_width >= 4 => {
                let bytes: [u8; 4] = self
                    .take(4)?
                    .try_into()
                    .expect("four-byte integer preflight");
                Ok(u64::from(u32::from_le_bytes(bytes)))
            }
            253 if maximum_width >= 8 => {
                let bytes: [u8; 8] = self
                    .take(8)?
                    .try_into()
                    .expect("eight-byte integer preflight");
                Ok(u64::from_le_bytes(bytes))
            }
            _ => Self::malformed(),
        }
    }

    fn u16(&mut self) -> Result<u16, LocalTransportError> {
        u16::try_from(self.unsigned(2)?)
            .map_err(|_| LocalTransportError::Payload(PayloadViolation::Malformed))
    }

    fn u32(&mut self) -> Result<u32, LocalTransportError> {
        u32::try_from(self.unsigned(4)?)
            .map_err(|_| LocalTransportError::Payload(PayloadViolation::Malformed))
    }

    fn u64(&mut self) -> Result<u64, LocalTransportError> {
        self.unsigned(8)
    }

    fn length(&mut self) -> Result<usize, LocalTransportError> {
        usize::try_from(self.unsigned(8)?)
            .map_err(|_| LocalTransportError::Payload(PayloadViolation::Malformed))
    }

    fn enumeration(&mut self, maximum: u32) -> Result<u32, LocalTransportError> {
        let value = self.u32()?;
        if value > maximum {
            return Self::malformed();
        }
        Ok(value)
    }

    fn boolean(&mut self) -> Result<bool, LocalTransportError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Self::malformed(),
        }
    }

    fn option(&mut self) -> Result<bool, LocalTransportError> {
        self.boolean()
    }

    /// Charge what one declared container costs in live memory once decoded.
    /// The total stands for the rest of the payload, so a container nested in
    /// another is charged on top of it rather than in place of it.
    fn charge<T>(&mut self, length: usize) -> Result<(), LocalTransportError> {
        let element_bytes = u64::try_from(std::mem::size_of::<T>().max(1))
            .map_err(|_| LocalTransportError::Payload(PayloadViolation::Malformed))?;
        let length = u64::try_from(length)
            .map_err(|_| LocalTransportError::Payload(PayloadViolation::Malformed))?;
        let charged = length
            .checked_mul(element_bytes)
            .and_then(|bytes| self.budget.charged_bytes.checked_add(bytes))
            .ok_or(LocalTransportError::Payload(PayloadViolation::Malformed))?;
        if charged > self.budget.memory_ceiling {
            return Err(LocalTransportError::Payload(
                PayloadViolation::MemoryCeilingExceeded {
                    ceiling: self.budget.memory_ceiling,
                },
            ));
        }
        self.budget.charged_bytes = charged;
        Ok(())
    }

    /// Walk a cursor page carried inside this payload under the same running
    /// total, so the page cannot claim a fresh allowance by being nested.
    fn nested_cursor_page(&mut self, payload: &[u8]) -> Result<(), LocalTransportError> {
        let mut nested = WirePreflight::with_budget(payload, self.budget);
        nested.cursor_page()?;
        self.budget = nested.finish_into_budget()?;
        Ok(())
    }

    /// Walk a metadata page carried inside this payload under the same running
    /// total, so the page cannot claim a fresh allowance by being nested.
    fn nested_metadata_page(&mut self, payload: &[u8]) -> Result<(), LocalTransportError> {
        let mut nested = WirePreflight::with_budget(payload, self.budget);
        nested.metadata_page()?;
        self.budget = nested.finish_into_budget()?;
        Ok(())
    }

    fn byte_vector(&mut self) -> Result<&'a [u8], LocalTransportError> {
        let length = self.length()?;
        self.charge::<u8>(length)?;
        self.take(length)
    }

    fn string(&mut self) -> Result<(), LocalTransportError> {
        let bytes = self.byte_vector()?;
        std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|_| LocalTransportError::Payload(PayloadViolation::Malformed))
    }

    /// A JSON value travels as text and decodes into a tree of nodes plus
    /// copies of the text it holds. Both are charged from the text itself,
    /// before the tree is built.
    fn json_text(&mut self) -> Result<(), LocalTransportError> {
        let text = self.byte_vector()?;
        if std::str::from_utf8(text).is_err() {
            return Self::malformed();
        }
        let nodes = json_node_ceiling(text);
        let text_length = text.len();
        self.charge::<serde_json::Value>(nodes)?;
        self.charge::<u8>(text_length)
    }

    fn value(&mut self) -> Result<(), LocalTransportError> {
        match self.enumeration(9)? {
            0 => {}
            1 => {
                self.boolean()?;
            }
            2 | 6 => {
                self.u64()?;
            }
            3 => self.fixed(std::mem::size_of::<f64>())?,
            4 => self.string()?,
            7 => self.json_text()?,
            5 => {
                let bytes = self.byte_vector()?;
                if bytes.len() != 16 {
                    return Self::malformed();
                }
            }
            8 => {
                let length = self.length()?;
                self.charge::<f32>(length)?;
                let byte_length = length
                    .checked_mul(std::mem::size_of::<f32>())
                    .ok_or(LocalTransportError::Payload(PayloadViolation::Malformed))?;
                self.fixed(byte_length)?;
            }
            9 => {
                self.u64()?;
            }
            _ => unreachable!("bounded value discriminant"),
        }
        Ok(())
    }

    /// Walk one reader's declared visibility. Both sets are unbounded in
    /// shape, so each declared length is charged before its elements are read
    /// and a length larger than the bytes that remain is malformed on sight.
    fn read_declaration(&mut self) -> Result<(), LocalTransportError> {
        if self.option()? {
            let length = self.length()?;
            if length > self.remaining() {
                return Self::malformed();
            }
            self.charge::<ContextId>(length)?;
            for _ in 0..length {
                let bytes = self.byte_vector()?;
                if bytes.len() != 16 {
                    return Self::malformed();
                }
            }
        }
        if self.option()? {
            let length = self.length()?;
            if length > self.remaining() {
                return Self::malformed();
            }
            self.charge::<ScopeLabel>(length)?;
            for _ in 0..length {
                self.string()?;
            }
        }
        if self.option()? {
            match self.enumeration(2)? {
                0 => {}
                1 | 2 => self.string()?,
                _ => unreachable!("bounded principal discriminant"),
            }
        }
        Ok(())
    }

    fn string_value_map(&mut self) -> Result<(), LocalTransportError> {
        let length = self.length()?;
        if length > self.remaining() {
            return Self::malformed();
        }
        self.charge::<(String, Value)>(length)?;
        for _ in 0..length {
            self.string()?;
            self.value()?;
        }
        Ok(())
    }

    fn cursor_page(&mut self) -> Result<(), LocalTransportError> {
        let columns = self.length()?;
        if columns > self.remaining() {
            return Self::malformed();
        }
        self.charge::<String>(columns)?;
        for _ in 0..columns {
            self.string()?;
        }

        let rows = self.length()?;
        if rows > self.remaining() {
            return Self::malformed();
        }
        self.charge::<Vec<Value>>(rows)?;
        for _ in 0..rows {
            let values = self.length()?;
            if values > self.remaining() {
                return Self::malformed();
            }
            self.charge::<Value>(values)?;
            for _ in 0..values {
                self.value()?;
            }
        }
        self.boolean()?;
        Ok(())
    }

    fn metadata_page(&mut self) -> Result<(), LocalTransportError> {
        self.enumeration(1)?;
        let items = self.length()?;
        if items > self.remaining() {
            return Self::malformed();
        }
        self.charge::<contextdb_core::read_contract::MetadataItem>(items)?;
        for _ in 0..items {
            match self.enumeration(1)? {
                0 => self.string()?,
                1 => self.string_value_map()?,
                _ => unreachable!("bounded metadata-item discriminant"),
            }
        }
        self.boolean()?;
        if self.option()? {
            self.string()?;
        }
        Ok(())
    }

    /// The engine answer travels as one canonical document. Its length is
    /// admitted here, before the buffer exists; the document itself is walked
    /// under the same running total when it is decoded.
    /// One progress report: the phase, then the six counters, one of which
    /// is optional. Every field is a fixed-width number, so a progress frame
    /// declares no container and can claim no memory beyond its own bytes.
    fn progress(&mut self) -> Result<(), LocalTransportError> {
        self.enumeration(1)?;
        self.u64()?;
        self.u64()?;
        self.u64()?;
        if self.option()? {
            self.u64()?;
        }
        self.u64()?;
        self.u64()?;
        Ok(())
    }

    fn engine_failure(&mut self) -> Result<(), LocalTransportError> {
        self.byte_vector()?;
        Ok(())
    }

    fn failure(&mut self) -> Result<(), LocalTransportError> {
        self.enumeration(READ_FAILURE_KIND_MAXIMUM_DISCRIMINANT)?;
        match self.enumeration(READ_FAILURE_DETAIL_MAXIMUM_DISCRIMINANT)? {
            0 => {}
            1 => {
                self.u64()?;
                let readers = self.length()?;
                if readers > self.remaining() {
                    return Self::malformed();
                }
                self.charge::<contextdb_core::read_contract::ReaderBreadcrumb>(readers)?;
                for _ in 0..readers {
                    self.u32()?;
                    self.string()?;
                    self.u64()?;
                }
            }
            2 => {
                self.enumeration(5)?;
                self.u64()?;
                if self.option()? {
                    self.u64()?;
                    self.string()?;
                }
                if self.option()? {
                    self.string()?;
                    self.string()?;
                }
            }
            3 => {
                self.enumeration(1)?;
            }
            4 => self.string()?,
            5 => {
                if self.option()? {
                    self.u64()?;
                }
                self.string()?;
            }
            6 => self.string()?,
            _ => unreachable!("bounded failure-detail discriminant"),
        }
        Ok(())
    }

    fn configured_value(&mut self) -> Result<(), LocalTransportError> {
        self.u64()?;
        self.enumeration(1)?;
        Ok(())
    }

    fn owner_status(&mut self) -> Result<(), LocalTransportError> {
        self.enumeration(3)?;
        if self.option()? {
            match self.enumeration(3)? {
                0 | 2 | 3 => {}
                1 => self.string()?,
                _ => unreachable!("bounded owner-reason discriminant"),
            }
        }
        for _ in 0..10 {
            self.configured_value()?;
        }
        for _ in 0..2 {
            self.configured_value()?;
        }
        self.u64()?;
        self.u64()?;
        self.u64()?;
        if self.option()? {
            self.u64()?;
        }
        Ok(())
    }
}

fn preflight_handshake(bytes: &[u8], memory_ceiling: u64) -> Result<(), LocalTransportError> {
    let mut wire = WirePreflight::with_memory_ceiling(bytes, memory_ceiling);
    wire.fixed(LOCAL_PROTOCOL_MARKER.len())?;
    // The version NAMES the shape of everything after it, so it is read before
    // any of that shape is walked. A peer speaking a different version is told
    // so in the words the protocol has for exactly that -- rather than being
    // reported as a peer sending damaged bytes, which is what walking its
    // handshake under THIS version's shape would conclude.
    if wire.u16()? != LOCAL_PROTOCOL_VERSION {
        return Err(super::refusal(
            contextdb_core::read_contract::ReadFailureKind::LocalProtocolMismatch,
        ));
    }
    wire.fixed(16)?;
    wire.fixed(16)?;
    wire.u64()?;
    if wire.option()? {
        wire.read_declaration()?;
    }
    wire.finish()
}

fn preflight_known_message<T>(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<(), LocalTransportError> {
    let message = std::any::type_name::<T>();
    if message.ends_with("::LocalHandshake") {
        return preflight_handshake(bytes, memory_ceiling);
    }
    if message.ends_with("::LocalRequestEnvelope") {
        return preflight_request(bytes, memory_ceiling);
    }
    if message.ends_with("::LocalResponse") {
        return preflight_response(
            bytes,
            &LocalResponseExpectation::Custom,
            memory_ceiling,
            false,
        );
    }
    if message == std::any::type_name::<Value>() {
        let mut wire = WirePreflight::with_memory_ceiling(bytes, memory_ceiling);
        wire.value()?;
        return wire.finish();
    }
    if message.ends_with("::CursorPage") {
        return preflight_cursor_page_payload(bytes, memory_ceiling);
    }
    if message.ends_with("::MetadataPage") {
        return preflight_metadata_page_payload(bytes, memory_ceiling);
    }
    if message.ends_with("::ReadChannelError") {
        return preflight_read_channel_error(bytes, memory_ceiling);
    }
    Ok(())
}

/// The engine answer's own grammar, walked without allocating. The tag names
/// the body shape, and the shape is read from the one table that also drives
/// how the document is written, so a body can never be admitted under a
/// grammar different from the one that produced it.
pub(crate) fn preflight_read_channel_error(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<(), LocalTransportError> {
    let mut wire = WirePreflight::with_memory_ceiling(bytes, memory_ceiling);
    read_channel_error_body(&mut wire)?;
    wire.finish()
}

fn read_channel_error_body(wire: &mut WirePreflight<'_>) -> Result<(), LocalTransportError> {
    let tag = wire.u16()?;
    let Some(grammar) = body_grammar(tag) else {
        return WirePreflight::malformed();
    };
    for field in grammar {
        match field {
            BodyField::Word => {
                wire.string()?;
            }
            BodyField::Number32 => {
                wire.u32()?;
            }
            BodyField::Number64 => {
                wire.u64()?;
            }
            BodyField::WordSequence => {
                let words = wire.length()?;
                if words > wire.remaining() {
                    return WirePreflight::malformed();
                }
                wire.charge::<String>(words)?;
                for _ in 0..words {
                    wire.string()?;
                }
            }
            BodyField::Failure => {
                wire.failure()?;
            }
        }
    }
    Ok(())
}

fn preflight_read_limits(wire: &mut WirePreflight<'_>) -> Result<(), LocalTransportError> {
    for _ in 0..9 {
        wire.u64()?;
    }
    Ok(())
}

fn preflight_request(bytes: &[u8], memory_ceiling: u64) -> Result<(), LocalTransportError> {
    let mut wire = WirePreflight::with_memory_ceiling(bytes, memory_ceiling);
    preflight_read_limits(&mut wire)?;
    match wire.enumeration(8)? {
        0 | 1 | 5 => {
            wire.string()?;
            wire.string_value_map()?;
        }
        2 => {
            wire.fixed(16)?;
            if wire.option()? {
                wire.u64()?;
            }
        }
        3 => wire.fixed(16)?,
        4 => match wire.enumeration(3)? {
            0 | 2 => {
                if wire.option()? {
                    wire.string()?;
                }
            }
            1 => wire.string()?,
            3 => {}
            _ => unreachable!("bounded metadata-request discriminant"),
        },
        6 => {}
        7 => {
            wire.string()?;
            wire.byte_vector()?;
        }
        8 => {
            wire.u64()?;
        }
        _ => unreachable!("bounded request discriminant"),
    }
    wire.finish()
}

/// Admit one canonical query-result payload on its own, charging its decoded
/// footprint against the memory ceiling in force.
pub(crate) fn preflight_canonical_query_result(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<(), crate::read_contract::ReadEncodingError> {
    CanonicalAllocationPreflight::new(bytes, memory_ceiling).query_result()
}

/// Admit one cursor-page payload on its own, charging its decoded footprint
/// against the memory ceiling in force.
pub(crate) fn preflight_cursor_page_payload(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<(), LocalTransportError> {
    let mut wire = WirePreflight::over_admitted_answer(bytes, memory_ceiling);
    wire.cursor_page()?;
    wire.finish()
}

/// Admit one metadata-page payload on its own, charging its decoded footprint
/// against the memory ceiling in force.
pub(crate) fn preflight_metadata_page_payload(
    bytes: &[u8],
    memory_ceiling: u64,
) -> Result<(), LocalTransportError> {
    let mut wire = WirePreflight::over_admitted_answer(bytes, memory_ceiling);
    wire.metadata_page()?;
    wire.finish()
}

/// The highest refusal-kind discriminant this wire admits, read from the
/// contract's own kind list so appending a kind cannot leave the preflight
/// refusing a refusal the owner can legitimately send.
const READ_FAILURE_KIND_MAXIMUM_DISCRIMINANT: u32 =
    (contextdb_core::read_contract::ReadFailureKind::ALL.len() - 1) as u32;

/// The highest refusal-detail discriminant this wire admits. The detail
/// vocabulary has no list to read a length from, so appending a detail means
/// raising this number and walking the new shape in `failure` above --
/// otherwise the preflight refuses a refusal the owner legitimately sends.
const READ_FAILURE_DETAIL_MAXIMUM_DISCRIMINANT: u32 = 6;

/// `continued` says earlier frames of this exchange already carried the
/// leading bytes of the answer, so this frame's payload is a tail: it is not a
/// nested answer of its own and cannot be read as one. The complete payload is
/// still checked, once, after the pieces are put back together.
fn preflight_response(
    bytes: &[u8],
    expectation: &LocalResponseExpectation,
    memory_ceiling: u64,
    continued: bool,
) -> Result<(), LocalTransportError> {
    let mut wire = WirePreflight::with_memory_ceiling(bytes, memory_ceiling);
    match wire.enumeration(12)? {
        0 | 1 | 6 | 8 => {
            wire.byte_vector()?;
        }
        2 => {
            wire.fixed(16)?;
            let payload = wire.byte_vector()?;
            if !continued {
                wire.nested_cursor_page(payload)?;
            }
        }
        3 => {
            let payload = wire.byte_vector()?;
            if !continued {
                wire.nested_cursor_page(payload)?;
            }
        }
        4 => {
            wire.boolean()?;
        }
        5 => {
            let payload = wire.byte_vector()?;
            if !continued
                && matches!(
                    expectation,
                    LocalResponseExpectation::Metadata(
                        LocalMetadataRequest::Tables { .. }
                            | LocalMetadataRequest::EventsStatus { .. }
                    )
                )
            {
                wire.nested_metadata_page(payload)?;
            }
        }
        7 => wire.owner_status()?,
        9 => wire.failure()?,
        10 => wire.engine_failure()?,
        11 => wire.progress()?,
        12 => {
            wire.u64()?;
        }
        _ => unreachable!("bounded response discriminant"),
    }
    wire.finish()
}

/// Check a payload length before allocating the payload or writing a prefix.
pub fn frame_length_prefix(payload_len: usize) -> Result<[u8; 4], LocalTransportError> {
    if payload_len > MAX_FRAME_BYTES {
        return Err(LocalTransportError::Frame(
            FrameViolation::LengthExceedsMaximum,
        ));
    }
    let length = u32::try_from(payload_len)
        .map_err(|_| LocalTransportError::Frame(FrameViolation::LengthExceedsMaximum))?;
    Ok(length.to_be_bytes())
}

/// A reader/allocator split makes it possible to prove that a hostile length
/// prefix is rejected before any payload-sized allocation is requested.
pub trait FrameReader {
    fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), LocalTransportError>;
}

pub trait FrameBufferAllocator {
    fn allocate(&mut self, length: usize) -> Result<Vec<u8>, LocalTransportError>;
}

pub fn read_payload_with_admission(
    reader: &mut dyn FrameReader,
    allocator: &mut dyn FrameBufferAllocator,
) -> Result<Vec<u8>, LocalTransportError> {
    let mut prefix = [0_u8; 4];
    reader.read_exact(&mut prefix)?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(LocalTransportError::Frame(
            FrameViolation::LengthExceedsMaximum,
        ));
    }
    let mut payload = allocator.allocate(length)?;
    if payload.len() != length {
        return Err(LocalTransportError::Frame(FrameViolation::TruncatedPayload));
    }
    reader.read_exact(&mut payload)?;
    Ok(payload)
}

/// Read, admit, decode, and publish one ordinary-result frame through the
/// production receiving state machine.
pub fn receive_framed_ordinary_result(
    receiver: &mut OrdinaryResultReceiver,
    reader: &mut dyn FrameReader,
    allocator: &mut dyn FrameBufferAllocator,
) -> Result<ResultReceiveOutcome, LocalTransportError> {
    let response = match (|| {
        let payload = read_payload_with_admission(reader, allocator).map_err(frame_read_refusal)?;
        preflight_response(
            &payload,
            &LocalResponseExpectation::OrdinaryResult,
            ReadLimits::SHIPPED_MEMORY,
            false,
        )?;
        decode_message_exact::<LocalResponse>(&payload)
    })() {
        Ok(response) => response,
        Err(error) => {
            receiver.discard_if_receiving();
            return Err(error);
        }
    };
    receiver.receive(response)
}

/// Encode one complete four-byte-length-prefixed payload frame.
pub fn encode_payload_frame(payload: &[u8]) -> Result<Vec<u8>, LocalTransportError> {
    let prefix = frame_length_prefix(payload.len())?;
    let capacity = payload
        .len()
        .checked_add(prefix.len())
        .ok_or(LocalTransportError::Frame(
            FrameViolation::LengthExceedsMaximum,
        ))?;
    let mut frame = Vec::with_capacity(capacity);
    frame.extend_from_slice(&prefix);
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Decode precisely one complete frame, with no trailing bytes.
pub fn decode_frame_exact(frame: &[u8]) -> Result<Vec<u8>, LocalTransportError> {
    let Some(prefix) = frame.get(..4) else {
        return Err(LocalTransportError::Frame(FrameViolation::TruncatedLength));
    };
    let length = u32::from_be_bytes(prefix.try_into().expect("fixed frame prefix")) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(LocalTransportError::Frame(
            FrameViolation::LengthExceedsMaximum,
        ));
    }
    let payload_end = 4_usize
        .checked_add(length)
        .ok_or(LocalTransportError::Frame(
            FrameViolation::LengthExceedsMaximum,
        ))?;
    if frame.len() < payload_end {
        return Err(LocalTransportError::Frame(FrameViolation::TruncatedPayload));
    }
    if frame.len() != payload_end {
        return Err(LocalTransportError::Frame(FrameViolation::TrailingBytes));
    }
    Ok(frame[4..].to_vec())
}

fn validate_request(request: &LocalRequestEnvelope) -> Result<(), LocalTransportError> {
    request
        .limits
        .validate()
        .map_err(|_| invalid_channel_data())
}

fn validate_response(
    response: &LocalResponse,
    expectation: &LocalResponseExpectation,
    limits: ReadLimits,
    position: PayloadPosition,
) -> Result<(), LocalTransportError> {
    limits.validate().map_err(|_| invalid_channel_data())?;
    // An answer larger than one frame travels as leading chunks followed by
    // the response that ends the exchange -- the same shape an ordinary
    // result has always used -- so a chunk is in shape wherever such an
    // answer is expected.
    if let LocalResponse::ResultChunk { chunk } = response
        && let PayloadPosition::AnswerFrame { .. } = position
        && let Some(budget) = payload_answer_budget(expectation, limits)
    {
        let total = u64::try_from(chunk.bytes.len())
            .map_err(|_| invalid_channel_data())?
            .saturating_add(position.earlier_bytes());
        return validate_payload_limit_bytes(total, budget);
    }
    let valid_shape = matches!(
        (response, expectation),
        (
            LocalResponse::ResultChunk { .. },
            LocalResponseExpectation::OrdinaryResult
        ) | (
            LocalResponse::TerminalSuccess { .. },
            LocalResponseExpectation::OrdinaryResult
        ) | (
            LocalResponse::CursorOpened { .. },
            LocalResponseExpectation::CursorOpen
        ) | (
            LocalResponse::CursorPage { .. },
            LocalResponseExpectation::CursorFetch
        ) | (
            LocalResponse::CursorClosed { .. },
            LocalResponseExpectation::CursorClose
        ) | (
            LocalResponse::Metadata { .. },
            LocalResponseExpectation::Metadata(_)
        ) | (
            LocalResponse::Explain { .. },
            LocalResponseExpectation::Explain
        ) | (
            LocalResponse::OwnerStatus { .. },
            LocalResponseExpectation::OwnerStatus
        ) | (
            LocalResponse::Custom { .. },
            LocalResponseExpectation::Custom
        ) | (LocalResponse::Failure { .. }, _)
            | (LocalResponse::EngineFailure { .. }, _)
            | (LocalResponse::Progress { .. }, _)
            | (LocalResponse::CancelApplied { .. }, _)
    );
    if !valid_shape {
        return Err(invalid_channel_data());
    }

    match response {
        LocalResponse::ResultChunk { chunk } => {
            validate_payload_limit(chunk.bytes.len(), limits.result_bytes)?;
        }
        LocalResponse::TerminalSuccess { success } => {
            validate_payload_limit(success.final_bytes.len(), limits.result_bytes)?;
        }
        LocalResponse::CursorOpened { opened } => {
            validate_answer_payload_limit(
                opened.payload.len(),
                position,
                limits.cursor_page_bytes,
            )?;
            if position.is_whole() {
                decode_cursor_page(&opened.payload).map_err(|_| invalid_channel_data())?;
            }
        }
        LocalResponse::CursorPage { page } => {
            validate_answer_payload_limit(page.payload.len(), position, limits.cursor_page_bytes)?;
            if position.is_whole() {
                decode_cursor_page(&page.payload).map_err(|_| invalid_channel_data())?;
            }
        }
        LocalResponse::Metadata { metadata } => {
            validate_answer_payload_limit(metadata.payload.len(), position, limits.result_bytes)?;
            if let LocalResponseExpectation::Metadata(request) = expectation
                && position.is_whole()
            {
                match request {
                    LocalMetadataRequest::Tables { .. } => {
                        let page = decode_metadata_page(&metadata.payload)
                            .map_err(|_| invalid_channel_data())?;
                        if page.vocabulary != MetadataPageVocabulary::Tables {
                            return Err(invalid_channel_data());
                        }
                    }
                    LocalMetadataRequest::EventsStatus { .. } => {
                        let page = decode_metadata_page(&metadata.payload)
                            .map_err(|_| invalid_channel_data())?;
                        if page.vocabulary != MetadataPageVocabulary::EventsStatus {
                            return Err(invalid_channel_data());
                        }
                    }
                    LocalMetadataRequest::Schema { .. }
                    | LocalMetadataRequest::MaintenanceStatus => {}
                }
            }
        }
        LocalResponse::OwnerStatus { status } => validate_owner_status(status)?,
        LocalResponse::Explain { payload } => {
            validate_payload_limit(payload.len(), limits.result_bytes)?;
        }
        LocalResponse::Custom { payload } => {
            if payload.len() > MAX_FRAME_BYTES {
                return Err(invalid_channel_data());
            }
        }
        LocalResponse::CursorClosed { .. }
        | LocalResponse::Failure { .. }
        | LocalResponse::EngineFailure { .. }
        | LocalResponse::Progress { .. }
        | LocalResponse::CancelApplied { .. } => {}
    }
    Ok(())
}

fn validate_payload_limit(length: usize, limit: u64) -> Result<(), LocalTransportError> {
    let length = u64::try_from(length).map_err(|_| invalid_channel_data())?;
    validate_payload_limit_bytes(length, limit)
}

fn validate_payload_limit_bytes(length: u64, limit: u64) -> Result<(), LocalTransportError> {
    if length > limit {
        return Err(invalid_channel_data());
    }
    Ok(())
}

/// A payload-carrying answer is measured whole: the budget the session
/// declared governs the answer, not whichever piece of it this frame holds.
fn validate_answer_payload_limit(
    length: usize,
    position: PayloadPosition,
    limit: u64,
) -> Result<(), LocalTransportError> {
    let length = u64::try_from(length).map_err(|_| invalid_channel_data())?;
    validate_payload_limit_bytes(length.saturating_add(position.earlier_bytes()), limit)
}

fn validate_owner_status(status: &LocalOwnerStatusResponse) -> Result<(), LocalTransportError> {
    status
        .status
        .validate()
        .map_err(|_| invalid_channel_data())?;
    let limits = ReadLimits {
        result_rows: status.effective_limits.result_rows.value,
        result_bytes: status.effective_limits.result_bytes.value,
        work: status.effective_limits.work.value,
        active_ms: status.effective_limits.active_ms.value,
        memory: status.effective_limits.memory.value,
        cursor_page_rows: status.effective_limits.cursor_page_rows.value,
        cursor_page_bytes: status.effective_limits.cursor_page_bytes.value,
        cursor_idle_ms: status.effective_limits.cursor_idle_ms.value,
        cursor_lifetime_ms: status.effective_limits.cursor_lifetime_ms.value,
    };
    OwnerReadLimits {
        limits,
        concurrency: status.effective_limits.concurrency.value,
    }
    .validate()
    .map_err(|_| invalid_channel_data())?;
    OwnerServiceTimeouts {
        request_ms: status.timeouts.request_ms.value,
        shutdown_drain_ms: status.timeouts.shutdown_drain_ms.value,
    }
    .validate()
    .map_err(|_| invalid_channel_data())?;
    if status.admission.active_readers > status.admission.capacity
        || status.admission.capacity != status.effective_limits.concurrency.value
    {
        return Err(invalid_channel_data());
    }
    Ok(())
}
