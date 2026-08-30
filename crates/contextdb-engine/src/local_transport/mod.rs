//! Crate-private building blocks for the authenticated local owner channel.
//!
//! This tree deliberately has no database, durable coordination, or remote
//! carrier dependency. Route assembly owns opening a channel and executing a
//! request; this module owns only the local wire and operating-system seams.
//!
//! The frame vocabulary is reader-initiated throughout — the owner answers,
//! and never speaks first. The reader asks for rows, pages, metadata, an
//! explanation, owner status, or an application-defined exchange; and, because
//! the thread that asked is blocked awaiting its reply, it may also interrupt
//! the request already in flight on that connection ([`LocalRequest::CancelInFlight`]).
//! The owner answers with the shape the request selected, with a read refusal
//! ([`LocalResponse::Failure`]), or with the engine's own answer
//! ([`LocalResponse::EngineFailure`]) so a caller reading over the channel
//! learns exactly what an in-process call would have told it.
//!
//! That engine answer travels as this channel's OWN error document
//! ([`ReadChannelError`]), never as the engine's error enum. The document is
//! the boundary: every class a read can return is named there under a tag
//! written down in source, so engine error maintenance -- new variants,
//! reordered variants, reshaped fields -- never moves a protocol byte, and an
//! engine answer this channel has no tag for still arrives with its class name
//! and its words. [`LOCAL_PROTOCOL_VERSION`] moves only when the stable read
//! message envelope itself changes incompatibly after a release ships it;
//! adding a document tag is not such a change.

mod address;
mod authentication;
mod carrier;
mod deadlines;
mod engine_answer;
mod framing;
mod runtime;
mod stale;

#[cfg(unix)]
pub use address::ChannelKernelAddress;
pub use address::{channel_socket_path, derive_channel_address, opaque_channel_basename};
#[cfg(unix)]
pub use authentication::{
    AdmittedReader, authenticate_framed_stream_handshake, authenticate_stream_handshake,
};
pub use authentication::{ReadPrincipal, authenticate_peer, peer_user_from_stream};
#[cfg(unix)]
pub use carrier::{StaleRemovalInterlock, UnixLocalCarrier};
pub use deadlines::{
    DeadlineOperationWait, DeadlineStage, LocalDeadlineOperation, ManualDeadlineClock,
    MonotonicDeadlineClock, connect_with_deadline, drain_shutdown_with_deadline,
    expire_cursor_idle_with_deadline, expire_cursor_lifetime_with_deadline,
    handshake_with_deadline, probe_stale_with_deadline, receive_response_with_deadline,
    retry_route_with_deadline, serve_request_with_deadline, write_request_with_deadline,
};
pub use engine_answer::{
    ALL_TAGS, BodyField, ReadChannelError, TAG_ACL_DENIED, TAG_BFS_DEPTH_EXCEEDED,
    TAG_BFS_VISITED_EXCEEDED, TAG_COLUMN_NOT_FOUND, TAG_COLUMN_TYPE_MISMATCH,
    TAG_CONTEXT_SCOPE_VIOLATION, TAG_DATABASE_LOCKED, TAG_DISK_BUDGET_EXCEEDED,
    TAG_FULL_TEXT_SEARCH_NOT_SUPPORTED, TAG_INDEX_NOT_FOUND, TAG_LEGACY_VECTOR_STORE_DETECTED,
    TAG_MEMORY_BUDGET_EXCEEDED, TAG_NOT_FOUND, TAG_ORDER_BY_EXPRESSION_NOT_SUPPORTED, TAG_OTHER,
    TAG_OWNER_READ_DRAIN_TIMEOUT, TAG_PARSE_ERROR, TAG_PERSISTED_ROW_VECTOR_CELL_NULL,
    TAG_PERSISTED_ROW_VECTOR_ROW_MISSING, TAG_PLAN_ERROR, TAG_PRINCIPAL_REQUIRED,
    TAG_RANK_POLICY_COLUMN_AMBIGUOUS, TAG_RANK_POLICY_COLUMN_TYPE, TAG_RANK_POLICY_COLUMN_UNKNOWN,
    TAG_RANK_POLICY_FORMULA_PARSE, TAG_RANK_POLICY_JOIN_COLUMN_UNINDEXED,
    TAG_RANK_POLICY_JOIN_COLUMN_UNKNOWN, TAG_RANK_POLICY_JOIN_TABLE_UNKNOWN,
    TAG_RANK_POLICY_NOT_FOUND, TAG_READ_CANCELLED, TAG_READ_FAILURE,
    TAG_READ_SESSION_NOT_IMPLEMENTED, TAG_RECURSIVE_CTE_NOT_SUPPORTED, TAG_SCHEMA_INVALID,
    TAG_SCOPE_LABEL_VIOLATION, TAG_STORE_CORRUPTED, TAG_STORE_IDENTITY_UNPROVABLE,
    TAG_STORED_PROC_NOT_SUPPORTED, TAG_SUBQUERY_NOT_SUPPORTED, TAG_TABLE_NOT_FOUND,
    TAG_UNBOUNDED_TRAVERSAL, TAG_UNBOUNDED_VECTOR_SEARCH, TAG_UNKNOWN, TAG_UNKNOWN_VECTOR_INDEX,
    TAG_USE_RANK_REQUIRES_LIMIT, TAG_USE_RANK_REQUIRES_VECTOR_ORDER,
    TAG_VECTOR_INDEX_DIMENSION_MISMATCH, TAG_WINDOW_FUNCTION_NOT_SUPPORTED, body_grammar,
};
pub use framing::{
    AssembledOrdinaryResult, CursorCloseAcknowledgement, CursorOpenedResponse, CursorPageResponse,
    FrameBufferAllocator, FrameReader, FrameViolation, LocalConfigurationSource,
    LocalConfiguredValue, LocalEffectiveLimits, LocalEngineFailure, LocalHandshake,
    LocalInboundKind, LocalInboundMessage, LocalMetadataRequest, LocalOutboundMessage,
    LocalOwnerStatusResponse, LocalOwnerTimeouts, LocalProtocolBoundary, LocalReadDeclaration,
    LocalRequest, LocalRequestEnvelope, LocalResponse, LocalResponseExpectation, MetadataResponse,
    OrdinaryResultReceiver, OwnerAdmissionCounters, OwnerMemoryCounters, PayloadViolation,
    ResultChunk, ResultReceiveOutcome, ResultReceiveState, TerminalSuccess, decode_frame_exact,
    decode_handshake_exact, decode_message_exact, encode_message, encode_payload_frame,
    frame_length_prefix, read_payload_with_admission, receive_framed_ordinary_result,
    split_canonical_result, split_payload_answer, validate_handshake,
};
pub(crate) use framing::{
    preflight_canonical_query_result, preflight_cursor_page_payload,
    preflight_metadata_page_payload, response_is_encodable,
};
pub use runtime::RuntimeDirectory;
#[cfg(target_os = "linux")]
pub use runtime::linux_filesystem_type_is_local;
pub(crate) use runtime::runtime_directory_for_store;
pub use runtime::{
    ChannelPathFacts, ChannelPathViolation, LocalPlatformAvailability, OWNER_ONLY_MODE,
    ProcessRuntimeDirectoryEnvironment, ResolvedRuntimeDirectory, RuntimeDirectoryEnvironment,
    RuntimeDirectoryFacts, RuntimeDirectoryInspector, RuntimeDirectoryRequest,
    RuntimeDirectorySource, RuntimeRootViolation, SystemRuntimeDirectoryInspector,
    inspect_channel_path, local_platform_availability, prepare_runtime_directory,
    prepare_runtime_directory_with_environment, resolve_runtime_directory,
    resolve_runtime_directory_with_environment, unix_socket_path_limit,
    validate_channel_addressability, validate_channel_path, validate_runtime_root,
    validate_socket_path_length,
};
pub use stale::{
    ChannelFilesystemIdentity, FinalStaleIdentityObservation, LivenessEvidence, StaleChannelAction,
    StaleChannelEvidence, StaleFilesystemEvidence, StaleIdentityEvidence,
    decide_stale_channel_action,
};
#[cfg(unix)]
pub use stale::{
    StaleChannelProbe, channel_filesystem_identity, reconcile_stale_channel,
    remove_own_bound_channel,
};

use contextdb_core::read_contract::{
    OwnerLimitExceededDetail, ReadFailure, ReadFailureDetail, ReadFailureKind, ReadFailureLimit,
};

/// The fixed local-channel protocol marker.
pub const LOCAL_PROTOCOL_MARKER: [u8; 20] = *b"contextdb-local-read";
/// The shape of every frame named in this module, taken together.
///
/// This value NAMES a shape, so it moves whenever the shape moves. Version 2
/// is the handshake carrying the visibility a reader declares for its session;
/// version 1 was the same vocabulary without it. Two builds whose handshakes
/// encode differently must never share a version name: the fence is an exact
/// equality check, so a peer from the other build is answered the typed
/// protocol-mismatch refusal it can read, instead of dying inside a decoder on
/// bytes that are not the shape it expected.
///
/// The golden byte fixtures are what make such a change impossible to miss.
pub const LOCAL_PROTOCOL_VERSION: u16 = 2;
/// A complete local payload may be no larger than four mebibytes.
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocalTransportError {
    #[error("local transport is not implemented")]
    Unimplemented,
    #[error("peer credentials are unavailable")]
    CredentialsUnavailable,
    #[error("filesystem inspection failed: {0}")]
    FilesystemInspection(String),
    #[error("local channels are unavailable on this platform")]
    PlatformUnsupported,
    #[error("runtime directory is invalid: {0:?}")]
    RuntimeRoot(RuntimeRootViolation),
    #[error("local channel path is invalid: {0:?}")]
    ChannelPath(ChannelPathViolation),
    #[error("local frame is invalid: {0:?}")]
    Frame(framing::FrameViolation),
    #[error("local payload is invalid: {0:?}")]
    Payload(framing::PayloadViolation),
    #[error("a stale channel path cannot be verified")]
    StaleChannelUnverifiable,
    #[error("local transport refusal: {0:?}")]
    Refusal(ReadFailure),
}

impl LocalTransportError {
    #[allow(dead_code)]
    pub(crate) const fn unimplemented() -> Self {
        Self::Unimplemented
    }
}

pub(crate) fn refusal(kind: ReadFailureKind) -> LocalTransportError {
    LocalTransportError::Refusal(
        ReadFailure::new(kind, ReadFailureDetail::None)
            .expect("the local transport only constructs empty-detail fixed refusals"),
    )
}

pub(crate) fn invalid_channel_data() -> LocalTransportError {
    refusal(ReadFailureKind::InvalidChannelData)
}

/// The budget document a caller receives when the memory ceiling in force for
/// their exchange is what stopped a decode. The shipped owner-limit refusal
/// already says "a read went past a ceiling it declared", so no new refusal
/// kind is invented here: this one names MEMORY and the number that was in
/// force, which is what a caller has to know to raise it.
pub(crate) fn memory_ceiling_read_failure(ceiling: u64) -> ReadFailure {
    ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
        limit: ReadFailureLimit::Memory,
        value: ceiling,
        required: None,
        statement: None,
    })
}

pub(crate) fn owner_disconnected() -> LocalTransportError {
    refusal(ReadFailureKind::OwnerDisconnected)
}

/// Whether this is the answer that says the owner is no longer there.
pub(crate) fn is_owner_disconnected(error: &LocalTransportError) -> bool {
    matches!(
        error,
        LocalTransportError::Refusal(failure)
            if failure.kind() == ReadFailureKind::OwnerDisconnected
    )
}

pub(crate) fn owner_timeout() -> LocalTransportError {
    refusal(ReadFailureKind::OwnerTimeout)
}

pub(crate) fn operation_already_completed() -> LocalTransportError {
    refusal(ReadFailureKind::OperationAlreadyCompleted)
}

/// What a failed frame read means about the peer. A frame that ends part-way
/// through is a peer that went away, which is the refusal the no-fallback rule
/// is decided on; a frame whose declared shape is impossible is content this
/// connection could not read. The two answers lead a caller to different
/// responses, so they are never collapsed into one.
pub(crate) fn frame_read_refusal(error: LocalTransportError) -> LocalTransportError {
    match error {
        LocalTransportError::Frame(
            framing::FrameViolation::TruncatedLength | framing::FrameViolation::TruncatedPayload,
        ) => owner_disconnected(),
        LocalTransportError::Frame(
            framing::FrameViolation::LengthExceedsMaximum | framing::FrameViolation::TrailingBytes,
        ) => invalid_channel_data(),
        other => other,
    }
}
