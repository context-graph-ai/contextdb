//! Transport seam: the interface between the sync logic and the thing that
//! moves bytes. Concrete transports live in submodules; the sync logic names
//! only these types.

#[cfg(any(test, feature = "in-process-test-seams", feature = "test-seams"))]
pub mod in_process;
#[cfg(feature = "iroh")]
pub mod iroh;
#[cfg(feature = "iroh")]
pub(crate) mod iroh_blobs_adapter;
#[cfg(feature = "iroh")]
pub(crate) use iroh_blobs_adapter as adapter;
#[cfg(feature = "iroh")]
mod large_request_staging;
#[cfg(feature = "iroh")]
pub(crate) use large_request_staging::{AssemblyCredit, LargeRequestAdmission};
#[cfg(test)]
mod native_buffer_transfer_tests;
mod native_wire;
#[doc(hidden)]
pub use native_wire::ReadBoundStreamInstallation;
#[cfg(test)]
pub(crate) use native_wire::native_delivery_error;
#[doc(hidden)]
pub use native_wire::{AdmittedWireFuture, DispatchedWireFuture};
pub(crate) use native_wire::{
    NativeStreamResponder, admit_wire_future, install_native_delivery_route,
};

// Transport-neutral surface for embedding consumers (e.g. a downstream fabric
// runtime): a peer-endpoint alias plus scheme-free spec builders, so an
// embedding consumer never spells the adapter module name or the `iroh:?`
// URI scheme literal itself. Established HERE, a file physically under the
// adapter boundary, per the same pattern as the `adapter` alias above;
// crate-root wiring in lib.rs re-exports these for consumers.
#[cfg(feature = "iroh")]
pub use iroh::EndpointSpec as PeerEndpointSpec;
#[cfg(feature = "iroh")]
pub use iroh::IrohServer as PeerEndpoint;
#[cfg(feature = "iroh")]
pub use iroh::{ServerResourcePolicy, bind_spec as peer_bind_spec, dial_spec as peer_dial_spec};

/// Gate endpoint-owned integration on the transport implementation without
/// exposing its feature selection to server-facing modules.
macro_rules! peer_endpoint_available {
    ($item:item) => {
        #[cfg(feature = "iroh")]
        $item
    };
}
pub(crate) use peer_endpoint_available;

#[cfg(feature = "production-smoke-driver")]
pub(crate) fn production_smoke_completed_apply_before_reply(
    request_digest: [u8; blake3::OUT_LEN],
    authenticated_node_id: String,
    source_lsn: u64,
    hub_lsn: u64,
    dependency_complete: bool,
) {
    iroh::production_smoke_completed_apply_before_reply(
        request_digest,
        authenticated_node_id,
        source_lsn,
        hub_lsn,
        dependency_complete,
    );
}

use contextdb_core::read_memory::ReadMemoryOwner;
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// Transport-neutral failure surface. Each adapter maps its own errors into
/// these variants; the sync logic never names a concrete transport.
#[derive(Debug)]
pub enum TransportError {
    /// No server is listening on the channel.
    NoResponder,
    /// The request deadline elapsed with no reply.
    Timeout,
    /// The transport returned a status/control reply instead of bytes.
    Status(String),
    /// The endpoint could not be reached; the string stays operator-actionable
    /// (e.g. it carries the address).
    Unreachable(String),
    /// A fragmented reply started arriving but did not complete.
    IncompleteReply(String),
    /// A complete oversized request was dispatched but its reply was lost.
    IndeterminateComplete(String),
    /// The adapter would have to use a retry-unsafe send path. This must be
    /// returned before any bytes are sent.
    RetryUnsafe(String),
    /// Any other transport-level failure.
    Other(String),
    /// The receiver refused the request before handling it, with a typed
    /// reason (for example, its budget cannot fund the request's assembly).
    /// Nothing was applied, so the sender may retry the same request.
    Refused(contextdb_core::Error),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::NoResponder => write!(f, "no responder on channel"),
            TransportError::Timeout => write!(f, "transport request timed out"),
            TransportError::Status(detail) => write!(f, "transport status reply: {detail}"),
            TransportError::Unreachable(detail) => write!(f, "transport unreachable: {detail}"),
            TransportError::IncompleteReply(detail) => write!(f, "incomplete reply: {detail}"),
            TransportError::IndeterminateComplete(detail) => {
                write!(f, "completed request reply lost: {detail}")
            }
            TransportError::RetryUnsafe(detail) => write!(f, "retry-unsafe request: {detail}"),
            TransportError::Other(detail) => write!(f, "transport error: {detail}"),
            TransportError::Refused(error) => write!(f, "request refused by receiver: {error}"),
        }
    }
}

impl std::error::Error for TransportError {}

pub type TransportResult<T> = Result<T, TransportError>;
pub type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = TransportResult<T>> + Send + 'a>>;
/// Resource failures retain their typed cause until the sync layer maps them.
#[doc(hidden)]
#[derive(Debug)]
pub enum WireError {
    Resource(contextdb_core::Error),
    Transport(TransportError),
    /// A native route's terminal, carried without any owned diagnostic until
    /// the public handoff converts it.
    Native(NativeTerminal),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resource(error) => fmt::Display::fmt(error, f),
            Self::Transport(error) => fmt::Display::fmt(error, f),
            Self::Native(terminal) => fmt::Display::fmt(terminal, f),
        }
    }
}
impl std::error::Error for WireError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Resource(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Native(_) => None,
        }
    }
}
impl From<NativeTerminal> for WireError {
    fn from(terminal: NativeTerminal) -> Self {
        Self::Native(terminal)
    }
}

/// Stable identifiers for native terminals that carry no typed refusal. Each
/// names one class, so a client can tell them apart; the formatted detail goes
/// to diagnostics only.
pub(crate) mod terminal_class {
    /// A native allocation or layout check failed (`NativeRefusal::Other`).
    pub(crate) const ALLOCATION: &str = "sync.native.allocation";
    /// The authenticated request failed native validation.
    pub(crate) const INVALID_REQUEST: &str = "sync.native.invalid_request";
    /// A storage or kernel fault without a typed refusal.
    pub(crate) const FAULT: &str = "sync.native.fault";
    /// A request body, responder or operation belonged to another reader.
    pub(crate) const OWNER_MISMATCH: &str = "sync.native.owner_mismatch";
    /// A received reply could not be decoded.
    pub(crate) const DECODE_CODEC: &str = "sync.decode.codec";
    /// A request could not be encoded into its admitted storage.
    pub(crate) const ENCODE: &str = "sync.native.encode";
    /// The reply stream failed before the answer was written.
    pub(crate) const CONNECTION_LOST: &str = "sync.transport.connection_lost";
    /// A reply broke the transport protocol.
    pub(crate) const INVALID_REPLY: &str = "sync.transport.invalid_reply";
}

/// The terminal of a natively routed request that ended without a success
/// body. Every variant is built without an owned allocation by the native
/// route itself: refusals carry `&'static str` fields, kernel values are moved
/// from their producer, never cloned, and classes are static identifiers.
#[doc(hidden)]
#[derive(Debug)]
pub enum NativeTerminal {
    /// A native admission refused.
    Refused(contextdb_core::read_memory::NativeRefusal),
    /// A typed refusal produced by the read kernel (`MemoryBudgetExceeded`,
    /// `ReadFailure` or `ReadCancelled`), moved from its producer.
    Kernel(contextdb_core::Error),
    /// The request's cancellation token fired.
    Cancelled,
    /// A failure with no typed refusal: one of `terminal_class`.
    Class(&'static str),
}

impl fmt::Display for NativeTerminal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(refusal) => fmt::Display::fmt(refusal, f),
            Self::Kernel(error) => fmt::Display::fmt(error, f),
            Self::Cancelled => f.write_str("read cancelled"),
            Self::Class(class) => f.write_str(class),
        }
    }
}

impl NativeTerminal {
    /// Classify a kernel error: its typed refusals keep their kind (moved),
    /// anything else is the static fault class.
    pub(crate) fn from_kernel(error: contextdb_core::Error) -> Self {
        match error {
            contextdb_core::Error::ReadCancelled => Self::Cancelled,
            error @ (contextdb_core::Error::MemoryBudgetExceeded { .. }
            | contextdb_core::Error::ReadFailure(_)) => Self::Kernel(error),
            error => {
                tracing::debug!(error = %error, "native delivery fault");
                Self::Class(terminal_class::FAULT)
            }
        }
    }

    /// The public error at the caller handoff. This is where its strings are
    /// first allocated.
    pub(crate) fn into_error(self) -> contextdb_core::Error {
        use contextdb_core::read_memory::NativeRefusal;
        match self {
            Self::Refused(NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            }) => contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint: hint.into(),
            },
            Self::Refused(NativeRefusal::ProcessMemoryBudgetExceeded {
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            }) => contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "process_native_ceiling".into(),
                operation: operation.into(),
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint: hint.into(),
            },
            Self::Refused(NativeRefusal::NativeUnsupported {
                subsystem,
                operation,
            }) => contextdb_core::Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::owner_route_unsupported(
                    contextdb_core::read_memory::native_unsupported_inspection(
                        subsystem, operation,
                    ),
                ),
            ),
            Self::Refused(NativeRefusal::Other(_)) => {
                contextdb_core::Error::SyncError(terminal_class::ALLOCATION.into())
            }
            Self::Kernel(error) => error,
            Self::Cancelled => contextdb_core::Error::ReadCancelled,
            Self::Class(class) => contextdb_core::Error::SyncError(class.into()),
        }
    }
}

/// The transport's frame ceiling: no single reply frame is longer
/// (a transport constant, 64 MiB).
pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// The longest subject a native route can serve (BAKED-IN constant, approved
/// as the inline bound). A longer subject is read into the Legacy route map,
/// and a hub whose delivery subject is longer serves it Legacy.
pub(crate) const MAX_NATIVE_SUBJECT_BYTES: usize = 256;

/// The longest `&'static str` any native refusal producer can supply. The
/// source audit `native_refusal_strings_fit_the_terminal_bound` holds every
/// literal argument of a native admission and every `&str` const in the
/// producing crates to this bound.
pub(crate) const MAX_TERMINAL_STR: usize = 256;
/// Every typed native terminal encodes within this many bytes: a fixed
/// overhead for the map keys, variant names and integers, plus three strings
/// of at most `MAX_TERMINAL_STR` (an owner-route inspection joins two of them).
pub(crate) const TERMINAL_BYTES: usize = 256 + 3 * MAX_TERMINAL_STR;

/// Borrowed mirror of `WireRefusal`: the same variant and field names, so its
/// named MessagePack encoding is byte-identical, with no owned field.
#[derive(serde::Serialize)]
#[serde(rename = "WireRefusal")]
enum WireRefusalRef<'a> {
    MemoryBudgetExceeded {
        subsystem: &'a str,
        operation: &'a str,
        requested_bytes: usize,
        available_bytes: usize,
        budget_limit_bytes: usize,
        hint: &'a str,
    },
    ReadFailure(ReadFailureRef<'a>),
    ReadCancelled,
}

/// A `ReadFailure` borrowed from its producer, or the owner-route refusal
/// built from two static parts without joining them on the heap.
enum ReadFailureRef<'a> {
    Borrowed(&'a contextdb_core::read_contract::ReadFailure),
    RouteUnsupported {
        subsystem: &'a str,
        operation: &'a str,
    },
}

impl serde::Serialize for ReadFailureRef<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        match self {
            Self::Borrowed(failure) => failure.serialize(serializer),
            Self::RouteUnsupported {
                subsystem,
                operation,
            } => {
                struct Detail<'a>(&'a str, &'a str);
                impl serde::Serialize for Detail<'_> {
                    fn serialize<S: serde::Serializer>(
                        &self,
                        serializer: S,
                    ) -> Result<S::Ok, S::Error> {
                        // `ReadFailureDetail::OwnerRouteUnsupported(OwnerRouteUnsupportedDetail
                        // { inspection })`, with the inspection joined in a stack buffer.
                        struct Inspection<'a>(&'a str, &'a str);
                        impl serde::Serialize for Inspection<'_> {
                            fn serialize<S: serde::Serializer>(
                                &self,
                                serializer: S,
                            ) -> Result<S::Ok, S::Error> {
                                let mut joined = [0u8; 2 * MAX_TERMINAL_STR + 1];
                                let length = self.0.len() + 1 + self.1.len();
                                if length > joined.len() {
                                    return Err(serde::ser::Error::custom(""));
                                }
                                joined[..self.0.len()].copy_from_slice(self.0.as_bytes());
                                joined[self.0.len()] = b'/';
                                joined[self.0.len() + 1..length].copy_from_slice(self.1.as_bytes());
                                let text = std::str::from_utf8(&joined[..length])
                                    .map_err(|_| serde::ser::Error::custom(""))?;
                                serializer.serialize_str(text)
                            }
                        }
                        struct Body<'a>(&'a str, &'a str);
                        impl serde::Serialize for Body<'_> {
                            fn serialize<S: serde::Serializer>(
                                &self,
                                serializer: S,
                            ) -> Result<S::Ok, S::Error> {
                                let mut body = serializer
                                    .serialize_struct("OwnerRouteUnsupportedDetail", 1)?;
                                body.serialize_field("inspection", &Inspection(self.0, self.1))?;
                                body.end()
                            }
                        }
                        // Positional, as in `ReadFailureDetail`'s declaration order;
                        // the byte-equality test pins it against the owned encoding.
                        serializer.serialize_newtype_variant(
                            "ReadFailureDetail",
                            6,
                            "OwnerRouteUnsupported",
                            &Body(self.0, self.1),
                        )
                    }
                }
                let mut failure = serializer.serialize_struct("ReadFailure", 2)?;
                failure.serialize_field(
                    "kind",
                    &contextdb_core::read_contract::ReadFailureKind::OwnerRouteUnsupported,
                )?;
                failure.serialize_field("detail", &Detail(subsystem, operation))?;
                failure.end()
            }
        }
    }
}

/// Encode a native terminal's typed refusal into `out` without any heap
/// allocation, byte-identical to `WireRefusal::encode` of the same refusal.
/// `None` means the terminal has no typed refusal (a static class).
pub(crate) fn encode_terminal(
    terminal: &NativeTerminal,
    out: &mut [u8; TERMINAL_BYTES],
) -> Option<usize> {
    use contextdb_core::read_memory::NativeRefusal;
    let refusal = match terminal {
        NativeTerminal::Refused(NativeRefusal::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        }) => WireRefusalRef::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes: *requested_bytes,
            available_bytes: *available_bytes,
            budget_limit_bytes: *budget_limit_bytes,
            hint,
        },
        NativeTerminal::Refused(NativeRefusal::ProcessMemoryBudgetExceeded {
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        }) => WireRefusalRef::MemoryBudgetExceeded {
            subsystem: "process_native_ceiling",
            operation,
            requested_bytes: *requested_bytes,
            available_bytes: *available_bytes,
            budget_limit_bytes: *budget_limit_bytes,
            hint,
        },
        NativeTerminal::Refused(NativeRefusal::NativeUnsupported {
            subsystem,
            operation,
        }) => WireRefusalRef::ReadFailure(ReadFailureRef::RouteUnsupported {
            subsystem,
            operation,
        }),
        NativeTerminal::Kernel(contextdb_core::Error::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        }) => WireRefusalRef::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes: *requested_bytes,
            available_bytes: *available_bytes,
            budget_limit_bytes: *budget_limit_bytes,
            hint,
        },
        NativeTerminal::Kernel(contextdb_core::Error::ReadFailure(failure)) => {
            WireRefusalRef::ReadFailure(ReadFailureRef::Borrowed(failure))
        }
        NativeTerminal::Cancelled => WireRefusalRef::ReadCancelled,
        NativeTerminal::Refused(NativeRefusal::Other(_))
        | NativeTerminal::Kernel(_)
        | NativeTerminal::Class(_) => return None,
    };
    let capacity = out.len();
    let mut cursor = std::io::Cursor::new(&mut out[..]);
    serde::Serialize::serialize(
        &refusal,
        &mut rmp_serde::Serializer::new(&mut cursor).with_struct_map(),
    )
    .ok()?;
    let written = usize::try_from(cursor.position()).ok()?;
    (written <= capacity).then_some(written)
}

/// The measuring callback of [`measure_native_terminal_encodings_for_test`]:
/// given a terminal's name and its encoder, it runs the encoder once.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub type TerminalEncodingMeasure<'a> =
    dyn FnMut(&'static str, &mut dyn FnMut() -> Option<usize>) + 'a;

/// Test seam: builds each typed native terminal reachable on the route (outside
/// `measure`), then encodes it inside `measure`, so a counting allocator can
/// observe the encoder alone.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn measure_native_terminal_encodings_for_test(measure: &mut TerminalEncodingMeasure<'_>) {
    use contextdb_core::read_contract::{OwnerLimitExceededDetail, ReadFailure, ReadFailureLimit};
    use contextdb_core::read_memory::NativeRefusal;
    let terminals = [
        (
            "memory",
            NativeTerminal::Refused(NativeRefusal::MemoryBudgetExceeded {
                subsystem: "sync_transport",
                operation: "materialize_body",
                requested_bytes: usize::MAX,
                available_bytes: 0,
                budget_limit_bytes: usize::MAX,
                hint: "legacy transport requires an admitted complete body",
            }),
        ),
        (
            "process",
            NativeTerminal::Refused(NativeRefusal::ProcessMemoryBudgetExceeded {
                operation: "materialize_body",
                requested_bytes: usize::MAX,
                available_bytes: 0,
                budget_limit_bytes: usize::MAX,
                hint: "raise the process ceiling",
            }),
        ),
        (
            "owner_route",
            NativeTerminal::Refused(NativeRefusal::NativeUnsupported {
                subsystem: "custody",
                operation: "durable_group_index",
            }),
        ),
        (
            "owner_limit",
            NativeTerminal::Kernel(contextdb_core::Error::ReadFailure(
                ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
                    limit: ReadFailureLimit::CursorPageBytes,
                    value: u64::MAX,
                    required: None,
                    statement: None,
                }),
            )),
        ),
        ("cancelled", NativeTerminal::Cancelled),
    ];
    for (name, terminal) in &terminals {
        let mut encoded = [0u8; TERMINAL_BYTES];
        measure(name, &mut || encode_terminal(terminal, &mut encoded));
    }
}

/// The static class a native terminal answers when it has no typed refusal.
pub(crate) fn terminal_class_of(terminal: &NativeTerminal) -> &'static str {
    use contextdb_core::read_memory::NativeRefusal;
    match terminal {
        NativeTerminal::Class(class) => class,
        NativeTerminal::Refused(NativeRefusal::Other(_)) => terminal_class::ALLOCATION,
        _ => terminal_class::FAULT,
    }
}
impl From<contextdb_core::Error> for WireError {
    fn from(error: contextdb_core::Error) -> Self {
        Self::Resource(error)
    }
}
impl From<TransportError> for WireError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

#[doc(hidden)]
pub type WireResult<T> = Result<T, WireError>;
#[doc(hidden)]
pub type WireFuture<'a, T> = Pin<Box<dyn Future<Output = WireResult<T>> + Send + 'a>>;

enum WireBodyStorage {
    Legacy(AdmittedWireVec),
}

/// A fixed-length request body. A cursor advancing is never a funding refund.
#[doc(hidden)]
pub struct BoundedWireBody {
    storage: WireBodyStorage,
    length: u64,
    position: u64,
    reader: ReadMemoryOwner,
}

impl BoundedWireBody {
    pub(crate) fn owner(&self) -> ReadMemoryOwner {
        self.reader.clone()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.length
    }

    pub fn read<'a>(&'a mut self, dst: &'a mut [u8]) -> WireFuture<'a, usize> {
        Box::pin(async move {
            let remaining = self.length.checked_sub(self.position).ok_or_else(|| {
                WireError::Transport(TransportError::IncompleteReply(
                    "body cursor exceeds its declared length".to_owned(),
                ))
            })?;
            let limit = remaining.min(dst.len() as u64) as usize;
            if limit == 0 {
                return Ok(0);
            }
            let count = match &self.storage {
                WireBodyStorage::Legacy(bytes) => {
                    let start = self.position as usize;
                    dst[..limit].copy_from_slice(&bytes.bytes[start..start + limit]);
                    limit
                }
            };
            if count == 0 || count > limit {
                return Err(TransportError::IncompleteReply(
                    "body source did not honor its declared length".to_owned(),
                )
                .into());
            }
            self.position += count as u64;
            Ok(count)
        })
    }
}

#[doc(hidden)]
pub struct BoundedWireReply {
    body: BoundedWireBody,
}
impl BoundedWireReply {
    pub(crate) fn legacy(body: BoundedWireBody) -> Self {
        Self { body }
    }
    pub fn body_mut(&mut self) -> &mut BoundedWireBody {
        &mut self.body
    }
    /// The whole reply when it is one unconsumed admitted buffer.
    pub(crate) fn contiguous_bytes(&self) -> Option<&[u8]> {
        self.body.native_contiguous_bytes()
    }
}

#[doc(hidden)]
pub struct IncomingStreamRequest {
    pub body: BoundedWireBody,
    pub responder: StreamResponder,
    pub node_id: Option<String>,
}
#[doc(hidden)]
pub type StreamResponder =
    Box<dyn FnOnce(BoundedWireBody) -> WireFuture<'static, ()> + Send + 'static>;
#[doc(hidden)]
pub type StreamRequestHandler =
    Arc<dyn Fn(IncomingStreamRequest) -> WireFuture<'static, ()> + Send + Sync + 'static>;
#[doc(hidden)]
pub struct StreamHandlerRegistration {
    pub subject: String,
    pub handler: StreamRequestHandler,
}

/// A bound request uses a fallible finish boundary without altering the
/// public legacy request/responder interface.
pub(crate) struct ReadBoundIncomingRequest {
    pub(crate) body: BoundedWireBody,
    pub(crate) responder: ReadBoundResponder,
    pub(crate) node_id: Option<String>,
}

pub(crate) enum ReadBoundResponder {
    Legacy(StreamResponder),
    Native(NativeStreamResponder),
}
impl ReadBoundResponder {
    pub(crate) fn try_finish(
        self,
        body: BoundedWireBody,
    ) -> WireResult<DispatchedWireFuture<'static, ()>> {
        match self {
            Self::Legacy(finish) => Ok(DispatchedWireFuture::legacy(finish(body))),
            Self::Native(finish) => finish.try_finish(body).map(DispatchedWireFuture::native),
        }
    }
}

/// Test-only release event: a serving transport signals it after the last
/// request-owned allocation of one native request is gone. Waiters re-check
/// their own byte count on every signal; no time-based polling is involved.
#[cfg(test)]
pub(crate) mod release_events {
    static RELEASED: std::sync::Mutex<u64> = std::sync::Mutex::new(0);
    static SIGNAL: std::sync::Condvar = std::sync::Condvar::new();

    pub(crate) fn signal() {
        *RELEASED.lock().unwrap_or_else(|p| p.into_inner()) += 1;
        SIGNAL.notify_all();
    }

    /// Signals one release event when dropped: held first in a task's frame,
    /// it fires after everything the task held, including when the task is
    /// aborted mid-phase.
    pub(crate) struct SignalOnDrop;
    impl Drop for SignalOnDrop {
        fn drop(&mut self) {
            signal();
        }
    }

    /// Wait until `done()` holds, re-checking after each release event. The
    /// long wait bound only turns a lost event into a failing test.
    pub(crate) fn wait_until(mut done: impl FnMut() -> bool) -> bool {
        let mut seen = RELEASED.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if done() {
                return true;
            }
            let observed = *seen;
            let (guard, timeout) = SIGNAL
                .wait_timeout_while(seen, std::time::Duration::from_secs(120), |count| {
                    *count == observed
                })
                .unwrap_or_else(|p| p.into_inner());
            seen = guard;
            if timeout.timed_out() {
                return done();
            }
        }
    }
}

/// Test-only pause points on the native route. An armed point holds the one
/// request that reaches it there, until the test releases it or the task
/// holding it is dropped; every other request passes through.
#[cfg(test)]
pub(crate) mod native_pause {
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Point {
        /// Hub: the subject and body are admitted, before the body is read.
        AfterAdmission,
        /// Hub: selection has built the reply page, before it is written.
        AfterSelection,
        /// Hub: the native success reply is written, before its receipt is read.
        BeforeReceipt,
        /// Hub: a refused or failed request's terminal is about to be written.
        BeforeTerminal,
    }

    #[derive(Debug, Default)]
    pub(crate) struct Gate {
        reached: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl Gate {
        /// Resolves once a request is held at the point (or already was).
        pub(crate) async fn reached(&self) {
            self.reached.notified().await;
        }
        /// Lets the held request continue.
        pub(crate) fn release(&self) {
            self.release.notify_one();
        }
    }

    static ARMED: Mutex<Option<(Point, Arc<Gate>)>> = Mutex::new(None);

    /// Arm `point` for the next request that reaches it.
    pub(crate) fn arm(point: Point) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *ARMED.lock().unwrap_or_else(|p| p.into_inner()) = Some((point, gate.clone()));
        gate
    }

    pub(crate) async fn at(point: Point) {
        let gate = {
            let mut armed = ARMED.lock().unwrap_or_else(|p| p.into_inner());
            match armed.as_ref() {
                Some((armed_point, _)) if *armed_point == point => {
                    armed.take().map(|(_, gate)| gate)
                }
                _ => None,
            }
        };
        if let Some(gate) = gate {
            gate.reached.notify_one();
            gate.release.notified().await;
        }
    }
}

/// The typed terminal of a natively routed request that ended without a
/// success body: memory refusal, owner-limit refusal or cancellation. It
/// crosses the wire as its own reply kind so the client sees the same
/// `contextdb_core::Error` variant the hub produced, never status text.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum WireRefusal {
    MemoryBudgetExceeded {
        subsystem: String,
        operation: String,
        requested_bytes: usize,
        available_bytes: usize,
        budget_limit_bytes: usize,
        hint: String,
    },
    ReadFailure(contextdb_core::read_contract::ReadFailure),
    ReadCancelled,
}
impl WireRefusal {
    pub(crate) fn from_error(error: &contextdb_core::Error) -> Option<Self> {
        use contextdb_core::Error;
        match error {
            Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => Some(Self::MemoryBudgetExceeded {
                subsystem: subsystem.to_string(),
                operation: operation.to_string(),
                requested_bytes: *requested_bytes,
                available_bytes: *available_bytes,
                budget_limit_bytes: *budget_limit_bytes,
                hint: hint.to_string(),
            }),
            Error::ReadFailure(failure) => Some(Self::ReadFailure(failure.clone())),
            Error::ReadCancelled => Some(Self::ReadCancelled),
            _ => None,
        }
    }
    pub(crate) fn into_error(self) -> contextdb_core::Error {
        use contextdb_core::Error;
        match self {
            Self::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint: hint.into(),
            },
            Self::ReadFailure(failure) => Error::ReadFailure(failure),
            Self::ReadCancelled => Error::ReadCancelled,
        }
    }
    pub(crate) fn encode(&self) -> Option<Vec<u8>> {
        rmp_serde::to_vec_named(self).ok()
    }
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        rmp_serde::from_slice(bytes).ok()
    }
}

/// Admit `bytes` on `owner` before the allocation it pays for. A native owner
/// refuses with a heap-free `NativeRefusal` (carried as a native terminal); a
/// Legacy owner keeps its ordinary refusal.
pub(crate) fn admit_on(
    owner: &ReadMemoryOwner,
    bytes: usize,
    operation: &'static str,
    hint: &'static str,
) -> WireResult<contextdb_core::read_memory::ReadCredit> {
    use contextdb_core::read_memory::ReadCredit;
    if owner.is_native() {
        ReadCredit::try_new_owned_native(owner.clone(), bytes, "sync_transport", operation, hint)
            .map_err(|refusal| NativeTerminal::Refused(refusal).into())
    } else {
        ReadCredit::try_new_owned(owner.clone(), bytes, "sync_transport", operation, hint)
            .map_err(WireError::Resource)
    }
}

/// An allocation that failed after its admission: heap-free on a native
/// owner, ordinary on a Legacy one.
pub(crate) fn allocation_failed(owner: &ReadMemoryOwner, reason: &'static str) -> WireError {
    if owner.is_native() {
        NativeTerminal::Refused(contextdb_core::read_memory::NativeRefusal::Other(reason)).into()
    } else {
        WireError::Resource(contextdb_core::Error::Other(reason.into()))
    }
}

/// Encode `value` into storage admitted on `owner` before it is allocated: a
/// counting pass sizes it exactly, then it is written into that storage.
pub(crate) fn encode_admitted<T: serde::Serialize>(
    owner: &ReadMemoryOwner,
    value: &T,
    named: bool,
) -> WireResult<AdmittedWireVec> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or(std::io::ErrorKind::InvalidData)?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn write<T: serde::Serialize, W: std::io::Write>(value: &T, writer: W, named: bool) -> bool {
        if named {
            value
                .serialize(&mut rmp_serde::Serializer::new(writer).with_struct_map())
                .is_ok()
        } else {
            value
                .serialize(&mut rmp_serde::Serializer::new(writer))
                .is_ok()
        }
    }
    let encode_failed = || WireError::from(NativeTerminal::Class(terminal_class::ENCODE));
    let mut count = Counter(0);
    if !write(value, &mut count, named) {
        return Err(encode_failed());
    }
    let mut admitted = AdmittedWireVec::allocate(count.0, owner.clone())?;
    let mut destination = admitted.bytes.as_mut_slice();
    if !write(value, &mut destination, named) || !destination.is_empty() {
        return Err(encode_failed());
    }
    Ok(admitted)
}

/// A legacy bridge's bytes are released before its exact reader credit.
pub(crate) struct AdmittedWireVec {
    bytes: Vec<u8>,
    _credit: contextdb_core::read_memory::ReadCredit,
    reader: ReadMemoryOwner,
}

impl AdmittedWireVec {
    fn allocate(length: usize, reader: ReadMemoryOwner) -> WireResult<Self> {
        let layout =
            std::alloc::Layout::array::<u8>(length).map_err(|_| native_wire::allocation_error())?;
        let credit = admit_on(
            &reader,
            layout.size(),
            "materialize_body",
            "legacy transport requires an admitted complete body",
        )?;
        let bytes = if length == 0 {
            Vec::new()
        } else {
            // SAFETY: exact admitted byte layout, transferred unchanged to
            // Vec's global-allocator ownership after initializing every byte.
            let storage = unsafe { std::alloc::alloc(layout) };
            let Some(storage) = std::ptr::NonNull::new(storage) else {
                return Err(allocation_failed(
                    &reader,
                    "cannot allocate admitted transport body",
                ));
            };
            unsafe {
                std::ptr::write_bytes(storage.as_ptr(), 0, length);
                Vec::from_raw_parts(storage.as_ptr(), length, length)
            }
        };
        Ok(Self {
            bytes,
            _credit: credit,
            reader,
        })
    }

    fn copy_from(bytes: &[u8], reader: ReadMemoryOwner) -> WireResult<Self> {
        let mut copy = Self::allocate(bytes.len(), reader)?;
        copy.bytes.copy_from_slice(bytes);
        Ok(copy)
    }

    /// Copy a borrowed eager payload only after the authentic operation exists.
    /// The source allocation is external custody, never adopted as prepaid.
    #[cfg(test)]
    fn copy_for_native(
        bytes: &[u8],
        operation: &crate::owner_read::NativeReadRequest,
    ) -> WireResult<Self> {
        if operation.cancellation().is_cancelled() {
            return Err(contextdb_core::Error::ReadCancelled.into());
        }
        Self::copy_from(bytes, operation.owner())
    }

    /// Adopt bytes whose exact allocation `reader` already paid for, keeping
    /// the same invoice. A foreign or inexact invoice refuses without reserving;
    /// the refused bytes free before their invoice.
    pub(crate) fn from_prepaid(
        bytes: Vec<u8>,
        credit: contextdb_core::read_memory::ReadCredit,
        reader: ReadMemoryOwner,
    ) -> WireResult<Self> {
        if !credit.is_funded_by(&reader)
            || bytes.len() != bytes.capacity()
            || credit.bytes() != bytes.capacity()
        {
            drop(bytes);
            drop(credit);
            return Err(TransportError::NoResponder.into());
        }
        Ok(Self {
            bytes,
            _credit: credit,
            reader,
        })
    }

    /// The admitted bytes.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Hand the exact physical bytes and their invoice to another prepaid
    /// holder. Nothing is copied, reserved or released.
    pub(crate) fn into_prepaid(self) -> (Vec<u8>, contextdb_core::read_memory::ReadCredit) {
        let Self { bytes, _credit, .. } = self;
        (bytes, _credit)
    }

    #[cfg(test)]
    pub(crate) fn as_ptr_for_test(&self) -> *const u8 {
        self.bytes.as_ptr()
    }

    fn from_legacy(bytes: Vec<u8>, reader: ReadMemoryOwner) -> WireResult<Self> {
        let capacity = bytes.capacity();
        let credit = contextdb_core::read_memory::ReadCredit::try_new_owned(
            reader.clone(),
            capacity,
            "sync_transport",
            "legacy_body_handoff",
            "legacy transport body requires an admitted complete allocation",
        )?;
        Ok(Self {
            bytes,
            _credit: credit,
            reader,
        })
    }
}

fn materialize_body(mut body: BoundedWireBody) -> WireFuture<'static, AdmittedWireVec> {
    Box::pin(async move {
        if body.position != 0 {
            return Err(TransportError::RetryUnsafe(
                "legacy handoff requires an unconsumed complete body".to_owned(),
            )
            .into());
        }
        let length = usize::try_from(body.length).map_err(|_| {
            WireError::Transport(TransportError::Other(
                ("body length cannot be represented on this host".to_owned()).into(),
            ))
        })?;
        let mut admitted = AdmittedWireVec::allocate(length, body.reader.clone())?;
        let mut filled = 0;
        while filled < length {
            filled += body.read(&mut admitted.bytes[filled..]).await?;
        }
        Ok(admitted)
    })
}

fn wrap_legacy_reply(bytes: Vec<u8>, reader: ReadMemoryOwner) -> WireResult<BoundedWireReply> {
    let admitted = AdmittedWireVec::from_legacy(bytes, reader.clone())?;
    let length = admitted.bytes.len() as u64;
    Ok(BoundedWireReply::legacy(BoundedWireBody {
        storage: WireBodyStorage::Legacy(admitted),
        length,
        position: 0,
        reader,
    }))
}
pub type TransportStatusFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
pub(crate) type LineageSigner = Arc<dyn Fn(&[u8]) -> contextdb_core::Result<Vec<u8>> + Send + Sync>;
pub type Responder = Box<dyn FnOnce(Vec<u8>) -> TransportFuture<'static, ()> + Send + 'static>;
pub type RequestHandler =
    Arc<dyn Fn(IncomingRequest) -> TransportFuture<'static, ()> + Send + Sync + 'static>;

pub struct IncomingRequest {
    pub bytes: Vec<u8>,
    pub responder: Responder,
    /// The transport-authenticated identity of the node that sent this
    /// request, when the transport authenticates one (the iroh sync path
    /// carries the dialing endpoint's `node_id`). Test transports may use
    /// `None` to exercise identity refusal. The hub uses authenticated values
    /// to record a per-node last-contact on every exchange it serves.
    pub node_id: Option<String>,
}

pub struct HandlerRegistration {
    pub subject: String,
    pub handler: RequestHandler,
}

/// Client (edge) side of the seam: send complete request bytes to a named
/// channel, get complete reply bytes back.
pub trait ClientTransport: Send + Sync {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn reconnect<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        Box::pin(async { true })
    }

    /// The transport-authenticated identity of the peer this client talks to,
    /// when the transport authenticates one. Production sync dials its hub by
    /// key, so a production client always knows who is on the other end. A
    /// test transport may return `None` to exercise identity refusal.
    fn peer_node_id(&self) -> Option<String> {
        None
    }

    /// The stable authenticated identity this client presents as, when the
    /// transport has one.  Sync provenance uses the fabric identity already
    /// held by the transport; it never mints a second author identity.
    fn local_node_id(&self) -> Option<String> {
        None
    }

    /// True only when this client presents one stable authenticated edge
    /// identity to the server. Status's applied-push frontier is meaningful
    /// for status-ahead recovery only under that condition; the remote peer's
    /// identity alone says nothing about which edge the frontier belongs to.
    fn has_stable_edge_identity(&self) -> bool {
        false
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>>;

    /// Additive eager bridge. Existing adapters keep their current request
    /// behavior and remain an honestly legacy dispatch branch.
    #[doc(hidden)]
    fn request_stream<'a>(
        &'a self,
        subject: &'a str,
        body: BoundedWireBody,
        timeout: Duration,
    ) -> WireFuture<'a, BoundedWireReply> {
        Box::pin(async move {
            let reader = body.reader.clone();
            let mut admitted = materialize_body(body).await?;
            let request = std::mem::take(&mut admitted.bytes);
            let response = self.request(subject, request, timeout).await;
            drop(admitted);
            wrap_legacy_reply(response?, reader)
        })
    }

    /// The default cannot prove opaque adapter controls were admitted, so it
    /// deliberately returns the genuine legacy branch.
    #[doc(hidden)]
    fn request_stream_admitted<'a>(
        &'a self,
        subject: &'a str,
        body: BoundedWireBody,
        timeout: Duration,
    ) -> WireResult<DispatchedWireFuture<'a, BoundedWireReply>> {
        Ok(DispatchedWireFuture::legacy(
            self.request_stream(subject, body, timeout),
        ))
    }

    /// Send a request whose response is known by the sync protocol to be one
    /// small message. Adapters may use a cheaper request path that does not
    /// preserve fragmented response collection. Pull must use `request`,
    /// because its reply may be chunked.
    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.request(subject, request_bytes, timeout)
    }

    /// Check whether a request with a known single-message response can be
    /// retried safely if no valid reply arrives. This is a borrowed preflight:
    /// adapters must not send bytes from this method.
    fn ensure_single_reply_retry_safe(&self, _request_bytes: &[u8]) -> TransportResult<()> {
        Ok(())
    }

    /// Release transport resources gracefully (connections, sockets) before
    /// process exit. Adapters with owned endpoints override this; the default
    /// is a no-op.
    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Shutdown for a native producer keeps the caller's exact flag and an
/// independently admitted wake cell. Only request_stop is the native writer.
/// A raw flag writer cannot wake parked work and remains a Legacy boundary.
#[doc(hidden)]
#[derive(Clone)]
pub struct NativeStop {
    flag: Arc<AtomicBool>,
    standing: ReadMemoryOwner,
    wake: contextdb_core::read_memory::ReadShared<NativeStopControl>,
}
struct NativeStopControl {
    state: std::sync::Mutex<NativeStopState>,
    released: std::sync::Condvar,
}
struct NativeStopState {
    // Remains true throughout listener retirement, including its release wait.
    // No later registration can overlap the previous registration's writers.
    registered: bool,
    writers: usize,
    waker: Option<contextdb_core::read_memory::NativeReadWakeLease>,
}
impl NativeStop {
    #[doc(hidden)]
    pub fn try_new(flag: Arc<AtomicBool>, standing: &ReadMemoryOwner) -> WireResult<Self> {
        let invoice = native_wire::wire_credit(
            standing.clone(),
            contextdb_core::read_memory::ReadShared::<NativeStopControl>::layout().size(),
            "native stop control",
        )?;
        let wake = contextdb_core::read_memory::ReadShared::try_from_prepaid_with(invoice, || {
            Ok(NativeStopControl {
                state: std::sync::Mutex::new(NativeStopState {
                    registered: false,
                    writers: 0,
                    waker: None,
                }),
                released: std::sync::Condvar::new(),
            })
        })?;
        Ok(Self {
            flag,
            standing: standing.clone(),
            wake,
        })
    }

    #[doc(hidden)]
    pub fn request_stop(&self) {
        // Publish before wake. A wake racing registration is delivered by
        // the post-registration flag check; no periodic wake is needed.
        self.flag.store(true, std::sync::atomic::Ordering::SeqCst);
        let writer = {
            let mut state = self.wake.state.lock().unwrap_or_else(|p| p.into_inner());
            state.waker.take().map(|waker| {
                // Removal and admission are one mutex transaction. There is
                // only one slot, so at most one writer per registration.
                state.writers += 1;
                NativeStopWriter {
                    control: &self.wake,
                    waker: Some(waker),
                }
            })
        };
        if let Some(writer) = writer {
            // The sole production registration is NativeInProcessWake: an
            // atomic store and futex syscall, with no reentrant user callback.
            // Both invocation and physical lease destruction stay outside state.
            writer.waker.as_ref().unwrap().wake_read();
            drop(writer);
        }
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.flag.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }

    pub(crate) fn is_funded_by(&self, standing: &ReadMemoryOwner) -> bool {
        self.standing.same_owner(standing)
    }

    /// The registration constructs its Waker from the admitted native cell;
    /// an opaque host Waker/StdWeak budget bridge cannot enter this slot.
    pub(crate) fn try_register_wake<T>(
        &self,
        wake: contextdb_core::read_memory::ReadShared<T>,
    ) -> WireResult<NativeStopListener>
    where
        T: contextdb_core::read_memory::NativeReadWake,
    {
        let mut state = self.wake.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.registered {
            return Err(TransportError::NoResponder.into());
        }
        state.registered = true;
        let listener = NativeStopListener {
            wake: self.wake.clone(),
        };
        let waker = wake.into_native_wake_lease();
        if self.is_stopped() {
            drop(state);
            waker.wake_read();
        } else {
            state.waker = Some(waker);
        }
        Ok(listener)
    }
}

pub(crate) struct NativeStopListener {
    wake: contextdb_core::read_memory::ReadShared<NativeStopControl>,
}
impl Drop for NativeStopListener {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.wake.state.lock().unwrap_or_else(|p| p.into_inner());
            // Keep the registration gate closed until all removed leases free.
            state.waker.take()
        };
        // Any retained paid wake cell is physically freed outside the lock.
        drop(waker);
        let mut state = self.wake.state.lock().unwrap_or_else(|p| p.into_inner());
        while state.writers != 0 {
            state = self
                .wake
                .released
                .wait(state)
                .unwrap_or_else(|p| p.into_inner());
        }
        state.registered = false;
    }
}

// Inline writer guard: the request_stop borrow retains the already paid control
// through publication. The removed wake keeps its original funder until free.
// No extra shared allocation, scheduler task, channel or polling is introduced.
struct NativeStopWriter<'a> {
    control: &'a NativeStopControl,
    waker: Option<contextdb_core::read_memory::NativeReadWakeLease>,
}
impl Drop for NativeStopWriter<'_> {
    fn drop(&mut self) {
        // ReadShared's finalizer deallocates before settling the wake invoice.
        // Publish completion only after this removed strong handle is gone.
        drop(self.waker.take());
        {
            let mut state = self.control.state.lock().unwrap_or_else(|p| p.into_inner());
            state.writers -= 1;
        }
        self.control.released.notify_all();
    }
}

/// Serving (hub) side of the seam: accept complete request bytes on named
/// channels and publish reply bytes back.
pub trait ServerTransport: Send + Sync {
    /// Native requires a concrete transport producer; default activation never
    /// silently executes Legacy. Empty/Bound retain their existing delegation.
    #[doc(hidden)]
    fn serve_native_installation<'a>(
        &'a self,
        legacy: Vec<HandlerRegistration>,
        bound: ReadBoundStreamInstallation,
        stop: NativeStop,
    ) -> WireResult<DispatchedWireFuture<'a, ()>> {
        if bound.native_routes().is_some() {
            return Err(TransportError::NoResponder.into());
        }
        self.serve_read_bound_installation(legacy, bound, stop.flag())
    }

    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()>;

    /// Existing implementations preserve their exact handler registration,
    /// asynchronous error, empty-route, and shutdown behavior.
    #[doc(hidden)]
    fn serve_stream<'a>(
        &'a self,
        legacy: Vec<HandlerRegistration>,
        streams: Vec<StreamHandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> WireFuture<'a, ()> {
        drop(streams);
        Box::pin(async move {
            self.serve(legacy, shutdown)
                .await
                .map_err(WireError::Transport)
        })
    }

    /// Opaque installations have exact pre-admitted slots. An unadapted
    /// transport keeps its genuine legacy serving behavior.
    #[doc(hidden)]
    fn serve_read_bound_installation<'a>(
        &'a self,
        legacy: Vec<HandlerRegistration>,
        bound: ReadBoundStreamInstallation,
        shutdown: Arc<AtomicBool>,
    ) -> WireResult<DispatchedWireFuture<'a, ()>> {
        drop(bound);
        Ok(DispatchedWireFuture::legacy(self.serve_stream(
            legacy,
            Vec::new(),
            shutdown,
        )))
    }
}

/// Construct the authenticated Iroh client for an enrollment ticket or dial
/// specification. Every other input is rejected as an invalid sync endpoint.
pub fn client_transport(endpoint: &str) -> Arc<dyn ClientTransport> {
    client_transport_with_lineage_signer(endpoint).0
}

pub(crate) fn client_transport_with_lineage_signer(
    endpoint: &str,
) -> (Arc<dyn ClientTransport>, Option<LineageSigner>) {
    #[cfg(feature = "iroh")]
    match iroh::EndpointSpec::parse_detailed(endpoint) {
        Ok(Some(_)) => iroh::client_with_lineage_signer(endpoint),
        // A shaped spec with a typo must error loudly.
        Err(message) => (Arc::new(InvalidSpecTransport { message }), None),
        Ok(None) => {
            (
                Arc::new(InvalidSpecTransport {
                    message:
                        "supplied sync endpoint/spec is unsupported; use an Iroh enrollment ticket or dial specification"
                            .to_string(),
                }),
                None,
            )
        }
    }
    #[cfg(not(feature = "iroh"))]
    (
        Arc::new(InvalidSpecTransport {
            message: "this build does not include the authenticated Iroh sync transport"
                .to_string(),
        }),
        None,
    )
}

/// Construct the transport used by [`crate::sync_client::SyncClient`]. A
/// file-backed database contributes its adjacent fabric identity path here;
/// a memory database must instead name a persisted identity in its endpoint.
/// The raw client factory above deliberately retains anonymous dialing for
/// transport-level protocol tests that have no database participant.
pub(crate) fn sync_client_transport_with_lineage_signer(
    endpoint: &str,
    default_identity_path: Option<PathBuf>,
) -> (Arc<dyn ClientTransport>, Option<LineageSigner>) {
    #[cfg(feature = "iroh")]
    match iroh::EndpointSpec::parse_detailed(endpoint) {
        Ok(Some(_)) => iroh::sync_client_with_lineage_signer(endpoint, default_identity_path),
        Err(message) => (Arc::new(InvalidSpecTransport { message }), None),
        Ok(None) => {
            (
                Arc::new(InvalidSpecTransport {
                    message:
                        "supplied sync endpoint/spec is unsupported; use an Iroh enrollment ticket or dial specification"
                            .to_string(),
                }),
                None,
            )
        }
    }
    #[cfg(not(feature = "iroh"))]
    (
        Arc::new(InvalidSpecTransport {
            message: "this build does not include the authenticated Iroh sync transport"
                .to_string(),
        }),
        None,
    )
}

pub fn server_transport(endpoint: &str) -> Arc<dyn ServerTransport> {
    #[cfg(feature = "iroh")]
    match iroh::EndpointSpec::parse_detailed(endpoint) {
        Ok(Some(_)) => iroh::server(endpoint),
        Err(message) => Arc::new(InvalidSpecTransport { message }),
        Ok(None) => Arc::new(InvalidSpecTransport {
            message: "supplied sync endpoint/spec is unsupported; use an Iroh bind specification"
                .to_string(),
        }),
    }
    #[cfg(not(feature = "iroh"))]
    Arc::new(InvalidSpecTransport {
        message: "this build does not include the authenticated Iroh sync transport".to_string(),
    })
}

/// Construct a lazy server transport with explicit server-local resource
/// bounds. Endpoint strings remain transport identity and routing only.
#[cfg(feature = "iroh")]
pub fn server_transport_with_resource_policy(
    endpoint: &str,
    policy: iroh::ServerResourcePolicy,
) -> Arc<dyn ServerTransport> {
    match iroh::EndpointSpec::parse_detailed(endpoint) {
        Ok(Some(_)) => iroh::server_with_resource_policy(endpoint, policy),
        Err(message) => Arc::new(InvalidSpecTransport { message }),
        Ok(None) => Arc::new(InvalidSpecTransport {
            message: "supplied sync endpoint/spec is unsupported; use an Iroh bind specification"
                .to_string(),
        }),
    }
}

/// Both-sides transport for a spec that names this stack's endpoint form but
/// fails to parse: every operation reports the parse error verbatim.
struct InvalidSpecTransport {
    message: String,
}

impl ClientTransport for InvalidSpecTransport {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async move { Err(TransportError::Unreachable(self.message.clone())) })
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        Box::pin(async { false })
    }

    fn request<'a>(
        &'a self,
        _subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move { Err(TransportError::Unreachable(self.message.clone())) })
    }
}

impl ServerTransport for InvalidSpecTransport {
    fn serve<'a>(
        &'a self,
        _handlers: Vec<HandlerRegistration>,
        _shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move { Err(TransportError::Unreachable(self.message.clone())) })
    }
}

impl BoundedWireBody {
    /// Move an unconsumed contiguous admitted body out with its invoice.
    /// A captured stream or a partly read body is handed back unchanged.
    pub(crate) fn into_native_contiguous(self) -> Result<AdmittedWireVec, Self> {
        match self {
            Self {
                storage: WireBodyStorage::Legacy(bytes),
                position: 0,
                ..
            } => Ok(bytes),
            other => Err(other),
        }
    }
    /// A Legacy reply's complete Vec, admitted on the selected reader.
    pub(crate) fn from_legacy_reply(bytes: Vec<u8>, reader: ReadMemoryOwner) -> WireResult<Self> {
        Ok(Self::from_native_reply(AdmittedWireVec::from_legacy(
            bytes, reader,
        )?))
    }
    pub(crate) fn from_native_reply(bytes: AdmittedWireVec) -> Self {
        Self {
            length: bytes.bytes.len() as u64,
            position: 0,
            reader: bytes.reader.clone(),
            storage: WireBodyStorage::Legacy(bytes),
        }
    }
    // Only an in-process admitted contiguous body can supply this borrow.
    pub(crate) fn native_contiguous_bytes(&self) -> Option<&[u8]> {
        match &self.storage {
            WireBodyStorage::Legacy(bytes) if self.position == 0 => Some(&bytes.bytes),
            _ => None,
        }
    }
}

// Match VersionedRow's derived JSON field order and Value's externally tagged
// enum representation. In particular Json remains an encoded JSON STRING;
// serializing the whole Value would call json_as_string::to_string and allocate.
