//! Transport seam: the interface between the sync logic and the thing that
//! moves bytes. Concrete transports live in submodules; the sync logic names
//! only these types.

#[cfg(any(
    test,
    feature = "in-process-test-seams",
    feature = "test-seams"
))]
pub mod in_process;
#[cfg(feature = "iroh")]
pub mod iroh;
#[cfg(feature = "iroh")]
mod large_request_staging;

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
pub use iroh::{
    ServerResourcePolicy, bind_spec as peer_bind_spec, dial_spec as peer_dial_spec,
};

/// Gate endpoint-owned integration on the transport implementation without
/// exposing its feature selection to server-facing modules.
macro_rules! peer_endpoint_available {
    ($item:item) => {
        #[cfg(feature = "iroh")]
        $item
    };
}
pub(crate) use peer_endpoint_available;

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
        }
    }
}

impl std::error::Error for TransportError {}

pub type TransportResult<T> = Result<T, TransportError>;
pub type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = TransportResult<T>> + Send + 'a>>;
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

/// Serving (hub) side of the seam: accept complete request bytes on named
/// channels and publish reply bytes back.
pub trait ServerTransport: Send + Sync {
    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()>;
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
        Ok(Some(_)) => return iroh::client_with_lineage_signer(endpoint),
        // A shaped spec with a typo must error loudly.
        Err(message) => return (Arc::new(InvalidSpecTransport { message }), None),
        Ok(None) => {
            return (
                Arc::new(InvalidSpecTransport {
                    message:
                        "supplied sync endpoint/spec is unsupported; use an Iroh enrollment ticket or dial specification"
                            .to_string(),
                }),
                None,
            );
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
        Ok(Some(_)) => {
            return iroh::sync_client_with_lineage_signer(endpoint, default_identity_path);
        }
        Err(message) => return (Arc::new(InvalidSpecTransport { message }), None),
        Ok(None) => {
            return (
                Arc::new(InvalidSpecTransport {
                    message:
                        "supplied sync endpoint/spec is unsupported; use an Iroh enrollment ticket or dial specification"
                            .to_string(),
                }),
                None,
            );
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
        Ok(Some(_)) => return iroh::server(endpoint),
        Err(message) => return Arc::new(InvalidSpecTransport { message }),
        Ok(None) => {
            return Arc::new(InvalidSpecTransport {
                message:
                    "supplied sync endpoint/spec is unsupported; use an Iroh bind specification"
                        .to_string(),
            });
        }
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
