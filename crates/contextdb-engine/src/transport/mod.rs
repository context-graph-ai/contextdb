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
mod native_wire;
#[doc(hidden)]
pub use native_wire::ReadBoundStreamInstallation;
#[doc(hidden)]
pub use native_wire::{AdmittedWireFuture, DispatchedWireFuture};
pub(crate) use native_wire::{NativeStreamResponder, NativeWireString, admit_wire_future};

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

use crate::vector_read_memory::{RequestReadOwner, RequestReadRoute};
use contextdb_core::read_memory::{OwnedReadBox, ReadCredit, ReadMemoryOwner};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};
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
/// Resource failures retain their typed cause until the sync layer maps them.
#[doc(hidden)]
#[derive(Debug)]
pub enum WireError {
    Resource(contextdb_core::Error),
    Transport(TransportError),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resource(error) => fmt::Display::fmt(error, f),
            Self::Transport(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for WireError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Resource(error) => Some(error),
            Self::Transport(error) => Some(error),
        }
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

/// The source captures a fixed operation body. Its implementation, including
/// the original source invoice, stays outside this transport foundation.
pub(crate) trait WireBodySource: Send + Sync {
    fn read_at<'a>(&'a self, position: u64, dst: &'a mut [u8]) -> WireFuture<'a, usize>;
    fn poll_read_at(
        &self,
        position: u64,
        dst: &mut [u8],
        cx: &mut Context<'_>,
    ) -> Poll<WireResult<usize>>;
    fn read_idle(&self) -> Option<Duration> {
        None
    }
    fn reopen(&self) -> WireResult<BoundedWireBody>;
    fn prepare_materialization(&self) -> WireResult<()>;
}

enum WireBodyStorage {
    Captured(contextdb_core::read_memory::OwnedReadBox<dyn WireBodySource>),
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
    pub(crate) fn from_captured(
        source: contextdb_core::read_memory::OwnedReadBox<dyn WireBodySource>,
        length: u64,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Self {
        Self {
            storage: WireBodyStorage::Captured(source),
            length,
            position: 0,
            reader: ReadMemoryOwner::from_legacy(reader),
        }
    }

    pub(crate) fn from_captured_owned(
        source: OwnedReadBox<dyn WireBodySource>,
        length: u64,
        reader: ReadMemoryOwner,
    ) -> Self {
        Self {
            storage: WireBodyStorage::Captured(source),
            length,
            position: 0,
            reader,
        }
    }

    pub(crate) fn owner(&self) -> ReadMemoryOwner {
        self.reader.clone()
    }

    pub(crate) fn poll_read(
        &mut self,
        dst: &mut [u8],
        cx: &mut Context<'_>,
    ) -> Poll<WireResult<usize>> {
        let Some(remaining) = self.length.checked_sub(self.position) else {
            return Poll::Ready(Err(TransportError::IncompleteReply(
                "body cursor exceeds its declared length".into(),
            )
            .into()));
        };
        let limit = remaining.min(dst.len() as u64) as usize;
        if limit == 0 {
            return Poll::Ready(Ok(0));
        }
        let count = match &self.storage {
            WireBodyStorage::Captured(source) => {
                match source.poll_read_at(self.position, &mut dst[..limit], cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(count)) => count,
                }
            }
            WireBodyStorage::Legacy(bytes) => {
                let start = self.position as usize;
                dst[..limit].copy_from_slice(&bytes.bytes[start..start + limit]);
                limit
            }
        };
        if count == 0 || count > limit {
            return Poll::Ready(Err(TransportError::IncompleteReply(
                "body source did not honor its declared length".into(),
            )
            .into()));
        }
        self.position += count as u64;
        Poll::Ready(Ok(count))
    }

    async fn read_concrete(&mut self, dst: &mut [u8]) -> WireResult<usize> {
        let idle = match &self.storage {
            WireBodyStorage::Captured(source) => source.read_idle(),
            WireBodyStorage::Legacy(_) => None,
        };
        let read = std::future::poll_fn(|cx| self.poll_read(dst, cx));
        match idle {
            Some(idle) => tokio::time::timeout(idle, read).await.map_err(|_| {
                WireError::Transport(TransportError::IncompleteReply(
                    "request frame made no byte progress before its idle deadline".into(),
                ))
            })?,
            None => read.await,
        }
    }

    pub(crate) fn read_admitted<'a>(
        &'a mut self,
        dst: &'a mut [u8],
    ) -> WireResult<AdmittedWireFuture<'a, usize>> {
        admit_wire_future(self.owner(), self.read_concrete(dst))
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
                WireBodyStorage::Captured(source) => {
                    source.read_at(self.position, &mut dst[..limit]).await?
                }
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

/// A provider-bearing route retains the exact installed provider. Its
/// installation controls have explicit layouts and original-owner invoices.
#[doc(hidden)]
pub struct ReadBoundStreamRegistration {
    subject: native_wire::NativeWireString,
    provider: Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
    handler: OwnedReadBox<BoundStreamRequestHandler>,
    installation: WireInstallationOwner,
}

type BoundStreamRequestHandler = dyn Fn(ReadBoundIncomingRequest, RequestReadOwner) -> WireResult<DispatchedWireFuture<'static, ()>>
    + Send
    + Sync
    + 'static;

pub(crate) enum WireInstallationOwner {
    Legacy(Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>),
    Native(ReadMemoryOwner),
}
impl WireInstallationOwner {
    fn acquire(request: RequestReadOwner) -> Self {
        match request.into_route() {
            RequestReadRoute::Legacy(reader) => Self::Legacy(reader),
            RequestReadRoute::Native(receipt) => Self::Native(receipt.into_owner()),
        }
    }
    pub(crate) fn owner(&self) -> ReadMemoryOwner {
        match self {
            Self::Legacy(reader) => ReadMemoryOwner::from_legacy(reader.clone()),
            Self::Native(owner) => owner.clone(),
        }
    }
}

impl ReadBoundStreamRegistration {
    pub(crate) fn try_new<H>(
        subject: &str,
        provider: Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
        installation: RequestReadOwner,
        handler: H,
    ) -> WireResult<Self>
    where
        H: Fn(
                ReadBoundIncomingRequest,
                RequestReadOwner,
            ) -> WireResult<DispatchedWireFuture<'static, ()>>
            + Send
            + Sync
            + 'static,
    {
        if subject.is_empty() {
            return Err(
                TransportError::Other("bound stream subject cannot be empty".to_owned()).into(),
            );
        }
        // Captured heap state must already own its original allocation
        // invoices; this credit admits only the concrete callback Box.
        let installation = WireInstallationOwner::acquire(installation);
        let owner = installation.owner();
        let subject = native_wire::NativeWireString::try_from_parts(owner.clone(), &[subject])?;
        let handler_credit = native_wire::wire_credit(
            owner,
            std::alloc::Layout::new::<H>().size(),
            "route_handler",
        )?;
        let handler: Box<BoundStreamRequestHandler> = Box::new(handler);
        Ok(Self {
            subject,
            provider,
            handler: OwnedReadBox::new(handler, handler_credit),
            installation,
        })
    }

    pub(crate) fn subject(&self) -> &str {
        self.subject.as_str()
    }
    pub(crate) fn installation(&self) -> &WireInstallationOwner {
        &self.installation
    }
    pub(crate) fn acquire_request(&self) -> WireResult<RequestReadOwner> {
        self.provider
            .request_budget_owner()
            .map_err(WireError::Resource)
    }
    pub(crate) fn dispatch(
        &self,
        request: ReadBoundIncomingRequest,
        owner: RequestReadOwner,
    ) -> WireResult<DispatchedWireFuture<'static, ()>> {
        let actual = WireInstallationOwner::acquire(owner.share()).owner();
        if !actual.same_owner(&request.body.owner()) {
            return Err(TransportError::Other(
                "bound request body belongs to another reader".into(),
            )
            .into());
        }
        (self.handler.as_ref())(request, owner)
    }
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
        let credit = contextdb_core::read_memory::ReadCredit::try_new_owned(
            reader.clone(),
            layout.size(),
            "sync_transport",
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
                return Err(WireError::Resource(contextdb_core::Error::Other(
                    "cannot allocate admitted transport body".into(),
                )));
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
    fn copy_for_native(
        bytes: &[u8],
        operation: &crate::owner_read::NativeReadRequest,
    ) -> WireResult<Self> {
        if operation.cancellation().is_cancelled() {
            return Err(contextdb_core::Error::ReadCancelled.into());
        }
        Self::copy_from(bytes, operation.owner())
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

/// An eager copy, explicitly not a bounded source/window route. Native
/// callers can admit its concrete driver without an erased read wrapper.
pub(crate) fn materialize_body_admitted(
    mut body: BoundedWireBody,
) -> WireResult<AdmittedWireFuture<'static, AdmittedWireVec>> {
    let owner = body.owner();
    admit_wire_future(owner, async move {
        if body.position != 0 {
            return Err(TransportError::RetryUnsafe(
                "eager handoff requires an unconsumed complete body".into(),
            )
            .into());
        }
        let length = usize::try_from(body.length).map_err(|_| native_wire::allocation_error())?;
        let mut admitted = AdmittedWireVec::allocate(length, body.owner())?;
        let mut filled = 0;
        while filled < length {
            filled += body.read_concrete(&mut admitted.bytes[filled..]).await?;
        }
        Ok(admitted)
    })
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
                "body length cannot be represented on this host".to_owned(),
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

    /// Bound registrations need a concrete native implementation before they
    /// can carry a native claim. The default intentionally delegates all
    /// legacy routes, including an empty bound vector, to `serve_stream`.
    #[doc(hidden)]
    fn serve_read_bound<'a>(
        &'a self,
        legacy: Vec<HandlerRegistration>,
        bound: Vec<ReadBoundStreamRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> WireResult<DispatchedWireFuture<'a, ()>> {
        drop(bound);
        Ok(DispatchedWireFuture::legacy(self.serve_stream(
            legacy,
            Vec::new(),
            shutdown,
        )))
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

/// Private resident-row reply producer. The source row remains borrowed under
/// its Database table guard through sizing and writing. Every nested value is
/// serialized by borrow, so the exact reply buffer is the only new payload.
pub(crate) fn encode_native_point_row(
    row: Option<&contextdb_core::VersionedRow>,
    context: &crate::executor::bounded::NativeReadOpenContext,
) -> std::result::Result<Option<AdmittedWireVec>, crate::executor::BoundedExecutionError> {
    use crate::executor::BoundedExecutionError;
    let mut size = NativeRowSize {
        bytes: 0,
        overflow: false,
    };
    write_native_point_row(&mut size, row).map_err(|_| BoundedExecutionError::Unimplemented)?;
    if size.overflow {
        return Err(BoundedExecutionError::Unimplemented);
    }
    context.admit_result(row.is_some(), size.bytes)?;
    let mut answer =
        AdmittedWireVec::allocate(size.bytes, context.owner()?).map_err(|error| match error {
            WireError::Resource(error) => BoundedExecutionError::from(error),
            WireError::Transport(_) => BoundedExecutionError::Unimplemented,
        })?;
    let mut destination = answer.bytes.as_mut_slice();
    write_native_point_row(&mut destination, row)
        .map_err(|_| BoundedExecutionError::Unimplemented)?;
    if !destination.is_empty() {
        return Err(BoundedExecutionError::Unimplemented);
    }
    context.checkpoint()?;
    Ok(Some(answer))
}
struct NativeRowSize {
    bytes: usize,
    overflow: bool,
}
impl std::io::Write for NativeRowSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        match self.bytes.checked_add(bytes.len()) {
            Some(length) => self.bytes = length,
            None => self.overflow = true,
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl BoundedWireBody {
    fn from_native_reply(bytes: AdmittedWireVec) -> Self {
        Self {
            length: bytes.bytes.len() as u64,
            position: 0,
            reader: bytes.reader.clone(),
            storage: WireBodyStorage::Legacy(bytes),
        }
    }
    // Only an in-process admitted contiguous body can supply this borrow.
    // Captured stream sources require the unactivated native decode companion.
    fn native_contiguous_bytes(&self) -> Option<&[u8]> {
        match &self.storage {
            WireBodyStorage::Legacy(bytes) if self.position == 0 => Some(&bytes.bytes),
            _ => None,
        }
    }
}

// Match VersionedRow's derived JSON field order and Value's externally tagged
// enum representation. In particular Json remains an encoded JSON STRING;
// serializing the whole Value would call json_as_string::to_string and allocate.
fn write_native_point_row<W: std::io::Write>(
    writer: &mut W,
    row: Option<&contextdb_core::VersionedRow>,
) -> std::io::Result<()> {
    let Some(row) = row else {
        return writer.write_all(b"null");
    };
    writer.write_all(b"{\"row_id\":")?;
    native_json_scalar(writer, &row.row_id)?;
    writer.write_all(b",\"values\":{")?;
    for (index, (name, value)) in row.values.iter().enumerate() {
        if index != 0 {
            writer.write_all(b",")?;
        }
        native_json_scalar(writer, name)?;
        writer.write_all(b":")?;
        write_native_row_value(writer, value)?;
    }
    writer.write_all(b"},\"created_tx\":")?;
    native_json_scalar(writer, &row.created_tx)?;
    writer.write_all(b",\"deleted_tx\":")?;
    native_json_scalar(writer, &row.deleted_tx)?;
    writer.write_all(b",\"lsn\":")?;
    native_json_scalar(writer, &row.lsn)?;
    writer.write_all(b",\"created_at\":")?;
    native_json_scalar(writer, &row.created_at)?;
    writer.write_all(b"}")
}
fn native_json_scalar<W: std::io::Write, T: serde::Serialize + ?Sized>(
    writer: &mut W,
    value: &T,
) -> std::io::Result<()> {
    // These borrowed scalar/array/serde_json::Value serializers allocate no
    // nested storage. The exact-sized writer cannot fail on valid source data.
    serde_json::to_writer(writer, value).map_err(|_| std::io::ErrorKind::InvalidData.into())
}
fn write_native_row_value<W: std::io::Write>(
    writer: &mut W,
    value: &contextdb_core::Value,
) -> std::io::Result<()> {
    use contextdb_core::Value;
    if matches!(value, Value::Null) {
        return writer.write_all(b"\"Null\"");
    }
    writer.write_all(b"{")?;
    match value {
        Value::Null => unreachable!("handled unit value"),
        Value::Bool(value) => {
            writer.write_all(b"\"Bool\":")?;
            native_json_scalar(writer, value)?;
        }
        Value::Int64(value) => {
            writer.write_all(b"\"Int64\":")?;
            native_json_scalar(writer, value)?;
        }
        Value::Float64(value) => {
            writer.write_all(b"\"Float64\":")?;
            native_json_scalar(writer, value)?;
        }
        Value::Text(value) => {
            writer.write_all(b"\"Text\":")?;
            native_json_scalar(writer, value)?;
        }
        Value::Uuid(value) => {
            writer.write_all(b"\"Uuid\":")?;
            native_json_scalar(writer, value)?;
        }
        Value::Timestamp(value) => {
            writer.write_all(b"\"Timestamp\":")?;
            native_json_scalar(writer, value)?;
        }
        Value::Vector(value) => {
            writer.write_all(b"\"Vector\":")?;
            native_json_scalar(writer, value)?;
        }
        Value::TxId(value) => {
            writer.write_all(b"\"TxId\":")?;
            native_json_scalar(writer, value)?;
        }
        Value::Json(value) => {
            writer.write_all(b"\"Json\":\"")?;
            // Escape borrowed inner JSON directly as the outer string. No
            // to_string, temporary Vec, nested clone, or growable writer.
            native_json_scalar(&mut NativeJsonStringWriter(writer), value)?;
            writer.write_all(b"\"")?;
        }
    }
    writer.write_all(b"}")
}
struct NativeJsonStringWriter<'a, W>(&'a mut W);
impl<W: std::io::Write> std::io::Write for NativeJsonStringWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for &byte in bytes {
            match byte {
                b'"' => self.0.write_all(b"\\\"")?,
                b'\\' => self.0.write_all(b"\\\\")?,
                b'\n' => self.0.write_all(b"\\n")?,
                b'\r' => self.0.write_all(b"\\r")?,
                b'\t' => self.0.write_all(b"\\t")?,
                8 => self.0.write_all(b"\\b")?,
                12 => self.0.write_all(b"\\f")?,
                0..=31 => self.0.write_all(&[
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX[(byte >> 4) as usize],
                    HEX[(byte & 15) as usize],
                ])?,
                _ => self.0.write_all(&[byte])?,
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
