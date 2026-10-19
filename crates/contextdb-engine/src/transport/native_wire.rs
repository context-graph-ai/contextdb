use super::{WireFuture, WireResult};
use crate::vector_read_memory::NativeReadChildReceipt;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryOwner};
use std::alloc::Layout;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An admitted concrete frame. Field order is custody: the frame is dropped
/// before its credit is released on completion, error, or cancellation.
#[doc(hidden)]
struct LegacyAdmittedWireFuture<'a, T> {
    frame: Pin<Box<dyn Future<Output = WireResult<T>> + Send + 'a>>,
    _credit: ReadCredit,
}

#[doc(hidden)]
pub struct AdmittedWireFuture<'a, T>(AdmittedWireFrame<'a, T>);

enum AdmittedWireFrame<'a, T> {
    Legacy(LegacyAdmittedWireFuture<'a, T>),
    Native(contextdb_core::read_memory::ReadOwnedFuture<'a, WireResult<T>>),
}
impl<'a, T> Future for AdmittedWireFuture<'a, T> {
    type Output = WireResult<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().0 {
            AdmittedWireFrame::Legacy(frame) => frame.frame.as_mut().poll(cx),
            AdmittedWireFrame::Native(frame) => Pin::new(frame).poll(cx),
        }
    }
}

/// Core allocates the complete concrete frame before invoking its producer.
/// A producer refusal is returned synchronously; no failed erased future exists.
pub(crate) fn admit_wire_future_with<'a, T, F>(
    owner: ReadMemoryOwner,
    make: impl FnOnce() -> WireResult<F>,
) -> WireResult<AdmittedWireFuture<'a, T>>
where
    F: Future<Output = WireResult<T>> + Send + 'a,
{
    if owner.is_native() {
        // The frame's admission refuses heap-free; a producer error is
        // returned unchanged.
        use contextdb_core::read_memory::NativeProduce;
        return match contextdb_core::read_memory::ReadOwnedFuture::try_new_with_native(owner, make)
        {
            Ok(frame) => Ok(AdmittedWireFuture(AdmittedWireFrame::Native(frame))),
            Err(NativeProduce::Refused(refusal)) => {
                Err(super::NativeTerminal::Refused(refusal).into())
            }
            Err(NativeProduce::Producer(error)) => Err(error),
        };
    }
    let mut refusal = None;
    let frame = contextdb_core::read_memory::ReadOwnedFuture::try_new_with(owner, || {
        make().map_err(|error| {
            refusal = Some(error);
            contextdb_core::Error::ReadCancelled
        })
    });
    match frame {
        Ok(frame) => Ok(AdmittedWireFuture(AdmittedWireFrame::Native(frame))),
        Err(error) => Err(refusal.unwrap_or(super::WireError::Resource(error))),
    }
}

/// Reserve the actual concrete frame layout before allocating its Box. No
/// erased or error future exists on a refusal path.
pub(crate) fn admit_wire_future<'a, T, F>(
    owner: ReadMemoryOwner,
    future: F,
) -> WireResult<AdmittedWireFuture<'a, T>>
where
    F: Future<Output = WireResult<T>> + Send + 'a,
{
    let layout = Layout::new::<F>();
    let credit = super::admit_on(
        &owner,
        layout.size(),
        "wire_future_frame",
        "reduce simultaneous wire operations or raise the declared reader memory limit",
    )?;
    Ok(AdmittedWireFuture(AdmittedWireFrame::Legacy(
        LegacyAdmittedWireFuture {
            frame: Box::pin(future),
            _credit: credit,
        },
    )))
}

/// The dispatch boundary is deliberately opaque: a legacy erased future is
/// never reclassified as native merely because an additive wrapper exists.
#[doc(hidden)]
pub struct DispatchedWireFuture<'a, T>(DispatchKind<'a, T>);

enum DispatchKind<'a, T> {
    Legacy(WireFuture<'a, T>),
    Native(AdmittedWireFuture<'a, T>),
}

impl<'a, T> DispatchedWireFuture<'a, T> {
    pub(crate) fn legacy(future: WireFuture<'a, T>) -> Self {
        Self(DispatchKind::Legacy(future))
    }

    pub(crate) fn native(future: AdmittedWireFuture<'a, T>) -> Self {
        Self(DispatchKind::Native(future))
    }
}

impl<'a, T> Future for DispatchedWireFuture<'a, T> {
    type Output = WireResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.0 {
            DispatchKind::Legacy(future) => future.as_mut().poll(cx),
            DispatchKind::Native(future) => Pin::new(future).poll(cx),
        }
    }
}

pub(crate) fn allocation_error() -> super::WireError {
    super::WireError::Resource(contextdb_core::Error::Other(
        "cannot allocate admitted transport control".into(),
    ))
}

pub(crate) fn wire_credit(
    owner: ReadMemoryOwner,
    bytes: usize,
    operation: &'static str,
) -> WireResult<ReadCredit> {
    super::admit_on(
        &owner,
        bytes,
        operation,
        "reduce simultaneous wire operations or raise the declared reader memory limit",
    )
}

use std::alloc::{alloc, dealloc};
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

/// A concrete callback whose allocation invoice survives consumption of
/// FnOnce, including a synchronous refusal or unwinding during invocation.
pub(crate) struct NativeStreamResponder {
    finish: Option<Box<NativeFinish>>,
    _credit: ReadCredit,
    owner: ReadMemoryOwner,
}
type NativeFinish = dyn FnOnce(super::BoundedWireBody) -> WireResult<AdmittedWireFuture<'static, ()>>
    + Send
    + 'static;
impl NativeStreamResponder {
    pub(crate) fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self.owner.same_owner(owner)
    }

    pub(crate) fn try_finish(
        mut self,
        body: super::BoundedWireBody,
    ) -> WireResult<AdmittedWireFuture<'static, ()>> {
        if !self.owner.same_owner(&body.owner()) {
            return Err(super::TransportError::Other(
                "native reply body belongs to another reader".into(),
            )
            .into());
        }
        let finish = self.finish.take().expect("one-use native finish callback");
        // Calling Box<dyn FnOnce> destroys its allocation before returning.
        // self retains the invoice across that call and its unwind cleanup.
        finish(body)
    }
}

/// Exact UTF-8 storage, admitted from borrowed pieces before allocation.
/// String's payload field frees before its original external invoice.
pub(crate) struct NativeWireString {
    value: String,
    _credit: ReadCredit,
}
impl NativeWireString {
    pub(crate) fn try_from_parts(owner: ReadMemoryOwner, parts: &[&str]) -> WireResult<Self> {
        let length = parts
            .iter()
            .try_fold(0usize, |total, part| total.checked_add(part.len()))
            .ok_or_else(allocation_error)?;
        let layout = Layout::array::<u8>(length).map_err(|_| allocation_error())?;
        let credit = wire_credit(owner, layout.size(), "wire_string")?;
        if length == 0 {
            return Ok(Self {
                value: String::new(),
                _credit: credit,
            });
        }
        // SAFETY: exact nonzero byte layout; String uses the same allocator,
        // capacity and alignment on destruction. Every input is valid UTF-8.
        let storage = unsafe { alloc(layout) };
        let Some(storage) = NonNull::new(storage) else {
            return Err(allocation_error());
        };
        let mut offset = 0;
        for part in parts {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    part.as_ptr(),
                    storage.as_ptr().add(offset),
                    part.len(),
                );
            }
            offset += part.len();
        }
        let value = unsafe { String::from_raw_parts(storage.as_ptr(), length, length) };
        Ok(Self {
            value,
            _credit: credit,
        })
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.value
    }
}

/// An opaque root that can be leased by cloning the same native block.
/// Installation consumers remain separate; constructors stay inside the engine.
#[doc(hidden)]
pub struct ReadBoundStreamInstallation {
    routes: Option<InstalledReadRoutes>,
}
impl ReadBoundStreamInstallation {
    pub(crate) fn empty() -> Self {
        Self { routes: None }
    }
    pub(crate) fn native_routes(
        &self,
    ) -> Option<&contextdb_core::read_memory::ReadShared<NativeWireRoutes>> {
        match self.routes.as_ref() {
            Some(InstalledReadRoutes::Native(routes)) => Some(routes),
            _ => None,
        }
    }
}
enum InstalledReadRoutes {
    Native(contextdb_core::read_memory::ReadShared<NativeWireRoutes>),
}

/// Starting a request never supplies a fabricated cancellation to a Legacy
/// provider. The actual caller must keep the native capability before dispatch.
pub(crate) enum NativeReadStreamBegin {
    Legacy(crate::vector_read_memory::RequestReadOwner),
    Native {
        operation: crate::owner_read::NativeReadRequest,
        cancellation: contextdb_core::read_contract::NativeReadCancellation,
    },
}

/// Retain the original operation in the concrete future frame. The producer
/// receives this operation before constructing F and must separately admit any
/// allocating captures. Executor wait/Ready/cancel ordering stays in its kernel;
/// this forwarding wrapper neither installs a wake listener nor replaces it.
struct NativeOperationFuture<F> {
    future: F,
    _operation: crate::owner_read::NativeReadRequest,
    completed: bool,
}
impl<T, F: Future<Output = WireResult<T>>> Future for NativeOperationFuture<F> {
    type Output = WireResult<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the wrapper never moves its pinned future. The operation is
        // retained through F's Drop, and no Drop implementation moves F.
        let this = unsafe { self.get_unchecked_mut() };
        let result = unsafe { Pin::new_unchecked(&mut this.future) }.poll(cx);
        if result.is_ready() {
            this.completed = true;
        }
        result
    }
}

/// Admit the wrapper INCLUDING operation custody before make sees the request.
/// The old by-value entry is retained for existing inactive callers only.
pub(crate) fn admit_native_operation_future_with<'a, T, F>(
    operation: crate::owner_read::NativeReadRequest,
    make: impl FnOnce(&crate::owner_read::NativeReadRequest) -> WireResult<F>,
) -> WireResult<DispatchedWireFuture<'a, T>>
where
    F: Future<Output = WireResult<T>> + Send + 'a,
{
    let owner = operation.owner();
    admit_wire_future_with(owner, || {
        let future = make(&operation)?;
        Ok(NativeOperationFuture {
            future,
            _operation: operation,
            completed: false,
        })
    })
    .map(DispatchedWireFuture::native)
}

impl NativeStreamResponder {
    pub(crate) fn try_new_with<H>(
        owner: ReadMemoryOwner,
        make: impl FnOnce() -> WireResult<H>,
    ) -> WireResult<Self>
    where
        H: FnOnce(super::BoundedWireBody) -> WireResult<AdmittedWireFuture<'static, ()>>
            + Send
            + 'static,
    {
        // The callback itself has an erased FnOnce signature; no credit or raw
        // payload is extracted from Core's opaque OwnedReadBox.
        let invoice = wire_credit(
            owner.clone(),
            Layout::new::<H>().size(),
            "native_finish_callback",
        )?;
        let layout = Layout::new::<H>();
        let storage = if layout.size() == 0 {
            NonNull::<H>::dangling()
        } else {
            NonNull::new(unsafe { alloc(layout) }.cast::<H>()).ok_or_else(|| {
                super::allocation_failed(&owner, "cannot allocate admitted transport control")
            })?
        };
        let guard = NativeInitGuard {
            storage: storage.cast(),
            layout,
            invoice,
        };
        let finish = make()?;
        unsafe {
            storage.as_ptr().write(finish);
        }
        // No fallible action after initialization, before Box takes custody.
        let finish: Box<NativeFinish> = unsafe { Box::from_raw(storage.as_ptr()) };
        let guard = ManuallyDrop::new(guard);
        let invoice = unsafe { std::ptr::read(&guard.invoice) };
        Ok(Self {
            finish: Some(finish),
            _credit: invoice,
            owner,
        })
    }
}

// This private construction guard frees uninitialized storage before its
// external invoice, on producer Err/panic. It is never a refund API.
struct NativeInitGuard {
    storage: NonNull<u8>,
    layout: Layout,
    invoice: ReadCredit,
}
impl Drop for NativeInitGuard {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            unsafe { dealloc(self.storage.as_ptr(), self.layout) };
        }
    }
}

/// Object-safe private installation companion. It inherits no Legacy default.
pub(crate) trait NativeRouteDispatch: Send + Sync {
    fn subject(&self) -> &str;
    fn begin_request(&self) -> WireResult<NativeReadStreamBegin>;
    fn dispatch_native(
        &self,
        request: super::ReadBoundIncomingRequest,
        operation: crate::owner_read::NativeReadRequest,
    ) -> WireResult<DispatchedWireFuture<'static, ()>>;
    fn dispatch_legacy(
        &self,
        request: super::ReadBoundIncomingRequest,
        owner: crate::vector_read_memory::RequestReadOwner,
    ) -> WireResult<DispatchedWireFuture<'static, ()>>;
}
pub(crate) struct NativeWireRoutes {
    entries: contextdb_core::read_memory::OwnedReadBox<
        [contextdb_core::read_memory::OwnedReadBox<dyn NativeRouteDispatch>],
    >,
    clock: contextdb_core::read_contract::NativeClockBinding,
    installation: ReadMemoryOwner,
    timeouts: contextdb_core::read_contract::OwnerServiceTimeouts,
}
impl NativeWireRoutes {
    pub(crate) fn installation_owner(&self) -> ReadMemoryOwner {
        self.installation.clone()
    }
    pub(crate) fn clock(&self) -> &contextdb_core::read_contract::NativeClockBinding {
        &self.clock
    }
    pub(super) fn shutdown_drain_ms(&self) -> u64 {
        self.timeouts.shutdown_drain_ms
    }

    pub(crate) fn find(&self, subject: &str) -> Option<&dyn NativeRouteDispatch> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.subject() == subject)
            .map(|entry| entry.as_ref())
    }
}
struct NativeRoute<H, L> {
    subject: NativeWireString,
    provider: std::sync::Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
    native: H,
    legacy: L,
}
impl<H, L> NativeRouteDispatch for NativeRoute<H, L>
where
    H: Fn(
            super::ReadBoundIncomingRequest,
            crate::owner_read::NativeReadRequest,
        ) -> WireResult<DispatchedWireFuture<'static, ()>>
        + Send
        + Sync
        + 'static,
    L: Fn(
            super::ReadBoundIncomingRequest,
            crate::vector_read_memory::RequestReadOwner,
        ) -> WireResult<DispatchedWireFuture<'static, ()>>
        + Send
        + Sync
        + 'static,
{
    fn subject(&self) -> &str {
        self.subject.as_str()
    }
    fn begin_request(&self) -> WireResult<NativeReadStreamBegin> {
        use crate::vector_read_memory::{RequestOwnerFailure, RequestReadOwner, RequestReadRoute};
        // A native provider's acquisition and the request's cancellation token
        // both refuse heap-free; a Legacy provider keeps its own refusal.
        let acquired =
            self.provider
                .request_budget_owner_native()
                .map_err(|failure| match failure {
                    RequestOwnerFailure::Native(refusal) => {
                        super::WireError::from(super::NativeTerminal::Refused(refusal))
                    }
                    RequestOwnerFailure::Legacy(error) => super::WireError::Resource(error),
                })?;
        match acquired.into_route() {
            RequestReadRoute::Legacy(reader) => Ok(NativeReadStreamBegin::Legacy(
                RequestReadOwner::legacy(reader),
            )),
            RequestReadRoute::Native(receipt) => {
                let (operation, cancellation) =
                    crate::owner_read::NativeReadRequest::try_new_native(receipt)
                        .map_err(super::NativeTerminal::Refused)?;
                Ok(NativeReadStreamBegin::Native {
                    operation,
                    cancellation,
                })
            }
        }
    }
    fn dispatch_native(
        &self,
        request: super::ReadBoundIncomingRequest,
        operation: crate::owner_read::NativeReadRequest,
    ) -> WireResult<DispatchedWireFuture<'static, ()>> {
        if !operation.owner().same_owner(&request.body.owner())
            || !matches!(&request.responder, super::ReadBoundResponder::Native(reply) if reply.is_funded_by(&operation.owner()))
        {
            return Err(super::TransportError::NoResponder.into());
        }
        if operation.cancellation().is_cancelled() {
            return Err(contextdb_core::Error::ReadCancelled.into());
        }
        // The selected handler's _with frame owns its original operation. A
        // second wrapper and a second operation invoice are unnecessary.
        let owner = operation.owner();
        let future = (self.native)(request, operation)?;
        match &future.0 {
            DispatchKind::Native(frame) if matches!(&frame.0, AdmittedWireFrame::Native(native) if native.is_funded_by(&owner)) => {
                Ok(future)
            }
            _ => Err(super::TransportError::NoResponder.into()),
        }
    }
    fn dispatch_legacy(
        &self,
        request: super::ReadBoundIncomingRequest,
        owner: crate::vector_read_memory::RequestReadOwner,
    ) -> WireResult<DispatchedWireFuture<'static, ()>> {
        (self.legacy)(request, owner)
    }
}

// Exact fixed slots, independently paid before any route factory runs.
pub(crate) struct NativeRouteBuilder {
    storage: Option<NonNull<contextdb_core::read_memory::OwnedReadBox<dyn NativeRouteDispatch>>>,
    slots: usize,
    initialized: usize,
    invoice: Option<ReadCredit>,
    installation: ReadMemoryOwner,
    clock: Option<contextdb_core::read_contract::NativeClockBinding>,
    timeouts: contextdb_core::read_contract::OwnerServiceTimeouts,
}
impl NativeRouteBuilder {
    pub(crate) fn try_new(
        installation: NativeReadChildReceipt,
        slots: usize,
        clock: &std::sync::Arc<dyn contextdb_core::read_contract::DeadlineClock>,
        timeouts: contextdb_core::read_contract::OwnerServiceTimeouts,
    ) -> WireResult<Self> {
        timeouts
            .validate_at(clock.now_ms())
            .map_err(|_| super::TransportError::NoResponder)?;
        let installation = installation.into_owner();
        let clock = if slots == 0 {
            None
        } else {
            Some(
                contextdb_core::read_contract::NativeClockBinding::try_select(
                    clock,
                    &installation,
                )?,
            )
        };
        let layout = Layout::array::<
            contextdb_core::read_memory::OwnedReadBox<dyn NativeRouteDispatch>,
        >(slots)
        .map_err(|_| allocation_error())?;
        let invoice = wire_credit(installation.clone(), layout.size(), "native_route_slots")?;
        let storage = if slots == 0 {
            None
        } else {
            Some(NonNull::new(unsafe { alloc(layout) }.cast()).ok_or_else(allocation_error)?)
        };
        Ok(Self {
            storage,
            slots,
            initialized: 0,
            invoice: Some(invoice),
            installation,
            clock,
            timeouts,
        })
    }
    pub(crate) fn try_push<H, L>(
        &mut self,
        subject: &str,
        provider: std::sync::Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
        make_native: impl FnOnce(ReadMemoryOwner) -> WireResult<H>,
        make_legacy: impl FnOnce(ReadMemoryOwner) -> WireResult<L>,
    ) -> WireResult<()>
    where
        H: Fn(
                super::ReadBoundIncomingRequest,
                crate::owner_read::NativeReadRequest,
            ) -> WireResult<DispatchedWireFuture<'static, ()>>
            + Send
            + Sync
            + 'static,
        L: Fn(
                super::ReadBoundIncomingRequest,
                crate::vector_read_memory::RequestReadOwner,
            ) -> WireResult<DispatchedWireFuture<'static, ()>>
            + Send
            + Sync
            + 'static,
    {
        if subject.is_empty() || self.initialized == self.slots {
            return Err(super::TransportError::NoResponder.into());
        }
        let owner = self.installation.clone();
        let subject = NativeWireString::try_from_parts(owner.clone(), &[subject])?;
        let layout = Layout::new::<NativeRoute<H, L>>();
        let invoice = wire_credit(owner.clone(), layout.size(), "native_route_dispatch")?;
        let storage = NonNull::new(unsafe { alloc(layout) }.cast::<NativeRoute<H, L>>())
            .ok_or_else(allocation_error)?;
        let guard = NativeInitGuard {
            storage: storage.cast(),
            layout,
            invoice,
        };
        // The concrete route, including H/L inline captures, is prepaid before
        // either factory. Their separately allocating captures keep own grants.
        let native = make_native(owner.clone())?;
        let legacy = make_legacy(owner)?;
        unsafe {
            storage.as_ptr().write(NativeRoute {
                subject,
                provider,
                native,
                legacy,
            });
        }
        let route: Box<dyn NativeRouteDispatch> = unsafe { Box::from_raw(storage.as_ptr()) };
        let guard = ManuallyDrop::new(guard);
        let invoice = unsafe { std::ptr::read(&guard.invoice) };
        let route = contextdb_core::read_memory::OwnedReadBox::new(route, invoice);
        unsafe {
            self.storage
                .expect("nonempty native slots")
                .as_ptr()
                .add(self.initialized)
                .write(route);
        }
        self.initialized += 1;
        Ok(())
    }
    pub(crate) fn finish(mut self) -> WireResult<ReadBoundStreamInstallation> {
        if self.initialized != self.slots {
            return Err(super::TransportError::NoResponder.into());
        }
        if self.slots == 0 {
            return Ok(ReadBoundStreamInstallation::empty());
        }
        let invoice = wire_credit(
            self.installation.clone(),
            contextdb_core::read_memory::ReadShared::<NativeWireRoutes>::layout().size(),
            "native_route_root",
        )?;
        let storage = self.storage.take().expect("complete native slots");
        let slot_invoice = self.invoice.take().expect("native slots paid");
        let entries = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                storage.as_ptr(),
                self.slots,
            ))
        };
        self.initialized = 0;
        let entries = contextdb_core::read_memory::OwnedReadBox::new(entries, slot_invoice);
        let installation = self.installation.clone();
        let routes =
            contextdb_core::read_memory::ReadShared::try_from_prepaid_with(invoice, || {
                Ok(NativeWireRoutes {
                    entries,
                    clock: self.clock.take().expect("nonempty native clock"),
                    installation,
                    timeouts: self.timeouts,
                })
            })?;
        Ok(ReadBoundStreamInstallation {
            routes: Some(InstalledReadRoutes::Native(routes)),
        })
    }
}
impl Drop for NativeRouteBuilder {
    fn drop(&mut self) {
        let Some(storage) = self.storage.take() else {
            return;
        };
        let layout = Layout::array::<
            contextdb_core::read_memory::OwnedReadBox<dyn NativeRouteDispatch>,
        >(self.slots)
        .expect("checked native slots");
        let guard = NativeInitGuard {
            storage: storage.cast(),
            layout,
            invoice: self.invoice.take().expect("partial native slots paid"),
        };
        unsafe {
            std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                storage.as_ptr(),
                self.initialized,
            ));
        }
        drop(guard);
    }
}

/// Authenticated delivery-outcome route. The native arm moves the admitted
/// request body into the paid custody selection and moves the paid reply into
/// the responder; neither direction copies the bytes or issues a second invoice.
/// Tenant and hub stay on the installation invoice; each request makes its own
/// paid copies of them and of the authenticated edge before the frame exists.
/// The Database, identity and clock are standing roots, retained, never re-labelled.
#[allow(clippy::too_many_arguments)] // Standing roots and policy stay explicit.
pub(crate) fn install_native_delivery_route<L>(
    installation: NativeReadChildReceipt,
    provider: std::sync::Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
    subject: &str,
    tenant: &str,
    hub: &str,
    database: std::sync::Arc<crate::database::Database>,
    identity: std::sync::Arc<crate::identity::FabricIdentity>,
    clock: std::sync::Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    limits: contextdb_core::read_contract::ReadLimits,
    timeouts: contextdb_core::read_contract::OwnerServiceTimeouts,
    on_native: impl Fn() + Send + Sync + 'static,
    legacy: L,
) -> WireResult<ReadBoundStreamInstallation>
where
    L: Fn(
            super::ReadBoundIncomingRequest,
            crate::vector_read_memory::RequestReadOwner,
        ) -> WireResult<DispatchedWireFuture<'static, ()>>
        + Send
        + Sync
        + 'static,
{
    use crate::custody::native_selection::NativeBuffer;
    let mut builder = NativeRouteBuilder::try_new(installation, 1, &clock, timeouts)?;
    builder.try_push(
        subject,
        provider,
        |owner| {
            let tenant = NativeWireString::try_from_parts(owner.clone(), &[tenant])?;
            let hub = NativeWireString::try_from_parts(owner, &[hub])?;
            Ok(
                move |request: super::ReadBoundIncomingRequest,
                      operation: crate::owner_read::NativeReadRequest| {
                    on_native();
                    let database = database.clone();
                    let identity = identity.clone();
                    let clock = clock.clone();
                    let owner = operation.owner();
                    let edge = request
                        .node_id
                        .as_deref()
                        .ok_or(super::TransportError::NoResponder)?;
                    let tenant = NativeBuffer::copy_from(&owner, tenant.as_str().as_bytes())
                        .map_err(native_delivery_error)?;
                    let hub = NativeBuffer::copy_from(&owner, hub.as_str().as_bytes())
                        .map_err(native_delivery_error)?;
                    let edge = NativeBuffer::copy_from(&owner, edge.as_bytes())
                        .map_err(native_delivery_error)?;
                    admit_native_operation_future_with(operation, |operation| {
                        let entry =
                            crate::executor::bounded::NativeReadExecutionEntry::try_new_native(
                                operation, limits, clock,
                            )?;
                        let operation = operation.share();
                        Ok(async move {
                            let super::ReadBoundIncomingRequest {
                                body, responder, ..
                            } = request;
                            let (bytes, credit) = body
                                .into_native_contiguous()
                                .map_err(|_| super::TransportError::NoResponder)?
                                .into_prepaid();
                            let input = NativeBuffer::from_prepaid(bytes, credit, &owner)
                                .map_err(native_delivery_error)?;
                            let context = entry.open_context();
                            let reply = crate::sync_native_delivery::paid_delivery_response(
                                &database,
                                &identity,
                                input,
                                tenant,
                                hub,
                                edge,
                                // One page per request, bounded by the declared
                                // cursor page policy.
                                usize::try_from(limits.cursor_page_rows).unwrap_or(usize::MAX),
                                limits.cursor_page_bytes,
                                &operation,
                                &context,
                            )
                            .map_err(native_delivery_error)?;
                            let (bytes, credit) = reply.into_prepaid();
                            let reply = super::AdmittedWireVec::from_prepaid(bytes, credit, owner)?;
                            #[cfg(test)]
                            super::native_pause::at(super::native_pause::Point::AfterSelection)
                                .await;
                            responder
                                .try_finish(super::BoundedWireBody::from_native_reply(reply))?
                                .await?;
                            // entry's only persistent logical strong then drops.
                            Ok(())
                        })
                    })
                },
            )
        },
        |_| Ok(legacy),
    )?;
    builder.finish()
}
/// Terminal for one refused or failed native delivery, built without any
/// owned allocation: native refusals keep their static fields, kernel refusals
/// are moved, and failures without a typed refusal answer a static class.
pub(crate) fn native_delivery_error(
    failure: crate::custody::native_selection::Failure,
) -> super::WireError {
    use super::{NativeTerminal, terminal_class};
    use crate::custody::native_selection::Failure;
    match failure {
        Failure::Refused(refusal) => NativeTerminal::Refused(refusal).into(),
        Failure::Unqualified(error) => NativeTerminal::from_kernel(error).into(),
        Failure::ColdUnqualified(error) => {
            tracing::debug!(error = ?error, "native delivery cold-read fault");
            NativeTerminal::Class(terminal_class::FAULT).into()
        }
        Failure::Invalid(kind) => {
            tracing::debug!(kind = ?kind, "native delivery request failed validation");
            NativeTerminal::Class(terminal_class::INVALID_REQUEST).into()
        }
        Failure::OwnerMismatch => NativeTerminal::Class(terminal_class::OWNER_MISMATCH).into(),
    }
}

impl<F> Drop for NativeOperationFuture<F> {
    fn drop(&mut self) {
        // Notify the ORIGINAL token before dropping a pending frame. The
        // frame/resource fields then drop before request custody, as declared.
        if !self.completed {
            self._operation.cancellation().cancel();
        }
    }
}
