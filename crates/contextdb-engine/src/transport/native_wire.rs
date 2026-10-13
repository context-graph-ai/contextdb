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
impl<'a, T> AdmittedWireFuture<'a, T> {
    fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        match &self.0 {
            AdmittedWireFrame::Legacy(frame) => frame._credit.is_funded_by(owner),
            AdmittedWireFrame::Native(frame) => frame.is_funded_by(owner),
        }
    }
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
    let credit = ReadCredit::try_new_owned(
        owner,
        layout.size(),
        "sync_transport",
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

pub(crate) fn admit_native_request_future<'a, T, F>(
    receipt: NativeReadChildReceipt,
    future: F,
) -> WireResult<DispatchedWireFuture<'a, T>>
where
    F: Future<Output = WireResult<T>> + Send + 'a,
{
    admit_wire_future(receipt.into_owner(), future).map(DispatchedWireFuture::native)
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
    ReadCredit::try_new_owned(
        owner,
        bytes,
        "sync_transport",
        operation,
        "reduce simultaneous wire operations or raise the declared reader memory limit",
    )
    .map_err(super::WireError::Resource)
}

use std::alloc::{alloc, dealloc};
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

/// A transport-owned strong control with one explicit native allocation.
/// Its credit is moved to the freeing stack before payload destruction, then
/// settled only after deallocation, including during unwinding. No Weak or
/// standard Arc facade is exposed.
pub(crate) struct WireShared<T: Send + Sync> {
    block: NonNull<WireSharedBlock<T>>,
}
struct WireSharedBlock<T> {
    owners: AtomicUsize,
    payload: ManuallyDrop<T>,
    invoice: ManuallyDrop<ReadCredit>,
}
// SAFETY: only the atomic strong count mutates without the payload's own
// synchronization. Shared payload access is immutable; T is Send + Sync.
unsafe impl<T: Send + Sync> Send for WireShared<T> {}
unsafe impl<T: Send + Sync> Sync for WireShared<T> {}

impl<T: Send + Sync> WireShared<T> {
    pub(crate) fn try_new(payload: T, owner: ReadMemoryOwner) -> WireResult<Self> {
        let layout = Layout::new::<WireSharedBlock<T>>();
        let invoice = wire_credit(owner, layout.size(), "shared_control")?;
        // SAFETY: exact nonzero, aligned layout (the count makes it nonzero).
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<WireSharedBlock<T>>()) else {
            // The payload is destroyed while its invoice is still live.
            drop(payload);
            drop(invoice);
            return Err(allocation_error());
        };
        // SAFETY: fresh storage, initialized once before publishing a handle.
        unsafe {
            block.as_ptr().write(WireSharedBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { block })
    }
}
impl<T: Send + Sync> Clone for WireShared<T> {
    fn clone(&self) -> Self {
        // SAFETY: this live handle retains the allocation during increment.
        let count = unsafe { &self.block.as_ref().owners };
        if count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                (value < isize::MAX as usize).then_some(value + 1)
            })
            .is_err()
        {
            std::process::abort();
        }
        Self { block: self.block }
    }
}
impl<T: Send + Sync> std::ops::Deref for WireShared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: a live strong owner keeps the initialized payload alive.
        unsafe { &self.block.as_ref().payload }
    }
}
struct WireFreeGuard<T> {
    block: NonNull<WireSharedBlock<T>>,
    _invoice: ReadCredit,
}
impl<T> Drop for WireFreeGuard<T> {
    fn drop(&mut self) {
        // SAFETY: only the final strong owner constructs this guard. Payload
        // and invoice have been moved/destroyed; exact layout is unchanged.
        unsafe {
            dealloc(
                self.block.as_ptr().cast(),
                Layout::new::<WireSharedBlock<T>>(),
            );
        }
        // The invoice field is destroyed AFTER this destructor deallocates.
    }
}
impl<T: Send + Sync> Drop for WireShared<T> {
    fn drop(&mut self) {
        // SAFETY: this handle retains the block through the final decrement.
        let ptr = self.block.as_ptr();
        if unsafe { (*ptr).owners.fetch_sub(1, Ordering::Release) } != 1 {
            return;
        }
        fence(Ordering::Acquire);
        // SAFETY: final owner has exclusive destruction authority. The guard
        // never reads the payload after its Drop starts, even on panic.
        let invoice = unsafe { ManuallyDrop::take(&mut (*ptr).invoice) };
        let guard = WireFreeGuard {
            block: self.block,
            _invoice: invoice,
        };
        unsafe {
            ManuallyDrop::drop(&mut (*ptr).payload);
        }
        drop(guard);
    }
}

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
    pub(crate) fn try_new<H>(owner: ReadMemoryOwner, finish: H) -> WireResult<Self>
    where
        H: FnOnce(super::BoundedWireBody) -> WireResult<AdmittedWireFuture<'static, ()>>
            + Send
            + 'static,
    {
        let credit = wire_credit(owner.clone(), Layout::new::<H>().size(), "finish_callback")?;
        Ok(Self {
            finish: Some(Box::new(finish)),
            _credit: credit,
            owner,
        })
    }

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
    pub(crate) fn routes(&self) -> Option<&WireShared<BoundWireRoutes>> {
        match self.routes.as_ref() {
            Some(InstalledReadRoutes::Bound(routes)) => Some(routes),
            _ => None,
        }
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
    Bound(WireShared<BoundWireRoutes>),
    Native(contextdb_core::read_memory::ReadShared<NativeWireRoutes>),
}

/// Every entry drops before the exact slot buffer frees; the buffer invoice
/// lives outside that Box and the root invoice lives outside the root payload.
pub(crate) struct BoundWireRoutes {
    entries: contextdb_core::read_memory::OwnedReadBox<[super::ReadBoundStreamRegistration]>,
    installation: super::WireInstallationOwner,
}
impl BoundWireRoutes {
    pub(crate) fn owner(&self) -> ReadMemoryOwner {
        self.installation.owner()
    }
    pub(crate) fn is_native(&self) -> bool {
        matches!(self.installation, super::WireInstallationOwner::Native(_))
    }
    pub(crate) fn find(&self, subject: &str) -> Option<&super::ReadBoundStreamRegistration> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.subject() == subject)
    }
}

/// Fixed slots are allocated once, with a partial-initialization guard. This
/// builder never accepts a pre-existing Vec as a preadmission certificate.
pub(crate) struct ReadBoundRouteBuilder {
    storage: Option<NonNull<super::ReadBoundStreamRegistration>>,
    initialized: usize,
    slots: usize,
    invoice: Option<ReadCredit>,
    install: crate::vector_read_memory::RequestReadOwner,
}
// SAFETY: the exclusively owned slot allocation contains Send registrations.
unsafe impl Send for ReadBoundRouteBuilder {}
impl ReadBoundRouteBuilder {
    pub(crate) fn try_new(
        install: crate::vector_read_memory::RequestReadOwner,
        slots: usize,
    ) -> WireResult<Self> {
        let layout = Layout::array::<super::ReadBoundStreamRegistration>(slots)
            .map_err(|_| allocation_error())?;
        let owner = super::WireInstallationOwner::acquire(install.share()).owner();
        let invoice = wire_credit(owner, layout.size(), "route_slots")?;
        let storage = if slots == 0 {
            None
        } else {
            // SAFETY: registration is nonzero-sized; checked array layout.
            Some(NonNull::new(unsafe { alloc(layout) }.cast()).ok_or_else(allocation_error)?)
        };
        Ok(Self {
            storage,
            initialized: 0,
            slots,
            invoice: Some(invoice),
            install,
        })
    }

    pub(crate) fn try_push<H>(
        &mut self,
        subject: &str,
        provider: std::sync::Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
        make: impl FnOnce(ReadMemoryOwner) -> WireResult<H>,
    ) -> WireResult<()>
    where
        H: Fn(
                super::ReadBoundIncomingRequest,
                crate::vector_read_memory::RequestReadOwner,
            ) -> WireResult<DispatchedWireFuture<'static, ()>>
            + Send
            + Sync
            + 'static,
    {
        if self.initialized == self.slots {
            return Err(super::TransportError::Other("bound route slots are full".into()).into());
        }
        // The factory is invoked only after the installation was acquired and
        // the slots admitted. It must fund its separately allocating captures.
        let owner = super::WireInstallationOwner::acquire(self.install.share()).owner();
        let handler = make(owner)?;
        let registration = super::ReadBoundStreamRegistration::try_new(
            subject,
            provider,
            self.install.share(),
            handler,
        )?;
        // SAFETY: this slot is in bounds and has never been initialized.
        unsafe {
            self.storage
                .expect("nonempty fixed slots")
                .as_ptr()
                .add(self.initialized)
                .write(registration);
        }
        self.initialized += 1;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> WireResult<ReadBoundStreamInstallation> {
        if self.initialized != self.slots {
            return Err(
                super::TransportError::Other("bound route slots are incomplete".into()).into(),
            );
        }
        if self.slots == 0 {
            return Ok(ReadBoundStreamInstallation::empty());
        }
        let owner = super::WireInstallationOwner::acquire(self.install.share()).owner();
        let storage = self.storage.take().expect("complete fixed slots");
        let invoice = self.invoice.take().expect("slot invoice");
        // SAFETY: every exact slot is initialized; Box<[T]> frees the same
        // array layout without resizing. Custody moves to OwnedReadBox.
        let entries = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                storage.as_ptr(),
                self.slots,
            ))
        };
        self.initialized = 0;
        let routes = BoundWireRoutes {
            entries: contextdb_core::read_memory::OwnedReadBox::new(entries, invoice),
            installation: super::WireInstallationOwner::acquire(self.install.share()),
        };
        let routes = WireShared::try_new(routes, owner)?;
        Ok(ReadBoundStreamInstallation {
            routes: Some(InstalledReadRoutes::Bound(routes)),
        })
    }
}
struct RouteSlotsFreeGuard {
    storage: NonNull<super::ReadBoundStreamRegistration>,
    layout: Layout,
    _invoice: ReadCredit,
}
impl Drop for RouteSlotsFreeGuard {
    fn drop(&mut self) {
        // SAFETY: the partial-entry destructor has exclusive allocation
        // authority; the external invoice remains live through this free.
        unsafe {
            dealloc(self.storage.as_ptr().cast(), self.layout);
        }
    }
}
impl Drop for ReadBoundRouteBuilder {
    fn drop(&mut self) {
        let Some(storage) = self.storage.take() else {
            return;
        };
        let guard = RouteSlotsFreeGuard {
            storage,
            layout: Layout::array::<super::ReadBoundStreamRegistration>(self.slots)
                .expect("checked fixed slot layout"),
            _invoice: self.invoice.take().expect("partial slot invoice"),
        };
        // SAFETY: only the initialized prefix is dropped. The guard frees
        // the whole buffer after entries, even if an entry destructor unwinds.
        unsafe {
            std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                storage.as_ptr(),
                self.initialized,
            ));
        }
        drop(guard);
    }
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

/// A separate, inactive native handler registration. Installation and request
/// children have different lifetimes; no operation uses the installation owner.
/// H's nested allocations and the supplied provider's standing controls require
/// their own invoices. ReadShared<H> prices only its concrete physical cell.
pub(crate) struct NativeReadStreamRegistration<H: Send + Sync> {
    subject: NativeWireString,
    provider: std::sync::Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
    handler: contextdb_core::read_memory::ReadShared<H>,
    installation: ReadMemoryOwner,
}

impl<H> NativeReadStreamRegistration<H>
where
    H: Fn(
            super::ReadBoundIncomingRequest,
            crate::owner_read::NativeReadRequest,
        ) -> WireResult<DispatchedWireFuture<'static, ()>>
        + Send
        + Sync
        + 'static,
{
    pub(crate) fn try_new(
        installation: NativeReadChildReceipt,
        subject: &str,
        provider: std::sync::Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
        make: impl FnOnce(ReadMemoryOwner) -> WireResult<H>,
    ) -> WireResult<Self> {
        if subject.is_empty() {
            return Err(super::TransportError::Other(
                "bound stream subject cannot be empty".into(),
            )
            .into());
        }
        let installation = installation.into_owner();
        let subject = NativeWireString::try_from_parts(installation.clone(), &[subject])?;
        let invoice = wire_credit(
            installation.clone(),
            contextdb_core::read_memory::ReadShared::<H>::layout().size(),
            "native_route_handler",
        )?;
        // The factory must separately admit allocating captures; the invoice
        // above is retained through its refusal/unwind and the H cell's free.
        let handler = make(installation.clone())?;
        let handler = contextdb_core::read_memory::ReadShared::try_from_prepaid(handler, invoice)?;
        Ok(Self {
            subject,
            provider,
            handler,
            installation,
        })
    }

    pub(crate) fn subject(&self) -> &str {
        self.subject.as_str()
    }

    pub(crate) fn installation_owner(&self) -> ReadMemoryOwner {
        self.installation.clone()
    }

    /// Acquire exactly once from the authentic installed provider, then select
    /// the sealed route before subject/body/frame controls can be constructed.
    pub(crate) fn begin_request(&self) -> WireResult<NativeReadStreamBegin> {
        use crate::vector_read_memory::{RequestReadOwner, RequestReadRoute};
        match self.provider.request_budget_owner()?.into_route() {
            RequestReadRoute::Legacy(reader) => Ok(NativeReadStreamBegin::Legacy(
                RequestReadOwner::legacy(reader),
            )),
            RequestReadRoute::Native(receipt) => {
                let (operation, cancellation) =
                    crate::owner_read::NativeReadRequest::try_new(receipt)?;
                Ok(NativeReadStreamBegin::Native {
                    operation,
                    cancellation,
                })
            }
        }
    }

    pub(crate) fn dispatch(
        &self,
        request: super::ReadBoundIncomingRequest,
        operation: crate::owner_read::NativeReadRequest,
    ) -> WireResult<DispatchedWireFuture<'static, ()>> {
        if !operation.owner().same_owner(&request.body.owner()) {
            return Err(super::TransportError::Other(
                "bound request body belongs to another reader".into(),
            )
            .into());
        }
        let responder_matches = match &request.responder {
            super::ReadBoundResponder::Native(responder) => {
                responder.is_funded_by(&operation.owner())
            }
            super::ReadBoundResponder::Legacy(_) => false,
        };
        if !responder_matches {
            return Err(super::TransportError::Other(
                "native request responder belongs to another route or reader".into(),
            )
            .into());
        }
        if operation.cancellation().is_cancelled() {
            return Err(contextdb_core::Error::ReadCancelled.into());
        }
        // The native handler consumes the authentic operation. The Legacy
        // registration and its existing dispatcher remain separate.
        let retained = operation.share();
        let future = (self.handler)(request, operation)?;
        // A native registration cannot adopt an opaque Legacy future or an
        // admitted frame belonging to a different operation. Check before
        // allocating the retaining driver; rejection destroys the returned
        // frame under its existing invoice, without repricing it.
        let future = match future.0 {
            DispatchKind::Native(future) if future.is_funded_by(&retained.owner()) => future,
            _ => {
                return Err(super::TransportError::Other(
                    "native handler did not return an originally funded native future".into(),
                )
                .into());
            }
        };
        admit_native_operation_future_with(retained, |_| Ok(future))
    }
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

pub(crate) fn admit_native_operation_future<'a, T, F>(
    operation: crate::owner_read::NativeReadRequest,
    make: impl FnOnce(&crate::owner_read::NativeReadRequest) -> F,
) -> WireResult<DispatchedWireFuture<'a, T>>
where
    F: Future<Output = WireResult<T>> + Send + 'a,
{
    let owner = operation.owner();
    let future = make(&operation);
    admit_wire_future(
        owner,
        NativeOperationFuture {
            future,
            _operation: operation,
            completed: false,
        },
    )
    .map(DispatchedWireFuture::native)
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
            NonNull::new(unsafe { alloc(layout) }.cast::<H>()).ok_or_else(allocation_error)?
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
        use crate::vector_read_memory::{RequestReadOwner, RequestReadRoute};
        match self.provider.request_budget_owner()?.into_route() {
            RequestReadRoute::Legacy(reader) => Ok(NativeReadStreamBegin::Legacy(
                RequestReadOwner::legacy(reader),
            )),
            RequestReadRoute::Native(receipt) => {
                let (operation, cancellation) =
                    crate::owner_read::NativeReadRequest::try_new(receipt)?;
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

/// Private addressed resident-row wiring through the borrowed-key selector.
/// This storage operation is not a public query or owner-session door.
/// The Database and clock are authentic standing roots, retained rather than
/// moved/reallocated or relabelled as request-owned installation payloads.
pub(crate) fn install_native_point_route(
    installation: NativeReadChildReceipt,
    provider: std::sync::Arc<dyn crate::vector_read_memory::ReadRequestBudgetProvider>,
    subject: &str,
    table: &str,
    database: std::sync::Arc<crate::database::Database>,
    clock: std::sync::Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    limits: contextdb_core::read_contract::ReadLimits,
    timeouts: contextdb_core::read_contract::OwnerServiceTimeouts,
) -> WireResult<ReadBoundStreamInstallation> {
    let mut builder = NativeRouteBuilder::try_new(installation, 1, &clock, timeouts)?;
    builder.try_push(
        subject,
        provider,
        |owner| {
            let table = NativeWireString::try_from_parts(owner, &[table])?;
            Ok(
                move |request: super::ReadBoundIncomingRequest,
                      operation: crate::owner_read::NativeReadRequest| {
                    let database = database.clone();
                    let clock = clock.clone();
                    // The table stays on its original installation invoice; this alias
                    // has its own per-request physical buffer, prepaid before copying.
                    let table =
                        NativeWireString::try_from_parts(operation.owner(), &[table.as_str()])?;
                    admit_native_operation_future_with(operation, |operation| {
                        let entry = crate::executor::bounded::NativeReadExecutionEntry::try_new(
                            operation, limits, clock,
                        )?;
                        Ok(async move {
                            let body = request
                                .body
                                .native_contiguous_bytes()
                                .ok_or(super::TransportError::NoResponder)?;
                            // Two original scalar identities, copied from borrowed
                            // admitted custody; no allocating decode or plan wrapper.
                            if body.len() != 16 {
                                return Err(super::TransportError::NoResponder.into());
                            }
                            let row_id = contextdb_core::RowId(u64::from_le_bytes(
                                body[..8].try_into().expect("eight row bytes"),
                            ));
                            let snapshot = contextdb_core::SnapshotId(u64::from_le_bytes(
                                body[8..].try_into().expect("eight snapshot bytes"),
                            ));
                            let key = table.into_point_key(row_id);
                            let context = entry.open_context();
                            context.checkpoint()?;
                            let answer = crate::executor::native_point_row(
                                &database, &key.value, snapshot, &entry,
                            )
                            .map_err(native_point_error)?
                            .ok_or(super::TransportError::NoResponder)?;
                            context.checkpoint()?;
                            request
                                .responder
                                .try_finish(super::BoundedWireBody::from_native_reply(answer))?
                                .await?;
                            // entry's only persistent logical strong then drops. Any
                            // weak decoder/source holder now expires with ReadCancelled.
                            Ok(())
                        })
                    })
                },
            )
        },
        |_| Ok(|_, _| Err(super::TransportError::NoResponder.into())),
    )?;
    builder.finish()
}
fn native_point_error(error: crate::executor::BoundedExecutionError) -> super::WireError {
    match error {
        crate::executor::BoundedExecutionError::Engine(error) => error.into(),
        crate::executor::BoundedExecutionError::Cancelled => {
            contextdb_core::Error::ReadCancelled.into()
        }
        crate::executor::BoundedExecutionError::Refused(failure) => {
            contextdb_core::Error::ReadFailure(failure).into()
        }
        // This private storage slice has no public unsupported-query vocabulary.
        // Unsupported sources remain unactivated; they never call Legacy.
        crate::executor::BoundedExecutionError::Unimplemented => {
            super::TransportError::NoResponder.into()
        }
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

// The original operation paid for this String before its allocation. Move its
// custody into the concrete future's inline key, retaining the same invoice.
// The tuple (including String storage) drops before the external credit.
struct NativePointKey {
    value: (contextdb_core::TableName, contextdb_core::RowId),
    _credit: ReadCredit,
}
impl NativeWireString {
    fn into_point_key(self, row_id: contextdb_core::RowId) -> NativePointKey {
        NativePointKey {
            value: (self.value, row_id),
            _credit: self._credit,
        }
    }
}
