//! Private exact-layout storage receipts with scalar admission refusal egress.
//! Original bootstrap refusals borrow their static diagnostics.
//! Joined Original callbacks move private diagnostic custody; legacy callbacks
//! and lower producers retain their explicit unqualified coverage.
use crate::memory_accounting::{
    OwnedMemoryReservation, StoreAccountantHandle, WriterUse, clone_persistence_original_writer,
    persistence_original_projection,
};
use contextdb_core::read_memory::ReadMemoryBudget as _;
use contextdb_core::read_memory::{NativeRefusal, ReadCredit, ReadMemoryOwner};
use contextdb_core::{Error, Lsn, Result, SnapshotId};
use contextdb_tx::SourceFunder;
use redb::{NativeStorageAdmission, NativeStorageCredit, NativeStorageRefused};
use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

const SUBSYSTEM: &str = "redb";
const OPERATION: &str = "exact_byte_read";
const HINT: &str = "Reduce the selected read or raise its declared memory limit.";

/// Native admission failures own no diagnostic bytes. Allocating body and
/// legacy reader bootstrap errors retain their explicit unqualified route.
#[derive(Debug)]
pub(crate) enum NativeStorageFailure {
    Refused(NativeRefusal),
    AllocatorFailed { site: &'static str, layout: Layout },
    Unqualified(Error),
}
impl std::fmt::Display for NativeStorageFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => std::fmt::Display::fmt(refusal, f),
            Self::AllocatorFailed { site, layout } => std::fmt::Display::fmt(
                &redb::NativeColdReadError::AllocatorFailed {
                    site,
                    layout: *layout,
                },
                f,
            ),
            Self::Unqualified(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for NativeStorageFailure {}

// This lends a synchronized diagnosis only during the callback. The actual
// owner travels in redb's native marker; it need not be the current TLS owner.
unsafe fn inspect_scope_failure(
    raw: NonNull<()>,
    inspect: &mut dyn FnMut(Option<&(dyn std::error::Error + Send + Sync + 'static)>),
) {
    let scope = &unsafe { raw.cast::<StorageScopeBlock>().as_ref() }.payload;
    let failure = scope.failure.lock().unwrap_or_else(|e| e.into_inner());
    inspect(
        failure
            .original
            .as_ref()
            .map(|cause| cause as &(dyn std::error::Error + Send + Sync + 'static)),
    );
}

pub(crate) enum StorageAllocation {
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    Writer(OwnedMemoryReservation),
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    Read(ReadCredit),
}
#[cfg(feature = "test-seams")]
unsafe fn inspect_scope_allocation_funding(raw: NonNull<()>) -> Option<(usize, usize)> {
    // Paired only with this module's StorageScopeBlock admission constructor.
    let scope = &unsafe { raw.cast::<StorageScopeBlock>().as_ref() }.payload;
    let owner = match &scope.funding {
        StorageFunding::Original(original) => Some(original.accountant().as_ptr() as usize),
        StorageFunding::Writer {
            original_handle, ..
        }
        | StorageFunding::PreparedWriter {
            original_handle, ..
        } => Some(original_handle.as_handle().as_ptr() as usize),
        StorageFunding::Read(reader) | StorageFunding::ReadUnqualified(reader) => {
            use contextdb_core::read_memory::ReadMemoryBudget;
            reader.funding_accountant_identity_for_test()
        }
    }?;
    Some((owner, Layout::new::<StorageReceipt>().size()))
}

enum StorageFunding {
    Original(OwnedMemoryReservation),
    #[allow(
        dead_code,
        reason = "LAYOUT-ONLY: never constructed; it sets StorageFunding to the size StorageScopeBlock is charged at, so removing it waits for the measured accounting rerun"
    )]
    Writer {
        original_use: WriterUse,
        original_handle: StoreAccountantHandle,
        genuine_funder: SourceFunder,
    },
    // Preparation already holds the authenticated Original. It does not need
    // a newly discovered SourceFunder or a current writer registration.
    PreparedWriter {
        original_use: WriterUse,
        original_handle: StoreAccountantHandle,
    },
    Read(ReadMemoryOwner),
    // Existing Error-producing reader adapters keep their ordinary admission.
    ReadUnqualified(ReadMemoryOwner),
}
impl StorageFunding {
    // Bootstrap uses the same held Original and its existing scalar policy.
    // Static diagnostic labels borrow their backing; no diagnostic invoice is
    // needed. Legacy reader adapters retain their existing Error kernel.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn reserve_unqualified(&self, layout: Layout) -> Result<StorageAllocation> {
        match self {
            Self::Original(original) => {
                reserve_original_bootstrap(original, layout.size()).map(StorageAllocation::Writer)
            }
            Self::Writer {
                original_use,
                original_handle,
                genuine_funder,
            } => {
                debug_assert!(original_handle.is_writer());
                // writer_unqualified/prepare_writer validated this immutable
                // funder against the held use before building StorageFunding.
                // Keep it alive with the scope; never rediscover a registration.
                let _ = genuine_funder;
                crate::memory_accounting::reserve_native_storage_from_writer_use(
                    original_use,
                    original_handle,
                    layout.size(),
                )
                .map(StorageAllocation::Writer)
                .map_err(|failure| original_storage_error(NativeStorageFailure::Refused(failure)))
            }
            Self::PreparedWriter { .. } => {
                unreachable!("prepared storage uses scalar admission exclusively")
            }
            Self::Read(owner) | Self::ReadUnqualified(owner) => {
                ReadCredit::try_new_owned(owner.clone(), layout.size(), SUBSYSTEM, OPERATION, HINT)
                    .map(StorageAllocation::Read)
            }
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn reserve_scope(
        &self,
        layout: Layout,
    ) -> std::result::Result<StorageAllocation, NativeStorageFailure> {
        match self {
            Self::ReadUnqualified(_) => self
                .reserve_unqualified(layout)
                .map_err(NativeStorageFailure::Unqualified),
            _ => self
                .reserve_native(layout)
                .map_err(NativeStorageFailure::Refused),
        }
    }
    // Native scope admissions never enter an allocating Error kernel.
    fn reserve_native(
        &self,
        layout: Layout,
    ) -> std::result::Result<StorageAllocation, NativeRefusal> {
        match self {
            Self::Original(original) => {
                let original_use =
                    clone_persistence_original_writer(original).expect("validated original writer");
                let handle = persistence_original_projection(original)
                    .expect("validated original projection");
                crate::memory_accounting::reserve_native_storage_from_writer_use_for(
                    &original_use,
                    &handle,
                    layout.size(),
                    "pending_directory",
                    "Raise MEMORY_LIMIT to admit pending table roots.",
                )
                .map(StorageAllocation::Writer)
            }
            Self::Writer {
                original_use,
                original_handle,
                ..
            }
            | Self::PreparedWriter {
                original_use,
                original_handle,
            } => crate::memory_accounting::reserve_native_storage_from_writer_use(
                original_use,
                original_handle,
                layout.size(),
            )
            .map(StorageAllocation::Writer),
            Self::Read(owner) | Self::ReadUnqualified(owner) => ReadCredit::try_new_owned_native(
                owner.clone(),
                layout.size(),
                SUBSYSTEM,
                OPERATION,
                HINT,
            )
            .map(StorageAllocation::Read),
        }
    }
}
// Validate and retain the exact held writer before entering scalar admission.
// Both the zero-byte capability clone and the actual scope-cell debit keep the
// bootstrap's original pending_directory labels and hint, including on refusal.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn reserve_original_bootstrap(
    original: &OwnedMemoryReservation,
    bytes: usize,
) -> Result<OwnedMemoryReservation> {
    let original_use = clone_persistence_original_writer(original)?;
    let original_handle = persistence_original_projection(original)?;
    crate::memory_accounting::reserve_native_storage_from_writer_use_for(
        &original_use,
        &original_handle,
        bytes,
        "pending_directory",
        HINT,
    )
    .map_err(|failure| original_storage_error(NativeStorageFailure::Refused(failure)))
}

/// Space a caller must leave available for the native receipts in its planned
/// pending directory. This quotes the producer's concrete control layout;
/// admission still happens once, on the original owner, when each credit exists.
pub(crate) fn pending_directory_receipt_quote(entries: usize) -> Option<usize> {
    Layout::new::<StorageReceipt>().size().checked_mul(entries)
}

struct StorageReceipt {
    owners: AtomicUsize,
    allocation: ManuallyDrop<StorageAllocation>,
    control: ManuallyDrop<StorageAllocation>,
}
fn retain(owners: &AtomicUsize) {
    owners
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            n.checked_add(1).filter(|n| *n <= isize::MAX as usize)
        })
        .expect("storage receipt strong count overflow");
}
unsafe fn retain_receipt(raw: NonNull<()>) {
    retain(&unsafe { raw.cast::<StorageReceipt>().as_ref() }.owners);
}
unsafe fn finish_receipt(raw: NonNull<()>) {
    let mut pointer = raw.cast::<StorageReceipt>();
    if unsafe { pointer.as_ref() }
        .owners
        .fetch_sub(1, Ordering::Release)
        != 1
    {
        return;
    }
    fence(Ordering::Acquire);
    let receipt = unsafe { pointer.as_mut() };
    let allocation = unsafe { ManuallyDrop::take(&mut receipt.allocation) };
    let control = unsafe { ManuallyDrop::take(&mut receipt.control) };
    unsafe { dealloc(pointer.as_ptr().cast(), Layout::new::<StorageReceipt>()) };
    // Both real invoices retain original use/child until the receipt is gone.
    drop(allocation);
    drop(control);
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn paid_receipt(
    scope: &StorageScope,
    layout: Layout,
) -> std::result::Result<NativeStorageCredit, NativeStorageFailure> {
    let allocation = scope.funding.reserve_scope(layout)?;
    let control = scope
        .funding
        .reserve_scope(Layout::new::<StorageReceipt>())?;
    let layout = Layout::new::<StorageReceipt>();
    let raw = unsafe { alloc(layout) }.cast::<StorageReceipt>();
    let Some(pointer) = NonNull::new(raw) else {
        return Err(NativeStorageFailure::AllocatorFailed {
            site: "native storage receipt",
            layout,
        });
    };
    unsafe {
        pointer.as_ptr().write(StorageReceipt {
            owners: AtomicUsize::new(1),
            allocation: ManuallyDrop::new(allocation),
            control: ManuallyDrop::new(control),
        });
    }
    Ok(unsafe {
        NativeStorageCredit::from_owned_receipt(pointer.cast(), retain_receipt, finish_receipt)
    })
}
struct StorageScope {
    // All error payloads and earlier scope owners die while this scope's
    // external exact-layout invoice still owns the original writer/child.
    error: Mutex<ScopeErrorCustody>,
    failure: Mutex<StorageFailureState>,
    funding: StorageFunding,
}
#[derive(Default)]
struct StorageFailureState {
    // A consumed funded scope keeps this set after moving its original cause
    // to the consumer. Reusable legacy scopes still reset between operations.
    refused: bool,
    original: Option<NativeStorageFailure>,
}
#[derive(Default)]
struct ScopeErrorCustody {
    // Primary diagnostics live outside every retained scope. A native view
    // can retain this scope, so this cell must never own that view as primary.
    phase: Option<crate::funded_error::OriginalStoragePhase>,
    secondary: Option<crate::funded_error::OriginalScopeCause>,
    // Scalar second cause; never owns a view or primary diagnostic cell.
    native_allocator: Option<(&'static str, Layout)>,
    discard_unwind: Option<Box<dyn std::any::Any + Send>>,
    previous: Option<crate::funded_error::OriginalScopeCustody>,
}
struct StorageScopeBlock {
    owners: AtomicUsize,
    payload: ManuallyDrop<StorageScope>,
    invoice: ManuallyDrop<StorageAllocation>,
}
struct ScopeFree {
    pointer: NonNull<StorageScopeBlock>,
    invoice: Option<StorageAllocation>,
}
impl Drop for ScopeFree {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.pointer.as_ptr().cast(),
                Layout::new::<StorageScopeBlock>(),
            )
        };
        drop(self.invoice.take());
    }
}
unsafe fn retain_scope(raw: NonNull<()>) {
    retain(&unsafe { raw.cast::<StorageScopeBlock>().as_ref() }.owners);
}
unsafe fn finish_scope(raw: NonNull<()>) {
    let mut pointer = raw.cast::<StorageScopeBlock>();
    if unsafe { pointer.as_ref() }
        .owners
        .fetch_sub(1, Ordering::Release)
        != 1
    {
        return;
    }
    fence(Ordering::Acquire);
    let block = unsafe { pointer.as_mut() };
    let invoice = unsafe { ManuallyDrop::take(&mut block.invoice) };
    let _free = ScopeFree {
        pointer,
        invoice: Some(invoice),
    };
    unsafe { ManuallyDrop::drop(&mut block.payload) };
}
unsafe fn admit_scope(
    raw: NonNull<()>,
    layout: Layout,
) -> std::result::Result<NativeStorageCredit, NativeStorageRefused> {
    let scope = &unsafe { raw.cast::<StorageScopeBlock>().as_ref() }.payload;
    // A refused operation cannot allocate again, even if its caller ignores the marker.
    if scope
        .failure
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .refused
    {
        return Err(NativeStorageRefused);
    }
    match paid_receipt(scope, layout) {
        Ok(receipt) => Ok(receipt),
        Err(original) => {
            let mut slot = scope.failure.lock().unwrap_or_else(|e| e.into_inner());
            if !slot.refused {
                slot.refused = true;
                slot.original = Some(original);
            }
            Err(NativeStorageRefused)
        }
    }
}

#[repr(transparent)]
pub(crate) struct StorageAdmissionScope {
    pointer: NonNull<StorageScopeBlock>,
}
// The scope payload is synchronized and its exact invoice has the same owned
// Send + Sync provenance as the NativeStorageAdmission it already exports.
unsafe impl Send for StorageAdmissionScope {}
unsafe impl Sync for StorageAdmissionScope {}
impl StorageAdmissionScope {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn writer_original(original: &OwnedMemoryReservation) -> Result<Self> {
        let original = reserve_original_bootstrap(original, 0)?;
        Self::new_unqualified(StorageFunding::Original(original))
    }
    /// Fund the complete error-custody backing before entering a native
    /// producer. Bootstrap refusals stay scalar, with the legacy Original's
    /// exact operation/hint and the same authenticated writer epoch.
    pub(super) fn writer_original_native(
        original: &OwnedMemoryReservation,
    ) -> std::result::Result<Self, NativeRefusal> {
        let (held, projection) = crate::memory_accounting::native_preparation_original(original)
            .map_err(NativeRefusal::Other)?;
        let original = crate::memory_accounting::reserve_native_storage_from_writer_use_for(
            &held,
            &projection,
            0,
            "pending_directory",
            HINT,
        )?;
        let layout = Layout::new::<StorageScopeBlock>();
        let invoice = crate::memory_accounting::reserve_native_storage_from_writer_use_for(
            &held,
            &projection,
            layout.size(),
            "pending_directory",
            HINT,
        )?;
        let raw = unsafe { alloc(layout) }.cast::<StorageScopeBlock>();
        let Some(pointer) = NonNull::new(raw) else {
            return Err(NativeRefusal::AllocatorFailed {
                site: "native storage scope",
                layout,
            });
        };
        unsafe {
            pointer.as_ptr().write(StorageScopeBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(StorageScope {
                    error: Mutex::new(ScopeErrorCustody::default()),
                    failure: Mutex::new(StorageFailureState::default()),
                    funding: StorageFunding::Original(original),
                }),
                invoice: ManuallyDrop::new(StorageAllocation::Writer(invoice)),
            });
        }
        Ok(Self { pointer })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_original_unqualified<R>(
        &self,
        operation: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        self.run_native(operation).map_err(original_storage_error)
    }

    /// The callback and native failure move in the SAME private error route.
    /// No helper/public-method return is an external ownership transfer.
    pub(crate) fn run_original<R>(
        self,
        phase: crate::funded_error::OriginalStoragePhase,
        operation: impl FnOnce() -> crate::funded_error::OriginalResult<R>,
    ) -> crate::funded_error::OriginalResult<R> {
        use crate::funded_error::{OriginalError, OriginalScopeCause};
        // This independent cell cannot be owned by the scope it may retain.
        // Refusal happens before callback entry, never after a cause exists.
        let diagnostic = crate::funded_error::PreparedDiagnostic::allocate_scope(&self)
            .map_err(OriginalError::native)?;
        let (result, original) = self.run_scoped(true, operation);
        let (cause, allocator) = match original {
            Some(NativeStorageFailure::Refused(refusal)) => {
                (Some(OriginalScopeCause::Refused(refusal)), None)
            }
            Some(NativeStorageFailure::Unqualified(error)) => {
                (Some(OriginalScopeCause::Unqualified(error)), None)
            }
            Some(NativeStorageFailure::AllocatorFailed { site, layout }) => {
                unsafe { self.pointer.as_ref() }
                    .payload
                    .error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .native_allocator = Some((site, layout));
                (None, Some((site, layout)))
            }
            None => (None, None),
        };
        match (result, cause) {
            (Err(error), cause) => {
                // A returned callback diagnosis is authoritative. The distinct
                // sticky refusal remains owned through the same consumer loan.
                // Retain the producer even without a sticky refusal: its native
                // controls belong to this error until its final consumer drops it.
                Err(self.attach_error(phase, error, cause, None))
            }
            (Ok(value), Some(cause)) => {
                // Ignoring an admission marker cannot turn failure into success.
                let error = match cause {
                    OriginalScopeCause::Refused(refusal) => OriginalError::native(refusal),
                    OriginalScopeCause::Unqualified(error) => diagnostic.unqualified(error),
                };
                let discard_unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    redb::with_native_storage_admission(self.retain_admission(), || drop(value))
                }))
                .err();
                Err(self.attach_error(phase, error, None, discard_unwind))
            }
            (Ok(value), None) => {
                let Some((site, layout)) = allocator else {
                    return Ok(value);
                };
                // All actual Original phases use this retained Original. The
                // cell was prepaid before the callback and the null producer.
                let StorageFunding::Original(original) =
                    &unsafe { self.pointer.as_ref() }.payload.funding
                else {
                    unreachable!("Original phases retain their Original funding")
                };
                let error = diagnostic.storage(
                    redb::Error::Native(redb::NativeColdReadError::AllocatorFailed {
                        site,
                        layout,
                    }),
                    original,
                );
                let discard_unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    redb::with_native_storage_admission(self.retain_admission(), || drop(value))
                }))
                .err();
                Err(self.attach_error(phase, error, None, discard_unwind))
            }
        }
    }

    fn attach_error(
        self,
        phase: crate::funded_error::OriginalStoragePhase,
        mut error: crate::funded_error::OriginalError,
        secondary: Option<crate::funded_error::OriginalScopeCause>,
        discard_unwind: Option<Box<dyn std::any::Any + Send>>,
    ) -> crate::funded_error::OriginalError {
        if let Some(previous) = &error.storage {
            assert!(
                !previous.scope.contains_error_phase(phase),
                "one entry of each funded storage phase"
            );
        }
        {
            let mut custody = unsafe { self.pointer.as_ref() }
                .payload
                .error
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            assert!(custody.phase.is_none(), "one failure per producing scope");
            custody.phase = Some(phase);
            custody.secondary = secondary;
            custody.discard_unwind = discard_unwind;
            custody.previous = error.storage.take();
        }
        error.storage = Some(crate::funded_error::OriginalScopeCustody { scope: self });
        error
    }

    fn contains_error_phase(&self, phase: crate::funded_error::OriginalStoragePhase) -> bool {
        let custody = unsafe { self.pointer.as_ref() }
            .payload
            .error
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        custody.phase == Some(phase)
            || custody
                .previous
                .as_ref()
                .is_some_and(|previous| previous.scope.contains_error_phase(phase))
    }

    pub(crate) fn error_source_is_qualified(&self) -> bool {
        if matches!(
            &unsafe { self.pointer.as_ref() }
                .payload
                .failure
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .original,
            Some(NativeStorageFailure::Unqualified(_))
        ) {
            return false;
        }
        let custody = unsafe { self.pointer.as_ref() }
            .payload
            .error
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        custody.discard_unwind.is_none()
            && !matches!(
                &custody.secondary,
                Some(crate::funded_error::OriginalScopeCause::Unqualified(_))
            )
            && custody
                .previous
                .as_ref()
                .is_none_or(|previous| previous.scope.error_source_is_qualified())
    }

    pub(crate) fn reserve_diagnostic(
        &self,
        layout: Layout,
    ) -> std::result::Result<StorageAllocation, NativeRefusal> {
        // Fund the independent carrier from the scope's payer, without making
        // that carrier a child owned by the scope itself.
        unsafe { self.pointer.as_ref() }
            .payload
            .funding
            .reserve_native(layout)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn new_unqualified(funding: StorageFunding) -> Result<Self> {
        let layout = Layout::new::<StorageScopeBlock>();
        // Original bootstrap uses scalar admission and borrowed diagnostics.
        // Reader adapters retain their existing kernel. The exact cell invoice
        // remains outside the allocation until finish_scope physically frees it.
        let invoice = funding.reserve_unqualified(layout)?;
        let raw = unsafe { alloc(layout) }.cast::<StorageScopeBlock>();
        let Some(pointer) = NonNull::new(raw) else {
            handle_alloc_error(layout)
        };
        unsafe {
            pointer.as_ptr().write(StorageScopeBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(StorageScope {
                    error: Mutex::new(ScopeErrorCustody::default()),
                    funding,
                    failure: Mutex::new(StorageFailureState::default()),
                }),
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { pointer })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reader_operation(
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Self, NativeStorageFailure> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        if memory.native_reserve_supported() {
            Self::reader_native_owner(memory)
        } else {
            Self::new_unqualified(StorageFunding::ReadUnqualified(memory))
                .map_err(NativeStorageFailure::Unqualified)
        }
    }

    /// Use the executing begin_request child, never acquire another child or
    /// recover a Legacy budget. The scope's exact physical cell is prepaid.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reader_native_owner(
        owner: ReadMemoryOwner,
    ) -> std::result::Result<Self, NativeStorageFailure> {
        let funding = StorageFunding::Read(owner);
        let layout = Layout::new::<StorageScopeBlock>();
        let invoice = funding
            .reserve_native(layout)
            .map_err(NativeStorageFailure::Refused)?;
        let raw = unsafe { alloc(layout) }.cast::<StorageScopeBlock>();
        let pointer = NonNull::new(raw).ok_or(NativeStorageFailure::AllocatorFailed {
            site: "native reader scope",
            layout,
        })?;
        // No fallible step after allocation: final scope release destroys its
        // payload, frees this cell, and only then refunds the original child.
        unsafe {
            pointer.as_ptr().write(StorageScopeBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(StorageScope {
                    error: Mutex::new(ScopeErrorCustody::default()),
                    funding,
                    failure: Mutex::new(StorageFailureState::default()),
                }),
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { pointer })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_native<R>(
        &self,
        operation: impl FnOnce() -> Result<R>,
    ) -> std::result::Result<R, NativeStorageFailure> {
        let (result, original) = self.run_scoped(false, operation);
        if let Some(original) = original {
            return Err(original);
        }
        result.map_err(NativeStorageFailure::Unqualified)
    }
    fn run_scoped<R, E>(
        &self,
        retain_refusal: bool,
        operation: impl FnOnce() -> std::result::Result<R, E>,
    ) -> (std::result::Result<R, E>, Option<NativeStorageFailure>) {
        unsafe { retain_scope(self.pointer.cast()) };
        let admission = unsafe {
            observe_allocation_funding(
                NativeStorageAdmission::from_owned_receipt(
                    self.pointer.cast(),
                    retain_scope,
                    finish_scope,
                    admit_scope,
                )
                .with_failure_inspector(inspect_scope_failure),
            )
        };
        let result = redb::with_native_storage_admission(admission, operation);
        let scope = &unsafe { self.pointer.as_ref() }.payload;
        let mut failure = scope.failure.lock().unwrap_or_else(|e| e.into_inner());
        let original = failure.original.take();
        if !retain_refusal {
            // run_native borrows its scope for one operation. Preserve its
            // existing ability to reuse that scope for a subsequent operation.
            failure.refused = false;
        }
        (result, original)
    }
}
impl Drop for StorageAdmissionScope {
    fn drop(&mut self) {
        unsafe { finish_scope(self.pointer.cast()) }
    }
}

/// Keep the actual native operation and numeric refusal intact for traces.
// Only the row-writing boundary supplies the user-facing table-root phase.
pub(super) fn transaction_row_root_refusal(mut error: Error, ws: &contextdb_tx::WriteSet) -> Error {
    if let Error::MemoryBudgetExceeded {
        subsystem,
        operation,
        hint,
        ..
    } = &mut error
        && subsystem.as_ref() == "redb"
        && operation.as_ref() == "pending_directory"
    {
        // The existing admission sidecar already has one key per inserted
        // table. Mixed/delete-only writes retain the truthful phase without
        // inventing a table count or allocating another directory on refusal.
        let tables = (ws.relational_deletes.is_empty() && !ws.relational_inserts.is_empty())
            .then_some(ws.relational_insert_admitted_bytes.len())
            .filter(|count| *count != 0);
        *hint = contextdb_core::error::DiagnosticLabel::transaction_row_roots_hint(tables);
    }
    error
}

// Scalar descriptor only; does not replace WriterSnapshot or adopt its schema.
#[derive(Clone, Copy)]
pub(crate) struct ColdSnapshotMetadata {
    pub(crate) identity: redb::StorageSnapshotIdentity,
    pub(crate) snapshot: SnapshotId,
    pub(crate) lsn: Lsn,
}

// Native failures retain only scalar/static data. Inherited allocating Errors
// keep their original owner; this conversion does not certify their provenance.
pub(crate) fn original_storage_error(failure: NativeStorageFailure) -> Error {
    match failure {
        NativeStorageFailure::Unqualified(error) => error,
        NativeStorageFailure::AllocatorFailed { site, layout } => {
            Error::NativeEngineOther(NativeRefusal::AllocatorFailed { site, layout })
        }
        NativeStorageFailure::Refused(NativeRefusal::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        }) => Error::MemoryBudgetExceeded {
            subsystem: subsystem.into(),
            operation: operation.into(),
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint: hint.into(),
        },
        NativeStorageFailure::Refused(NativeRefusal::Other(reason)) => Error::Other(reason.into()),
        // These retain the existing Other classification and exact Display,
        // without allocating text while the original admission is refusing.
        NativeStorageFailure::Refused(other) => Error::NativeEngineOther(other),
    }
}

/// Errors cross preparation without text conversion. `Lower`
/// retains the producer value unchanged; its pre-existing diagnostic funding
/// is NOT established by this carrier. The funded-error consumer must join it
/// before exposing an active queue route. `Refused` has no owned diagnostic.
#[derive(Debug)]
pub(crate) enum PreparedReadCause {
    /// Existing Error::Other diagnosis, retained before allocating its text.
    EngineStatic(&'static str),
    Lower(redb::PreparedReadError),
    // Unknown future commit variants remain their original unqualified cause.
    Commit(redb::CommitError),
    Refused(NativeRefusal),
    Unqualified(Error),
}

/// Move-only result custody. Keep this entire value through abort/retirement
/// and the consumer's result loan. There is deliberately no bare Error escape.
pub(crate) struct PreparedReadFailure {
    pub(crate) cause: PreparedReadCause,
    // If a callback produces a real error after a refused allocation, retain
    // BOTH values rather than allowing sticky admission to replace its error.
    pub(crate) admission: Option<NativeStorageFailure>,
    _scope: Option<StorageAdmissionScope>,
    _original: Option<WriterUse>,
}
impl PreparedReadFailure {
    pub(super) fn static_engine(reason: &'static str) -> Self {
        Self {
            cause: PreparedReadCause::EngineStatic(reason),
            admission: None,
            _scope: None,
            _original: None,
        }
    }

    /// Consume a cell prepared BEFORE read/commit entry. No formatting, source
    /// copy, admission or error Box construction occurs after the producer fails.
    pub(super) fn into_original(
        self,
        diagnostic: crate::funded_error::PreparedDiagnostic,
        original: &OwnedMemoryReservation,
    ) -> crate::funded_error::OriginalError {
        let Self {
            cause,
            admission,
            _scope,
            _original,
        } = self;
        let error = match cause {
            PreparedReadCause::EngineStatic(reason) => {
                diagnostic.unqualified(Error::Other(reason.into()))
            }
            PreparedReadCause::Refused(refusal) => {
                crate::funded_error::OriginalError::native(refusal)
            }
            PreparedReadCause::Unqualified(error) => diagnostic.unqualified(error),
            PreparedReadCause::Lower(error) => {
                diagnostic.storage(redb::Error::PreparedRead(error), original)
            }
            PreparedReadCause::Commit(error) => diagnostic.storage(error.into(), original),
        };
        // The primary error is never put into the scope it can retain through
        // its native descriptor/page. The compact carrier owns both separately.
        match _scope {
            Some(scope) => scope.attach_prepared_error(error, admission),
            None => {
                debug_assert!(admission.is_none());
                error
            }
        }
    }
}

impl StorageAdmissionScope {
    /// Authenticate the supplied Original before constructing the scope. Scope
    /// and per-allocation receipt controls use its scalar admission kernel.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepared_original(
        original: &OwnedMemoryReservation,
    ) -> std::result::Result<Self, PreparedReadFailure> {
        let (original_use, original_handle) =
            crate::memory_accounting::native_preparation_original(original)
                .map_err(PreparedReadFailure::static_engine)?;
        let funding = StorageFunding::PreparedWriter {
            original_use,
            original_handle,
        };
        let layout = Layout::new::<StorageScopeBlock>();
        let invoice = match funding.reserve_native(layout) {
            Ok(invoice) => invoice,
            Err(refusal) => {
                let StorageFunding::PreparedWriter { original_use, .. } = funding else {
                    unreachable!()
                };
                return Err(PreparedReadFailure {
                    cause: PreparedReadCause::Refused(refusal),
                    admission: None,
                    _scope: None,
                    _original: Some(original_use),
                });
            }
        };
        let raw = unsafe { alloc(layout) }.cast::<StorageScopeBlock>();
        let Some(pointer) = NonNull::new(raw) else {
            // This is allocator failure, not a fabricated budget refusal.
            let StorageFunding::PreparedWriter { original_use, .. } = funding else {
                unreachable!()
            };
            return Err(PreparedReadFailure {
                cause: PreparedReadCause::Lower(redb::PreparedReadError::Native(
                    redb::NativeColdReadError::AllocatorFailed {
                        site: "prepared storage scope",
                        layout,
                    },
                )),
                admission: None,
                _scope: None,
                _original: Some(original_use),
            });
        };
        unsafe {
            pointer.as_ptr().write(StorageScopeBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(StorageScope {
                    error: Mutex::new(ScopeErrorCustody::default()),
                    funding,
                    failure: Mutex::new(StorageFailureState::default()),
                }),
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { pointer })
    }

    pub(super) fn reserve_prepared_control(
        &self,
        bytes: usize,
    ) -> std::result::Result<OwnedMemoryReservation, NativeRefusal> {
        let scope = &unsafe { self.pointer.as_ref() }.payload;
        let StorageFunding::PreparedWriter {
            original_use,
            original_handle,
        } = &scope.funding
        else {
            unreachable!("prepared controls keep their exact Original")
        };
        crate::memory_accounting::reserve_native_storage_from_writer_use(
            original_use,
            original_handle,
            bytes,
        )
    }

    /// A fixed external queue phase retains this already admitted owner on
    /// unwind too; it never rediscovers the current scope or an Original.
    pub(crate) fn retain_admission(&self) -> NativeStorageAdmission {
        unsafe { retain_scope(self.pointer.cast()) };
        unsafe {
            observe_allocation_funding(
                NativeStorageAdmission::from_owned_receipt(
                    self.pointer.cast(),
                    retain_scope,
                    finish_scope,
                    admit_scope,
                )
                .with_failure_inspector(inspect_scope_failure),
            )
        }
    }

    fn retain_scope_owner(&self) -> Self {
        unsafe { retain_scope(self.pointer.cast()) };
        Self {
            pointer: self.pointer,
        }
    }

    /// Retain preparation custody without certifying a synthetic phase. The
    /// primary stays in its separate prepaid cell, outside scopes held by views.
    fn attach_prepared_error(
        self,
        mut error: crate::funded_error::OriginalError,
        admission: Option<NativeStorageFailure>,
    ) -> crate::funded_error::OriginalError {
        let scope = &unsafe { self.pointer.as_ref() }.payload;
        if let Some(admission) = admission {
            let mut failure = scope.failure.lock().unwrap_or_else(|e| e.into_inner());
            assert!(
                failure.original.is_none(),
                "one retained native secondary cause"
            );
            failure.original = Some(admission);
        }
        {
            let mut custody = scope.error.lock().unwrap_or_else(|e| e.into_inner());
            assert!(
                custody.phase.is_none() && custody.previous.is_none(),
                "one prepared failure"
            );
            custody.previous = error.storage.take();
        }
        error.storage = Some(crate::funded_error::OriginalScopeCustody { scope: self });
        error
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_read<T>(
        &self,
        operation: impl FnOnce(
            &Self,
            NativeStorageAdmission,
        ) -> std::result::Result<T, PreparedReadCause>,
    ) -> std::result::Result<T, PreparedReadFailure> {
        unsafe { retain_scope(self.pointer.cast()) };
        let admission = unsafe {
            observe_allocation_funding(
                NativeStorageAdmission::from_owned_receipt(
                    self.pointer.cast(),
                    retain_scope,
                    finish_scope,
                    admit_scope,
                )
                .with_failure_inspector(inspect_scope_failure),
            )
        };
        // The lower API installs this owned scope before verified-root reads.
        // No formatting, boxing or losing conversion occurs on either exit.
        let result = operation(self, admission).map_err(|cause| match cause {
            PreparedReadCause::Lower(redb::PreparedReadError::Native(
                redb::NativeColdReadError::AdmissionRefusedAt(producer),
            )) => {
                if producer.same_source(&self.retain_admission()) {
                    // The normal match below moves this scope's actual typed cause.
                    PreparedReadCause::Lower(redb::PreparedReadError::Native(
                        redb::NativeColdReadError::AdmissionRefused,
                    ))
                } else {
                    let actual = producer.inspect_failure(|cause| {
                        cause
                            .and_then(|cause| cause.downcast_ref::<NativeStorageFailure>())
                            .and_then(|cause| match cause {
                                NativeStorageFailure::Refused(refusal) => {
                                    Some(PreparedReadCause::Refused(*refusal))
                                }
                                NativeStorageFailure::AllocatorFailed { site, layout } => {
                                    Some(PreparedReadCause::Lower(redb::PreparedReadError::Native(
                                        redb::NativeColdReadError::AllocatorFailed {
                                            site,
                                            layout: *layout,
                                        },
                                    )))
                                }
                                NativeStorageFailure::Unqualified(_) => None,
                            })
                    });
                    actual.unwrap_or(PreparedReadCause::Lower(redb::PreparedReadError::Native(
                        redb::NativeColdReadError::AdmissionRefusedAt(producer),
                    )))
                }
            }
            cause => cause,
        });
        let refused = unsafe { self.pointer.as_ref() }
            .payload
            .failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original
            .take();
        match (result, refused) {
            (Ok(value), None) => Ok(value),
            (
                Err(PreparedReadCause::Lower(redb::PreparedReadError::Native(
                    redb::NativeColdReadError::AdmissionRefused,
                ))),
                Some(refusal),
            ) => {
                let cause = match refusal {
                    NativeStorageFailure::Refused(r) => PreparedReadCause::Refused(r),
                    NativeStorageFailure::AllocatorFailed { site, layout } => {
                        PreparedReadCause::Lower(redb::PreparedReadError::Native(
                            redb::NativeColdReadError::AllocatorFailed { site, layout },
                        ))
                    }
                    NativeStorageFailure::Unqualified(e) => PreparedReadCause::Unqualified(e),
                };
                Err(PreparedReadFailure {
                    cause,
                    admission: None,
                    _scope: Some(self.retain_scope_owner()),
                    _original: None,
                })
            }
            (Err(cause), admission) => Err(PreparedReadFailure {
                cause,
                admission,
                _scope: Some(self.retain_scope_owner()),
                _original: None,
            }),
            (Ok(value), Some(refusal)) => {
                // A caller that ignored a marker cannot publish its result.
                // Keep the genuine refusal alive while retiring that value.
                let cause = match refusal {
                    NativeStorageFailure::Refused(r) => PreparedReadCause::Refused(r),
                    NativeStorageFailure::AllocatorFailed { site, layout } => {
                        PreparedReadCause::Lower(redb::PreparedReadError::Native(
                            redb::NativeColdReadError::AllocatorFailed { site, layout },
                        ))
                    }
                    NativeStorageFailure::Unqualified(e) => PreparedReadCause::Unqualified(e),
                };
                if let Err(secondary) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        redb::with_native_storage_admission(self.retain_admission(), || drop(value))
                    }))
                {
                    // The scope is still owned here. Retain the actual panic
                    // payload in its prepaid custody cell instead of leaking it
                    // or allocating another carrier after the native refusal.
                    unsafe { self.pointer.as_ref() }
                        .payload
                        .error
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .discard_unwind = Some(secondary);
                }
                Err(PreparedReadFailure {
                    cause,
                    admission: None,
                    _scope: Some(self.retain_scope_owner()),
                    _original: None,
                })
            }
        }
    }
}

#[cfg(test)]
impl StorageAdmissionScope {
    pub(crate) fn receipt_layout_for_scan_test() -> Layout {
        Layout::new::<StorageReceipt>()
    }
    pub(crate) fn allocation_observation_for_scan_test(&self) -> (usize, Layout) {
        (
            self.pointer.as_ptr() as usize,
            Layout::new::<StorageScopeBlock>(),
        )
    }
}

/// Borrow the actual original invoice of this module's native storage credit.
///
/// # Safety
/// The supplied credit must have been created by `paid_receipt` and remain live.
#[cfg(test)]
pub(crate) unsafe fn original_invoice_for_test(credit: &NativeStorageCredit) -> (usize, usize) {
    let receipt = unsafe { &*(credit.identity_for_test() as *const StorageReceipt) };
    match &*receipt.allocation {
        StorageAllocation::Writer(original) => {
            (original.accountant().as_ptr() as usize, original.bytes())
        }
        _ => panic!("the pending name uses its original writer invoice"),
    }
}

// All native admission constructors pass through this same observation seam.
unsafe fn observe_allocation_funding(admission: NativeStorageAdmission) -> NativeStorageAdmission {
    #[cfg(feature = "test-seams")]
    let admission =
        unsafe { admission.with_allocation_funding_inspector(inspect_scope_allocation_funding) };
    admission
}

#[cfg(all(test, feature = "test-seams"))]
#[path = "coexistence_native_writer_cap_guard.rs"]
mod coexistence_native_writer_cap_guard;
