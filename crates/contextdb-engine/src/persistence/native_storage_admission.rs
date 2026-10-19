//! Private exact-layout storage receipts with scalar admission refusal egress.
//! Original bootstrap refusals borrow their static diagnostics.
//! Joined Original callbacks move private diagnostic custody; legacy callbacks
//! and lower producers retain their explicit unqualified coverage.
use crate::memory_accounting::{
    AccountantHandle, ControlCredit, OwnedMemoryReservation, StoreAccountantHandle, WriterUse,
    clone_persistence_original_writer, persistence_original_projection,
    validate_owned_original,
};
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
    Unqualified(Error),
}
impl std::fmt::Display for NativeStorageFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => std::fmt::Display::fmt(refusal, f),
            Self::Unqualified(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

enum StorageAllocation {
    Writer(OwnedMemoryReservation),
    Read(ReadCredit),
    Store(ControlCredit),
}
enum StorageFunding {
    Original(OwnedMemoryReservation),
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
    fn reserve_unqualified(&self, layout: Layout) -> Result<StorageAllocation> {
        match self {
            Self::Original(original) => {
                reserve_original_bootstrap(original, layout.size())
                    .map(StorageAllocation::Writer)
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
            } | Self::PreparedWriter { original_use, original_handle } => crate::memory_accounting::reserve_native_storage_from_writer_use(
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
        return Err(NativeStorageFailure::Refused(NativeRefusal::Other(
            "native storage receipt allocation failed",
        )));
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
    funding: StorageFunding,
    failure: Mutex<Option<NativeStorageFailure>>,
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
        .is_some()
    {
        return Err(NativeStorageRefused);
    }
    match paid_receipt(scope, layout) {
        Ok(receipt) => Ok(receipt),
        Err(original) => {
            let mut slot = scope.failure.lock().unwrap_or_else(|e| e.into_inner());
            if slot.is_none() {
                *slot = Some(original);
            }
            Err(NativeStorageRefused)
        }
    }
}

pub(crate) struct StorageAdmissionScope {
    pointer: NonNull<StorageScopeBlock>,
}
impl StorageAdmissionScope {
    pub(super) fn writer_original(original: &OwnedMemoryReservation) -> Result<Self> {
        let original = reserve_original_bootstrap(original, 0)?;
        Self::new_unqualified(StorageFunding::Original(original))
    }
    pub(super) fn run_original_unqualified<R>(&self, operation: impl FnOnce() -> Result<R>) -> Result<R> {
        self.run_native(operation).map_err(original_storage_error)
    }

    /// The callback and native failure move in the SAME private error route.
    /// No helper/public-method return is an external ownership transfer.
    pub(crate) fn run_original<R>(
        self,
        phase: crate::funded_error::OriginalStoragePhase,
        operation: impl FnOnce() -> crate::funded_error::OriginalResult<R>,
    ) -> crate::funded_error::OriginalResult<R> {
        use crate::funded_error::{OriginalError, OriginalScopeCause, OriginalScopeCustody};
        let (result, original) = self.run_scoped(operation);
        let Some(original) = original else { return result; };
        let cause = match original {
            NativeStorageFailure::Refused(refusal) => OriginalScopeCause::Refused(refusal),
            NativeStorageFailure::Unqualified(error) => OriginalScopeCause::Unqualified(error),
        };
        let mut custody = OriginalScopeCustody {
            secondary: None, discard_unwind: None, _scope: self.into_admission(),
        };
        match result {
            Err(error) => {
                // A returned callback diagnosis is authoritative. The distinct
                // sticky refusal remains owned through the same consumer loan.
                custody.secondary = Some(cause);
                Err(error.retain_storage_phase(phase, custody))
            }
            Ok(value) => {
                // Ignoring an admission marker cannot turn failure into success.
                let error = match cause {
                    OriginalScopeCause::Refused(refusal) => OriginalError::native(refusal),
                    OriginalScopeCause::Unqualified(error) => OriginalError::unqualified(error),
                };
                custody.discard_unwind = std::panic::catch_unwind(
                    std::panic::AssertUnwindSafe(|| drop(value))).err();
                Err(error.retain_storage_phase(phase, custody))
            }
        }
    }

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
                    funding,
                    failure: Mutex::new(None),
                }),
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { pointer })
    }
    pub(super) fn writer_unqualified(
        original: &OwnedMemoryReservation,
        expected: &AccountantHandle,
        funder: &SourceFunder,
    ) -> Result<Self> {
        validate_owned_original(original, expected, funder)?;
        Self::new_unqualified(StorageFunding::Writer {
            original_use: clone_persistence_original_writer(original)?,
            original_handle: persistence_original_projection(original)?,
            genuine_funder: funder.clone(),
        })
    }
    pub(super) fn reader_unqualified(
        receipt: crate::vector_read_memory::NativeReadChildReceipt,
    ) -> Result<Self> {
        Self::new_unqualified(StorageFunding::Read(receipt.into_owner()))
    }
    pub(super) fn reader_operation(
        memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<Self, NativeStorageFailure> {
        if memory.native_reserve_supported() {
            Self::reader_native_owner(ReadMemoryOwner::from_legacy(memory))
        } else {
            Self::new_unqualified(StorageFunding::ReadUnqualified(
                ReadMemoryOwner::from_legacy(memory),
            ))
            .map_err(NativeStorageFailure::Unqualified)
        }
    }

    /// Use the executing begin_request child, never acquire another child or
    /// recover a Legacy budget. The scope's exact physical cell is prepaid.
    pub(super) fn reader_native_owner(
        owner: ReadMemoryOwner,
    ) -> std::result::Result<Self, NativeStorageFailure> {
        let funding = StorageFunding::Read(owner);
        let layout = Layout::new::<StorageScopeBlock>();
        let invoice = funding
            .reserve_native(layout)
            .map_err(NativeStorageFailure::Refused)?;
        let raw = unsafe { alloc(layout) }.cast::<StorageScopeBlock>();
        let pointer = NonNull::new(raw).ok_or(NativeStorageFailure::Refused(
            NativeRefusal::Other("native storage scope allocation failed"),
        ))?;
        // No fallible step after allocation: final scope release destroys its
        // payload, frees this cell, and only then refunds the original child.
        unsafe {
            pointer.as_ptr().write(StorageScopeBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(StorageScope {
                    funding,
                    failure: Mutex::new(None),
                }),
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { pointer })
    }

    pub(super) fn run_native<R>(
        &self,
        operation: impl FnOnce() -> Result<R>,
    ) -> std::result::Result<R, NativeStorageFailure> {
        let (result, original) = self.run_scoped(operation);
        if let Some(original) = original { return Err(original); }
        result.map_err(NativeStorageFailure::Unqualified)
    }
    fn run_scoped<R, E>(
        &self,
        operation: impl FnOnce() -> std::result::Result<R, E>,
    ) -> (std::result::Result<R, E>, Option<NativeStorageFailure>) {
        unsafe { retain_scope(self.pointer.cast()) };
        let admission = unsafe {
            NativeStorageAdmission::from_owned_receipt(
                self.pointer.cast(), retain_scope, finish_scope, admit_scope,
            )
        };
        let result = redb::with_native_storage_admission(admission, operation);
        let scope = &unsafe { self.pointer.as_ref() }.payload;
        let original = scope.failure.lock().unwrap_or_else(|e| e.into_inner()).take();
        (result, original)
    }
}
impl Drop for StorageAdmissionScope {
    fn drop(&mut self) {
        unsafe { finish_scope(self.pointer.cast()) }
    }
}

/// Scalar descriptor only; does not replace WriterSnapshot or adopt its schema.
#[derive(Clone, Copy)]
pub(crate) struct ColdSnapshotMetadata {
    pub(crate) identity: redb::StorageSnapshotIdentity,
    pub(crate) snapshot: SnapshotId,
    pub(crate) lsn: Lsn,
}
struct ColdMetadataBlock {
    owners: AtomicUsize,
    metadata: ColdSnapshotMetadata,
    invoice: ManuallyDrop<ControlCredit>,
}
pub(super) struct ColdMetadataOwner {
    pointer: NonNull<ColdMetadataBlock>,
}
unsafe impl Send for ColdMetadataOwner {}
unsafe impl Sync for ColdMetadataOwner {}
impl ColdMetadataOwner {
    pub(super) fn new_unqualified(
        accountant: &AccountantHandle,
        metadata: ColdSnapshotMetadata,
    ) -> Result<Self> {
        let original_untagged = StoreAccountantHandle::from_original_nonwriter(accountant);
        let layout = Layout::new::<ColdMetadataBlock>();
        let invoice = ControlCredit::admit_from_handle(
            &original_untagged,
            layout.size(),
            "redb",
            "cold_snapshot_metadata",
            "Raise the store memory limit for this snapshot descriptor.",
        )?;
        let raw = unsafe { alloc(layout) }.cast::<ColdMetadataBlock>();
        let Some(pointer) = NonNull::new(raw) else {
            handle_alloc_error(layout)
        };
        unsafe {
            pointer.as_ptr().write(ColdMetadataBlock {
                owners: AtomicUsize::new(1),
                metadata,
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { pointer })
    }
    pub(super) fn get(&self) -> ColdSnapshotMetadata {
        unsafe { self.pointer.as_ref() }.metadata
    }
}
impl Clone for ColdMetadataOwner {
    fn clone(&self) -> Self {
        retain(&unsafe { self.pointer.as_ref() }.owners);
        Self {
            pointer: self.pointer,
        }
    }
}
impl Drop for ColdMetadataOwner {
    fn drop(&mut self) {
        if unsafe { self.pointer.as_ref() }
            .owners
            .fetch_sub(1, Ordering::Release)
            != 1
        {
            return;
        }
        fence(Ordering::Acquire);
        let invoice = unsafe { ManuallyDrop::take(&mut self.pointer.as_mut().invoice) };
        unsafe {
            dealloc(
                self.pointer.as_ptr().cast(),
                Layout::new::<ColdMetadataBlock>(),
            )
        };
        drop(invoice);
    }
}

/// Stack-held physical name with exact backing and external invoice.
struct StorageName {
    bytes: ManuallyDrop<Vec<u8>>,
    invoice: Option<NativeStorageCredit>,
}
impl Drop for StorageName {
    fn drop(&mut self) {
        let invoice = self.invoice.take();
        unsafe { ManuallyDrop::drop(&mut self.bytes) };
        drop(invoice);
    }
}
impl StorageAdmissionScope {
    pub(super) fn with_rel_name<R>(
        &self,
        logical: &str,
        operation: impl FnOnce(&str) -> Result<std::result::Result<R, redb::NativeColdReadError>>,
    ) -> Result<std::result::Result<R, redb::NativeColdReadError>> {
        let Some(len) = logical.len().checked_add(4) else {
            return Ok(Err(redb::NativeColdReadError::Corrupt(
                redb::NativeColdFault {
                    reason: "physical table name length overflow",
                    page: [0; 8],
                    offset: 0,
                    expected: 0,
                    actual: 0,
                },
            )));
        };
        let Ok(layout) = Layout::array::<u8>(len) else {
            return Ok(Err(redb::NativeColdReadError::Corrupt(
                redb::NativeColdFault {
                    reason: "physical table name layout overflow",
                    page: [0; 8],
                    offset: 0,
                    expected: 0,
                    actual: 0,
                },
            )));
        };
        // The native admission marker is recovered as its original scalar by
        // run_native before the lower result is inspected. A sticky refusal
        // also prevents a later physical-name allocation in this scope.
        let invoice = match unsafe { admit_scope(self.pointer.cast(), layout) } {
            Ok(invoice) => invoice,
            Err(_) => return Ok(Err(redb::NativeColdReadError::AdmissionRefused)),
        };
        let raw = unsafe { alloc(layout) };
        let Some(pointer) = NonNull::new(raw) else {
            handle_alloc_error(layout)
        };
        // No fallible callback between alloc and initialized owner construction.
        unsafe {
            std::ptr::copy_nonoverlapping(b"rel_".as_ptr(), pointer.as_ptr(), 4);
            std::ptr::copy_nonoverlapping(logical.as_ptr(), pointer.as_ptr().add(4), logical.len());
        }
        let name = StorageName {
            bytes: ManuallyDrop::new(unsafe { Vec::from_raw_parts(pointer.as_ptr(), len, len) }),
            invoice: Some(invoice),
        };
        let name_str =
            std::str::from_utf8(&name.bytes).expect("prefix and logical table are UTF-8");
        operation(name_str)
    }
}

// Scalar budget/Other labels borrow their static backing and need no invoice.
// Allocating body and dynamic-display fallbacks remain explicitly unqualified;
// a bare Error has no slot for their physical-lifetime invoices.
pub(super) fn original_storage_error(failure: NativeStorageFailure) -> Error {
    match failure {
        NativeStorageFailure::Unqualified(error) => error,
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
        NativeStorageFailure::Refused(NativeRefusal::NativeUnsupported {
            subsystem,
            operation,
        }) => Error::Other(
            contextdb_core::read_memory::native_unsupported_full_display(subsystem, operation),
        ),
        NativeStorageFailure::Refused(other) => Error::Other((other.to_string()).into()),
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
    _scope: Option<NativeStorageAdmission>,
    _original: Option<WriterUse>,
}
impl PreparedReadFailure {
    pub(super) fn static_engine(reason: &'static str) -> Self {
        Self { cause: PreparedReadCause::EngineStatic(reason), admission: None,
            _scope: None, _original: None }
    }
}

impl StorageAdmissionScope {
    /// Authenticate the supplied Original before constructing the scope. Scope
    /// and per-allocation receipt controls use its scalar admission kernel.
    pub(super) fn prepared_original(
        original: &OwnedMemoryReservation,
    ) -> std::result::Result<Self, PreparedReadFailure> {
        let (original_use, original_handle) = crate::memory_accounting::native_preparation_original(original)
            .map_err(PreparedReadFailure::static_engine)?;
        let funding = StorageFunding::PreparedWriter { original_use, original_handle };
        let layout = Layout::new::<StorageScopeBlock>();
        let invoice = match funding.reserve_native(layout) {
            Ok(invoice) => invoice,
            Err(refusal) => {
                let StorageFunding::PreparedWriter { original_use, .. } = funding else { unreachable!() };
                return Err(PreparedReadFailure { cause: PreparedReadCause::Refused(refusal),
                    admission: None, _scope: None, _original: Some(original_use) });
            }
        };
        let raw = unsafe { alloc(layout) }.cast::<StorageScopeBlock>();
        let Some(pointer) = NonNull::new(raw) else {
            // This is allocator failure, not a fabricated budget refusal.
            let StorageFunding::PreparedWriter { original_use, .. } = funding else { unreachable!() };
            return Err(PreparedReadFailure {
                cause: PreparedReadCause::Lower(redb::PreparedReadError::Native(
                    redb::NativeColdReadError::Io(std::io::ErrorKind::OutOfMemory.into()))),
                admission: None, _scope: None, _original: Some(original_use),
            });
        };
        unsafe {
            pointer.as_ptr().write(StorageScopeBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(StorageScope { funding, failure: Mutex::new(None) }),
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
        let StorageFunding::PreparedWriter { original_use, original_handle } = &scope.funding
            else { unreachable!("prepared controls keep their exact Original") };
        crate::memory_accounting::reserve_native_storage_from_writer_use(original_use, original_handle, bytes)
    }

    /// A fixed external queue phase retains this already admitted owner on
    /// unwind too; it never rediscovers the current scope or an Original.
    pub(crate) fn retain_admission(&self) -> NativeStorageAdmission {
        unsafe { retain_scope(self.pointer.cast()) };
        unsafe { NativeStorageAdmission::from_owned_receipt(self.pointer.cast(),
            retain_scope, finish_scope, admit_scope) }
    }

    fn into_admission(self) -> NativeStorageAdmission {
        let scope = ManuallyDrop::new(self);
        // Move this existing strong owner into the already Send + Sync lower
        // transport. No retain/release window, allocation or new payer exists.
        unsafe {
            NativeStorageAdmission::from_owned_receipt(scope.pointer.cast(),
                retain_scope, finish_scope, admit_scope)
        }
    }

    pub(super) fn prepare_read<T>(
        self,
        operation: impl FnOnce(&Self, NativeStorageAdmission) -> std::result::Result<T, PreparedReadCause>,
    ) -> std::result::Result<T, PreparedReadFailure> {
        unsafe { retain_scope(self.pointer.cast()) };
        let admission = unsafe {
            NativeStorageAdmission::from_owned_receipt(
                self.pointer.cast(), retain_scope, finish_scope, admit_scope,
            )
        };
        // The lower API installs this owned scope before verified-root reads.
        // No formatting, boxing or losing conversion occurs on either exit.
        let result = operation(&self, admission);
        let refused = unsafe { self.pointer.as_ref() }.payload.failure
            .lock().unwrap_or_else(|e| e.into_inner()).take();
        match (result, refused) {
            (Ok(value), None) => Ok(value),
            (Err(PreparedReadCause::Lower(redb::PreparedReadError::Native(
                redb::NativeColdReadError::AdmissionRefused))), Some(refusal)) => {
                let cause = match refusal { NativeStorageFailure::Refused(r) => PreparedReadCause::Refused(r), NativeStorageFailure::Unqualified(e) => PreparedReadCause::Unqualified(e) };
                Err(PreparedReadFailure { cause,
                    admission: None, _scope: Some(self.into_admission()), _original: None })
            }
            (Err(cause), admission) => Err(PreparedReadFailure {
                cause, admission, _scope: Some(self.into_admission()), _original: None,
            }),
            (Ok(value), Some(refusal)) => {
                // A caller that ignored a marker cannot publish its result.
                // Keep the genuine refusal alive while retiring that value.
                let cause = match refusal { NativeStorageFailure::Refused(r) => PreparedReadCause::Refused(r), NativeStorageFailure::Unqualified(e) => PreparedReadCause::Unqualified(e) };
                let failure = PreparedReadFailure { cause,
                    admission: None, _scope: Some(self.into_admission()), _original: None };
                if let Err(secondary) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))) {
                    std::mem::forget(secondary);
                }
                Err(failure)
            }
        }
    }
}
