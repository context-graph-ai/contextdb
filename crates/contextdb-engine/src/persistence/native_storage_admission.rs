//! Private exact-layout storage receipts with scalar admission refusal egress.
//! Bootstrap and body Error construction remain explicitly unqualified.
use crate::memory_accounting::{
    AccountantHandle, ControlCredit, OwnedMemoryReservation, StoreAccountantHandle, WriterUse,
    persistence_original_projection, reserve_preparation_from_writer_use,
    clone_persistence_original_writer, validate_owned_original,
};
use contextdb_core::{Error, Lsn, Result, SnapshotId};
use contextdb_core::read_memory::{NativeRefusal, ReadCredit, ReadMemoryOwner};
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

/// Native admission failures own no diagnostic bytes. Existing body and
/// bootstrap errors remain unqualified until their actual terminal is funded.
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
    Writer {
        original_use: WriterUse,
        original_handle: StoreAccountantHandle,
        genuine_funder: SourceFunder,
    },
    Read(ReadMemoryOwner),
}
impl StorageFunding {
    // Deliberately uses the existing error kernel, including its current gap.
    // Never substitute SourceFunding, current registration or zero-byte ownership.
    fn reserve_unqualified(&self, layout: Layout) -> Result<StorageAllocation> {
        match self {
            Self::Writer { original_use, original_handle, genuine_funder } => {
                debug_assert!(original_handle.is_writer());
                reserve_preparation_from_writer_use(
                    original_use, &original_handle.as_handle(), genuine_funder,
                    layout.size(), SUBSYSTEM, OPERATION, HINT,
                ).map(StorageAllocation::Writer)
            }
            Self::Read(owner) => ReadCredit::try_new_owned(
                owner.clone(), layout.size(), SUBSYSTEM, OPERATION, HINT,
            ).map(StorageAllocation::Read),
        }
    }
    // Native scope admissions never enter an allocating Error kernel.
    fn reserve_native(&self, layout: Layout) -> std::result::Result<StorageAllocation, NativeRefusal> {
        match self {
            Self::Writer { original_use, original_handle, .. } => {
                crate::memory_accounting::reserve_native_storage_from_writer_use(
                    original_use, original_handle, layout.size(),
                ).map(StorageAllocation::Writer)
            }
            Self::Read(owner) => ReadCredit::try_new_owned_native(
                owner.clone(), layout.size(), SUBSYSTEM, OPERATION, HINT,
            ).map(StorageAllocation::Read),
        }
    }
}
/// Private funded typed slot. There is deliberately no `into_error`: a bare
/// Error cannot retain these invoices through its String frees. Integration
/// with the existing Result<_, Error> escape remains a manager-owned decision.
/// Field order destroys all Strings before settling any backing invoice.
struct FundedStorageError {
    error: Error,
    _invoices: [StorageAllocation; 3],
    funding: StorageFunding,
}
impl FundedStorageError {
    fn prepare_writer(
        original: &OwnedMemoryReservation,
        expected: &AccountantHandle,
        funder: &SourceFunder,
    ) -> Result<Self> {
        validate_owned_original(original, expected, funder)?;
        let funding = StorageFunding::Writer {
            original_use: clone_persistence_original_writer(original)?,
            original_handle: persistence_original_projection(original)?,
            genuine_funder: funder.clone(),
        };
        // Bootstrap is outside native TLS. Failure returns the unchanged
        // existing typed error, whose construction is honestly unqualified.
        // Admit all actual backing layouts before constructing any String.
        let subsystem = funding.reserve_unqualified(Layout::array::<u8>(SUBSYSTEM.len())
            .expect("static subsystem has a valid layout"))?;
        let operation = funding.reserve_unqualified(Layout::array::<u8>(OPERATION.len())
            .expect("static operation has a valid layout"))?;
        let hint = funding.reserve_unqualified(Layout::array::<u8>(HINT.len())
            .expect("static hint has a valid layout"))?;
        let error = Error::MemoryBudgetExceeded {
            subsystem: Self::string_from_admitted(SUBSYSTEM),
            operation: Self::string_from_admitted(OPERATION),
            requested_bytes: 0,
            available_bytes: 0,
            budget_limit_bytes: 0,
            hint: Self::string_from_admitted(HINT),
        };
        Ok(Self { error, _invoices: [subsystem, operation, hint], funding })
    }

    // Every caller above owns a loan for exactly this nonempty layout. Do not
    // use String::with_capacity/try_reserve and inspect a post-allocation size.
    fn string_from_admitted(text: &'static str) -> String {
        let layout = Layout::array::<u8>(text.len()).expect("static text layout");
        assert_ne!(layout.size(), 0);
        let raw = unsafe { alloc(layout) };
        let Some(pointer) = NonNull::new(raw) else { handle_alloc_error(layout) };
        unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr(), pointer.as_ptr(), text.len());
            // Static input is UTF-8; len and capacity equal the admitted layout.
            String::from_raw_parts(pointer.as_ptr(), text.len(), text.len())
        }
    }

    fn set_budget_failure(&mut self, failure: NativeRefusal) {
        let NativeRefusal::MemoryBudgetExceeded {
            requested_bytes: requested, available_bytes: available, budget_limit_bytes: limit, ..
        } = failure else { unreachable!("writer scalar policy returns a budget refusal") };
        let Error::MemoryBudgetExceeded {
            requested_bytes, available_bytes, budget_limit_bytes, ..
        } = &mut self.error else { unreachable!("funded slot keeps its typed variant") };
        *requested_bytes = requested;
        *available_bytes = available;
        *budget_limit_bytes = limit;
    }

    // The scalar policy wrapper preserves give-back and admitted-unit excess.
    // On failure only numeric fields change;
    // the caller may borrow the typed error while this carrier keeps it funded.
    fn try_counter_reservation(
        &mut self,
        layout: Layout,
    ) -> std::result::Result<StorageAllocation, &Error> {
        let StorageFunding::Writer { original_use, original_handle, .. } = &self.funding
            else { unreachable!("this template is prepared only from a genuine writer") };
        match crate::memory_accounting::reserve_native_storage_from_writer_use(
            original_use, original_handle, layout.size(),
        ) {
            Ok(invoice) => Ok(StorageAllocation::Writer(invoice)),
            Err(failure) => {
                self.set_budget_failure(failure);
                Err(self.error())
            }
        }
    }

    fn error(&self) -> &Error { &self.error }
}

struct StorageReceipt {
    owners: AtomicUsize,
    allocation: ManuallyDrop<StorageAllocation>,
    control: ManuallyDrop<StorageAllocation>,
}
fn retain(owners: &AtomicUsize) {
    owners.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
        |n| n.checked_add(1).filter(|n| *n <= isize::MAX as usize))
        .expect("storage receipt strong count overflow");
}
unsafe fn retain_receipt(raw: NonNull<()>) {
    retain(&unsafe { raw.cast::<StorageReceipt>().as_ref() }.owners);
}
unsafe fn finish_receipt(raw: NonNull<()>) {
    let mut pointer = raw.cast::<StorageReceipt>();
    if unsafe { pointer.as_ref() }.owners.fetch_sub(1, Ordering::Release) != 1 { return; }
    fence(Ordering::Acquire);
    let receipt = unsafe { pointer.as_mut() };
    let allocation = unsafe { ManuallyDrop::take(&mut receipt.allocation) };
    let control = unsafe { ManuallyDrop::take(&mut receipt.control) };
    unsafe { dealloc(pointer.as_ptr().cast(), Layout::new::<StorageReceipt>()) };
    // Both real invoices retain original use/child until the receipt is gone.
    drop(allocation);
    drop(control);
}
fn paid_receipt(scope: &StorageScope, layout: Layout) -> std::result::Result<NativeStorageCredit, NativeRefusal> {
    let allocation = scope.funding.reserve_native(layout)?;
    let control = scope.funding.reserve_native(Layout::new::<StorageReceipt>())?;
    let layout = Layout::new::<StorageReceipt>();
    let raw = unsafe { alloc(layout) }.cast::<StorageReceipt>();
    let Some(pointer) = NonNull::new(raw) else {
        return Err(NativeRefusal::Other("native storage receipt allocation failed"));
    };
    unsafe { pointer.as_ptr().write(StorageReceipt {
        owners: AtomicUsize::new(1), allocation: ManuallyDrop::new(allocation),
        control: ManuallyDrop::new(control),
    }); }
    Ok(unsafe { NativeStorageCredit::from_owned_receipt(pointer.cast(), retain_receipt, finish_receipt) })
}
struct StorageScope {
    funding: StorageFunding,
    failure: Mutex<Option<NativeRefusal>>,
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
        unsafe { dealloc(self.pointer.as_ptr().cast(), Layout::new::<StorageScopeBlock>()) };
        drop(self.invoice.take());
    }
}
unsafe fn retain_scope(raw: NonNull<()>) {
    retain(&unsafe { raw.cast::<StorageScopeBlock>().as_ref() }.owners);
}
unsafe fn finish_scope(raw: NonNull<()>) {
    let mut pointer = raw.cast::<StorageScopeBlock>();
    if unsafe { pointer.as_ref() }.owners.fetch_sub(1, Ordering::Release) != 1 { return; }
    fence(Ordering::Acquire);
    let block = unsafe { pointer.as_mut() };
    let invoice = unsafe { ManuallyDrop::take(&mut block.invoice) };
    let _free = ScopeFree { pointer, invoice: Some(invoice) };
    unsafe { ManuallyDrop::drop(&mut block.payload) };
}
unsafe fn admit_scope(raw: NonNull<()>, layout: Layout) -> std::result::Result<NativeStorageCredit, NativeStorageRefused> {
    let scope = &unsafe { raw.cast::<StorageScopeBlock>().as_ref() }.payload;
    // A refused operation cannot allocate again, even if its caller ignores the marker.
    if scope.failure.lock().unwrap_or_else(|e| e.into_inner()).is_some() { return Err(NativeStorageRefused); }
    match paid_receipt(scope, layout) {
        Ok(receipt) => Ok(receipt),
        Err(original) => {
            let mut slot = scope.failure.lock().unwrap_or_else(|e| e.into_inner());
            if slot.is_none() { *slot = Some(original); }
            Err(NativeStorageRefused)
        }
    }
}

pub(crate) struct StorageAdmissionScope { pointer: NonNull<StorageScopeBlock> }
impl StorageAdmissionScope {
    fn new_unqualified(funding: StorageFunding) -> Result<Self> {
        let layout = Layout::new::<StorageScopeBlock>();
        // Bootstrap returns the genuine existing error; it is not a qualified
        // no-allocation refusal and there is no fallback owner or stipend.
        let invoice = funding.reserve_unqualified(layout)?;
        let raw = unsafe { alloc(layout) }.cast::<StorageScopeBlock>();
        let Some(pointer) = NonNull::new(raw) else { handle_alloc_error(layout) };
        unsafe { pointer.as_ptr().write(StorageScopeBlock {
            owners: AtomicUsize::new(1),
            payload: ManuallyDrop::new(StorageScope { funding, failure: Mutex::new(None) }),
            invoice: ManuallyDrop::new(invoice),
        }); }
        Ok(Self { pointer })
    }
    pub(super) fn writer_unqualified(
        original: &OwnedMemoryReservation, expected: &AccountantHandle, funder: &SourceFunder,
    ) -> Result<Self> {
        validate_owned_original(original, expected, funder)?;
        Self::new_unqualified(StorageFunding::Writer {
            original_use: clone_persistence_original_writer(original)?,
            original_handle: persistence_original_projection(original)?,
            genuine_funder: funder.clone(),
        })
    }
    pub(super) fn reader_unqualified(receipt: crate::vector_read_memory::NativeReadChildReceipt) -> Result<Self> {
        Self::new_unqualified(StorageFunding::Read(receipt.into_owner()))
    }
    /// Use the executing begin_request child, never acquire another child or
    /// recover a Legacy budget. The scope's exact physical cell is prepaid.
    pub(super) fn reader_native_owner(
        owner: ReadMemoryOwner,
    ) -> std::result::Result<Self, NativeStorageFailure> {
        let funding = StorageFunding::Read(owner);
        let layout = Layout::new::<StorageScopeBlock>();
        let invoice = funding.reserve_native(layout).map_err(NativeStorageFailure::Refused)?;
        let raw = unsafe { alloc(layout) }.cast::<StorageScopeBlock>();
        let pointer = NonNull::new(raw).ok_or(NativeStorageFailure::Refused(
            NativeRefusal::Other("native storage scope allocation failed"),
        ))?;
        // No fallible step after allocation: final scope release destroys its
        // payload, frees this cell, and only then refunds the original child.
        unsafe { pointer.as_ptr().write(StorageScopeBlock {
            owners: AtomicUsize::new(1),
            payload: ManuallyDrop::new(StorageScope { funding, failure: Mutex::new(None) }),
            invoice: ManuallyDrop::new(invoice),
        }); }
        Ok(Self { pointer })
    }

    pub(super) fn run_native<R>(
        &self, operation: impl FnOnce() -> Result<R>,
    ) -> std::result::Result<R, NativeStorageFailure> {
        unsafe { retain_scope(self.pointer.cast()) };
        let admission = unsafe { NativeStorageAdmission::from_owned_receipt(
            self.pointer.cast(), retain_scope, finish_scope, admit_scope,
        ) };
        let result = redb::with_native_storage_admission(admission, operation);
        let scope = &unsafe { self.pointer.as_ref() }.payload;
        // Move the scalar admission refusal before adapting any unqualified body Error.
        let original = scope.failure.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(original) = original { return Err(NativeStorageFailure::Refused(original)); }
        result.map_err(NativeStorageFailure::Unqualified)
    }
}
impl Drop for StorageAdmissionScope {
    fn drop(&mut self) { unsafe { finish_scope(self.pointer.cast()) } }
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
pub(super) struct ColdMetadataOwner { pointer: NonNull<ColdMetadataBlock> }
unsafe impl Send for ColdMetadataOwner {}
unsafe impl Sync for ColdMetadataOwner {}
impl ColdMetadataOwner {
    pub(super) fn new_unqualified(accountant: &AccountantHandle, metadata: ColdSnapshotMetadata) -> Result<Self> {
        let original_untagged = StoreAccountantHandle::from_original_nonwriter(accountant);
        let layout = Layout::new::<ColdMetadataBlock>();
        let invoice = ControlCredit::admit_from_handle(
            &original_untagged, layout.size(), "redb", "cold_snapshot_metadata",
            "Raise the store memory limit for this snapshot descriptor.",
        )?;
        let raw = unsafe { alloc(layout) }.cast::<ColdMetadataBlock>();
        let Some(pointer) = NonNull::new(raw) else { handle_alloc_error(layout) };
        unsafe { pointer.as_ptr().write(ColdMetadataBlock {
            owners: AtomicUsize::new(1), metadata, invoice: ManuallyDrop::new(invoice),
        }); }
        Ok(Self { pointer })
    }
    pub(super) fn get(&self) -> ColdSnapshotMetadata { unsafe { self.pointer.as_ref() }.metadata }
}
impl Clone for ColdMetadataOwner {
    fn clone(&self) -> Self {
        retain(&unsafe { self.pointer.as_ref() }.owners);
        Self { pointer: self.pointer }
    }
}
impl Drop for ColdMetadataOwner {
    fn drop(&mut self) {
        if unsafe { self.pointer.as_ref() }.owners.fetch_sub(1, Ordering::Release) != 1 { return; }
        fence(Ordering::Acquire);
        let invoice = unsafe { ManuallyDrop::take(&mut self.pointer.as_mut().invoice) };
        unsafe { dealloc(self.pointer.as_ptr().cast(), Layout::new::<ColdMetadataBlock>()) };
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
        &self, logical: &str,
        operation: impl FnOnce(&str) -> Result<std::result::Result<R, redb::NativeColdReadError>>,
    ) -> Result<std::result::Result<R, redb::NativeColdReadError>> {
        let Some(len) = logical.len().checked_add(4) else {
            return Ok(Err(redb::NativeColdReadError::Corrupt(redb::NativeColdFault {
                reason: "physical table name length overflow", page: [0; 8],
                offset: 0, expected: 0, actual: 0,
            })));
        };
        let Ok(layout) = Layout::array::<u8>(len) else {
            return Ok(Err(redb::NativeColdReadError::Corrupt(redb::NativeColdFault {
                reason: "physical table name layout overflow", page: [0; 8],
                offset: 0, expected: 0, actual: 0,
            })));
        };
        // The native admission marker is recovered as its original scalar by
        // run_native before the lower result is inspected. A sticky refusal
        // also prevents a later physical-name allocation in this scope.
        let invoice = match unsafe { admit_scope(self.pointer.cast(), layout) } {
            Ok(invoice) => invoice,
            Err(_) => return Ok(Err(redb::NativeColdReadError::AdmissionRefused)),
        };
        let raw = unsafe { alloc(layout) };
        let Some(pointer) = NonNull::new(raw) else { handle_alloc_error(layout) };
        // No fallible callback between alloc and initialized owner construction.
        unsafe {
            std::ptr::copy_nonoverlapping(b"rel_".as_ptr(), pointer.as_ptr(), 4);
            std::ptr::copy_nonoverlapping(logical.as_ptr(), pointer.as_ptr().add(4), logical.len());
        }
        let name = StorageName {
            bytes: ManuallyDrop::new(unsafe { Vec::from_raw_parts(pointer.as_ptr(), len, len) }),
            invoice: Some(invoice),
        };
        let name_str = std::str::from_utf8(&name.bytes).expect("prefix and logical table are UTF-8");
        operation(name_str)
    }
}
