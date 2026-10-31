//! Source extents are separate owners from generation decode/build workspaces.
//! In particular, a borrowed graph still holds its entire redb source page.

#[cfg(test)]
use crate::memory_accounting::MemoryAccountant;
#[cfg(test)]
use contextdb_core::Error;
use contextdb_core::Result;
use std::sync::Arc;

struct SourcePageCharge {
    accountant: Option<crate::memory_accounting::AccountantHandle>,
    bytes: usize,
    own_layout: std::alloc::Layout,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

impl Drop for SourcePageCharge {
    fn drop(&mut self) {
        let Some(accountant) = self.accountant.take() else {
            return;
        };
        accountant.release(self.bytes);
        if self.own_layout.size() != 0 {
            // The concrete single owner already moved this authentic receipt
            // out and physically freed Layout<SourcePageCharge> (or allocator
            // null created no backing). Keep the
            // original accountant and writer use alive through its refund.
            let refund = accountant.retain_box_control_refund(self.own_layout.size());
            refund
                .release_owned(self.own_layout.size())
                .expect("source charge control refund is owned");
        }
    }
}

/// Admit the actual extent before redb allocates it. Uncached pages retain
/// the charge until their last handle drops; cached pages hand ownership to
/// the writer's already-reserved fixed page cache. Decode/build reservations
/// cover other allocations and must not include this source buffer again.
///
/// Redb restores enclosing callbacks on return or unwind. Admission errors
/// travel with their original typed payload through storage adaptation.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn with_source_page_admission<T>(
    accountant: crate::memory_accounting::AccountantHandle,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_source_page_admission_for(
        accountant,
        "read_vector_generation_source_page",
        "Raise MEMORY_LIMIT before reading this maintained generation's source page.",
        read,
    )
}

/// Frontier metadata can occupy an arbitrarily large physical extent even
/// when its decoded descriptor is bounded. Charge that extent before redb
/// allocates it, while retaining the preparation operation's early refusal.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn with_preparation_source_page_admission<T>(
    accountant: crate::memory_accounting::AccountantHandle,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_source_page_admission_for(
        accountant,
        "prepare_vector_generation",
        "Raise MEMORY_LIMIT before inspecting this maintained route.",
        || crate::persistence::vector_membership_source::with_maintenance_inspection(read),
    )
}

// This private producer cell is funded before installing the callback. Its
// native marker retains the actual cell; no post-denial error Box is needed.
struct SourceRefusalBlock {
    owners: std::sync::atomic::AtomicUsize,
    refusal: std::sync::Mutex<
        Option<crate::persistence::native_storage_admission::NativeStorageFailure>,
    >,
    invoice: Option<crate::memory_accounting::OwnedMemoryReservation>,
}
#[derive(Clone)]
struct SourceRefusalScope(
    redb::NativeStorageAdmission,
    std::ptr::NonNull<SourceRefusalBlock>,
);
// SAFETY: the concrete block is synchronized; the admission owns its lifetime.
unsafe impl Send for SourceRefusalScope {}
unsafe impl Sync for SourceRefusalScope {}
unsafe fn retain_refusal(raw: std::ptr::NonNull<()>) {
    unsafe { raw.cast::<SourceRefusalBlock>().as_ref() }
        .owners
        .fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |n| n.checked_add(1).filter(|n| *n <= isize::MAX as usize),
        )
        .expect("source refusal strong count overflow");
}
struct RefusalFree {
    pointer: std::ptr::NonNull<SourceRefusalBlock>,
    _invoice: Option<crate::memory_accounting::OwnedMemoryReservation>,
}
impl Drop for RefusalFree {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(
                self.pointer.as_ptr().cast(),
                std::alloc::Layout::new::<SourceRefusalBlock>(),
            )
        };
    }
}
unsafe fn finish_refusal(raw: std::ptr::NonNull<()>) {
    let mut pointer = raw.cast::<SourceRefusalBlock>();
    if unsafe { pointer.as_ref() }
        .owners
        .fetch_sub(1, std::sync::atomic::Ordering::Release)
        != 1
    {
        return;
    }
    std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
    let block = unsafe { pointer.as_mut() };
    let _free = RefusalFree {
        pointer,
        _invoice: block.invoice.take(),
    };
    unsafe { std::ptr::drop_in_place(pointer.as_ptr()) };
}
unsafe fn inspect_refusal(
    raw: std::ptr::NonNull<()>,
    inspect: &mut dyn FnMut(Option<&(dyn std::error::Error + Send + Sync + 'static)>),
) {
    let block = unsafe { raw.cast::<SourceRefusalBlock>().as_ref() };
    let refusal = block.refusal.lock().unwrap_or_else(|e| e.into_inner());
    inspect(
        refusal
            .as_ref()
            .map(|e| e as &(dyn std::error::Error + Send + Sync + 'static)),
    );
}
// This scope is only an error-producer capability. It never installs a native
// storage allocation callback or fabricates a native receipt for cache pages.
unsafe fn refuse_native_allocation(
    _: std::ptr::NonNull<()>,
    _: std::alloc::Layout,
) -> std::result::Result<redb::NativeStorageCredit, redb::NativeStorageRefused> {
    Err(redb::NativeStorageRefused)
}
impl SourceRefusalScope {
    fn new(
        accountant: crate::memory_accounting::AccountantHandle,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<Self, contextdb_core::read_memory::NativeRefusal> {
        let layout = std::alloc::Layout::new::<SourceRefusalBlock>();
        let invoice = crate::memory_accounting::OwnedMemoryReservation::try_new_for_native(
            accountant,
            layout.size(),
            "vector_maintenance",
            operation,
            hint,
        )?;
        let Some(pointer) = std::ptr::NonNull::new(
            unsafe { std::alloc::alloc(layout) }.cast::<SourceRefusalBlock>(),
        ) else {
            return Err(contextdb_core::read_memory::NativeRefusal::Other(
                "source refusal control allocation failed",
            ));
        };
        unsafe {
            pointer.as_ptr().write(SourceRefusalBlock {
                owners: std::sync::atomic::AtomicUsize::new(1),
                refusal: std::sync::Mutex::new(None),
                invoice: Some(invoice),
            })
        };
        // SAFETY: concrete synchronized scalar payload and genuine Send + Sync
        // invoice; last release frees this cell before returning its invoice.
        Ok(Self(
            unsafe {
                redb::NativeStorageAdmission::from_owned_receipt(
                    pointer.cast(),
                    retain_refusal,
                    finish_refusal,
                    refuse_native_allocation,
                )
                .with_failure_inspector(inspect_refusal)
            },
            pointer,
        ))
    }
    fn record(&self, refusal: contextdb_core::read_memory::NativeRefusal) {
        let mut slot = unsafe { self.1.as_ref() }
            .refusal
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(
                crate::persistence::native_storage_admission::NativeStorageFailure::Refused(
                    refusal,
                ),
            );
        }
    }
    fn refusal(&self) -> Option<contextdb_core::read_memory::NativeRefusal> {
        self.0.inspect_failure(|error| {
            error
                .and_then(|e| {
                    e.downcast_ref::<
            crate::persistence::native_storage_admission::NativeStorageFailure>()
                })
                .and_then(|e| match e {
                    crate::persistence::native_storage_admission::NativeStorageFailure::Refused(
                        refusal,
                    ) => Some(*refusal),
                    _ => None,
                })
        })
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn with_source_page_admission_for<T>(
    accountant: crate::memory_accounting::AccountantHandle,
    operation: &'static str,
    hint: &'static str,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    use crate::persistence::native_storage_admission::{
        NativeStorageFailure, original_storage_error,
    };
    let scope = SourceRefusalScope::new(accountant.clone(), operation, hint)
        .map_err(|e| original_storage_error(NativeStorageFailure::Refused(e)))?;
    let callback_scope = scope.clone();
    let callback_accountant = accountant.clone();
    let callback = move |bytes: usize| {
        if callback_scope.refusal().is_some() {
            return Err(redb::StorageError::Native(
                redb::NativeColdReadError::AdmissionRefusedAt(callback_scope.0.clone()),
            ));
        }
        let admit = || -> std::result::Result<redb::OwnedReadPageCharge, contextdb_core::read_memory::NativeRefusal> {
            callback_accountant.try_allocate_for_native(bytes, "vector_maintenance", operation, hint)?;
            let mut charge = SourcePageCharge {
                _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&callback_accountant),
                accountant: Some(callback_accountant.clone()),
                bytes,
                own_layout: std::alloc::Layout::new::<()>(),
            };
            let layout = std::alloc::Layout::new::<SourcePageCharge>();
            callback_accountant.admit_box_control_native(layout.size(), operation, hint)?;
            charge.own_layout = layout;
            redb::OwnedReadPageCharge::try_new_prepaid(charge).map_err(|layout| {
                contextdb_core::read_memory::NativeRefusal::AllocatorFailed {
                    site: "source page credit control",
                    layout,
                }
            })
        };
        admit().map_err(|refusal| {
            // Record scalar data in the actual prepaid producer cell. No
            // source copy, formatting, allocation or new admission follows.
            callback_scope.record(refusal);
            redb::StorageError::Native(redb::NativeColdReadError::AdmissionRefusedAt(
                callback_scope.0.clone(),
            ))
        })
    };
    // External stack custody outlives all callback Arc clones: ACTIVE removes
    // its clone on return/unwind before this local invoice is destroyed. The
    // callback API has no escape hatch for this private closure or a Weak.
    let callback_layout = crate::memory_accounting::writer_adapter_layout(&callback);
    let _callback_invoice = crate::memory_accounting::OwnedMemoryReservation::try_new_for_native(
        accountant,
        callback_layout.size(),
        "vector_maintenance",
        operation,
        hint,
    )
    .map_err(|e| original_storage_error(NativeStorageFailure::Refused(e)))?;
    // The first-refusal latch denies later source allocations and retains the
    // original cause for storage error extraction. It does not veto the result:
    // read may have handled a refusal as a classification or published outcome
    // with cleanup failure. Preserve its actual success or error unchanged.
    redb::with_owned_read_page_charge_storage(Arc::new(callback), read)
}
