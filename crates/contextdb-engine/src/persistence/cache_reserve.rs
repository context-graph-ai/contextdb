//! The original fixed cache charge is owned once, outside every physical cache
//! allocation's free. This is custody, not a second invoice for cached pages.
use crate::memory_accounting::{AccountantHandle, ControlCredit, WriterUse};
use contextdb_core::{Error, Result};
use redb::NativeStorageCredit;
use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

struct ReserveBlock {
    owners: AtomicUsize,
    reserve: Option<ControlCredit>,
    control: Option<ControlCredit>,
    writer: Option<WriterUse>,
    #[cfg(feature = "test-seams")]
    observation: Option<redb::native_page_observations::NativeFixedReserveObservationForTest>,
}
unsafe fn retain(raw: NonNull<()>) {
    unsafe { raw.cast::<ReserveBlock>().as_ref() }
        .owners
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            n.checked_add(1).filter(|n| *n <= isize::MAX as usize)
        })
        .expect("cache reserve strong count overflow");
}
struct ReserveFree {
    pointer: NonNull<ReserveBlock>,
    // Each is the original genuine receipt, extracted before destruction.
    _reserve: Option<ControlCredit>,
    _control: Option<ControlCredit>,
    _writer: Option<WriterUse>,
    // Last: genuine reserve/control/writer fields settle before this notification.
    #[cfg(feature = "test-seams")]
    _observation: Option<redb::native_page_observations::NativeFixedReserveObservationForTest>,
}
impl Drop for ReserveFree {
    fn drop(&mut self) {
        unsafe { dealloc(self.pointer.as_ptr().cast(), Layout::new::<ReserveBlock>()) };
    }
}
unsafe fn finish(raw: NonNull<()>) {
    let mut pointer = raw.cast::<ReserveBlock>();
    if unsafe { pointer.as_ref() }
        .owners
        .fetch_sub(1, Ordering::Release)
        != 1
    {
        return;
    }
    fence(Ordering::Acquire);
    let block = unsafe { pointer.as_mut() };
    #[cfg(feature = "test-seams")]
    if let Some(observation) = &mut block.observation {
        observation.actual_release_bytes(
            block
                .reserve
                .as_ref()
                .map_or(0, |credit| credit.observation_for_test().1)
                + block
                    .control
                    .as_ref()
                    .map_or(0, |credit| credit.observation_for_test().1),
        );
    }
    let _free = ReserveFree {
        pointer,
        _reserve: block.reserve.take(),
        _control: block.control.take(),
        _writer: block.writer.take(),
        #[cfg(feature = "test-seams")]
        _observation: block.observation.take(),
    };
    unsafe { std::ptr::drop_in_place(pointer.as_ptr()) };
}

/// Preserve startup's existing retained-charge semantics for declared writers,
/// and its process admission semantics for undeclared writers. Both receipts
/// precede this new control's allocation; null/unwind owns their rollback.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn original(
    accountant: &AccountantHandle,
    bytes: usize,
    process_residue: bool,
) -> Result<NativeStorageCredit> {
    let reserve = if process_residue {
        ControlCredit::admit_open_residue_native(accountant, bytes, "undeclared_writer_cache")
            .map_err(|refusal| {
                super::native_storage_admission::original_storage_error(
                    super::native_storage_admission::NativeStorageFailure::Refused(refusal),
                )
            })?
    } else {
        ControlCredit::retained_cache_reserve_startup(accountant, bytes)
    };
    let layout = Layout::new::<ReserveBlock>();
    let control = if process_residue {
        ControlCredit::admit_open_residue_native(accountant, layout.size(), "writer_cache_control")
            .map_err(|refusal| {
                super::native_storage_admission::original_storage_error(
                    super::native_storage_admission::NativeStorageFailure::Refused(refusal),
                )
            })?
    } else {
        ControlCredit::retained_startup(accountant, layout.size())
    };
    let writer = WriterUse::retain_if_writer(accountant);
    let Some(pointer) = NonNull::new(unsafe { alloc(layout) }.cast::<ReserveBlock>()) else {
        return Err(Error::Other(
            "cache reserve control allocation failed".into(),
        ));
    };
    unsafe {
        pointer.as_ptr().write(ReserveBlock {
            owners: AtomicUsize::new(1),
            reserve: Some(reserve),
            control: Some(control),
            writer,
            #[cfg(feature = "test-seams")]
            observation: None,
        })
    };
    #[cfg(feature = "test-seams")]
    unsafe {
        let block = &mut *pointer.as_ptr();
        let bytes = block
            .reserve
            .as_ref()
            .map_or(0, |credit| credit.observation_for_test().1)
            + block
                .control
                .as_ref()
                .map_or(0, |credit| credit.observation_for_test().1);
        let funder = block
            .reserve
            .as_ref()
            .expect("live original reserve")
            .observation_for_test()
            .0;
        block.observation = Some(
            redb::native_page_observations::observe_fixed_reserve_for_test(
                pointer.as_ptr() as usize,
                funder,
                bytes,
                layout,
            ),
        );
    }
    #[cfg(test)]
    LAST_RESERVE.with(|slot| slot.set(pointer.as_ptr() as usize));
    // SAFETY: immutable original receipts and held writer use are Send + Sync;
    // only atomic strong count mutates until exclusive last release. There is
    // no Weak tail, borrowed callback context or external registry.
    Ok(unsafe { NativeStorageCredit::from_owned_receipt(pointer.cast(), retain, finish) })
}

// One bounded preparation scope owns an aggregate invoice, extended at each
// actual index/window/control producer. It outlives both rollback and the
// handoff to the original fixed reserve. Receipts clone this SAME owner; they
// neither invent per-page invoices nor allocate a control for each callback.
struct PreparationBlock {
    owners: AtomicUsize,
    startup: bool,
    direct_source: bool,
    invoice: std::sync::Mutex<Option<ControlCredit>>,
    failure: std::sync::Mutex<Option<super::native_storage_admission::NativeStorageFailure>>,
    writer: Option<WriterUse>,
}
unsafe fn retain_preparation(raw: NonNull<()>) {
    unsafe { raw.cast::<PreparationBlock>().as_ref() }
        .owners
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            n.checked_add(1).filter(|n| *n <= isize::MAX as usize)
        })
        .expect("cache preparation strong count overflow");
}
struct PreparationFree {
    pointer: NonNull<PreparationBlock>,
    _invoice: Option<ControlCredit>,
    _writer: Option<WriterUse>,
}
impl Drop for PreparationFree {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.pointer.as_ptr().cast(),
                Layout::new::<PreparationBlock>(),
            )
        };
    }
}
unsafe fn finish_preparation(raw: NonNull<()>) {
    let mut pointer = raw.cast::<PreparationBlock>();
    if unsafe { pointer.as_ref() }
        .owners
        .fetch_sub(1, Ordering::Release)
        != 1
    {
        return;
    }
    fence(Ordering::Acquire);
    let block = unsafe { pointer.as_mut() };
    let _free = PreparationFree {
        pointer,
        _invoice: block
            .invoice
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take(),
        _writer: block.writer.take(),
    };
    unsafe { std::ptr::drop_in_place(pointer.as_ptr()) };
}
unsafe fn inspect_preparation(
    raw: NonNull<()>,
    inspect: &mut dyn FnMut(Option<&(dyn std::error::Error + Send + Sync + 'static)>),
) {
    let block = unsafe { raw.cast::<PreparationBlock>().as_ref() };
    let failure = block.failure.lock().unwrap_or_else(|e| e.into_inner());
    inspect(
        failure
            .as_ref()
            .map(|e| e as &(dyn std::error::Error + Send + Sync + 'static)),
    );
}
unsafe fn admit_preparation(
    raw: NonNull<()>,
    layout: Layout,
) -> std::result::Result<NativeStorageCredit, redb::NativeStorageRefused> {
    let block = unsafe { raw.cast::<PreparationBlock>().as_ref() };
    let mut failure = block.failure.lock().unwrap_or_else(|e| e.into_inner());
    if failure.is_some() {
        return Err(redb::NativeStorageRefused);
    }
    let mut invoice = block.invoice.lock().unwrap_or_else(|e| e.into_inner());
    let invoice = invoice.as_mut().expect("live preparation invoice");
    #[cfg(test)]
    if block.direct_source {
        super::direct_source_contract_rows::before_native_invoice(layout, invoice);
    }
    let admitted = if block.direct_source {
        invoice.grow_direct_source_native(layout.size())
    } else {
        invoice.grow_cache_preparation_native(layout.size(), block.startup)
    };
    if let Err(refusal) = admitted {
        *failure = Some(super::native_storage_admission::NativeStorageFailure::Refused(refusal));
        return Err(redb::NativeStorageRefused);
    }
    unsafe { retain_preparation(raw) };
    Ok(unsafe {
        NativeStorageCredit::from_owned_receipt(raw, retain_preparation, finish_preparation)
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn with_preparation<T>(
    accountant: &AccountantHandle,
    startup: bool,
    prepare: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let layout = Layout::new::<PreparationBlock>();
    let invoice = ControlCredit::admit_cache_preparation_native(accountant, layout.size(), startup)
        .map_err(|refusal| {
            super::native_storage_admission::original_storage_error(
                super::native_storage_admission::NativeStorageFailure::Refused(refusal),
            )
        })?;
    let writer = WriterUse::retain_if_writer(accountant);
    let Some(pointer) = NonNull::new(unsafe { alloc(layout) }.cast::<PreparationBlock>()) else {
        return Err(Error::Other(
            "cache preparation control allocation failed".into(),
        ));
    };
    unsafe {
        pointer.as_ptr().write(PreparationBlock {
            owners: AtomicUsize::new(1),
            startup,
            direct_source: false,
            invoice: std::sync::Mutex::new(Some(invoice)),
            failure: std::sync::Mutex::new(None),
            writer,
        })
    };
    // SAFETY: concrete Mutex-protected credit and scalar failure, pinned original
    // writer, checked strong count and external physical-free-before-refund.
    let admission = unsafe {
        redb::NativeStorageAdmission::from_owned_receipt(
            pointer.cast(),
            retain_preparation,
            finish_preparation,
            admit_preparation,
        )
        .with_failure_inspector(inspect_preparation)
    };
    redb::with_cache_preparation_admission(admission, prepare)
}

/// Install only around the actual read-only native open. Every native backing
/// retains this same invoice owner, so successful open, null and unwind all
/// keep original source funding until the last physical backing is freed.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn direct_preparation(
    memory: &crate::read_image_memory::ImageMemory,
) -> Result<redb::NativeStorageAdmission> {
    let layout = Layout::new::<PreparationBlock>();
    let invoice = memory
        .direct_source_native_credit(layout.size())
        .map_err(|failure| {
            super::native_storage_admission::original_storage_error(
                super::native_storage_admission::NativeStorageFailure::Refused(failure),
            )
        })?;
    let Some(pointer) = NonNull::new(unsafe { alloc(layout) }.cast::<PreparationBlock>()) else {
        return Err(super::native_storage_admission::original_storage_error(
            super::native_storage_admission::NativeStorageFailure::AllocatorFailed {
                site: "direct source cache preparation",
                layout,
            },
        ));
    };
    unsafe {
        pointer.as_ptr().write(PreparationBlock {
            owners: AtomicUsize::new(1),
            startup: false,
            direct_source: true,
            invoice: std::sync::Mutex::new(Some(invoice)),
            failure: std::sync::Mutex::new(None),
            writer: None,
        })
    };
    // SAFETY: concrete owned receipt; source-only ordinary admission; failure
    // inspector borrows the same scalar slot. No writer discovery or Weak tail.
    Ok(unsafe {
        redb::NativeStorageAdmission::from_owned_receipt(
            pointer.cast(),
            retain_preparation,
            finish_preparation,
            admit_preparation,
        )
        .with_failure_inspector(inspect_preparation)
    })
}

#[cfg(test)]
thread_local! {
    static LAST_RESERVE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
pub(super) fn last_reserve_for_test() -> (usize, usize, usize) {
    let layout = Layout::new::<ReserveBlock>();
    (
        LAST_RESERVE.with(std::cell::Cell::get),
        layout.size(),
        layout.align(),
    )
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn direct_observation_for_test(pointer: usize) -> ((usize, usize, usize), usize, usize) {
    assert_ne!(pointer, 0);
    // The caller borrows a live native counter which owns this exact receipt.
    let block = unsafe { &*(pointer as *const PreparationBlock) };
    assert!(block.direct_source);
    let invoice = block.invoice.lock().unwrap_or_else(|e| e.into_inner());
    let (payer, bytes) = invoice
        .as_ref()
        .expect("live preparation invoice")
        .observation_for_test();
    let layout = Layout::new::<PreparationBlock>();
    ((pointer, layout.size(), layout.align()), bytes, payer)
}

#[cfg(test)]
pub(super) fn preparation_layout_for_test() -> Layout {
    Layout::new::<PreparationBlock>()
}
