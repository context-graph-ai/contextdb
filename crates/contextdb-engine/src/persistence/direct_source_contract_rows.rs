//! Actual direct-session rows. The original image accountant is observed before
//! the real producers; no substitute budget, late event or heap log certifies it.
use super::content_identity::proof_allocation_oracle_tests as oracle;
use super::*;
use crate::memory_accounting::AccountantHandle;
use crate::{ReadSession, ReadSessionOptions};
use std::alloc::Layout;
use std::cell::{Cell, RefCell};

#[derive(Clone, Copy)]
struct Cut {
    requests: [(usize, usize, usize); 256],
    credit: [usize; 256],
    count: usize,
    standing: usize,
}
impl Cut {
    const EMPTY: Self = Self {
        requests: [(0, 0, 0); 256],
        credit: [0; 256],
        count: 0,
        standing: 0,
    };
}
thread_local! {
    static TRANSFER: Cell<Option<([usize; 3], [usize; 3], usize)>> = const { Cell::new(None) };
    static IMAGE_POINTER: Cell<usize> = const { Cell::new(0) };
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static OWNER: RefCell<Option<AccountantHandle>> = const { RefCell::new(None) };
    static CUTS: Cell<[Cut; 3]> = const { Cell::new([Cut::EMPTY; 3]) };
    static INVOICES: Cell<[(usize, usize); 140]> = const { Cell::new([(0, 0); 140]) };
    static INVOICE_COUNT: Cell<usize> = const { Cell::new(0) };
    static PANIC_INVOICE: Cell<Option<usize>> = const { Cell::new(None) };
    static DENY_INVOICE: Cell<Option<usize>> = const { Cell::new(None) };
    static DENY_PREPARATION: Cell<bool> = const { Cell::new(false) };
    static NULL_TARGET: Cell<Option<(usize, usize, usize, usize)>> = const { Cell::new(None) };

}
struct Observation;
impl Observation {
    fn begin() -> Self {
        assert!(!ENABLED.replace(true));
        CUTS.set([Cut::EMPTY; 3]);
        IMAGE_POINTER.set(0);
        TRANSFER.set(None);
        INVOICES.set([(0, 0); 140]);
        INVOICE_COUNT.set(0);
        DENY_INVOICE.set(None);
        PANIC_INVOICE.set(None);
        DENY_PREPARATION.set(false);
        NULL_TARGET.set(None);
        OWNER.with(|slot| *slot.borrow_mut() = None);
        Self
    }
    fn owner(&self) -> AccountantHandle {
        OWNER.with(|slot| slot.borrow().as_ref().unwrap().clone())
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        ENABLED.set(false);
        ACTIVE.set(false);
        oracle::disarm_source_free_watch();
        OWNER.with(|slot| *slot.borrow_mut() = None);
        IMAGE_POINTER.set(0);
    }
}
pub(super) struct SourceObservation {
    phase: usize,
    window: oracle::SourceRequestsWindow,
    standing: usize,
}
impl Drop for SourceObservation {
    fn drop(&mut self) {
        let (requests, credit, count) = self.window.receipt();
        let mut cuts = CUTS.get();
        assert_eq!(cuts[self.phase].count, 0, "one actual producer window");
        cuts[self.phase] = Cut {
            requests,
            credit,
            count,
            standing: self.standing,
        };
        CUTS.set(cuts);
        ACTIVE.set(false);
    }
}
pub(super) fn begin(
    phase: usize,
    memory: &crate::read_image_memory::ImageMemory,
) -> Option<SourceObservation> {
    if !ENABLED.get() || ACTIVE.get() {
        return None;
    }
    IMAGE_POINTER.set(memory as *const crate::read_image_memory::ImageMemory as usize);
    OWNER.with(|slot| {
        let mut owner = slot.borrow_mut();
        if let Some(owner) = owner.as_ref() {
            assert_eq!(owner.as_ptr(), memory.accountant.as_ptr());
        } else {
            *owner = Some(memory.accountant.clone());
        }
    });
    if phase == 1 && DENY_PREPARATION.get() {
        let bytes = super::cache_reserve::preparation_layout_for_test().size();
        memory
            .accountant
            .set_budget(Some(memory.accountant.usage().used + bytes - 1))
            .unwrap();
    }
    Some(begin_counter(phase, &memory.accountant))
}
fn begin_counter(phase: usize, owner: &AccountantHandle) -> SourceObservation {
    assert!(!ACTIVE.replace(true));
    let counter = owner.writer_registration_observation_for_test().4;
    if let Some((target, size, align, skip)) = NULL_TARGET.get() {
        if target == phase {
            crate::vector_read_memory::native_ownership_proof::arm_refuse_nth_live_layout(
                Layout::from_size_align(size, align).unwrap(),
                skip,
            );
        }
    }
    SourceObservation {
        phase,
        standing: owner.usage().used,
        window: oracle::SourceRequestsWindow::begin(counter),
    }
}
pub(super) fn begin_shared(
    control: &crate::memory_accounting::ControlCredit,
) -> Option<SourceObservation> {
    if !ENABLED.get() || ACTIVE.get() {
        return None;
    }
    let owner = OWNER.with(|slot| slot.borrow().as_ref().unwrap().clone());
    assert_eq!(control.observation_for_test().0, owner.as_ptr() as usize);
    Some(begin_counter(2, &owner))
}
pub(super) struct TransferObservation<'a> {
    source: &'a SourceMemoryReservation,
    before: [usize; 3],
    bytes: usize,
}
impl<'a> TransferObservation<'a> {
    pub(super) fn begin(source: &'a SourceMemoryReservation, bytes: usize) -> Option<Self> {
        if !ENABLED.get() {
            return None;
        }
        Some(Self {
            source,
            bytes,
            before: [
                source.bytes.load(Ordering::SeqCst),
                source.memory.held_for_test(),
                source.memory.accountant.usage().used,
            ],
        })
    }
}
impl Drop for TransferObservation<'_> {
    fn drop(&mut self) {
        assert!(TRANSFER.get().is_none());
        TRANSFER.set(Some((
            self.before,
            [
                self.source.bytes.load(Ordering::SeqCst),
                self.source.memory.held_for_test(),
                self.source.memory.accountant.usage().used,
            ],
            self.bytes,
        )));
    }
}
// Called at the actual production admission callback, before the debit and
// before allocation. It records no invented allocation or post-hoc invoice.
pub(super) fn before_native_invoice(
    layout: Layout,
    invoice: &crate::memory_accounting::ControlCredit,
) {
    if !ENABLED.get() {
        return;
    }
    let index = INVOICE_COUNT.get();
    let mut invoices = INVOICES.get();
    invoices[index] = (layout.size(), layout.align());
    INVOICES.set(invoices);
    INVOICE_COUNT.set(index + 1);
    let owner = OWNER.with(|slot| slot.borrow().as_ref().unwrap().clone());
    assert_eq!(invoice.observation_for_test().0, owner.as_ptr() as usize);
    if PANIC_INVOICE.get() == Some(index) {
        panic!("unwind the actual native constructor after earlier cache controls allocated");
    }
    if DENY_INVOICE.get() == Some(index) {
        owner
            .set_budget(Some(owner.usage().used + layout.size() - 1))
            .unwrap();
    }
}
fn seed(path: &Path, limit: usize) {
    let database = crate::Database::open(path).unwrap();
    database.set_memory_limit(Some(limit)).unwrap();
    database
        .execute(
            "CREATE TABLE source_rows (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL",
            &HashMap::new(),
        )
        .unwrap();
    database
        .execute(
            "INSERT INTO source_rows VALUES (1, 'one'), (2, 'two')",
            &HashMap::new(),
        )
        .unwrap();
    database.close().unwrap();
}
fn open(path: &Path, limit: usize) -> contextdb_core::Result<ReadSession> {
    ReadSession::open_with_options(
        path,
        ReadSessionOptions {
            memory_limit: Some(limit),
            ..Default::default()
        },
    )
}
fn request_credit(request: (usize, usize, usize), cuts: &[Cut; 3]) -> usize {
    for cut in cuts.iter().rev() {
        assert!(
            cut.count <= cut.requests.len(),
            "fixed allocator receipt must not overflow"
        );
        if let Some(index) = cut.requests[..cut.count]
            .iter()
            .rposition(|item| item.0 == request.0)
        {
            assert_eq!(
                cut.requests[index], request,
                "latest live pointer belongs to the real producer Layout"
            );
            return cut.credit[index];
        }
    }
    panic!("the actual native allocator must observe the producer pointer")
}

// New family: existing cache rows use replacement/legacy payers. They cannot
// detect M/P/I debit after allocation, or original B refund while a real cached
// page survives ReadSession. Decisive mutants are enumerated in the evidence.
#[test]
#[serial_test::serial]
fn direct_session_native_sources_prepay_and_keep_original_invoices_until_last_physical_free() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    for limit in [64 * 1024 * 1024, 256 * 1024 * 1024] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.db");
        seed(&path, limit);
        // Watch all index backings in two bounded cuts of the existing 128-slot
        // free oracle. Both cuts reopen the same real file and retain a real page.
        for chunk in [0..100, 100..140] {
            for (unwind, retain_write) in
                [(false, false), (true, false), (false, true), (true, true)]
            {
                let observation = Observation::begin();
                let reader = open(&path, limit).unwrap();
                let owner = observation.owner();
                let source = reader.file_cache_index_charge_for_test().unwrap();
                assert_eq!(source.payer_identity, owner.as_ptr() as usize);
                assert_eq!(owner.writer_registration_observation_for_test().2, 0);
                assert_eq!(owner.retained_charge_bytes_for_test(), 0);
                let (page_cap, fixed) =
                    read_source_cache_reservation_for_limit_for_test(Some(limit));
                let base = page_cap + fixed;
                let m = source.source_control.1;
                let p = source.preparation.1;
                let r = source.retained_control.1;
                let i = source.index_bytes;
                let total = base + m + p + i;
                assert_eq!(source.remaining_base, base - r);
                assert_eq!(source.preparation_invoice, p + i);
                assert_eq!(
                    source.allocations[..source.allocation_count]
                        .iter()
                        .map(|a| a.1)
                        .sum::<usize>(),
                    i
                );
                let cuts = CUTS.get();
                assert_eq!(INVOICE_COUNT.get(), source.allocation_count);
                let mut invoices = INVOICES.get();
                let mut layouts = [(0, 0); 140];
                for (slot, request) in layouts
                    .iter_mut()
                    .zip(source.allocations[..source.allocation_count].iter())
                {
                    *slot = (request.1, request.2);
                }
                invoices[..source.allocation_count].sort_unstable();
                layouts[..source.allocation_count].sort_unstable();
                assert_eq!(
                    invoices[..source.allocation_count],
                    layouts[..source.allocation_count],
                    "different native invoices conserve the actual producer Layouts once"
                );
                assert_eq!(cuts[0].count, 1, "actual source shared control allocation");
                assert_eq!(
                    cuts[2].count, 1,
                    "actual retained source control allocation"
                );
                assert_eq!(
                    request_credit(source.source_control, &cuts),
                    cuts[0].standing + base + m
                );
                assert_eq!(
                    request_credit(source.preparation, &cuts),
                    cuts[1].standing + p
                );
                assert_eq!(
                    request_credit(source.retained_control, &cuts),
                    cuts[2].standing
                );
                let (before, after, transferred) =
                    TRANSFER.get().expect("actual source transfer observation");
                assert_eq!(transferred, r);
                assert_eq!(before[0] - after[0], r);
                assert_eq!(before[1] - after[1], r);
                assert_eq!(
                    before[2], after[2],
                    "R transfers without refund or readmission"
                );
                // Follow actual allocation order; every previous native backing
                // remains paid when the next one reaches System.alloc.
                let mut native_live = 0;
                for index in 0..cuts[1].count {
                    let request = cuts[1].requests[index];
                    if source.allocations[..source.allocation_count].contains(&request)
                        && cuts[1].requests[..cuts[1].count]
                            .iter()
                            .rposition(|item| item.0 == request.0)
                            == Some(index)
                    {
                        native_live += request.1;
                        assert!(
                            cuts[1].credit[index] >= cuts[1].standing + p + native_live,
                            "incremental native funding must precede every physical allocation"
                        );
                    }
                }
                assert_eq!(
                    native_live, i,
                    "every actual index/control pointer was measured"
                );
                assert_eq!(
                    reader
                        .execute(
                            "SELECT id, body FROM source_rows ORDER BY id",
                            &HashMap::new()
                        )
                        .unwrap()
                        .rows,
                    vec![
                        vec![Value::Int64(1), Value::Text("one".into())],
                        vec![Value::Int64(2), Value::Text("two".into())]
                    ]
                );
                let held = reader.file_cached_header_for_test();
                let clone = held.clone();
                let header: [u8; 320] = held.bytes().try_into().unwrap();
                let payment = held.payment();
                assert_eq!(payment.0, "cache");
                assert_eq!(
                    payment.1, source.allocations[0].0,
                    "the page retains the original native counter"
                );
                let page = held.allocation();
                let write = reader.file_write_control_for_test();
                let write_request = write.allocation();
                assert_eq!(
                    write_request, source.allocations[2],
                    "borrow the actual original CacheShared control"
                );
                let write_alias = if retain_write {
                    Some(write)
                } else {
                    drop(write);
                    None
                };
                let mut requests = [(0, 0, 0); 105];
                requests[..5].copy_from_slice(&[
                    source.source_control,
                    source.preparation,
                    source.retained_control,
                    page,
                    write_request,
                ]);
                let mut count = 5;
                for request in source.allocations[chunk.start.min(source.allocation_count)
                    ..chunk.end.min(source.allocation_count)]
                    .iter()
                {
                    if *request != write_request {
                        requests[count] = *request;
                        count += 1;
                    }
                }
                let counter = owner.writer_registration_observation_for_test().4;
                oracle::watch_exact_frees_for_test(&requests[..count], counter);
                drop(reader);
                let after_close = owner.usage().used;
                let free = oracle::source_free_for_test(count);
                assert_eq!(free[0].2, 0);
                assert_eq!(free[1].2, 0);
                assert_eq!(free[2].2, 1);
                assert_eq!(
                    free[2].0,
                    after_close + r,
                    "R stays charged until its actual shared backing is freed"
                );
                assert_eq!(free[2].0, free[2].1);
                assert_eq!(free[3].2, 0);
                assert_eq!(free[4].2, usize::from(!retain_write));
                // The proven-live source cell still owns this same ImageMemory.
                // Its actual held counter includes the separately charged image,
                // whose release is intentionally not confused with B/M/P/I.
                let image_held = unsafe {
                    &*(IMAGE_POINTER.get() as *const crate::read_image_memory::ImageMemory)
                }
                .held_for_test();
                assert!(image_held >= base - r);
                assert_eq!(
                    after_close,
                    image_held + m + p + i,
                    "the original payer reconciles its actual image-held counter and the separate source/native controls"
                );
                assert!(
                    after_close >= total - r,
                    "B-R, M, P and I remain funded while the real page alias survives close"
                );
                drop(held);
                assert_eq!(owner.usage().used, after_close);
                assert_eq!(oracle::source_free_for_test(count)[3].2, 0);
                assert_eq!(clone.payment().1, payment.1);
                assert_eq!(clone.bytes(), &header);
                if unwind {
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                            let _last_page = clone;
                            panic!("release the actual final native alias during unwind");
                        }))
                        .is_err()
                    );
                } else {
                    std::thread::scope(|scope| {
                        scope.spawn(move || drop(clone)).join().unwrap();
                    });
                }
                if retain_write {
                    assert_eq!(
                        owner.usage().used,
                        p + i,
                        "after the final page frees, the retained actual CacheShared alone owns P and I"
                    );
                    let free = oracle::source_free_for_test(count);
                    assert_eq!(free[0].2, 1);
                    assert_eq!(free[1].2, 0);
                    assert_eq!(free[4].2, 0);
                    assert_eq!(write_alias.as_ref().unwrap().allocation(), write_request);
                }
                if unwind {
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                            let _last_write_control = write_alias;
                            panic!("release the final actual CacheShared during unwind");
                        }))
                        .is_err()
                    );
                } else {
                    std::thread::scope(|scope| {
                        scope.spawn(move || drop(write_alias)).join().unwrap();
                    });
                }
                let free = oracle::source_free_for_test(count);
                assert_eq!(
                    free[0].0,
                    m + if retain_write { p + i } else { 0 },
                    "M remains paid at its exact physical free"
                );
                assert_eq!(
                    free[1].0,
                    if retain_write { p + i } else { after_close },
                    "the original P+I invoice remains intact at the preparation's physical free"
                );
                assert_eq!(
                    free[3].0, after_close,
                    "the original cached page remains fully paid through its physical free"
                );
                if retain_write {
                    assert_eq!(
                        free[4].0,
                        p + i,
                        "the last CacheShared backing frees before its retained invoice"
                    );
                }
                for (index, cut) in free[..count].iter().enumerate() {
                    assert_eq!(cut.2, 1, "each actual backing frees exactly once");
                    assert_eq!(
                        cut.0, cut.1,
                        "original invoice survives System.dealloc, index {index}"
                    );
                    assert!(cut.0 >= requests[index].1, "physical free precedes refund");
                }
                oracle::disarm_source_free_watch();
                assert_eq!(
                    owner.usage().used,
                    0,
                    "no original-source residue after final physical release"
                );
                drop(observation);
            }
        }
    }
}

// Separate denial family: the positive/free row cannot falsify allocation on
// an admission refusal or allocator null. Use the same actual open path and
// retry its original store; never replace the payer with a stand-in meter.
#[test]
#[serial_test::serial]
fn direct_session_source_constructor_refusals_and_native_nulls_leave_no_original_charge() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source-errors.db");
    const LIMIT: usize = 64 * 1024 * 1024;
    seed(&path, LIMIT);
    let observation = Observation::begin();
    let reader = open(&path, LIMIT).unwrap();
    let source = reader.file_cache_index_charge_for_test().unwrap();
    let successful_cuts = CUTS.get();
    drop(reader);
    assert_eq!(observation.owner().usage().used, 0);
    drop(observation);
    let (page_cap, fixed) = read_source_cache_reservation_for_limit_for_test(Some(LIMIT));
    let base = page_cap + fixed;
    // Exact original-owner cuts on its actual producer. A public memory limit
    // also changes the cache cap, so it cannot isolate P/I/R by subtraction.
    for phase in [0, 1] {
        let memory = crate::read_image_memory::ImageMemory::new(None)
            .expect("unlimited direct-source fixture image owner");
        let m = SourceShared::<SourceMemoryReservation>::layout().size();
        memory
            .accountant
            .set_budget(Some(if phase == 0 { m - 1 } else { m + base - 1 }))
            .unwrap();
        let result = SourceMemoryReservation::new(memory.clone(), base);
        assert!(result.is_err());
        drop(result);
        assert_eq!(memory.accountant.usage().used, 0);
    }
    for denied in 0..=source.allocation_count {
        let observation = Observation::begin();
        if denied == source.allocation_count {
            DENY_PREPARATION.set(true);
        } else {
            DENY_INVOICE.set(Some(denied));
        }
        let failed = open(&path, LIMIT);
        assert!(
            failed.is_err(),
            "the actual original payer refuses the selected preallocation invoice"
        );
        drop(failed);
        assert_eq!(observation.owner().usage().used, 0);
        if denied == source.allocation_count {
            assert_eq!(
                CUTS.get()[1].count,
                0,
                "P denial precedes the native preparation allocation"
            );
            assert_eq!(INVOICE_COUNT.get(), 0);
        } else {
            assert_eq!(
                INVOICE_COUNT.get(),
                denied + 1,
                "no invoice or producer after the refused callback"
            );
        }
        drop(observation);
        let retry = open(&path, LIMIT).unwrap();
        assert_eq!(
            retry
                .execute("SELECT COUNT(*) FROM source_rows", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Int64(2)]]
        );
        drop(retry);
    }
    for target in [1, 2] {
        let observation = Observation::begin();
        PANIC_INVOICE.set(Some(target));
        let failed = open(&path, LIMIT);
        assert!(failed.is_err(), "the production native-open boundary catches constructor unwind");
        drop(failed);
        assert_eq!(INVOICE_COUNT.get(), target + 1);
        assert!(CUTS.get()[1].count >= target + 1,
            "P and the earlier actual cache counters allocated before constructor unwind");
        assert_eq!(observation.owner().usage().used, 0,
            "caught native constructor unwind returns the original source invoices");
        drop(observation);
        let retry = open(&path, LIMIT).unwrap();
        assert_eq!(retry.execute("SELECT COUNT(*) FROM source_rows", &HashMap::new()).unwrap().rows,
            vec![vec![Value::Int64(2)]]);
        drop(retry);
    }
    for request in std::iter::once(source.source_control)
        .chain(std::iter::once(source.preparation))
        .chain(std::iter::once(source.retained_control))
        .chain(
            source.allocations[..source.allocation_count]
                .iter()
                .copied(),
        )
    {
        let observation = Observation::begin();
        let (phase, ordinal) = successful_cuts
            .iter()
            .enumerate()
            .find_map(|(phase, cut)| {
                cut.requests[..cut.count]
                    .iter()
                    .rposition(|item| item.0 == request.0)
                    .filter(|ordinal| cut.requests[*ordinal] == request)
                    .map(|ordinal| (phase, ordinal))
            })
            .expect("target must be an actual production request");
        let cut = successful_cuts[phase];
        let preceding_matches = cut.requests[..ordinal]
            .iter()
            .filter(|item| item.1 == request.1 && item.2 == request.2)
            .count();
        NULL_TARGET.set(Some((phase, request.1, request.2, preceding_matches)));
        let failed = open(&path, LIMIT);
        assert!(
            !crate::vector_read_memory::native_ownership_proof::refusal_armed_for_test(),
            "the actual constructor must consume the targeted allocator-null cut"
        );
        assert!(
            failed.is_err(),
            "native null must refuse actual direct open"
        );
        drop(failed);
        assert_eq!(
            observation.owner().usage().used,
            0,
            "constructor rollback returns the original source invoices"
        );
        drop(observation);
        let retry = open(&path, LIMIT).unwrap();
        assert_eq!(
            retry
                .execute("SELECT COUNT(*) FROM source_rows", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Int64(2)]]
        );
        drop(retry);
    }
}
