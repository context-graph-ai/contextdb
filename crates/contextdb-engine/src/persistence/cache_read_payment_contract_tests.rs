//! Independent guards: the genuine cache reserve pays before page construction.
use super::*;
use crate::memory_accounting::MemoryAccountant;
use redb::StorageBackend;
use redb::page_payer_observation::{Observation, Payer, Phase};
use serial_test::serial;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default, Debug)]
struct IoState {
    reads: AtomicUsize,
    fail: AtomicBool,
}
#[derive(Debug)]
struct Backend {
    inner: redb::backends::InMemoryBackend,
    state: Arc<IoState>,
}
impl StorageBackend for Backend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.state.reads.fetch_add(1, Ordering::SeqCst);
        if self.state.fail.load(Ordering::SeqCst) {
            return Err(io::Error::other("independent cache-read I/O failure"));
        }
        self.inner.read(offset, out)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
    fn write(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.write(offset, bytes)
    }
}
thread_local! {
    static EVENTS: std::cell::Cell<([Option<Observation>; 256], usize, bool)> =
        const { std::cell::Cell::new(([None; 256], 0, false)) };
}
fn record(event: Observation) {
    EVENTS.with(|slot| {
        let (mut events, count, overflow) = slot.get();
        if count < events.len() {
            events[count] = Some(event);
        }
        slot.set((
            events,
            count.saturating_add(1),
            overflow || count >= events.len(),
        ));
    });
    if event.phase == Phase::Allocated && event.counter_value != 0 {
        crate::persistence::content_identity::proof_allocation_oracle_tests::watch_exact_free_for_test(
            (event.pointer, event.bytes, event.alignment), event.counter_value);
    }
}
fn begin() {
    EVENTS.with(|slot| slot.set(([None; 256], 0, false)));
}
fn events() -> Vec<Observation> {
    let (events, count, overflow) = EVENTS.with(std::cell::Cell::get);
    assert!(!overflow, "an incomplete payer observation never qualifies");
    events[..count].iter().map(|event| event.unwrap()).collect()
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn with_fixture(
    action: impl FnOnce(redb::CachePressureFixtureForTest, redb::CacheReserveOwner, Arc<IoState>),
) {
    let writer = MemoryAccountant::writer_budget(8 * 1024 * 1024).unwrap();
    let owner = writer.accountant();
    let baseline = owner.usage().used;
    let funder = writer.prepare_source_funder().unwrap();
    let original = writer.reserve_assembly_original(&funder).unwrap();
    let scope = StorageAdmissionScope::writer_original_native(&original).unwrap();
    let state = Arc::new(IoState::default());
    let backend = redb::backends::InMemoryBackend::new();
    backend.set_len(64 * 4096).unwrap();
    let fixture = scope
        .run_native(|| {
            redb::CachePressureFixtureForTest::with_backend_for_test(Box::new(Backend {
                inner: backend,
                state: state.clone(),
            }))
            .map_err(|error| contextdb_core::Error::Other(error.to_string().into()))
        })
        .unwrap();
    let (capacity, index) = fixture.capacity_and_index();
    let reserve =
        crate::persistence::cache_reserve::original(&owner, capacity + index, false).unwrap();
    let controls = fixture.retain_reserve(reserve);
    // The actual read has neither native nor legacy source admission. Its
    // sole possible payer is this installed real fixed cache reserve.
    action(fixture, controls, state);
    drop(scope);
    drop(original);
    drop(funder);
    assert_eq!(owner.usage().used, baseline, "all original funding settles");
}
fn assert_fixed(event: &Observation, bytes: usize) {
    assert_eq!(
        event.payer,
        Payer::Cache,
        "actual page payer at {:?}",
        event.phase
    );
    assert!(
        event.fixed && event.reserve_identity != 0,
        "actual fixed reserve is bound"
    );
    assert_eq!(event.bytes, bytes);
    assert_eq!(
        event.occupancy, bytes,
        "exactly one occupancy claim at {:?}",
        event.phase
    );
    assert_eq!(event.fixed_occupancy, bytes);
}
fn assert_free_before_refund(events: &[Observation], bytes: usize) {
    let freed = events
        .iter()
        .position(|event| event.phase == Phase::AfterFree)
        .expect("physical free observed");
    assert_fixed(&events[freed], bytes);
    let refund = events
        .iter()
        .position(|event| event.phase == Phase::BeforeRefund)
        .expect("refund observed");
    assert!(freed < refund, "physical free precedes occupancy refund");
    let refunded = events
        .iter()
        .find(|event| event.phase == Phase::AfterRefund)
        .unwrap();
    assert_eq!(refunded.occupancy, 0);
    let physical =
        crate::persistence::content_identity::proof_allocation_oracle_tests::source_free_for_test(
            1,
        )[0];
    assert_eq!(
        physical,
        (bytes, bytes, 1),
        "the independent allocator saw the original occupancy through System.dealloc"
    );
}

#[test]
#[serial]
fn cache_read_has_one_genuine_payment_before_allocation_io_and_handoff() {
    begin();
    with_fixture(|fixture, controls, state| {
        redb::page_payer_observation::observe(record, || {
            let page = fixture.read_one_for_test(1).unwrap();
            let bytes = page.allocation().1;
            let alias = page.clone();
            fixture.invalidate_one_for_test(1);
            drop(page);
            assert!(events().iter().all(|event| event.phase != Phase::AfterFree));
            drop(alias);
            let observed = events();
            let selected: Vec<_> = observed
                .iter()
                .filter(|event| {
                    matches!(
                        event.phase,
                        Phase::BeforeAllocation
                            | Phase::Allocated
                            | Phase::BeforeIo
                            | Phase::Handoff
                    )
                })
                .collect();
            assert!(selected.iter().any(|event| event.phase == Phase::BeforeIo));
            assert!(selected.iter().any(|event| event.phase == Phase::Handoff));
            for event in &selected {
                assert_fixed(event, bytes);
            }
            assert!(
                selected.iter().all(
                    |event| event.counter_identity == selected[0].counter_identity
                        && event.reserve_identity == selected[0].reserve_identity
                ),
                "same real loan throughout"
            );
            assert_eq!(
                observed
                    .iter()
                    .filter(|event| event.phase == Phase::Reserved)
                    .count(),
                1,
                "handoff must not reserve a second occupancy claim"
            );
            assert_free_before_refund(&observed, bytes);
            assert_eq!(controls.observation_for_test()[0].1, 0);
            assert_eq!(state.reads.load(Ordering::SeqCst), 1);
        });
        drop(controls);
        drop(fixture);
    });
}

#[test]
#[serial]
fn failed_cache_read_frees_the_page_before_rolling_back_its_same_payment() {
    begin();
    with_fixture(|fixture, controls, state| {
        state.fail.store(true, Ordering::SeqCst);
        redb::page_payer_observation::observe(record, || {
            let result = fixture.read_one_for_test(1);
            assert!(result.is_err(), "the real backend I/O fault is returned");
            let observed = events();
            let allocation = observed
                .iter()
                .find(|event| event.phase == Phase::BeforeAllocation)
                .unwrap();
            assert_fixed(allocation, allocation.bytes);
            assert_eq!(state.reads.load(Ordering::SeqCst), 1);
            assert_eq!(
                controls.observation_for_test()[0].1,
                0,
                "no provisional occupancy leaks"
            );
            assert_eq!(
                fixture.clean_resident(),
                0,
                "failed I/O publishes no cached page"
            );
            assert_free_before_refund(&observed, allocation.bytes);
        });
        drop(controls);
        drop(fixture);
    });
}

#[test]
#[serial]
fn cache_capacity_refusal_precedes_unpaid_allocation_and_backend_io() {
    begin();
    with_fixture(|fixture, controls, state| {
        let (capacity, _) = fixture.capacity_and_index();
        // Derive an extent larger than this actual reserve, without inventing
        // a second source payer or changing any declared cache default.
        let pages = capacity / 4096 + 1;
        redb::page_payer_observation::observe(record, || {
            let result = fixture.read_extent_for_test(pages);
            assert!(
                result.is_err(),
                "a page with neither source nor cache room must refuse"
            );
            assert_eq!(
                state.reads.load(Ordering::SeqCst),
                0,
                "refusal precedes I/O"
            );
            assert!(
                events().iter().all(|event| !matches!(
                    event.phase,
                    Phase::BeforeAllocation | Phase::Allocated | Phase::BeforeIo
                )),
                "refusal precedes page construction"
            );
            assert_eq!(
                controls.observation_for_test()[0].1,
                0,
                "the rejected claim rolls back"
            );
        });
        drop(controls);
        drop(fixture);
    });
}

/// A failed legacy scan read owes its unchanged growth allowance to retry;
/// a completed miss consumes the actual backing quote once, including when
/// the scan is resumed through a clone of the same existing allowance.
#[test]
#[serial]
fn failed_legacy_scan_read_preserves_growth_and_success_consumes_once() {
    let allowance = redb::ScanAllowance::new();
    let before = allowance
        .legacy_growth_for_test()
        .expect("actual legacy allowance");
    let resumed = allowance.clone();
    assert_eq!(resumed.legacy_growth_for_test(), Some(before));
    let mut failed_reads = 0;
    with_fixture(|fixture, controls, state| {
        {
            let _scan = allowance.enter();
            state.fail.store(true, Ordering::SeqCst);
            assert!(
                fixture.read_one_for_test(1).is_err(),
                "the real backend failure is returned"
            );
            assert_eq!(state.reads.load(Ordering::SeqCst), 1);
            assert_eq!(
                allowance.legacy_growth_for_test(),
                Some(before),
                "failed I/O leaves the exact original growth allowance intact"
            );
            assert_eq!(
                controls.observation_for_test()[0].1,
                0,
                "failed I/O leaves no cache occupancy"
            );
        }
        failed_reads = state.reads.load(Ordering::SeqCst);
        drop(controls);
        drop(fixture);
    });
    // A real I/O error permanently poisons that CachedFile. Continue through
    // a fresh authenticated backend/cache, retaining the SAME allowance Arc.
    with_fixture(|fixture, controls, state| {
        assert_eq!(resumed.legacy_growth_for_test(), Some(before));
        let consumed;
        {
            let _scan = resumed.enter();
            let page = fixture
                .read_one_for_test(1)
                .expect("retry succeeds under the same allowance");
            consumed = page.allocation().1;
            assert!(
                consumed <= before.1,
                "the actual allowance funds this one miss"
            );
            let expected = Some((before.0, before.1 - consumed));
            assert_eq!(
                allowance.legacy_growth_for_test(),
                expected,
                "one successful miss consumes its actual backing quote once"
            );
            let hit = fixture.read_one_for_test(1).expect("read the cached page");
            assert_eq!(
                hit.allocation(),
                page.allocation(),
                "the second read is the same cached backing"
            );
            assert_eq!(
                failed_reads + state.reads.load(Ordering::SeqCst),
                2,
                "only failed read and successful retry reached I/O"
            );
            assert_eq!(
                allowance.legacy_growth_for_test(),
                expected,
                "cache hits do not consume again"
            );
            drop(hit);
            drop(page);
        }
        fixture.invalidate_one_for_test(1);
        assert_eq!(
            allowance.legacy_growth_for_test(),
            Some((before.0, before.1 - consumed)),
            "a completed read's growth remains consumed across scan exit and physical free"
        );
        eprintln!(
            "LEGACY_SCAN_RETRY original={before:?} consumed={consumed} after={:?}",
            allowance.legacy_growth_for_test()
        );
        drop(controls);
        drop(fixture);
    });
}
