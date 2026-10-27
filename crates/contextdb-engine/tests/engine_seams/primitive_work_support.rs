//! Shared observations at the real engine search, decode and admission doors.
use contextdb_engine::database::resident_history_test_seams::observe as observe_resident;
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::memory_accounting::{
    take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use contextdb_engine::vector_read_memory::{
    take_native_admissions_for_test, watch_native_admissions_for_test,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::sync::{Arc, Mutex};

// Shared by all callers; unrelated global-recorder users are outside this scope.
static OBSERVATION: Mutex<()> = Mutex::new(());

struct RecorderCleanup;
impl Drop for RecorderCleanup {
    fn drop(&mut self) {
        take_budget_ledger_for_test();
        take_native_admissions_for_test();
    }
}
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct SearchWork {
    /// Page requests to the writer's page cache, hits and misses alike.
    pub page_requests: u64,
    /// Live and superseded membership records examined.
    pub membership_records: u64,
    pub decoded_rows: u64,
    /// Existing query-path decode notes; import uses stored_vector_decodes.
    pub decoded_vectors: u64,
    pub vector_body_accesses: u64,
    pub vectors_scored: u64,
    pub reader_admissions: u64,
    pub writer_budget_charges: u64,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct Work {
    pub search: SearchWork,
    /// None in a ceiling means diagnostic only; observations always contain counts.
    pub backend_reads: Option<u64>,
    pub backend_bytes: Option<u64>,
    pub stored_vector_decodes: usize,
    pub fresh_dirty_pages: u64,
    pub fresh_dirty_page_bytes: usize,
    pub native_pages: Option<u64>,
    pub native_page_bytes: usize,
    pub other_native_pages: u64,
    pub other_native_page_bytes: usize,
    pub writer_before: usize,
    pub writer_peak: usize,
    pub writer_after: usize,
    /// Parent bytes after successful admissions, not allocated process bytes.
    pub reader_peak: Option<u64>,
}
struct CountScores(AtomicU64);
impl NativeVectorScoreObserver for CountScores {
    fn before_native_vector_score(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[expect(
    clippy::redundant_closure,
    reason = "The fixture keeps an explicit invocation boundary around the observed operation."
)]
pub(crate) fn observe_work<T>(
    db: &contextdb_engine::Database,
    operation: impl FnOnce() -> T,
) -> (T, Work) {
    let _serial = OBSERVATION.lock().unwrap_or_else(|err| err.into_inner());
    let writer_before = db.__reset_writer_peak_for_test();
    let reader = db
        .declared_read_memory_for_test()
        .witness_identity_for_test();
    let scored = Arc::new(CountScores(AtomicU64::new(0)));
    let cache_before = db.__writer_cache_stats_for_test().unwrap();
    let backend_reads = Arc::new(AtomicU64::new(0));
    let backend_bytes = Arc::new(AtomicU64::new(0));
    let count = backend_reads.clone();
    let bytes = backend_bytes.clone();
    let backend = redb::observe_backend_reads(Arc::new(move |size| {
        count.fetch_add(1, Ordering::SeqCst);
        bytes.fetch_add(size as u64, Ordering::SeqCst);
    }));
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    watch_native_admissions_for_test();
    watch_budget_ledger_for_test();
    let _cleanup = RecorderCleanup;
    let ((answer, resident), pages) = redb::observe_page_allocations_for_test(|| {
        observe_resident(|| {
            with_native_vector_score_observer_for_test(scored.clone(), || operation())
        })
    });
    drop(backend);
    let ledger = take_budget_ledger_for_test();
    let admissions = take_native_admissions_for_test();
    let read = scope.counters();
    drop(scope);
    let cache_after = db.__writer_cache_stats_for_test().unwrap();
    let writer_peak = db.write_memory_high_water_for_test();
    let writer_after = db.__writer_charged_bytes_for_test();
    let reader_peak = admissions
        .iter()
        .filter(|w| w.parent == reader && w.admitted)
        .map(|w| w.retained_after)
        .max();
    let search = SearchWork {
        page_requests: (cache_after.0 + cache_after.1) - (cache_before.0 + cache_before.1),
        membership_records: resident.membership_keys_examined as u64,
        decoded_rows: read.decoded_rows,
        decoded_vectors: read.decoded_vectors,
        vector_body_accesses: read.vector_body_accesses,
        vectors_scored: scored.0.load(Ordering::SeqCst),
        reader_admissions: admissions
            .iter()
            .filter(|witness| witness.parent == reader && witness.admitted)
            .count() as u64,
        writer_budget_charges: ledger
            .iter()
            .filter(|entry| entry.what != "release")
            .count() as u64,
    };
    (
        answer,
        Work {
            search,
            backend_reads: Some(backend_reads.load(Ordering::SeqCst)),
            backend_bytes: Some(backend_bytes.load(Ordering::SeqCst)),
            stored_vector_decodes: resident.stored_vector_decodes,
            fresh_dirty_pages: pages.fresh_dirty_pages,
            fresh_dirty_page_bytes: pages.fresh_dirty_page_bytes,
            native_pages: Some(pages.native_pages),
            native_page_bytes: pages.native_page_bytes,
            other_native_pages: pages.other_native_pages,
            other_native_page_bytes: pages.other_native_page_bytes,
            writer_before,
            writer_peak,
            writer_after,
            reader_peak,
        },
    )
}

impl Work {
    /// Add the declared cache policy and its native index to the measured
    /// fixed controls and operation workspace allowances in a byte ceiling.
    pub fn with_declared_writer_cache(mut self, db: &contextdb_engine::Database) -> Self {
        let (capacity, _) =
            contextdb_engine::persistence::read_source_cache_reservation_for_limit_for_test(
                db.memory_limit(),
            );
        let reserve = capacity + db.__writer_cache_index_bytes_for_test().unwrap();
        self.writer_before += reserve;
        self.writer_peak += reserve;
        self.writer_after += reserve;
        self
    }

    /// Price the native cache-read and dirty backings with their distinct
    /// source receipts. Physical work is capped independently of billing.
    pub fn with_native_page_invoices(
        self,
        db: &contextdb_engine::Database,
        observed: &Self,
        fresh_page_allowance: u64,
        cache_read_page_allowance: u64,
    ) -> Self {
        let mut ceiling = self.with_declared_writer_cache(db);
        ceiling.search.writer_budget_charges += 2 * observed
            .native_pages
            .expect("the producer observation is present");
        ceiling.writer_peak += observed.native_page_bytes;
        ceiling.fresh_dirty_pages = fresh_page_allowance;
        ceiling.native_pages = Some(cache_read_page_allowance + fresh_page_allowance);
        ceiling
    }

    pub fn assert_within(&self, label: &str, ceiling: &Self) {
        macro_rules! cap {
            ($observed:expr, $limit:expr, $($field:ident),+ $(,)?) => {$(
                assert!($observed.$field <= $limit.$field,
                    "{label}: {} grew: observed {:?}, ceiling {:?}", stringify!($field), self, ceiling);
            )+};
        }
        cap!(self, ceiling, fresh_dirty_pages);
        if let Some(limit) = ceiling.native_pages {
            assert!(
                self.native_pages.is_some_and(|pages| pages <= limit),
                "{label}: native physical page work grew: observed {self:?}, ceiling {ceiling:?}"
            );
        }
        cap!(
            self.search,
            ceiling.search,
            page_requests,
            membership_records,
            decoded_rows,
            decoded_vectors,
            vector_body_accesses,
            vectors_scored,
            reader_admissions,
            writer_budget_charges
        );
        cap!(
            self,
            ceiling,
            stored_vector_decodes,
            writer_before,
            writer_peak,
            writer_after
        );
        for (name, actual, limit) in [
            ("backend_reads", self.backend_reads, ceiling.backend_reads),
            ("backend_bytes", self.backend_bytes, ceiling.backend_bytes),
        ] {
            if let Some(limit) = limit {
                assert!(
                    actual.is_some_and(|actual| actual <= limit),
                    "{label}: {name} grew: observed {self:?}, ceiling {ceiling:?}"
                );
            }
        }
        match (self.reader_peak, ceiling.reader_peak) {
            (Some(actual), Some(limit)) => assert!(
                actual <= limit,
                "{label}: native reader admission peak grew: {self:?}, ceiling {ceiling:?}"
            ),
            (None, None) => {}
            _ => panic!(
                "{label}: native reader admission observation changed: {self:?}, ceiling {ceiling:?}"
            ),
        }
    }
}
