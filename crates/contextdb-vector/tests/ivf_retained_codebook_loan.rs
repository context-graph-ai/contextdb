#![cfg(feature = "test-seams")]
//! A retained producer object supplies real values without moving reader funding.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::{Result, VectorIndexRef};
use contextdb_vector::ivf::storage::{
    BuildState, DistanceKind, Manifest, PAGE_BYTES, PageKey, PageKind, ReadPages, Verdict,
};
use contextdb_vector::ivf::{
    ClusteredIndex, Hit, Mode, Policy, RetainedCodebook, TrainingPolicy, search,
};
use std::{
    cell::Cell,
    sync::{Arc, atomic::Ordering},
};

struct Loan<'a> {
    pages: &'a ivf_pages::Pages,
    book: &'a RetainedCodebook,
    loans: Cell<usize>,
    codebook_reads: Cell<usize>,
}
impl ReadPages for Loan<'_> {
    fn retained_codebook(&self, _manifest: Manifest) -> Option<&RetainedCodebook> {
        self.loans.set(self.loans.get() + 1);
        Some(self.book)
    }
    fn inspection_verdict(&self, p: u64, g: u64) -> Option<Verdict> {
        self.pages.inspection_verdict(p, g)
    }
    fn record_inspection_verdict(&self, p: u64, g: u64, v: Verdict) {
        self.pages.record_inspection_verdict(p, g, v)
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        self.pages.snapshot_lsn()
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.pages.index_identity()
    }
    fn check_cancel(&self) -> Result<()> {
        self.pages.check_cancel()
    }
    fn before_distance(&self, k: DistanceKind) -> Result<()> {
        self.pages.before_distance(k)
    }
    fn manifest(&self, p: u64) -> Result<Option<Manifest>> {
        self.pages.manifest(p)
    }
    fn build_state(&self, p: u64) -> Result<Option<BuildState>> {
        self.pages.build_state(p)
    }
    fn next_partition(&self, p: Option<u64>) -> Result<Option<u64>> {
        self.pages.next_partition(p)
    }
    fn read_admitted_page(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        if key.kind == PageKind::Codebook {
            self.codebook_reads.set(self.codebook_reads.get() + 1);
            // An accidental fallback cannot obtain the values from these pages.
            return Ok(false);
        }
        self.pages.read_admitted_page(key, out)
    }
}
#[test]
fn retained_values_feed_the_real_reader_constructor_and_keep_their_original_owner_until_drop() {
    let build = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 1, 1, 8, 1024, true).unwrap();
    let training = TrainingPolicy {
        iterations: 1,
        reservoir_samples: 2,
        seed: 7,
        ingest_batch_keys: 2,
        phase_batch_entries: 2,
    };
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy,
        training,
        build.clone().into(),
    )
    .unwrap();
    index
        .build_partition(0, [(11, [1.0_f32, 0.0]), (37, [0.0, 1.0])].into_iter())
        .unwrap();
    let pages = index.into_storage();
    assert_eq!(build.live.load(Ordering::SeqCst), 0);
    let manifest = pages.manifest(0).unwrap().unwrap();
    let maintenance = Arc::new(ivf_pages::Budget::default());
    let retained = RetainedCodebook::load(&maintenance.clone().into(), &pages, manifest).unwrap();
    let held = retained.charged_bytes();
    assert!(held > 0);
    assert_eq!(maintenance.live.load(Ordering::SeqCst), held);
    let reader = Arc::new(ivf_pages::Budget::default());
    let mut baseline = [Hit::default(); 2];
    let (expected, work) = search(
        &pages,
        policy,
        &reader.clone().into(),
        &[1.0, 0.0],
        Some(&[0]),
        Mode::Indexed,
        &mut baseline,
    )
    .unwrap();
    assert!(work.codebook_pages_read > 0);
    assert_eq!(reader.live.load(Ordering::SeqCst), 0);
    let loan = Loan {
        pages: &pages,
        book: &retained,
        loans: Cell::new(0),
        codebook_reads: Cell::new(0),
    };
    for _ in 0..8 {
        let mut actual = [Hit::default(); 2];
        let (count, work) = search(
            &loan,
            policy,
            &reader.clone().into(),
            &[1.0, 0.0],
            Some(&[0]),
            Mode::Indexed,
            &mut actual,
        )
        .unwrap();
        assert_eq!(count, expected);
        for (a, b) in actual[..count].iter().zip(&baseline[..count]) {
            assert_eq!(a.row, b.row);
            assert_eq!(a.distance.to_bits(), b.distance.to_bits());
        }
        assert_eq!(work.codebook_pages_read, 0);
        assert_eq!(loan.codebook_reads.get(), 0);
        assert_eq!(reader.live.load(Ordering::SeqCst), 0);
        assert_eq!(maintenance.live.load(Ordering::SeqCst), held);
    }
    assert_eq!(loan.loans.get(), 8);
    assert!(reader.peak.load(Ordering::SeqCst) >= held);
    drop(loan);
    drop(retained);
    assert_eq!(maintenance.live.load(Ordering::SeqCst), 0);
}

#[test]
fn an_unclustered_legacy_generation_never_requests_a_retained_codebook_loan() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 1, 1, 8, 1024, true).unwrap();
    let training = TrainingPolicy {
        iterations: 1,
        reservoir_samples: 2,
        seed: 7,
        ingest_batch_keys: 2,
        phase_batch_entries: 2,
    };
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy,
        training,
        budget.clone().into(),
    )
    .unwrap();
    let rows = [(11, [1.0_f32, 0.0]), (37, [0.0, 1.0])];
    for (row, values) in &rows {
        index
            .commit_insert(contextdb_vector::ivf::Vector {
                partition: 0,
                row: *row,
                values,
            })
            .unwrap();
    }
    index.seal().unwrap();
    assert_eq!(
        index.storage().manifest(0).unwrap().unwrap().active_lists,
        0
    );
    index.build_partition(0, rows.into_iter()).unwrap();
    let pages = index.into_storage();
    let catalog = contextdb_vector::ivf::inspect_catalog(&pages, 0, 2)
        .unwrap()
        .unwrap();
    assert!(
        catalog.legacy_runs > 0,
        "the initial unclustered generation is still physically retained"
    );
    assert_eq!(catalog.retained_codebook, Some([0; 32]));
    assert!(
        contextdb_vector::ivf::retained_codebook_manifest(&pages, 0)
            .unwrap()
            .is_none(),
        "an unclustered legacy generation has no codebook to loan or reserve"
    );
    assert_eq!(budget.live.load(Ordering::SeqCst), 0);
}
