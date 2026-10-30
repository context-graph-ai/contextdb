#![cfg(feature = "test-seams")]
//! Calibration reads a real unpublished catalog while ordinary readers keep the old generation.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::{Result, VectorIndexRef};
use contextdb_vector::ivf::storage::{
    BuildPhase, BuildState, DistanceKind, Manifest, PAGE_BYTES, PageKey, ReadPages, Verdict,
};
use contextdb_vector::ivf::{ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, Vector, search};
use std::sync::Arc;

struct Candidate<'a>(&'a ivf_pages::Pages, Manifest);
impl ReadPages for Candidate<'_> {
    fn inspection_verdict(&self, p: u64, g: u64) -> Option<Verdict> {
        self.0.inspection_verdict(p, g)
    }
    fn record_inspection_verdict(&self, p: u64, g: u64, v: Verdict) {
        self.0.record_inspection_verdict(p, g, v);
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        self.0.snapshot_lsn()
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.0.index_identity()
    }
    fn check_cancel(&self) -> Result<()> {
        self.0.check_cancel()
    }
    fn before_distance(&self, kind: DistanceKind) -> Result<()> {
        self.0.before_distance(kind)
    }
    fn manifest(&self, p: u64) -> Result<Option<Manifest>> {
        Ok((p == self.1.partition).then_some(self.1))
    }
    fn build_state(&self, p: u64) -> Result<Option<BuildState>> {
        self.0.build_state(p)
    }
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>> {
        Ok(after
            .is_none_or(|p| p < self.1.partition)
            .then_some(self.1.partition))
    }
    fn read_admitted_page(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.0.read_admitted_page(key, out)
    }
}
fn rows(source: &impl ReadPages, policy: Policy, budget: &Arc<ivf_pages::Budget>) -> Vec<u64> {
    let mut out = [Hit::default(); 32];
    let (n, _) = search(
        source,
        policy,
        &budget.clone().into(),
        &[1.0, 0.0],
        Some(&[0]),
        Mode::Indexed,
        &mut out,
    )
    .unwrap();
    let mut rows = out[..n].iter().map(|h| h.row).collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}
#[test]
fn preparation_keeps_the_old_generation_serving_and_publication_includes_intervening_writes() {
    for population in [0, 8] {
        let budget = Arc::new(ivf_pages::Budget::default());
        let policy = Policy::checked(2, 2, 2, 32, 192, true)
            .unwrap()
            .with_coalesce_pages_per_step(4)
            .unwrap();
        let training = TrainingPolicy {
            iterations: 1,
            reservoir_samples: 16,
            seed: 7,
            ingest_batch_keys: 4,
            phase_batch_entries: 4,
        };
        let mut index = ClusteredIndex::new(
            ivf_pages::Pages::default(),
            policy,
            training,
            budget.clone().into(),
        )
        .unwrap();
        index
            .build_partition(0, (0..population).map(|id| (id, [1.0, id as f32 / 100.0])))
            .unwrap();
        let old = index.storage().manifest(0).unwrap().unwrap();
        let historical = index.storage().clone();
        let mut build = index.begin_build(0).unwrap();
        let mut input = (0..population).map(|id| (id, [1.0, id as f32 / 100.0]));
        while index.ingest_batch(&mut build, &mut input).unwrap() != 0 {}
        index.finish_ingestion(&mut build).unwrap();
        let mut prepared = false;
        for _ in 0..32 {
            if index.advance_build_preparation(&mut build).unwrap() {
                prepared = true;
                break;
            }
            assert_eq!(index.storage().manifest(0).unwrap(), Some(old));
        }
        assert!(prepared);
        assert_eq!(build.phase, BuildPhase::Publish);
        assert_eq!(index.storage().manifest(0).unwrap(), Some(old));
        let mut view = None;
        for _ in 0..32 {
            view = index.prepare_publication_view(&build).unwrap();
            if view.is_some() {
                break;
            }
        }
        let candidate = view.unwrap();
        assert_ne!(candidate.generation, old.generation);
        assert_eq!(index.storage().manifest(0).unwrap(), Some(old));
        assert_eq!(index.storage().build_state(0).unwrap(), Some(build));
        let capture = index.storage().clone();
        assert_eq!(
            rows(&Candidate(&capture, candidate), policy, &budget),
            (0..population).collect::<Vec<_>>()
        );
        assert_eq!(
            rows(index.storage(), policy, &budget),
            (0..population).collect::<Vec<_>>()
        );
        index
            .commit_insert(Vector {
                partition: 0,
                row: 99,
                values: &[0.0, 1.0],
            })
            .unwrap();
        let expected = (0..population).chain([99]).collect::<Vec<_>>();
        assert_eq!(rows(index.storage(), policy, &budget), expected);
        assert_eq!(
            rows(&Candidate(&capture, candidate), policy, &budget),
            (0..population).collect::<Vec<_>>()
        );
        // Ordinary writes extend the old manifest's slot extent without replacing its generation.
        let serving_after_write = index.storage().manifest(0).unwrap().unwrap();
        assert_eq!(serving_after_write.generation, old.generation);
        // A failed final publication leaves both the durable cursor and serving generation intact.
        index.storage().fail_next_write.set(true);
        assert!(index.advance_build(&mut build).is_err());
        assert_eq!(build.phase, BuildPhase::Publish);
        assert_eq!(
            index.storage().manifest(0).unwrap(),
            Some(serving_after_write)
        );
        assert_eq!(rows(index.storage(), policy, &budget), expected);
        let mut published = false;
        for _ in 0..32 {
            if index.advance_build(&mut build).unwrap() {
                published = true;
                break;
            }
        }
        assert!(published);
        assert_eq!(index.storage().manifest(0).unwrap(), Some(candidate));
        assert_eq!(rows(index.storage(), policy, &budget), expected);
        assert_eq!(
            rows(&historical, policy, &budget),
            (0..population).collect::<Vec<_>>()
        );
        drop(index);
        assert_eq!(budget.live.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
