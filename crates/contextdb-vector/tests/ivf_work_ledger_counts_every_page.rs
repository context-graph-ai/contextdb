#![cfg(feature = "test-seams")]
//! A search's work ledger counts every page it reads: list, canonical,
//! visibility, codebook and directory pages each match the pages the page
//! port actually served for that kind, and a search reads no accumulator
//! page.
#[path = "support/hiding_pages.rs"]
mod hiding_pages;
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_vector::ivf::storage::PageKind;
use contextdb_vector::ivf::{ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, search};
use hiding_pages::Hiding;
use std::collections::BTreeMap;
use std::sync::Arc;

fn angle(i: u64) -> Vec<f32> {
    let x = i as f64 * 0.02;
    vec![x.cos() as f32, x.sin() as f32]
}

#[test]
fn the_work_ledger_counts_codebook_directory_and_every_other_page_read() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 4, 2, 8, 1_024, false).expect("a valid policy");
    let training = TrainingPolicy {
        iterations: 2,
        reservoir_samples: 300_000,
        seed: 11,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    };
    let mut index = ClusteredIndex::new(Hiding::default(), policy, training, budget.clone().into())
        .expect("open the index");
    let rows: BTreeMap<u64, Vec<f32>> = (0..64).map(|n| (n, angle(n))).collect();
    index
        .build_partition(0, rows.iter().map(|(id, v)| (*id, v.as_slice())))
        .expect("build the partition");
    for mode in [Mode::Indexed, Mode::Exact] {
        index.storage().reads.borrow_mut().clear();
        let mut out = [Hit::default(); 5];
        let (_, work) = search(
            index.storage(),
            policy,
            &budget.clone().into(),
            &angle(9),
            None,
            mode,
            &mut out,
        )
        .expect("search");
        let reads = index.storage().reads.borrow().clone();
        let read = |kind: PageKind| reads.get(&kind).copied().unwrap_or(0);
        assert_eq!(
            work.list_pages_read,
            read(PageKind::Lists),
            "{mode:?}: list pages: {reads:?}"
        );
        assert_eq!(
            work.canonical_pages_read,
            read(PageKind::Canonical),
            "{mode:?}: canonical pages"
        );
        assert_eq!(
            work.visibility_pages_read,
            read(PageKind::Visibility),
            "{mode:?}: visibility pages"
        );
        assert_eq!(
            work.codebook_pages_read,
            read(PageKind::Codebook),
            "{mode:?}: codebook pages"
        );
        assert_eq!(
            work.directory_pages_read,
            read(PageKind::Directory),
            "{mode:?}: directory pages"
        );
        assert_eq!(
            read(PageKind::Accumulators),
            0,
            "{mode:?}: a search reads no accumulator page"
        );
        if mode == Mode::Indexed {
            assert!(
                work.codebook_pages_read > 0,
                "an indexed search reads its codebook"
            );
            assert!(
                work.directory_pages_read > 0,
                "an indexed search reads its probed lists' directory"
            );
        }
    }
}
