#![cfg(feature = "test-seams")]
#[path = "support/hiding_pages.rs"]
mod hiding_pages;
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_vector::ivf::storage::{PAGE_BYTES, PageKind, PageStore, ReadPages, WritePages};
use contextdb_vector::ivf::{ClusteredIndex, Policy, TrainingPolicy, inspect_catalog};
use std::sync::Arc;
#[test]
fn maintenance_inspection_persists_damage_and_later_readers_use_only_the_verdict() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let p = Policy::checked(2, 4, 4, 64, 192, true).unwrap();
    let t = TrainingPolicy {
        iterations: 2,
        reservoir_samples: 128,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    };
    let mut index =
        ClusteredIndex::new(hiding_pages::Hiding::default(), p, t, budget.clone().into()).unwrap();
    index
        .build_partition(0, (0..16).map(|n| (n, [n as f32 + 1.0, 1.0])))
        .unwrap();
    let m = index.storage().manifest(0).unwrap().unwrap();
    let mut store = index.into_storage();
    let key = m.key(PageKind::Codebook, 0, 1);
    let mut raw = [0; PAGE_BYTES];
    assert!(store.read_admitted_page(key, &mut raw).unwrap());
    raw[0] ^= 1;
    let mut tx = store.begin().unwrap();
    tx.put_page(key, &raw).unwrap();
    tx.commit().unwrap();
    let mut index = ClusteredIndex::new(store, p, t, budget.into()).unwrap();
    // Read-only inspection reports known state; maintenance verifies and persists.
    assert_eq!(
        inspect_catalog(index.storage(), 0, 2)
            .unwrap()
            .unwrap()
            .recovery_action,
        None
    );
    index.storage().reads.borrow_mut().clear();
    assert_eq!(
        index.inspect_catalog(0).unwrap().unwrap().recovery_action,
        Some("rebuild")
    );
    assert!(
        index
            .storage()
            .reads
            .borrow()
            .keys()
            .any(|k| *k == PageKind::Codebook)
    );
    for writer_path in [false, true] {
        index.storage().reads.borrow_mut().clear();
        let state = if writer_path {
            index.inspect_catalog(0)
        } else {
            inspect_catalog(index.storage(), 0, 2)
        }
        .unwrap()
        .unwrap();
        assert_eq!(state.recovery_action, Some("rebuild"));
        assert!(index.storage().reads.borrow().keys().all(|k| !matches!(
            *k,
            PageKind::Codebook | PageKind::Directory | PageKind::DirectoryPresence
        )));
    }
    let index = ClusteredIndex::new(
        index.into_storage(),
        p,
        t,
        Arc::new(ivf_pages::Budget::default()).into(),
    )
    .unwrap();
    assert_eq!(
        inspect_catalog(index.storage(), 0, 2)
            .unwrap()
            .unwrap()
            .recovery_action,
        Some("rebuild")
    );
}
