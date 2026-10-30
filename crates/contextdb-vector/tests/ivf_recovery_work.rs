#![cfg(feature = "test-seams")]
#[path = "support/hiding_pages.rs"]
mod hiding_pages;
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_vector::ivf::storage::{PAGE_BYTES, PageKind, PageStore, ReadPages, WritePages};
use contextdb_vector::ivf::{
    ClusteredIndex, Hit, Mode, Policy, RowIdentity, TrainingPolicy, Vector, Work, search_with_work,
};
use std::sync::Arc;
fn policy() -> Policy {
    Policy::checked(2, 4, 4, 256, 192, true)
        .unwrap()
        .with_max_delta_runs(1)
        .unwrap()
        .with_coalesce_pages_per_step(26)
        .unwrap()
}
fn training() -> TrainingPolicy {
    TrainingPolicy {
        iterations: 2,
        reservoir_samples: 128,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    }
}
fn vector(n: u64) -> [f32; 2] {
    let a = n as f32 * 0.031;
    [a.cos(), a.sin()]
}
#[test]
fn coalescing_counts_all_payload_reads_and_writes_inside_its_declared_step() {
    let b = Arc::new(ivf_pages::Budget::default());
    let mut i = ClusteredIndex::new(
        hiding_pages::Hiding::default(),
        policy(),
        training(),
        b.into(),
    )
    .unwrap();
    i.build_partition(0, (0..16).map(|n| (n, vector(n))))
        .unwrap();
    for row in 16..25 {
        i.commit_insert(Vector {
            partition: 0,
            row,
            values: &vector(row),
        })
        .unwrap();
        i.seal().unwrap();
    }
    let mut restored = false;
    for _ in 0..32 {
        i.storage().reads.borrow_mut().clear();
        i.storage().inner.writes.borrow_mut().clear();
        let done = i.advance_coalescing(0, 26).unwrap();
        let reads: u64 = i.storage().reads.borrow().values().sum();
        let writes: u64 = i.storage().inner.writes.borrow().values().sum();
        assert!(
            reads + writes <= 26,
            "actual payload reads{reads}+writes{writes} exceed26"
        );
        if done {
            restored = true;
            break;
        }
    }
    assert!(restored);
}
#[test]
fn auto_fallback_and_failed_indexed_work_are_production_readable() {
    let b = Arc::new(ivf_pages::Budget::default());
    let mut i = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy(),
        training(),
        b.clone().into(),
    )
    .unwrap();
    i.build_partition(0, (0..16).map(|n| (n, vector(n))))
        .unwrap();
    let m = i.storage().manifest(0).unwrap().unwrap();
    let mut storage = i.into_storage();
    let mut page = [0; PAGE_BYTES];
    let key = m.key(PageKind::Codebook, 0, 1);
    assert!(storage.read_admitted_page(key, &mut page).unwrap());
    page[0] ^= 1;
    let mut tx = storage.begin().unwrap();
    tx.put_page(key, &page).unwrap();
    tx.commit().unwrap();
    let mut work = Work::default();
    let mut out = [Hit::default(); 32];
    assert!(matches!(
        search_with_work(
            &storage,
            policy(),
            &b.clone().into(),
            &vector(7),
            None,
            Mode::Indexed,
            &mut out,
            &mut work
        ),
        Err(contextdb_core::Error::StoreCorrupted { .. })
    ));
    assert!(work.codebook_pages_read > 0);
    assert_eq!(work.exact_score_count, 0);
    let n = search_with_work(
        &storage,
        policy(),
        &b.clone().into(),
        &vector(7),
        None,
        Mode::Auto,
        &mut out,
        &mut work,
    )
    .unwrap();
    assert_eq!(n, 16);
    assert_eq!(out[0].row, 7);
    let mut ids = out[..n].iter().map(|x| x.row).collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, (0..16).collect::<Vec<_>>());
    assert_eq!(work.exact_score_count, 16);
    assert_eq!(work.distance_evaluations(), 16);
    assert!(work.codebook_pages_read > 0);
    assert!(work.canonical_pages_read > 0);
}
#[test]
fn versioned_ingestion_keeps_source_identities_instead_of_the_source_frontier() {
    let b = Arc::new(ivf_pages::Budget::default());
    let mut storage = ivf_pages::Pages::default();
    for _ in 0..20 {
        storage.begin().unwrap().commit().unwrap();
    }
    let mut i = ClusteredIndex::new(storage, policy(), training(), b.into()).unwrap();
    let mut state = i.begin_build(0).unwrap();
    let identities = [
        RowIdentity {
            row: 10,
            commit_lsn: 3,
        },
        RowIdentity {
            row: 11,
            commit_lsn: 7,
        },
        RowIdentity {
            row: 12,
            commit_lsn: 13,
        },
    ];
    let mut source = identities.into_iter().map(|id| (id, vector(id.row)));
    while i.ingest_versioned_batch(&mut state, &mut source).unwrap() > 0 {}
    i.finish_ingestion(&mut state).unwrap();
    while !i.advance_build(&mut state).unwrap() {}
    for identity in identities {
        assert_eq!(
            i.current_row_identity(0, identity.row).unwrap(),
            Some(identity)
        );
        i.commit_update(0, identity, &vector(identity.row + 100))
            .unwrap();
    }
}
