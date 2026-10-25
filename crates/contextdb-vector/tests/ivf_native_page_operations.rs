#![cfg(feature = "test-seams")]
//! Operation outcomes against the native page port, independent of SQL delegation.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::Error;
use contextdb_vector::ivf::storage::{PAGE_BYTES, PageKind, ReadPages};
use contextdb_vector::ivf::{ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, Vector, search};
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
};
fn angle(i: u64) -> Vec<f32> {
    let x = i as f64 * 0.02;
    vec![x.cos() as f32, x.sin() as f32]
}
fn fixture() -> (ClusteredIndex<ivf_pages::Pages>, Arc<ivf_pages::Budget>) {
    let budget = Arc::new(ivf_pages::Budget::default());
    let p = Policy::checked(2, 4, 2, 8, 1024, true).unwrap();
    let t = TrainingPolicy {
        iterations: 2,
        reservoir_samples: 300000,
        seed: 11,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    };
    let mut i =
        ClusteredIndex::new(ivf_pages::Pages::default(), p, t, budget.clone().into()).unwrap();
    let rows: BTreeMap<_, _> = (0..32).map(|n| (n, angle(n))).collect();
    i.build_partition(0, rows.iter().map(|(id, v)| (*id, v.as_slice())))
        .unwrap();
    (i, budget)
}
#[test]
fn cancellation_after_admission_releases_query_objects_and_the_store_keeps_answering() {
    let (i, budget) = fixture();
    let before = budget.live.load(Ordering::SeqCst);
    let mut out = [Hit {
        row: u64::MAX,
        distance: f64::INFINITY,
    }; 3];
    i.storage()
        .cancel_at_distance
        .set(Some(i.storage().comparisons.get() + 2));
    let error = search(
        i.storage(),
        i.policy(),
        &budget.clone().into(),
        &angle(7),
        None,
        Mode::Indexed,
        &mut out,
    )
    .unwrap_err();
    assert!(matches!(error, Error::ReadCancelled));
    assert_eq!(budget.live.load(Ordering::SeqCst), before);
    assert!(out.iter().all(|x| x.row == u64::MAX));
    i.storage().cancel_at_distance.set(None);
    let (n, _) = search(
        i.storage(),
        i.policy(),
        &budget.clone().into(),
        &angle(7),
        None,
        Mode::Exact,
        &mut out,
    )
    .unwrap();
    assert_eq!(n, 3);
    assert_eq!(out[0].row, 7);
    assert_eq!(budget.live.load(Ordering::SeqCst), before);
}
#[test]
fn an_unsealed_insert_replays_and_a_committed_delete_survives_seal_and_reopen() {
    let (mut i, budget) = fixture();
    let policy = i.policy();
    let training = TrainingPolicy {
        iterations: 2,
        reservoir_samples: 300000,
        seed: 11,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    };
    let q = angle(70);
    i.commit_insert(Vector {
        partition: 0,
        row: 900,
        values: &q,
    })
    .unwrap();
    let storage = i.into_storage();
    assert_eq!(budget.live.load(Ordering::SeqCst), 0);
    let mut i = ClusteredIndex::new(storage, policy, training, budget.clone().into()).unwrap();
    let mut out = [Hit::default(); 1];
    let (_, w) = search(
        i.storage(),
        policy,
        &budget.clone().into(),
        &q,
        None,
        Mode::Exact,
        &mut out,
    )
    .unwrap();
    assert_eq!(out[0].row, 900);
    assert_eq!(w.exact_score_count, 33);
    assert_eq!(w.buffer_evaluations, 1);
    i.delete_slot(0, 32).unwrap();
    i.seal().unwrap();
    let storage = i.into_storage();
    let i = ClusteredIndex::new(storage, policy, training, budget.clone().into()).unwrap();
    let m = i.storage().manifest(0).unwrap().unwrap();
    let mut page = [0; PAGE_BYTES];
    assert!(
        i.storage()
            .read_admitted_page(m.key(PageKind::Tombstones, 0, 0), &mut page)
            .unwrap()
    );
    for mode in [Mode::Exact, Mode::Indexed] {
        let (_, w) = search(
            i.storage(),
            policy,
            &budget.clone().into(),
            &q,
            None,
            mode,
            &mut out,
        )
        .unwrap();
        assert_ne!(out[0].row, 900);
        if mode == Mode::Exact {
            assert_eq!(w.exact_score_count, 32);
            assert_eq!(w.distance_evaluations(), 32);
        }
    }
}
#[test]
fn a_failed_build_page_write_keeps_its_cursor_and_old_published_generation() {
    let (mut i, budget) = fixture();
    let old = i.storage().manifest(0).unwrap().unwrap();
    let mut state = i.begin_build(0).unwrap();
    let rows: BTreeMap<_, _> = (100..110).map(|n| (n, angle(n))).collect();
    let mut source = rows.iter().map(|(n, v)| (*n, v.as_slice()));
    while i.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    i.finish_ingestion(&mut state).unwrap();
    let before = state;
    let live = budget.live.load(Ordering::SeqCst);
    i.storage().fail_next_write.set(true);
    assert!(matches!(
        i.advance_build(&mut state),
        Err(Error::ReadCancelled)
    ));
    assert_eq!(state, before);
    assert_eq!(i.storage().manifest(0).unwrap().unwrap(), old);
    assert_eq!(budget.live.load(Ordering::SeqCst), live);
    while !i.advance_build(&mut state).unwrap() {}
    assert_ne!(
        i.storage().manifest(0).unwrap().unwrap().generation,
        old.generation
    );
}

#[test]
fn reservoir_and_seed_policy_survive_a_reopen_without_a_second_source_pass() {
    use std::cell::Cell;
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 4, 2, 8, 1024, true).unwrap();
    let training = TrainingPolicy {
        iterations: 2,
        reservoir_samples: 8,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    };
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy,
        training,
        budget.clone().into(),
    )
    .unwrap();
    let mut state = index.begin_build(9).unwrap();
    let rows: BTreeMap<_, _> = (0..32).map(|n| (n, angle(n))).collect();
    let visited = Cell::new(0);
    let mut source = rows.iter().map(|(n, v)| {
        visited.set(visited.get() + 1);
        (*n, v.as_slice())
    });
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    index.finish_ingestion(&mut state).unwrap();
    assert_eq!(visited.get(), 32);
    let storage = index.into_storage();
    let frozen = storage.build_state(9).unwrap().unwrap();
    assert_eq!(frozen.training.seed, 0);
    assert_eq!(frozen.training.reservoir_samples, 8);
    let changed = TrainingPolicy {
        seed: 99,
        reservoir_samples: 1,
        ..training
    };
    let mut index = ClusteredIndex::new(storage, policy, changed, budget.clone().into()).unwrap();
    let mut state = frozen;
    while !index.advance_build(&mut state).unwrap() {}
    assert_eq!(visited.get(), 32);
    let mut page = [0u8; PAGE_BYTES];
    assert!(
        index
            .storage()
            .read_admitted_page(state.manifest.key(PageKind::Reservoir, 0, 0), &mut page)
            .unwrap()
    );
    let slots = page[..64]
        .as_chunks::<8>()
        .0
        .iter()
        .map(|v| u64::from_le_bytes(*v))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(slots.len(), 8);
    assert!(slots.iter().all(|v| *v < 32));
    let mut out = [Hit::default(); 1];
    let (_, w) = search(
        index.storage(),
        policy,
        &budget.clone().into(),
        &angle(7),
        Some(&[9]),
        Mode::Exact,
        &mut out,
    )
    .unwrap();
    assert_eq!(out[0].row, 7);
    assert_eq!(w.exact_score_count, 32);
    assert_eq!(w.distance_evaluations(), 32);
}

#[test]
fn a_mismatched_codebook_identity_refuses_indexed_but_keeps_canonical_exact_usable() {
    use contextdb_vector::ivf::storage::{PageStore, WritePages};
    let (i, budget) = fixture();
    let policy = i.policy();
    let mut storage = i.into_storage();
    let mut manifest = storage.manifest(0).unwrap().unwrap();
    manifest.codebook[0] ^= 1;
    let mut tx = storage.begin().unwrap();
    tx.publish_manifest(manifest).unwrap();
    tx.commit().unwrap();
    let mut out = [Hit::default(); 1];
    assert!(matches!(
        search(
            &storage,
            policy,
            &budget.clone().into(),
            &angle(7),
            None,
            Mode::Indexed,
            &mut out
        ),
        Err(Error::StoreCorrupted { .. })
    ));
    assert_eq!(budget.live.load(Ordering::SeqCst), 0);
    let (_, w) = search(
        &storage,
        policy,
        &budget.clone().into(),
        &angle(7),
        None,
        Mode::Exact,
        &mut out,
    )
    .unwrap();
    assert_eq!(out[0].row, 7);
    assert_eq!(w.exact_score_count, 32);
    assert_eq!(w.distance_evaluations(), 32);
    assert_eq!(budget.live.load(Ordering::SeqCst), 0);
}

#[test]
fn owned_source_bodies_are_released_one_at_a_time_during_the_only_ingestion_pass() {
    use std::cell::Cell;
    struct Body<'a> {
        values: Vec<f32>,
        live: &'a Cell<usize>,
    }
    impl AsRef<[f32]> for Body<'_> {
        fn as_ref(&self) -> &[f32] {
            &self.values
        }
    }
    impl Drop for Body<'_> {
        fn drop(&mut self) {
            self.live.set(self.live.get() - 1);
        }
    }
    let live = Cell::new(0);
    let peak = Cell::new(0);
    let seen = Cell::new(0);
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 4, 2, 8, 1024, true).unwrap();
    let training = TrainingPolicy {
        iterations: 2,
        reservoir_samples: 8,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    };
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy,
        training,
        budget.clone().into(),
    )
    .unwrap();
    index
        .build_partition(
            0,
            (0..64).map(|row| {
                seen.set(seen.get() + 1);
                live.set(live.get() + 1);
                peak.set(peak.get().max(live.get()));
                (
                    row,
                    Body {
                        values: angle(row),
                        live: &live,
                    },
                )
            }),
        )
        .unwrap();
    assert_eq!(seen.get(), 64);
    assert_eq!(live.get(), 0);
    assert_eq!(peak.get(), 1);
    let mut out = [Hit::default(); 1];
    let (_, work) = search(
        index.storage(),
        policy,
        &budget.clone().into(),
        &angle(7),
        None,
        Mode::Exact,
        &mut out,
    )
    .unwrap();
    assert_eq!(out[0].row, 7);
    assert_eq!(work.exact_score_count, 64);
    assert_eq!(work.distance_evaluations(), 64);
}
