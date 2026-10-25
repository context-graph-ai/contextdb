#![cfg(feature = "test-seams")]
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_vector::ivf::storage::{BuildPhase, ReadPages};
use contextdb_vector::ivf::{
    ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, Vector, inspect_catalog, search,
};
use std::sync::Arc;
fn training() -> TrainingPolicy {
    TrainingPolicy {
        iterations: 2,
        reservoir_samples: 128,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    }
}
fn policy(cap: u64) -> Policy {
    Policy::checked(2, 4, 4, 256, 192, true)
        .unwrap()
        .with_max_delta_runs(cap)
        .unwrap()
        .with_coalesce_pages_per_step(32)
        .unwrap()
}
fn vector(row: u64) -> [f32; 2] {
    let x = row as f32 * 0.031;
    [x.cos(), x.sin()]
}
fn ids(s: &impl ReadPages, p: Policy, b: &Arc<ivf_pages::Budget>, mode: Mode) -> Vec<u64> {
    let mut out = vec![Hit::default(); 256];
    let (n, _) = search(s, p, &b.clone().into(), &vector(0), None, mode, &mut out).unwrap();
    let mut result = out[..n].iter().map(|x| x.row).collect::<Vec<_>>();
    result.sort_unstable();
    result
}
fn built(cap: u64) -> (ClusteredIndex<ivf_pages::Pages>, Arc<ivf_pages::Budget>) {
    let b = Arc::new(ivf_pages::Budget::default());
    let mut i = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy(cap),
        training(),
        b.clone().into(),
    )
    .unwrap();
    i.build_partition(0, (0..16).map(|n| (n, vector(n))))
        .unwrap();
    (i, b)
}
#[test]
fn capped_run_catalog_accepts_continuous_seals_and_coalesces_without_losing_snapshots() {
    for cap in [1, 2] {
        let (mut i, b) = built(cap);
        let saved = i.storage().clone();
        for row in 16..160 {
            i.commit_insert(Vector {
                partition: 0,
                row,
                values: &vector(row),
            })
            .unwrap();
            i.seal().unwrap();
            let state = inspect_catalog(i.storage(), 0, cap).unwrap().unwrap();
            assert!(state.delta_runs <= (cap + 1).max(3));
        }
        for mode in [Mode::Exact, Mode::Indexed] {
            assert_eq!(
                ids(i.storage(), policy(cap), &b, mode),
                (0..160).collect::<Vec<_>>()
            );
            assert_eq!(
                ids(&saved, policy(cap), &b, mode),
                (0..16).collect::<Vec<_>>()
            );
        }
        let before = i.storage().clone();
        let state = inspect_catalog(i.storage(), 0, cap).unwrap().unwrap();
        assert!(state.runs_over_cap > 0);
        for _ in 0..32 {
            if i.advance_coalescing(0, 32).unwrap() {
                break;
            }
        }
        let state = inspect_catalog(i.storage(), 0, cap).unwrap().unwrap();
        assert!(!state.coalescing);
        assert!(state.delta_runs <= cap);
        for mode in [Mode::Exact, Mode::Indexed] {
            assert_eq!(
                ids(i.storage(), policy(cap), &b, mode),
                ids(&before, policy(cap), &b, mode)
            );
        }
    }
}
#[test]
fn legacy_remap_is_entry_bounded_survives_reopen_and_publishes_each_run_once() {
    let (mut i, b) = built(2);
    let mut state = i.begin_build(0).unwrap();
    let mut source = (0..16).map(|n| (n, vector(n)));
    while i.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    for row in 16..40 {
        i.commit_insert(Vector {
            partition: 0,
            row,
            values: &vector(row),
        })
        .unwrap();
        i.seal().unwrap();
    }
    i.finish_ingestion(&mut state).unwrap();
    for _ in 0..128 {
        if i.advance_build(&mut state).unwrap() {
            break;
        }
    }
    assert_eq!(state.phase, BuildPhase::Ready);
    let published = i.storage().clone();
    assert!(
        inspect_catalog(i.storage(), 0, 2)
            .unwrap()
            .unwrap()
            .legacy_runs
            > 0
    );
    let first = i.advance_remap(0, 1).unwrap();
    assert!(first.entries_examined <= 1);
    assert!(first.entries_remapped <= 1);
    let mut i =
        ClusteredIndex::new(i.into_storage(), policy(2), training(), b.clone().into()).unwrap();
    let mut remapped = first.entries_remapped;
    let mut finished = first.complete;
    for _ in 0..256 {
        for mode in [Mode::Exact, Mode::Indexed] {
            assert_eq!(
                ids(i.storage(), policy(2), &b, mode),
                (0..40).collect::<Vec<_>>()
            );
        }
        if finished {
            break;
        }
        i.advance_coalescing(0, 32).unwrap();
        let step = i.advance_remap(0, 1).unwrap();
        assert!(step.entries_examined <= 1);
        assert!(step.entries_remapped <= 1);
        remapped += step.entries_remapped;
        finished = step.complete;
    }
    assert!(finished);
    assert_eq!(remapped, 24);
    let final_state = inspect_catalog(i.storage(), 0, 2).unwrap().unwrap();
    assert_eq!(final_state.legacy_runs, 0);
    assert_eq!(final_state.retained_codebook, None);
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            ids(i.storage(), policy(2), &b, mode),
            ids(&published, policy(2), &b, mode)
        );
    }
}
#[test]
fn deletion_and_replacement_during_build_and_prepared_remap_do_not_resurrect_rows() {
    let (mut i, b) = built(2);
    let mut state = i.begin_build(0).unwrap();
    let mut source = (0..16).map(|n| (n, vector(n)));
    i.ingest_batch(&mut state, &mut source).unwrap();
    i.delete_slot(0, 3).unwrap();
    i.commit_insert(Vector {
        partition: 0,
        row: 9,
        values: &vector(209),
    })
    .unwrap();
    for row in 16..22 {
        i.commit_insert(Vector {
            partition: 0,
            row,
            values: &vector(row),
        })
        .unwrap();
    }
    while i.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    i.finish_ingestion(&mut state).unwrap();
    while !i.advance_build(&mut state).unwrap() {}
    let expected = (0..22).filter(|x| *x != 3).collect::<Vec<_>>();
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(ids(i.storage(), policy(2), &b, mode), expected);
    }
    // Prepare one live legacy entry, then update that row before its run retires.
    let step = i.advance_remap(0, 1).unwrap();
    assert_eq!(step.entries_examined, 1);
    i.commit_insert(Vector {
        partition: 0,
        row: 9,
        values: &vector(309),
    })
    .unwrap();
    let mut done = step.complete;
    for _ in 0..128 {
        if done {
            break;
        }
        i.advance_coalescing(0, 32).unwrap();
        done = i.advance_remap(0, 1).unwrap().complete;
    }
    assert!(done);
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(ids(i.storage(), policy(2), &b, mode), expected);
    }
}
#[test]
fn cap_one_seals_write_only_the_appended_payload_and_each_coalesce_step_is_bounded() {
    use contextdb_vector::ivf::storage::PageKind;
    let (mut i, b) = built(1);
    for row in 16..160 {
        i.commit_insert(Vector {
            partition: 0,
            row,
            values: &vector(row),
        })
        .unwrap();
        i.storage().writes.borrow_mut().clear();
        i.seal().unwrap();
        assert_eq!(
            i.storage()
                .writes
                .borrow()
                .get(&PageKind::Lists)
                .copied()
                .unwrap_or(0),
            1,
            "one appended vector never rewrites old list payload pages, at row {row}"
        );
    }
    for _ in 0..32 {
        i.storage().touched.borrow_mut().clear();
        let done = i.advance_coalescing(0, 32).unwrap();
        assert!(
            i.storage().touched.borrow().len() <= 32,
            "each step stays within the admitted logical payload-frame bound"
        );
        if done {
            break;
        }
    }
    assert!(
        !inspect_catalog(i.storage(), 0, 1)
            .unwrap()
            .unwrap()
            .coalescing
    );
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            ids(i.storage(), policy(1), &b, mode),
            (0..160).collect::<Vec<_>>()
        );
    }
}
#[test]
fn cancelled_coalesce_and_remap_leave_the_cursor_and_answers_usable_with_no_extra_charge() {
    use contextdb_core::Error;
    use std::sync::atomic::Ordering;
    let (mut i, b) = built(2);
    for row in 16..24 {
        i.commit_insert(Vector {
            partition: 0,
            row,
            values: &vector(row),
        })
        .unwrap();
        i.seal().unwrap();
    }
    let before = inspect_catalog(i.storage(), 0, 2).unwrap();
    let charge = b.live.load(Ordering::SeqCst);
    i.storage().fail_next_write.set(true);
    assert!(matches!(
        i.advance_coalescing(0, 32),
        Err(Error::ReadCancelled)
    ));
    assert_eq!(inspect_catalog(i.storage(), 0, 2).unwrap(), before);
    assert_eq!(b.live.load(Ordering::SeqCst), charge);
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            ids(i.storage(), policy(2), &b, mode),
            (0..24).collect::<Vec<_>>()
        );
    }
    while !i.advance_coalescing(0, 32).unwrap() {}
    let mut state = i.begin_build(0).unwrap();
    let mut source = (0..24).map(|n| (n, vector(n)));
    while i.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    i.commit_insert(Vector {
        partition: 0,
        row: 24,
        values: &vector(24),
    })
    .unwrap();
    i.finish_ingestion(&mut state).unwrap();
    while !i.advance_build(&mut state).unwrap() {}
    let before = inspect_catalog(i.storage(), 0, 2).unwrap();
    let manifest = i.storage().manifest(0).unwrap();
    let charge = b.live.load(Ordering::SeqCst);
    i.storage().fail_next_write.set(true);
    assert!(matches!(i.advance_remap(0, 1), Err(Error::ReadCancelled)));
    assert_eq!(inspect_catalog(i.storage(), 0, 2).unwrap(), before);
    assert_eq!(i.storage().manifest(0).unwrap(), manifest);
    assert_eq!(b.live.load(Ordering::SeqCst), charge);
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            ids(i.storage(), policy(2), &b, mode),
            (0..25).collect::<Vec<_>>()
        );
    }
    let mut done = false;
    for _ in 0..64 {
        let p = i.advance_remap(0, 1).unwrap();
        if p.complete {
            done = true;
            break;
        }
    }
    assert!(done);
}
