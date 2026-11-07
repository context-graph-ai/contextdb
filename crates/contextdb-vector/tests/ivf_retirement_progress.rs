#![cfg(feature = "test-seams")]
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_vector::ivf::storage::{BuildPhase, PAGE_BYTES, PageKind, ReadPages};
use contextdb_vector::ivf::{
    ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, Vector, inspect_catalog, search,
};
use std::sync::Arc;
fn policy(cap: u64) -> Policy {
    Policy::checked(2, 4, 4, 256, 192, true)
        .unwrap()
        .with_max_delta_runs(cap)
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
fn vector(row: u64) -> [f32; 2] {
    let a = row as f32 * 0.031;
    [a.cos(), a.sin()]
}
fn commit(i: &mut ClusteredIndex<ivf_pages::Pages>, row: u64) {
    i.commit_insert(Vector {
        partition: 0,
        row,
        values: &vector(row),
    })
    .unwrap();
    i.seal().unwrap();
}
fn ids(i: &ClusteredIndex<ivf_pages::Pages>, b: &Arc<ivf_pages::Budget>, mode: Mode) -> Vec<u64> {
    let mut out = [Hit::default(); 256];
    let (n, _) = search(
        i.storage(),
        i.policy(),
        &b.clone().into(),
        &vector(0),
        None,
        mode,
        &mut out,
    )
    .unwrap();
    let mut rows = out[..n].iter().map(|x| x.row).collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}
fn exercise(cap: u64, alternate: bool, mutate: bool) {
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
    let mut state = i.begin_build(0).unwrap();
    let mut source = (0..16).map(|n| (n, vector(n)));
    while i.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    for row in 16..24 {
        commit(&mut i, row);
    }
    i.finish_ingestion(&mut state).unwrap();
    for _ in 0..128 {
        if i.advance_build(&mut state).unwrap() {
            break;
        }
    }
    assert_eq!(state.phase, BuildPhase::Ready);
    let mut next = 24;
    for _ in 0..6 {
        commit(&mut i, next);
        next += 1;
    }
    let mut complete = false;
    let mut joined = false;
    let mut changed = false;
    for step in 0..96 {
        if alternate {
            i.advance_coalescing(0, 26).unwrap();
        }
        // A committed write separates maintenance calls; no idle adjacency exists.
        commit(&mut i, next);
        next += 1;
        let p = i.advance_remap(0, 1).unwrap();
        assert!(p.entries_examined <= 1 && p.entries_remapped <= 1);
        let m = i.storage().manifest(0).unwrap().unwrap();
        let mut raw = [0; PAGE_BYTES];
        assert!(
            i.storage()
                .read_admitted_page(m.key(PageKind::Catalog, 0, 0), &mut raw)
                .unwrap()
        );
        let joining = u64::from_le_bytes(raw[64..72].try_into().unwrap()) != 0;
        joined |= joining;
        if joining && mutate && !changed {
            let old = i.current_row_identity(0, 16).unwrap().unwrap();
            i.commit_update(0, old, &vector(216)).unwrap();
            let old = i.current_row_identity(0, 17).unwrap().unwrap();
            i.delete_row(0, old).unwrap();
            changed = true;
        }
        let wanted = (0..next)
            .filter(|n| !changed || *n != 17)
            .collect::<Vec<_>>();
        for mode in [Mode::Exact, Mode::Indexed] {
            assert_eq!(
                ids(&i, &b, mode),
                wanted,
                "cap{cap} step{step} mode{mode:?}"
            );
        }
        assert!(
            inspect_catalog(i.storage(), 0, cap)
                .unwrap()
                .unwrap()
                .delta_runs
                <= (cap + 1).max(3)
        );
        if p.complete {
            complete = true;
            break;
        }
        i = ClusteredIndex::new(i.into_storage(), policy(cap), training(), b.clone().into())
            .unwrap();
    }
    assert!(
        complete,
        "cap{cap}: continued seals cannot starve retirement"
    );
    if !alternate {
        assert!(joined, "the full-descriptor join path was exercised");
    }
    if mutate {
        assert!(changed);
    }
    assert_eq!(
        inspect_catalog(i.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .legacy_runs,
        0
    );
    for _ in 0..128 {
        if i.advance_coalescing(0, 26).unwrap() {
            break;
        }
    }
    assert!(
        inspect_catalog(i.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .delta_runs
            <= cap
    );
    assert!(i.begin_build(0).is_ok());
}
#[test]
fn continuous_seals_allow_retirement_with_or_without_other_coalesce_calls() {
    for cap in [1, 2] {
        for alternate in [false, true] {
            exercise(cap, alternate, false);
        }
    }
}
#[test]
fn partial_joins_remain_masked_through_reopen_update_and_delete() {
    for cap in [1, 2] {
        exercise(cap, false, true);
    }
}
