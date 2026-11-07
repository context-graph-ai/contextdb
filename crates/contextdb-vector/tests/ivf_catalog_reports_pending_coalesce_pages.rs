#![cfg(feature = "test-seams")]
//! Catalog inspection reports the coalesce work still pending, in pages:
//! positive while a coalesce is in progress, falling as each maintenance step
//! joins lists, and zero once the catalog is back within its run cap.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_vector::ivf::{ClusteredIndex, Policy, TrainingPolicy, Vector, inspect_catalog};
use std::sync::Arc;

#[test]
fn pending_coalesce_pages_fall_with_each_step_and_reach_zero() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 4, 4, 256, 192, true)
        .unwrap()
        .with_max_delta_runs(1)
        .unwrap()
        .with_coalesce_pages_per_step(26)
        .unwrap();
    let training = TrainingPolicy {
        iterations: 2,
        reservoir_samples: 128,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    };
    let vector = |row: u64| {
        let x = row as f32 * 0.031;
        [x.cos(), x.sin()]
    };
    let mut index =
        ClusteredIndex::new(ivf_pages::Pages::default(), policy, training, budget.into()).unwrap();
    index
        .build_partition(0, (0..16).map(|row| (row, vector(row))))
        .unwrap();
    let state = inspect_catalog(index.storage(), 0, 1).unwrap().unwrap();
    assert_eq!(
        (state.coalescing, state.pending_coalesce_pages),
        (false, 0),
        "no coalesce is pending before any seal: {state:?}"
    );
    for row in 16..18 {
        index
            .commit_insert(Vector {
                partition: 0,
                row,
                values: &vector(row),
            })
            .unwrap();
        index.seal().unwrap();
    }
    let mut state = inspect_catalog(index.storage(), 0, 1).unwrap().unwrap();
    assert!(
        state.coalescing && state.pending_coalesce_pages > 0,
        "a coalesce in progress reports its pending pages: {state:?}"
    );
    for step in 0..64 {
        let done = index.advance_coalescing(0, 26).unwrap();
        let after = inspect_catalog(index.storage(), 0, 1).unwrap().unwrap();
        if after.coalescing {
            assert!(
                after.pending_coalesce_pages > 0
                    && after.pending_coalesce_pages < state.pending_coalesce_pages,
                "step {step} joined a list, so fewer pages are pending: {state:?} -> {after:?}"
            );
        }
        state = after;
        if done {
            break;
        }
    }
    assert_eq!(
        (state.coalescing, state.pending_coalesce_pages),
        (false, 0),
        "nothing is pending once the catalog is back within its cap: {state:?}"
    );
}
