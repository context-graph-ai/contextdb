#![cfg(feature = "test-seams")]
//! The clustered kernel keeps every committed vector searchable and reports
//! caller mistakes as caller mistakes:
//! - a writer reopened with a smaller write buffer still serves every
//!   committed tail row through INDEXED, as EXACT does;
//! - a vector committed while its partition is being rebuilt is served by
//!   EXACT and INDEXED after the rebuild publishes, after the next seal and
//!   after reopen, and no search reports store corruption;
//! - a wrong query, insert or ingest dimension is the typed dimension
//!   mismatch naming the port's index, a zero training value is the typed
//!   InvalidTrainingValue policy refusal naming its member, and a build call
//!   out of phase is a typed state-transition refusal naming the phase, never
//!   store corruption;
//! - a policy at the largest dimension a page holds is accepted and searched,
//!   and one dimension more is refused before anything is built;
//! - a missing accumulator page is typed corruption when training resumes,
//!   and a search reads no accumulator page at all.
#[path = "support/hiding_pages.rs"]
mod hiding_pages;
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::Error;
use contextdb_vector::ivf::storage::{BuildPhase, PAGE_BYTES, PageKind, ReadPages};
use contextdb_vector::ivf::{ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, Vector, search};
use hiding_pages::{Hiding, port_identity};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn angle(i: u64) -> Vec<f32> {
    let x = i as f64 * 0.02;
    vec![x.cos() as f32, x.sin() as f32]
}

fn training() -> TrainingPolicy {
    TrainingPolicy {
        iterations: 2,
        reservoir_samples: 300_000,
        seed: 11,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    }
}

/// Every list probed and every candidate rescored, so INDEXED misses a row
/// only when the row is in no list and in no searched tail.
fn exhaustive_policy(buffer_cap_bytes: usize) -> Policy {
    Policy::checked(2, 4, 4, 256, buffer_cap_bytes, true).expect("a valid policy")
}

const BUILT: u64 = 32;

fn built(policy: Policy, budget: &Arc<ivf_pages::Budget>) -> ClusteredIndex<ivf_pages::Pages> {
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy,
        training(),
        budget.clone().into(),
    )
    .expect("open the index");
    let rows: BTreeMap<_, _> = (0..BUILT).map(|n| (n, angle(n))).collect();
    index
        .build_partition(0, rows.iter().map(|(id, v)| (*id, v.as_slice())))
        .expect("build the first generation");
    index
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn top(
    store: &impl ReadPages,
    policy: Policy,
    budget: &Arc<ivf_pages::Budget>,
    query: &[f32],
    k: usize,
    mode: Mode,
) -> contextdb_core::Result<Vec<u64>> {
    let mut out = vec![Hit::default(); k];
    let (found, _) = search(
        store,
        policy,
        &budget.clone().into(),
        query,
        None,
        mode,
        &mut out,
    )?;
    Ok(out[..found].iter().map(|hit| hit.row).collect())
}

fn assert_every_row_served(
    store: &impl ReadPages,
    policy: Policy,
    budget: &Arc<ivf_pages::Budget>,
    rows: &BTreeMap<u64, Vec<f32>>,
    stage: &str,
) {
    for (row, vector) in rows {
        for mode in [Mode::Exact, Mode::Indexed] {
            let found = top(store, policy, budget, vector, 1, mode).unwrap_or_else(|error| {
                panic!("{stage}: {mode:?} search for row {row}: {error:?}")
            });
            assert_eq!(found, vec![*row], "{stage}: {mode:?} serves row {row}");
        }
    }
    let all = rows.keys().copied().collect::<BTreeSet<_>>();
    for mode in [Mode::Exact, Mode::Indexed] {
        let answered = top(store, policy, budget, &angle(0), rows.len() + 4, mode)
            .unwrap_or_else(|error| panic!("{stage}: full-depth {mode:?}: {error:?}"))
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            answered, all,
            "{stage}: a full-depth {mode:?} search answers every committed row"
        );
    }
}

#[test]
fn a_writer_reopened_with_a_smaller_buffer_still_serves_every_committed_tail_row() {
    let budget = Arc::new(ivf_pages::Budget::default());
    // A 2-dimensional record is 24 + 8 = 32 bytes: 32 records fit in 1,024.
    let large = exhaustive_policy(1_024);
    let mut index = built(large, &budget);
    let mut rows: BTreeMap<u64, Vec<f32>> = (0..BUILT).map(|n| (n, angle(n))).collect();
    for row in 100..120 {
        let vector = angle(row);
        index
            .commit_insert(Vector {
                partition: 0,
                row,
                values: &vector,
            })
            .expect("commit one tail vector");
        rows.insert(row, vector);
    }
    assert_eq!(
        index.buffer().sealed_runs,
        0,
        "the 20-row tail fits the large buffer unsealed"
    );
    assert_every_row_served(index.storage(), large, &budget, &rows, "before reopen");

    // Half the tail fits the reopened buffer, so replay fills it mid-tail.
    let small = exhaustive_policy(320);
    let storage = index.into_storage();
    let reopened = ClusteredIndex::new(storage, small, training(), budget.clone().into())
        .expect("reopen with half the buffer");
    assert_every_row_served(
        reopened.storage(),
        small,
        &budget,
        &rows,
        "after reopen with a smaller buffer",
    );
    let storage = reopened.into_storage();
    let again = ClusteredIndex::new(storage, small, training(), budget.clone().into())
        .expect("reopen once more");
    assert_every_row_served(
        again.storage(),
        small,
        &budget,
        &rows,
        "after a second reopen",
    );
}

#[test]
fn a_vector_committed_during_a_rebuild_is_served_after_publication_seal_and_reopen() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = exhaustive_policy(1_024);
    let mut index = built(policy, &budget);
    let mut rows: BTreeMap<u64, Vec<f32>> = (0..BUILT).map(|n| (n, angle(n))).collect();
    let generation = index
        .storage()
        .manifest(0)
        .unwrap()
        .expect("the first generation")
        .generation;

    // Rebuild the same partition from its pinned source rows, committing new
    // vectors while the rebuild is ingesting and while it is training.
    let source: Vec<(u64, Vec<f32>)> = rows.iter().map(|(id, v)| (*id, v.clone())).collect();
    let mut source = source.into_iter();
    let mut state = index.begin_build(0).expect("begin the rebuild");
    assert!(
        index
            .ingest_batch(&mut state, &mut source)
            .expect("one ingest batch")
            > 0
    );
    let during_ingest = angle(500);
    index
        .commit_insert(Vector {
            partition: 0,
            row: 500,
            values: &during_ingest,
        })
        .expect("commit during ingestion");
    rows.insert(500, during_ingest);
    while index.ingest_batch(&mut state, &mut source).expect("ingest") > 0 {}
    index
        .finish_ingestion(&mut state)
        .expect("finish ingestion");
    let mut committed_during_training = false;
    while !index.advance_build(&mut state).expect("one build step") {
        if !committed_during_training && state.phase == BuildPhase::Train {
            let during_training = angle(600);
            index
                .commit_insert(Vector {
                    partition: 0,
                    row: 600,
                    values: &during_training,
                })
                .expect("commit during training");
            rows.insert(600, during_training);
            committed_during_training = true;
        }
    }
    assert!(
        committed_during_training,
        "the rebuild passed through training"
    );
    assert!(
        index.storage().manifest(0).unwrap().unwrap().generation > generation,
        "the rebuild published a new generation"
    );
    assert_every_row_served(
        index.storage(),
        policy,
        &budget,
        &rows,
        "after the rebuild publishes",
    );
    index.seal().expect("seal after the rebuild");
    assert_every_row_served(
        index.storage(),
        policy,
        &budget,
        &rows,
        "after the next seal",
    );
    let storage = index.into_storage();
    let reopened = ClusteredIndex::new(storage, policy, training(), budget.clone().into())
        .expect("reopen after the rebuild");
    assert_every_row_served(reopened.storage(), policy, &budget, &rows, "after reopen");
}

/// The typed dimension mismatch, naming both dimensions and the index
/// identity the page port supplies.
fn assert_dimension_mismatch(
    result: contextdb_core::Result<impl std::fmt::Debug>,
    port: &impl ReadPages,
    expected: usize,
    actual: usize,
    what: &str,
) {
    match result {
        Err(Error::VectorIndexDimensionMismatch {
            index,
            expected: e,
            actual: a,
        }) => {
            assert_eq!(
                (e, a),
                (expected, actual),
                "{what}: the mismatch names both dimensions"
            );
            assert_eq!(
                index,
                port_identity(port),
                "{what}: the mismatch names the port's index"
            );
        }
        other => panic!("{what}: the typed dimension mismatch, never store corruption: {other:?}"),
    }
}

/// The typed state-transition refusal, naming the phase the build is in.
fn assert_phase_refusal(result: contextdb_core::Result<()>, phase: BuildPhase, what: &str) {
    match result {
        Err(Error::InvalidStateTransition(message)) => assert!(
            message
                .to_ascii_lowercase()
                .contains(&format!("{phase:?}").to_ascii_lowercase()),
            "{what}: the refusal names the {phase:?} phase: {message}"
        ),
        other => panic!("{what} is a typed state-transition refusal, got {other:?}"),
    }
}

#[test]
fn caller_mistakes_are_typed_refusals_not_store_corruption() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = exhaustive_policy(1_024);
    let mut index = built(policy, &budget);

    let query = top(
        index.storage(),
        policy,
        &budget,
        &[1.0, 0.0, 0.0],
        3,
        Mode::Indexed,
    );
    assert_dimension_mismatch(
        query,
        index.storage(),
        2,
        3,
        "a query of the wrong dimension",
    );
    let query = top(index.storage(), policy, &budget, &[1.0], 3, Mode::Exact);
    assert_dimension_mismatch(
        query,
        index.storage(),
        2,
        1,
        "an EXACT query of the wrong dimension",
    );
    let insert = index.commit_insert(Vector {
        partition: 0,
        row: 700,
        values: &[1.0, 0.0, 0.0],
    });
    assert_dimension_mismatch(
        insert,
        index.storage(),
        2,
        3,
        "an insert of the wrong dimension",
    );
    let mut state = index.begin_build(1).expect("begin a build");
    let mut wrong = vec![(1_u64, vec![1.0_f32, 0.0, 0.0])].into_iter();
    let ingest = index.ingest_batch(&mut state, &mut wrong);
    assert_dimension_mismatch(
        ingest,
        index.storage(),
        2,
        3,
        "an ingested vector of the wrong dimension",
    );

    for (field, named, broken) in [
        (
            "iterations",
            "iterations",
            TrainingPolicy {
                iterations: 0,
                ..training()
            },
        ),
        (
            "reservoir_samples",
            "samples",
            TrainingPolicy {
                reservoir_samples: 0,
                ..training()
            },
        ),
        (
            "ingest_batch_keys",
            "ingest",
            TrainingPolicy {
                ingest_batch_keys: 0,
                ..training()
            },
        ),
        (
            "phase_batch_entries",
            "phase",
            TrainingPolicy {
                phase_batch_entries: 0,
                ..training()
            },
        ),
    ] {
        match ClusteredIndex::new(
            ivf_pages::Pages::default(),
            policy,
            broken,
            budget.clone().into(),
        ) {
            Err(Error::InvalidVectorPolicyDeclaration { issue, .. }) => {
                let issue = format!("{issue:?}");
                assert!(
                    issue.starts_with("InvalidTrainingValue")
                        && issue.to_ascii_lowercase().contains(named)
                        && issue.contains("value: 0"),
                    "a zero {field} is InvalidTrainingValue naming the member and its value: {issue}"
                );
            }
            Err(other) => panic!("a zero {field} is a typed policy refusal, got {other:?}"),
            Ok(_) => panic!("a zero {field} is refused"),
        }
    }

    // Out-of-phase build calls.
    let mut state = index.begin_build(2).expect("begin another build");
    let advance = index.advance_build(&mut state).map(|_| ());
    assert_phase_refusal(
        advance,
        BuildPhase::Ingest,
        "advancing a build still ingesting",
    );
    let mut rows = vec![(1_u64, angle(1))].into_iter();
    index
        .ingest_batch(&mut state, &mut rows)
        .expect("ingest one row");
    index
        .finish_ingestion(&mut state)
        .expect("finish ingestion");
    let mut more = vec![(2_u64, angle(2))].into_iter();
    let ingest = index.ingest_batch(&mut state, &mut more).map(|_| ());
    assert_phase_refusal(
        ingest,
        BuildPhase::Seed,
        "ingesting after ingestion finished",
    );
    let finish = index.finish_ingestion(&mut state);
    assert_phase_refusal(finish, BuildPhase::Seed, "finishing ingestion twice");
    while !index.advance_build(&mut state).expect("one build step") {}
    assert_eq!(state.phase, BuildPhase::Ready);
}

#[test]
fn a_policy_at_the_largest_page_dimension_is_searched_and_one_more_is_refused() {
    let largest = PAGE_BYTES - 16;
    assert!(
        Policy::checked(largest + 1, 2, 1, 4, 1 << 20, true).is_none(),
        "a dimension a list page cannot hold is refused before anything is built"
    );
    let policy = Policy::checked(largest, 2, 2, 4, 1 << 20, true)
        .expect("the largest dimension a page holds");
    let budget = Arc::new(ivf_pages::Budget::default());
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy,
        training(),
        budget.clone().into(),
    )
    .expect("open at the largest dimension");
    let vector = |seed: u64| {
        let mut values = vec![0.0_f32; largest];
        values[(seed as usize) % largest] = 1.0;
        values
    };
    let rows: BTreeMap<u64, Vec<f32>> = (0..4).map(|n| (n, vector(n * 7))).collect();
    index
        .build_partition(0, rows.iter().map(|(id, v)| (*id, v.as_slice())))
        .expect("build at the largest dimension");
    for (row, values) in &rows {
        assert_eq!(
            top(index.storage(), policy, &budget, values, 1, Mode::Indexed).unwrap(),
            vec![*row]
        );
    }
}

#[test]
fn a_missing_accumulator_page_is_typed_corruption_and_a_search_reads_none() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = exhaustive_policy(1_024);
    let mut index =
        ClusteredIndex::new(Hiding::default(), policy, training(), budget.clone().into())
            .expect("open the index");
    let rows: BTreeMap<u64, Vec<f32>> = (0..BUILT).map(|n| (n, angle(n))).collect();
    let mut source = rows
        .iter()
        .map(|(id, v)| (*id, v.clone()))
        .collect::<Vec<_>>()
        .into_iter();
    let mut state = index.begin_build(0).expect("begin the build");
    while index.ingest_batch(&mut state, &mut source).expect("ingest") > 0 {}
    index
        .finish_ingestion(&mut state)
        .expect("finish ingestion");
    while state.phase != BuildPhase::Train {
        assert!(
            !index.advance_build(&mut state).expect("seed the codebook"),
            "seeding stops at training"
        );
    }
    let staged = state.manifest;
    let mut page = [0u8; PAGE_BYTES];
    assert!(
        index
            .storage()
            .inner
            .read_admitted_page(staged.key(PageKind::Accumulators, 0, 0), &mut page)
            .expect("read the accumulator page"),
        "precondition: seeding persisted the accumulator page training resumes from"
    );
    index.storage().hide.set(Some(PageKind::Accumulators));
    match index.advance_build(&mut state) {
        Err(Error::StoreCorrupted { .. }) => {}
        other => panic!(
            "resuming training without its accumulator page is typed corruption, got {other:?}"
        ),
    }
    index.storage().hide.set(None);
    while !index.advance_build(&mut state).expect("finish the build") {}

    index.storage().reads.borrow_mut().clear();
    let found = top(
        index.storage(),
        policy,
        &budget,
        &angle(7),
        3,
        Mode::Indexed,
    )
    .expect("indexed search");
    assert_eq!(found.first(), Some(&7));
    assert_eq!(
        index
            .storage()
            .reads
            .borrow()
            .get(&PageKind::Accumulators)
            .copied()
            .unwrap_or(0),
        0,
        "a search reads no accumulator page"
    );
    index.storage().hide.set(Some(PageKind::Accumulators));
    assert_eq!(
        top(
            index.storage(),
            policy,
            &budget,
            &angle(7),
            3,
            Mode::Indexed
        )
        .expect("a search needs no accumulator"),
        found,
        "a search answers the same without accumulator pages"
    );
}
