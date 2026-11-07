#![cfg(feature = "test-seams")]
//! A row is one row in every answer, through a generation replacement and its
//! legacy remap. The kernel's row journal knows every live copy of a row
//! (base, new delta, legacy, build and prepared remap copies), and update and
//! delete name the row version they replace by its identity, the row and the
//! commit LSN of that version, never a slot of one generation:
//! - a row present at the build's source frontier, updated before the
//!   replacement publishes, whose legacy entry the remap then prepares, and
//!   which is updated or deleted again before its legacy run retires, is
//!   served exactly once with its current vector (or not at all once
//!   deleted) by EXACT and INDEXED at every point: at publication, after
//!   every remap step, after retirement and after reopen;
//! - a row whose only live copy is a legacy entry is deleted by its identity
//!   while the remap runs, and stays deleted through retirement and reopen;
//! - an identity that does not name the row's current version is the typed
//!   StaleRowVersion refusal carrying the expected LSN and the current one
//!   (none once the row is deleted), and changes nothing in either mode;
//! - rows built through the unversioned entry point carry the genesis version
//!   (LSN 0), and a rebuild keeps every row's own version identity, never the
//!   build's source frontier.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::Error;
use contextdb_vector::ivf::storage::BuildPhase;
use contextdb_vector::ivf::{
    ClusteredIndex, Hit, Mode, Policy, RowIdentity, TrainingPolicy, Vector, inspect_catalog, search,
};
use std::collections::BTreeMap;
use std::sync::Arc;

type Index = ClusteredIndex<ivf_pages::Pages>;
const CAP: u64 = 2;
const BASE: u64 = 16;
/// Rows present at the source frontier, updated before publication.
const PINNED: [u64; 2] = [5, 6];
/// New rows committed between the source frontier and publication.
const LATE: std::ops::Range<u64> = 16..22;

fn training() -> TrainingPolicy {
    TrainingPolicy {
        iterations: 2,
        reservoir_samples: 128,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    }
}

fn policy() -> Policy {
    Policy::checked(2, 4, 4, 256, 192, true)
        .unwrap()
        .with_max_delta_runs(CAP)
        .unwrap()
        .with_coalesce_pages_per_step(32)
        .unwrap()
}

fn vector(seed: u64) -> [f32; 2] {
    let x = seed as f32 * 0.031;
    [x.cos(), x.sin()]
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut aa, mut bb) = (0.0, 0.0, 0.0);
    for (&x, &y) in a.iter().zip(b) {
        let (x, y) = (f64::from(x), f64::from(y));
        dot += x * y;
        aa += x * x;
        bb += y * y;
    }
    1.0 - dot / (aa.sqrt() * bb.sqrt())
}

fn built() -> (Index, Arc<ivf_pages::Budget>) {
    let budget = Arc::new(ivf_pages::Budget::default());
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy(),
        training(),
        budget.clone().into(),
    )
    .unwrap();
    index
        .build_partition(0, (0..BASE).map(|row| (row, vector(row))))
        .unwrap();
    (index, budget)
}

fn reopen(index: Index, budget: &Arc<ivf_pages::Budget>) -> Index {
    ClusteredIndex::new(
        index.into_storage(),
        policy(),
        training(),
        budget.clone().into(),
    )
    .unwrap()
}

/// The identity of `row`'s current version, read back through the typed
/// refusal of an identity no version carries: the refusal reports the
/// current version (none when the row is deleted) and changes nothing.
fn current(index: &mut Index, row: u64) -> Option<RowIdentity> {
    let probe = RowIdentity {
        row,
        commit_lsn: u64::MAX,
    };
    match index.delete_row(0, probe) {
        Err(Error::StaleRowVersion { expected, current }) => {
            assert_eq!(
                expected,
                u64::MAX,
                "the refusal names the identity supplied"
            );
            current.map(|commit_lsn| RowIdentity { row, commit_lsn })
        }
        other => panic!("an identity no version of row {row} carries is refused, got {other:?}"),
    }
}

fn update(index: &mut Index, row: u64, values: [f32; 2]) {
    let expected = current(index, row).expect("the row exists");
    index
        .commit_update(0, expected, &values[..])
        .unwrap_or_else(|error| panic!("update row {row} at {expected:?}: {error:?}"));
}

fn delete(index: &mut Index, row: u64) {
    let expected = current(index, row).expect("the row exists");
    index
        .delete_row(0, expected)
        .unwrap_or_else(|error| panic!("delete row {row} at {expected:?}: {error:?}"));
}

/// Every row in `expected` is answered exactly once, at the distance of its
/// current vector, and no other row is answered, by EXACT and by INDEXED,
/// for queries at the old and the new vectors of the pinned rows.
fn assert_served_once(
    index: &Index,
    budget: &Arc<ivf_pages::Budget>,
    expected: &BTreeMap<u64, [f32; 2]>,
    when: &str,
) {
    let queries = [vector(0), vector(205), vector(206), vector(405)];
    for mode in [Mode::Exact, Mode::Indexed] {
        for query in &queries {
            let mut out = vec![Hit::default(); 256];
            let (count, _) = search(
                index.storage(),
                index.policy(),
                &budget.clone().into(),
                query,
                None,
                mode,
                &mut out,
            )
            .unwrap_or_else(|error| panic!("{when}: {mode:?} search: {error:?}"));
            let mut answered = BTreeMap::<u64, Vec<f64>>::new();
            for hit in &out[..count] {
                answered.entry(hit.row).or_default().push(hit.distance);
            }
            assert_eq!(
                answered.keys().copied().collect::<Vec<_>>(),
                expected.keys().copied().collect::<Vec<_>>(),
                "{when}: {mode:?} answers exactly the live rows"
            );
            for (row, distances) in &answered {
                let own = cosine_distance(query, &expected[row]);
                assert!(
                    distances.len() == 1 && (distances[0] - own).abs() < 1e-9,
                    "{when}: {mode:?} serves row {row} once at its current vector \
                     ({own}), got {distances:?}"
                );
            }
        }
    }
}

/// State B with a pinned row's legacy entry still to be remapped: the build
/// ingests every row at its source frontier, the pinned rows are updated and
/// the late rows committed and sealed before publication.
fn published_replacement() -> (Index, Arc<ivf_pages::Budget>, BTreeMap<u64, [f32; 2]>) {
    let (mut index, budget) = built();
    let mut expected = (0..BASE)
        .map(|row| (row, vector(row)))
        .collect::<BTreeMap<_, _>>();
    let mut state = index.begin_build(0).unwrap();
    let mut source = (0..BASE).map(|row| (row, vector(row)));
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    for row in PINNED {
        update(&mut index, row, vector(200 + row));
        expected.insert(row, vector(200 + row));
    }
    for row in LATE {
        index
            .commit_insert(Vector {
                partition: 0,
                row,
                values: &vector(row),
            })
            .unwrap();
        expected.insert(row, vector(row));
    }
    index.seal().unwrap();
    index.finish_ingestion(&mut state).unwrap();
    for _ in 0..128 {
        if index.advance_build(&mut state).unwrap() {
            break;
        }
    }
    assert_eq!(state.phase, BuildPhase::Ready, "the replacement publishes");
    assert!(
        inspect_catalog(index.storage(), 0, CAP)
            .unwrap()
            .unwrap()
            .legacy_runs
            > 0,
        "fixture: the replacement serves in state B with a legacy run to remap"
    );
    (index, budget, expected)
}

/// Remaps one entry, then serves one coalescing step; reports whether the
/// remap is complete and whether this step retired a legacy run.
fn remap_step(index: &mut Index) -> (bool, bool, u64) {
    let progress = index.advance_remap(0, 1).unwrap();
    index.advance_coalescing(0, 32).unwrap();
    (
        progress.complete,
        progress.run_retired,
        progress.entries_remapped,
    )
}

#[derive(Clone, Copy, Debug)]
enum Change {
    Update,
    Delete,
}

#[test]
fn a_row_changed_inside_the_remap_window_is_served_once_at_every_point() {
    for change in [Change::Update, Change::Delete] {
        let mut window_reached = false;
        let mut every_prefix_covered = false;
        // Change the pinned rows after 0, 1, 2, ... remap steps, until a run
        // whose remap completed before the change.
        for prefix in 0..256 {
            let (mut index, budget, mut expected) = published_replacement();
            assert_served_once(&index, &budget, &expected, "at publication");
            let mut complete = false;
            let mut prepared = 0;
            let mut retired = false;
            for step in 0..prefix {
                let (done, retired_now, remapped) = remap_step(&mut index);
                prepared += remapped;
                retired |= retired_now;
                complete = done;
                assert_served_once(
                    &index,
                    &budget,
                    &expected,
                    &format!("{change:?} after {prefix} steps: remap step {step}"),
                );
                if complete {
                    break;
                }
            }
            let completed_before_change = complete;
            if prepared > 0 && !retired {
                window_reached = true;
            }
            for row in PINNED {
                match change {
                    Change::Update => {
                        update(&mut index, row, vector(400 + row));
                        expected.insert(row, vector(400 + row));
                    }
                    Change::Delete => {
                        delete(&mut index, row);
                        expected.remove(&row);
                    }
                }
            }
            let when = format!("{change:?} after {prefix} remap steps");
            assert_served_once(
                &index,
                &budget,
                &expected,
                &format!("{when}: at the change"),
            );
            for step in 0..256 {
                if complete {
                    break;
                }
                let (done, _, _) = remap_step(&mut index);
                complete = done;
                assert_served_once(
                    &index,
                    &budget,
                    &expected,
                    &format!("{when}: remap step {step} after the change"),
                );
            }
            assert!(complete, "{when}: the remap completes");
            assert_served_once(
                &index,
                &budget,
                &expected,
                &format!("{when}: after retirement"),
            );
            let index = reopen(index, &budget);
            assert_served_once(&index, &budget, &expected, &format!("{when}: after reopen"));
            if completed_before_change {
                every_prefix_covered = true;
                break;
            }
        }
        assert!(
            every_prefix_covered,
            "{change:?}: the remap completes within the fixture"
        );
        assert!(
            window_reached,
            "{change:?}: some run changed the pinned rows after a remap step prepared a \
             legacy entry and before its legacy run retired"
        );
    }
}

#[test]
fn a_row_whose_only_live_copy_is_legacy_is_deleted_by_its_identity_in_state_b() {
    let (mut index, budget, mut expected) = published_replacement();
    // A late row lives only as a legacy entry until its run retires; a pinned
    // row's live copy is its legacy update.
    for row in [LATE.start, LATE.start + 1, PINNED[0]] {
        delete(&mut index, row);
        expected.remove(&row);
        assert_served_once(
            &index,
            &budget,
            &expected,
            &format!("after deleting row {row}"),
        );
        assert_eq!(
            current(&mut index, row),
            None,
            "row {row} has no current version"
        );
    }
    for step in 0..256 {
        let (done, _, _) = remap_step(&mut index);
        assert_served_once(&index, &budget, &expected, &format!("remap step {step}"));
        if done {
            break;
        }
    }
    assert_eq!(
        inspect_catalog(index.storage(), 0, CAP)
            .unwrap()
            .unwrap()
            .legacy_runs,
        0,
        "the remap completes"
    );
    let index = reopen(index, &budget);
    assert_served_once(&index, &budget, &expected, "after reopen");
}

#[test]
fn a_stale_identity_is_refused_and_changes_nothing() {
    let (mut index, budget) = built();
    let mut expected = (0..BASE)
        .map(|row| (row, vector(row)))
        .collect::<BTreeMap<_, _>>();
    let genesis = RowIdentity {
        row: PINNED[0],
        commit_lsn: 0,
    };
    assert_eq!(
        current(&mut index, PINNED[0]),
        Some(genesis),
        "a row built through the unversioned entry point carries the genesis version"
    );
    index
        .commit_update(0, genesis, &vector(300)[..])
        .expect("the genesis identity names the current version");
    expected.insert(PINNED[0], vector(300));
    let now = current(&mut index, PINNED[0]).expect("the updated row exists");
    assert!(
        now.commit_lsn > 0,
        "the update carries its own commit LSN: {now:?}"
    );

    let stale_cases = [
        genesis,
        RowIdentity {
            row: PINNED[0],
            commit_lsn: now.commit_lsn + 1,
        },
    ];
    for stale in stale_cases {
        for attempt in [
            index.commit_update(0, stale, &vector(301)[..]).map(|_| ()),
            index.delete_row(0, stale).map(|_| ()),
        ] {
            match attempt {
                Err(Error::StaleRowVersion {
                    expected: lsn,
                    current: found,
                }) => {
                    assert_eq!(lsn, stale.commit_lsn, "the refusal names the stale LSN");
                    assert_eq!(found, Some(now.commit_lsn), "and the current version");
                }
                other => panic!("a stale identity {stale:?} is refused, got {other:?}"),
            }
        }
        assert_eq!(current(&mut index, PINNED[0]), Some(now), "nothing changed");
        assert_served_once(
            &index,
            &budget,
            &expected,
            &format!("after refusing {stale:?}"),
        );
    }

    index
        .delete_row(0, now)
        .expect("the current identity deletes the row");
    expected.remove(&PINNED[0]);
    match index.commit_update(0, now, &vector(302)[..]) {
        Err(Error::StaleRowVersion {
            expected: lsn,
            current: None,
        }) => {
            assert_eq!(lsn, now.commit_lsn);
        }
        other => {
            panic!("an update of a deleted row is refused with no current version, got {other:?}")
        }
    }
    assert_served_once(
        &index,
        &budget,
        &expected,
        "after refusing the update of a deleted row",
    );
}

#[test]
fn a_rebuild_keeps_every_rows_own_version_identity() {
    let (mut index, budget) = built();
    let mut expected = (0..BASE)
        .map(|row| (row, vector(row)))
        .collect::<BTreeMap<_, _>>();
    for row in PINNED {
        update(&mut index, row, vector(200 + row));
        expected.insert(row, vector(200 + row));
    }
    let identities = (0..BASE)
        .map(|row| (row, current(&mut index, row).unwrap()))
        .collect::<BTreeMap<_, _>>();
    let mut state = index.begin_build(0).unwrap();
    let frontier = state.source_lsn;
    let mut source = expected
        .iter()
        .map(|(row, v)| (*row, *v))
        .collect::<Vec<_>>()
        .into_iter();
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    index.finish_ingestion(&mut state).unwrap();
    while !index.advance_build(&mut state).unwrap() {}
    for _ in 0..256 {
        if remap_step(&mut index).0 {
            break;
        }
    }
    for (row, identity) in &identities {
        let after = current(&mut index, *row).unwrap();
        assert_eq!(after, *identity, "row {row} keeps its own version identity");
        if PINNED.contains(row) {
            assert_ne!(
                after.commit_lsn, frontier,
                "never the build's source frontier"
            );
        }
    }
    let row = PINNED[1];
    index
        .commit_update(0, identities[&row], &vector(500)[..])
        .expect("the identity held before the rebuild still names the current version");
    expected.insert(row, vector(500));
    assert_served_once(
        &index,
        &budget,
        &expected,
        "after an update across the rebuild",
    );
}
