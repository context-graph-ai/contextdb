#![cfg(feature = "test-seams")]
#![expect(
    clippy::result_large_err,
    reason = "Preserve the original typed engine refusal."
)]
//! Eligibility precedes truncation and canonical traversal is exhaustive.
//! These exercise the production kernel through its byte-page fixture, not
//! native SQL authorization, formula evaluation or transaction overlays.
#[path = "support/hiding_pages.rs"]
mod hiding_pages;
#[path = "support/ivf_pages.rs"]
mod ivf_pages;

use contextdb_core::Error;
use contextdb_vector::ivf::storage::{PAGE_BYTES, PageKind, ReadPages};
use contextdb_vector::ivf::{
    CandidateIdentity, CandidateSource, CanonicalCandidate, ClusteredIndex, Hit, Mode, Policy,
    TrainingPolicy, Vector, Work, search, search_with_eligibility, visit_visible_canonical,
};
use hiding_pages::Hiding;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, atomic::Ordering};

type Index = ClusteredIndex<Hiding>;
const QUERY: [f32; 2] = [1.0, 0.0];
const ROWS: u64 = 8;
const LOG_CAPACITY: usize = 64;

fn values(row: u64) -> [f32; 2] {
    [1.0, row as f32 / 16.0]
}
fn policy() -> Policy {
    Policy::checked(2, 1, 1, 2, 1_024, true).unwrap()
}
fn training() -> TrainingPolicy {
    TrainingPolicy {
        iterations: 2,
        reservoir_samples: 32,
        seed: 11,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    }
}
fn built() -> (Index, Arc<ivf_pages::Budget>) {
    let budget = Arc::new(ivf_pages::Budget::default());
    let mut index = ClusteredIndex::new(
        Hiding::default(),
        policy(),
        training(),
        budget.clone().into(),
    )
    .unwrap();
    index
        .build_partition(0, (0..ROWS).map(|id| (id, values(id))))
        .unwrap();
    (index, budget)
}
fn record(log: &mut Vec<(u64, u64, u64)>, candidate: CandidateIdentity<'_>) {
    assert!(log.len() < LOG_CAPACITY, "candidate recorder overflow");
    log.push((candidate.partition, candidate.row.row, candidate.slot));
}
fn score(values: &[f32]) -> f64 {
    // Independent scalar oracle on the inserted stored F32 values.
    1.0 - f64::from(values[0])
        / (f64::from(values[0]).powi(2) + f64::from(values[1]).powi(2)).sqrt()
}
fn run(
    source: &impl ReadPages,
    budget: &Arc<ivf_pages::Budget>,
    mode: Mode,
    check: &mut dyn FnMut(CandidateIdentity<'_>) -> Result<bool, Error>,
) -> (Vec<u64>, Work) {
    let mut output = [Hit::default(); 2];
    let mut work = Work::default();
    let found = search_with_eligibility(
        source,
        policy(),
        &budget.clone().into(),
        &QUERY,
        Some(&[0]),
        mode,
        check,
        &mut output,
        &mut work,
    )
    .unwrap();
    (output[..found].iter().map(|hit| hit.row).collect(), work)
}

#[test]
fn rejected_nearest_rows_cannot_fill_the_shortlist_or_poison_a_shared_visibility_page() {
    let (index, budget) = built();
    let source = index.storage();
    let manifest = source.manifest(0).unwrap().unwrap();
    let mut before = [0; PAGE_BYTES];
    assert!(
        source
            .read_admitted_page(manifest.key(PageKind::Visibility, 0, 0), &mut before)
            .unwrap()
    );
    let mut calls = Vec::with_capacity(LOG_CAPACITY);
    let (ids, work) = run(source, &budget, Mode::Indexed, &mut |candidate| {
        record(&mut calls, candidate);
        Ok(candidate.row.row >= 6)
    });
    assert_eq!(
        ids,
        [6, 7],
        "eligibility precedes distance top-2 truncation"
    );
    assert_eq!(
        calls.len(),
        ROWS as usize,
        "every examined list identity is checked once"
    );
    assert_eq!(
        calls
            .iter()
            .map(|(_, row, _)| *row)
            .collect::<BTreeSet<_>>(),
        (0..ROWS).collect()
    );
    assert!(
        calls
            .iter()
            .all(|(_, _, slot)| slot / (PAGE_BYTES as u64 * 8) == 0),
        "both eligible and rejected records really share the cached bitmap page"
    );
    assert_eq!(work.quantized_evaluations, 2, "no rejected row is scored");
    assert_eq!(work.full_precision_evaluations, 2);
    let mut after = [0; PAGE_BYTES];
    assert!(
        source
            .read_admitted_page(manifest.key(PageKind::Visibility, 0, 0), &mut after)
            .unwrap()
    );
    assert_eq!(
        after, before,
        "query eligibility never edits durable visibility"
    );

    let (alternating, _) = run(source, &budget, Mode::Indexed, &mut |candidate| {
        Ok(candidate.row.row % 2 == 1)
    });
    assert_eq!(
        alternating,
        [1, 3],
        "one bitmap page contains different eligibility decisions"
    );
}

#[test]
fn rejecting_every_indexed_candidate_reads_no_canonical_payload_through_wrappers() {
    let (index, budget) = built();
    let source = index.storage();
    // Hiding and the internal Tracked decorator must preserve explicit eligibility.
    source.hide.set(Some(PageKind::Canonical));
    source.reads.borrow_mut().clear();
    let mut called = 0;
    let (ids, work) = run(source, &budget, Mode::Indexed, &mut |_| {
        called += 1;
        Ok(false)
    });
    assert!(ids.is_empty());
    assert_eq!(called, ROWS);
    assert_eq!(work.quantized_evaluations, 0);
    assert_eq!(work.full_precision_evaluations, 0);
    assert_eq!(
        source
            .reads
            .borrow()
            .get(&PageKind::Canonical)
            .copied()
            .unwrap_or(0),
        0
    );
    source.hide.set(None);
    let (positive, positive_work) = run(source, &budget, Mode::Indexed, &mut |_| Ok(true));
    assert_eq!(positive, [0, 1]);
    assert!(
        positive_work.canonical_pages_read > 0,
        "the positive control uses canonical pages"
    );
}

#[test]
fn canonical_visitor_reaches_a_formula_winner_outside_distance_limit_and_rerank() {
    let (index, budget) = built();
    for k in [1, 2] {
        let mut finalists: Vec<(u64, f64)> = Vec::with_capacity(k);
        let mut visited = [false; ROWS as usize];
        let mut work = Work::default();
        visit_visible_canonical(
            index.storage(),
            &budget.clone().into(),
            &QUERY,
            Some(&[0]),
            &mut |_| Ok::<_, Error>(true),
            &mut |candidate: CanonicalCandidate<'_>| {
                let row = candidate.identity.row.row;
                assert!(row < ROWS && !visited[row as usize]);
                visited[row as usize] = true;
                assert_eq!(candidate.values, values(row));
                assert_eq!(candidate.distance, score(candidate.values));
                // The port must feed the real engine evaluator in the native
                // companion guard; this scalar consumer pins traversal only.
                let formula = if row == 7 { 10.0 } else { 0.0 };
                let at = finalists
                    .iter()
                    .position(|(other, value)| {
                        formula > *value || (formula == *value && row < *other)
                    })
                    .unwrap_or(finalists.len());
                if at < k {
                    if finalists.len() == k {
                        finalists.pop();
                    }
                    finalists.insert(at, (row, formula));
                }
                Ok::<_, Error>(())
            },
            &mut work,
        )
        .unwrap();
        assert_eq!(finalists[0].0, 7);
        assert!(
            7 > k && 7 > policy().rerank(),
            "winner is outside both distance shortlists"
        );
        if k == 2 {
            assert_eq!(
                finalists[1].0, 0,
                "equal formula values use ascending row ID"
            );
        }
        assert!(
            visited.into_iter().all(|v| v),
            "LIMIT never ends exhaustive visitation"
        );
        assert_eq!(work.exact_score_count, ROWS);
        assert_eq!(
            work.full_precision_evaluations + work.buffer_evaluations,
            ROWS
        );
        assert_eq!(work.quantized_evaluations, 0);
        assert_eq!(work.centroid_evaluations, 0);
    }
}

#[test]
fn explicit_accept_all_preserves_existing_unfiltered_search_answers_and_work() {
    let (index, budget) = built();
    for mode in [Mode::Exact, Mode::Indexed] {
        let mut output = [Hit::default(); 2];
        let (found, original) = search(
            index.storage(),
            policy(),
            &budget.clone().into(),
            &QUERY,
            Some(&[0]),
            mode,
            &mut output,
        )
        .unwrap();
        let (ids, explicit) = run(index.storage(), &budget, mode, &mut |_| Ok(true));
        assert_eq!(
            ids,
            output[..found]
                .iter()
                .map(|hit| hit.row)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            explicit, original,
            "new explicit port retains the unchanged unfiltered route ledger"
        );
    }
}

#[derive(Debug)]
enum Refusal {
    Original(u64),
    Core(Error),
}
impl From<Error> for Refusal {
    fn from(value: Error) -> Self {
        Self::Core(value)
    }
}

#[test]
fn eligibility_failure_returns_the_original_consumer_error_and_stops_every_route() {
    let (index, budget) = built();
    for mode in [Mode::Exact, Mode::Indexed, Mode::Auto] {
        let mut calls = 0;
        let mut output = [Hit::default(); 2];
        let mut work = Work::default();
        let result = search_with_eligibility(
            index.storage(),
            policy(),
            &budget.clone().into(),
            &QUERY,
            Some(&[0]),
            mode,
            &mut |_| {
                calls += 1;
                if calls == 2 {
                    Err(Refusal::Original(0x7151))
                } else {
                    Ok(true)
                }
            },
            &mut output,
            &mut work,
        );
        assert!(
            matches!(result, Err(Refusal::Original(0x7151))),
            "original error survives {mode:?}: {result:?}"
        );
        assert_eq!(calls, 2, "no subsequent eligibility or AUTO retry");
        assert!(
            work.visibility_pages_read > 0,
            "failure keeps actual page work"
        );
        assert_eq!(work.quantized_evaluations + work.exact_score_count, 1);
    }
}

#[test]
fn consumer_corruption_shaped_error_never_triggers_auto_storage_fallback() {
    let (index, budget) = built();
    let mut calls = 0;
    let mut output = [Hit::default(); 2];
    let mut work = Work::default();
    let result = search_with_eligibility(
        index.storage(),
        policy(),
        &budget.clone().into(),
        &QUERY,
        Some(&[0]),
        Mode::Auto,
        &mut |_| {
            calls += 1;
            Err::<bool, Refusal>(Refusal::Core(Error::StoreCorrupted {
                path: "consumer-eligibility".into(),
                reason: "original-sentinel".into(),
            }))
        },
        &mut output,
        &mut work,
    );
    match result {
        Err(Refusal::Core(Error::StoreCorrupted { path, reason })) => {
            assert_eq!(path, "consumer-eligibility");
            assert_eq!(reason, "original-sentinel");
        }
        other => panic!("consumer error is returned unchanged: {other:?}"),
    }
    assert_eq!(
        calls, 1,
        "AUTO cannot retry a consumer error as storage corruption"
    );
    assert_eq!(work.exact_score_count, 0);
}

#[test]
fn visitor_failure_keeps_its_original_error_and_the_one_completed_distance() {
    let (index, budget) = built();
    let mut calls = 0;
    let before = budget.live.load(Ordering::SeqCst);
    let mut work = Work::default();
    let result = visit_visible_canonical(
        index.storage(),
        &budget.clone().into(),
        &QUERY,
        Some(&[0]),
        &mut |_| Ok::<_, Refusal>(true),
        &mut |_| {
            calls += 1;
            Err(Refusal::Original(0x8151))
        },
        &mut work,
    );
    assert!(
        matches!(result, Err(Refusal::Original(0x8151))),
        "{result:?}"
    );
    assert_eq!(calls, 1);
    assert_eq!(
        work.exact_score_count, 1,
        "the kernel calculated before calling the visitor"
    );
    assert_eq!(work.full_precision_evaluations + work.buffer_evaluations, 1);
    assert_eq!(work.quantized_evaluations + work.centroid_evaluations, 0);
    assert!(
        work.canonical_pages_read > 0,
        "page work survives the callback failure"
    );
    assert_eq!(
        budget.live.load(Ordering::SeqCst),
        before,
        "scratch refunded to its original funder"
    );
}

#[test]
fn distance_refusal_does_not_count_a_calculation_or_call_the_visitor() {
    let (index, budget) = built();
    let source = index.storage();
    source.inner.comparisons.set(0);
    source.inner.cancel_at_distance.set(Some(2));
    let mut visits = 0;
    let mut work = Work::default();
    let result = visit_visible_canonical(
        source,
        &budget.clone().into(),
        &QUERY,
        Some(&[0]),
        &mut |_| Ok::<_, Error>(true),
        &mut |_| {
            visits += 1;
            Ok(())
        },
        &mut work,
    );
    assert!(matches!(result, Err(Error::ReadCancelled)), "{result:?}");
    assert_eq!(visits, 1);
    assert_eq!(
        source.inner.comparisons.get(),
        2,
        "one successful and one refused admission"
    );
    assert_eq!(work.exact_score_count, 1);
    assert_eq!(work.full_precision_evaluations + work.buffer_evaluations, 1);
    source.inner.cancel_at_distance.set(None);
}

#[test]
fn a_pinned_page_view_keeps_its_versions_after_update_delete_and_tail_commit() {
    let (mut index, budget) = built();
    // A copied page fixture models a captured immutable transaction; this is
    // not proof of native reader registration, authentication or retention.
    let pinned = index.storage().inner.clone();
    let pinned_frontier = pinned.snapshot_lsn().unwrap();
    let original = index.current_row_identity(0, 1).unwrap().unwrap();
    let new_identity = index.commit_update(0, original, &values(15)).unwrap();
    index
        .delete_row(0, index.current_row_identity(0, 2).unwrap().unwrap())
        .unwrap();
    index
        .commit_insert(Vector {
            partition: 0,
            row: 20,
            values: &values(20),
        })
        .unwrap();
    assert!(index.storage().snapshot_lsn().unwrap() > pinned_frontier);
    let mut old_rows = BTreeSet::new();
    let mut work = Work::default();
    visit_visible_canonical(
        &pinned,
        &budget.clone().into(),
        &QUERY,
        Some(&[0]),
        &mut |candidate| {
            assert_eq!(candidate.snapshot_lsn.0, pinned_frontier);
            if candidate.row.row == 1 {
                assert_eq!(candidate.row, original);
            }
            Ok::<_, Error>(true)
        },
        &mut |candidate| {
            assert!(old_rows.insert(candidate.identity.row.row));
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    assert_eq!(old_rows, (0..ROWS).collect());
    let mut current = BTreeMap::new();
    let mut tail_seen = false;
    visit_visible_canonical(
        index.storage(),
        &budget.clone().into(),
        &QUERY,
        Some(&[0]),
        &mut |_| Ok::<_, Error>(true),
        &mut |candidate| {
            if candidate.identity.row.row == 20 {
                assert_eq!(candidate.identity.source, CandidateSource::UnsealedTail);
                tail_seen = true;
            }
            assert!(
                current
                    .insert(candidate.identity.row.row, candidate.identity.row)
                    .is_none()
            );
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    assert_eq!(
        current.keys().copied().collect::<BTreeSet<_>>(),
        [0, 1, 3, 4, 5, 6, 7, 20].into_iter().collect()
    );
    assert_eq!(
        current[&1], new_identity,
        "actual row LSN, never captured F0"
    );
    assert!(
        tail_seen,
        "the actual unsealed tail participates in exhaustive traversal"
    );
    assert_eq!(work.exact_score_count, ROWS);
}

#[test]
fn unrelated_partition_growth_changes_neither_selected_work_nor_query_scratch() {
    let (mut index, budget) = built();
    let baseline_live = budget.live.load(Ordering::SeqCst);
    budget.peak.store(baseline_live, Ordering::SeqCst);
    let (expected, before_work) = run(index.storage(), &budget, Mode::Indexed, &mut |_| Ok(true));
    let before_scratch = budget.peak.load(Ordering::SeqCst) - baseline_live;
    assert_eq!(budget.live.load(Ordering::SeqCst), baseline_live);
    for count in [16, 32] {
        index
            .build_partition(1, (0..count).map(|id| (id, values(id))))
            .unwrap();
        let live = budget.live.load(Ordering::SeqCst);
        budget.peak.store(live, Ordering::SeqCst);
        let (actual, after_work) = run(index.storage(), &budget, Mode::Indexed, &mut |candidate| {
            assert_eq!(
                candidate.partition, 0,
                "no unrelated eligibility prepass or scope collision"
            );
            Ok(true)
        });
        assert_eq!(actual, expected);
        assert_eq!(
            after_work, before_work,
            "fixed-scope logical work is independent of another tuple"
        );
        assert_eq!(budget.peak.load(Ordering::SeqCst) - live, before_scratch);
        assert_eq!(budget.live.load(Ordering::SeqCst), live);
    }
}

#[test]
fn rejected_exact_identity_needs_its_prefix_page_but_not_later_component_pages() {
    const DIM: usize = 2_048;
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(DIM, 1, 1, 1, 64 * 1_024, true).unwrap();
    let mut index =
        ClusteredIndex::new(Hiding::default(), policy, training(), budget.clone().into()).unwrap();
    let mut vector = vec![0.0; DIM];
    vector[0] = 1.0;
    index
        .build_partition(0, [(17, vector.clone())].into_iter())
        .unwrap();
    let source = index.storage();
    let manifest = source.manifest(0).unwrap().unwrap();
    let payload_page = manifest.key(PageKind::Canonical, 0, 1);
    let mut page = [0; PAGE_BYTES];
    assert!(
        source.read_admitted_page(payload_page, &mut page).unwrap(),
        "positive setup: this canonical vector really crosses the first page"
    );
    source.hide_key.set(Some(payload_page));
    let mut eligible_calls = 0;
    let mut visitor_calls = 0;
    let mut work = Work::default();
    let result = visit_visible_canonical(
        source,
        &budget.clone().into(),
        &vector,
        Some(&[0]),
        &mut |candidate| {
            assert_eq!(candidate.row.row, 17);
            eligible_calls += 1;
            Ok::<_, Error>(false)
        },
        &mut |_| {
            visitor_calls += 1;
            Ok(())
        },
        &mut work,
    );
    assert!(
        result.is_ok(),
        "rejected identity requires no vector component page: {result:?}"
    );
    assert_eq!(eligible_calls, 1);
    assert_eq!(visitor_calls, 0);
    assert_eq!(work.exact_score_count, 0);
    assert_eq!(
        work.canonical_pages_read, 1,
        "the authenticated identity-prefix page is real charged work"
    );
    let mut work = Work::default();
    let accepted = visit_visible_canonical(
        source,
        &budget.clone().into(),
        &vector,
        Some(&[0]),
        &mut |_| Ok::<_, Error>(true),
        &mut |_| Ok(()),
        &mut work,
    );
    assert!(
        matches!(accepted, Err(Error::StoreCorrupted { .. })),
        "positive control: an accepted row needs the missing vector payload: {accepted:?}"
    );
    source.hide_key.set(None);
}

#[test]
fn an_indexed_canonical_prefix_mismatch_is_corruption_before_any_answer() {
    use contextdb_vector::ivf::storage::{PageStore, WritePages};
    let (index, budget) = built();
    let mut source = index.into_storage();
    let manifest = source.manifest(0).unwrap().unwrap();
    let key = manifest.key(PageKind::Canonical, 0, 0);
    let mut page = [0; PAGE_BYTES];
    assert!(source.read_admitted_page(key, &mut page).unwrap());
    assert_eq!(
        u64::from_le_bytes(page[..8].try_into().unwrap()),
        0,
        "the nearest indexed candidate's canonical prefix is row0"
    );
    page[..8].copy_from_slice(&999_u64.to_le_bytes());
    let mut tx = source.begin().unwrap();
    tx.put_page(key, &page).unwrap();
    tx.commit().unwrap();
    let mut output = [Hit::default(); 2];
    let mut work = Work::default();
    let result = search_with_eligibility(
        &source,
        policy(),
        &budget.clone().into(),
        &QUERY,
        Some(&[0]),
        Mode::Indexed,
        &mut |_| Ok::<_, Error>(true),
        &mut output,
        &mut work,
    );
    assert!(
        matches!(result, Err(Error::StoreCorrupted { .. })),
        "a different canonical row cannot be returned under the list identity: {result:?}"
    );
}

#[test]
fn unexamined_list_identities_on_a_shared_bitmap_page_are_not_prechecked() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 2, 1, 2, 1_024, false).unwrap();
    let mut index =
        ClusteredIndex::new(Hiding::default(), policy, training(), budget.clone().into()).unwrap();
    // Two well-separated, deterministic populations; explicit breadth1.
    index
        .build_partition(
            0,
            (0..16).map(|id| {
                (
                    id,
                    if id < 8 {
                        [1.0, id as f32 / 128.0]
                    } else {
                        [-1.0, (id - 8) as f32 / 128.0]
                    },
                )
            }),
        )
        .unwrap();
    let mut output = [Hit::default(); 2];
    let mut calls = Vec::with_capacity(LOG_CAPACITY);
    let mut work = Work::default();
    let found = search_with_eligibility(
        index.storage(),
        policy,
        &budget.clone().into(),
        &QUERY,
        Some(&[0]),
        Mode::Indexed,
        &mut |candidate| {
            record(&mut calls, candidate);
            assert!(
                candidate.row.row < 8,
                "unselected list's identities must not be prechecked"
            );
            Ok::<_, Error>(true)
        },
        &mut output,
        &mut work,
    )
    .unwrap();
    assert_eq!(found, 2);
    assert_eq!([output[0].row, output[1].row], [0, 1]);
    assert_eq!(work.lists_probed, 1, "explicit breadth must not widen");
    assert_eq!(
        calls.iter().map(|(_, id, _)| *id).collect::<BTreeSet<_>>(),
        (0..8).collect(),
        "positive control: every selected-list identity was checked"
    );
    assert!(
        calls
            .iter()
            .all(|(_, _, slot)| slot / (PAGE_BYTES as u64 * 8) == 0)
    );
    let manifest = index.storage().manifest(0).unwrap().unwrap();
    assert_eq!(manifest.slots, 16);
    assert!(
        manifest.slots < PAGE_BYTES as u64 * 8,
        "unselected canonical slots share the same actual visibility-page range"
    );
}

#[test]
fn default_probe_extension_counts_eligible_rows_and_explicit_breadth_does_not_widen() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let explicit = Policy::checked(2, 2, 1, 2, 1_024, false).unwrap();
    let default_extension = Policy::checked(2, 2, 1, 2, 1_024, true).unwrap();
    let mut index = ClusteredIndex::new(
        Hiding::default(),
        explicit,
        training(),
        budget.clone().into(),
    )
    .unwrap();
    index
        .build_partition(
            0,
            (0..16).map(|id| {
                (
                    id,
                    if id < 8 {
                        [1.0, id as f32 / 128.0]
                    } else {
                        [-1.0, (id - 8) as f32 / 128.0]
                    },
                )
            }),
        )
        .unwrap();
    for (policy, expected_lists, expected_count) in [(explicit, 1, 0), (default_extension, 2, 2)] {
        let mut output = [Hit::default(); 2];
        let mut work = Work::default();
        let found = search_with_eligibility(
            index.storage(),
            policy,
            &budget.clone().into(),
            &QUERY,
            Some(&[0]),
            Mode::Indexed,
            &mut |candidate| Ok::<_, Error>(candidate.row.row >= 8),
            &mut output,
            &mut work,
        )
        .unwrap();
        assert_eq!(found, expected_count);
        assert_eq!(work.lists_probed, expected_lists);
        assert!(output[..found].iter().all(|hit| hit.row >= 8));
        if policy.extend_to_k() {
            assert_eq!(
                work.quantized_evaluations, 8,
                "only the second list's eligible rows are scored"
            );
        } else {
            assert_eq!(work.quantized_evaluations, 0);
        }
    }
}

#[test]
fn legacy_remap_and_committed_runs_visit_each_effective_row_version_once() {
    use contextdb_vector::ivf::{inspect_catalog, storage::BuildPhase};
    let (mut index, budget) = built();
    let mut state = index.begin_build(0).unwrap();
    let mut source = (0..ROWS).map(|id| (id, values(id)));
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    index
        .commit_insert(Vector {
            partition: 0,
            row: 20,
            values: &values(20),
        })
        .unwrap();
    let legacy_identity = index.current_row_identity(0, 20).unwrap().unwrap();
    index.seal().unwrap();
    index.finish_ingestion(&mut state).unwrap();
    for _ in 0..128 {
        if index.advance_build(&mut state).unwrap() {
            break;
        }
    }
    assert_eq!(state.phase, BuildPhase::Ready);
    assert!(
        inspect_catalog(index.storage(), 0, 1_024)
            .unwrap()
            .unwrap()
            .legacy_runs
            > 0,
        "precondition: a real legacy run remains after publication"
    );
    index
        .commit_insert(Vector {
            partition: 0,
            row: 21,
            values: &values(21),
        })
        .unwrap();
    index.seal().unwrap();
    let committed_identity = index.current_row_identity(0, 21).unwrap().unwrap();
    let mut complete = false;
    for step in 0..128 {
        let mut seen = BTreeSet::new();
        let mut legacy_seen = false;
        let mut committed_seen = false;
        let mut work = Work::default();
        visit_visible_canonical(
            index.storage(),
            &budget.clone().into(),
            &QUERY,
            Some(&[0]),
            &mut |_| Ok::<_, Error>(true),
            &mut |candidate| {
                let identity = candidate.identity;
                assert!(
                    seen.insert(identity.row.row),
                    "no prepared/remapped duplicate at step{step}"
                );
                match identity.row.row {
                    20 => {
                        assert_eq!(identity.row, legacy_identity);
                        legacy_seen = identity.source == CandidateSource::LegacyRun;
                    }
                    21 => {
                        assert_eq!(identity.row, committed_identity);
                        committed_seen = identity.source == CandidateSource::CommittedRun;
                    }
                    _ => {}
                }
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        assert_eq!(seen, (0..ROWS).chain([20, 21]).collect());
        assert_eq!(work.exact_score_count, ROWS + 2);
        if step == 0 {
            assert!(legacy_seen, "the legacy role was actually traversed");
            assert!(
                committed_seen,
                "the current committed-run role was actually traversed"
            );
        }
        let progress = index.advance_remap(0, 1).unwrap();
        index.advance_coalescing(0, 32).unwrap();
        if progress.complete {
            complete = true;
            break;
        }
    }
    assert!(complete, "the explicit remap driver terminates");
}
