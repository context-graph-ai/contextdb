//! Independent native ports of public assertions in the frozen HNSW inventory.
//! The assertion map and private-counterpart gaps live in the index-ports receipt.
#![cfg(feature = "test-seams")]
#![expect(
    clippy::result_large_err,
    reason = "The public typed error retains inline refusal diagnostics."
)]
#[path = "hnsw_public_ports/pages.rs"]
mod ivf_pages;
use contextdb_core::{Error, Result, VectorIndexRef};
use contextdb_vector::ivf::storage::*;
use contextdb_vector::{
    MemoryBudget,
    ivf::{
        self, CandidateIdentity, ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, Vector, Work,
    },
};
use std::{
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug, Default)]
struct Payer {
    live: AtomicUsize,
    peak: AtomicUsize,
    limit: AtomicUsize,
    calls: AtomicUsize,
    refuse: AtomicUsize,
}
impl Payer {
    fn unlimited() -> Arc<Self> {
        Arc::new(Self {
            limit: AtomicUsize::new(usize::MAX),
            ..Self::default()
        })
    }
    fn used(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }
}
impl MemoryBudget for Payer {
    fn try_allocate_for(&self, n: usize, _: &str, operation: &str, _: &str) -> Result<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let held = self.used();
        let limit = self.limit.load(Ordering::SeqCst);
        if call == self.refuse.load(Ordering::SeqCst) || n > limit.saturating_sub(held) {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: "port".into(),
                operation: operation.to_owned().into(),
                requested_bytes: n,
                available_bytes: limit.saturating_sub(held),
                budget_limit_bytes: limit,
                hint: "independent admission guard".into(),
            });
        }
        let next = self.live.fetch_add(n, Ordering::SeqCst) + n;
        self.peak.fetch_max(next, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, n: usize) {
        assert!(self.live.fetch_sub(n, Ordering::SeqCst) >= n);
    }
}
fn policy(d: usize, breadth: usize) -> Policy {
    Policy::checked(
        d,
        1,
        1,
        breadth,
        (d * size_of::<f32>() + size_of::<RowIdentity>()) * breadth.max(4),
        true,
    )
    .unwrap()
}
use ivf::RowIdentity;
fn training() -> TrainingPolicy {
    TrainingPolicy {
        iterations: 2,
        reservoir_samples: 64,
        seed: 11,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    }
}
fn spread(n: usize, d: usize) -> Vec<f32> {
    (0..d)
        .map(|axis| {
            let mixed = (n as u64 + 17)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add((axis as u64 + 31).wrapping_mul(0xbf58_476d_1ce4_e5b9));
            ((mixed ^ (mixed >> 29)) % 100_003) as f32 / 100_003.0 - 0.5
        })
        .collect()
}
fn built(
    rows: &[(u64, Vec<f32>)],
    p: Policy,
    payer: &Arc<Payer>,
) -> ClusteredIndex<ivf_pages::Pages> {
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        p,
        training(),
        payer.clone().into(),
    )
    .unwrap();
    index
        .build_partition(0, rows.iter().map(|(id, v)| (*id, v.as_slice())))
        .unwrap();
    index
}
fn run(
    source: &impl ReadPages,
    p: Policy,
    payer: &Arc<Payer>,
    q: &[f32],
    k: usize,
    mode: Mode,
    allow: impl Fn(u64) -> bool,
) -> Result<(Vec<Hit>, Work)> {
    let mut output = vec![Hit::default(); k];
    let mut work = Work::default();
    let n = ivf::search_with_eligibility(
        source,
        p,
        &payer.clone().into(),
        q,
        None,
        mode,
        &mut |identity: CandidateIdentity<'_>| Ok::<_, Error>(allow(identity.row.row)),
        &mut output,
        &mut work,
    )?;
    output.truncate(n);
    Ok((output, work))
}
fn ids(hits: &[Hit]) -> Vec<u64> {
    hits.iter().map(|h| h.row).collect()
}
fn rows(n: usize, d: usize) -> Vec<(u64, Vec<f32>)> {
    (0..n).map(|i| (i as u64 + 1, spread(i, d))).collect()
}

#[test]
fn prepared_insert_validation_fails_without_changing_graph_points() {
    let payer = Payer::unlimited();
    let p = policy(2, 4);
    let mut index = built(&[(1, vec![1., 0.])], p, &payer);
    let before = index.storage().manifest(0).unwrap();
    assert!(
        index
            .commit_insert(Vector {
                partition: 0,
                row: 2,
                values: &[1., 0., 0.]
            })
            .is_err()
    );
    assert_eq!(index.storage().manifest(0).unwrap(), before);
    assert_eq!(
        ids(&run(
            index.storage(),
            p,
            &payer,
            &[1., 0.],
            4,
            Mode::Indexed,
            |_| true
        )
        .unwrap()
        .0),
        [1]
    );
}
#[test]
fn predicate_search_without_seed_rows_still_traverses_but_absent_candidates_do_not() {
    let payer = Payer::unlimited();
    let p = policy(3, 2);
    let index = built(&[(1, vec![1., 0., 0.]), (2, vec![0., 1., 0.])], p, &payer);
    let (answer, work) = run(
        index.storage(),
        p,
        &payer,
        &[0.9, 0.1, 0.],
        1,
        Mode::Indexed,
        |_| true,
    )
    .unwrap();
    assert_eq!(answer[0].row, 1);
    assert!(work.distance_evaluations() > 0);
    let (answer, work) = run(
        index.storage(),
        p,
        &payer,
        &[0.9, 0.1, 0.],
        1,
        Mode::Indexed,
        |id| id == 99,
    )
    .unwrap();
    assert!(answer.is_empty());
    assert_eq!(
        work.quantized_evaluations + work.full_precision_evaluations + work.buffer_evaluations,
        0
    );
}
#[test]
fn mutable_quantized_graphs_insert_without_rebuilding() {
    let payer = Payer::unlimited();
    let p = policy(3, 4);
    let mut index = built(&[(11, vec![1., 0., 0.])], p, &payer);
    let generation = index.storage().manifest(0).unwrap().unwrap().generation;
    index
        .commit_insert(Vector {
            partition: 0,
            row: 29,
            values: &[0., 1., 0.],
        })
        .unwrap();
    assert_eq!(
        index.storage().manifest(0).unwrap().unwrap().generation,
        generation
    );
    let (answer, _) = run(
        index.storage(),
        p,
        &payer,
        &[0., 1., 0.],
        1,
        Mode::Indexed,
        |_| true,
    )
    .unwrap();
    assert_eq!(answer[0].row, 29);
    assert_eq!(
        run(
            index.storage(),
            p,
            &payer,
            &[0., 1., 0.],
            4,
            Mode::Indexed,
            |_| true
        )
        .unwrap()
        .0
        .len(),
        2
    );
}
#[test]
fn paged_durable_generation_keeps_topology_and_nearest_neighbor() {
    let payer = Payer::unlimited();
    let p = policy(3, 4);
    let index = built(
        &[
            (11, vec![1., 0., 0.]),
            (29, vec![0., 1., 0.]),
            (31, vec![0., 0., 1.]),
        ],
        p,
        &payer,
    );
    let manifest = index.storage().manifest(0).unwrap();
    let reopened =
        ClusteredIndex::new(index.into_storage(), p, training(), payer.clone().into()).unwrap();
    assert_eq!(reopened.storage().manifest(0).unwrap(), manifest);
    assert_eq!(
        run(
            reopened.storage(),
            p,
            &payer,
            &[0., 1., 0.],
            1,
            Mode::Indexed,
            |_| true
        )
        .unwrap()
        .0[0]
            .row,
        29
    );
    drop(reopened);
    assert_eq!(payer.used(), 0);
}
#[test]
fn saved_graph_pages_recover_exact_top_ten_when_search_breadth_covers_every_row() {
    let payer = Payer::unlimited();
    let vectors = rows(64, 16);
    let p = policy(16, vectors.len());
    let index = built(&vectors, p, &payer);
    for probe in 0..200 {
        let q = spread(vectors.len() + probe, 16);
        let expected = run(index.storage(), p, &payer, &q, 10, Mode::Exact, |_| true)
            .unwrap()
            .0;
        let actual = run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |_| true)
            .unwrap()
            .0;
        assert_eq!(ids(&actual), ids(&expected), "probe={probe}");
    }
}
#[test]
fn saved_graph_pages_return_the_exact_lowest_row_ids_for_equal_vectors() {
    let payer = Payer::unlimited();
    let vectors = (1..=64)
        .map(|id| (id, vec![1., 0., 0.]))
        .collect::<Vec<_>>();
    let index = built(&vectors, policy(3, vectors.len()), &payer);
    for breadth in [64, 10] {
        let answer = run(
            index.storage(),
            policy(3, breadth),
            &payer,
            &[1., 0., 0.],
            10,
            Mode::Indexed,
            |_| true,
        )
        .unwrap()
        .0;
        assert_eq!(ids(&answer), (1..=10).collect::<Vec<_>>());
    }
}
#[test]
fn a_full_result_heap_replaces_larger_row_ids_at_equal_distance() {
    let payer = Payer::unlimited();
    let p = policy(2, 3);
    let mut index = built(
        &[(21, vec![1., 0.]), (22, vec![1., 0.]), (23, vec![1., 0.])],
        p,
        &payer,
    );
    for row in [1, 2, 3] {
        index
            .commit_insert(Vector {
                partition: 0,
                row,
                values: &[1., 0.],
            })
            .unwrap();
    }
    assert_eq!(
        ids(&run(
            index.storage(),
            p,
            &payer,
            &[0., 1.],
            3,
            Mode::Indexed,
            |_| true
        )
        .unwrap()
        .0),
        [1, 2, 3]
    );
}
#[test]
fn layer_seed_spacing_fills_declared_breadth_with_uneven_membership() {
    let payer = Payer::unlimited();
    let p = policy(3, 10);
    let vectors = (1..=17)
        .map(|row| (row, vec![row as f32, 19. - row as f32, 1.]))
        .collect::<Vec<_>>();
    let index = built(&vectors, p, &payer);
    let answer = run(
        index.storage(),
        p,
        &payer,
        &[0.9, 0.2, 0.1],
        10,
        Mode::Indexed,
        |_| true,
    )
    .unwrap()
    .0;
    assert_eq!(answer.len(), 10);
    assert!(answer.iter().all(|h| (1..=17).contains(&h.row)));
}
#[test]
fn ordinary_allowed_search_excluded_history_adds_no_visit_or_distance_work() {
    let payer = Payer::unlimited();
    let p = policy(3, 16);
    let vectors = (0..8)
        .map(|i| (i + 1, vec![i as f32 + 1., 9. - i as f32, 1.]))
        .collect::<Vec<_>>();
    let mut index = built(&vectors, p, &payer);
    let before = run(
        index.storage(),
        p,
        &payer,
        &[1., 8., 1.],
        4,
        Mode::Indexed,
        |id| id <= 8,
    )
    .unwrap();
    for ordinal in 8..2000 {
        index
            .commit_insert(Vector {
                partition: 0,
                row: ordinal + 1,
                values: &[ordinal as f32 + 1., 2001. - ordinal as f32, 1.],
            })
            .unwrap();
    }
    let after = run(
        index.storage(),
        p,
        &payer,
        &[1., 8., 1.],
        4,
        Mode::Indexed,
        |id| id <= 8,
    )
    .unwrap();
    assert!(
        after.1.quantized_evaluations
            + after.1.full_precision_evaluations
            + after.1.buffer_evaluations
            <= before.1.quantized_evaluations
                + before.1.full_precision_evaluations
                + before.1.buffer_evaluations
    );
    assert!(after.0.iter().all(|h| h.row <= 8));
}
#[test]
fn the_settled_rows_copy_is_charged_and_given_back() {
    let payer = Payer::unlimited();
    let p = policy(8, 16);
    let mut index = built(&rows(16, 8), p, &payer);
    index
        .build_partition(1, rows(16, 8).into_iter().map(|(id, v)| (id + 16, v)))
        .unwrap();
    let held = payer.used();
    payer.peak.store(held, Ordering::SeqCst);
    assert!(
        !run(
            index.storage(),
            p,
            &payer,
            &spread(100, 8),
            10,
            Mode::Indexed,
            |_| true
        )
        .unwrap()
        .0
        .is_empty()
    );
    assert!(payer.peak.load(Ordering::SeqCst) > held);
    assert_eq!(payer.used(), held);
    drop(index);
    assert_eq!(payer.used(), 0);
}

/// A caller-owned captured native page source. Only failure injection and its
/// counters are private; scoring, eligibility, admission and refunds stay real.
struct Controlled<'a> {
    inner: &'a ivf_pages::Pages,
    checks: Cell<usize>,
    distances: Cell<usize>,
    stop: Option<usize>,
    distance_limit: Option<usize>,
    refusal: &'static str,
    fail_page: Cell<bool>,
}
impl<'a> Controlled<'a> {
    fn new(inner: &'a ivf_pages::Pages) -> Self {
        Self {
            inner,
            checks: Cell::new(0),
            distances: Cell::new(0),
            stop: None,
            distance_limit: None,
            refusal: "distance budget exhausted",
            fail_page: Cell::new(false),
        }
    }
}
impl ReadPages for Controlled<'_> {
    fn inspection_verdict(&self, p: u64, g: u64) -> Option<Verdict> {
        self.inner.inspection_verdict(p, g)
    }
    fn record_inspection_verdict(&self, p: u64, g: u64, v: Verdict) {
        self.inner.record_inspection_verdict(p, g, v)
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        self.inner.snapshot_lsn()
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.inner.index_identity()
    }
    fn check_cancel(&self) -> Result<()> {
        let next = self.checks.get() + 1;
        self.checks.set(next);
        if self.stop == Some(next) {
            Err(Error::ReadCancelled)
        } else {
            self.inner.check_cancel()
        }
    }
    fn before_distance(&self, _: DistanceKind) -> Result<()> {
        let next = self.distances.get() + 1;
        self.distances.set(next);
        if self.distance_limit.is_some_and(|limit| next > limit) {
            Err(Error::Other(self.refusal.into()))
        } else {
            Ok(())
        }
    }
    fn manifest(&self, p: u64) -> Result<Option<Manifest>> {
        self.inner.manifest(p)
    }
    fn build_state(&self, p: u64) -> Result<Option<BuildState>> {
        self.inner.build_state(p)
    }
    fn next_partition(&self, p: Option<u64>) -> Result<Option<u64>> {
        self.inner.next_partition(p)
    }
    fn read_admitted_page(&self, k: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        if self.fail_page.replace(false) {
            Err(Error::Other("short source page".into()))
        } else {
            self.inner.read_admitted_page(k, out)
        }
    }
}
fn failure_refunds(filtered: bool) {
    let payer = Payer::unlimited();
    let p = policy(16, 64);
    let index = built(&rows(64, 16), p, &payer);
    let held = payer.used();
    let q = spread(77, 16);
    let source = Controlled::new(index.storage());
    let answer = run(&source, p, &payer, &q, 10, Mode::Indexed, |id| {
        !filtered || id.is_multiple_of(2)
    })
    .unwrap()
    .0;
    assert_eq!(answer.len(), 10);
    assert_eq!(payer.used(), held);
    let checks = source.checks.get();
    assert!(checks > 0);
    // Every real cancellation checkpoint, including page faults and scores.
    for at in 1..=checks {
        let mut source = Controlled::new(index.storage());
        source.stop = Some(at);
        let result = run(&source, p, &payer, &q, 10, Mode::Indexed, |id| {
            !filtered || id.is_multiple_of(2)
        });
        assert!(
            matches!(result, Err(Error::ReadCancelled)),
            "checkpoint {at}"
        );
        assert_eq!(payer.used(), held, "checkpoint {at}");
    }
    payer.calls.store(0, Ordering::SeqCst);
    run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |id| {
        !filtered || id.is_multiple_of(2)
    })
    .unwrap();
    let calls = payer.calls.load(Ordering::SeqCst);
    assert!(calls > 0);
    for at in 1..=calls {
        payer.calls.store(0, Ordering::SeqCst);
        payer.refuse.store(at, Ordering::SeqCst);
        let result = run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |id| {
            !filtered || id.is_multiple_of(2)
        });
        assert!(
            matches!(result, Err(Error::MemoryBudgetExceeded { .. })),
            "allocation {at}"
        );
        assert_eq!(payer.used(), held, "allocation {at}");
    }
    payer.refuse.store(0, Ordering::SeqCst);
    assert_eq!(
        run(
            index.storage(),
            p,
            &payer,
            &q,
            10,
            Mode::Indexed,
            |id| !filtered || id.is_multiple_of(2)
        )
        .unwrap()
        .0,
        answer
    );
    drop(index);
    assert_eq!(payer.used(), 0);
}
#[test]
fn an_unfiltered_search_refused_or_cancelled_gives_back_all_it_was_charged() {
    failure_refunds(false)
}
#[test]
fn a_filtered_search_refused_or_cancelled_gives_back_all_it_was_charged() {
    failure_refunds(true)
}
#[test]
fn a_failed_page_read_leaves_nothing_resident() {
    let payer = Payer::unlimited();
    let p = policy(16, 64);
    let index = built(&rows(64, 16), p, &payer);
    let held = payer.used();
    let source = Controlled::new(index.storage());
    source.fail_page.set(true);
    assert!(
        run(
            &source,
            p,
            &payer,
            &spread(69, 16),
            10,
            Mode::Indexed,
            |_| true
        )
        .is_err()
    );
    assert_eq!(payer.used(), held);
    assert_eq!(
        run(
            &source,
            p,
            &payer,
            &spread(69, 16),
            10,
            Mode::Indexed,
            |_| true
        )
        .unwrap()
        .0
        .len(),
        10
    );
}
#[test]
fn saved_graph_pages_refuse_the_same_distance_budget_as_owned_graphs() {
    let payer = Payer::unlimited();
    let vectors = rows(128, 16);
    let p = policy(16, 200);
    let index = built(&vectors, p, &payer);
    let saved = index.storage().clone();
    for probe in 0..20 {
        for breadth in [10, 50, 200] {
            for limit in [1, 5, 20] {
                let q = spread(128 + probe, 16);
                let mut outcomes = Vec::new();
                for pages in [index.storage(), &saved] {
                    let mut source = Controlled::new(pages);
                    source.distance_limit = Some(limit);
                    let answer = run(
                        &source,
                        policy(16, breadth),
                        &payer,
                        &q,
                        10,
                        Mode::Indexed,
                        |_| true,
                    )
                    .map(|(hits, _)| ids(&hits))
                    .map_err(|e| e.to_string());
                    outcomes.push((source.distances.get(), answer));
                }
                assert_eq!(
                    outcomes[1], outcomes[0],
                    "probe={probe} breadth={breadth} limit={limit}"
                );
            }
        }
    }
}
#[test]
fn saved_graph_pages_charge_the_same_distance_expansions_as_owned_graphs() {
    let payer = Payer::unlimited();
    let p = policy(16, 200);
    let index = built(&rows(128, 16), p, &payer);
    let saved = index.storage().clone();
    for probe in 0..50 {
        for breadth in [10, 50, 200] {
            let q = spread(128 + probe, 16);
            let mut outcomes = Vec::new();
            for pages in [index.storage(), &saved] {
                let source = Controlled::new(pages);
                let (answer, work) = run(
                    &source,
                    policy(16, breadth),
                    &payer,
                    &q,
                    10,
                    Mode::Indexed,
                    |_| true,
                )
                .unwrap();
                assert_eq!(source.distances.get() as u64, work.distance_evaluations());
                outcomes.push((source.distances.get(), ids(&answer)));
            }
            assert_eq!(outcomes[1], outcomes[0], "probe={probe} breadth={breadth}");
        }
    }
}
#[test]
fn saved_graph_pages_preserve_owned_neighbor_ids_and_order_across_search_breadths() {
    let payer = Payer::unlimited();
    for population in [64, 5000] {
        let p = policy(16, 200);
        let index = built(&rows(population, 16), p, &payer);
        let saved = index.storage().clone();
        for probe in 0..200 {
            for breadth in [10, 50, 200] {
                let q = spread(population + probe, 16);
                let expected = run(
                    index.storage(),
                    policy(16, breadth),
                    &payer,
                    &q,
                    10,
                    Mode::Indexed,
                    |_| true,
                )
                .unwrap()
                .0;
                let actual = run(
                    &saved,
                    policy(16, breadth),
                    &payer,
                    &q,
                    10,
                    Mode::Indexed,
                    |_| true,
                )
                .unwrap()
                .0;
                assert_eq!(
                    ids(&actual),
                    ids(&expected),
                    "population={population} probe={probe} breadth={breadth}"
                );
            }
        }
    }
}
#[test]
fn saved_graph_pages_preserve_multi_partition_union_neighbor_order() {
    let payer = Payer::unlimited();
    let p = policy(16, 200);
    let mut index = built(&rows(64, 16), p, &payer);
    index
        .build_partition(1, (64..128).map(|i| (i as u64 + 1, spread(i, 16))))
        .unwrap();
    let saved = index.storage().clone();
    for probe in 0..200 {
        for breadth in [10, 50, 200] {
            let q = spread(128 + probe, 16);
            assert_eq!(
                run(
                    &saved,
                    policy(16, breadth),
                    &payer,
                    &q,
                    10,
                    Mode::Indexed,
                    |_| true
                )
                .unwrap()
                .0,
                run(
                    index.storage(),
                    policy(16, breadth),
                    &payer,
                    &q,
                    10,
                    Mode::Indexed,
                    |_| true
                )
                .unwrap()
                .0
            );
        }
    }
}
#[test]
fn mixed_saved_base_and_resident_tail_keep_exact_row_id_ties_at_breadth_one() {
    for (q, base, tail) in [
        ([0., 0., 0.], [1., 0., 0.], [1., 0., 0.]),
        ([1., 0., 0.], [0., 0., 0.], [0., 0., 0.]),
        ([0., 1., 0.], [1., 0., 0.], [1., 0., 0.]),
    ] {
        let payer = Payer::unlimited();
        let p = policy(3, 1);
        let mut index = built(&[(1, base.to_vec())], p, &payer);
        index
            .commit_insert(Vector {
                partition: 0,
                row: 2,
                values: &tail,
            })
            .unwrap();
        let source = Controlled::new(index.storage());
        let (answer, work) = run(&source, p, &payer, &q, 1, Mode::Indexed, |_| true).unwrap();
        assert_eq!(ids(&answer), [1]);
        assert!(work.distance_evaluations() > 0);
        assert_eq!(source.distances.get() as u64, work.distance_evaluations());
    }
}
#[test]
fn restricted_exact_ties_probe_only_allowed_layer_membership() {
    for history in [0, 64, 2048_u64] {
        let payer = Payer::unlimited();
        let p = policy(3, 16);
        let vectors = (1..=history)
            .chain(10000..10010)
            .map(|id| (id, vec![1., 0., 0.]))
            .collect::<Vec<_>>();
        let index = built(&vectors, p, &payer);
        let (answer, work) = run(
            index.storage(),
            p,
            &payer,
            &[1., 0., 0.],
            10,
            Mode::Indexed,
            |id| id >= 10000,
        )
        .unwrap();
        assert_eq!(ids(&answer), (10000..10010).collect::<Vec<_>>());
        assert!(work.distance_evaluations() < 128);
    }
}

#[test]
fn embedded_filtered_breadth_matches_the_same_walk_without_caller_work_limits() {
    let payer = Payer::unlimited();
    let p = policy(32, 64);
    let index = built(&rows(512, 32), p, &payer);
    for probe in 0..8 {
        let q = spread(1003 + probe, 32);
        let plain = run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |id| {
            id.is_multiple_of(2)
        })
        .unwrap();
        let source = Controlled::new(index.storage());
        let bounded = run(&source, p, &payer, &q, 10, Mode::Indexed, |id| {
            id.is_multiple_of(2)
        })
        .unwrap();
        assert_eq!(plain, bounded);
        assert!(bounded.1.quantized_evaluations < 512);
    }
}
#[test]
fn minimum_one_page_cache_decodes_a_cross_boundary_vector_with_balanced_scratch() {
    let payer = Payer::unlimited();
    let dimension = PAGE_BYTES / size_of::<f32>() + 1;
    let p = policy(dimension, 4);
    let vectors = rows(4, dimension);
    let index = built(&vectors, p, &payer);
    let held = payer.used();
    payer.peak.store(held, Ordering::SeqCst);
    let answer = run(
        index.storage(),
        p,
        &payer,
        &vectors[0].1,
        1,
        Mode::Indexed,
        |_| true,
    )
    .unwrap()
    .0;
    assert_eq!(answer[0].row, vectors[0].0);
    assert!(answer[0].distance.abs() <= 1e-6);
    assert_eq!(payer.used(), held);
    assert!(payer.peak.load(Ordering::SeqCst) > held);
    assert!(
        index
            .storage()
            .touched
            .borrow()
            .iter()
            .any(|k| k.kind == PageKind::Canonical && k.ordinal > 0)
    );
}
#[test]
fn paged_wide_vector_scores_every_middle_page_and_partial_tail() {
    let dimension = PAGE_BYTES * 2 / size_of::<f32>() + 257;
    let vector = (0..dimension)
        .map(|i| ((i * 37 + 11) % 997) as f32 / 997.)
        .collect::<Vec<_>>();
    let q = (0..dimension)
        .map(|i| ((i * 19 + 7) % 991) as f32 / 991.)
        .collect::<Vec<_>>();
    let payer = Payer::unlimited();
    let p = policy(dimension, 4);
    let index = built(&[(1, vector.clone())], p, &payer);
    index.storage().touched.borrow_mut().clear();
    let actual = run(index.storage(), p, &payer, &q, 1, Mode::Indexed, |_| true)
        .unwrap()
        .0[0]
        .distance;
    let expected = 1. - f64::from(contextdb_vector::cosine_similarity(&vector, &q));
    assert!(
        (actual - expected).abs() <= 1e-6,
        "paged score {actual}, body score {expected}"
    );
    let canonical = index
        .storage()
        .touched
        .borrow()
        .iter()
        .filter(|k| k.kind == PageKind::Canonical)
        .count();
    let spanned = (vector.len() * size_of::<f32>()).div_ceil(PAGE_BYTES);
    assert!(spanned >= 3);
    assert!(canonical >= spanned);
}
#[test]
fn a_page_scored_vector_matches_the_score_its_own_body_gives() {
    for dimension in [
        96,
        PAGE_BYTES / size_of::<f32>(),
        PAGE_BYTES / size_of::<f32>() + 129,
    ] {
        let payer = Payer::unlimited();
        let p = policy(dimension, 8);
        let vectors = (1..=5)
            .map(|id| {
                (
                    id,
                    (0..dimension)
                        .map(|c| (((c as u64 * 31 + id * 17) % 1021) as f32 / 1021.) - 0.5)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let q = (0..dimension)
            .map(|c| (((c * 23 + 5) % 1013) as f32 / 1013.) - 0.5)
            .collect::<Vec<_>>();
        let index = built(&vectors, p, &payer);
        let answer = run(
            index.storage(),
            p,
            &payer,
            &q,
            vectors.len(),
            Mode::Indexed,
            |_| true,
        )
        .unwrap()
        .0;
        for hit in answer {
            let vector = &vectors.iter().find(|(id, _)| *id == hit.row).unwrap().1;
            let (dot, aa, bb) = vector.iter().zip(&q).fold(
                (0.0_f64, 0.0_f64, 0.0_f64),
                |(dot, aa, bb), (&a, &b)| {
                    let (a, b) = (f64::from(a), f64::from(b));
                    (dot + a * b, aa + a * a, bb + b * b)
                },
            );
            let from_body = (dot / (aa.sqrt() * bb.sqrt())) as f32;
            let from_pages = (1. - hit.distance) as f32;
            assert_eq!(
                from_pages.to_bits(),
                from_body.to_bits(),
                "dimension={dimension} row={}",
                hit.row
            );
        }
    }
}
#[test]
fn a_refused_settled_copy_stops_the_second_partition_inside_its_charge() {
    let payer = Payer::unlimited();
    let p = policy(8, 16);
    let mut index = built(&rows(16, 8), p, &payer);
    index
        .build_partition(1, rows(16, 8).into_iter().map(|(id, v)| (id + 16, v)))
        .unwrap();
    let held = payer.used();
    payer.calls.store(0, Ordering::SeqCst);
    run(
        index.storage(),
        p,
        &payer,
        &spread(100, 8),
        10,
        Mode::Indexed,
        |_| true,
    )
    .unwrap();
    let calls = payer.calls.load(Ordering::SeqCst);
    for at in 1..=calls {
        payer.calls.store(0, Ordering::SeqCst);
        payer.refuse.store(at, Ordering::SeqCst);
        assert!(
            run(
                index.storage(),
                p,
                &payer,
                &spread(100, 8),
                10,
                Mode::Indexed,
                |_| true
            )
            .is_err()
        );
        assert_eq!(payer.used(), held);
    }
    payer.refuse.store(0, Ordering::SeqCst);
    drop(index);
    assert_eq!(payer.used(), 0);
}

#[test]
fn mutable_graph_insertion_preserves_existing_points_and_build_identity() {
    let payer = Payer::unlimited();
    let p = policy(3, 4);
    let mut index = built(&[(11, vec![1., 0., 0.])], p, &payer);
    let generation = index.storage().manifest(0).unwrap().unwrap().generation;
    index
        .commit_insert(Vector {
            partition: 0,
            row: 11,
            values: &[0., 1., 0.],
        })
        .unwrap();
    index
        .commit_insert(Vector {
            partition: 0,
            row: 29,
            values: &[0., 0., 1.],
        })
        .unwrap();
    assert_eq!(
        index.storage().manifest(0).unwrap().unwrap().generation,
        generation
    );
    for (q, wanted) in [([1., 0., 0.], 11), ([0., 1., 0.], 11), ([0., 0., 1.], 29)] {
        assert_eq!(
            run(index.storage(), p, &payer, &q, 1, Mode::Indexed, |_| true)
                .unwrap()
                .0[0]
                .row,
            wanted
        );
    }
}
#[test]
fn durable_generation_round_trip_keeps_search_maps_and_seals_the_graph() {
    let payer = Payer::unlimited();
    let p = policy(3, 4);
    let index = built(&[(11, vec![1., 0., 0.]), (29, vec![0., 1., 0.])], p, &payer);
    let saved = index.into_storage();
    let before = saved.manifest(0).unwrap();
    let reopened = ClusteredIndex::new(saved, p, training(), payer.clone().into()).unwrap();
    assert_eq!(reopened.storage().manifest(0).unwrap(), before);
    assert_eq!(
        run(
            reopened.storage(),
            p,
            &payer,
            &[0., 1., 0.],
            4,
            Mode::Indexed,
            |_| true
        )
        .unwrap()
        .0
        .len(),
        2
    );
    assert_eq!(
        run(
            reopened.storage(),
            p,
            &payer,
            &[0., 1., 0.],
            1,
            Mode::Indexed,
            |_| true
        )
        .unwrap()
        .0[0]
            .row,
        29
    );
    // Published pages are read only: ReadPages has no mutation operation.
    // The old sealed-HnswIndex::insert refusal has no native object counterpart.
}
#[test]
fn durable_codec_capacity_bounds_all_stored_representations_and_checks_overflow() {
    let payer = Payer::unlimited();
    for (count, dimension) in [(0, 1), (2, 9), (31, 9), (31, 768)] {
        let p = policy(dimension, count.max(4));
        let layout = ivf::workspace_allocation_layout(p, training()).unwrap();
        let peak = payer.used();
        let index = built(&rows(count, dimension), p, &payer);
        assert!(payer.used() - peak <= layout.charged_bytes);
        assert_eq!(
            run(
                index.storage(),
                p,
                &payer,
                &spread(100, dimension),
                count,
                Mode::Exact,
                |_| true
            )
            .unwrap()
            .0
            .len(),
            count
        );
        drop(index);
        assert_eq!(payer.used(), 0);
    }
    assert!(ivf::codebook_allocation_layout(usize::MAX, 1, true).is_err());
    assert!(ivf::query_workspace_allocation_bytes(policy(9, 31), usize::MAX).is_err());
    assert!(
        Policy::checked(9, 1, 1, 31, usize::MAX, true)
            .and_then(|p| ivf::workspace_allocation_layout(p, training()).ok())
            .is_none()
    );
}
#[test]
fn a_resident_paged_visit_pays_only_its_own_distance() {
    let payer = Payer::unlimited();
    let p = policy(768, 100);
    let index = built(&rows(1250, 768), p, &payer);
    let q = spread(1257, 768);
    let source = Controlled::new(index.storage());
    let first = run(&source, p, &payer, &q, 10, Mode::Indexed, |_| true).unwrap();
    assert_eq!(first.0.len(), 10);
    assert!(source.distances.get() > 0);
    assert_eq!(
        source.distances.get() as u64,
        first.1.distance_evaluations()
    );
    let second = run(&source, p, &payer, &q, 10, Mode::Indexed, |_| true).unwrap();
    assert_eq!(second.0, first.0);
}
#[test]
fn reaching_a_point_costs_the_same_however_much_a_walk_has_reached() {
    let payer = Payer::unlimited();
    let p = policy(768, 400);
    let index = built(&rows(1250, 768), p, &payer);
    let source = Controlled::new(index.storage());
    let (answer, work) = run(
        &source,
        p,
        &payer,
        &spread(1261, 768),
        10,
        Mode::Indexed,
        |_| true,
    )
    .unwrap();
    assert_eq!(answer.len(), 10);
    assert_eq!(source.distances.get() as u64, work.distance_evaluations());
    // Reached-entry/edge bookkeeping belongs only to the deleted graph walk.
}
#[test]
fn layered_allowed_membership_bounds_history_work_and_preserves_results() {
    let payer = Payer::unlimited();
    let p = policy(3, 16);
    let current = (1..=10)
        .map(|id| (id, vec![id as f32 + 0.5, 11. - id as f32, 1.]))
        .collect::<Vec<_>>();
    let mut expected = None;
    for history in [0, 64, 512, 2048] {
        let mut vectors = current[..1].to_vec();
        vectors.extend((0..history).map(|id| (100 + id, vec![0.2, 0.3 + id as f32 / 4096., 0.9])));
        let mut index = built(&vectors, p, &payer);
        for (id, v) in &current[1..] {
            index
                .commit_insert(Vector {
                    partition: 0,
                    row: *id,
                    values: v,
                })
                .unwrap();
        }
        let (answer, work) = run(
            index.storage(),
            p,
            &payer,
            &[0.83, 0.41, 0.19],
            10,
            Mode::Indexed,
            |id| id <= 10,
        )
        .unwrap();
        assert_eq!(answer.len(), 10);
        let mut got = ids(&answer);
        got.sort_unstable();
        assert_eq!(got, (1..=10).collect::<Vec<_>>());
        assert!(work.distance_evaluations() < 128);
        if let Some(expected) = &expected {
            assert_eq!(&answer, expected);
        } else {
            expected = Some(answer);
        }
    }
}
fn selective_spread(role: u64, ordinal: usize, dims: usize) -> Vec<f32> {
    (0..dims)
        .map(|axis| {
            let seed = (ordinal as u64)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add((axis as u64).wrapping_mul(0x85eb_ca6b_ba21_1f3d))
                .wrapping_add(role.wrapping_mul(0xc2b2_ae35_9d5e_e1b1))
                % 10007;
            (seed as f32 / 10007.) - 0.5
        })
        .collect()
}
#[test]
fn a_selective_filter_does_not_cost_the_graph_its_answer() {
    let mut failures = Vec::new();
    for population in [1250, 10000] {
        let payer = Payer::unlimited();
        let p = policy(32, 100);
        let vectors = (0..population)
            .map(|i| (i as u64 + 1, selective_spread(1, i, 32)))
            .collect::<Vec<_>>();
        let index = built(&vectors, p, &payer);
        let saved = index.storage().clone();
        for (route, pages) in [("writer", index.storage()), ("saved", &saved)] {
            for every in [1, 2, 10, 20, 50, 100] {
                let mut matched = 0;
                let allowed_count = population.div_ceil(every);
                for probe in 0..100 {
                    let q = selective_spread(2, probe, 32);
                    let allowed = |id: u64| (id as usize - 1).is_multiple_of(every);
                    let expected = run(pages, p, &payer, &q, 10, Mode::Exact, allowed)
                        .unwrap()
                        .0;
                    let answer = run(pages, p, &payer, &q, 10, Mode::Indexed, allowed)
                        .unwrap()
                        .0;
                    matched += answer
                        .iter()
                        .filter(|h| expected.iter().any(|e| e.row == h.row))
                        .count();
                }
                let possible = 100 * 10.min(allowed_count);
                if matched * 100 < possible * 95 {
                    failures.push(format!("route={route} population={population} every={every} recall={matched}/{possible}"));
                }
            }
            let measured = Controlled::new(pages);
            run(
                &measured,
                p,
                &payer,
                &selective_spread(2, 0, 32),
                10,
                Mode::Indexed,
                |id| (id as usize - 1).is_multiple_of(50),
            )
            .unwrap();
            let measured_distances = measured.distances.get();
            let tight = measured_distances
                .checked_sub(1)
                .expect("the native query performs distance work");
            println!(
                "derived_tight_work route={route} population={population} measured={measured_distances} limit={tight}"
            );
            let mut source = Controlled::new(pages);
            source.distance_limit = Some(tight);
            let answer = run(
                &source,
                p,
                &payer,
                &selective_spread(2, 0, 32),
                10,
                Mode::Indexed,
                |id| (id as usize - 1).is_multiple_of(50),
            );
            if answer.is_ok() {
                failures.push(format!("BEHAVIOR CHANGE candidate route={route} population={population}: original tight work limit requires incomplete, native completes in {} distances",source.distances.get()));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[path = "hnsw_public_ports/custody.rs"]
mod physical_custody;

#[test]
fn base_body_cache_separates_captured_layers_and_releases_the_original_funder() {
    let payer = Payer::unlimited();
    let p = policy(128, 8);
    let mut index = built(&[(7, vec![1.; 128])], p, &payer);
    let older = index.storage().clone();
    let old_manifest = older.manifest(0).unwrap().unwrap();
    let old = ivf::RetainedCodebook::load(&payer.clone().into(), &older, old_manifest).unwrap();
    let held = payer.used();
    assert!(old.charged_bytes() >= 128 * size_of::<f32>());
    index
        .build_partition(0, [(7, vec![2.; 128])].into_iter())
        .unwrap();
    let current = index.storage().manifest(0).unwrap().unwrap();
    let new = ivf::RetainedCodebook::load(&payer.clone().into(), index.storage(), current).unwrap();
    assert!(payer.used() > held);
    assert!(!old.matches(current));
    assert!(new.matches(current));
    assert!(old.matches(old_manifest));
    let old_answer = run(&older, p, &payer, &[1.; 128], 1, Mode::Indexed, |_| true)
        .unwrap()
        .0;
    let new_answer = run(
        index.storage(),
        p,
        &payer,
        &[2.; 128],
        1,
        Mode::Indexed,
        |_| true,
    )
    .unwrap()
    .0;
    assert_eq!(ids(&old_answer), [7]);
    assert_eq!(ids(&new_answer), [7]);
    drop(new);
    assert_eq!(payer.used(), held);
    drop(old);
    drop(index);
    assert_eq!(payer.used(), 0);
}
#[test]
fn a_large_declared_degree_opens_under_its_own_resident_bound() {
    let payer = Payer::unlimited();
    let vectors = rows(600, 16);
    for lists in [16, 64, 128] {
        let p = Policy::checked(
            16,
            lists,
            lists,
            64,
            policy(16, 64).buffer_cap_bytes(),
            true,
        )
        .unwrap();
        let index = built(&vectors, p, &payer);
        let storage = index.into_storage();
        assert_eq!(payer.used(), 0);
        let expected = ivf::workspace_allocation_layout(p, training())
            .unwrap()
            .charged_bytes;
        payer.limit.store(expected, Ordering::SeqCst);
        let reopened = ClusteredIndex::new(storage, p, training(), payer.clone().into()).unwrap();
        assert!(payer.used() <= expected);
        assert_eq!(
            reopened.storage().manifest(0).unwrap().unwrap().slots,
            vectors.len() as u64
        );
        drop(reopened);
        assert_eq!(payer.used(), 0);
        payer.limit.store(usize::MAX, Ordering::SeqCst);
    }
}

fn member_fixture() -> Vec<(u64, Vec<f32>)> {
    (1..=250)
        .map(|id| (id, vec![1.; 8]))
        .chain((251..=300).map(|id| (id, spread(id as usize, 8))))
        .collect()
}
#[test]
fn unsuccessful_member_probes_of_the_disconnected_count_are_each_charged() {
    let payer = Payer::unlimited();
    let p = policy(8, 400);
    let index = built(&member_fixture(), p, &payer);
    let held = payer.used();
    let source = Controlled::new(index.storage());
    let previous = Cell::new(0);
    let probes = Cell::new(0);
    let mut output = [Hit::default(); 10];
    let mut work = Work::default();
    let n = ivf::search_with_eligibility(
        &source,
        p,
        &payer.clone().into(),
        &spread(1000, 8),
        None,
        Mode::Indexed,
        &mut |identity: CandidateIdentity<'_>| {
            assert!(
                source.checks.get() > previous.replace(source.checks.get()),
                "a checkpoint precedes every member probe, a miss included"
            );
            probes.set(probes.get() + 1);
            Ok::<_, Error>(identity.row.row == 1 || identity.row.row > 250)
        },
        &mut output,
        &mut work,
    )
    .unwrap();
    assert_eq!(n, 10);
    assert!(probes.get() >= 51);
    assert_eq!(payer.used(), held);
    for at in 1..=source.checks.get() {
        let mut stopped = Controlled::new(index.storage());
        stopped.stop = Some(at);
        assert!(matches!(
            run(
                &stopped,
                p,
                &payer,
                &spread(1000, 8),
                10,
                Mode::Indexed,
                |id| id == 1 || id > 250
            ),
            Err(Error::ReadCancelled)
        ));
        assert_eq!(payer.used(), held);
    }
}
#[test]
fn member_growth_of_the_disconnected_count_is_charged_stoppable_and_refusable() {
    let payer = Payer::unlimited();
    let p = policy(8, 400);
    let index = built(&member_fixture(), p, &payer);
    let held = payer.used();
    let q = spread(1000, 8);
    let source = Controlled::new(index.storage());
    let answer = run(&source, p, &payer, &q, 10, Mode::Indexed, |_| true)
        .unwrap()
        .0;
    assert!(!answer.is_empty());
    assert_eq!(payer.used(), held);
    for at in 1..=source.checks.get() {
        let mut stopped = Controlled::new(index.storage());
        stopped.stop = Some(at);
        assert!(matches!(
            run(&stopped, p, &payer, &q, 10, Mode::Indexed, |_| true),
            Err(Error::ReadCancelled)
        ));
        assert_eq!(payer.used(), held);
    }
    let limit = held + ivf::query_workspace_allocation_bytes(p, 10).unwrap() - 1;
    payer.limit.store(limit, Ordering::SeqCst);
    assert!(run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |_| true).is_err());
    assert_eq!(payer.used(), held);
    payer.limit.store(usize::MAX, Ordering::SeqCst);
    assert_eq!(
        run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |_| true)
            .unwrap()
            .0
            .len(),
        answer.len()
    );
}
#[test]
fn opening_and_expansion_share_the_original_per_index_cap() {
    let payer = Payer::unlimited();
    let p = policy(16, 32);
    let index = built(&rows(96, 16), p, &payer);
    let mut saw_refusal = false;
    let mut saw_scoring = false;
    for cap in 1..=96 {
        let mut source = Controlled::new(index.storage());
        source.distance_limit = Some(cap);
        let mut output = [Hit::default(); 10];
        let mut work = Work::default();
        let result = ivf::search_with_work(
            &source,
            p,
            &payer.clone().into(),
            &spread(1000, 16),
            None,
            Mode::Indexed,
            &mut output,
            &mut work,
        );
        let spent = work.distance_evaluations() as usize;
        saw_refusal |= result.is_err();
        saw_scoring |= spent > 1;
        assert!(spent <= cap, "all phases spend the original single cap");
        assert_eq!(
            source.distances.get(),
            spent + usize::from(result.is_err()),
            "every successful comparison shares the cap; one unpaid callback may refuse"
        );
    }
    assert!(saw_scoring);
    assert!(saw_refusal);
}

#[test]
fn paged_traversal_retains_independently_authenticated_source_pages() {
    let payer = Payer::unlimited();
    let p = policy(768, 200);
    let vectors = (0..192)
        .map(|ordinal| {
            (
                ordinal + 1,
                (0..768)
                    .map(|c| {
                        (((ordinal + 17) * (c as u64 + 31) + c as u64 * 7) % 997) as f32 / 997.
                    })
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    let index = built(&vectors, p, &payer);
    let q = vec![0.314159; 768];
    let first = run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |_| true)
        .unwrap()
        .0;
    assert!(!first.is_empty());
    let second = run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |_| true)
        .unwrap()
        .0;
    assert!(!second.is_empty());
    let selected = (0..10).map(|i| 1 + i * 19).collect::<Vec<_>>();
    let filtered = run(index.storage(), p, &payer, &q, 10, Mode::Indexed, |id| {
        selected.binary_search(&id).is_ok()
    })
    .unwrap()
    .0;
    assert_eq!(filtered.len(), selected.len());
    assert!(
        filtered
            .iter()
            .all(|h| selected.binary_search(&h.row).is_ok())
    );
    assert!(!index.storage().touched.borrow().is_empty());
    // Physical authenticated-page retention and cache-hit I/O counts need the
    // unavailable engine read-I/O witness; logical PageKey reads do not prove it.
}
#[test]
fn a_search_locks_its_serving_cache_once_per_page_it_resolves() {
    let payer = Payer::unlimited();
    let p = policy(128, 256);
    let vectors = rows(256, 128);
    let index = built(&vectors, p, &payer);
    let q = &vectors[0].1;
    let first = run(index.storage(), p, &payer, q, 256, Mode::Indexed, |_| true)
        .unwrap()
        .0;
    let unchanged = run(index.storage(), p, &payer, q, 256, Mode::Indexed, |_| true)
        .unwrap()
        .0;
    let reloaded =
        ClusteredIndex::new(index.into_storage(), p, training(), payer.clone().into()).unwrap();
    let reread = run(reloaded.storage(), p, &payer, q, 256, Mode::Indexed, |_| {
        true
    })
    .unwrap()
    .0;
    assert_eq!(first, unchanged, "cache lookup does not change scores");
    assert_eq!(first, reread, "scores survive source reload");
    // HNSW serving-cache lock/directory/eviction counters have no native hook.
}
#[test]
fn optional_base_body_refusal_preserves_navigation_and_unwind_refunds_it() {
    let build = Payer::unlimited();
    let p = policy(128, 8);
    let index = built(&rows(8, 128), p, &build);
    let pages = index.into_storage();
    let payer = Payer::unlimited();
    let m = pages.manifest(0).unwrap().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let book = ivf::RetainedCodebook::load(&payer.clone().into(), &pages, m).unwrap();
        assert!(payer.used() > 0);
        assert!(book.matches(m));
        panic!("caller unwinds with admitted native values");
    }));
    assert!(result.is_err());
    assert_eq!(payer.used(), 0);
    let charge = ivf::codebook_allocation_layout(m.dimensions, m.active_lists, false)
        .unwrap()
        .charged_bytes;
    payer.limit.store(charge - 1, Ordering::SeqCst);
    assert!(ivf::RetainedCodebook::load(&payer.clone().into(), &pages, m).is_err());
    assert_eq!(payer.used(), 0);
    // There is no native optional graph-navigation cache: fallback navigation
    // and its fixed-limit eviction clauses remain explicit counterpart gaps.
}
#[test]
fn unfiltered_upper_descent_finishes_each_level_before_searching_bottom() {
    let payer = Payer::unlimited();
    let p = policy(2, 4);
    let index = built(
        &[
            (4, vec![0., 1.]),
            (6, vec![0.5, 0.5]),
            (8, vec![0.9, 0.1]),
            (10, vec![1., 0.]),
        ],
        p,
        &payer,
    );
    let saved = index.storage().clone();
    for (route, pages) in [("writer", index.storage()), ("saved-page", &saved)] {
        assert_eq!(
            ids(
                &run(pages, p, &payer, &[1., 0.], 1, Mode::Indexed, |_| true)
                    .unwrap()
                    .0
            ),
            [10],
            "{route}"
        );
        let mut source = Controlled::new(pages);
        source.distance_limit = Some(2);
        source.refusal = "upper descent work limit";
        let refused = run(&source, p, &payer, &[1., 0.], 1, Mode::Indexed, |_| true);
        assert!(
            matches!(refused,Err(Error::Other(message))if message=="upper descent work limit"),
            "{route}"
        );
        assert_eq!(source.distances.get(), 3, "{route}");
    }
    // Graph level descent and pre-bottom scratch ordering have no counterpart.
}
