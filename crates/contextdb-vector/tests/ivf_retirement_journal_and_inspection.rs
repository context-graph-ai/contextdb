#![cfg(feature = "test-seams")]
#![expect(
    clippy::result_large_err,
    reason = "The shared engine Error keeps inline refusal diagnostics; the guards return it unchanged."
)]
//! A replacement always finishes, deletions live in the row journal, and
//! inspection and stored counters never mislead:
//! - with a seal before every maintenance call, the remap examines each of the
//!   L legacy entries once and retires every legacy run within
//!   ceil(L / B) + R + 1 remap calls, B entries per call over R legacy runs,
//!   at a run cap of one and of two; state B ends, a rebuild is accepted, and
//!   a partition whose new codebook was damaged in state B reaches state C
//!   and rebuilds;
//! - a deletion writes only the row journal (row versions and row copies) and
//!   visibility pages, and stays in force after the generation it was made
//!   under retires and its pages are reclaimed, through reopen;
//! - catalog inspection reads no codebook or directory page, healthy or
//!   damaged, and still reports the rebuild recovery action once a write has
//!   met the damage;
//! - a stored list-page used count above what a page holds never panics a
//!   seal: writes commit, EXACT answers, and inspection shows the rebuild
//!   action; a stored remap cursor past its run's entries is store
//!   corruption with the rebuild action, never a remap that silently never
//!   progresses, and maintenance then still reaches state C so the rebuild it
//!   advertises is accepted and serves every row.
#[path = "support/hiding_pages.rs"]
mod hiding_pages;
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::{Error, Result};
use contextdb_vector::ivf::storage::{
    BuildPhase, PAGE_BYTES, PageKind, PageStore, ReadPages, WritePages,
};
use contextdb_vector::ivf::{
    ClusteredIndex, Hit, Mode, Policy, RowIdentity, TrainingPolicy, Vector, inspect_catalog, search,
};
use hiding_pages::Hiding;
use std::sync::Arc;

const BASE: u64 = 16;
/// Rows committed and sealed between the source frontier and publication:
/// the legacy entries a remap must carry over, one entry each.
const LATE: std::ops::Range<u64> = 16..28;
const LEGACY_ENTRIES: u64 = LATE.end - LATE.start;
const REMAP_ENTRIES_PER_CALL: usize = 2;
const COALESCE_PAGES: usize = 32;

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
        .with_coalesce_pages_per_step(COALESCE_PAGES)
        .unwrap()
}

fn vector(row: u64) -> [f32; 2] {
    let x = row as f32 * 0.031;
    [x.cos(), x.sin()]
}

fn built<S: PageStore>(store: S, cap: u64) -> (ClusteredIndex<S>, Arc<ivf_pages::Budget>) {
    let budget = Arc::new(ivf_pages::Budget::default());
    let mut index =
        ClusteredIndex::new(store, policy(cap), training(), budget.clone().into()).unwrap();
    index
        .build_partition(0, (0..BASE).map(|row| (row, vector(row))))
        .unwrap();
    (index, budget)
}

fn answered(
    store: &impl ReadPages,
    policy: Policy,
    budget: &Arc<ivf_pages::Budget>,
    mode: Mode,
) -> Result<Vec<u64>> {
    let mut out = vec![Hit::default(); 512];
    let (count, _) = search(
        store,
        policy,
        &budget.clone().into(),
        &vector(0),
        None,
        mode,
        &mut out,
    )?;
    let mut rows = out[..count].iter().map(|hit| hit.row).collect::<Vec<_>>();
    rows.sort_unstable();
    Ok(rows)
}

fn commit<S: PageStore>(index: &mut ClusteredIndex<S>, row: u64) {
    index
        .commit_insert(Vector {
            partition: 0,
            row,
            values: &vector(row),
        })
        .unwrap_or_else(|error| panic!("committing row {row} succeeds: {error:?}"));
}

/// Publishes a replacement of rows 0..BASE with the late rows committed and
/// sealed in between, so it serves in state B with legacy entries to remap.
fn in_state_b<S: PageStore>(index: &mut ClusteredIndex<S>, cap: u64) {
    let mut state = index.begin_build(0).unwrap();
    let mut source = (0..BASE).map(|row| (row, vector(row)));
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    for row in LATE {
        commit(index, row);
    }
    index.seal().unwrap();
    index.finish_ingestion(&mut state).unwrap();
    for _ in 0..256 {
        if index.advance_build(&mut state).unwrap() {
            break;
        }
        index.advance_coalescing(0, COALESCE_PAGES).unwrap();
    }
    assert_eq!(state.phase, BuildPhase::Ready, "the replacement publishes");
    assert!(
        inspect_catalog(index.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .legacy_runs
            > 0,
        "fixture: the replacement serves in state B"
    );
}

/// The bound on remap calls from the fixture's own population: every legacy
/// entry needs one examination at `REMAP_ENTRIES_PER_CALL` per call, each of
/// the `legacy_runs` runs may end its last call short of a full batch, and
/// one call more covers a retirement commit on its own.
fn remap_call_bound(legacy_runs: u64) -> usize {
    (LEGACY_ENTRIES as usize).div_ceil(REMAP_ENTRIES_PER_CALL) + legacy_runs as usize + 1
}

/// Drives the remap with one seal before every maintenance call (each
/// `advance_remap` and each `advance_coalescing`) and returns the rows
/// committed, after requiring every legacy entry to be examined exactly once
/// and the remap to complete within `remap_call_bound`.
fn remap_under_continuous_sealing<S: PageStore>(
    index: &mut ClusteredIndex<S>,
    cap: u64,
    mut next: u64,
    when: &str,
) -> u64 {
    let legacy_runs = inspect_catalog(index.storage(), 0, cap)
        .unwrap()
        .unwrap()
        .legacy_runs;
    let bound = remap_call_bound(legacy_runs);
    let mut remap_calls = 0usize;
    let mut examined = 0u64;
    let mut complete = false;
    for _ in 0..256 {
        commit(index, next);
        index.seal().unwrap();
        next += 1;
        let progress = index
            .advance_remap(0, REMAP_ENTRIES_PER_CALL)
            .unwrap_or_else(|error| panic!("{when}: a remap step proceeds: {error:?}"));
        remap_calls += 1;
        examined += progress.entries_examined;
        if progress.complete {
            complete = true;
            break;
        }
        commit(index, next);
        index.seal().unwrap();
        next += 1;
        index.advance_coalescing(0, COALESCE_PAGES).unwrap();
    }
    assert!(
        complete && remap_calls <= bound,
        "{when}, cap {cap}: the remap of {LEGACY_ENTRIES} legacy entries in {legacy_runs} runs \
         retires within {bound} remap calls though a seal lands before every maintenance call; \
         remap calls: {remap_calls}, complete: {complete}, entries examined: {examined}"
    );
    assert_eq!(
        examined, LEGACY_ENTRIES,
        "{when}, cap {cap}: each legacy entry is examined exactly once"
    );
    assert_eq!(
        inspect_catalog(index.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .legacy_runs,
        0,
        "{when}, cap {cap}: state B has ended"
    );
    next
}

fn rebuild<S: PageStore>(index: &mut ClusteredIndex<S>, rows: u64, when: &str) {
    let mut state = index
        .begin_build(0)
        .unwrap_or_else(|error| panic!("{when}: a rebuild is accepted after state B: {error:?}"));
    let mut source = (0..rows).map(|row| (row, vector(row)));
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    index.finish_ingestion(&mut state).unwrap();
    for _ in 0..256 {
        if index.advance_build(&mut state).unwrap() {
            break;
        }
        index.advance_coalescing(0, COALESCE_PAGES).unwrap();
    }
    assert_eq!(
        state.phase,
        BuildPhase::Ready,
        "{when}: the rebuild publishes"
    );
}

fn damage_codebook(store: &mut impl PageStore) {
    let manifest = store.manifest(0).unwrap().unwrap();
    // The first codebook page after the identity header page holds centroid
    // values: one changed byte no longer matches the digest the manifest's
    // codebook identity binds.
    let key = manifest.key(PageKind::Codebook, 0, 1);
    let mut page = [0u8; PAGE_BYTES];
    assert!(
        store.read_admitted_page(key, &mut page).unwrap(),
        "fixture: the serving codebook's first value page exists"
    );
    page[0] ^= 0x40;
    let mut tx = store.begin().unwrap();
    tx.put_page(key, &page).unwrap();
    tx.commit().unwrap();
}

#[test]
fn a_completed_remap_retires_under_continuous_sealing_at_every_cap() {
    for cap in [1, 2] {
        let (mut index, budget) = built(ivf_pages::Pages::default(), cap);
        in_state_b(&mut index, cap);
        let next = remap_under_continuous_sealing(&mut index, cap, LATE.end, "healthy");
        rebuild(&mut index, next, "healthy");
        let policy = index.policy();
        for mode in [Mode::Exact, Mode::Indexed] {
            assert_eq!(
                answered(index.storage(), policy, &budget, mode).unwrap(),
                (0..next).collect::<Vec<_>>(),
                "cap {cap}: {mode:?} serves every committed row after the rebuild"
            );
        }
    }
}

#[test]
fn a_partition_damaged_in_state_b_reaches_state_c_and_rebuilds_under_continuous_sealing() {
    let cap = 1;
    let (mut index, budget) = built(ivf_pages::Pages::default(), cap);
    in_state_b(&mut index, cap);
    let policy = index.policy();
    let mut store = index.into_storage();
    damage_codebook(&mut store);
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into())
        .expect("the writer opens over a damaged replacement codebook");
    let next = remap_under_continuous_sealing(&mut index, cap, LATE.end, "damaged in state B");
    assert_eq!(
        answered(index.storage(), policy, &budget, Mode::Exact).unwrap(),
        (0..next).collect::<Vec<_>>(),
        "every committed row is exact-visible in state C"
    );
    rebuild(&mut index, next, "damaged in state B");
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            answered(index.storage(), policy, &budget, mode).unwrap(),
            (0..next).collect::<Vec<_>>(),
            "{mode:?} serves every committed row after the rebuild"
        );
    }
}

#[test]
fn a_deletion_lives_in_the_journal_through_its_generations_retirement_and_reclamation() {
    let cap = 2;
    let deleted = 5;
    let (mut index, budget) = built(Hiding::default(), cap);
    let old_generation = index.storage().manifest(0).unwrap().unwrap().generation;
    let mut state = index.begin_build(0).unwrap();
    let mut source = (0..BASE).map(|row| (row, vector(row)));
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    // Deleted under the old generation, after the build captured the row.
    let writes = |index: &ClusteredIndex<Hiding>| index.storage().inner.writes.borrow().clone();
    let before = writes(&index);
    index
        .delete_row(
            0,
            RowIdentity {
                row: deleted,
                commit_lsn: 0,
            },
        )
        .expect("the genesis identity deletes the row");
    let written = writes(&index)
        .into_iter()
        .filter(|(kind, count)| before.get(kind).copied().unwrap_or(0) < *count)
        .map(|(kind, _)| kind)
        .collect::<Vec<_>>();
    assert!(
        written.iter().all(|kind| matches!(
            kind,
            PageKind::RowVersions | PageKind::RowCopies | PageKind::Visibility
        )),
        "a deletion writes only the row journal and visibility pages: {written:?}"
    );
    for row in LATE {
        commit(&mut index, row);
    }
    index.seal().unwrap();
    index.finish_ingestion(&mut state).unwrap();
    while !index.advance_build(&mut state).unwrap() {}
    for _ in 0..256 {
        let progress = index.advance_remap(0, REMAP_ENTRIES_PER_CALL).unwrap();
        index.advance_coalescing(0, COALESCE_PAGES).unwrap();
        if progress.complete {
            break;
        }
    }
    assert_eq!(
        inspect_catalog(index.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .legacy_runs,
        0,
        "fixture: the old generation has retired"
    );
    assert_ne!(
        index.storage().manifest(0).unwrap().unwrap().generation,
        old_generation
    );
    index.storage().hide_generation.set(Some(old_generation));

    let live = (0..LATE.end)
        .filter(|row| *row != deleted)
        .collect::<Vec<_>>();
    let policy = index.policy();
    let check = |index: &ClusteredIndex<Hiding>, when: &str| {
        assert_eq!(
            index.current_row_identity(0, deleted).unwrap(),
            None,
            "{when}: the journal still records the deletion"
        );
        for mode in [Mode::Exact, Mode::Indexed] {
            assert_eq!(
                answered(index.storage(), policy, &budget, mode).unwrap(),
                live,
                "{when}: {mode:?} never answers the deleted row"
            );
        }
    };
    check(&index, "after the old generation's pages are reclaimed");
    match index.commit_update(
        0,
        RowIdentity {
            row: deleted,
            commit_lsn: 0,
        },
        &vector(900)[..],
    ) {
        Err(Error::StaleRowVersion {
            expected: 0,
            current: None,
        }) => {}
        other => {
            panic!("the deleted row's old identity is stale with no current version: {other:?}")
        }
    }
    let store = index.into_storage();
    let index = ClusteredIndex::new(store, policy, training(), budget.clone().into()).unwrap();
    check(&index, "after reopen");
}

#[test]
fn catalog_inspection_reads_no_codebook_or_directory_page_and_reports_known_damage() {
    let cap = 2;
    let (mut index, _budget) = built(Hiding::default(), cap);
    commit(&mut index, BASE);
    index.seal().unwrap();
    let derived = [
        PageKind::Codebook,
        PageKind::Directory,
        PageKind::DirectoryPresence,
    ];
    let inspect_twice = |index: &ClusteredIndex<Hiding>, when: &str| {
        let mut states = Vec::new();
        for call in 0..2 {
            index.storage().reads.borrow_mut().clear();
            states.push(inspect_catalog(index.storage(), 0, cap).unwrap().unwrap());
            let reads = index.storage().reads.borrow();
            for kind in derived {
                assert_eq!(
                    reads.get(&kind).copied().unwrap_or(0),
                    0,
                    "{when}: inspection call {call} reads no {kind:?} page: {reads:?}"
                );
            }
        }
        assert_eq!(states[0], states[1], "{when}: repeated inspection agrees");
        states[0]
    };
    assert_eq!(
        inspect_twice(&index, "healthy").recovery_action,
        None,
        "a healthy partition shows no recovery action"
    );
    let policy = index.policy();
    let budget = Arc::new(ivf_pages::Budget::default());
    let mut store = index.into_storage();
    damage_codebook(&mut store);
    let mut index = ClusteredIndex::new(store, policy, training(), budget.into()).unwrap();
    commit(&mut index, BASE + 1);
    index
        .seal()
        .expect("a seal over the damaged codebook commits");
    assert_eq!(
        inspect_twice(&index, "after a seal met the damage").recovery_action,
        Some("rebuild"),
        "the damage the seal met is reported without re-reading the codebook"
    );
}

#[test]
fn a_stored_list_page_used_count_above_a_page_never_panics_a_seal() {
    let cap = 1;
    let (mut index, budget) = built(ivf_pages::Pages::default(), cap);
    // Three single-row seals leave an appendable delta run at a cap of one.
    let mut next = BASE;
    for _ in 0..3 {
        commit(&mut index, next);
        index.seal().unwrap();
        next += 1;
    }
    let policy = index.policy();
    let mut store = index.into_storage();
    let manifest = store.manifest(0).unwrap().unwrap();
    // Planted against the list-page format: one used-record count word per
    // list page id in the list's used-count page, records of 16 + dimensions
    // bytes.
    let per_page = (PAGE_BYTES / (16 + 2)) as u64;
    let mut planted = 0;
    for list in 0..manifest.lists {
        let key = manifest.key(PageKind::ListUsed, list, 0);
        let mut page = [0u8; PAGE_BYTES];
        if !store.read_admitted_page(key, &mut page).unwrap() {
            continue;
        }
        for word in page.as_chunks_mut::<8>().0 {
            let used = u64::from_le_bytes(*word);
            if used != 0 {
                assert!(used <= per_page, "fixture: a stored count fits its page");
                *word = (per_page + 1).to_le_bytes();
                planted += 1;
            }
        }
        let mut tx = store.begin().unwrap();
        tx.put_page(key, &page).unwrap();
        tx.commit().unwrap();
    }
    assert!(planted > 0, "fixture: list pages carry used counts");
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into()).unwrap();
    for _ in 0..4 {
        commit(&mut index, next);
        index
            .seal()
            .unwrap_or_else(|error| panic!("a seal over a damaged used count commits: {error:?}"));
        next += 1;
    }
    assert_eq!(
        answered(index.storage(), policy, &budget, Mode::Exact).unwrap(),
        (0..next).collect::<Vec<_>>(),
        "EXACT answers every committed row"
    );
    assert_eq!(
        inspect_catalog(index.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .recovery_action,
        Some("rebuild"),
        "inspection shows the rebuild action for the damaged list pages"
    );
}

#[test]
fn a_stored_remap_cursor_past_its_run_is_corruption_never_a_silent_stall() {
    let cap = 2;
    let (mut index, budget) = built(ivf_pages::Pages::default(), cap);
    in_state_b(&mut index, cap);
    index.advance_remap(0, 1).expect("one remap step");
    let policy = index.policy();
    let mut store = index.into_storage();
    let manifest = store.manifest(0).unwrap().unwrap();
    let key = manifest.key(PageKind::Catalog, 0, 0);
    let mut page = [0u8; PAGE_BYTES];
    assert!(store.read_admitted_page(key, &mut page).unwrap());
    // Planted against catalog format IVFCAT02, whose twenty-second word is
    // the remap cursor's count of examined entries.
    assert_eq!(
        &page[..8],
        b"IVFCAT02",
        "fixture: the catalog format the plant targets"
    );
    page[21 * 8..22 * 8].copy_from_slice(&1_000_000u64.to_le_bytes());
    let mut tx = store.begin().unwrap();
    tx.put_page(key, &page).unwrap();
    tx.commit().unwrap();
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into()).unwrap();
    match index.advance_remap(0, 1) {
        Err(Error::StoreCorrupted { .. }) => {}
        other => panic!("a remap over a cursor past its run is store corruption, got {other:?}"),
    }
    assert_eq!(
        inspect_catalog(index.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .recovery_action,
        Some("rebuild"),
        "inspection shows the rebuild action for the damaged remap cursor"
    );
    assert_eq!(
        answered(index.storage(), policy, &budget, Mode::Exact).unwrap(),
        (0..LATE.end).collect::<Vec<_>>(),
        "EXACT still answers every committed row"
    );
    // The advertised recovery is carried out: later maintenance calls may
    // still report the damage, but they reach state C, whether by remapping
    // the run again from its start or by retiring it from canonical rows.
    let legacy_runs = inspect_catalog(index.storage(), 0, cap)
        .unwrap()
        .unwrap()
        .legacy_runs;
    let bound = remap_call_bound(legacy_runs) + 2;
    let mut reached_c = false;
    for _ in 0..bound {
        match index.advance_remap(0, REMAP_ENTRIES_PER_CALL) {
            Ok(progress) if progress.complete => {
                reached_c = true;
                break;
            }
            Ok(_) | Err(Error::StoreCorrupted { .. }) => {}
            other => panic!("a recovering remap call proceeds or reports the damage: {other:?}"),
        }
        index.advance_coalescing(0, COALESCE_PAGES).unwrap();
    }
    assert!(
        reached_c
            && inspect_catalog(index.storage(), 0, cap)
                .unwrap()
                .unwrap()
                .legacy_runs
                == 0,
        "maintenance reaches state C within {bound} remap calls after the damage"
    );
    rebuild(&mut index, LATE.end, "after the damaged remap cursor");
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            answered(index.storage(), policy, &budget, mode).unwrap(),
            (0..LATE.end).collect::<Vec<_>>(),
            "{mode:?} serves every committed row after the rebuild"
        );
    }
}
