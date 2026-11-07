#![cfg(feature = "test-seams")]
#![expect(
    clippy::result_large_err,
    reason = "The shared engine Error keeps inline refusal diagnostics; the guards return it unchanged."
)]
//! Damaged derived index state never stops writes and never answers wrongly,
//! and a ready replacement is never starved:
//! - a codebook page that fails its digest, or a missing run-directory page,
//!   refuses INDEXED with store corruption naming that page, while EXACT
//!   keeps answering every committed row, writes keep committing and sealing,
//!   the writer reopens, and a rebuild from the committed rows publishes and
//!   serves INDEXED again; a missing directory page is never an empty list;
//! - a replacement whose new codebook is damaged while the legacy remap runs
//!   finishes the remap with every row exact-visible, a rebuild is refused
//!   until the remap retires the legacy catalog (three catalog roots at
//!   most), and then starts and publishes;
//! - a replacement ready while a coalesce is in progress publishes within the
//!   steps of that coalesce, though a seal lands before every maintenance
//!   call, at a run cap of one and of two;
//! - a stored run count or list page id at the counter's maximum is store
//!   corruption, never a panic or a wrap.
#[path = "support/hiding_pages.rs"]
mod hiding_pages;
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::{Error, Result};
use contextdb_vector::ivf::storage::{
    BuildPhase, PAGE_BYTES, PageKind, PageStore, ReadPages, WritePages,
};
use contextdb_vector::ivf::{
    ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, Vector, inspect_catalog, search,
};
use hiding_pages::Hiding;
use std::sync::Arc;

const BASE: u64 = 16;
/// Write-buffer records per seal: 192 bytes over 48-byte two-dimension records.
const BUFFERED: u64 = 4;

fn training() -> TrainingPolicy {
    TrainingPolicy {
        iterations: 2,
        reservoir_samples: 128,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    }
}

fn policy(cap: u64, coalesce_pages: usize) -> Policy {
    Policy::checked(2, 4, 4, 256, 192, true)
        .unwrap()
        .with_max_delta_runs(cap)
        .unwrap()
        .with_coalesce_pages_per_step(coalesce_pages)
        .unwrap()
}

fn vector(row: u64) -> [f32; 2] {
    let x = row as f32 * 0.031;
    [x.cos(), x.sin()]
}

fn built<S: PageStore>(store: S, policy: Policy) -> (ClusteredIndex<S>, Arc<ivf_pages::Budget>) {
    let budget = Arc::new(ivf_pages::Budget::default());
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into()).unwrap();
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

fn commit<S: PageStore>(index: &mut ClusteredIndex<S>, rows: std::ops::Range<u64>) {
    for row in rows {
        index
            .commit_insert(Vector {
                partition: 0,
                row,
                values: &vector(row),
            })
            .unwrap_or_else(|error| panic!("committing row {row} succeeds: {error:?}"));
    }
}

fn rebuild<S: PageStore>(index: &mut ClusteredIndex<S>, rows: u64) {
    let mut state = index.begin_build(0).expect("a rebuild starts");
    let mut source = (0..rows).map(|row| (row, vector(row)));
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    index.finish_ingestion(&mut state).unwrap();
    for _ in 0..256 {
        if index.advance_build(&mut state).unwrap() {
            break;
        }
        index.advance_coalescing(0, 32).unwrap();
    }
    assert_eq!(state.phase, BuildPhase::Ready, "the rebuild publishes");
}

fn assert_corruption_naming(result: Result<Vec<u64>>, page: &str, when: &str) {
    match result {
        Err(error @ Error::StoreCorrupted { .. }) => assert!(
            format!("{error:?}").to_lowercase().contains(page),
            "{when}: INDEXED is refused as store corruption naming the {page} page: {error:?}"
        ),
        other => panic!("{when}: INDEXED is refused as store corruption, got {other:?}"),
    }
}

/// Writes, exact answers, reopen and a rebuild survive damaged derived state.
fn assert_damage_never_stops_writes<S: PageStore>(
    mut index: ClusteredIndex<S>,
    budget: &Arc<ivf_pages::Budget>,
    page: &str,
    reopen: impl FnOnce(ClusteredIndex<S>) -> ClusteredIndex<S>,
    committed: u64,
) {
    let policy = index.policy();
    assert_corruption_naming(
        answered(index.storage(), policy, budget, Mode::Indexed),
        page,
        "after the damage",
    );
    assert_eq!(
        answered(index.storage(), policy, budget, Mode::Exact).unwrap(),
        (0..committed).collect::<Vec<_>>(),
        "EXACT answers every committed row"
    );
    let total = committed + 3 * BUFFERED;
    commit(&mut index, committed..total);
    index.seal().expect("sealing continues");
    assert_eq!(
        answered(index.storage(), policy, budget, Mode::Exact).unwrap(),
        (0..total).collect::<Vec<_>>(),
        "rows written after the damage are exact-visible"
    );
    let mut index = reopen(index);
    assert_eq!(
        answered(index.storage(), policy, budget, Mode::Exact).unwrap(),
        (0..total).collect::<Vec<_>>(),
        "the writer reopens and EXACT still answers every row"
    );
    rebuild(&mut index, total);
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            answered(index.storage(), policy, budget, mode).unwrap(),
            (0..total).collect::<Vec<_>>(),
            "{mode:?} serves every row after the rebuild"
        );
    }
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
fn a_codebook_that_fails_its_digest_refuses_indexed_and_never_stops_writes() {
    let (index, budget) = built(ivf_pages::Pages::default(), policy(2, 32));
    let policy = index.policy();
    let mut store = index.into_storage();
    damage_codebook(&mut store);
    let index = ClusteredIndex::new(store, policy, training(), budget.clone().into())
        .expect("the writer opens over a damaged codebook");
    let reopen_budget = budget.clone();
    assert_damage_never_stops_writes(
        index,
        &budget,
        "codebook",
        move |index| {
            ClusteredIndex::new(
                index.into_storage(),
                policy,
                training(),
                reopen_budget.into(),
            )
            .expect("the writer reopens over a damaged codebook")
        },
        BASE,
    );
}

#[test]
fn a_missing_run_directory_page_is_corruption_never_an_empty_list_and_never_stops_writes() {
    let (mut index, budget) = built(Hiding::default(), policy(2, 32));
    let committed = BASE + BUFFERED;
    commit(&mut index, BASE..committed);
    index.seal().unwrap();
    let state = inspect_catalog(index.storage(), 0, 2).unwrap().unwrap();
    assert!(
        state.delta_runs > 0,
        "fixture: a sealed delta run exists: {state:?}"
    );
    let mut run_view = index.storage().manifest(0).unwrap().unwrap();
    run_view.run = 1;
    let key = run_view.key(PageKind::Directory, 0, 0);
    let mut page = [0u8; PAGE_BYTES];
    assert!(
        index.storage().read_admitted_page(key, &mut page).unwrap(),
        "fixture: the first delta run's directory page exists"
    );
    index.storage().hide_key.set(Some(key));
    let policy = index.policy();
    let reopen_budget = budget.clone();
    assert_damage_never_stops_writes(
        index,
        &budget,
        "directory",
        move |index| {
            let store = index.into_storage();
            ClusteredIndex::new(store, policy, training(), reopen_budget.into())
                .expect("the writer reopens with the directory page missing")
        },
        committed,
    );
}

#[test]
fn a_damaged_replacement_codebook_finishes_its_remap_exact_and_then_rebuilds() {
    let (mut index, budget) = built(ivf_pages::Pages::default(), policy(2, 32));
    let policy = index.policy();
    let mut state = index.begin_build(0).unwrap();
    let mut source = (0..BASE).map(|row| (row, vector(row)));
    while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
    let committed = BASE + 2 * BUFFERED;
    commit(&mut index, BASE..committed);
    index.seal().unwrap();
    index.finish_ingestion(&mut state).unwrap();
    while !index.advance_build(&mut state).unwrap() {}
    assert!(
        inspect_catalog(index.storage(), 0, 2)
            .unwrap()
            .unwrap()
            .legacy_runs
            > 0,
        "fixture: the replacement serves in state B"
    );
    let mut store = index.into_storage();
    damage_codebook(&mut store);
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into())
        .expect("the writer opens over a damaged replacement codebook");
    let all = (0..committed).collect::<Vec<_>>();
    let mut complete = false;
    for step in 0..256 {
        assert_eq!(
            answered(index.storage(), policy, &budget, Mode::Exact).unwrap(),
            all,
            "remap step {step}: every row is exact-visible"
        );
        assert_corruption_naming(
            answered(index.storage(), policy, &budget, Mode::Indexed),
            "codebook",
            &format!("remap step {step}"),
        );
        assert!(
            matches!(index.begin_build(0), Err(Error::InvalidStateTransition(_))),
            "remap step {step}: a rebuild waits for the legacy catalog to retire"
        );
        let progress = index.advance_remap(0, 1).unwrap_or_else(|error| {
            panic!("remap step {step} proceeds without a codebook: {error:?}")
        });
        index.advance_coalescing(0, 32).unwrap();
        if progress.complete {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "the remap completes as unclustered canonical runs"
    );
    assert_eq!(
        inspect_catalog(index.storage(), 0, 2)
            .unwrap()
            .unwrap()
            .legacy_runs,
        0,
        "state C: the legacy catalog retired"
    );
    assert_eq!(
        answered(index.storage(), policy, &budget, Mode::Exact).unwrap(),
        all,
        "every row is exact-visible after the remap"
    );
    rebuild(&mut index, committed);
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            answered(index.storage(), policy, &budget, mode).unwrap(),
            all,
            "{mode:?} serves every row after the rebuild"
        );
    }
}

#[test]
fn a_ready_replacement_publishes_within_the_coalesce_in_progress_under_continuous_sealing() {
    // The smallest admitted coalesce allowance joins one list per call, so a
    // coalesce over the fixture's lists takes one call per list.
    const ONE_LIST_PER_STEP: usize = 26;
    for cap in [1, 2] {
        let (mut index, budget) =
            built(ivf_pages::Pages::default(), policy(cap, ONE_LIST_PER_STEP));
        let lists = index.storage().manifest(0).unwrap().unwrap().active_lists as usize;
        let mut next = BASE;
        let mut seal_one = |index: &mut ClusteredIndex<ivf_pages::Pages>| {
            commit(index, next..next + 1);
            index.seal().unwrap();
            next += 1;
        };
        let mut state = index.begin_build(0).unwrap();
        let mut source = (0..BASE).map(|row| (row, vector(row)));
        while index.ingest_batch(&mut state, &mut source).unwrap() > 0 {}
        index.finish_ingestion(&mut state).unwrap();
        let mut published = false;
        let mut coalescing_at_readiness = false;
        let mut calls_after_ready = None::<usize>;
        for _ in 0..256 {
            seal_one(&mut index);
            let coalescing = inspect_catalog(index.storage(), 0, cap)
                .unwrap()
                .unwrap()
                .coalescing;
            let was_ready = matches!(state.phase, BuildPhase::Publish | BuildPhase::Ready);
            published = index.advance_build(&mut state).unwrap();
            let ready = matches!(state.phase, BuildPhase::Publish | BuildPhase::Ready);
            if ready && !was_ready {
                coalescing_at_readiness = coalescing;
                calls_after_ready = Some(0);
            }
            if let Some(calls) = calls_after_ready.as_mut() {
                *calls += 1;
            }
            if published {
                break;
            }
            if calls_after_ready.is_some_and(|calls| calls > lists + 1) {
                break;
            }
        }
        assert!(
            coalescing_at_readiness,
            "cap {cap}, fixture: a coalesce is in progress when the replacement becomes ready"
        );
        assert!(
            published && calls_after_ready.is_some_and(|calls| calls <= lists + 1),
            "cap {cap}: the ready replacement publishes within the {lists} steps of the \
             coalesce in progress plus its own call, though a seal lands before every call; \
             calls after readiness: {calls_after_ready:?}, published: {published}"
        );
        let policy = index.policy();
        for mode in [Mode::Exact, Mode::Indexed] {
            assert_eq!(
                answered(index.storage(), policy, &budget, mode).unwrap(),
                (0..next).collect::<Vec<_>>(),
                "cap {cap}: {mode:?} serves every committed row after publication"
            );
        }
    }
}

fn plant_word(
    store: &mut ivf_pages::Pages,
    key: contextdb_vector::ivf::storage::PageKey,
    at: usize,
    value: u64,
) {
    let mut page = [0u8; PAGE_BYTES];
    assert!(
        store.read_admitted_page(key, &mut page).unwrap(),
        "fixture: page {key:?} exists"
    );
    page[at..at + 8].copy_from_slice(&value.to_le_bytes());
    let mut tx = store.begin().unwrap();
    tx.put_page(key, &page).unwrap();
    tx.commit().unwrap();
}

fn assert_corrupt_not_panic(
    store: &ivf_pages::Pages,
    policy: Policy,
    budget: &Arc<ivf_pages::Budget>,
    when: &str,
) {
    match answered(store, policy, budget, Mode::Indexed) {
        Err(Error::StoreCorrupted { .. }) => {}
        other => panic!("{when}: INDEXED is store corruption, got {other:?}"),
    }
    match answered(store, policy, budget, Mode::Exact) {
        Ok(_) | Err(Error::StoreCorrupted { .. }) => {}
        other => panic!("{when}: EXACT answers or reports store corruption, got {other:?}"),
    }
}

#[test]
fn a_stored_run_count_at_its_maximum_is_store_corruption_not_a_panic() {
    let (mut index, budget) = built(ivf_pages::Pages::default(), policy(2, 32));
    commit(&mut index, BASE..BASE + BUFFERED);
    index.seal().unwrap();
    let policy = index.policy();
    let mut store = index.into_storage();
    let manifest = store.manifest(0).unwrap().unwrap();
    let key = manifest.key(PageKind::Catalog, 0, 0);
    let mut page = [0u8; PAGE_BYTES];
    assert!(store.read_admitted_page(key, &mut page).unwrap());
    // Planted against catalog format IVFCAT02, whose fifth word is the
    // new-delta run count.
    assert_eq!(
        &page[..8],
        b"IVFCAT02",
        "fixture: the catalog format the plant targets"
    );
    assert_eq!(
        u64::from_le_bytes(page[32..40].try_into().unwrap()),
        1,
        "fixture: one delta run"
    );
    plant_word(&mut store, key, 32, u64::MAX);
    assert_corrupt_not_panic(&store, policy, &budget, "a run count at u64::MAX");
}

#[test]
fn a_stored_list_page_id_at_its_maximum_is_store_corruption_not_a_panic() {
    let (index, budget) = built(ivf_pages::Pages::default(), policy(2, 32));
    let policy = index.policy();
    let mut store = index.into_storage();
    let manifest = store.manifest(0).unwrap().unwrap();
    let directory = manifest.key(PageKind::Directory, 0, 0);
    let mut page = [0u8; PAGE_BYTES];
    assert!(store.read_admitted_page(directory, &mut page).unwrap());
    // Planted against the directory format: one 24-byte (count, head, tail)
    // entry per list.
    let word = |page: &[u8; PAGE_BYTES], at: usize| {
        u64::from_le_bytes(page[at..at + 8].try_into().unwrap())
    };
    let list = (0..manifest.active_lists)
        .find(|list| word(&page, *list as usize * 24) > 0)
        .expect("fixture: a list holds entries");
    let at = list as usize * 24;
    let head = word(&page, at + 8);
    // The planted id addresses a real copy of the list's head page, so the
    // read succeeds and the next step derives offsets from the stored id.
    let mut payload = [0u8; PAGE_BYTES];
    assert!(
        store
            .read_admitted_page(manifest.key(PageKind::Lists, list, head - 1), &mut payload)
            .unwrap()
    );
    let mut tx = store.begin().unwrap();
    tx.put_page(manifest.key(PageKind::Lists, list, u64::MAX - 1), &payload)
        .unwrap();
    tx.commit().unwrap();
    plant_word(&mut store, directory, at + 8, u64::MAX);
    plant_word(&mut store, directory, at + 16, u64::MAX);
    assert_corrupt_not_panic(&store, policy, &budget, "a list page id at u64::MAX");
}
