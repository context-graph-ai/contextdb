#![cfg(feature = "test-seams")]
#![expect(
    clippy::result_large_err,
    reason = "The shared engine Error keeps inline refusal diagnostics; the guards return it unchanged."
)]
//! Damage met during a remap is recorded once, reported, and recovered from:
//! - a stored used count above what a page holds, on the tail page of the run
//!   a remap is preparing, is store corruption naming the used-count page
//!   with the rebuild action, never a remap that fails the same way forever;
//!   the replacement still reaches state C and a rebuild serves every row;
//! - the first corruption verdict a partition records is kept: a later
//!   damaged remap cursor does not replace the recorded codebook verdict;
//! - once a remap is joining its prepared run, a stored examined-entries
//!   count that no longer matches its source run is store corruption with
//!   the rebuild action, never re-read as a legacy slot, and the replacement
//!   still reaches state C and rebuilds.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::{Error, Result};
use contextdb_vector::ivf::storage::{
    BuildPhase, PAGE_BYTES, PageKey, PageKind, PageStore, ReadPages, WritePages,
};
use contextdb_vector::ivf::{
    ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, Vector, inspect_catalog, search,
};
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

/// Stored list-page used counts: one word per list page id in the list's
/// used-count pages, records of 16 + dimensions bytes.
const PER_PAGE: u64 = (PAGE_BYTES / (16 + 2)) as u64;

fn read_page(store: &impl ReadPages, key: PageKey) -> [u8; PAGE_BYTES] {
    let mut page = [0u8; PAGE_BYTES];
    assert!(
        store.read_admitted_page(key, &mut page).unwrap(),
        "fixture: page {key:?} exists"
    );
    page
}

fn write_page(store: &mut impl PageStore, key: PageKey, page: &[u8; PAGE_BYTES]) {
    let mut tx = store.begin().unwrap();
    tx.put_page(key, page).unwrap();
    tx.commit().unwrap();
}

fn word(page: &[u8; PAGE_BYTES], index: usize) -> u64 {
    u64::from_le_bytes(page[index * 8..index * 8 + 8].try_into().unwrap())
}

/// The catalog page, checked against format IVFCAT02, the format the
/// cursor plants target: word 8 is the join target, word 21 the examined
/// entries of the remap cursor.
fn catalog_page(store: &impl ReadPages) -> (PageKey, [u8; PAGE_BYTES]) {
    let manifest = store.manifest(0).unwrap().unwrap();
    let key = manifest.key(PageKind::Catalog, 0, 0);
    let page = read_page(store, key);
    assert_eq!(
        &page[..8],
        b"IVFCAT02",
        "fixture: the catalog format the plant targets"
    );
    (key, page)
}

fn plant_examined(store: &mut impl PageStore, examined: u64) {
    let (key, mut page) = catalog_page(store);
    page[21 * 8..22 * 8].copy_from_slice(&examined.to_le_bytes());
    write_page(store, key, &page);
}

/// The persisted verdict: `None` unless a writer path has recorded it.
fn recorded(store: &impl ReadPages, cap: u64) -> Option<PageKey> {
    inspect_catalog(store, 0, cap)
        .unwrap()
        .unwrap()
        .unserviceable
        .filter(|_| persisted(store, cap))
}

/// Drives remap and coalescing calls, which may report the damage or
/// proceed, until state C; then a rebuild must be accepted and both modes
/// serve every committed row.
fn recover_to_state_c_and_rebuild(
    index: &mut ClusteredIndex<ivf_pages::Pages>,
    budget: &Arc<ivf_pages::Budget>,
    cap: u64,
    rows: u64,
    when: &str,
) {
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
            other => {
                panic!("{when}: a recovering remap call proceeds or reports the damage: {other:?}")
            }
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
        "{when}: maintenance reaches state C within {bound} remap calls"
    );
    rebuild(index, rows, when);
    let policy = index.policy();
    for mode in [Mode::Exact, Mode::Indexed] {
        assert_eq!(
            answered(index.storage(), policy, budget, mode).unwrap(),
            (0..rows).collect::<Vec<_>>(),
            "{when}: {mode:?} serves every committed row after the rebuild"
        );
    }
}

fn persisted(store: &impl ReadPages, cap: u64) -> bool {
    inspect_catalog(store, 0, cap)
        .unwrap()
        .unwrap()
        .verdict_recorded
}

#[test]
fn a_damaged_used_count_on_the_prepared_runs_tail_is_reported_and_recovered() {
    let cap = 2;
    let (mut index, budget) = built(ivf_pages::Pages::default(), cap);
    in_state_b(&mut index, cap);
    let mut prepared_any = false;
    for _ in 0..remap_call_bound(LEGACY_ENTRIES) {
        let progress = index.advance_remap(0, 1).unwrap();
        assert!(!progress.complete, "fixture: the remap is still preparing");
        if progress.entries_remapped > 0 {
            prepared_any = true;
            break;
        }
    }
    assert!(prepared_any, "fixture: the remap has prepared an entry");
    let state = inspect_catalog(index.storage(), 0, cap).unwrap().unwrap();
    assert!(
        state.remap_output_run != 0,
        "fixture: a prepared run is open: {state:?}"
    );
    let policy = index.policy();
    let mut store = index.into_storage();
    let mut view = store.manifest(0).unwrap().unwrap();
    view.run = state.remap_output_run;
    // Planted against the directory format: one 24-byte (count, head, tail)
    // entry per list; every list the prepared run holds gets its tail page's
    // used count above what a page holds.
    let directory = read_page(&store, view.key(PageKind::Directory, 0, 0));
    let mut planted = 0;
    for list in 0..view.active_lists {
        let at = list as usize * 3;
        if word(&directory, at) == 0 {
            continue;
        }
        let tail = word(&directory, at + 2);
        let offset = (tail - 1) * 8;
        let key = view.key(PageKind::ListUsed, list, offset / PAGE_BYTES as u64);
        let mut used = read_page(&store, key);
        let index = (offset % PAGE_BYTES as u64) as usize / 8;
        let stored = word(&used, index);
        assert!(
            stored > 0 && stored <= PER_PAGE,
            "fixture: the tail's count fits its page"
        );
        used[index * 8..index * 8 + 8].copy_from_slice(&(PER_PAGE + 1).to_le_bytes());
        write_page(&mut store, key, &used);
        planted += 1;
    }
    assert!(planted > 0, "fixture: the prepared run holds list pages");
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into()).unwrap();
    match index.advance_remap(0, REMAP_ENTRIES_PER_CALL) {
        Err(error @ Error::StoreCorrupted { .. }) => assert!(
            format!("{error:?}").contains("ListUsed"),
            "the refusal names the used-count page: {error:?}"
        ),
        Ok(_) => {}
        other => panic!("the next remap call reports the damage or proceeds, got {other:?}"),
    }
    let state = inspect_catalog(index.storage(), 0, cap).unwrap().unwrap();
    assert_eq!(
        (
            state.recovery_action,
            state.unserviceable.map(|key| key.kind),
            state.verdict_recorded
        ),
        (Some("rebuild"), Some(PageKind::ListUsed), true),
        "the damage is persisted, naming the used-count page: {state:?}"
    );
    recover_to_state_c_and_rebuild(&mut index, &budget, cap, LATE.end, "prepared-run damage");
}

#[test]
fn the_first_recorded_verdict_is_never_replaced_by_a_later_one() {
    let cap = 2;
    let (mut index, budget) = built(ivf_pages::Pages::default(), cap);
    in_state_b(&mut index, cap);
    let policy = index.policy();
    let mut store = index.into_storage();
    damage_codebook(&mut store);
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into()).unwrap();
    commit(&mut index, LATE.end);
    index
        .seal()
        .expect("a seal over the damaged codebook commits");
    let first = recorded(index.storage(), cap);
    assert_eq!(
        first.map(|key| key.kind),
        Some(PageKind::Codebook),
        "fixture: the seal persisted the codebook verdict"
    );
    let mut store = index.into_storage();
    plant_examined(&mut store, 1_000_000);
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into()).unwrap();
    assert!(
        matches!(index.advance_remap(0, 1), Err(Error::StoreCorrupted { .. })),
        "the damaged cursor is reported"
    );
    assert_eq!(
        recorded(index.storage(), cap),
        first,
        "the first persisted verdict is kept, not replaced by the cursor's"
    );
    assert_eq!(
        inspect_catalog(index.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .recovery_action,
        Some("rebuild")
    );
}

#[test]
fn a_mismatched_cursor_while_joining_is_corruption_never_a_legacy_slot() {
    let cap = 1;
    let (mut index, budget) = built(ivf_pages::Pages::default(), cap);
    in_state_b(&mut index, cap);
    let mut next = LATE.end;
    let mut joining = false;
    for _ in 0..remap_call_bound(LEGACY_ENTRIES) * 2 {
        if word(&catalog_page(index.storage()).1, 8) != 0 {
            joining = true;
            break;
        }
        commit(&mut index, next);
        index.seal().unwrap();
        next += 1;
        let progress = index.advance_remap(0, REMAP_ENTRIES_PER_CALL).unwrap();
        assert!(
            !progress.complete,
            "fixture: the remap joins before it completes"
        );
    }
    assert!(
        joining,
        "fixture: the remap is joining its prepared run under continuous sealing"
    );
    let policy = index.policy();
    let mut store = index.into_storage();
    // The source pass is complete while joining; an examined count below the
    // source run's entries no longer describes any cursor.
    plant_examined(&mut store, 0);
    let mut index = ClusteredIndex::new(store, policy, training(), budget.clone().into()).unwrap();
    match index.advance_remap(0, REMAP_ENTRIES_PER_CALL) {
        Err(Error::StoreCorrupted { .. }) => {}
        other => {
            panic!("a cursor that no longer matches its joining run is corruption, got {other:?}")
        }
    }
    assert_eq!(
        inspect_catalog(index.storage(), 0, cap)
            .unwrap()
            .unwrap()
            .recovery_action,
        Some("rebuild"),
        "the damaged join cursor shows the rebuild action"
    );
    assert!(
        persisted(index.storage(), cap),
        "the damaged join cursor's verdict is persisted"
    );
    recover_to_state_c_and_rebuild(&mut index, &budget, cap, next, "join-cursor damage");
}
