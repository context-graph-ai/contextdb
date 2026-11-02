//! While a replacement's remap still has legacy runs to retire, an update or
//! delete of a row whose old copy sits in an already-retired legacy run is
//! served exactly once with its current vector (or not at all). It clears
//! that old copy's visibility in the old generation's own pages, which stay
//! exactly the pages they were at publication, none released and none
//! allocated, until the last legacy run retires. A run reclaimed early, or
//! one of its pages recreated by the change, shows as the set changing. After that the old
//! generation's pages are reclaimed, every one of them, none is recreated by
//! later updates, and every row is still answered once, through reopen.

use super::support::{
    GenerationPage, IndexTestOptions, drive_until_serving, empty, generation_pages, int_ids, ivf,
    legacy_runs, maintenance_cycles, observe_search, old_copy_visible,
    open_with_index_test_options, params, retained_generations, retired_legacy_rows, serving,
    switch_state, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::BTreeSet;
use std::path::Path;

const DIMENSIONS: usize = 16;
const OPTIONS: IndexTestOptions = IndexTestOptions {
    write_buffer_cap_bytes: Some(1024),
    build_batch_rows: Some(8),
};
const BUILT: std::ops::Range<i64> = 0..200;
/// Committed one per maintenance cycle while the replacement builds:
/// several sealed runs that become the legacy runs at publication.
const DURING: std::ops::Range<i64> = 1_000..1_100;
const TABLE: &str = "retired_items";

fn open(path: &Path) -> Database {
    open_with_index_test_options(path, OPTIONS)
}

fn vector(id: i64) -> Vec<f32> {
    unit_vector(id as u64, DIMENSIONS)
}

fn write(db: &Database, sql: &str, id: i64, vector: Vec<f32>) {
    db.execute(
        sql,
        &params([
            ("id", Value::Int64(id)),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .unwrap_or_else(|error| panic!("{sql} for row {id}: {error:?}"));
}

fn insert(db: &Database, id: i64) {
    write(
        db,
        "INSERT INTO retired_items (id, embedding) VALUES ($id, $embedding)",
        id,
        vector(id),
    );
}

fn update(db: &Database, id: i64, vector: Vec<f32>) {
    write(
        db,
        "UPDATE retired_items SET embedding = $embedding WHERE id = $id",
        id,
        vector,
    );
}

fn answers(db: &Database, query: Vec<f32>, mode: &str, limit: usize) -> Vec<i64> {
    let (result, _) = observe_search(db, || {
        db.execute(
            &format!(
                "SELECT id FROM retired_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {limit}"
            ),
            &params([("query", Value::Vector(query))]),
        )
        .expect("vector search")
    });
    int_ids(&result)
}

/// The changed row is answered once, nearest to its new vector and never at
/// its old one, the deleted row never, and an untouched row still finds
/// itself.
fn assert_current(db: &Database, changed: i64, deleted: i64, untouched: i64, when: &str) {
    for mode in ["EXACT", "INDEXED"] {
        let near_new = answers(db, vector(9_000), mode, 20);
        assert_eq!(
            near_new.first(),
            Some(&changed),
            "{when}: {mode} finds the changed row at its new vector"
        );
        assert_eq!(
            near_new.iter().filter(|id| **id == changed).count(),
            1,
            "{when}: {mode} answers the changed row once"
        );
        let near_old = answers(db, vector(changed), mode, 20);
        assert!(
            near_old.first() != Some(&changed)
                && near_old.iter().filter(|id| **id == changed).count() <= 1,
            "{when}: {mode} never serves the changed row's old vector: {near_old:?}"
        );
        assert!(
            !answers(db, vector(deleted), mode, 20).contains(&deleted),
            "{when}: {mode} never answers the deleted row"
        );
        assert_eq!(
            answers(db, vector(untouched), mode, 1),
            vec![untouched],
            "{when}: {mode} still finds an untouched row"
        );
    }
}

fn pages(db: &Database, generation: u64) -> BTreeSet<GenerationPage> {
    generation_pages(db, TABLE, "embedding", &[], generation)
}

fn cycle(db: &Database) {
    db.run_maintenance_cycle().expect("one maintenance cycle");
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn a_change_to_a_row_in_a_retired_legacy_run_keeps_old_pages_until_state_c() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("retired.db");
    let db = open(&path);
    db.execute(
        &format!(
            "CREATE TABLE retired_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    for id in BUILT {
        insert(&db, id);
    }
    drive_until_serving(&db, (BUILT.end - BUILT.start) as usize, 8, 10);
    let old = serving(&db, TABLE, "embedding", &[])
        .expect("a generation serves")
        .generation;
    // Enough change to make a replacement due; while it builds, each commit
    // seals into the old generation's delta runs.
    for id in 200..240 {
        insert(&db, id);
    }
    let mut during = DURING;
    let mut published = false;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start) as usize + 40 + (DURING.end - DURING.start) as usize,
        8,
        10,
    ) * 4
    {
        cycle(&db);
        if serving(&db, TABLE, "embedding", &[]).is_some_and(|serving| serving.generation > old) {
            published = true;
            break;
        }
        if let Some(id) = during.next() {
            insert(&db, id);
        }
    }
    assert!(published, "fixture: the replacement publishes");
    assert!(
        switch_state(&db, TABLE, "embedding", &[]).legacy.is_some(),
        "fixture: the replacement serves in state B"
    );
    let at_publication = legacy_runs(&db, TABLE, "embedding", &[]);
    assert!(
        at_publication >= 2,
        "fixture: at least two legacy runs to retire, got {at_publication}"
    );
    let held = pages(&db, old);
    assert!(
        !held.is_empty(),
        "fixture: the old generation holds pages at publication"
    );

    // Retire legacy runs one maintenance cycle at a time until at least one
    // has retired and at least one remains; no old-generation page is
    // released or allocated meanwhile.
    for step in 0..maintenance_cycles(
        (BUILT.end - BUILT.start) as usize + 40 + (DURING.end - DURING.start) as usize,
        8,
        10,
    ) * 4
    {
        let remaining = legacy_runs(&db, TABLE, "embedding", &[]);
        if remaining < at_publication && remaining > 0 {
            break;
        }
        cycle(&db);
        assert_eq!(
            pages(&db, old),
            held,
            "retirement step {step}: the old generation's pages are exactly those at publication"
        );
    }
    let remaining = legacy_runs(&db, TABLE, "embedding", &[]);
    assert!(
        remaining > 0 && remaining < at_publication,
        "fixture: a legacy run has retired and another remains ({remaining} of {at_publication})"
    );

    // Rows the engine reports as having their old copies in retired legacy
    // runs.
    let retired = retired_legacy_rows(&db, TABLE, "embedding", &[])
        .into_iter()
        .map(|id| match id {
            Value::Int64(id) => id,
            other => panic!("a retired legacy row id is an integer: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(
        retired.len() >= 3,
        "fixture: retired legacy runs hold at least three rows, got {retired:?}"
    );
    let (changed, deleted, untouched) = (retired[0], retired[1], retired[2]);
    for id in [changed, deleted] {
        assert_eq!(
            old_copy_visible(&db, TABLE, "embedding", &[], old, Value::Int64(id)),
            Some(true),
            "fixture: row {id}'s old copy is still visible in the old generation's pages"
        );
    }

    update(&db, changed, vector(9_000));
    db.execute(
        &format!("DELETE FROM retired_items WHERE id = {deleted}"),
        &empty(),
    )
    .expect("delete a row whose old copy sits in a retired legacy run");
    for id in [changed, deleted] {
        assert_eq!(
            old_copy_visible(&db, TABLE, "embedding", &[], old, Value::Int64(id)),
            Some(false),
            "row {id}'s old copy is cleared in the old generation's own pages"
        );
    }
    assert_eq!(
        pages(&db, old),
        held,
        "the change clears visibility in the same old-generation pages: none released, none allocated"
    );
    assert_current(
        &db,
        changed,
        deleted,
        untouched,
        "with a legacy run remaining",
    );
    assert!(
        retained_generations(&db, TABLE, "embedding", &[]).contains(&old),
        "the old generation is retained while a legacy run remains"
    );

    let mut reached_c = false;
    for step in 0..maintenance_cycles(
        (BUILT.end - BUILT.start) as usize + 40 + (DURING.end - DURING.start) as usize,
        8,
        10,
    ) * 4
    {
        if switch_state(&db, TABLE, "embedding", &[]).legacy.is_none() {
            reached_c = true;
            break;
        }
        assert_eq!(
            pages(&db, old),
            held,
            "remap step {step}: the old generation's pages are unchanged until state C"
        );
        cycle(&db);
    }
    assert!(reached_c, "the remap reaches state C");
    assert_current(&db, changed, deleted, untouched, "in state C");

    let mut reclaimed = false;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start) as usize + 40 + (DURING.end - DURING.start) as usize,
        8,
        10,
    ) * 4
    {
        if !retained_generations(&db, TABLE, "embedding", &[]).contains(&old) {
            reclaimed = true;
            break;
        }
        cycle(&db);
    }
    assert!(reclaimed, "after state C the old generation is reclaimed");
    assert_eq!(
        pages(&db, old),
        BTreeSet::new(),
        "no page of any kind remains in the reclaimed generation"
    );
    update(&db, changed, vector(9_001));
    update(&db, changed, vector(9_000));
    assert_eq!(
        pages(&db, old),
        BTreeSet::new(),
        "later updates recreate no page of the reclaimed generation"
    );
    assert_current(&db, changed, deleted, untouched, "after reclamation");
    db.close().expect("close the store");
    drop(db);
    let db = open(&path);
    assert_eq!(
        pages(&db, old),
        BTreeSet::new(),
        "after reopen the generation stays reclaimed"
    );
    assert_current(&db, changed, deleted, untouched, "after reopen");
}
