//! While a replacement's remap still has legacy runs to retire, an update or
//! delete of a row whose old copy sat in an already-retired legacy run is
//! served exactly once with its current vector (or not at all), and no page
//! of the old generation is reclaimed until the last legacy run retires.
//! After that, the old generation's pages go, none is recreated by a later
//! update, and every row is still answered once, through reopen.

use super::support::{
    IndexTestOptions, PhysicalPlace, drive_until_serving, empty, int_ids, ivf, legacy_runs,
    observe_search, open_with_index_test_options, params, physical_pages, retained_generations,
    serving, switch_state, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use std::path::Path;

const DIMENSIONS: usize = 16;
const OPTIONS: IndexTestOptions = IndexTestOptions {
    write_buffer_cap_bytes: Some(1024),
    build_batch_rows: Some(8),
};
const BUILT: std::ops::Range<i64> = 0..200;
/// Committed one per transaction while the replacement builds: several
/// sealed runs that become the legacy runs at publication.
const DURING: std::ops::Range<i64> = 1_000..1_060;
const CHANGED: i64 = DURING.start;
const DELETED: i64 = DURING.start + 1;

fn open(path: &Path) -> Database {
    open_with_index_test_options(path, OPTIONS)
}

fn insert(db: &Database, id: i64, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO retired_items (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Int64(id)),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .unwrap_or_else(|error| panic!("row {id} commits: {error:?}"));
}

fn update(db: &Database, id: i64, vector: Vec<f32>) {
    db.execute(
        "UPDATE retired_items SET embedding = $embedding WHERE id = $id",
        &params([
            ("id", Value::Int64(id)),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .unwrap_or_else(|error| panic!("row {id} updates: {error:?}"));
}

fn vector(id: i64) -> Vec<f32> {
    unit_vector(id as u64, DIMENSIONS)
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

/// The changed row is answered once, nearest to its new vector, the deleted
/// row never, and an untouched row from the same runs still finds itself.
fn assert_current(db: &Database, when: &str) {
    for mode in ["EXACT", "INDEXED"] {
        let near_new = answers(db, vector(9_000), mode, 20);
        assert_eq!(
            near_new.first(),
            Some(&CHANGED),
            "{when}: {mode} finds the changed row at its new vector"
        );
        assert_eq!(
            near_new.iter().filter(|id| **id == CHANGED).count(),
            1,
            "{when}: {mode} answers the changed row once"
        );
        let near_old = answers(db, vector(CHANGED), mode, 20);
        assert!(
            near_old.first() != Some(&CHANGED)
                && near_old.iter().filter(|id| **id == CHANGED).count() <= 1,
            "{when}: {mode} never serves the changed row's old vector: {near_old:?}"
        );
        assert!(
            !answers(db, vector(DELETED), mode, 20).contains(&DELETED),
            "{when}: {mode} never answers the deleted row"
        );
        assert_eq!(
            answers(db, vector(DURING.end - 1), mode, 1),
            vec![DURING.end - 1],
            "{when}: {mode} still finds an untouched row"
        );
    }
}

fn old_generation_pages(db: &Database, old: u64) -> usize {
    physical_pages(db, "retired_items", "embedding")
        .iter()
        .filter(|page| {
            matches!(
                page.place,
                PhysicalPlace::ListPage { generation } | PhysicalPlace::SealedRun { generation }
                    if generation == old
            )
        })
        .count()
}

#[test]
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
        insert(&db, id, vector(id));
    }
    drive_until_serving(&db);
    let old = serving(&db, "retired_items", "embedding", &[])
        .expect("a generation serves")
        .generation;
    // Enough change to make a replacement due; while it builds, each commit
    // seals into the old generation's delta runs.
    for id in 200..240 {
        insert(&db, id, vector(id));
    }
    let mut during = DURING;
    let mut published = false;
    for _ in 0..256 {
        db.run_maintenance_cycle().expect("one maintenance cycle");
        if serving(&db, "retired_items", "embedding", &[])
            .is_some_and(|serving| serving.generation > old)
        {
            published = true;
            break;
        }
        if let Some(id) = during.next() {
            insert(&db, id, vector(id));
        }
    }
    assert!(published, "fixture: the replacement publishes");
    assert!(
        during.start > DELETED + 1,
        "fixture: the changed and deleted rows were committed while the replacement built"
    );
    assert!(
        switch_state(&db, "retired_items", "embedding", &[])
            .legacy
            .is_some(),
        "fixture: the replacement serves in state B"
    );
    let at_publication = legacy_runs(&db, "retired_items", "embedding", &[]);
    assert!(
        at_publication >= 2,
        "fixture: at least two legacy runs to retire, got {at_publication}"
    );

    // Retire legacy runs one maintenance cycle at a time until at least one
    // has retired and at least one remains.
    for _ in 0..256 {
        let remaining = legacy_runs(&db, "retired_items", "embedding", &[]);
        if remaining < at_publication && remaining > 0 {
            break;
        }
        db.run_maintenance_cycle().expect("one maintenance cycle");
    }
    let remaining = legacy_runs(&db, "retired_items", "embedding", &[]);
    assert!(
        remaining > 0 && remaining < at_publication,
        "fixture: a legacy run has retired and another remains ({remaining} of {at_publication})"
    );

    // The changed and deleted rows were the first committed during the
    // build, so their old copies sat in the oldest legacy run.
    update(&db, CHANGED, vector(9_000));
    db.execute(
        &format!("DELETE FROM retired_items WHERE id = {DELETED}"),
        &empty(),
    )
    .expect("delete a row whose old copy sat in a retired legacy run");
    assert_current(&db, "with a legacy run remaining");
    assert!(
        retained_generations(&db, "retired_items", "embedding", &[]).contains(&old),
        "the old generation's pages are retained while a legacy run remains"
    );
    assert!(
        old_generation_pages(&db, old) > 0,
        "no old-generation page is reclaimed before state C"
    );

    drive_until_serving(&db);
    assert!(
        switch_state(&db, "retired_items", "embedding", &[])
            .legacy
            .is_none(),
        "the remap reaches state C"
    );
    assert_current(&db, "in state C");
    for _ in 0..8 {
        db.run_maintenance_cycle().expect("reclamation cycle");
    }
    assert!(
        !retained_generations(&db, "retired_items", "embedding", &[]).contains(&old),
        "after state C the old generation is reclaimed"
    );
    update(&db, CHANGED, vector(9_001));
    update(&db, CHANGED, vector(9_000));
    assert_eq!(
        old_generation_pages(&db, old),
        0,
        "a later update recreates no page of the reclaimed generation"
    );
    assert_current(&db, "after reclamation");
    db.close().expect("close the store");
    drop(db);
    let db = open(&path);
    assert_current(&db, "after reopen");
}
