//! A deleted vector never appears in an answer on a snapshot that cannot see
//! it, before or after maintenance rewrites its list, and deleted entries
//! never crowd live neighbours out of a full answer. PURGE goes further: when
//! it returns, the erased vectors' bytes are gone from every list page,
//! sealed run, write buffer, retained generation and prepared replacement,
//! from an export taken afterwards, and from the store after reopen. A PURGE
//! issued while a multi-cycle build is in flight abandons that build and
//! leaves none of the erased vector in its training reservoir, accumulators or
//! unpublished list runs.

use super::support::{
    PhysicalPage, build_progress, contains_bytes, cosine_distance, count, drive_until_serving,
    empty, int_ids, ivf, little_endian_bytes, maintenance_cycles, observe_search,
    open_caller_driven, open_with_build_batch_rows, params, physical_pages, quantized_record,
    serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;

const DIMENSIONS: usize = 8;
const ROWS: i64 = 60;
/// Wide enough that an erased vector's bytes cannot occur by accident.
const PURGE_DIMENSIONS: usize = 32;

fn insert(db: &Database, table: &str, id: i64, vector: Vec<f32>) {
    db.execute(
        &format!("INSERT INTO {table} (id, embedding) VALUES ($id, $embedding)"),
        &params([
            ("id", Value::Int64(id)),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .expect("commit one vector");
}

#[test]
fn a_deleted_vector_never_appears_on_a_snapshot_that_cannot_see_it() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("deleted.db"));
    db.execute(
        &format!(
            "CREATE TABLE deleted_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {})",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    for id in 0..ROWS {
        insert(&db, "deleted_items", id, unit_vector(id as u64, DIMENSIONS));
    }
    drive_until_serving(&db, ROWS as usize, 1, 10);

    let query = unit_vector(7, DIMENSIONS);
    let mut by_distance = (0..ROWS)
        .map(|id| {
            (
                cosine_distance(&query, &unit_vector(id as u64, DIMENSIONS)),
                id,
            )
        })
        .collect::<Vec<_>>();
    by_distance.sort_by(|left, right| left.partial_cmp(right).unwrap());
    // The ten rows nearest the query, the query's own row first.
    let deleted = by_distance[..10]
        .iter()
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();
    let live_answer = by_distance[10..15]
        .iter()
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();
    assert_eq!(deleted[0], 7);

    let indexed =
        "SELECT id FROM deleted_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";
    let exact =
        "SELECT id FROM deleted_items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 5";
    let bound = params([("query", Value::Vector(query.clone()))]);

    let before = db.snapshot();
    let pin = db
        .pin_snapshot(before)
        .expect("pin a snapshot that sees the rows");
    for id in &deleted {
        db.execute(
            "DELETE FROM deleted_items WHERE id = $id",
            &params([("id", Value::Int64(*id))]),
        )
        .expect("delete one near neighbour");
    }

    let check = |stage: &str| {
        let (current, work) = observe_search(&db, || db.execute(indexed, &bound).unwrap());
        assert!(
            work.clustered_route,
            "{stage}: the search ran on the clustered index"
        );
        assert_eq!(
            int_ids(&current),
            live_answer,
            "{stage}: deleted rows are absent and do not crowd out the five nearest live rows"
        );
        assert_eq!(
            int_ids(&db.execute(exact, &bound).unwrap()),
            live_answer,
            "{stage}: the exact answer agrees"
        );
    };
    check("before list maintenance");
    assert_eq!(
        int_ids(&db.execute_at_snapshot(indexed, &bound, before).unwrap())[0],
        7,
        "the pinned snapshot still sees the row it was opened with"
    );
    drive_until_serving(&db, ROWS as usize, 1, 10);
    check("after list maintenance with a reader pinned");
    drop(pin);
    drive_until_serving(&db, ROWS as usize, 1, 10);
    check("after list maintenance with no reader");
}

fn victim(seed: u64) -> Vec<f32> {
    let mut vector = unit_vector(seed, PURGE_DIMENSIONS);
    vector[0] = 0.123_456_79;
    vector
}

fn pages_holding(pages: &[PhysicalPage], needles: &[Vec<u8>]) -> Vec<String> {
    pages
        .iter()
        .filter(|page| {
            needles
                .iter()
                .any(|needle| contains_bytes(&page.bytes, needle))
        })
        .map(|page| format!("{:?}", page.place))
        .collect()
}

#[test]
fn purge_erases_vector_bytes_from_every_index_page_buffer_generation_and_export() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("purged.db");
    let db = open_caller_driven(&path);
    db.execute(
        &format!(
            "CREATE TABLE purged_items (id INTEGER PRIMARY KEY, embedding VECTOR({PURGE_DIMENSIONS}) SEARCH_MODE INDEXED {})",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    for id in 0..ROWS {
        let vector = if id == 7 {
            victim(7)
        } else {
            unit_vector(id as u64, PURGE_DIMENSIONS)
        };
        insert(&db, "purged_items", id, vector);
    }
    drive_until_serving(&db, ROWS as usize, 1, 10);
    // A second victim committed after the build lives in the write buffer or
    // the committed delta, not in the built lists.
    insert(&db, "purged_items", 1_000, victim(1_000));

    let needles = [victim(7), victim(1_000)]
        .iter()
        .flat_map(|vector| {
            [
                little_endian_bytes(vector),
                quantized_record(&db, "purged_items", "embedding", vector),
            ]
        })
        .collect::<Vec<_>>();
    let before = physical_pages(&db, "purged_items", "embedding");
    assert!(
        before
            .iter()
            .any(|page| contains_bytes(&page.bytes, &needles[1])),
        "the scan sees the built victim's list record before PURGE"
    );
    assert!(
        before
            .iter()
            .any(|page| contains_bytes(&page.bytes, &needles[2])
                || contains_bytes(&page.bytes, &needles[3])),
        "the scan sees the buffered victim before PURGE"
    );
    let early_export = root.path().join("before-purge.export");
    db.export_snapshot(&early_export)
        .expect("export before PURGE");
    let early = std::fs::read(&early_export).expect("read the early export");
    assert!(
        contains_bytes(&early, &needles[0]),
        "an export taken before PURGE carries the victim's vector"
    );

    let erased = db
        .execute("PURGE FROM purged_items WHERE id IN (7, 1000)", &empty())
        .expect("PURGE the two victims");
    assert_eq!(erased.rows_affected(), 2);

    let after = physical_pages(&db, "purged_items", "embedding");
    assert!(
        !after.is_empty(),
        "the index still has pages for the surviving rows"
    );
    assert_eq!(
        pages_holding(&after, &needles),
        Vec::<String>::new(),
        "when PURGE returns, no index page, run, buffer or retained generation holds the erased bytes"
    );

    let late_export = root.path().join("after-purge.export");
    db.export_snapshot(&late_export)
        .expect("export after PURGE");
    let late = std::fs::read(&late_export).expect("read the late export");
    for needle in &needles {
        assert!(
            !contains_bytes(&late, needle),
            "an export taken after PURGE holds none of the erased bytes"
        );
    }
    drive_until_serving(&db, ROWS as usize, 1, 10);
    db.close().expect("close the purged store");
    drop(db);

    let db = open_caller_driven(&path);
    assert_eq!(
        pages_holding(&physical_pages(&db, "purged_items", "embedding"), &needles),
        Vec::<String>::new(),
        "after reopen no index page holds the erased bytes"
    );
    let (result, work) = observe_search(&db, || {
        db.execute(
            "SELECT id FROM purged_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3",
            &params([("query", Value::Vector(victim(7)))]),
        )
        .unwrap()
    });
    assert!(
        work.clustered_route,
        "the search ran on the clustered index"
    );
    let ids = int_ids(&result);
    assert_eq!(ids.len(), 3, "the surviving rows still answer");
    assert!(
        !ids.contains(&7) && !ids.contains(&1_000),
        "an erased vector is never an answer: {ids:?}"
    );
}

#[test]
fn purge_during_a_multi_cycle_build_abandons_it_and_leaves_no_sampled_vector() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_with_build_batch_rows(&root.path().join("building.db"), 16);
    db.execute(
        &format!(
            "CREATE TABLE building_items (id INTEGER PRIMARY KEY, embedding VECTOR({PURGE_DIMENSIONS}) SEARCH_MODE INDEXED {})",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    let tx = db.begin_or_panic();
    for id in 0..200_i64 {
        let vector = if id == 0 {
            victim(0)
        } else {
            unit_vector(id as u64, PURGE_DIMENSIONS)
        };
        db.execute_in_tx(
            tx,
            "INSERT INTO building_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the population");

    // Advance the build one bounded cycle at a time until its persisted
    // state holds the victim, while it is still unpublished.
    let mut needles = Vec::new();
    for _ in 0..maintenance_cycles(200, 16, 10) {
        db.run_maintenance_cycle().expect("one bounded build cycle");
        if !build_progress(&db, "building_items", "embedding", &[]).in_flight {
            assert_eq!(
                serving(&db, "building_items", "embedding", &[]),
                None,
                "the build spans several bounded cycles and is still unpublished when its state holds the victim"
            );
            continue;
        }
        let candidates = [
            little_endian_bytes(&victim(0)),
            quantized_record(&db, "building_items", "embedding", &victim(0)),
        ];
        let held = physical_pages(&db, "building_items", "embedding")
            .into_iter()
            .filter(|page| page.place.belongs_to_an_unpublished_build())
            .any(|page| {
                candidates
                    .iter()
                    .any(|needle| contains_bytes(&page.bytes, needle))
            });
        if held {
            needles = candidates
                .into_iter()
                .filter(|needle| !needle.is_empty())
                .collect();
            break;
        }
    }
    assert!(
        !needles.is_empty(),
        "the in-flight build's persisted state came to hold the victim"
    );
    assert_eq!(
        serving(&db, "building_items", "embedding", &[]),
        None,
        "nothing is published while the first build is in flight"
    );
    let aborted_before = build_progress(&db, "building_items", "embedding", &[]).aborted;

    let erased = db
        .execute("PURGE FROM building_items WHERE id = 0", &empty())
        .expect("PURGE the victim while the build is in flight");
    assert_eq!(erased.rows_affected(), 1);
    assert_eq!(
        build_progress(&db, "building_items", "embedding", &[]).aborted,
        aborted_before + 1,
        "PURGE abandons the in-flight build that sampled the victim"
    );
    assert_eq!(
        pages_holding(
            &physical_pages(&db, "building_items", "embedding"),
            &needles
        ),
        Vec::<String>::new(),
        "no training reservoir, accumulator, unpublished run or other page holds the victim"
    );

    drive_until_serving(&db, 200, 16, 10);
    assert!(
        serving(&db, "building_items", "embedding", &[]).is_some(),
        "a fresh build publishes after the abandoned one"
    );
    assert_eq!(
        pages_holding(
            &physical_pages(&db, "building_items", "embedding"),
            &needles
        ),
        Vec::<String>::new(),
        "the published generation holds none of the victim"
    );
    assert_eq!(count(&db, "building_items"), 199);
    let (result, work) = observe_search(&db, || {
        db.execute(
            "SELECT id FROM building_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3",
            &params([("query", Value::Vector(victim(0)))]),
        )
        .unwrap()
    });
    assert!(
        work.clustered_route,
        "the search ran on the clustered index"
    );
    assert!(
        !int_ids(&result).contains(&0),
        "the erased vector is never an answer"
    );
}

/// A partition emptied by deletes keeps serving (an empty answer) after
/// reopen, maintenance of it is not a failure, and rows written into it
/// afterwards are served and survive another reopen.
#[test]
fn a_partition_emptied_by_deletes_keeps_serving_after_reopen_and_accepts_new_rows() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("emptied.db");
    let search = |db: &Database, mode: &str| {
        int_ids(
            &db.execute(
                &format!(
                    "SELECT id FROM emptied_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 5"
                ),
                &params([("query", Value::Vector(unit_vector(0, DIMENSIONS)))]),
            )
            .expect("search the emptied partition"),
        )
    };
    let db = open_caller_driven(&path);
    db.execute(
        &format!(
            "CREATE TABLE emptied_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {})",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    for id in 0..ROWS {
        insert(&db, "emptied_items", id, unit_vector(id as u64, DIMENSIONS));
    }
    drive_until_serving(&db, ROWS as usize, 1, 10);
    for id in 0..ROWS {
        db.execute(
            "DELETE FROM emptied_items WHERE id = $id",
            &params([("id", Value::Int64(id))]),
        )
        .expect("delete one row");
    }
    drive_until_serving(&db, ROWS as usize, 1, 10);
    db.close().expect("close the emptied partition");

    let reopened = open_caller_driven(&path);
    for _ in 0..16 {
        let report = reopened
            .run_maintenance_cycle()
            .expect("maintenance of the reopened emptied partition returns");
        assert!(
            report.vector.first_failure.is_none(),
            "maintaining an emptied partition is not a failure: {:?}",
            report.vector.first_failure
        );
    }
    for mode in ["EXACT", "INDEXED"] {
        assert_eq!(
            search(&reopened, mode),
            Vec::<i64>::new(),
            "{mode} serves no rows from the emptied partition after reopen"
        );
    }

    let fresh = ROWS + 1;
    insert(
        &reopened,
        "emptied_items",
        fresh,
        unit_vector(0, DIMENSIONS),
    );
    drive_until_serving(&reopened, 1, 1, 10);
    for mode in ["EXACT", "INDEXED"] {
        assert_eq!(
            search(&reopened, mode),
            vec![fresh],
            "{mode} serves a row written into the emptied partition"
        );
    }
    reopened.close().expect("close the refilled partition");
    let again = open_caller_driven(&path);
    assert_eq!(
        search(&again, "INDEXED"),
        vec![fresh],
        "the refilled partition survives reopen"
    );
}

/// A PURGE refused on a declared disk or memory limit changes nothing: the
/// rows and every index page stay as they were, and once the limit is lifted
/// the same PURGE succeeds.
#[test]
fn a_purge_refused_on_a_declared_limit_changes_nothing_and_succeeds_once_lifted() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("purge-limits.db"));
    db.execute(
        &format!(
            "CREATE TABLE limit_items (id INTEGER PRIMARY KEY, embedding VECTOR({PURGE_DIMENSIONS}) SEARCH_MODE INDEXED {})",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    for id in 0..ROWS {
        insert(
            &db,
            "limit_items",
            id,
            unit_vector(id as u64, PURGE_DIMENSIONS),
        );
    }
    drive_until_serving(&db, ROWS as usize, 1, 10);
    let rows_before = count(&db, "limit_items");
    let pages = |db: &Database| {
        physical_pages(db, "limit_items", "embedding")
            .into_iter()
            .map(|page| page.bytes)
            .collect::<Vec<_>>()
    };
    let pages_before = pages(&db);

    let size = db.disk_file_size().expect("a file-backed store has a size");
    db.set_disk_limit(Some(size))
        .expect("install a disk limit with no room for the purge's growth");
    let refused = db.execute("PURGE FROM limit_items WHERE id = 7", &empty());
    assert!(
        matches!(
            refused,
            Err(contextdb_core::Error::DiskBudgetExceeded { .. })
        ),
        "a purge past the declared disk limit is refused with the typed disk error: {refused:?}"
    );
    assert_eq!(count(&db, "limit_items"), rows_before);
    assert_eq!(
        pages(&db),
        pages_before,
        "the disk refusal changed no index page"
    );
    db.set_disk_limit(None).expect("lift the disk limit");

    let used = db.accountant().usage().used;
    db.set_memory_limit(Some(used + 4_096))
        .expect("install a memory limit with no room for the purge's workspace");
    let refused = db.execute("PURGE FROM limit_items WHERE id = 7", &empty());
    assert!(
        matches!(
            refused,
            Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
        ),
        "a purge past the declared memory limit is refused with the typed memory error: {refused:?}"
    );
    assert_eq!(count(&db, "limit_items"), rows_before);
    assert_eq!(
        pages(&db),
        pages_before,
        "the memory refusal changed no index page"
    );
    assert_eq!(
        db.accountant().usage().used,
        used,
        "the refused purge released its workspace"
    );
    db.set_memory_limit(None).expect("lift the memory limit");

    let erased = db
        .execute("PURGE FROM limit_items WHERE id = 7", &empty())
        .expect("the same purge succeeds once the limits are lifted");
    assert_eq!(erased.rows_affected(), 1);
    assert_eq!(count(&db, "limit_items"), rows_before - 1);
}
