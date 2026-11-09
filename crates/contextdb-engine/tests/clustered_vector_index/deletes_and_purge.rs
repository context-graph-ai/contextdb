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
    cosine_distance, count, drive_until_serving, empty, int_ids, ivf, open_caller_driven, params,
    physical_pages, unit_vector,
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
            "CREATE TABLE deleted_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE AUTO {})",
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

    let auto = "SELECT id FROM deleted_items ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 5";
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
        assert_eq!(
            int_ids(&db.execute(auto, &bound).unwrap()),
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
        int_ids(&db.execute_at_snapshot(auto, &bound, before).unwrap())[0],
        7,
        "the pinned snapshot still sees the row it was opened with"
    );
    drive_until_serving(&db, ROWS as usize, 1, 10);
    check("after list maintenance with a reader pinned");
    drop(pin);
    drive_until_serving(&db, ROWS as usize, 1, 10);
    check("after list maintenance with no reader");
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
            "CREATE TABLE emptied_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE AUTO {})",
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
    for mode in ["EXACT", "AUTO"] {
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
    for mode in ["EXACT", "AUTO"] {
        assert_eq!(
            search(&reopened, mode),
            vec![fresh],
            "{mode} serves a row written into the emptied partition"
        );
    }
    reopened.close().expect("close the refilled partition");
    let again = open_caller_driven(&path);
    assert_eq!(
        search(&again, "AUTO"),
        vec![fresh],
        "the refilled partition survives reopen"
    );
}

/// A PURGE refused on the declared memory limit changes nothing: the rows and
/// every index page stay as they were and its workspace is released, and once
/// the limit is lifted the same PURGE succeeds. (A native PURGE reuses existing
/// pages and never grows the file, so no disk ceiling can refuse it.)
#[test]
fn a_purge_refused_on_the_declared_memory_limit_changes_nothing_and_succeeds_once_lifted() {
    purge_refused_on_the_declared_memory_limit("AUTO", false);
}

fn purge_refused_on_the_declared_memory_limit(search_mode: &str, index_pages: bool) {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("purge-limits.db"));
    db.execute(
        &format!(
            "CREATE TABLE limit_items (id INTEGER PRIMARY KEY, embedding VECTOR({PURGE_DIMENSIONS}) SEARCH_MODE {search_mode} {})",
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
    let pages_before = index_pages.then(|| pages(&db));

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
    assert_eq!(
        db.accountant().usage().used,
        used,
        "the refused purge released its workspace"
    );
    db.set_memory_limit(None).expect("lift the memory limit");
    assert_eq!(count(&db, "limit_items"), rows_before);
    if let Some(pages_before) = pages_before {
        assert_eq!(
            pages(&db),
            pages_before,
            "the memory refusal changed no index page"
        );
    }

    let erased = db
        .execute("PURGE FROM limit_items WHERE id = 7", &empty())
        .expect("the same purge succeeds once the limits are lifted");
    assert_eq!(erased.rows_affected(), 1);
    assert_eq!(count(&db, "limit_items"), rows_before - 1);
}
