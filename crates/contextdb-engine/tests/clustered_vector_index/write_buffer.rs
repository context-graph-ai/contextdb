//! An unlimited sequence of separately committed vector inserts stays inside a
//! fixed write-buffer cap: twice as many commits never raise the buffer's
//! high-water, and every commit is searchable when it is acknowledged. No
//! maintenance runs between the commits, so the bound cannot depend on a
//! background pass keeping up.

use super::support::{
    WriteBuffer, drive_until_serving, int_ids, ivf, observe_search, open_with_write_buffer_cap,
    params, unit_vector, write_buffer,
};
use contextdb_core::Value;
use contextdb_engine::Database;

const DIMENSIONS: usize = 64;
const CAP_BYTES: u64 = 16 * 1024;
const SEED_ROWS: i64 = 64;
/// N commits carry four times the cap in raw vector bytes alone.
const N: i64 = (4 * CAP_BYTES as i64) / (DIMENSIONS as i64 * 4);

fn insert(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO buffered_items (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Int64(id)),
            (
                "embedding",
                Value::Vector(unit_vector(id as u64, DIMENSIONS)),
            ),
        ]),
    )
    .expect("commit one vector insert");
}

/// Builds a serving index, then commits `commits` inserts one at a time and
/// checks each is found by an indexed search as soon as its commit returns.
fn commit_one_at_a_time(commits: i64) -> WriteBuffer {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_with_write_buffer_cap(&root.path().join("buffered.db"), CAP_BYTES);
    db.execute(
        &format!(
            "CREATE TABLE buffered_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {})",
            ivf(4, 4, 64)
        ),
        &super::support::empty(),
    )
    .expect("declare the clustered index");
    let tx = db.begin_or_panic();
    for id in 0..SEED_ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO buffered_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one seed row");
    }
    db.commit(tx).expect("commit the seed rows");
    drive_until_serving(&db, SEED_ROWS as usize, 1, 10);

    for id in SEED_ROWS..SEED_ROWS + commits {
        insert(&db, id);
        let (result, work) = observe_search(&db, || {
            db.execute(
                "SELECT id FROM buffered_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
                &params([("query", Value::Vector(unit_vector(id as u64, DIMENSIONS)))]),
            )
            .expect("search right after the commit")
        });
        assert!(
            work.clustered_route,
            "the search ran on the clustered index"
        );
        assert_eq!(
            int_ids(&result),
            vec![id],
            "row {id} is searchable when its commit is acknowledged"
        );
    }
    let buffer = write_buffer(&db, "buffered_items", "embedding");
    db.close().expect("close the store");
    buffer
}

#[test]
#[ignore = "the vector index route is switched off in this release"]
fn twice_as_many_separate_commits_never_raise_the_write_buffer_high_water() {
    let n = commit_one_at_a_time(N);
    let two_n = commit_one_at_a_time(2 * N);

    for buffer in [n, two_n] {
        assert_eq!(buffer.cap_bytes, CAP_BYTES, "the declared cap is in force");
        assert!(
            buffer.high_water_bytes > 0,
            "the commits went through the write buffer: {buffer:?}"
        );
        assert!(
            buffer.high_water_bytes <= buffer.cap_bytes,
            "the buffer never grew past its cap: {buffer:?}"
        );
    }
    assert!(
        n.sealed_runs >= 1,
        "{N} commits carry four caps of vectors, so the buffer sealed at least once: {n:?}"
    );
    assert!(
        two_n.sealed_runs > n.sealed_runs,
        "doubling the commits seals more runs instead of growing the buffer: N {n:?}, 2N {two_n:?}"
    );
    assert_eq!(
        two_n.high_water_bytes, n.high_water_bytes,
        "N and 2N separate commits reach the same buffer high-water"
    );
}
