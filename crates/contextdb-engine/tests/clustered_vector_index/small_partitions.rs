//! Small partitions answer completely. Under default PROBES, an indexed
//! search keeps probing the remaining active lists in centroid-distance order
//! until it has k eligible candidates or every list is exhausted, so LIMIT 128
//! over 128 rows spread across 64 lists returns all 128, and a partition with
//! deleted rows returns every live row. An explicit INDEXED search on a
//! 150-row partition is served COMPLETE from that partition's published
//! generation, with the quality status of its calibration receipt.

use super::support::{
    ObjectiveStatus, SearchOutcome, drive_until_serving, empty, int_ids, observe_search,
    open_caller_driven, params, serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use uuid::Uuid;

const DIMENSIONS: usize = 16;

fn query() -> std::collections::HashMap<String, Value> {
    params([("query", Value::Vector(unit_vector(4_242, DIMENSIONS)))])
}

fn indexed_and_exact(
    db: &Database,
    limit: usize,
) -> (Vec<i64>, Vec<i64>, super::support::SearchWork) {
    let (indexed, work) = observe_search(db, || {
        db.execute(
            &format!(
                "SELECT id FROM small_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT {limit}"
            ),
            &query(),
        )
        .expect("indexed search")
    });
    let exact = db
        .execute(
            &format!(
                "SELECT id FROM small_items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT {limit}"
            ),
            &query(),
        )
        .expect("exact search");
    (int_ids(&indexed), int_ids(&exact), work)
}

#[test]
fn default_probes_keep_probing_until_k_eligible_candidates_are_found() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("small.db"));
    db.execute(
        &format!(
            "CREATE TABLE small_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED IVF (LISTS = 64))"
        ),
        &empty(),
    )
    .expect("declare 64 lists with default PROBES");
    let tx = db.begin_or_panic();
    for id in 0..128_i64 {
        db.execute_in_tx(
            tx,
            "INSERT INTO small_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit 128 rows");
    drive_until_serving(&db);
    let lists = serving(&db, "small_items", "embedding", &[])
        .expect("a generation serves")
        .lists;

    let (indexed, exact, work) = indexed_and_exact(&db, 128);
    assert!(
        work.clustered_route,
        "the search ran on the clustered index"
    );
    assert_eq!(work.outcome, Some(SearchOutcome::Complete), "{work:?}");
    assert_eq!(
        indexed.len(),
        128,
        "LIMIT 128 over 128 rows returns every row: {work:?}"
    );
    assert_eq!(indexed, exact, "every row, in full-precision order");
    assert!(
        work.lists_probed >= 1 && work.lists_probed <= lists,
        "the route record reports the lists it probed: {work:?}"
    );

    for id in (0..128_i64).step_by(4).take(28) {
        db.execute(
            "DELETE FROM small_items WHERE id = $id",
            &params([("id", Value::Int64(id))]),
        )
        .expect("delete one row");
    }
    let (indexed, exact, work) = indexed_and_exact(&db, 128);
    assert_eq!(work.outcome, Some(SearchOutcome::Complete), "{work:?}");
    assert_eq!(
        indexed.len(),
        100,
        "every live row is returned when fewer than k remain: {work:?}"
    );
    assert_eq!(indexed, exact, "only eligible rows count toward k");
}

#[test]
fn explicit_indexed_on_a_150_row_partition_is_served_complete_from_its_generation() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("partition.db"));
    db.execute(
        &format!(
            "CREATE TABLE small_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id))"
        ),
        &empty(),
    )
    .expect("declare a partitioned column with derived policy");
    let context = |ordinal: u128| Uuid::from_u128(0x5A_0000 + ordinal);
    let tx = db.begin_or_panic();
    for ordinal in 0..2_u128 {
        for row in 0..150_i64 {
            let id = ordinal as i64 * 150 + row;
            db.execute_in_tx(
                tx,
                "INSERT INTO small_items (id, context_id, embedding) VALUES ($id, $context, $embedding)",
                &params([
                    ("id", Value::Int64(id)),
                    ("context", Value::Uuid(context(ordinal))),
                    ("embedding", Value::Vector(unit_vector(id as u64, DIMENSIONS))),
                ]),
            )
            .expect("stage one row");
        }
    }
    db.commit(tx).expect("commit both partitions");
    drive_until_serving(&db);
    let generation = serving(&db, "small_items", "embedding", &[Value::Uuid(context(0))])
        .expect("the 150-row partition has a published generation")
        .generation;

    let (result, work) = observe_search(&db, || {
        db.execute(
            "SELECT id FROM small_items WHERE context_id = $c0 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &params([
                ("c0", Value::Uuid(context(0))),
                ("query", Value::Vector(unit_vector(4_242, DIMENSIONS))),
            ]),
        )
        .expect("explicit indexed search")
    });
    let ids = int_ids(&result);
    assert_eq!(ids.len(), 10, "a full page");
    assert!(
        ids.iter().all(|id| (0..150).contains(id)),
        "only the searched partition answers"
    );
    assert!(
        work.clustered_route,
        "explicit INDEXED runs on the clustered index: {work:?}"
    );
    assert_eq!(work.outcome, Some(SearchOutcome::Complete), "{work:?}");
    assert_eq!(
        work.served_generation,
        Some(generation),
        "it is served from the partition's published generation: {work:?}"
    );
    assert!(
        matches!(
            work.objective_status,
            Some(ObjectiveStatus::Met | ObjectiveStatus::Unmet | ObjectiveStatus::Unqualified)
        ),
        "the answer carries its calibration receipt's status: {work:?}"
    );
}
