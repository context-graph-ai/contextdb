//! `PROPAGATE ON STATE ... EXCLUDE VECTOR` (docs/query-language.md
//! ~797-825) removes a row's vector from `<=>` similarity search once the
//! row transitions into the named state, without deleting the row itself.
//! Confirmed working WITHIN one process. The zero-sampling doc tester found
//! that across a close-and-reopen of the same file-backed store, the
//! excluded vector REAPPEARS in search results while the row's `status`
//! column correctly persists as the post-transition value -- reproduced
//! twice on separate stores via separate CLI invocations. These tests pin
//! that defect against the in-process `Database::open`/`close`/reopen path.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// A minimal table matching docs/query-language.md's `PROPAGATE ON STATE
/// EXCLUDE VECTOR` example: a state machine column plus a single vector
/// column, with the transition into `invalidated` excluding the vector.
const CREATE_DECISIONS: &str = "CREATE TABLE decisions (\
     id UUID PRIMARY KEY, \
     status TEXT, \
     embedding VECTOR(3)\
     ) STATE MACHINE (status: active -> [invalidated]) \
     PROPAGATE ON STATE invalidated EXCLUDE VECTOR";

fn axis(index: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[index.min(2)] = 1.0;
    vector
}

/// The nearest-neighbor id for `query`, or `None` if the search returns no
/// rows (an all-excluded index legitimately returns nothing).
fn nearest_id(db: &Database, query: Vec<f32>) -> Option<Uuid> {
    let result = db
        .execute(
            "SELECT id FROM decisions ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(query))]),
        )
        .expect("vector search must succeed");
    result.rows.first().map(|row| match row[0] {
        Value::Uuid(id) => id,
        ref other => panic!("expected UUID id, got {other:?}"),
    })
}

/// RED: a vector excluded by a state transition must stay excluded from
/// `<=>` search after the store is closed and reopened from the same file
/// path -- not just within the process that performed the transition.
#[test]
fn excluded_vector_stays_excluded_across_reopen() {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("vector-exclusion.db");
    let excluded_id = Uuid::from_u128(1);
    let surviving_id = Uuid::from_u128(2);

    {
        let db = Database::open(&path).expect("open fresh store");
        db.execute(CREATE_DECISIONS, &params(vec![]))
            .expect("create decisions table");
        db.execute(
            "INSERT INTO decisions (id, status, embedding) VALUES ($id, 'active', $embedding)",
            &params(vec![
                ("id", Value::Uuid(excluded_id)),
                ("embedding", Value::Vector(axis(0))),
            ]),
        )
        .expect("insert the row that will be excluded");
        db.execute(
            "INSERT INTO decisions (id, status, embedding) VALUES ($id, 'active', $embedding)",
            &params(vec![
                ("id", Value::Uuid(surviving_id)),
                ("embedding", Value::Vector(axis(1))),
            ]),
        )
        .expect("insert a sibling row that stays active");

        // The pre-transition sanity check: querying axis(0) finds the
        // soon-to-be-excluded row while it is still active.
        assert_eq!(
            nearest_id(&db, axis(0)),
            Some(excluded_id),
            "premise: the excluded row's own vector must be the nearest match before the \
             state transition"
        );

        db.execute(
            "UPDATE decisions SET status = 'invalidated' WHERE id = $id",
            &params(vec![("id", Value::Uuid(excluded_id))]),
        )
        .expect("transition the row to invalidated");

        // In-process: PROPAGATE ON STATE invalidated EXCLUDE VECTOR already
        // works today -- querying axis(0) no longer finds the excluded
        // row's own vector; the sibling (axis(1)) is the closest survivor.
        assert_eq!(
            nearest_id(&db, axis(0)),
            Some(surviving_id),
            "in-process: the excluded row must not be the nearest match to its own vector \
             once invalidated -- if this fails, EXCLUDE VECTOR itself is broken, not just its \
             persistence across reopen"
        );

        db.close().expect("close the store cleanly");
    }

    let reopened = Database::open(&path).expect("reopen the same store");

    // GREEN pin: the status column persists correctly across reopen --
    // this half of the defect's premise already works today.
    let status = reopened
        .execute(
            "SELECT status FROM decisions WHERE id = $id",
            &params(vec![("id", Value::Uuid(excluded_id))]),
        )
        .expect("read back status after reopen");
    assert_eq!(
        status.rows,
        vec![vec![Value::Text("invalidated".to_string())]],
        "the row's status column must persist as 'invalidated' across reopen -- this is the \
         half of the bug that already works; if THIS fails, the defect is broader than the \
         doc tester reported"
    );

    // RED: the vector exclusion itself must survive the same reopen. Today
    // it does not -- the excluded row's vector reappears as the nearest
    // match to its own embedding, exactly as if EXCLUDE VECTOR had never
    // fired.
    assert_eq!(
        nearest_id(&reopened, axis(0)),
        Some(surviving_id),
        "the excluded row's vector must stay excluded from <=> search after a close+reopen of \
         the same file-backed store -- the row's own vector must not be the nearest match to \
         itself post-reopen. If this assertion instead sees the excluded row's own id, the \
         vector-index deletion from PROPAGATE ON STATE ... EXCLUDE VECTOR was not durably \
         persisted, and reopening silently un-excludes it while the status column (checked \
         above) correctly stays 'invalidated'"
    );
}
