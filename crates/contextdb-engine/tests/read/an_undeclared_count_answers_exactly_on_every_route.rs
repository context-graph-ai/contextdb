//! `COUNT(*)` answers the true number on every read route, whatever the table
//! declares.
//!
//! A maintained count is a cost choice, never an answer. Silence maintains
//! none, on the condition that an undeclared `COUNT(*)` still answers
//! EXACTLY. So the absence of a
//! stored count record must never be read as the number zero: it is the
//! absence of a shortcut, and the ordinary index-range aggregate counts the
//! live postings instead.
//!
//! The direct-file route is the one that can actually reach a stored count, so
//! it is the one where mistaking "no record" for "zero" produces a wrong
//! answer rather than a missed optimisation. It is checked here beside the
//! owner route and a writable handle, through declare and undeclare, because a
//! record left behind by an earlier declaration must never be trusted once the
//! declaration that made it is gone.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::path::Path;

const OVER_A: &str = "SELECT COUNT(*) FROM placements WHERE a = $a";
const OVER_AB: &str = "SELECT COUNT(*) FROM placements WHERE a = $a AND b = $b";

fn over_a() -> HashMap<String, Value> {
    HashMap::from([("a".to_string(), Value::Text("left".into()))])
}

fn over_ab() -> HashMap<String, Value> {
    HashMap::from([
        ("a".to_string(), Value::Text("left".into())),
        ("b".to_string(), Value::Text("up".into())),
    ])
}

fn execute(db: &Database, sql: &str, params: HashMap<String, Value>) {
    db.execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture statement `{sql}`: {error}"));
}

fn seed(path: &Path) -> Database {
    let db = Database::open(path).expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE placements (id UUID PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL)",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX placements_by_a_b ON placements (a, b)",
        HashMap::new(),
    );
    // 12 rows: `a` is left on 6 of them, and `(left, up)` holds 2.
    for n in 0..12u128 {
        execute(
            &db,
            "INSERT INTO placements (id, a, b) VALUES ($id, $a, $b)",
            HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(n))),
                (
                    "a".into(),
                    Value::Text(if n % 2 == 0 {
                        "left".into()
                    } else {
                        "right".into()
                    }),
                ),
                (
                    "b".into(),
                    Value::Text(if n % 3 == 0 {
                        "up".into()
                    } else {
                        "down".into()
                    }),
                ),
            ]),
        );
    }
    db
}

/// The same two counts on a writable handle, on the owner route while that
/// handle still holds the store, and on the direct-file route after it lets
/// go. `stage` names which declaration the store is carrying.
fn every_route_answers(path: &Path, db: &Database, stage: &str) {
    for (sql, params, expected) in [(OVER_A, over_a(), 6i64), (OVER_AB, over_ab(), 2i64)] {
        let writable = db.execute(sql, &params).expect("writable-handle count");
        assert_eq!(
            writable.rows()[0][0],
            Value::Int64(expected),
            "{stage}: the writable handle answers `{sql}` exactly"
        );

        let owner = ReadSession::open_with_options(path, ReadSessionOptions::default())
            .expect("open a reading session while the writer holds the store");
        assert_eq!(owner.route(), ReadRoute::Owner);
        let answered = owner.execute(sql, &params).expect("owner-route count");
        assert_eq!(
            answered.rows()[0][0],
            Value::Int64(expected),
            "{stage}: the owner route answers `{sql}` exactly"
        );
    }
}

fn file_route_answers(path: &Path, stage: &str) {
    let session = ReadSession::open_with_options(path, ReadSessionOptions::default())
        .expect("open a reading session on an unheld store");
    assert_eq!(session.route(), ReadRoute::File);
    for (sql, params, expected) in [(OVER_A, over_a(), 6i64), (OVER_AB, over_ab(), 2i64)] {
        let answered = session.execute(sql, &params).expect("file-route count");
        assert_eq!(
            answered.rows()[0][0],
            Value::Int64(expected),
            "{stage}: the direct-file route answers `{sql}` exactly"
        );
    }
}

#[test]
fn an_undeclared_count_answers_exactly_on_every_route() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("undeclared-every-route.db");

    // 1. Nothing declared. No count record exists for either list, and the
    //    absence of a record is not the number zero.
    {
        let db = seed(&path);
        every_route_answers(&path, &db, "undeclared");
        db.close().expect("close the writer");
    }
    file_route_answers(&path, "undeclared");

    // 2. One list declared. The declared count is stored and correct; the
    //    other list is still undeclared and still exact.
    {
        let db = Database::open(&path).expect("reopen the writable fixture");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        execute(
            &db,
            "ALTER TABLE placements SET MAINTAIN COUNT (a)",
            HashMap::new(),
        );
        every_route_answers(&path, &db, "declared (a)");
        db.close().expect("close the writer");
    }
    file_route_answers(&path, "declared (a)");

    // 3. The declaration dropped again. Its records go with it, and no route
    //    may trust a record the declaration no longer covers.
    {
        let db = Database::open(&path).expect("reopen the writable fixture");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        execute(
            &db,
            "ALTER TABLE placements SET MAINTAIN COUNT DEFAULT",
            HashMap::new(),
        );
        every_route_answers(&path, &db, "undeclared again");
        db.close().expect("close the writer");
    }
    file_route_answers(&path, "undeclared again");
}
