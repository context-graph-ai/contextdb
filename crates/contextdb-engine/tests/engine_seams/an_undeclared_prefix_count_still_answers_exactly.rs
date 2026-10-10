//! A `COUNT(*)` over a prefix the table does not declare is still exact, and
//! the trace says so.
//!
//! Only one read path can consume a maintained count: a `COUNT(*)` whose
//! pushed predicate is exact equality over a full leading run of one index.
//! When the table declares that column list, the answer is one stored number.
//! When it does not, the ordinary aggregate opens the same exact prefix range
//! and counts live postings — the identical number, charged one index-entry
//! touch per posting, refused by the ordinary typed limit refusal if it
//! exceeds the statement's declared budget rather than silently degrading.
//!
//! What a consumer could NOT see before is which of the two it got: both
//! traces said `IndexScan` with `index_used` set. The `maintained count:` line
//! is that difference, and it prints the exact `ALTER TABLE` that would make
//! the undeclared arm O(1).

use contextdb_core::Value;
use contextdb_engine::cli_render::render_explain_trace;
use contextdb_engine::database::QueryTrace;
use contextdb_engine::maintained_counts::MaintainedCountDisclosure;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn execute(db: &Database, sql: &str, params: HashMap<String, Value>) {
    db.execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture statement `{sql}`: {error}"));
}

/// `owner` is declared and maintained; `(owner, slot)` is the second prefix of
/// the same index and is deliberately NOT declared.
fn fixture(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE placements (id UUID PRIMARY KEY, owner TEXT NOT NULL, slot TEXT NOT NULL)",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX placements_by_owner_slot ON placements (owner, slot)",
        HashMap::new(),
    );
    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT (owner)",
        HashMap::new(),
    );
    db
}

fn insert(db: &Database, n: u128, owner: &str, slot: &str) {
    execute(
        db,
        "INSERT INTO placements (id, owner, slot) VALUES ($id, $owner, $slot)",
        HashMap::from([
            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(n))),
            ("owner".into(), Value::Text(owner.into())),
            ("slot".into(), Value::Text(slot.into())),
        ]),
    );
}

fn answer(db: &Database, sql: &str, params: HashMap<String, Value>) -> (i64, QueryTrace) {
    let result = db
        .execute(sql, &params)
        .unwrap_or_else(|error| panic!("count `{sql}`: {error}"));
    let count = match &result.rows[0][0] {
        Value::Int64(count) => *count,
        other => panic!("a count is an integer, got {other:?}"),
    };
    (count, result.trace)
}

fn by_owner(owner: &str) -> HashMap<String, Value> {
    HashMap::from([("owner".into(), Value::Text(owner.into()))])
}

fn by_owner_slot(owner: &str, slot: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("owner".into(), Value::Text(owner.into())),
        ("slot".into(), Value::Text(slot.into())),
    ])
}

const DECLARED: &str = "SELECT COUNT(*) FROM placements WHERE owner = $owner";
const UNDECLARED: &str = "SELECT COUNT(*) FROM placements WHERE owner = $owner AND slot = $slot";

#[test]
fn an_undeclared_prefix_count_still_answers_exactly() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = Arc::new(fixture(&root.path().join("undeclared-counts.db")));

    for n in 0..40u128 {
        insert(&db, n, if n % 4 == 0 { "ada" } else { "bob" }, "left");
    }
    // Ten rows are ada/left, thirty are bob/left; the declared prefix and the
    // undeclared one are asked about the same rows.
    let (declared, declared_trace) = answer(&db, DECLARED, by_owner("ada"));
    let (undeclared, undeclared_trace) = answer(&db, UNDECLARED, by_owner_slot("ada", "left"));
    assert_eq!(declared, 10);
    assert_eq!(
        undeclared, declared,
        "the undeclared prefix answers the same number as the declared one"
    );

    // The writable route serves a declared count from the captured writer
    // root; the undeclared prefix still names the missing declaration.
    assert_eq!(
        declared_trace.maintained_count.as_deref(),
        Some(&MaintainedCountDisclosure::Used {
            table: "placements".to_owned(),
            columns: vec![("owner".to_owned(), contextdb_core::SortDirection::Asc)],
        })
    );
    assert_eq!(
        undeclared_trace.maintained_count.as_deref(),
        Some(&MaintainedCountDisclosure::NotDeclared {
            table: "placements".to_owned(),
            columns: vec![
                ("owner".to_owned(), contextdb_core::SortDirection::Asc),
                ("slot".to_owned(), contextdb_core::SortDirection::Asc),
            ],
            // `SET` replaces the whole set, so the advice carries the list
            // this table already declares alongside the missing one.
            already_declared: vec![vec![(
                "owner".to_owned(),
                contextdb_core::SortDirection::Asc,
            )]],
        }),
        "the omission is named on the trace of the aggregate that answered it"
    );

    let declared_explain = render_explain_trace(&declared_trace);
    assert!(
        declared_explain.contains("maintained count: used (owner)")
            && !declared_explain.contains("not declared"),
        "a declared count is never reported as undeclared: {declared_explain}"
    );
    let undeclared_explain = render_explain_trace(&undeclared_trace);
    assert!(
        undeclared_explain.contains(
            "maintained count: not declared — ALTER TABLE placements SET MAINTAIN COUNT \
             (owner), (owner, slot) makes this O(1)"
        ),
        "the explain names the exact statement that restores O(1): {undeclared_explain}"
    );

    // Declaring it makes the same query O(1) and does not change the answer.
    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT (owner), (owner, slot)",
        HashMap::new(),
    );
    let (after, after_trace) = answer(&db, UNDECLARED, by_owner_slot("ada", "left"));
    assert_eq!(after, declared, "the declaration changes cost, not answer");
    assert!(
        matches!(
            after_trace.maintained_count.as_deref(),
            Some(MaintainedCountDisclosure::Used { .. })
        ),
        "once declared it is no longer advised about: {:?}",
        after_trace.maintained_count
    );

    db.close().expect("close the fixture");
}

/// The same pair of counts while another thread is inserting into the very
/// rows they are about. Each answer is compared against the range it is
/// allowed to be in at the instant it ran, and once the writer is quiet both
/// routes are asserted against the same exact number.
#[test]
fn the_two_routes_agree_under_a_concurrent_writer() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = Arc::new(fixture(&root.path().join("concurrent-counts.db")));
    for n in 0..20u128 {
        insert(&db, n, "ada", "left");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut n = 1_000u128;
            while !stop.load(Ordering::SeqCst) {
                insert(&db, n, "ada", "left");
                n += 1;
            }
            n - 1_000
        })
    };

    for _ in 0..50 {
        let (before, _) = answer(&db, DECLARED, by_owner("ada"));
        let (undeclared, trace) = answer(&db, UNDECLARED, by_owner_slot("ada", "left"));
        let (after, _) = answer(&db, DECLARED, by_owner("ada"));
        assert!(
            undeclared >= before && undeclared <= after,
            "the undeclared route's answer is a real count taken between two declared ones: \
             {before} <= {undeclared} <= {after}"
        );
        assert!(matches!(
            trace.maintained_count.as_deref(),
            Some(MaintainedCountDisclosure::NotDeclared { .. })
        ));
    }

    stop.store(true, Ordering::SeqCst);
    let inserted = writer.join().expect("the concurrent writer finishes");
    let expected = 20 + i64::try_from(inserted).expect("insert count fits");
    let (declared, _) = answer(&db, DECLARED, by_owner("ada"));
    let (undeclared, _) = answer(&db, UNDECLARED, by_owner_slot("ada", "left"));
    assert_eq!(declared, expected, "the maintained count is exact");
    assert_eq!(
        undeclared, expected,
        "so is the index-range aggregate that answered without one"
    );

    Arc::try_unwrap(db)
        .expect("the writer released the database")
        .close()
        .expect("close the fixture");
}
