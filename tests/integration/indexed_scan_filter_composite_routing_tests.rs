//! Composite indexed-scan routing tests for full-prefix selection and trace honesty.

use contextdb_core::types::ContextId;
use contextdb_core::{Lsn, Value};
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_engine::{Database, QueryResult};
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;
use uuid::Uuid;

const FEWER: &str = "fewer predicate columns matched than chosen index";
const FIRST: &str = "first column not in WHERE";
const LOWER: &str = "lower selectivity than chosen index";
const TIED: &str = "tied with chosen index; lost by creation order";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn tid(n: u128) -> Uuid {
    Uuid::from_u128(0xC013_0000_0000_0000_0000_0000_0000_0000 + n)
}

fn pushed(r: &QueryResult) -> Vec<&str> {
    r.trace
        .predicates_pushed
        .iter()
        .map(|c| c.as_ref())
        .collect()
}

fn reason<'a>(r: &'a QueryResult, name: &str) -> Option<&'a str> {
    r.trace
        .indexes_considered
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.rejected_reason.as_ref())
}

fn pushed_owned(r: &QueryResult) -> Vec<String> {
    pushed(r).into_iter().map(str::to_string).collect()
}

fn considered_owned(r: &QueryResult) -> Vec<(String, String)> {
    r.trace
        .indexes_considered
        .iter()
        .map(|c| (c.name.clone(), c.rejected_reason.to_string()))
        .collect()
}

fn trace_capture(r: &QueryResult) -> (Option<String>, Vec<String>, Vec<(String, String)>) {
    (
        r.trace.index_used.clone(),
        pushed_owned(r),
        considered_owned(r),
    )
}

fn result_capture(
    r: QueryResult,
    examined: u64,
) -> (Vec<Vec<Value>>, Option<String>, Vec<String>, u64) {
    let index = r.trace.index_used.clone();
    let pushed = pushed_owned(&r);
    (r.rows, index, pushed, examined)
}

fn create_three_col_int(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, col3 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2, col3)", &empty())
        .unwrap();
}

fn insert_int3(db: &Database, id: Uuid, col: i64, col2: i64, col3: i64) {
    db.execute(
        "INSERT INTO t (id, col, col2, col3) VALUES ($id, $col, $col2, $col3)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("col", Value::Int64(col)),
            ("col2", Value::Int64(col2)),
            ("col3", Value::Int64(col3)),
        ]),
    )
    .unwrap();
}

fn create_two_col_int(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2)", &empty())
        .unwrap();
}

fn insert_int2(db: &Database, id: Uuid, col: i64, col2: i64) {
    db.execute(
        "INSERT INTO t (id, col, col2) VALUES ($id, $col, $col2)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("col", Value::Int64(col)),
            ("col2", Value::Int64(col2)),
        ]),
    )
    .unwrap();
}

fn create_uuid_hop(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col UUID, col2 UUID, col3 TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2, col3)", &empty())
        .unwrap();
}

fn insert_uuid_hop(db: &Database, id: Uuid, col: Uuid, col2: Uuid, col3: &str) {
    db.execute(
        "INSERT INTO t (id, col, col2, col3) VALUES ($id, $col, $col2, $col3)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("col", Value::Uuid(col)),
            ("col2", Value::Uuid(col2)),
            ("col3", Value::Text(col3.into())),
        ]),
    )
    .unwrap();
}

fn seed_is02_fixture(db: &Database) {
    create_two_col_int(db);
    let mut n = 1;
    for col in 1..=4 {
        for col2 in [10, 20, 30] {
            insert_int2(db, tid(n), col, col2);
            n += 1;
        }
    }
}

fn seed_is03_fixture(db: &Database) -> (Uuid, Uuid, Uuid, Uuid) {
    create_uuid_hop(db);
    let x = tid(0xE0);
    let other = tid(0xE1);
    for col_n in 1..=4 {
        let col = tid(0x100 + col_n);
        insert_uuid_hop(db, tid(0x200 + col_n * 10), col, x, "Y");
        insert_uuid_hop(db, tid(0x200 + col_n * 10 + 1), col, other, "Z");
        insert_uuid_hop(db, tid(0x200 + col_n * 10 + 2), col, x, "Z");
    }
    (tid(0x102), tid(0x103), x, other)
}

fn seed_is04_fixture(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER UNIQUE, col2 INTEGER, col3 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2, col3)", &empty())
        .unwrap();
    for col in 1..=8 {
        insert_int3(db, tid(col as u128), col, col * 10, col * 100);
    }
}

fn is04_query(db: &Database) -> QueryResult {
    db.__reset_rows_examined();
    db.execute(
        "SELECT col FROM t WHERE col IN (2, 5) AND col2 = 20 AND col3 = 200",
        &empty(),
    )
    .unwrap()
}

fn seed_is13_fixture(db: &Database, short_first: bool) {
    db.execute(
        "CREATE TABLE tie (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, col3 INTEGER)",
        &empty(),
    )
    .unwrap();
    if short_first {
        db.execute("CREATE INDEX idx_short ON tie (col, col2)", &empty())
            .unwrap();
        db.execute("CREATE INDEX idx_long ON tie (col, col2, col3)", &empty())
            .unwrap();
    } else {
        db.execute("CREATE INDEX idx_long ON tie (col, col2, col3)", &empty())
            .unwrap();
        db.execute("CREATE INDEX idx_short ON tie (col, col2)", &empty())
            .unwrap();
    }
    for (n, col2) in [5, 20, 30].into_iter().enumerate() {
        db.execute(
            "INSERT INTO tie (id, col, col2, col3) VALUES ($id, 2, $col2, $col3)",
            &params(vec![
                ("id", Value::Uuid(tid(0x300 + n as u128))),
                ("col2", Value::Int64(col2)),
                ("col3", Value::Int64(100 + n as i64)),
            ]),
        )
        .unwrap();
    }
    for col in 10..15 {
        db.execute(
            "INSERT INTO tie (id, col, col2, col3) VALUES ($id, $col, 1, 1)",
            &params(vec![
                ("id", Value::Uuid(tid(0x400 + col as u128))),
                ("col", Value::Int64(col)),
            ]),
        )
        .unwrap();
    }
}

fn seed_skew_int(db: &Database) {
    create_three_col_int(db);
    for i in 0..1000 {
        let (col2, col3) = if i < 3 {
            (7, 9)
        } else {
            (i as i64 + 100, i as i64 + 200)
        };
        insert_int3(db, tid(0x5000 + i), 500, col2, col3);
    }
    for i in 0..5 {
        insert_int3(db, tid(0x7000 + i), 700 + i as i64, 7, 9);
    }
}

fn seed_skew_uuid(db: &Database, matches: usize, total_for_s: usize) -> (Uuid, Uuid) {
    create_uuid_hop(db);
    let s = tid(0x500);
    let x = tid(0x501);
    for i in 0..total_for_s {
        let (col2, col3) = if i < matches {
            (x, "HOP".to_string())
        } else {
            (tid(0x600 + i as u128), format!("NO{i}"))
        };
        insert_uuid_hop(db, tid(0x800 + i as u128), s, col2, &col3);
    }
    for i in 0..5 {
        insert_uuid_hop(db, tid(0x20_000 + i), tid(0xB00 + i), x, "HOP");
    }
    (s, x)
}

#[test]
fn in01_composite_only_inlist_routes() {
    let db = Database::open_memory();
    create_three_col_int(&db);
    let mut n = 1;
    for col in 1..=5 {
        for _ in 0..3 {
            insert_int3(&db, tid(n), col, col * 10, col * 100);
            n += 1;
        }
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM t WHERE col IN (2, 4) ORDER BY col ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col"]);
    assert_eq!(
        r.rows,
        vec![vec![Value::Int64(2)]; 3]
            .into_iter()
            .chain(vec![vec![Value::Int64(4)]; 3])
            .collect::<Vec<_>>()
    );
    assert_eq!(db.__rows_examined(), 6);
}

#[test]
fn in02_inlist_plus_one_equality_pushes_two() {
    let db = Database::open_memory();
    seed_is02_fixture(&db);
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, col2 FROM t WHERE col IN (2, 3) AND col2 = 20 ORDER BY col ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(2), Value::Int64(20)],
            vec![Value::Int64(3), Value::Int64(20)]
        ]
    );
    assert_eq!(db.__rows_examined(), 2);
}

#[test]
fn in03_consumer_three_column_prefix_pushes_three() {
    let db = Database::open_memory();
    let (c2, c3, x, _) = seed_is03_fixture(&db);
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col3 FROM t WHERE col IN ($c2, $c3) AND col2 = $x AND col3 = $y",
            &params(vec![
                ("c2", Value::Uuid(c2)),
                ("c3", Value::Uuid(c3)),
                ("x", Value::Uuid(x)),
                ("y", Value::Text("Y".into())),
            ]),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2", "col3"]);
    assert_eq!(r.rows, vec![vec![Value::Text("Y".into())]; 2]);
    assert_eq!(db.__rows_examined(), 2);
}

#[test]
fn in04_composite_beats_leading_unique() {
    let db = Database::open_memory();
    seed_is04_fixture(&db);
    let r = is04_query(&db);
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2", "col3"]);
    assert_eq!(reason(&r, "__unique_col"), Some(FEWER));
    assert_eq!(r.rows, vec![vec![Value::Int64(2)]]);
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn in05_unrelated_index_on_suffix_col_explained() {
    let db = Database::open_memory();
    create_two_col_int(&db);
    db.execute("CREATE INDEX idx_only2 ON t (col2)", &empty())
        .unwrap();
    for i in 1..=10 {
        insert_int2(&db, tid(i as u128), i, i * 10);
    }
    db.__reset_rows_examined();
    let r = db
        .execute("SELECT col FROM t WHERE col IN (3) AND col2 = 30", &empty())
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert_eq!(reason(&r, "idx_only2"), Some(FEWER));
    assert_eq!(r.rows, vec![vec![Value::Int64(3)]]);
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn in06_inlist_single_col_index_routes() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_only ON t (col)", &empty())
        .unwrap();
    for col in 1..=5 {
        for k in 0..2 {
            db.execute(
                "INSERT INTO t (id, col) VALUES ($id, $col)",
                &params(vec![
                    ("id", Value::Uuid(tid((col * 10 + k) as u128))),
                    ("col", Value::Int64(col)),
                ]),
            )
            .unwrap();
        }
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM t WHERE col IN (2, 4) ORDER BY col ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_only"));
    assert_eq!(pushed(&r), vec!["col"]);
    assert_eq!(
        r.rows,
        vec![vec![Value::Int64(2)]; 2]
            .into_iter()
            .chain(vec![vec![Value::Int64(4)]; 2])
            .collect::<Vec<_>>()
    );
    assert_eq!(db.__rows_examined(), 4);
}

#[test]
fn in07_three_column_prefix_at_10k_inlist_uuid() {
    let db = Database::open_memory();
    create_uuid_hop(&db);
    let x = Uuid::from_u128(0xC7);
    for i in 0..10_000u128 {
        let col = Uuid::from_u128(i);
        let col2 = Uuid::from_u128(0xC0 + (i % 100));
        let tag = format!("T{}", i % 100);
        insert_uuid_hop(&db, tid(0x10_000 + i), col, col2, &tag);
    }
    let in_list = (0..10_000u128)
        .map(|i| format!("'{}'", Uuid::from_u128(i)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT col FROM t WHERE col IN ({in_list}) AND col2 = $x AND col3 = $y");
    db.__reset_rows_examined();
    let r = db
        .execute(
            &sql,
            &params(vec![("x", Value::Uuid(x)), ("y", Value::Text("T7".into()))]),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2", "col3"]);
    assert_eq!(r.rows.len(), 100);
    for row in &r.rows {
        let Value::Uuid(id) = row[0] else {
            panic!("expected uuid row: {row:?}")
        };
        assert_eq!(id.as_u128() % 100, 7);
    }
    assert_eq!(db.__rows_examined(), 100);
}

#[test]
fn in08_no_leading_match_scan_all_explained() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, tag TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_only2 ON t (col2)", &empty())
        .unwrap();
    for i in 0..6 {
        db.execute(
            "INSERT INTO t (id, col, col2, tag) VALUES ($id, $col, $col2, $tag)",
            &params(vec![
                ("id", Value::Uuid(tid(i))),
                ("col", Value::Int64(i as i64)),
                ("col2", Value::Int64(i as i64)),
                ("tag", Value::Text(format!("tag{i}"))),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT col FROM t WHERE tag = 'z'", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "Scan");
    assert_eq!(r.trace.index_used, None);
    assert_eq!(pushed(&r), Vec::<&str>::new());
    assert_eq!(reason(&r, "idx_c"), Some(FIRST));
    assert_eq!(reason(&r, "idx_only2"), Some(FIRST));
}

#[test]
fn in09_two_composites_suffix_match_decides() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, col3 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c2 ON t (col, col2)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_c3 ON t (col, col3)", &empty())
        .unwrap();
    for i in 1..=8 {
        insert_int3(&db, tid(i as u128), i, i * 10, i * 100);
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM t WHERE col IN (2) AND col3 = 200",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c3"));
    assert_eq!(pushed(&r), vec!["col", "col3"]);
    assert_eq!(reason(&r, "idx_c2"), Some(FEWER));
    assert_eq!(r.rows, vec![vec![Value::Int64(2)]]);
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn in10_equality_leading_pushes_both() {
    let db = Database::open_memory();
    create_two_col_int(&db);
    for (n, (col, col2)) in [(1, 10), (1, 20), (2, 10)].into_iter().enumerate() {
        insert_int2(&db, tid(n as u128), col, col2);
    }
    for i in 10..30 {
        insert_int2(&db, tid(0x1000 + i as u128), i, i);
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, col2 FROM t WHERE col = 1 AND col2 = 20",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert_eq!(r.rows, vec![vec![Value::Int64(1), Value::Int64(20)]]);
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn in11_suffix_range_terminates_prefix() {
    let db = Database::open_memory();
    create_two_col_int(&db);
    let mut n = 0;
    for col in 1..=4 {
        for col2 in [5, 15, 25] {
            insert_int2(&db, tid(n), col, col2);
            n += 1;
        }
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, col2 FROM t WHERE col IN (2, 3) AND col2 > 10 ORDER BY col ASC, col2 ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(pushed(&r), vec!["col"]);
    assert_eq!(db.__rows_examined(), 6);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(2), Value::Int64(15)],
            vec![Value::Int64(2), Value::Int64(25)],
            vec![Value::Int64(3), Value::Int64(15)],
            vec![Value::Int64(3), Value::Int64(25)]
        ]
    );
    db.__reset_rows_examined();
    let r_between = db
        .execute(
            "SELECT col, col2 FROM t WHERE col IN (2, 3) AND col2 BETWEEN 12 AND 28 ORDER BY col ASC, col2 ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(pushed(&r_between), vec!["col"]);
    assert_eq!(db.__rows_examined(), 6);
    assert_eq!(r_between.rows, r.rows);

    db.execute(
        "CREATE TABLE t2 (id UUID PRIMARY KEY, col INTEGER, col2 TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c2 ON t2 (col, col2)", &empty())
        .unwrap();
    for col in 1..=4 {
        for tag in ["alpha", "x1", "x2"] {
            db.execute(
                "INSERT INTO t2 (id, col, col2) VALUES ($id, $col, $tag)",
                &params(vec![
                    ("id", Value::Uuid(tid(0x2000 + n))),
                    ("col", Value::Int64(col)),
                    ("tag", Value::Text(tag.into())),
                ]),
            )
            .unwrap();
            n += 1;
        }
    }
    db.__reset_rows_examined();
    let r_like = db
        .execute(
            "SELECT col, col2 FROM t2 WHERE col IN (2, 3) AND col2 LIKE 'x%' ORDER BY col ASC, col2 ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r_like.trace.index_used.as_deref(), Some("idx_c2"));
    assert_eq!(pushed(&r_like), vec!["col"]);
    assert_eq!(db.__rows_examined(), 6);
    assert_eq!(
        r_like.rows,
        vec![
            vec![Value::Int64(2), Value::Text("x1".into())],
            vec![Value::Int64(2), Value::Text("x2".into())],
            vec![Value::Int64(3), Value::Text("x1".into())],
            vec![Value::Int64(3), Value::Text("x2".into())]
        ]
    );
}

#[test]
fn in12_leading_collision_pushes_col_once() {
    let db = Database::open_memory();
    create_two_col_int(&db);
    let mut n = 0;
    for col in [5, 7, 9] {
        for col2 in 0..4 {
            insert_int2(&db, tid(n), col, col2);
            n += 1;
        }
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM t WHERE col = 5 AND col IN (7, 9) ORDER BY col ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(pushed(&r), vec!["col"]);
    assert_eq!(db.__rows_examined(), 4);
    assert!(r.rows.is_empty());
    db.__reset_rows_examined();
    let tier = db
        .execute(
            "SELECT col FROM t WHERE col > 3 AND col = 5 ORDER BY col ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(pushed(&tier), vec!["col"]);
    assert_eq!(db.__rows_examined(), 4);
    assert_eq!(tier.rows, vec![vec![Value::Int64(5)]; 4]);
}

#[test]
fn in13_equal_matchcount_tie_creation_order() {
    let db = Database::open_memory();
    seed_is13_fixture(&db, false);
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM tie WHERE col IN (2) AND col2 > 10 ORDER BY col2 ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_long"));
    assert_eq!(pushed(&r), vec!["col"]);
    assert_eq!(reason(&r, "idx_short"), Some(TIED));
    assert_eq!(db.__rows_examined(), 3);
    assert_eq!(r.rows, vec![vec![Value::Int64(2)], vec![Value::Int64(2)]]);
}

#[test]
fn in13b_shorter_index_created_first_wins_tie() {
    let db = Database::open_memory();
    seed_is13_fixture(&db, true);
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM tie WHERE col IN (2) AND col2 > 10 ORDER BY col2 ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_short"));
    assert_eq!(pushed(&r), vec!["col"]);
    assert_eq!(reason(&r, "idx_long"), Some(TIED));
    assert_eq!(db.__rows_examined(), 3);
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn in14_contradictory_suffix_empty_zero_examined() {
    let db = Database::open_memory();
    let (c2, _, x, other) = seed_is03_fixture(&db);
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col3 FROM t WHERE col IN ($c2) AND col2 = $x AND col2 = $other",
            &params(vec![
                ("c2", Value::Uuid(c2)),
                ("x", Value::Uuid(x)),
                ("other", Value::Uuid(other)),
            ]),
        )
        .unwrap();
    assert!(r.rows.is_empty());
    assert_eq!(db.__rows_examined(), 0);
    db.__reset_rows_examined();
    let dedup = db
        .execute(
            "SELECT col3 FROM t WHERE col IN ($c2) AND col2 = $x AND col2 = $x AND col3 = $y",
            &params(vec![
                ("c2", Value::Uuid(c2)),
                ("x", Value::Uuid(x)),
                ("y", Value::Text("Y".into())),
            ]),
        )
        .unwrap();
    assert_eq!(dedup.rows, vec![vec![Value::Text("Y".into())]]);
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn in15_null_and_nan_suffix_bind_empty_zero_examined() {
    let db = Database::open_memory();
    let (c2, _, x, _) = seed_is03_fixture(&db);
    db.__reset_rows_examined();
    let null_suffix = db
        .execute(
            "SELECT col3 FROM t WHERE col IN ($c2) AND col2 = $x AND col3 = $y",
            &params(vec![
                ("c2", Value::Uuid(c2)),
                ("x", Value::Uuid(x)),
                ("y", Value::Null),
            ]),
        )
        .unwrap();
    assert!(null_suffix.rows.is_empty());
    assert_eq!(db.__rows_examined(), 0);
    db.__reset_rows_examined();
    let positive = db
        .execute(
            "SELECT col3 FROM t WHERE col IN ($c2) AND col2 = $x AND col3 = $y",
            &params(vec![
                ("c2", Value::Uuid(c2)),
                ("x", Value::Uuid(x)),
                ("y", Value::Text("Y".into())),
            ]),
        )
        .unwrap();
    assert_eq!(positive.rows, vec![vec![Value::Text("Y".into())]]);
    assert_eq!(db.__rows_examined(), 1);

    let dbf = Database::open_memory();
    dbf.execute(
        "CREATE TABLE tf (id UUID PRIMARY KEY, col INTEGER, f REAL)",
        &empty(),
    )
    .unwrap();
    dbf.execute("CREATE INDEX idx_cf ON tf (col, f)", &empty())
        .unwrap();
    for i in 1..=4 {
        dbf.execute(
            "INSERT INTO tf (id, col, f) VALUES ($id, $col, $f)",
            &params(vec![
                ("id", Value::Uuid(tid(0x3000 + i))),
                ("col", Value::Int64(i as i64)),
                ("f", Value::Float64(i as f64)),
            ]),
        )
        .unwrap();
    }
    dbf.__reset_rows_examined();
    let nan = dbf
        .execute(
            "SELECT col FROM tf WHERE col IN (1, 2) AND f = $nan",
            &params(vec![("nan", Value::Float64(f64::NAN))]),
        )
        .unwrap();
    assert!(nan.rows.is_empty());
    assert_eq!(dbf.__rows_examined(), 0);
}

#[test]
fn in15b_numeric_suffix_literal_matches_evaluator_semantics() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, f REAL)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_cf ON t (col, f)", &empty())
        .unwrap();
    let id = tid(0x3500);
    db.execute(
        "INSERT INTO t (id, col, f) VALUES ($id, $col, $f)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("col", Value::Int64(1)),
            ("f", Value::Float64(1.0)),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, col, f) VALUES ($id, $col, $f)",
        &params(vec![
            ("id", Value::Uuid(tid(0x3501))),
            ("col", Value::Int64(1)),
            ("f", Value::Float64(2.0)),
        ]),
    )
    .unwrap();
    db.__reset_rows_examined();
    let r = db
        .execute("SELECT id FROM t WHERE col = 1 AND f = 1", &empty())
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_cf"));
    assert_eq!(pushed(&r), vec!["col", "f"]);
    assert_eq!(r.rows, vec![vec![Value::Uuid(id)]]);
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn in16_skew_killshot_examines_full_key_not_leading() {
    let db = Database::open_memory();
    seed_skew_int(&db);
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM t WHERE col IN (500) AND col2 = 7 AND col3 = 9",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2", "col3"]);
    assert_eq!(r.rows, vec![vec![Value::Int64(500)]; 3]);
    assert_eq!(db.__rows_examined(), 3);
}

#[test]
fn in17_mixed_direction_composite_full_pushdown() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_md ON t (col ASC, col2 DESC)", &empty())
        .unwrap();
    let mut n = 0;
    for col in [1, 2, 3] {
        for col2 in [10, 20, 30] {
            insert_int2(&db, tid(n), col, col2);
            n += 1;
        }
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, col2 FROM t WHERE col IN (1, 3) AND col2 = 20",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_md"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(1), Value::Int64(20)],
            vec![Value::Int64(3), Value::Int64(20)]
        ]
    );
    assert_eq!(db.__rows_examined(), 2);
}

#[test]
fn deg1_inlist_longer_than_table_exact_examined() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_only ON t (col)", &empty())
        .unwrap();
    for col in 1..=3 {
        db.execute(
            "INSERT INTO t (id, col) VALUES ($id, $col)",
            &params(vec![
                ("id", Value::Uuid(tid(col as u128))),
                ("col", Value::Int64(col)),
            ]),
        )
        .unwrap();
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM t WHERE col IN (1,2,3,4,5,6,7,8,9,10) ORDER BY col ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_only"));
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Int64(3)]
        ]
    );
    assert_eq!(db.__rows_examined(), 3);
}

#[test]
fn deg2_all_inlist_absent_empty_zero_examined() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_only ON t (col)", &empty())
        .unwrap();
    for col in 1..=3 {
        db.execute(
            "INSERT INTO t (id, col) VALUES ($id, $col)",
            &params(vec![
                ("id", Value::Uuid(tid(col as u128))),
                ("col", Value::Int64(col)),
            ]),
        )
        .unwrap();
    }
    db.__reset_rows_examined();
    let r = db
        .execute("SELECT col FROM t WHERE col IN (100, 200, 300)", &empty())
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_only"));
    assert!(r.rows.is_empty());
    assert_eq!(db.__rows_examined(), 0);
}

#[test]
fn ord1_inlist_value_order_rowid_asc_within_group() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, seq INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2)", &empty())
        .unwrap();
    let mut seq = 1;
    for col in [5, 1, 3] {
        for _ in 0..3 {
            db.execute(
                "INSERT INTO t (id, col, col2, seq) VALUES ($id, $col, 0, $seq)",
                &params(vec![
                    ("id", Value::Uuid(tid(seq as u128))),
                    ("col", Value::Int64(col)),
                    ("seq", Value::Int64(seq)),
                ]),
            )
            .unwrap();
            seq += 1;
        }
        db.execute(
            "INSERT INTO t (id, col, col2, seq) VALUES ($id, $col, 99, $seq)",
            &params(vec![
                ("id", Value::Uuid(tid(seq as u128))),
                ("col", Value::Int64(col)),
                ("seq", Value::Int64(seq)),
            ]),
        )
        .unwrap();
        seq += 1;
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, seq FROM t WHERE col IN (3, 5) AND col2 = 0",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(3), Value::Int64(9)],
            vec![Value::Int64(3), Value::Int64(10)],
            vec![Value::Int64(3), Value::Int64(11)],
            vec![Value::Int64(5), Value::Int64(1)],
            vec![Value::Int64(5), Value::Int64(2)],
            vec![Value::Int64(5), Value::Int64(3)],
        ]
    );
    assert_eq!(db.__rows_examined(), 6);
}

#[test]
fn se_gain_composite_routing_gains_sort_elision() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_only ON t (col)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col ASC, col2 ASC)", &empty())
        .unwrap();
    for col2 in [10, 20, 30, 40] {
        insert_int2(&db, tid(col2 as u128), 3, col2);
    }
    for i in 0..6 {
        insert_int2(&db, tid(0x4000 + i), 10 + i as i64, i as i64);
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, col2 FROM t WHERE col = 3 AND col2 = 30 ORDER BY col ASC, col2 ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert!(r.trace.sort_elided);
    assert_eq!(reason(&r, "idx_only"), Some(FEWER));
    assert_eq!(r.rows, vec![vec![Value::Int64(3), Value::Int64(30)]]);
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn se_lose_composite_routing_loses_elision_rows_still_sorted() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, col3 INTEGER, payload INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_c_payload ON t (col, payload ASC)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c3 ON t (col, col2, col3)", &empty())
        .unwrap();
    for (n, payload) in [30, 10, 20].into_iter().enumerate() {
        db.execute(
            "INSERT INTO t (id, col, col2, col3, payload) VALUES ($id, 3, 30, 300, $payload)",
            &params(vec![
                ("id", Value::Uuid(tid(0x5000 + n as u128))),
                ("payload", Value::Int64(payload)),
            ]),
        )
        .unwrap();
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, payload FROM t WHERE col = 3 AND col2 = 30 AND col3 = 300 ORDER BY payload ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c3"));
    assert_eq!(pushed(&r), vec!["col", "col2", "col3"]);
    assert!(!r.trace.sort_elided);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(3), Value::Int64(10)],
            vec![Value::Int64(3), Value::Int64(20)],
            vec![Value::Int64(3), Value::Int64(30)],
        ]
    );
}

#[test]
fn se_dir_direction_mismatch_blocks_elision() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_only ON t (col)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col ASC, col2 ASC)", &empty())
        .unwrap();
    for col2 in [10, 20, 30, 40] {
        insert_int2(&db, tid(col2 as u128), 3, col2);
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, col2 FROM t WHERE col = 3 AND col2 = 30 ORDER BY col ASC, col2 DESC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert!(!r.trace.sort_elided);
    assert_eq!(r.rows, vec![vec![Value::Int64(3), Value::Int64(30)]]);
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn ov_sel_staged_suffix_violator_not_returned() {
    let db = Database::open_memory();
    create_two_col_int(&db);
    let a = tid(0xA1);
    let b = tid(0xB1);
    db.execute("BEGIN", &empty()).unwrap();
    insert_int2(&db, a, 5, 50);
    insert_int2(&db, b, 5, 99);
    let r = db
        .execute("SELECT id FROM t WHERE col IN (5) AND col2 = 50", &empty())
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert_eq!(r.rows, vec![vec![Value::Uuid(a)]]);
    db.execute("COMMIT", &empty()).unwrap();
    let after = db
        .execute("SELECT id FROM t WHERE col IN (5) AND col2 = 50", &empty())
        .unwrap();
    assert_eq!(after.rows, vec![vec![Value::Uuid(a)]]);
}

#[test]
fn ov_upd_staged_suffix_violator_not_updated() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, marked INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2)", &empty())
        .unwrap();
    let a = tid(0xA2);
    let b = tid(0xB2);
    db.execute("BEGIN", &empty()).unwrap();
    for (id, col2) in [(a, 50), (b, 99)] {
        db.execute(
            "INSERT INTO t (id, col, col2, marked) VALUES ($id, 5, $col2, 0)",
            &params(vec![("id", Value::Uuid(id)), ("col2", Value::Int64(col2))]),
        )
        .unwrap();
    }
    db.execute(
        "UPDATE t SET marked = 1 WHERE col IN (5) AND col2 = 50",
        &empty(),
    )
    .unwrap();
    let r = db
        .execute(
            "SELECT id, marked FROM t WHERE col IN (5) ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Uuid(a), Value::Int64(1)],
            vec![Value::Uuid(b), Value::Int64(0)]
        ]
    );
    db.execute("COMMIT", &empty()).unwrap();
    let committed = db
        .execute(
            "SELECT id, marked FROM t WHERE col IN (5) ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(committed.rows, r.rows);
}

#[test]
fn ov_del_staged_suffix_violator_not_deleted() {
    let db = Database::open_memory();
    create_two_col_int(&db);
    let a = tid(0xA3);
    let b = tid(0xB3);
    db.execute("BEGIN", &empty()).unwrap();
    insert_int2(&db, a, 5, 50);
    insert_int2(&db, b, 5, 99);
    db.execute("DELETE FROM t WHERE col IN (5) AND col2 = 50", &empty())
        .unwrap();
    let r = db
        .execute(
            "SELECT id FROM t WHERE col IN (5) ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::Uuid(b)]]);
    db.execute("COMMIT", &empty()).unwrap();
    let committed = db
        .execute(
            "SELECT id FROM t WHERE col IN (5) ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(committed.rows, vec![vec![Value::Uuid(b)]]);
}

#[test]
fn rid_sel_routed_equals_unindexed_scan() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE ti (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, col3 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON ti (col, col2, col3)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE tn (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, col3 INTEGER)",
        &empty(),
    )
    .unwrap();
    for i in 0..30 {
        let id = tid(0xD00 + i);
        let col = match i % 5 {
            0 => 2,
            1 => 3,
            2 => 5,
            _ => 9,
        };
        let col2 = if i % 3 == 0 { 20 } else { 21 };
        let col3 = if i % 4 == 0 { 200 } else { 201 };
        for table in ["ti", "tn"] {
            db.execute(
                &format!(
                    "INSERT INTO {table} (id, col, col2, col3) VALUES ($id, $col, $col2, $col3)"
                ),
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("col", Value::Int64(col)),
                    ("col2", Value::Int64(col2)),
                    ("col3", Value::Int64(col3)),
                ]),
            )
            .unwrap();
        }
    }
    let q = "SELECT id, col, col2, col3 FROM ti WHERE col IN (2, 3, 5) AND col2 = 20 AND col3 = 200 ORDER BY id ASC";
    let indexed = db.execute(q, &empty()).unwrap();
    let scan = db
        .execute(
            "SELECT id, col, col2, col3 FROM tn WHERE col IN (2, 3, 5) AND col2 = 20 AND col3 = 200 ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(indexed.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&indexed), vec!["col", "col2", "col3"]);
    assert_eq!(scan.trace.physical_plan, "Sort");
    assert_eq!(indexed.rows, scan.rows);
    let empty_i = db
        .execute(
            "SELECT id FROM ti WHERE col IN (2,3,5) AND col2 = -1 ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    let empty_n = db
        .execute(
            "SELECT id FROM tn WHERE col IN (2,3,5) AND col2 = -1 ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert!(empty_i.rows.is_empty());
    assert_eq!(empty_i.rows, empty_n.rows);
}

#[test]
fn in16b_skew_killshot_uuid_text_suffix() {
    let db = Database::open_memory();
    let (s, x) = seed_skew_uuid(&db, 3, 1000);
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col3 FROM t WHERE col IN ($s) AND col2 = $x AND col3 = $hop",
            &params(vec![
                ("s", Value::Uuid(s)),
                ("x", Value::Uuid(x)),
                ("hop", Value::Text("HOP".into())),
            ]),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col", "col2", "col3"]);
    assert_eq!(r.rows, vec![vec![Value::Text("HOP".into())]; 3]);
    assert_eq!(db.__rows_examined(), 3);
}

#[test]
fn isn1_isnull_leading_composite_returns_correct_rows_and_pushes_suffix() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE tn (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
        &empty(),
    )
    .unwrap();
    let mut n = 0;
    for col2 in [10, 20, 30, 40] {
        for table in ["t", "tn"] {
            db.execute(
                &format!("INSERT INTO {table} (id, col, col2) VALUES ($id, $col, $col2)"),
                &params(vec![
                    ("id", Value::Uuid(tid(0x900 + n))),
                    ("col", Value::Null),
                    ("col2", Value::Int64(col2)),
                ]),
            )
            .unwrap();
        }
        n += 1;
    }
    for i in 0..6 {
        for table in ["t", "tn"] {
            db.execute(
                &format!("INSERT INTO {table} (id, col, col2) VALUES ($id, $col, $col2)"),
                &params(vec![
                    ("id", Value::Uuid(tid(0xA00 + n))),
                    ("col", Value::Int64(i)),
                    ("col2", Value::Int64(100 + i)),
                ]),
            )
            .unwrap();
        }
        n += 1;
    }
    db.__reset_rows_examined();
    let indexed = db
        .execute(
            "SELECT col2 FROM t WHERE col IS NULL AND col2 = 20",
            &empty(),
        )
        .unwrap();
    let indexed_examined = db.__rows_examined();
    let scan = db
        .execute(
            "SELECT col2 FROM tn WHERE col IS NULL AND col2 = 20",
            &empty(),
        )
        .unwrap();
    assert_eq!(indexed.rows, vec![vec![Value::Int64(20)]]);
    assert_eq!(indexed.rows, scan.rows);
    assert_eq!(indexed.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&indexed), vec!["col", "col2"]);
    assert_eq!(indexed_examined, 1);
}

#[test]
fn or1_or_connected_suffix_stays_residual() {
    let db = Database::open_memory();
    seed_is02_fixture(&db);
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col, col2 FROM t WHERE col IN (2, 3) AND (col2 = 10 OR col2 = 30) ORDER BY col ASC, col2 ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
    assert_eq!(pushed(&r), vec!["col"]);
    assert_eq!(db.__rows_examined(), 6);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(2), Value::Int64(10)],
            vec![Value::Int64(2), Value::Int64(30)],
            vec![Value::Int64(3), Value::Int64(10)],
            vec![Value::Int64(3), Value::Int64(30)]
        ]
    );
}

#[test]
fn sel1_composite_beats_unique_winner_only() {
    let db = Database::open_memory();
    seed_is04_fixture(&db);
    let r = is04_query(&db);
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_c"));
}

#[test]
fn psh1_two_column_prefix_pushed_only() {
    let db = Database::open_memory();
    seed_is02_fixture(&db);
    let r = db
        .execute(
            "SELECT col, col2 FROM t WHERE col IN (2, 3) AND col2 = 20 ORDER BY col ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(pushed(&r), vec!["col", "col2"]);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(2), Value::Int64(20)],
            vec![Value::Int64(3), Value::Int64(20)]
        ]
    );
}

#[test]
fn det1_repeat_determinism_three_runs() {
    let db = Database::open_memory();
    create_three_col_int(&db);
    for i in 0..30 {
        insert_int3(
            &db,
            tid(0xE00 + i),
            (i % 5) as i64,
            if i % 3 == 0 { 20 } else { 21 },
            if i % 4 == 0 { 200 } else { 201 },
        );
    }
    let mut captures = Vec::new();
    for _ in 0..3 {
        db.__reset_rows_examined();
        let r = db
            .execute(
                "SELECT id, col, col2, col3 FROM t WHERE col IN (2, 3, 5) AND col2 = 20 AND col3 = 200 ORDER BY id ASC",
                &empty(),
            )
            .unwrap();
        captures.push(result_capture(r, db.__rows_examined()));
    }
    assert_eq!(captures[0], captures[1]);
    assert_eq!(captures[1], captures[2]);
}

#[test]
fn rsn_fewer_columns_exact_string() {
    let db = Database::open_memory();
    seed_is04_fixture(&db);
    let r = is04_query(&db);
    assert_eq!(reason(&r, "__unique_col"), Some(FEWER));
}

#[test]
fn rsn_first_col_not_in_where_exact_string() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, tag TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_c ON t (col, col2)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_tag ON t (tag)", &empty())
        .unwrap();
    db.execute(
        "INSERT INTO t (id, col, col2, tag) VALUES ($id, 3, 30, 'x')",
        &params(vec![("id", Value::Uuid(tid(1)))]),
    )
    .unwrap();
    let r = db
        .execute("SELECT col FROM t WHERE col IN (3) AND col2 = 30", &empty())
        .unwrap();
    assert_eq!(reason(&r, "idx_tag"), Some(FIRST));
}

#[test]
fn rsn_lower_selectivity_exact_string() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (col)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_b ON t (col2)", &empty())
        .unwrap();
    for i in 0..10 {
        insert_int2(&db, tid(i), if i == 5 { 5 } else { i as i64 }, i as i64);
    }
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT col FROM t WHERE col = 5 AND col2 BETWEEN 1 AND 100",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_a"));
    assert_eq!(reason(&r, "idx_b"), Some(LOWER));
    assert_eq!(db.__rows_examined(), 1);
}

#[test]
fn rsn_tied_creation_order_exact_string() {
    let db = Database::open_memory();
    seed_is13_fixture(&db, false);
    let r = db
        .execute(
            "SELECT col FROM tie WHERE col IN (2) AND col2 > 10",
            &empty(),
        )
        .unwrap();
    assert_eq!(reason(&r, "idx_short"), Some(TIED));
}

#[test]
fn lc_upsert_then_requery_examined_contract() {
    let db = Database::open_memory();
    let (s, x) = seed_skew_uuid(&db, 3, 5);
    let rekeyed = tid(0x800);
    db.execute(
        "INSERT INTO t (id, col, col2, col3) VALUES ($id, $s, $newx, 'HOP') ON CONFLICT (id) DO UPDATE SET col2 = $newx",
        &params(vec![
            ("id", Value::Uuid(rekeyed)),
            ("s", Value::Uuid(s)),
            ("newx", Value::Uuid(tid(0x999))),
        ]),
    )
    .unwrap();
    db.__reset_rows_examined();
    let old_key = db
        .execute(
            "SELECT id FROM t WHERE col IN ($s) AND col2 = $x AND col3 = $hop ORDER BY id ASC",
            &params(vec![
                ("s", Value::Uuid(s)),
                ("x", Value::Uuid(x)),
                ("hop", Value::Text("HOP".into())),
            ]),
        )
        .unwrap();
    assert_eq!(
        old_key.rows,
        vec![vec![Value::Uuid(tid(0x801))], vec![Value::Uuid(tid(0x802))]]
    );
    assert_eq!(db.__rows_examined(), 3);
    let new_key = db
        .execute(
            "SELECT id FROM t WHERE col IN ($s) AND col2 = $newx AND col3 = $hop",
            &params(vec![
                ("s", Value::Uuid(s)),
                ("newx", Value::Uuid(tid(0x999))),
                ("hop", Value::Text("HOP".into())),
            ]),
        )
        .unwrap();
    assert_eq!(new_key.rows, vec![vec![Value::Uuid(rekeyed)]]);
}

#[test]
fn lc_delete_then_requery_tombstone_examined() {
    let db = Database::open_memory();
    let (s, x) = seed_skew_uuid(&db, 4, 4);
    db.execute(
        "DELETE FROM t WHERE id = $id",
        &params(vec![("id", Value::Uuid(tid(0x800)))]),
    )
    .unwrap();
    db.__reset_rows_examined();
    let r = db
        .execute(
            "SELECT id FROM t WHERE col IN ($s) AND col2 = $x AND col3 = $hop ORDER BY id ASC",
            &params(vec![
                ("s", Value::Uuid(s)),
                ("x", Value::Uuid(x)),
                ("hop", Value::Text("HOP".into())),
            ]),
        )
        .unwrap();
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Uuid(tid(0x801))],
            vec![Value::Uuid(tid(0x802))],
            vec![Value::Uuid(tid(0x803))]
        ]
    );
    assert_eq!(db.__rows_examined(), 4);
}

#[test]
fn pers1_reopen_winner_and_reasons_stable() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("pers.contextdb");
    let before = {
        let db = Database::open(&path).unwrap();
        seed_is04_fixture(&db);
        seed_is13_fixture(&db, false);
        let first = is04_query(&db);
        let second = db
            .execute(
                "SELECT col FROM tie WHERE col IN (2) AND col2 > 10",
                &empty(),
            )
            .unwrap();
        (trace_capture(&first), trace_capture(&second))
    };
    let db = Database::open(&path).unwrap();
    let first = is04_query(&db);
    let second = db
        .execute(
            "SELECT col FROM tie WHERE col IN (2) AND col2 > 10",
            &empty(),
        )
        .unwrap();
    let after = (trace_capture(&first), trace_capture(&second));
    assert_eq!(before, after);
    assert_eq!(after.0.0.as_deref(), Some("idx_c"));
    assert_eq!(after.1.0.as_deref(), Some("idx_long"));
}

#[test]
fn sync1_peer_routes_identical_winner() {
    let source = Database::open_memory();
    seed_is04_fixture(&source);
    let primary = is04_query(&source);
    let peer = Database::open_memory();
    peer.apply_changes(
        source.changes_since(Lsn(0)),
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .unwrap();
    let replica = is04_query(&peer);
    assert_eq!(primary.trace.index_used, replica.trace.index_used);
    assert_eq!(pushed(&primary), pushed(&replica));
    assert_eq!(
        reason(&primary, "__unique_col"),
        reason(&replica, "__unique_col")
    );
    assert_eq!(replica.trace.index_used.as_deref(), Some("idx_c"));
}

#[test]
fn scope1_scoped_handle_suffix_pushdown_examined_counts_hidden() {
    let admin = Database::open_memory();
    admin
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, col INTEGER, col2 INTEGER, col3 INTEGER, context_id UUID CONTEXT_ID)",
            &empty(),
        )
        .unwrap();
    admin
        .execute("CREATE INDEX idx_c ON t (col, col2, col3)", &empty())
        .unwrap();
    let ctx_a = tid(0xCA);
    let ctx_b = tid(0xCB);
    for i in 0..1000 {
        let (col2, col3, ctx) = match i {
            0 | 1 => (7, 9, ctx_a),
            2 => (7, 9, ctx_b),
            _ => (100 + i as i64, 200 + i as i64, ctx_a),
        };
        admin
            .execute(
                "INSERT INTO t (id, col, col2, col3, context_id) VALUES ($id, 500, $col2, $col3, $ctx)",
                &params(vec![
                    ("id", Value::Uuid(tid(0xB000 + i))),
                    ("col2", Value::Int64(col2)),
                    ("col3", Value::Int64(col3)),
                    ("ctx", Value::Uuid(ctx)),
                ]),
            )
            .unwrap();
    }
    let scoped = admin.scoped_with_contexts(BTreeSet::from([ContextId::new(ctx_a)]));
    scoped.__reset_rows_examined();
    let r = scoped
        .execute(
            "SELECT id FROM t WHERE col IN (500) AND col2 = 7 AND col3 = 9 ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(pushed(&r), vec!["col", "col2", "col3"]);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Uuid(tid(0xB000))],
            vec![Value::Uuid(tid(0xB001))]
        ]
    );
    assert_eq!(scoped.__rows_examined(), 3);
    let all = admin
        .execute(
            "SELECT id FROM t WHERE col IN (500) AND col2 = 7 AND col3 = 9 ORDER BY id ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        all.rows,
        vec![
            vec![Value::Uuid(tid(0xB000))],
            vec![Value::Uuid(tid(0xB001))],
            vec![Value::Uuid(tid(0xB002))]
        ]
    );
}
