use contextdb_core::{
    ColumnDef, ColumnType, CompositeForeignKey, Error, ForeignKeyReference, Lsn,
    SingleColumnForeignKey, TableMeta, Value,
};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
};
use contextdb_server::error::SyncError;
use contextdb_server::protocol::{Envelope, MessageType, WireDdlChange, decode, encode};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn db() -> Database {
    Database::open_memory()
}

fn file_db(name: &str) -> (TempDir, PathBuf, Database) {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join(name);
    let db = Database::open(&path).unwrap();
    (tmp, path, db)
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap()
        .rows
        .len()
}

fn persisted_row_count(path: &Path, table: &str) -> usize {
    let db = Database::open(path).unwrap();
    row_count(&db, table)
}

fn persisted_rows(path: &Path, sql: &str) -> Vec<Vec<Value>> {
    let db = Database::open(path).unwrap();
    db.execute(sql, &p()).unwrap().rows
}

fn create_parent_child(db: &Database) {
    db.execute(
        "CREATE TABLE parent (a INTEGER, b INTEGER, payload TEXT, UNIQUE(a, b))",
        &p(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, note TEXT, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
        &p(),
    )
    .unwrap();
}

fn insert_parent(db: &Database, a: i64, b: i64) {
    db.execute(
        &format!("INSERT INTO parent (a, b, payload) VALUES ({a}, {b}, 'p')"),
        &p(),
    )
    .unwrap();
}

fn insert_child(db: &Database, id: i64, c1: &str, c2: &str) -> Result<(), Error> {
    db.execute(
        &format!("INSERT INTO child (id, c1, c2, note) VALUES ({id}, {c1}, {c2}, 'c')"),
        &p(),
    )
    .map(|_| ())
}

fn expect_fk_violation(result: Result<(), Error>, label: &str) {
    expect_fk_violation_on(result, label, "child", &["c1", "c2"], "parent", &["a", "b"]);
}

fn expect_fk_violation_on(
    result: Result<(), Error>,
    label: &str,
    expected_child_table: &str,
    expected_child_columns: &[&str],
    expected_parent_table: &str,
    expected_parent_columns: &[&str],
) {
    let expected_child_columns = expected_child_columns
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>();
    let expected_parent_columns = expected_parent_columns
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>();
    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                ref child_table,
                ref child_columns,
                ref parent_table,
                ref parent_columns,
                ..
            }) if child_table == expected_child_table
                && child_columns == &expected_child_columns
                && parent_table == expected_parent_table
                && parent_columns == &expected_parent_columns
        ),
        "{label} must return FK violation on {expected_child_table}({}) -> {expected_parent_table}({}), got {result:?}",
        expected_child_columns.join(", "),
        expected_parent_columns.join(", ")
    );
}

fn assert_child_descriptor(db: &Database, table: &str, expected: CompositeForeignKey) {
    let meta = db.table_meta(table).expect("table metadata");
    assert!(
        meta.composite_foreign_keys.contains(&expected),
        "{table} missing composite FK descriptor {expected:?}; got {:?}",
        meta.composite_foreign_keys
    );
}

fn cfk(child: &[&str], parent_table: &str, parent: &[&str]) -> CompositeForeignKey {
    CompositeForeignKey {
        child_columns: child.iter().map(|s| s.to_string()).collect(),
        parent_table: parent_table.to_string(),
        parent_columns: parent.iter().map(|s| s.to_string()).collect(),
    }
}

fn apply_policy() -> ConflictPolicies {
    ConflictPolicies::uniform(ConflictPolicy::LatestWins)
}

#[test]
fn ddl_register_2x2() {
    let db = db();
    create_parent_child(&db);
    assert_child_descriptor(&db, "child", cfk(&["c1", "c2"], "parent", &["a", "b"]));
}

#[test]
fn ddl_register_3x3() {
    let db = db();
    db.execute(
        "CREATE TABLE parent3 (a INTEGER, b INTEGER, c INTEGER, UNIQUE(a, b, c))",
        &p(),
    )
    .unwrap();
    db.execute("CREATE TABLE child3 (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, z INTEGER, FOREIGN KEY (x, y, z) REFERENCES parent3(a, b, c))", &p()).unwrap();
    assert_child_descriptor(
        &db,
        "child3",
        cfk(&["x", "y", "z"], "parent3", &["a", "b", "c"]),
    );
}

#[test]
fn ddl_register_two_fks_one_table() {
    let db = db();
    db.execute("CREATE TABLE p1 (a INTEGER, b INTEGER, UNIQUE(a, b))", &p())
        .unwrap();
    db.execute("CREATE TABLE p2 (a INTEGER, b INTEGER, UNIQUE(a, b))", &p())
        .unwrap();
    db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, x1 INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER, FOREIGN KEY (x1, y1) REFERENCES p1(a, b), FOREIGN KEY (x2, y2) REFERENCES p2(a, b))", &p()).unwrap();
    let meta = db.table_meta("c").unwrap();
    assert_eq!(
        meta.composite_foreign_keys.len(),
        2,
        "two FK descriptors must be recorded"
    );
}

#[test]
fn ddl_register_self_ref() {
    let db = db();
    db.execute("CREATE TABLE node (a INTEGER, b INTEGER, pa INTEGER, pb INTEGER, UNIQUE(a, b), FOREIGN KEY (pa, pb) REFERENCES node(a, b))", &p()).unwrap();
    assert_child_descriptor(&db, "node", cfk(&["pa", "pb"], "node", &["a", "b"]));
}

#[test]
fn ddl_register_mixed_nullability_density() {
    let db = db();
    db.execute("CREATE TABLE p (a INTEGER, b INTEGER, UNIQUE(a, b))", &p())
        .unwrap();
    db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n1 INTEGER NULL, n2 INTEGER NULL, r1 INTEGER NOT NULL, r2 INTEGER NOT NULL, FOREIGN KEY (n1, n2) REFERENCES p(a, b), FOREIGN KEY (r1, r2) REFERENCES p(a, b), FOREIGN KEY (n1, r2) REFERENCES p(a, b), FOREIGN KEY (r1, n2) REFERENCES p(a, b), FOREIGN KEY (n2, n1) REFERENCES p(a, b), FOREIGN KEY (r2, r1) REFERENCES p(a, b))", &p()).unwrap();
    let meta = db.table_meta("c").unwrap();
    assert!(
        meta.columns
            .iter()
            .find(|c| c.name == "n1")
            .unwrap()
            .nullable
    );
    assert!(
        !meta
            .columns
            .iter()
            .find(|c| c.name == "r1")
            .unwrap()
            .nullable
    );
    assert_eq!(
        meta.composite_foreign_keys.len(),
        6,
        "six composite FK descriptors must be recorded"
    );
}

macro_rules! ddl_reject_test {
    ($name:ident, $sql:expr) => {
        #[test]
        fn $name() {
            let db = db();
            db.execute(
                "CREATE TABLE parent (a INTEGER, b INTEGER, UNIQUE(a, b))",
                &p(),
            )
            .unwrap();
            let result = db.execute($sql, &p());
            assert!(
                result.is_err(),
                "invalid composite FK DDL must be rejected, got {result:?}"
            );
        }
    };
}

ddl_reject_test!(
    ddl_reject_arity_mismatch,
    "CREATE TABLE c (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a))"
);
ddl_reject_test!(
    ddl_reject_unknown_child_column,
    "CREATE TABLE c (id INTEGER PRIMARY KEY, c1 INTEGER, FOREIGN KEY (c1, missing) REFERENCES parent(a, b))"
);
ddl_reject_test!(
    ddl_reject_unknown_parent_table,
    "CREATE TABLE c (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES missing(a, b))"
);
ddl_reject_test!(
    ddl_reject_unknown_parent_column,
    "CREATE TABLE c (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, missing))"
);
#[test]
fn ddl_reject_not_key_covered() {
    let db = db();
    db.execute("CREATE TABLE parent (a INTEGER, b INTEGER)", &p())
        .unwrap();
    let result = db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
        &p(),
    );
    assert!(
        result.is_err(),
        "composite FK parent tuple must be covered by PK or ordered UNIQUE, got {result:?}"
    );
}

ddl_reject_test!(
    ddl_reject_permutation_not_covered,
    "CREATE TABLE c (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(b, a))"
);
ddl_reject_test!(
    ddl_reject_duplicate_child_column,
    "CREATE TABLE c (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c1) REFERENCES parent(a, b))"
);

#[test]
fn ddl_reopen_preserves_descriptor() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cfk.db");
    {
        let db = Database::open(&path).unwrap();
        create_parent_child(&db);
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    assert_child_descriptor(
        &reopened,
        "child",
        cfk(&["c1", "c2"], "parent", &["a", "b"]),
    );
}

#[test]
fn ddl_reopen_two_fks_per_table() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cfk.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE p1 (a INTEGER, b INTEGER, UNIQUE(a, b))", &p())
            .unwrap();
        db.execute("CREATE TABLE p2 (a INTEGER, b INTEGER, UNIQUE(a, b))", &p())
            .unwrap();
        db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, x1 INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER, FOREIGN KEY (x1, y1) REFERENCES p1(a, b), FOREIGN KEY (x2, y2) REFERENCES p2(a, b))", &p()).unwrap();
        db.close().unwrap();
    }
    assert_eq!(
        Database::open(&path)
            .unwrap()
            .table_meta("c")
            .unwrap()
            .composite_foreign_keys
            .len(),
        2
    );
}

#[test]
fn ddl_reopen_self_ref() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cfk.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE node (a INTEGER, b INTEGER, pa INTEGER, pb INTEGER, UNIQUE(a, b), FOREIGN KEY (pa, pb) REFERENCES node(a, b))", &p()).unwrap();
        db.close().unwrap();
    }
    assert_child_descriptor(
        &Database::open(&path).unwrap(),
        "node",
        cfk(&["pa", "pb"], "node", &["a", "b"]),
    );
}

#[test]
fn ddl_estimated_bytes_grows_with_each_descriptor_kind() {
    let base = TableMeta::default();
    let mut with_unique = base.clone();
    with_unique
        .unique_constraints
        .push(vec!["a".into(), "b".into()]);
    assert!(with_unique.estimated_bytes() > base.estimated_bytes());

    let mut with_single_fk = base.clone();
    with_single_fk.columns.push(ColumnDef {
        name: "c1".into(),
        column_type: ColumnType::Integer,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
        references: Some(ForeignKeyReference {
            table: "p".into(),
            column: "a".into(),
        }),
        expires: false,
        immutable: false,
        quantization: Default::default(),
        rank_policy: None,
        context_id: false,
        scope_label: None,
        acl_ref: None,
    });
    assert!(with_single_fk.estimated_bytes() > base.estimated_bytes());

    let mut with_composite_fk = base.clone();
    with_composite_fk
        .composite_foreign_keys
        .push(cfk(&["c1", "c2"], "p", &["a", "b"]));
    assert!(
        with_composite_fk.estimated_bytes() > base.estimated_bytes(),
        "composite FK descriptors must be accounted"
    );
}

#[test]
fn cfk_insert_parent_visible_then_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cfk.db");
    {
        let db = Database::open(&path).unwrap();
        create_parent_child(&db);
        insert_parent(&db, 1, 2);
        insert_child(&db, 1, "1", "2").unwrap();
        db.close().unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    assert_eq!(row_count(&reopened, "child"), 1);
}

#[test]
fn cfk_insert_rejects_missing_parent() {
    let (_tmp, path, db) = file_db("missing-parent.db");
    create_parent_child(&db);
    expect_fk_violation(insert_child(&db, 1, "9", "9"), "missing parent insert");
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "child"), 0);
}

#[test]
fn cfk_insert_rejects_partial_parent_match() {
    let (_tmp, path, db) = file_db("partial-parent.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    expect_fk_violation(insert_child(&db, 1, "1", "9"), "partial parent insert");
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "parent"), 1);
    assert_eq!(persisted_row_count(&path, "child"), 0);
}

#[test]
fn cfk_insert_sametx_parent_either_order() {
    let (_tmp, path, db) = file_db("same-tx-parent.db");
    create_parent_child(&db);
    let tx = db.begin();
    db.execute_in_tx(tx, "INSERT INTO child (id, c1, c2) VALUES (1, 1, 2)", &p())
        .unwrap();
    db.execute_in_tx(tx, "INSERT INTO parent (a, b) VALUES (1, 2)", &p())
        .unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "child"), 1);
}

#[test]
fn cfk_insert_self_ref_within_one_tx() {
    let (_tmp, path, db) = file_db("self-ref-same-tx.db");
    db.execute("CREATE TABLE node (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, pa INTEGER, pb INTEGER, UNIQUE(a, b), FOREIGN KEY (pa, pb) REFERENCES node(a, b))", &p()).unwrap();
    let tx = db.begin();
    db.execute_in_tx(tx, "INSERT INTO node (id, a, b) VALUES (1, 1, 2)", &p())
        .unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO node (id, a, b, pa, pb) VALUES (2, 3, 4, 1, 2)",
        &p(),
    )
    .unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "node"), 2);
}

#[test]
fn cfk_insert_self_ref_own_key() {
    let (_tmp, path, db) = file_db("self-ref-own-key.db");
    db.execute("CREATE TABLE node (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, pa INTEGER, pb INTEGER, UNIQUE(a, b), FOREIGN KEY (pa, pb) REFERENCES node(a, b))", &p()).unwrap();
    db.execute(
        "INSERT INTO node (id, a, b, pa, pb) VALUES (1, 1, 2, 1, 2)",
        &p(),
    )
    .unwrap();
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "node"), 1);
}

#[test]
fn cfk_self_ref_negative_dangling() {
    let (_tmp, path, db) = file_db("self-ref-dangling.db");
    db.execute("CREATE TABLE node (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, pa INTEGER, pb INTEGER, UNIQUE(a, b), FOREIGN KEY (pa, pb) REFERENCES node(a, b))", &p()).unwrap();
    let result = db
        .execute(
            "INSERT INTO node (id, a, b, pa, pb) VALUES (1, 1, 2, 9, 9)",
            &p(),
        )
        .map(|_| ());
    expect_fk_violation_on(
        result,
        "dangling self ref",
        "node",
        &["pa", "pb"],
        "node",
        &["a", "b"],
    );
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "node"), 0);
}

#[test]
fn cfk_insert_null_tuple_bypasses_with_real_parent_misses() {
    let (_tmp, path, db) = file_db("null-bypass.db");
    create_parent_child(&db);
    insert_child(&db, 1, "NULL", "99").unwrap();
    expect_fk_violation(insert_child(&db, 2, "99", "99"), "paired non-null miss");
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "child"), 1);
}

#[test]
fn cfk_insert_full_null_tuple_bypasses() {
    let (_tmp, path, db) = file_db("full-null-bypass.db");
    create_parent_child(&db);
    insert_child(&db, 1, "NULL", "NULL").unwrap();
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "child"), 1);
}

#[test]
fn cfk_update_changes_fk_tuple_validates_at_commit() {
    let (_tmp, path, db) = file_db("update-valid.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    insert_parent(&db, 3, 4);
    insert_child(&db, 1, "1", "2").unwrap();
    db.execute("UPDATE child SET c1 = 3, c2 = 4 WHERE id = 1", &p())
        .unwrap();
    db.close().unwrap();
    assert_eq!(
        persisted_rows(&path, "SELECT c1, c2 FROM child WHERE id = 1"),
        vec![vec![Value::Int64(3), Value::Int64(4)]]
    );
}

#[test]
fn cfk_update_rejects_missing_parent() {
    let (_tmp, path, db) = file_db("update-missing-parent.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    insert_child(&db, 1, "1", "2").unwrap();
    expect_fk_violation(
        db.execute("UPDATE child SET c1 = 9, c2 = 9 WHERE id = 1", &p())
            .map(|_| ()),
        "missing parent update",
    );
    let rows = db
        .execute("SELECT c1, c2 FROM child WHERE id = 1", &p())
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![Value::Int64(1), Value::Int64(2)]]);
    db.close().unwrap();
    assert_eq!(
        persisted_rows(&path, "SELECT c1, c2 FROM child WHERE id = 1"),
        vec![vec![Value::Int64(1), Value::Int64(2)]]
    );
}

#[test]
fn cfk_update_non_fk_columns_does_not_fire_check() {
    let (_tmp, path, db) = file_db("update-non-fk.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    insert_child(&db, 1, "1", "2").unwrap();
    db.execute("UPDATE child SET note = 'updated' WHERE id = 1", &p())
        .unwrap();
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "child"), 1);
}

#[test]
fn cfk_delete_parent_with_children_rejects() {
    let (_tmp, path, db) = file_db("delete-parent-rejects.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    insert_child(&db, 1, "1", "2").unwrap();
    expect_fk_violation(
        db.execute("DELETE FROM parent WHERE a = 1 AND b = 2", &p())
            .map(|_| ()),
        "parent delete",
    );
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "parent"), 1);
    assert_eq!(persisted_row_count(&path, "child"), 1);
}

#[test]
fn cfk_delete_parent_no_children_commits() {
    let (_tmp, path, db) = file_db("delete-parent-ok.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    db.execute("DELETE FROM parent WHERE a = 1 AND b = 2", &p())
        .unwrap();
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "parent"), 0);
}

#[test]
fn cfk_delete_race_serializes_with_barrier() {
    let (_tmp, path, db) = file_db("delete-race.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    let child_tx = db.begin();
    let delete_tx = db.begin();
    db.execute_in_tx(
        child_tx,
        "INSERT INTO child (id, c1, c2) VALUES (1, 1, 2)",
        &p(),
    )
    .unwrap();
    db.execute_in_tx(delete_tx, "DELETE FROM parent WHERE a = 1 AND b = 2", &p())
        .unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let db_ref = &db;
    thread::scope(|scope| {
        let child_barrier = Arc::clone(&barrier);
        let child_db = db_ref;
        let child = scope.spawn(move || {
            child_barrier.wait();
            child_db.commit(child_tx)
        });
        let delete_barrier = Arc::clone(&barrier);
        let delete_db = db_ref;
        let delete = scope.spawn(move || {
            delete_barrier.wait();
            delete_db.commit(delete_tx)
        });
        barrier.wait();
        let child_result = child.join().unwrap();
        let delete_result = delete.join().unwrap();
        assert!(
            child_result.is_ok() ^ delete_result.is_ok(),
            "exactly one side of the FK race may commit; child={child_result:?}, delete={delete_result:?}"
        );
        if child_result.is_err() {
            expect_fk_violation(child_result, "delete race child loser");
        }
        if delete_result.is_err() {
            expect_fk_violation(delete_result, "delete race delete loser");
        }
    });
    db.close().unwrap();
    let parent_count = persisted_row_count(&path, "parent");
    let child_count = persisted_row_count(&path, "child");
    assert_eq!(
        parent_count, child_count,
        "post-race persistence must not contain a dangling child"
    );
}

#[test]
fn cfk_concurrent_child_inserts_against_live_parent_both_commit() {
    let db = db();
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    let tx1 = db.begin();
    let tx2 = db.begin();
    db.execute_in_tx(tx1, "INSERT INTO child (id, c1, c2) VALUES (1, 1, 2)", &p())
        .unwrap();
    db.execute_in_tx(tx2, "INSERT INTO child (id, c1, c2) VALUES (2, 1, 2)", &p())
        .unwrap();
    db.commit(tx1).unwrap();
    db.commit(tx2).unwrap();
    assert_eq!(row_count(&db, "child"), 2);
}

#[test]
fn cfk_netzero_child_insert_then_delete_same_tx() {
    let (_tmp, path, db) = file_db("netzero-child.db");
    create_parent_child(&db);
    let tx = db.begin();
    db.execute_in_tx(tx, "INSERT INTO child (id, c1, c2) VALUES (1, 9, 9)", &p())
        .unwrap();
    db.execute_in_tx(tx, "DELETE FROM child WHERE id = 1", &p())
        .unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "child"), 0);
}

#[test]
fn cfk_netzero_parent_only_no_sibling_break() {
    let (_tmp, path, db) = file_db("netzero-parent-only.db");
    create_parent_child(&db);
    let tx = db.begin();
    db.execute_in_tx(tx, "INSERT INTO parent (a, b) VALUES (1, 2)", &p())
        .unwrap();
    db.execute_in_tx(tx, "DELETE FROM parent WHERE a = 1 AND b = 2", &p())
        .unwrap();
    db.execute_in_tx(tx, "INSERT INTO child (id, c1, c2) VALUES (1, 9, 9)", &p())
        .unwrap();
    expect_fk_violation(db.commit(tx), "netzero parent with sibling miss");
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "parent"), 0);
    assert_eq!(persisted_row_count(&path, "child"), 0);
}

#[test]
fn cfk_netzero_parent_and_child_rejects() {
    let (_tmp, path, db) = file_db("netzero-parent-child.db");
    create_parent_child(&db);
    let tx = db.begin();
    db.execute_in_tx(tx, "INSERT INTO parent (a, b) VALUES (1, 2)", &p())
        .unwrap();
    db.execute_in_tx(tx, "INSERT INTO child (id, c1, c2) VALUES (1, 1, 2)", &p())
        .unwrap();
    db.execute_in_tx(tx, "DELETE FROM parent WHERE a = 1 AND b = 2", &p())
        .unwrap();
    expect_fk_violation(db.commit(tx), "netzero parent and child");
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "parent"), 0);
    assert_eq!(persisted_row_count(&path, "child"), 0);
}

#[test]
fn cfk_parent_rewrite_new_tuple_child_matches_new() {
    let (_tmp, path, db) = file_db("parent-rewrite-valid.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    insert_child(&db, 1, "1", "2").unwrap();
    let tx = db.begin();
    db.execute_in_tx(
        tx,
        "UPDATE parent SET a = 3, b = 4 WHERE a = 1 AND b = 2",
        &p(),
    )
    .unwrap();
    db.execute_in_tx(tx, "UPDATE child SET c1 = 3, c2 = 4 WHERE id = 1", &p())
        .unwrap();
    db.commit(tx).unwrap();
    let rows = db
        .execute("SELECT c1, c2 FROM child WHERE id = 1", &p())
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![Value::Int64(3), Value::Int64(4)]]);
    db.close().unwrap();
    assert_eq!(
        persisted_rows(&path, "SELECT c1, c2 FROM child WHERE id = 1"),
        vec![vec![Value::Int64(3), Value::Int64(4)]]
    );
}

#[test]
fn cfk_parent_rewrite_orphans_child_rejects() {
    let (_tmp, path, db) = file_db("parent-rewrite-orphan.db");
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    insert_child(&db, 1, "1", "2").unwrap();
    expect_fk_violation(
        db.execute("UPDATE parent SET a = 3, b = 4 WHERE a = 1 AND b = 2", &p())
            .map(|_| ()),
        "parent rewrite orphan",
    );
    db.close().unwrap();
    assert_eq!(
        persisted_rows(&path, "SELECT a, b FROM parent"),
        vec![vec![Value::Int64(1), Value::Int64(2)]]
    );
    assert_eq!(
        persisted_rows(&path, "SELECT c1, c2 FROM child WHERE id = 1"),
        vec![vec![Value::Int64(1), Value::Int64(2)]]
    );
}

#[test]
fn cfk_arity_one_via_composite_path() {
    let (_tmp, path, db) = file_db("arity-one.db");
    db.execute("CREATE TABLE p (a INTEGER UNIQUE)", &p())
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, x INTEGER, FOREIGN KEY (x) REFERENCES p(a))",
        &p(),
    )
    .unwrap();
    let result = db
        .execute("INSERT INTO c (id, x) VALUES (1, 9)", &p())
        .map(|_| ());
    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                ref child_columns,
                ref parent_columns,
                ..
            }) if child_columns == &vec!["x".to_string()]
                && parent_columns == &vec!["a".to_string()]
        ),
        "arity-one table-level FK must use tuple-aware FK validation, got {result:?}"
    );
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "c"), 0);
}

#[test]
fn cfk_arity_three_enforces_at_commit() {
    let (_tmp, path, db) = file_db("arity-three.db");
    db.execute(
        "CREATE TABLE p (a INTEGER, b INTEGER, c INTEGER, UNIQUE(a, b, c))",
        &p(),
    )
    .unwrap();
    db.execute("CREATE TABLE ch (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, z INTEGER, FOREIGN KEY (x, y, z) REFERENCES p(a, b, c))", &p()).unwrap();
    expect_fk_violation_on(
        db.execute("INSERT INTO ch (id, x, y, z) VALUES (1, 1, 2, 3)", &p())
            .map(|_| ()),
        "arity three miss",
        "ch",
        &["x", "y", "z"],
        "p",
        &["a", "b", "c"],
    );
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "ch"), 0);
}

#[test]
fn cfk_batch_uses_indexed_tuple_probes() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cfk-batch.db");
    let db = Database::open(&path).unwrap();
    create_parent_child(&db);
    insert_parent(&db, 1, 2);
    db.__reset_fk_probe_stats();
    let tx = db.begin();
    for id in 0..1000 {
        db.execute_in_tx(
            tx,
            &format!("INSERT INTO child (id, c1, c2) VALUES ({id}, 1, 2)"),
            &p(),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    assert_eq!(row_count(&db, "child"), 1000);
    let stats = db.__fk_probe_stats();
    assert!(
        stats.indexed_tuple_probes >= 1000,
        "expected indexed tuple probes, got {stats:?}"
    );
    assert_eq!(stats.full_scan_fallbacks, 0);
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    assert_eq!(row_count(&reopened, "child"), 1000);
    assert_eq!(
        reopened
            .execute("SELECT c1, c2 FROM child WHERE id = 0", &p())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1), Value::Int64(2)]]
    );
    assert_eq!(
        reopened
            .execute("SELECT c1, c2 FROM child WHERE id = 999", &p())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1), Value::Int64(2)]]
    );
}

#[test]
fn cfk_schema_agnostic_three_shapes() {
    for (parent, child, a, b) in [
        ("accounts", "lines", "tenant", "number"),
        ("files", "chunks", "bucket", "path"),
        ("runs", "events", "workflow", "seq"),
    ] {
        let (_tmp, path, db) = file_db(&format!("{parent}-{child}.db"));
        db.execute(
            &format!("CREATE TABLE {parent} ({a} INTEGER, {b} INTEGER, UNIQUE({a}, {b}))"),
            &p(),
        )
        .unwrap();
        db.execute(&format!("CREATE TABLE {child} (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, FOREIGN KEY (x, y) REFERENCES {parent}({a}, {b}))"), &p()).unwrap();
        db.execute(
            &format!("INSERT INTO {parent} ({a}, {b}) VALUES (1, 2)"),
            &p(),
        )
        .unwrap();
        db.execute(
            &format!("INSERT INTO {child} (id, x, y) VALUES (1, 1, 2)"),
            &p(),
        )
        .unwrap();
        expect_fk_violation_on(
            db.execute(
                &format!("INSERT INTO {child} (id, x, y) VALUES (2, 9, 9)"),
                &p(),
            )
            .map(|_| ()),
            &format!("{child} orphan tuple"),
            child,
            &["x", "y"],
            parent,
            &[a, b],
        );
        db.close().unwrap();
        assert_eq!(persisted_row_count(&path, child), 1);
    }
}

#[test]
fn ddl_sync_reopen_round_trip_composite_fk_descriptor_present_on_follower() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("origin.db");
    {
        let origin = Database::open(&path).unwrap();
        origin
            .execute(
                "CREATE TABLE parent (a INTEGER, b INTEGER, UNIQUE(a, b))",
                &p(),
            )
            .unwrap();
        origin
            .execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
                &p(),
            )
            .unwrap();
        origin.close().unwrap();
    }
    let origin = Database::open(&path).unwrap();
    let follower = db();
    let apply_result = follower.apply_changes(origin.changes_since(Lsn(0)), &apply_policy());
    assert!(
        apply_result.is_ok(),
        "full-snapshot DDL apply must preserve valid parent UNIQUE syntax and order: {apply_result:?}"
    );
    assert_child_descriptor(
        &follower,
        "child",
        cfk(&["c1", "c2"], "parent", &["a", "b"]),
    );
}

#[test]
fn ddl_sync_round_trip_single_column_fk_structured_payload_present() {
    let origin = db();
    origin
        .execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &p())
        .unwrap();
    origin
        .execute(
            "CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES p(id))",
            &p(),
        )
        .unwrap();
    let ddl = origin.ddl_log_since(Lsn(0));
    let create = ddl
        .iter()
        .find(|d| matches!(d, DdlChange::CreateTable { name, .. } if name == "c"))
        .unwrap();
    match create {
        DdlChange::CreateTable { foreign_keys, .. } => {
            assert_eq!(
                foreign_keys,
                &vec![SingleColumnForeignKey {
                    child_column: "p_id".into(),
                    parent_table: "p".into(),
                    parent_column: "id".into(),
                }],
                "single-column FK must be structured exactly"
            )
        }
        _ => unreachable!(),
    }
    let follower = db();
    follower
        .apply_changes(origin.changes_since(Lsn(0)), &apply_policy())
        .unwrap();
    let c_meta = follower.table_meta("c").unwrap();
    let p_id = c_meta
        .columns
        .iter()
        .find(|column| column.name == "p_id")
        .unwrap();
    assert_eq!(
        p_id.references,
        Some(ForeignKeyReference {
            table: "p".into(),
            column: "id".into(),
        }),
        "follower metadata must reconstruct the single-column FK"
    );
}

#[test]
fn ddl_sync_round_trip_composite_unique_structured_payload_present() {
    let origin = db();
    origin
        .execute("CREATE TABLE p (a INTEGER, b INTEGER, UNIQUE(a, b))", &p())
        .unwrap();
    let create = origin.ddl_log_since(Lsn(0)).pop().unwrap();
    match create {
        DdlChange::CreateTable {
            composite_unique, ..
        } => assert_eq!(
            composite_unique,
            vec![vec!["a".to_string(), "b".to_string()]]
        ),
        _ => unreachable!(),
    }
    let follower = db();
    follower
        .apply_changes(origin.changes_since(Lsn(0)), &apply_policy())
        .unwrap();
    assert_eq!(
        follower.table_meta("p").unwrap().unique_constraints,
        vec![vec!["a".to_string(), "b".to_string()]],
        "follower metadata must reconstruct composite UNIQUE coverage"
    );
}

#[test]
fn ddl_sync_follower_enforces_composite_fk_on_local_writes() {
    let origin = db();
    origin
        .execute(
            "CREATE TABLE parent (a INTEGER, b INTEGER, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    origin
        .execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
            &p(),
        )
        .unwrap();
    let follower = db();
    let apply_result = follower.apply_changes(origin.changes_since(Lsn(0)), &apply_policy());
    assert!(
        apply_result.is_ok(),
        "structured DDL sync must apply before follower local validation: {apply_result:?}"
    );
    expect_fk_violation(
        follower
            .execute("INSERT INTO child (id, c1, c2) VALUES (1, 9, 9)", &p())
            .map(|_| ()),
        "follower local orphan",
    );
}

#[test]
fn ddl_sync_row_apply_rejects_orphan_child_returns_apply_result_with_conflict() {
    let receiver = db();
    create_parent_child(&receiver);
    let row = RowChange {
        table: "child".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("c1".into(), Value::Int64(9)),
            ("c2".into(), Value::Int64(9)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    let result = receiver
        .apply_changes(
            ChangeSet {
                rows: vec![row],
                ..Default::default()
            },
            &apply_policy(),
        )
        .unwrap();
    assert_eq!(
        result.conflicts.len(),
        1,
        "orphan row apply must become a bounded conflict"
    );
    assert_eq!(row_count(&receiver, "child"), 0);
}

#[test]
fn ddl_sync_apply_ok_when_receiver_has_parent() {
    let receiver = db();
    create_parent_child(&receiver);
    insert_parent(&receiver, 1, 2);
    let row = RowChange {
        table: "child".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("c1".into(), Value::Int64(1)),
            ("c2".into(), Value::Int64(2)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    receiver
        .apply_changes(
            ChangeSet {
                rows: vec![row],
                ..Default::default()
            },
            &apply_policy(),
        )
        .unwrap();
    assert_eq!(row_count(&receiver, "child"), 1);
}

#[test]
fn ddl_sync_skipped_parent_row_does_not_satisfy_composite_fk_child() {
    let receiver = db();
    receiver
        .execute(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    receiver
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();
    receiver
        .execute("INSERT INTO parent (id, a, b) VALUES (1, 1, 1)", &p())
        .unwrap();

    let child = RowChange {
        table: "child".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(2),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(2)),
            ("c1".into(), Value::Int64(9)),
            ("c2".into(), Value::Int64(9)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    let skipped_parent = RowChange {
        table: "parent".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("a".into(), Value::Int64(9)),
            ("b".into(), Value::Int64(9)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };

    let result = receiver
        .apply_changes(
            ChangeSet {
                rows: vec![child, skipped_parent],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    assert_eq!(
        row_count(&receiver, "child"),
        0,
        "child must be a bounded FK conflict, not committed against a skipped parent"
    );
    assert_eq!(
        receiver
            .execute("SELECT a, b FROM parent WHERE id = 1", &p())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1), Value::Int64(1)]],
        "server_wins parent conflict must leave the local parent unchanged"
    );
    assert!(
        result
            .conflicts
            .iter()
            .any(|conflict| conflict.reason.as_deref().is_some_and(|reason| {
                reason.contains("child(c1, c2)") && reason.contains("parent(a, b)")
            })),
        "missing bounded child FK conflict: {result:?}"
    );
}

#[test]
fn ddl_sync_child_before_parent_same_batch_uses_applied_parent_projection() {
    let receiver = db();
    receiver
        .execute(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    receiver
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();

    let child = RowChange {
        table: "child".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("c1".into(), Value::Int64(1)),
            ("c2".into(), Value::Int64(2)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    let parent = RowChange {
        table: "parent".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("a".into(), Value::Int64(1)),
            ("b".into(), Value::Int64(2)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };

    let result = receiver
        .apply_changes(
            ChangeSet {
                rows: vec![child, parent],
                ..Default::default()
            },
            &apply_policy(),
        )
        .unwrap();

    assert!(
        result.conflicts.is_empty(),
        "same-batch parent projected to apply must satisfy child FK: {result:?}"
    );
    assert_eq!(row_count(&receiver, "parent"), 1);
    assert_eq!(row_count(&receiver, "child"), 1);
}

#[test]
fn ddl_sync_future_parent_delete_does_not_satisfy_composite_fk_child() {
    let receiver = db();
    receiver
        .execute(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    receiver
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();
    receiver
        .execute("INSERT INTO parent (id, a, b) VALUES (1, 1, 2)", &p())
        .unwrap();

    let child = RowChange {
        table: "child".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("c1".into(), Value::Int64(1)),
            ("c2".into(), Value::Int64(2)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    let delete_parent = RowChange {
        table: "parent".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("a".into(), Value::Int64(1)),
            ("b".into(), Value::Int64(2)),
        ]),
        deleted: true,
        lsn: Lsn(10),
    };

    let result = receiver
        .apply_changes(
            ChangeSet {
                rows: vec![child, delete_parent],
                ..Default::default()
            },
            &apply_policy(),
        )
        .unwrap();

    assert_eq!(
        row_count(&receiver, "child"),
        0,
        "child must not be inserted against a parent projected to delete in the same batch"
    );
    assert_eq!(
        row_count(&receiver, "parent"),
        0,
        "parent delete should still apply once the child is bounded as a conflict"
    );
    assert!(
        result
            .conflicts
            .iter()
            .any(|conflict| conflict.reason.as_deref().is_some_and(|reason| {
                reason.contains("child(c1, c2)") && reason.contains("parent(a, b)")
            })),
        "missing bounded child FK conflict: {result:?}"
    );
}

#[test]
fn ddl_sync_future_parent_update_away_does_not_satisfy_composite_fk_child() {
    let receiver = db();
    receiver
        .execute(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    receiver
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();
    receiver
        .execute("INSERT INTO parent (id, a, b) VALUES (1, 1, 2)", &p())
        .unwrap();

    let child = RowChange {
        table: "child".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("c1".into(), Value::Int64(1)),
            ("c2".into(), Value::Int64(2)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    let parent_update = RowChange {
        table: "parent".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("a".into(), Value::Int64(9)),
            ("b".into(), Value::Int64(9)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };

    let result = receiver
        .apply_changes(
            ChangeSet {
                rows: vec![child, parent_update],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    assert_eq!(
        row_count(&receiver, "child"),
        0,
        "child must not be inserted against a parent tuple projected to move away"
    );
    assert_eq!(
        receiver
            .execute("SELECT a, b FROM parent WHERE id = 1", &p())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(9), Value::Int64(9)]],
        "parent update should still apply after child is bounded as a conflict"
    );
    assert!(
        result
            .conflicts
            .iter()
            .any(|conflict| conflict.reason.as_deref().is_some_and(|reason| {
                reason.contains("child(c1, c2)") && reason.contains("parent(a, b)")
            })),
        "missing bounded child FK conflict: {result:?}"
    );
}

#[test]
fn ddl_sync_parent_update_rejected_by_immutable_does_not_satisfy_composite_fk_child() {
    let receiver = db();
    receiver
        .execute(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, frozen TEXT IMMUTABLE, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    receiver
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();
    receiver
        .execute(
            "INSERT INTO parent (id, a, b, frozen) VALUES (1, 1, 2, 'x')",
            &p(),
        )
        .unwrap();

    let child = RowChange {
        table: "child".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("c1".into(), Value::Int64(9)),
            ("c2".into(), Value::Int64(9)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    let rejected_parent_update = RowChange {
        table: "parent".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("a".into(), Value::Int64(9)),
            ("b".into(), Value::Int64(9)),
            ("frozen".into(), Value::Text("y".into())),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };

    let result = receiver
        .apply_changes(
            ChangeSet {
                rows: vec![child, rejected_parent_update],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    assert_eq!(
        row_count(&receiver, "child"),
        0,
        "child must not be inserted against a parent update rejected by immutable constraints"
    );
    assert_eq!(
        receiver
            .execute("SELECT a, b, frozen FROM parent WHERE id = 1", &p())
            .unwrap()
            .rows,
        vec![vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Text("x".into())
        ]],
        "immutable parent update must remain a bounded conflict"
    );
    assert!(
        result
            .conflicts
            .iter()
            .any(|conflict| conflict.reason.as_deref().is_some_and(|reason| {
                reason.contains("child(c1, c2)") && reason.contains("parent(a, b)")
            })),
        "missing bounded child FK conflict: {result:?}"
    );
    assert!(
        result.conflicts.iter().any(|conflict| conflict
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("immutable"))),
        "missing immutable parent conflict: {result:?}"
    );
}

#[test]
fn ddl_sync_parent_update_rejected_by_projected_unique_does_not_satisfy_composite_fk_child() {
    let receiver = db();
    receiver
        .execute(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, u TEXT UNIQUE, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    receiver
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();
    receiver
        .execute(
            "INSERT INTO parent (id, a, b, u) VALUES (1, 1, 2, 'old')",
            &p(),
        )
        .unwrap();

    let child = RowChange {
        table: "child".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("c1".into(), Value::Int64(9)),
            ("c2".into(), Value::Int64(9)),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    let projected_unique_holder = RowChange {
        table: "parent".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(2),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(2)),
            ("a".into(), Value::Int64(5)),
            ("b".into(), Value::Int64(5)),
            ("u".into(), Value::Text("taken".into())),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };
    let rejected_parent_update = RowChange {
        table: "parent".into(),
        natural_key: NaturalKey {
            column: "id".into(),
            value: Value::Int64(1),
        },
        values: HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("a".into(), Value::Int64(9)),
            ("b".into(), Value::Int64(9)),
            ("u".into(), Value::Text("taken".into())),
        ]),
        deleted: false,
        lsn: Lsn(10),
    };

    let result = receiver
        .apply_changes(
            ChangeSet {
                rows: vec![child, projected_unique_holder, rejected_parent_update],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    assert_eq!(
        row_count(&receiver, "child"),
        0,
        "child must not be inserted against a parent update rejected by projected unique state"
    );
    assert_eq!(
        receiver
            .execute("SELECT a, b, u FROM parent WHERE id = 1", &p())
            .unwrap()
            .rows,
        vec![vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Text("old".into())
        ]],
        "unique-rejected parent update must remain a bounded conflict"
    );
    assert_eq!(
        receiver
            .execute("SELECT a, b, u FROM parent WHERE id = 2", &p())
            .unwrap()
            .rows,
        vec![vec![
            Value::Int64(5),
            Value::Int64(5),
            Value::Text("taken".into())
        ]],
        "earlier projected unique holder should still apply"
    );
    assert!(
        result
            .conflicts
            .iter()
            .any(|conflict| conflict.reason.as_deref().is_some_and(|reason| {
                reason.contains("child(c1, c2)") && reason.contains("parent(a, b)")
            })),
        "missing bounded child FK conflict: {result:?}"
    );
    assert!(
        result.conflicts.iter().any(|conflict| conflict
            .reason
            .as_deref()
            .is_some_and(|reason| reason.to_ascii_lowercase().contains("unique"))),
        "missing unique parent conflict: {result:?}"
    );
}

#[test]
fn ddl_sync_ddl_apply_rejects_when_receiver_index_missing() {
    let receiver = db();
    receiver
        .execute("CREATE TABLE parent (a INTEGER, b INTEGER)", &p())
        .unwrap();
    let change = DdlChange::CreateTable {
        name: "child".into(),
        columns: vec![
            ("id".into(), "INTEGER PRIMARY KEY".into()),
            ("c1".into(), "INTEGER".into()),
            ("c2".into(), "INTEGER".into()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: vec![cfk(&["c1", "c2"], "parent", &["a", "b"])],
        composite_unique: Vec::new(),
    };
    let result = receiver.apply_changes(
        ChangeSet {
            ddl: vec![change],
            ddl_lsn: vec![Lsn(1)],
            ..Default::default()
        },
        &apply_policy(),
    );
    assert!(result.is_err(), "unenforceable structured FK DDL must fail");
    assert!(receiver.table_meta("child").is_none());
}

#[test]
fn ddl_sync_descriptor_comparator_ignores_irrelevant_whitespace() {
    let local = db();
    local
        .execute(
            "CREATE TABLE parent (a INTEGER, b INTEGER, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    local
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, note TEXT, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();
    let remote = DdlChange::CreateTable {
        name: "child".into(),
        columns: vec![
            ("id".into(), " INTEGER   PRIMARY   KEY ".into()),
            ("c1".into(), " INTEGER ".into()),
            ("c2".into(), "INTEGER".into()),
            ("note".into(), " TEXT ".into()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: vec![cfk(&["c1", "c2"], "parent", &["a", "b"])],
        composite_unique: Vec::new(),
    };
    let result = local.apply_changes(
        ChangeSet {
            ddl: vec![remote],
            ddl_lsn: vec![Lsn(1)],
            ..Default::default()
        },
        &apply_policy(),
    );
    assert!(
        matches!(result, Ok(ref applied) if applied.conflicts.is_empty()),
        "structural descriptor comparison must ignore whitespace: {result:?}"
    );
    assert_child_descriptor(&local, "child", cfk(&["c1", "c2"], "parent", &["a", "b"]));

    local
        .execute("CREATE TABLE p2 (a INTEGER, b INTEGER, UNIQUE(a, b))", &p())
        .unwrap();
    local
        .execute("CREATE TABLE multi (id INTEGER PRIMARY KEY, x1 INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER, FOREIGN KEY (x1, y1) REFERENCES parent(a, b), FOREIGN KEY (x2, y2) REFERENCES p2(a, b))", &p())
        .unwrap();
    let reversed = DdlChange::CreateTable {
        name: "multi".into(),
        columns: vec![
            ("id".into(), "INTEGER PRIMARY KEY".into()),
            ("x1".into(), "INTEGER".into()),
            ("y1".into(), "INTEGER".into()),
            ("x2".into(), "INTEGER".into()),
            ("y2".into(), "INTEGER".into()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: vec![
            cfk(&["x2", "y2"], "p2", &["a", "b"]),
            cfk(&["x1", "y1"], "parent", &["a", "b"]),
        ],
        composite_unique: Vec::new(),
    };
    let reversed_result = local.apply_changes(
        ChangeSet {
            ddl: vec![reversed],
            ddl_lsn: vec![Lsn(2)],
            ..Default::default()
        },
        &apply_policy(),
    );
    assert!(
        matches!(reversed_result, Ok(ref applied) if applied.conflicts.is_empty()),
        "descriptor list order must compare as a multiset: {reversed_result:?}"
    );

    let mismatch = DdlChange::CreateTable {
        name: "child".into(),
        columns: vec![
            ("id".into(), "INTEGER PRIMARY KEY".into()),
            ("c1".into(), "INTEGER".into()),
            ("c2".into(), "INTEGER".into()),
            ("note".into(), "TEXT".into()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: vec![cfk(&["c2", "c1"], "parent", &["a", "b"])],
        composite_unique: Vec::new(),
    };
    let mismatch_result = local.apply_changes(
        ChangeSet {
            ddl: vec![mismatch],
            ddl_lsn: vec![Lsn(3)],
            ..Default::default()
        },
        &apply_policy(),
    );
    assert!(
        mismatch_result.is_err(),
        "structured descriptor mismatch is a schema conflict and must fail fast: {mismatch_result:?}"
    );
}

#[test]
fn ddl_sync_wire_round_trip_byte_shape_preserves_structured_constraints() {
    let change = DdlChange::CreateTable {
        name: "child".into(),
        columns: vec![("id".into(), "INTEGER PRIMARY KEY".into())],
        constraints: Vec::new(),
        foreign_keys: vec![SingleColumnForeignKey {
            child_column: "p_id".into(),
            parent_table: "p".into(),
            parent_column: "id".into(),
        }],
        composite_foreign_keys: vec![cfk(&["c1", "c2"], "p", &["a", "b"])],
        composite_unique: vec![vec!["c1".into(), "c2".into()]],
    };
    let wire: WireDdlChange = change.clone().into();
    let bytes = rmp_serde::to_vec_named(&wire).unwrap();
    let decoded_wire: WireDdlChange = rmp_serde::from_slice(&bytes).unwrap();
    let decoded: DdlChange = decoded_wire.into();
    assert_eq!(
        decoded, change,
        "wire round trip must preserve structured constraints"
    );

    let alter = DdlChange::AlterTable {
        name: "child".into(),
        columns: vec![("p_id".into(), "INTEGER".into())],
        constraints: Vec::new(),
        foreign_keys: vec![SingleColumnForeignKey {
            child_column: "p_id".into(),
            parent_table: "p".into(),
            parent_column: "id".into(),
        }],
        composite_foreign_keys: vec![cfk(&["c1", "c2"], "p", &["a", "b"])],
        composite_unique: vec![vec!["c1".into(), "c2".into()]],
    };
    let wire: WireDdlChange = alter.clone().into();
    let bytes = rmp_serde::to_vec_named(&wire).unwrap();
    let decoded_wire: WireDdlChange = rmp_serde::from_slice(&bytes).unwrap();
    let decoded: DdlChange = decoded_wire.into();
    assert_eq!(
        decoded, alter,
        "ALTER TABLE wire round trip must preserve structured constraints"
    );

    #[derive(serde::Serialize)]
    #[allow(dead_code, clippy::enum_variant_names)]
    enum LegacyDdlChange {
        CreateTable {
            name: String,
            columns: Vec<(String, String)>,
            constraints: Vec<String>,
        },
        DropTable {
            name: String,
        },
        AlterTable {
            name: String,
            columns: Vec<(String, String)>,
            constraints: Vec<String>,
        },
    }
    let legacy = LegacyDdlChange::CreateTable {
        name: "legacy".into(),
        columns: Vec::new(),
        constraints: Vec::new(),
    };
    let legacy_bytes = rmp_serde::to_vec_named(&legacy).unwrap();
    let decoded_legacy: DdlChange = rmp_serde::from_slice(&legacy_bytes).unwrap();
    match decoded_legacy {
        DdlChange::CreateTable {
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
            ..
        } => {
            assert!(
                foreign_keys.is_empty()
                    && composite_foreign_keys.is_empty()
                    && composite_unique.is_empty()
            );
        }
        _ => unreachable!(),
    }
    let legacy_bincode =
        bincode::serde::encode_to_vec(&legacy, bincode::config::standard()).unwrap();
    let (decoded_legacy, _): (DdlChange, usize) =
        bincode::serde::decode_from_slice(&legacy_bincode, bincode::config::standard()).unwrap();
    match decoded_legacy {
        DdlChange::CreateTable {
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
            ..
        } => {
            assert!(
                foreign_keys.is_empty()
                    && composite_foreign_keys.is_empty()
                    && composite_unique.is_empty(),
                "legacy bincode DDL log decode must default new structured fields"
            );
        }
        _ => unreachable!(),
    }

    let legacy_alter = LegacyDdlChange::AlterTable {
        name: "legacy".into(),
        columns: Vec::new(),
        constraints: Vec::new(),
    };
    let legacy_bytes = rmp_serde::to_vec_named(&legacy_alter).unwrap();
    let decoded_legacy: DdlChange = rmp_serde::from_slice(&legacy_bytes).unwrap();
    match decoded_legacy {
        DdlChange::AlterTable {
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
            ..
        } => {
            assert!(
                foreign_keys.is_empty()
                    && composite_foreign_keys.is_empty()
                    && composite_unique.is_empty()
            );
        }
        _ => unreachable!(),
    }
    let legacy_bincode =
        bincode::serde::encode_to_vec(&legacy_alter, bincode::config::standard()).unwrap();
    let (decoded_legacy, _): (DdlChange, usize) =
        bincode::serde::decode_from_slice(&legacy_bincode, bincode::config::standard()).unwrap();
    match decoded_legacy {
        DdlChange::AlterTable {
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
            ..
        } => {
            assert!(
                foreign_keys.is_empty()
                    && composite_foreign_keys.is_empty()
                    && composite_unique.is_empty(),
                "legacy bincode ALTER DDL log decode must default new structured fields"
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn err_display_composite_via_real_engine_violation() {
    let db = db();
    create_parent_child(&db);
    match insert_child(&db, 1, "9", "9") {
        Err(err) => {
            let rendered = err.to_string();
            assert!(
                rendered.contains("child(c1, c2)") && rendered.contains("parent(a, b)"),
                "tuple-aware display missing: {rendered}"
            );
        }
        Ok(()) => panic!("composite FK violation must render tuple-aware error, got Ok(())"),
    }
}

#[test]
fn err_display_single_column_via_real_engine_violation() {
    let db = db();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &p())
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES p(id))",
        &p(),
    )
    .unwrap();
    let err = db
        .execute("INSERT INTO c (id, p_id) VALUES (1, 9)", &p())
        .unwrap_err();
    let rendered = err.to_string();
    assert!(
        rendered.contains("c(p_id)") && rendered.contains("p(id)"),
        "single-column FK display must use tuple format: {rendered}"
    );
}

#[test]
fn cfk_unique_violation_surfaces_before_fk_violation() {
    let (_tmp, path, db) = file_db("unique-before-fk.db");
    db.execute(
        "CREATE TABLE parent (a INTEGER, b INTEGER, UNIQUE(a, b))",
        &p(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
        &p(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO child (id, c1, c2) VALUES (1, NULL, NULL)",
        &p(),
    )
    .unwrap();
    let err = db
        .execute("INSERT INTO child (id, c1, c2) VALUES (1, 9, 9)", &p())
        .unwrap_err();
    assert!(
        matches!(err, Error::UniqueViolation { .. }),
        "UNIQUE must surface before FK; got {err:?}"
    );
    db.close().unwrap();
    assert_eq!(persisted_row_count(&path, "child"), 1);
}

#[test]
fn cfk_retention_eviction_of_parent_with_children_rejects() {
    let (_checked_tmp, checked_path, checked_db) = file_db("retention-checked.db");
    checked_db
        .execute(
            "CREATE TABLE parent (a INTEGER, b INTEGER, UNIQUE(a, b)) RETAIN 1 SECONDS",
            &p(),
        )
        .unwrap();
    checked_db
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();
    checked_db
        .execute("INSERT INTO parent (a, b) VALUES (1, 2)", &p())
        .unwrap();
    checked_db
        .execute("INSERT INTO child (id, c1, c2) VALUES (1, 1, 2)", &p())
        .unwrap();
    let (_legacy_tmp, legacy_path, legacy) = file_db("retention-legacy.db");
    legacy
        .execute(
            "CREATE TABLE parent (a INTEGER, b INTEGER, UNIQUE(a, b)) RETAIN 1 SECONDS",
            &p(),
        )
        .unwrap();
    legacy
        .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b))", &p())
        .unwrap();
    legacy
        .execute("INSERT INTO parent (a, b) VALUES (1, 2)", &p())
        .unwrap();
    legacy
        .execute("INSERT INTO child (id, c1, c2) VALUES (1, 1, 2)", &p())
        .unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    let report = checked_db.run_pruning_cycle_checked().unwrap();
    assert_eq!(
        report.pruned_rows, 0,
        "FK-protected parent must not be pruned"
    );
    assert!(
        report.blocked_count >= 1
            && report
                .blocked
                .iter()
                .any(|b| b.contains("parent(a, b)") && b.contains("child(c1, c2)")),
        "blocked FK reason missing: {report:?}"
    );
    assert_eq!(row_count(&checked_db, "parent"), 1);
    checked_db.close().unwrap();
    assert_eq!(persisted_row_count(&checked_path, "parent"), 1);
    assert_eq!(persisted_row_count(&checked_path, "child"), 1);
    assert_eq!(
        legacy.run_pruning_cycle(),
        0,
        "compatibility pruning path must count only actual prunes"
    );
    legacy.close().unwrap();
    assert_eq!(persisted_row_count(&legacy_path, "parent"), 1);
    assert_eq!(persisted_row_count(&legacy_path, "child"), 1);
}

#[test]
fn cfk_retention_keeps_transitively_referenced_expired_parent_chain() {
    let (_tmp, _path, db) = file_db("retention-transitive-chain.db");
    db.execute(
        "CREATE TABLE grandparent (a INTEGER, b INTEGER, UNIQUE(a, b)) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE parent (a INTEGER, b INTEGER, ga INTEGER, gb INTEGER, UNIQUE(a, b), FOREIGN KEY (ga, gb) REFERENCES grandparent(a, b)) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pa INTEGER, pb INTEGER, FOREIGN KEY (pa, pb) REFERENCES parent(a, b))",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO grandparent (a, b) VALUES (1, 2)", &p())
        .unwrap();
    db.execute(
        "INSERT INTO parent (a, b, ga, gb) VALUES (10, 20, 1, 2)",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO child (id, pa, pb) VALUES (1, 10, 20)", &p())
        .unwrap();

    std::thread::sleep(std::time::Duration::from_secs(2));
    let report = db.run_pruning_cycle_checked().unwrap();

    assert_eq!(
        report.pruned_rows, 0,
        "retention must not prune any row in an FK chain pinned by a live leaf"
    );
    assert_eq!(row_count(&db, "grandparent"), 1);
    assert_eq!(row_count(&db, "parent"), 1);
    assert_eq!(row_count(&db, "child"), 1);
    assert!(
        report
            .blocked
            .iter()
            .any(|b| b.contains("grandparent(a, b)") && b.contains("parent(ga, gb)")),
        "missing grandparent blocker reason: {report:?}"
    );
    assert!(
        report
            .blocked
            .iter()
            .any(|b| b.contains("parent(a, b)") && b.contains("child(pa, pb)")),
        "missing parent blocker reason: {report:?}"
    );
}

#[test]
fn ddl_sync_round_trip_distinguishes_pk_covered_from_unique_covered_fk() {
    let (_tmp, path, origin) = file_db("pk-vs-unique-sync.db");
    origin
        .execute(
            "CREATE TABLE pk_parent (id INTEGER PRIMARY KEY, b INTEGER)",
            &p(),
        )
        .unwrap();
    origin
        .execute(
            "CREATE TABLE uq_parent (a INTEGER, b INTEGER, UNIQUE(a, b))",
            &p(),
        )
        .unwrap();
    origin.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, FOREIGN KEY (x) REFERENCES pk_parent(id), FOREIGN KEY (x, y) REFERENCES uq_parent(a, b))", &p()).unwrap();
    let create_child = origin
        .ddl_log_since(Lsn(0))
        .into_iter()
        .find(|change| matches!(change, DdlChange::CreateTable { name, .. } if name == "child"))
        .expect("child DDL");
    let DdlChange::CreateTable {
        composite_foreign_keys,
        ..
    } = create_child
    else {
        unreachable!();
    };
    assert_eq!(
        composite_foreign_keys,
        vec![
            cfk(&["x"], "pk_parent", &["id"]),
            cfk(&["x", "y"], "uq_parent", &["a", "b"]),
        ],
        "PK-covered and UNIQUE-covered table-level descriptors must both be emitted distinctly"
    );
    origin.close().unwrap();
    let origin = Database::open(&path).unwrap();
    let follower = db();
    follower
        .apply_changes(origin.changes_since(Lsn(0)), &apply_policy())
        .unwrap();
    assert_eq!(
        follower.table_meta("child").unwrap().composite_foreign_keys,
        vec![
            cfk(&["x"], "pk_parent", &["id"]),
            cfk(&["x", "y"], "uq_parent", &["a", "b"]),
        ],
        "follower must preserve distinct PK-covered and UNIQUE-covered descriptors"
    );
    follower
        .execute("INSERT INTO uq_parent (a, b) VALUES (9, 9)", &p())
        .unwrap();
    expect_fk_violation_on(
        follower
            .execute("INSERT INTO child (id, x, y) VALUES (1, 9, 9)", &p())
            .map(|_| ()),
        "follower PK/UNIQUE descriptor enforcement",
        "child",
        &["x"],
        "pk_parent",
        &["id"],
    );
    follower
        .execute("INSERT INTO pk_parent (id, b) VALUES (9, 0)", &p())
        .unwrap();
    expect_fk_violation_on(
        follower
            .execute("INSERT INTO child (id, x, y) VALUES (2, 9, 8)", &p())
            .map(|_| ()),
        "follower UNIQUE-covered descriptor enforcement",
        "child",
        &["x", "y"],
        "uq_parent",
        &["a", "b"],
    );
}

#[test]
fn protocol_version_bumps_for_structured_constraint_wire() {
    assert_eq!(contextdb_server::protocol::PROTOCOL_VERSION, 4);
    let envelope = Envelope {
        version: 3,
        message_type: MessageType::PullRequest,
        payload: Vec::new(),
    };
    let bytes = rmp_serde::to_vec_named(&envelope).unwrap();
    let err = decode(&bytes).unwrap_err();
    assert!(matches!(
        err,
        SyncError::ProtocolVersionMismatch {
            received: 3,
            supported: 4
        }
    ));

    let encoded = encode(MessageType::PullRequest, &()).unwrap();
    let decoded = decode(&encoded).unwrap();
    assert_eq!(decoded.version, 4);
}
