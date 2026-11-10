//! What the commit function does for a transaction that another commit overtook, and what a
//! closed handle answers.

use std::collections::HashMap;

use super::{Database, Transaction};
use crate::types::{Error, Value};

/// Runs a test on a fresh store in a folder of its own, which is removed when the test passes.
fn with_store(name: &str, test: impl FnOnce(&Database)) {
    let folder = std::env::temp_dir().join(format!("contextdb-{name}-{}", std::process::id()));
    let db = Database::open(&folder).expect("open a fresh store");
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, tag INTEGER)",
        "INSERT INTO t (id, tag) VALUES (1, 7)",
    ] {
        db.execute(sql, &HashMap::new()).expect(sql);
    }
    test(&db);
    drop(db);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    db.execute(sql, &HashMap::new()).expect(sql).rows().to_vec()
}

fn ints<const N: usize>(numbers: [i64; N]) -> Vec<Vec<Value>> {
    numbers.map(|number| vec![Value::Int64(number)]).to_vec()
}

#[test]
fn transactions_that_each_declare_a_table_commit_with_a_number_each() {
    with_store("declare-side-by-side", |db| {
        let declaring = [
            "CREATE TABLE a (id INTEGER PRIMARY KEY)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY)",
            "CREATE TABLE a (id INTEGER PRIMARY KEY, more TEXT)",
        ];
        // All three begin from the same tables.
        let [first, second, third] = declaring.map(|sql| {
            let tx = db.transaction().expect("begin");
            tx.execute(sql, &HashMap::new()).expect(sql);
            tx
        });
        first.commit().expect("the first declaration commits");
        second.commit().expect("a table of another name commits");
        // The name was free when the third declared it and is taken now.
        assert!(matches!(third.commit(), Err(Error::SchemaInvalid { .. })));
        assert_eq!(db.table_names(), ["a", "b", "t"]);

        // A row of one table is no row of the other.
        db.execute("INSERT INTO a (id) VALUES (1)", &HashMap::new())
            .expect("insert into a");
        assert_eq!(rows(db, "SELECT COUNT(*) FROM a"), ints([1]));
        assert_eq!(rows(db, "SELECT COUNT(*) FROM b"), ints([0]));
        assert!(rows(db, "SELECT id FROM b").is_empty());
    });
}

#[test]
fn a_new_index_has_the_rows_committed_while_its_transaction_was_open() {
    with_store("index-overtaken", |db| {
        let tx = db.transaction().expect("begin");
        tx.execute("CREATE INDEX t_tag ON t (tag)", &HashMap::new())
            .expect("declare the index");
        db.execute("INSERT INTO t (id, tag) VALUES (2, 7)", &HashMap::new())
            .expect("insert while the index is being declared");
        tx.commit().expect("commit the index");
        assert_eq!(
            rows(db, "SELECT id FROM t WHERE tag = 7 ORDER BY id"),
            ints([1, 2])
        );
    });
}

#[test]
fn rows_staged_before_an_index_was_declared_are_not_committed_without_its_entries() {
    with_store("rows-overtaken", |db| {
        let tx = db.transaction().expect("begin");
        tx.execute("INSERT INTO t (id, tag) VALUES (2, 7)", &HashMap::new())
            .expect("insert in the transaction");
        db.execute("CREATE INDEX t_tag ON t (tag)", &HashMap::new())
            .expect("declare an index beside the transaction");
        assert!(matches!(tx.commit(), Err(Error::SchemaInvalid { .. })));
        assert_eq!(rows(db, "SELECT id FROM t WHERE tag = 7"), ints([1]));
    });
}

#[test]
fn a_closed_handle_refuses_by_name() {
    with_store("closed-handle", |db| {
        let tx = db.transaction().expect("begin");
        db.close().expect("close");
        db.close().expect("close a second time");
        let select = "SELECT id FROM t";
        assert!(matches!(
            db.execute(select, &HashMap::new()),
            Err(Error::DatabaseClosed)
        ));
        assert!(matches!(
            tx.execute(select, &HashMap::new()),
            Err(Error::DatabaseClosed)
        ));
        assert!(matches!(tx.commit(), Err(Error::DatabaseClosed)));
        assert!(matches!(db.transaction(), Err(Error::DatabaseClosed)));
    });
}

fn shared<T: Send + Sync>() {}
fn sent<T: Send>() {}

/// No test may start a thread, so what "calls may be made from many threads" rests on is
/// checked here by the compiler.
#[test]
fn a_handle_is_shared_between_threads_and_a_transaction_moves_to_one() {
    shared::<Database>();
    sent::<Transaction<'_>>();
}
