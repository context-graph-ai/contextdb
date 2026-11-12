//! The number a transaction has from the moment it begins, the `TXID` column that holds it, and
//! the number its commit carries: the transaction's own, or the next after the newest commit
//! when another commit overtook it, and never below a `TXID` value the commit holds.

mod support;

use contextdb::{Database, Error, TxId, Value, params};
use support::Run;

const LEDGER: &str = "CREATE TABLE ledger (id INTEGER PRIMARY KEY, at TXID, note TEXT)";

/// Writes ledger row `id`, stamped `at`.
fn stamp(on: &impl Run, id: i64, at: TxId) -> contextdb::Result<contextdb::QueryResult> {
    on.run(
        "INSERT INTO ledger (id, at) VALUES ($id, $at)",
        &params! { "id" => id, "at" => Value::TxId(at) },
    )
}

/// The stamp of ledger row `id`.
fn stamp_of(on: &impl Run, id: i64) -> Value {
    let rows = support::rows(on, &format!("SELECT at FROM ledger WHERE id = {id}"));
    match rows.as_slice() {
        [row] => row[0].clone(),
        rows => panic!("ledger row {id} came back as {} rows", rows.len()),
    }
}

#[test]
fn a_txid_column_holds_its_transactions_number_and_a_commit_is_never_numbered_below_it() {
    let folder = support::folder("transaction-numbers");
    let db = support::open(&folder);
    support::run(&db, LEDGER);
    support::run(&db, "CREATE INDEX ledger_at ON ledger (at)");
    let newest = db.committed_watermark();

    // A transaction has its number from the moment it begins, above every commit there is.
    let tx = db.transaction().expect("begin");
    let first = tx.id();
    assert!(
        first > newest,
        "{first:?} is not above the newest commit, {newest:?}"
    );
    assert_eq!(tx.id(), first, "a transaction has one number");
    stamp(&tx, 1, first).expect("stamp a row with the transaction's own number");
    // A number of an earlier commit is a value like any other, and so is no value.
    stamp(&tx, 2, newest).expect("stamp a row with an earlier number");
    support::run(&tx, "INSERT INTO ledger (id, at) VALUES (3, NULL)");
    assert_eq!(stamp_of(&tx, 1), Value::TxId(first));
    tx.commit().expect("commit");
    // No other commit came between: the commit carries the transaction's number.
    assert_eq!(db.committed_watermark(), first);
    assert_eq!(stamp_of(&db, 1), Value::TxId(first));
    assert_eq!(stamp_of(&db, 2), Value::TxId(newest));
    assert_eq!(stamp_of(&db, 3), Value::Null);

    // Two transactions side by side have two numbers. The second to begin commits first, under
    // its own number; the first was overtaken, and commits under the next number after that.
    let slow = db.transaction().expect("begin");
    let quick = db.transaction().expect("begin");
    assert!(slow.id() > first && quick.id() > slow.id());
    let (slow_id, quick_id) = (slow.id(), quick.id());
    stamp(&slow, 4, slow_id).expect("stamp in the slow transaction");
    stamp(&quick, 5, quick_id).expect("stamp in the quick transaction");
    quick.commit().expect("commit the quick transaction");
    assert_eq!(db.committed_watermark(), quick_id);
    slow.commit().expect("commit the slow transaction");
    let overtaken = db.committed_watermark();
    assert_eq!(
        overtaken,
        TxId(quick_id.0 + 1),
        "an overtaken commit takes the next number after the newest"
    );
    // The row keeps the number its transaction had, which is below the commit's number.
    assert_eq!(stamp_of(&db, 4), Value::TxId(slow_id));
    assert!(slow_id < overtaken);

    // A transaction that commits nothing does not hand its number to the next one.
    let unused = db.transaction().expect("begin");
    let unused_id = unused.id();
    assert!(unused_id > overtaken);
    unused.rollback();
    let next = db.transaction().expect("begin");
    assert!(next.id() > unused_id);
    stamp(&next, 6, next.id()).expect("stamp");
    let next_id = next.id();
    next.commit().expect("commit");
    assert_eq!(db.committed_watermark(), next_id);

    // A stamp is found by its value through the index, and kept across a reopen.
    let stamped = |on: &Database, at: TxId| {
        on.execute(
            "SELECT id FROM ledger WHERE at = $at",
            &params! { "at" => Value::TxId(at) },
        )
        .expect("find rows by their stamp")
    };
    let found = stamped(&db, slow_id);
    assert_eq!(found.rows(), [[Value::Int64(4)]]);
    assert_eq!(found.rows_examined(), 1);
    support::close(db);
    let db = support::open(&folder);
    assert_eq!(stamped(&db, next_id).rows(), [[Value::Int64(6)]]);
    assert_eq!(stamp_of(&db, 5), Value::TxId(quick_id));
    // Numbers go on rising from the newest commit.
    assert!(db.transaction().expect("begin").id() > next_id);
}

#[test]
fn a_stamp_above_the_transactions_own_number_and_a_stamp_written_as_a_number_are_refused() {
    let db = support::open(&support::folder("transaction-number-refusals"));
    support::run(&db, LEDGER);
    let tx = db.transaction().expect("begin");
    let own = tx.id();
    stamp(&tx, 1, own).expect("stamp a row with the transaction's own number");

    let out_of_range =
        |outcome: contextdb::Result<contextdb::QueryResult>, wrote: u64| match outcome {
            Err(Error::TxIdOutOfRange { value, max }) => {
                assert_eq!((value, max), (wrote, own.0));
            }
            other => panic!("a stamp of {wrote} in transaction {} gave {other:?}", own.0),
        };
    out_of_range(stamp(&tx, 2, TxId(own.0 + 1)), own.0 + 1);
    out_of_range(stamp(&tx, 2, TxId(u64::MAX)), u64::MAX);
    out_of_range(
        tx.execute(
            "UPDATE ledger SET at = $at WHERE id = 1",
            &params! { "at" => Value::TxId(TxId(own.0 + 7)) },
        ),
        own.0 + 7,
    );

    // SQL has no way to write a transaction's number: a number and a text are of another type.
    for sql in [
        "INSERT INTO ledger (id, at) VALUES (3, 7)",
        "INSERT INTO ledger (id, at) VALUES (3, '7')",
        "UPDATE ledger SET at = 7 WHERE id = 1",
    ] {
        match support::refusal(&tx, sql) {
            Error::ColumnTypeMismatch { table, column, .. } => {
                assert_eq!((table.as_str(), column.as_str()), ("ledger", "at"), "{sql}");
            }
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
    }
    // The transaction holds the one row it stamped, as it stamped it.
    assert_eq!(
        support::rows(&tx, "SELECT id, at FROM ledger ORDER BY id"),
        [[Value::Int64(1), Value::TxId(own)]]
    );
    tx.commit().expect("commit");

    // A statement on the writer has a number of its own, and is held to it the same way.
    match stamp(&db, 4, TxId(u64::MAX)) {
        Err(Error::TxIdOutOfRange { value, .. }) => assert_eq!(value, u64::MAX),
        other => panic!("a stamp of the largest number on the writer gave {other:?}"),
    }
    stamp(&db, 4, db.committed_watermark()).expect("stamp with the newest commit's number");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM ledger"), 2);
}

#[test]
fn a_vector_column_is_partitioned_by_the_number_of_the_transaction_that_wrote_the_row() {
    const DIMENSION: usize = 4;
    let db = support::open(&support::folder("partitioned-by-txid"));
    support::run(
        &db,
        &format!(
            "CREATE TABLE marks (id INTEGER PRIMARY KEY, at TXID NOT NULL, \
             v VECTOR({DIMENSION}) PARTITION_KEY (at))"
        ),
    );
    // Two transactions of five rows each: each transaction's rows are one partition.
    let mut numbers = Vec::new();
    for batch in 0..2_i64 {
        let tx = db.transaction().expect("begin");
        for n in 1..=5_i64 {
            let id = batch * 5 + n;
            tx.execute(
                "INSERT INTO marks (id, at, v) VALUES ($id, $at, $v)",
                &params! { "id" => id, "at" => Value::TxId(tx.id()), "v" => support::vector(id, DIMENSION) },
            )
            .expect("insert a mark");
        }
        numbers.push(tx.id());
        tx.commit().expect("commit");
    }
    let query = support::vector(77, DIMENSION);
    for (batch, at) in numbers.into_iter().enumerate() {
        let found = db
            .execute(
                "SELECT id FROM marks WHERE at = $at ORDER BY v <=> $query LIMIT 5",
                &params! { "at" => Value::TxId(at), "query" => query.clone() },
            )
            .expect("search one transaction's rows");
        let mut ids: Vec<i64> = found
            .rows()
            .iter()
            .map(|row| match row[0] {
                Value::Int64(id) => id,
                ref other => panic!("an id came back as {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        let first = batch as i64 * 5 + 1;
        assert_eq!(ids, (first..first + 5).collect::<Vec<_>>());
    }
}
