//! The row guard: a transaction asks whether a row holds some values now, and asks its commit to
//! stand only if the row still holds them then. A guard writes nothing, so a guarded row is not
//! a changed row.

mod support;

use std::collections::BTreeSet;

use contextdb::{ContextId, Database, Error, Transaction, Uuid, Value, params};

const LEASES: &str = "CREATE TABLE leases (id INTEGER PRIMARY KEY, generation UUID UNIQUE, \
                      owner TEXT, epoch INTEGER NOT NULL)";
const ALL_LEASES: &str = "SELECT id, owner, epoch FROM leases ORDER BY id";

/// A store with `leases` 1 (owner `a`, epoch 1) and 2 (no owner, epoch 5), each with a
/// generation of its own number.
fn store_of_leases(name: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::run(&db, LEASES);
    for (id, owner, epoch) in [(1_i64, Value::from("a"), 1_i64), (2, Value::Null, 5)] {
        db.execute(
            "INSERT INTO leases (id, generation, owner, epoch) VALUES ($id, $generation, $owner, $epoch)",
            &params! {
                "id" => id, "generation" => Uuid::from_u128(id as u128), "owner" => owner,
                "epoch" => epoch,
            },
        )
        .expect("insert a lease");
    }
    db
}

/// Asks the transaction to stand only if lease `id` still holds these values when it commits.
fn guard(tx: &Transaction<'_>, id: i64, values: &[(&str, Value)]) -> bool {
    let predicates: Vec<(String, Value)> = values
        .iter()
        .map(|(column, value)| ((*column).to_owned(), value.clone()))
        .collect();
    tx.guard_row_conditions("leases", "id", &Value::Int64(id), &predicates)
        .expect("ask for a row guard")
}

fn assert_guard_conflict(outcome: contextdb::Result<()>, when: &str) {
    match outcome {
        Err(Error::ConditionalUpdateConflict { count }) => assert_eq!(count, 1, "{when}"),
        other => panic!("{when}: {other:?}"),
    }
}

#[test]
fn a_row_guard_answers_true_only_for_a_row_that_is_there_and_holds_every_value() {
    let db = store_of_leases("guard-answers");
    let tx = db.transaction().expect("begin");

    assert!(guard(
        &tx,
        1,
        &[("owner", "a".into()), ("epoch", 1_i64.into())]
    ));
    assert!(guard(&tx, 1, &[("epoch", 1_i64.into())]));
    // A NULL that is there is a value the row holds.
    assert!(guard(
        &tx,
        2,
        &[("owner", Value::Null), ("epoch", 5_i64.into())]
    ));
    // With nothing to hold, the guard asks only that the row is there.
    assert!(guard(&tx, 1, &[]));

    // One value of two is another, a value where the row has NULL, NULL where it has a value.
    assert!(!guard(
        &tx,
        1,
        &[("owner", "a".into()), ("epoch", 2_i64.into())]
    ));
    assert!(!guard(&tx, 2, &[("owner", "a".into())]));
    assert!(!guard(&tx, 1, &[("owner", Value::Null)]));
    // No such row.
    assert!(!guard(&tx, 404, &[]));
    assert!(!guard(&tx, 404, &[("epoch", 1_i64.into())]));

    // The row may be named by another column that names one row.
    let by_generation = |generation: u128, epoch: i64| {
        tx.guard_row_conditions(
            "leases",
            "generation",
            &Value::Uuid(Uuid::from_u128(generation)),
            &[("epoch".to_owned(), Value::Int64(epoch))],
        )
        .expect("ask for a row guard by another column")
    };
    assert!(by_generation(2, 5));
    assert!(!by_generation(2, 6));
    assert!(!by_generation(9, 5));

    // A table or a column that is not there is refused by name, and the transaction goes on.
    assert!(matches!(
        tx.guard_row_conditions("nowhere", "id", &Value::Int64(1), &[]),
        Err(Error::TableNotFound(table)) if table == "nowhere"
    ));
    assert!(matches!(
        tx.guard_row_conditions("leases", "id", &Value::Int64(1), &[("nope".to_owned(), Value::Null)]),
        Err(Error::ColumnNotFound { table, column }) if table == "leases" && column == "nope"
    ));
    assert!(guard(&tx, 1, &[("epoch", 1_i64.into())]));

    // The guard reads through the transaction: a value the transaction itself wrote is the
    // value the row holds.
    support::run(&tx, "UPDATE leases SET epoch = 6 WHERE id = 2");
    assert!(guard(&tx, 2, &[("epoch", 6_i64.into())]));
    assert!(!guard(&tx, 2, &[("epoch", 5_i64.into())]));
    tx.rollback();
    assert_eq!(
        support::rows(&db, ALL_LEASES),
        [
            vec![Value::Int64(1), support::text("a"), Value::Int64(1)],
            vec![Value::Int64(2), Value::Null, Value::Int64(5)],
        ]
    );
}

#[test]
fn a_row_guard_answers_false_for_a_row_hidden_from_its_handle() {
    let db = support::open(&support::folder("guard-hidden"));
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, body TEXT)",
    );
    let (mine, theirs) = (Uuid::from_u128(1), Uuid::from_u128(2));
    for (id, context) in [(1_i64, mine), (2, theirs)] {
        db.execute(
            "INSERT INTO notes (id, context_id, body) VALUES ($id, $context, 'kept')",
            &params! { "id" => id, "context" => context },
        )
        .expect("insert a note");
    }
    let holds = |tx: &Transaction<'_>, id: i64| {
        tx.guard_row_conditions(
            "notes",
            "id",
            &Value::Int64(id),
            &[("body".to_owned(), support::text("kept"))],
        )
        .expect("ask for a row guard")
    };

    // The writer sees both rows. A handle given one Context sees its own row and not the other,
    // though the other is there and holds the value.
    let tx = db.transaction().expect("begin");
    assert!(holds(&tx, 1) && holds(&tx, 2));
    tx.rollback();
    let narrowed = db.scoped_with_contexts(BTreeSet::from([ContextId::new(mine)]));
    let tx = narrowed
        .transaction()
        .expect("begin on the narrowed handle");
    assert!(holds(&tx, 1));
    assert!(
        !holds(&tx, 2),
        "a guard answered for a row its handle cannot see"
    );
    tx.commit().expect("commit a transaction that only guarded");
}

#[test]
fn a_commit_is_refused_when_a_guarded_value_was_changed_meanwhile_and_writes_nothing() {
    let db = store_of_leases("guard-conflict");
    let insert_three = "INSERT INTO leases (id, owner, epoch) VALUES (3, 'c', 1)";

    // The guarded value was changed by another commit.
    let tx = db.transaction().expect("begin");
    assert!(guard(
        &tx,
        1,
        &[("owner", "a".into()), ("epoch", 1_i64.into())]
    ));
    support::run(&tx, insert_three);
    support::run(&db, "UPDATE leases SET epoch = 2 WHERE id = 1");
    assert_guard_conflict(tx.commit(), "a commit over a guarded value that changed");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM leases"), 2);

    // The guarded row was deleted by another commit.
    let tx = db.transaction().expect("begin");
    assert!(guard(&tx, 2, &[("epoch", 5_i64.into())]));
    support::run(&tx, insert_three);
    support::run(&db, "DELETE FROM leases WHERE id = 2");
    assert_guard_conflict(tx.commit(), "a commit over a guarded row that is gone");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM leases"), 1);

    // A column the guard did not name changed: the guarded values are still there, and the
    // commit stands.
    let tx = db.transaction().expect("begin");
    assert!(guard(&tx, 1, &[("epoch", 2_i64.into())]));
    support::run(&tx, insert_three);
    support::run(&db, "UPDATE leases SET owner = 'b' WHERE id = 1");
    tx.commit()
        .expect("commit over a guarded row whose other column changed");
    assert_eq!(
        support::rows(&db, ALL_LEASES),
        [
            vec![Value::Int64(1), support::text("b"), Value::Int64(2)],
            vec![Value::Int64(3), support::text("c"), Value::Int64(1)],
        ]
    );

    // A guard that answered false asked for nothing: the commit stands whatever the row does.
    let tx = db.transaction().expect("begin");
    assert!(!guard(&tx, 1, &[("epoch", 99_i64.into())]));
    support::run(
        &tx,
        "INSERT INTO leases (id, owner, epoch) VALUES (4, 'd', 1)",
    );
    support::run(&db, "UPDATE leases SET epoch = 3 WHERE id = 1");
    tx.commit().expect("commit after a guard that did not hold");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM leases"), 3);
}

#[test]
fn a_guard_taken_after_a_declaration_refuses_the_commit_when_its_value_was_changed_meanwhile() {
    let db = store_of_leases("guard-after-declaration");
    let before = db.table_names();
    let tx = db.transaction().expect("begin");
    support::run(
        &tx,
        "CREATE TABLE audits (id INTEGER PRIMARY KEY, note TEXT)",
    );
    assert!(guard(
        &tx,
        1,
        &[("owner", "a".into()), ("epoch", 1_i64.into())]
    ));
    // The store's own handle commits a change to the guarded value.
    support::run(&db, "UPDATE leases SET epoch = 2 WHERE id = 1");
    assert_guard_conflict(
        tx.commit(),
        "a commit of a declaration over a guarded value that changed",
    );
    // Nothing the transaction declared is stored.
    assert_eq!(db.table_names(), before);
    assert!(matches!(
        support::refusal(&db, "SELECT COUNT(*) FROM audits"),
        Error::TableNotFound(table) if table == "audits"
    ));
    assert_eq!(
        support::rows(&db, ALL_LEASES),
        [
            vec![Value::Int64(1), support::text("a"), Value::Int64(2)],
            vec![Value::Int64(2), Value::Null, Value::Int64(5)],
        ]
    );

    // A guard names places in the row as the transaction shows it, and holds the same values
    // whatever column the transaction dropped before or after it. Each case has a table of
    // its own: the transaction drops `a`, guards `epoch` (before the drop in the
    // third case), and the store's own handle changes one cell before the commit.
    for (table, guard_first, meanwhile, held) in [
        ("seats_a", false, "a = 9", true),
        ("seats_b", false, "epoch = 2", false),
        ("seats_c", true, "a = 9", true),
    ] {
        support::run(
            &db,
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, a INTEGER, epoch INTEGER)"),
        );
        support::run(&db, &format!("INSERT INTO {table} VALUES (1, 1, 1)"));
        let tx = db.transaction().expect("begin");
        let ask = || {
            tx.guard_row_conditions(
                table,
                "id",
                &Value::Int64(1),
                &[("epoch".into(), 1_i64.into())],
            )
            .expect("ask for a row guard")
        };
        let before_drop = guard_first.then(ask);
        support::run(&tx, &format!("ALTER TABLE {table} DROP COLUMN a"));
        assert!(
            before_drop.unwrap_or_else(ask),
            "{table}: the guard on epoch after the drop"
        );
        support::run(&db, &format!("UPDATE {table} SET {meanwhile} WHERE id = 1"));
        if held {
            tx.commit()
                .unwrap_or_else(|e| panic!("{table}: a guard that still held: {e:?}"));
            assert_eq!(
                support::rows(&db, &format!("SELECT * FROM {table}")),
                [[Value::Int64(1), Value::Int64(1)]],
                "{table}: the drop stands and no cell moved"
            );
        } else {
            assert_guard_conflict(tx.commit(), table);
            assert_eq!(
                support::rows(&db, &format!("SELECT a FROM {table}")),
                [[Value::Int64(1)]],
                "{table}: the refused commit dropped nothing"
            );
        }
    }
}

#[test]
fn a_guarded_row_is_not_a_changed_row() {
    let db = store_of_leases("guard-is-no-write");
    let before = support::rows(&db, ALL_LEASES);
    let watermark = db.committed_watermark();

    // A transaction that only guards commits and writes nothing.
    let tx = db.transaction().expect("begin");
    assert!(guard(&tx, 1, &[("epoch", 1_i64.into())]));
    tx.commit().expect("commit a transaction that only guarded");
    assert_eq!(support::rows(&db, ALL_LEASES), before);
    assert_eq!(
        db.committed_watermark(),
        watermark,
        "a transaction that only guarded made a commit"
    );

    // Two transactions guard the same row and write other rows. Two that had changed the row
    // could not both commit; a guard changes nothing, so both do.
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    for (tx, id) in [(&first, 3_i64), (&second, 4)] {
        assert!(guard(tx, 1, &[("owner", "a".into())]));
        tx.execute(
            "INSERT INTO leases (id, owner, epoch) VALUES ($id, 'n', 1)",
            &params! { "id" => id },
        )
        .expect("insert beside the guarded row");
    }
    first.commit().expect("the first guarded commit");
    second
        .commit()
        .expect("the second commit over the same guarded row");

    // And a transaction that changed the guarded row's other column commits beside one that
    // guarded it: the guard holds a value, not the row.
    let guarding = db.transaction().expect("begin");
    assert!(guard(&guarding, 1, &[("epoch", 1_i64.into())]));
    support::run(&guarding, "UPDATE leases SET owner = 'z' WHERE id = 2");
    let writing = db.transaction().expect("begin");
    support::run(&writing, "UPDATE leases SET owner = 'w' WHERE id = 1");
    writing.commit().expect("commit a change to the row");
    guarding
        .commit()
        .expect("commit over a guard whose value is still held");
    assert_eq!(
        support::rows(
            &db,
            "SELECT id, owner, epoch FROM leases WHERE id <= 2 ORDER BY id"
        ),
        [
            vec![Value::Int64(1), support::text("w"), Value::Int64(1)],
            vec![Value::Int64(2), support::text("z"), Value::Int64(5)],
        ]
    );
}

#[test]
fn a_guard_expects_null_in_a_column_added_after_the_row_was_written() {
    let db = store_of_leases("guard-added-column");
    support::run(&db, "ALTER TABLE leases ADD COLUMN holder TEXT");
    let insert_three = "INSERT INTO leases (id, owner, epoch) VALUES (3, 'c', 1)";

    // The row was written before the column was there, and holds NULL in it.
    let tx = db.transaction().expect("begin");
    assert!(guard(
        &tx,
        1,
        &[("holder", Value::Null), ("epoch", 1_i64.into())]
    ));
    assert!(!guard(&tx, 1, &[("holder", "x".into())]));
    support::run(&tx, insert_three);
    tx.commit()
        .expect("commit over a guard that expects NULL in the added column");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM leases"), 3);

    // Another commit gives the row a value there: the guarded commit is refused.
    let tx = db.transaction().expect("begin");
    assert!(guard(&tx, 2, &[("holder", Value::Null)]));
    support::run(
        &tx,
        "INSERT INTO leases (id, owner, epoch) VALUES (4, 'd', 1)",
    );
    support::run(&db, "UPDATE leases SET holder = 'x' WHERE id = 2");
    assert_guard_conflict(
        tx.commit(),
        "a commit over a guard on the added column, after the column was written",
    );
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM leases"), 3);
    assert_eq!(
        support::rows(&db, "SELECT holder FROM leases ORDER BY id"),
        [[Value::Null], [support::text("x")], [Value::Null]]
    );
}

#[test]
fn a_guard_is_checked_again_at_commit_though_the_transaction_wrote_nothing() {
    let db = store_of_leases("guard-rechecked-at-commit");

    // The transaction only guards. Another commit changes the guarded value before it commits:
    // the guard no longer holds, and the commit is refused though it has nothing to write.
    let tx = db.transaction().expect("begin");
    assert!(guard(&tx, 1, &[("epoch", 1_i64.into())]));
    support::run(&db, "UPDATE leases SET epoch = 2 WHERE id = 1");
    assert_guard_conflict(
        tx.commit(),
        "a commit that only guarded, over a value another commit changed",
    );

    // The guard is checked against the row as it is at the commit, not as it was on the way: a
    // value changed and put back is a value the row holds.
    let tx = db.transaction().expect("begin");
    assert!(guard(&tx, 2, &[("epoch", 5_i64.into())]));
    support::run(&db, "UPDATE leases SET epoch = 6 WHERE id = 2");
    support::run(&db, "UPDATE leases SET epoch = 5 WHERE id = 2");
    tx.commit()
        .expect("commit over a guarded value that is held again");

    // Only the other commits wrote.
    assert_eq!(
        support::rows(&db, ALL_LEASES),
        [
            vec![Value::Int64(1), support::text("a"), Value::Int64(2)],
            vec![Value::Int64(2), Value::Null, Value::Int64(5)],
        ]
    );
}
