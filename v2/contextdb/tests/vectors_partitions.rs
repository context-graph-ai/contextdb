//! The partitions of a VECTOR column: a search over one, several or all of them gives one
//! answer; two key tuples are one partition only when every value is equal as SQL `=` has it;
//! and a column's partition limit counts the partitions that hold a live vector, refuses the
//! statement that would pass it and changes nothing when it does.

mod support;

use std::sync::atomic::Ordering;

use contextdb::{
    Database, DatabaseOpenOptions, Error, ReadSession, ReadSessionOptions, Value, VectorIndexRef,
    params,
};

use support::{Child, Near, Run};

/// A table whose VECTOR column is partitioned by two columns.
const NOTES: &str = "SELECT id, ctx, kind, v FROM notes";

fn declare_notes(db: &Database, dimension: usize, clauses: &str) {
    support::run(
        db,
        &format!(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, \
             kind INTEGER NOT NULL, v VECTOR({dimension}) PARTITION_KEY (ctx, kind) {clauses})"
        ),
    );
}

fn insert_note(on: &impl Run, id: i64, ctx: &str, kind: i64, vector: Vec<f32>) {
    let inserted = on.run(
        "INSERT INTO notes (id, ctx, kind, v) VALUES ($id, $ctx, $kind, $v)",
        &params! { "id" => id, "ctx" => ctx, "kind" => kind, "v" => vector },
    );
    if let Err(error) = inserted {
        panic!("note {id} under ({ctx:?}, {kind}) was refused: {error:?}");
    }
}

/// True for a row of `notes` under this key tuple.
fn under(row: &[Value], ctx: &str, kind: i64) -> bool {
    row[1] == support::text(ctx) && row[2] == Value::Int64(kind)
}

fn ids(answer: &[Near]) -> Vec<i64> {
    answer
        .iter()
        .map(|near| match near.key {
            Value::Int64(id) => id,
            ref other => panic!("the key {other:?} is not an INTEGER"),
        })
        .collect()
}

#[test]
fn multi_partition_search_returns_one_global_answer() {
    const DIMENSION: usize = 16;
    let folder = support::folder("multi-partition-search");
    let db = support::open(&folder);
    // Six partitions of a hundred notes each: three contexts, two kinds.
    declare_notes(&db, DIMENSION, "");
    let ctx = |n: i64| ["a", "b", "c"][(n % 3) as usize];
    let kind = |n: i64| (n / 3) % 2;
    let tx = db.transaction().expect("begin");
    for n in 1..=600 {
        insert_note(&tx, n, ctx(n), kind(n), support::vector(n, DIMENSION));
    }
    tx.commit().expect("commit");
    // And a table partitioned by one column: three partitions of a hundred.
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 1..=300, DIMENSION, |n| ctx(n).to_owned());
    tx.commit().expect("commit");

    let queries = [
        support::vector(80_001, DIMENSION),
        support::vector(80_002, DIMENSION),
    ];
    // Each scope as a filter, the rows it lets through, and how many partitions it selects.
    type Scope = (
        &'static str,
        Option<&'static str>,
        fn(&[Value]) -> bool,
        u64,
    );
    let of_notes: [Scope; 9] = [
        (
            "one",
            Some("ctx = 'a' AND kind = 1"),
            |row| under(row, "a", 1),
            1,
        ),
        (
            "several, as a list of row values",
            Some("(ctx, kind) IN (('a', 1), ('b', 0))"),
            |row| under(row, "a", 1) || under(row, "b", 0),
            2,
        ),
        (
            "the same list the other way round",
            Some("(ctx, kind) IN (('b', 0), ('a', 1))"),
            |row| under(row, "a", 1) || under(row, "b", 0),
            2,
        ),
        (
            "several, as an OR",
            Some("(ctx = 'a' AND kind = 1) OR (ctx = 'b' AND kind = 0)"),
            |row| under(row, "a", 1) || under(row, "b", 0),
            2,
        ),
        ("all", None, |_| true, 6),
        (
            "every partition of one first key value",
            Some("ctx = 'a'"),
            |row| row[1] == support::text("a"),
            2,
        ),
        (
            "a tuple no row has",
            Some("ctx = 'zz' AND kind = 7"),
            |_| false,
            0,
        ),
        (
            "a tuple with a first value rows have and a second none has",
            Some("ctx = 'a' AND kind = 7"),
            |_| false,
            0,
        ),
        (
            "one tuple rows have and one no row has",
            Some("(ctx, kind) IN (('a', 1), ('zz', 7))"),
            |row| under(row, "a", 1),
            1,
        ),
    ];
    let of_docs: [Scope; 5] = [
        (
            "one",
            Some("ctx = 'b'"),
            |row| row[1] == support::text("b"),
            1,
        ),
        (
            "several, as a list",
            Some("ctx IN ('a', 'b')"),
            |row| row[1] != support::text("c"),
            2,
        ),
        (
            "the same list the other way round",
            Some("ctx IN ('b', 'a')"),
            |row| row[1] != support::text("c"),
            2,
        ),
        ("all", None, |_| true, 3),
        ("a key no row has", Some("ctx IN ('zz')"), |_| false, 0),
    ];
    // Every scope of both tables equals the reference, for ten rows and for more rows than
    // there are. `pending` says how many vectors of each partition are on its pending list:
    // then a search reads exactly those of the partitions it selects, and each row it returns
    // once more.
    let check = |pending: Option<u64>, state: &str| {
        for (table, plain, scopes) in [
            ("notes", NOTES, of_notes.as_slice()),
            ("docs", support::DOCS, of_docs.as_slice()),
        ] {
            let stored = support::stored(&db, plain);
            for (name, filter, passes, partitions) in scopes {
                for query in &queries {
                    for k in [10, 1000] {
                        let wanted = stored.nearest(passes, query, k);
                        let sql = support::nearest_sql(table, "v", *filter, k);
                        let what = format!("{state}, {table}, {name}");
                        let result = support::assert_nearest(&db, &sql, query, &wanted, &what);
                        if let Some(pending) = pending {
                            assert_eq!(
                                result.rows_examined(),
                                partitions * pending + wanted.len() as u64,
                                "{what}: rows read by {sql}"
                            );
                        }
                    }
                }
            }
        }
    };

    check(Some(100), "unsealed");
    support::seal_all(&db);
    check(None, "sealed");

    // LIMIT cuts after the whole scope: three notes of the direction searched for go into the
    // partition that was made last, the one of note 6, and a search of everything with LIMIT 3
    // returns them. Multiplying by two and by four is exact, so the three are at one distance.
    let near = &queries[0];
    for (id, times) in [(601, 1.0_f32), (602, 2.0), (603, 4.0)] {
        let vector = near.iter().map(|number| number * times).collect();
        insert_note(&db, id, ctx(6), kind(6), vector);
    }
    for state in ["the three pending", "the three sealed"] {
        let stored = support::stored(&db, NOTES);
        let wanted = stored.nearest(|_| true, near, 3);
        assert_eq!(ids(&wanted), [601, 602, 603]);
        let sql = support::nearest_sql("notes", "v", None, 3);
        support::assert_nearest(&db, &sql, near, &wanted, state);
        support::seal_all(&db);
    }

    // With everything sealed, a search of one partition gives the same rows for the same
    // counted work while other partitions grow, pending or sealed, and while new ones are made.
    let one = support::nearest_sql("notes", "v", Some("ctx = 'c' AND kind = 0"), 10);
    let measure = |state: &str| -> Vec<(Vec<Near>, u64)> {
        let stored = support::stored(&db, NOTES);
        let measured = queries.iter().map(|query| {
            let wanted = stored.nearest(|row| under(row, "c", 0), query, 10);
            let result = support::assert_nearest(&db, &one, query, &wanted, state);
            (wanted, result.rows_examined())
        });
        measured.collect()
    };
    let before = measure("before other partitions grow");
    let tx = db.transaction().expect("begin");
    for n in 1001..=1500 {
        insert_note(&tx, n, "b", 0, support::vector(n, DIMENSION));
    }
    tx.commit().expect("commit");
    assert!(measure("another partition grew, its new rows pending") == before);
    support::seal_all(&db);
    assert!(measure("another partition grew, its new rows sealed") == before);
    for n in 1501..=1600 {
        insert_note(&db, n, "d", n % 5, support::vector(n, DIMENSION));
    }
    assert!(measure("five more partitions were made") == before);
    check(None, "at the end");
}

#[test]
fn vector_partition_identity_is_typed_sql_equality() {
    const DIMENSION: usize = 8;
    let folder = support::folder("partition-identity");
    let db = support::open(&folder);
    declare_notes(&db, DIMENSION, "MAX_PARTITIONS 5");
    // Five key tuples that a careless comparison takes for fewer: three texts that differ in
    // the case of a letter or in a space at the end, and two tuples that print alike when
    // their values are written one after the other.
    let tuples: [(&str, i64); 5] = [("ab", 1), ("Ab", 1), ("ab ", 1), ("1", 11), ("11", 1)];
    let tx = db.transaction().expect("begin");
    for n in 1..=100 {
        let (ctx, kind) = tuples[(n % 5) as usize];
        insert_note(&tx, n, ctx, kind, support::vector(n, DIMENSION));
    }
    tx.commit().expect("commit");
    // They are five partitions and the limit is five: a sixth tuple is refused.
    let sixth = db.execute(
        "INSERT INTO notes (id, ctx, kind, v) VALUES ($id, $ctx, $kind, $v)",
        &params! {
            "id" => 101_i64, "ctx" => "AB", "kind" => 1_i64,
            "v" => support::vector(101, DIMENSION),
        },
    );
    match sixth {
        Err(Error::VectorPartitionLimitExceeded {
            max_partitions: 5, ..
        }) => {}
        other => panic!("a sixth key tuple under MAX_PARTITIONS 5 got {other:?}"),
    }

    let queries = [
        support::vector(81_001, DIMENSION),
        support::vector(81_002, DIMENSION),
    ];
    // A search of one tuple returns that tuple's rows and no other's. While nothing is sealed
    // it reads the twenty vectors of its own partition and each row it returns once more.
    let each_alone = |on: &dyn Run, pending: Option<u64>, state: &str| {
        let stored = support::stored(&on, NOTES);
        for (ctx, kind) in tuples {
            let filter = format!("ctx = '{ctx}' AND kind = {kind}");
            for query in &queries {
                let wanted = stored.nearest(|row| under(row, ctx, kind), query, 1000);
                let sql = support::nearest_sql("notes", "v", Some(&filter), 1000);
                let what = format!("{state}, ({ctx:?}, {kind})");
                let result = support::assert_nearest(&on, &sql, query, &wanted, &what);
                if let Some(pending) = pending {
                    assert_eq!(wanted.len(), 20, "{what}");
                    assert_eq!(result.rows_examined(), pending + 20, "{what}");
                }
            }
        }
    };
    each_alone(&db, Some(20), "unsealed");
    support::seal_all(&db);
    each_alone(&db, None, "sealed");

    // An UPDATE of a key column moves the row's vector. Row 5 is under ("ab", 1).
    let found_under = |on: &dyn Run, ctx: &str, state: &str| -> bool {
        let stored = support::stored(&on, NOTES);
        let query = support::vector(5, DIMENSION);
        let wanted = stored.nearest(|row| under(row, ctx, 1), &query, 1000);
        let filter = format!("ctx = '{ctx}' AND kind = 1");
        let sql = support::nearest_sql("notes", "v", Some(&filter), 1000);
        support::assert_nearest(&on, &sql, &query, &wanted, state);
        ids(&wanted).contains(&5)
    };
    assert!(found_under(&db, "ab", "before the move"));
    assert!(!found_under(&db, "Ab", "before the move"));
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    let tx = db.transaction().expect("begin");
    support::run(&tx, "UPDATE notes SET ctx = 'Ab' WHERE id = 5");
    // Inside the transaction it is under the new key and not under the old.
    assert!(found_under(&tx, "Ab", "inside the moving transaction"));
    assert!(!found_under(&tx, "ab", "inside the moving transaction"));
    // For everyone else it has not moved.
    for on in [&db as &dyn Run, &reader as &dyn Run] {
        assert!(found_under(on, "ab", "beside the moving transaction"));
        assert!(!found_under(on, "Ab", "beside the moving transaction"));
    }
    // The commit moves the row and its vector together.
    tx.commit().expect("commit");
    for on in [&db as &dyn Run, &reader as &dyn Run] {
        assert!(found_under(on, "Ab", "after the move"));
        assert!(!found_under(on, "ab", "after the move"));
    }
    each_alone(&db, None, "after the move, the moved vector pending");
    support::seal_all(&db);
    each_alone(&db, None, "after the move, everything sealed");
    // Back again, in a statement of its own.
    support::run(&db, "UPDATE notes SET ctx = 'ab' WHERE id = 5");
    assert!(found_under(&db, "ab", "after the move back"));
    assert!(!found_under(&db, "Ab", "after the move back"));
    each_alone(&db, None, "after the move back");
}

/// The column the limit refusals of the next test name.
fn docs_v() -> VectorIndexRef {
    VectorIndexRef {
        table: "docs".to_owned(),
        column: "v".to_owned(),
    }
}

/// Asserts that a statement or a commit was refused because `docs.v` may have two partitions.
fn refused_at_two<T: std::fmt::Debug>(outcome: contextdb::Result<T>, what: &str) {
    match outcome {
        Err(Error::VectorPartitionLimitExceeded {
            index,
            max_partitions,
        }) => {
            assert_eq!(index, docs_v(), "{what}: the column the refusal names");
            assert_eq!(max_partitions, 2, "{what}: the limit the refusal names");
        }
        other => panic!("{what}: was to be refused at the partition limit and got {other:?}"),
    }
}

/// The rows of `docs`, the commit position, and the answer of a search of everything and of
/// each context a test uses: what a refused statement must leave exactly as it was.
fn state_of(db: &Database) -> (Vec<Vec<Value>>, contextdb::TxId, Vec<Vec<Near>>) {
    let rows = support::rows(db, "SELECT id, ctx, tag, v FROM docs ORDER BY id");
    let stored = support::stored(db, support::DOCS);
    let query = support::vector(82_000, 8);
    let mut answers = Vec::new();
    let all = stored.nearest(|_| true, &query, 1000);
    let sql = support::nearest_sql("docs", "v", None, 1000);
    support::assert_nearest(db, &sql, &query, &all, "the state of docs");
    answers.push(all);
    for ctx in ["a", "b", "c", "d", "e", "m", "n", "z"] {
        let wanted = stored.nearest(|row| row[1] == support::text(ctx), &query, 1000);
        let filter = format!("ctx = '{ctx}'");
        let sql = support::nearest_sql("docs", "v", Some(&filter), 1000);
        support::assert_nearest(db, &sql, &query, &wanted, "the state of docs");
        answers.push(wanted);
    }
    (rows, db.committed_watermark(), answers)
}

fn insert_one(on: &impl Run, id: i64, ctx: &str) -> contextdb::Result<()> {
    support::insert_doc(on, id, ctx, Some(support::vector(id, 8))).map(|_| ())
}

#[test]
fn vector_partition_limit_counts_and_refuses() {
    let folder = support::folder("partition-limit");
    let db = support::open(&folder);

    // With no MAX_PARTITIONS written the limit is 256.
    support::run(
        &db,
        "CREATE TABLE wide (id INTEGER PRIMARY KEY, ctx INTEGER NOT NULL, \
         v VECTOR(4) PARTITION_KEY (ctx))",
    );
    let wide = "INSERT INTO wide (id, ctx, v) VALUES ($id, $ctx, $v)";
    let tx = db.transaction().expect("begin");
    for n in 1..=256_i64 {
        tx.execute(
            wide,
            &params! { "id" => n, "ctx" => n, "v" => support::vector(n, 4) },
        )
        .unwrap_or_else(|error| panic!("partition {n} of 256 was refused: {error:?}"));
    }
    tx.commit().expect("commit 256 partitions");
    let past = db.execute(
        wide,
        &params! { "id" => 257_i64, "ctx" => 257_i64, "v" => support::vector(257, 4) },
    );
    match past {
        Err(Error::VectorPartitionLimitExceeded {
            index,
            max_partitions: 256,
        }) => assert_eq!((index.table.as_str(), index.column.as_str()), ("wide", "v")),
        other => panic!("partition 257 with no MAX_PARTITIONS written got {other:?}"),
    }
    db.execute(
        wide,
        &params! { "id" => 258_i64, "ctx" => 256_i64, "v" => support::vector(258, 4) },
    )
    .expect("a second row under an existing partition");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM wide"), 257);

    // A column with MAX_PARTITIONS 2, and two partitions of ten rows.
    support::declare_docs(&db, 8, "PARTITION_KEY (ctx) MAX_PARTITIONS 2");
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 1..=10, 8, |_| "a".to_owned());
    support::insert_docs(&tx, 11..=20, 8, |_| "b".to_owned());
    tx.commit().expect("commit");
    let two = state_of(&db);
    assert_eq!(two.0.len(), 20);

    // The INSERT of a third key tuple is refused, and so is the UPDATE that moves a row to
    // one while its partition keeps other rows. Row, vector and partitions are as before.
    refused_at_two(insert_one(&db, 21, "c"), "the INSERT of a third tuple");
    assert!(state_of(&db) == two);
    let moved = db.execute("UPDATE docs SET ctx = 'c' WHERE id = 1", &params! {});
    refused_at_two(moved, "the UPDATE that moves a row to a third tuple");
    assert!(state_of(&db) == two);
    // A multi-row INSERT whose second row would pass the limit stores neither row; nor does
    // one whose first row would.
    for (first, second) in [("a", "c"), ("c", "a")] {
        let both = db.execute(
            "INSERT INTO docs (id, ctx, tag, v) VALUES (22, $first, 0, $v), (23, $second, 0, $v)",
            &params! { "first" => first, "second" => second, "v" => support::vector(22, 8) },
        );
        refused_at_two(both, "an INSERT of two rows, one under a third tuple");
        assert!(state_of(&db) == two);
    }

    // Inside a transaction the refusal is the statement's: what the transaction did before is
    // still there, a later statement under an existing tuple succeeds, and it commits.
    let tx = db.transaction().expect("begin");
    insert_one(&tx, 30, "a").expect("a row under an existing tuple");
    refused_at_two(
        insert_one(&tx, 31, "c"),
        "a third tuple inside a transaction",
    );
    assert_eq!(support::int(&tx, "SELECT COUNT(*) FROM docs"), 21);
    insert_one(&tx, 32, "b").expect("a later statement under an existing tuple");
    tx.commit()
        .expect("a transaction commits after one of its statements was refused");
    assert_eq!(
        support::rows(&db, "SELECT id FROM docs WHERE id >= 30 ORDER BY id"),
        [[Value::Int64(30)], [Value::Int64(32)]]
    );

    // A statement refused after it opened a tuple puts the count back, whatever refused it:
    // with b emptied, a two-row INSERT opens c and then breaks the key on its second row, and
    // b is still the tuple there is room for.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DELETE FROM docs WHERE ctx = 'b'");
    let broken = tx
        .execute(
            "INSERT INTO docs (id, ctx, tag, v) VALUES (90, 'c', 0, $v), (1, 'a', 0, $v)",
            &params! { "v" => support::vector(90, 8) },
        )
        .expect_err("the second row repeats a key");
    assert!(
        !matches!(broken, Error::VectorPartitionLimitExceeded { .. }),
        "the second row breaks the key, not the limit: {broken:?}"
    );
    insert_one(&tx, 91, "b").expect("b once the refused statement left its count");
    tx.rollback();

    // In one transaction at the limit: deleting a partition's last vector and then inserting
    // under a new tuple succeeds, and inserting under the emptied tuple after that is refused.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DELETE FROM docs WHERE ctx = 'b'");
    insert_one(&tx, 40, "c").expect("a new tuple once a partition was emptied");
    refused_at_two(insert_one(&tx, 41, "b"), "the emptied tuple, now a third");
    insert_one(&tx, 42, "c").expect("the transaction is still usable");
    tx.commit().expect("commit");
    // The partitions are a and c now.
    refused_at_two(insert_one(&db, 43, "b"), "a third tuple after the commit");
    let a_and_c = state_of(&db);
    assert!(a_and_c.2[2].is_empty() && a_and_c.2[3].len() == 2);

    // The one refusal only a commit sees. Deleting c leaves room for one partition. A
    // transaction starts d while there is room; another statement starts e meanwhile; the
    // transaction's commit is refused with the same kind and nothing of it is stored.
    support::run(&db, "DELETE FROM docs WHERE ctx = 'c'");
    let tx = db.transaction().expect("begin");
    insert_one(&tx, 50, "d").expect("a second partition while there is room");
    insert_one(&tx, 51, "a").expect("and a row under an existing tuple");
    insert_one(&db, 52, "e").expect("another statement takes the room");
    let a_and_e = state_of(&db);
    refused_at_two(
        tx.commit(),
        "the commit of a partition there is no room for now",
    );
    assert!(state_of(&db) == a_and_e);
    assert!(support::rows(&db, "SELECT id FROM docs WHERE id IN (50, 51)").is_empty());

    // A NULL vector takes no partition: at the limit a row with none goes in under a third
    // tuple, and giving it a vector is what is refused.
    db.execute(
        "INSERT INTO docs (id, ctx, tag) VALUES (60, 'z', 0)",
        &params! {},
    )
    .expect("a row with no vector under a third tuple");
    let with_vector = "UPDATE docs SET v = $v WHERE id = 60";
    let vector = params! { "v" => support::vector(60, 8) };
    refused_at_two(
        db.execute(with_vector, &vector),
        "a vector for the row under z",
    );
    refused_at_two(
        insert_one(&db, 61, "n"),
        "a third tuple beside the row with no vector",
    );
    // Taking the only vector of e away frees its partition, and the row under z takes it.
    support::run(&db, "UPDATE docs SET v = NULL WHERE ctx = 'e'");
    db.execute(with_vector, &vector)
        .expect("a vector for the row under z once e holds none");
    refused_at_two(
        db.execute("UPDATE docs SET v = $v WHERE id = 52", &vector),
        "e again",
    );

    // Deleting a partition's last vector frees it.
    support::run(&db, "DELETE FROM docs WHERE ctx = 'z'");
    insert_one(&db, 70, "m").expect("a new tuple once z was deleted");
    refused_at_two(insert_one(&db, 71, "n"), "a third tuple beside a and m");

    // At the limit, one UPDATE that moves the last vector of a partition to a new key
    // succeeds; one that moves a vector out of a partition that keeps others is refused.
    let before = state_of(&db);
    let moved = db.execute("UPDATE docs SET ctx = 'n' WHERE id = 1", &params! {});
    refused_at_two(moved, "moving one of the vectors of a to a third tuple");
    assert!(state_of(&db) == before);
    support::run(&db, "UPDATE docs SET ctx = 'n' WHERE id = 70");
    let after = state_of(&db);
    // Under m there is nothing now, and under n the one row.
    assert!(after.2[6].is_empty());
    assert_eq!(ids(&after.2[7]), [70]);
    refused_at_two(insert_one(&db, 72, "m"), "m, which is a third tuple again");

    // After sealing everything, an UPDATE of a column that is neither the vector nor a key
    // column leaves the next cycle with nothing to do and the searches' counted work as it was.
    support::seal_all(&db);
    let sealed = state_of(&db);
    let work = |db: &Database| -> Vec<u64> {
        let query = support::vector(82_001, 8);
        let filters = [None, Some("ctx = 'a'"), Some("ctx = 'n'")];
        let searches = filters.iter().map(|filter| {
            let sql = support::nearest_sql("docs", "v", *filter, 5);
            let result = support::search(db, &sql, &query).expect("a search");
            result.rows_examined()
        });
        searches.collect()
    };
    let read_before = work(&db);
    let updated = support::run(&db, "UPDATE docs SET tag = tag + 100");
    assert_eq!(updated.rows_affected(), sealed.0.len() as u64);
    let position = db.committed_watermark();
    let cycle = db.run_maintenance_cycle().expect("a cycle").vector;
    assert_eq!(
        (
            cycle.maintenance_vectors_done,
            cycle.maintenance_vectors_remaining
        ),
        (0, 0),
        "a cycle after an UPDATE that changed no vector and no key"
    );
    assert_eq!(
        db.committed_watermark(),
        position,
        "a cycle with nothing to do"
    );
    assert_eq!(work(&db), read_before);
    assert!(state_of(&db).2 == sealed.2);

    // The storage reads of one INSERT under an existing partition, from a cold cache, are the
    // same with 8 partitions and with 512, within the depth of the file's tree: the insert
    // finds its partition by its key and does not visit the others. The key tuples are long, so
    // that 512 of them cannot sit in the one or two pages that 8 do.
    let reads_of_one_insert = |partitions: i64| -> u64 {
        let folder = support::folder(&format!("insert-reads-{partitions}-partitions"));
        let db = support::open(&folder);
        support::run(
            &db,
            "CREATE TABLE long_keys (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, \
             v VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS 4096)",
        );
        let insert = "INSERT INTO long_keys (id, ctx, v) VALUES ($id, $ctx, $v)";
        let tx = db.transaction().expect("begin");
        for n in 1..=partitions {
            let ctx = format!("{n:0>200}");
            tx.execute(
                insert,
                &params! { "id" => n, "ctx" => ctx, "v" => support::vector(n, 4) },
            )
            .unwrap_or_else(|error| panic!("partition {n} was refused: {error:?}"));
        }
        tx.commit().expect("commit the partitions");
        support::close(db);

        let (storage, bytes) = support::counting_bytes_read();
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(storage);
        let db = support::open_with(&folder, options).expect("open with a counting door");
        let opened = bytes.load(Ordering::SeqCst);
        db.execute(
            insert,
            &params! {
                "id" => partitions + 1,
                "ctx" => format!("{:0>200}", 1),
                "v" => support::vector(partitions + 1, 4),
            },
        )
        .expect("a row under an existing partition");
        bytes.load(Ordering::SeqCst) - opened
    };
    // The promise: the bytes read for one insert at 512 partitions are at most four times
    // those at 8: a walk of the partition list reads 64 times as much, a lookup through a few
    // trees that each grow a level reads a few more pages.
    let (few, most) = (reads_of_one_insert(8), reads_of_one_insert(512));
    assert!(
        most <= few * 4,
        "one insert read {few} bytes of the file with 8 partitions and {most} with 512: its \
         reads grow with the number of partitions as a walk of the partition list does"
    );
}

/// Rows in each of the two partitions of the next test.
const HELD: i64 = 600;

/// The search the child of the next test is stopped inside: both partitions, every row.
fn of_both() -> String {
    support::nearest_sql("docs", "v", Some("ctx IN ('a', 'b')"), 100_000)
}

/// An answer as lines a child announces: `<word> <key> <the distance's bits>`.
fn lines(word: &str, answer: &[Near]) -> Vec<String> {
    let answer = answer.iter();
    answer
        .map(|near| format!("{word} {:?} {:016x}", near.key, near.distance.to_bits()))
        .collect()
}

#[test]
fn the_partition_cap_counts_live_partitions_while_a_reader_holds_an_earlier_state() {
    if let Some(role) = support::role() {
        return searcher(&role);
    }
    let test = "the_partition_cap_counts_live_partitions_while_a_reader_holds_an_earlier_state";
    const DIMENSION: usize = 64;
    let folder = support::folder("cap-beside-a-reader");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx) MAX_PARTITIONS 2");
    let ctx = |n: i64| String::from(if n <= HELD { "a" } else { "b" });
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 1..=2 * HELD, DIMENSION, ctx);
    tx.commit().expect("commit");
    support::seal_all(&db);
    let query = support::vector(83_000, DIMENSION);
    let began_at = support::stored(&db, support::DOCS).nearest(|_| true, &query, usize::MAX);
    assert_eq!(began_at.len() as i64, 2 * HELD);

    // Run the search once to its end in a child, to learn how many storage reads it makes.
    let mut told = Child::start(test, "searcher pause_at=0", &folder).end();
    let reads = support::last_number(&told, "reads").expect("the searcher announces its reads");
    told.retain(|line| line.starts_with("row "));
    assert!(
        told == lines("row", &began_at),
        "the rows of a search nothing stopped"
    );
    assert!(
        reads > 1,
        "a search of every row made {reads} storage reads"
    );
    // A second child, stopped halfway through the storage reads of the same search.
    let halfway = reads.div_ceil(2);
    let mut reader = Child::start(test, &format!("searcher pause_at={halfway}"), &folder);
    reader.expect("reading");

    // At the cap, the writer deletes every row of one partition and inserts a row under a
    // third key tuple. It is not refused: the partition the stopped reader still sees holds
    // no live vector, so it does not count.
    support::run(&db, "DELETE FROM docs WHERE ctx = 'b'");
    if let Err(error) = support::insert_doc(&db, 5000, "c", Some(support::vector(5000, DIMENSION)))
    {
        panic!("a third key tuple, after the second was emptied, was refused: {error:?}");
    }
    // The cap still holds for the partitions that are live now.
    match support::insert_doc(&db, 5001, "d", Some(support::vector(5001, DIMENSION))) {
        Err(Error::VectorPartitionLimitExceeded {
            max_partitions: 2, ..
        }) => {}
        other => panic!("a third live partition got {other:?}"),
    }
    // More commits and a seal, so that pages the deleted partition gave up come up for reuse.
    for first in (5100..5700_i64).step_by(100) {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, first..first + 100, DIMENSION, |_| "c".to_owned());
        tx.commit().expect("commit");
    }
    support::seal_all(&db);
    let now = support::stored(&db, support::DOCS).nearest(|_| true, &query, usize::MAX);
    assert_eq!(now.len() as i64, HELD + 601);

    // Let go, the reader's search answers with the rows of the commit it began at, the
    // removed partition's rows among them; its next search sees the store as it is now.
    reader.tell("go");
    let searched = reader.until("searched");
    assert!(
        searched == lines("row", &began_at),
        "the rows of the stopped search"
    );
    assert!(
        searched
            .iter()
            .any(|line| line.starts_with(&format!("row Int64({}) ", 2 * HELD)))
    );
    let mut after = reader.until("done");
    after.retain(|line| line.starts_with("now "));
    assert!(
        after == lines("now", &now),
        "the rows of the reader's next search"
    );
    reader.end();
}

/// Opens the store for reading and searches both partitions for every row, announcing each
/// row of the answer as `row <key> <bits>`, then `searched` and `reads <n>`, the storage reads
/// the search made. At read number `pause_at` of that search it announces `reading` and waits
/// for a line. Then it searches every partition again and announces each row as
/// `now <key> <bits>`, then `done`.
fn searcher(role: &support::Role) {
    let (door, searching, reads) = support::pausing_at_read(role.number("pause_at"));
    let mut options = ReadSessionOptions::default();
    options.storage = Some(door);
    let reader =
        ReadSession::open_with_options(&role.folder, options).expect("open the store for reading");
    let query = support::vector(83_000, 64);

    searching.store(true, std::sync::atomic::Ordering::SeqCst);
    let sql = of_both();
    let result = support::search(&reader, &sql, &query).expect("a reader's search is not refused");
    searching.store(false, std::sync::atomic::Ordering::SeqCst);
    for line in lines("row", &support::found(&result, &sql)) {
        support::say(line);
    }
    support::say("searched");
    let reads = reads.load(std::sync::atomic::Ordering::SeqCst);
    support::say(format!("reads {reads}"));

    let sql = support::nearest_sql("docs", "v", None, 100_000);
    let result = support::search(&reader, &sql, &query).expect("the reader's next search");
    for line in lines("now", &support::found(&result, &sql)) {
        support::say(line);
    }
    support::say("done");
}
