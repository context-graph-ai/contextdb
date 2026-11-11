//! Exact similarity search: the rows a search returns are the ones a comparison of every
//! stored vector returns, in the order of `(distance, primary key)`, and the distance it shows
//! is that distance to the bit. This holds whatever has been sealed, changed, deleted or moved,
//! and for every reader from the moment it reads.
//!
//! Every test here works out its own answer from the rows a plain SELECT returns
//! (`support::Stored::nearest`) and holds the search to it.

mod support;

use std::collections::BTreeMap;

use contextdb::{Database, ReadSession, Value, params};

use support::{Near, Run};

/// The keys of an answer, in its order.
fn ids(answer: &[Near]) -> Vec<i64> {
    answer
        .iter()
        .map(|near| match near.key {
            Value::Int64(id) => id,
            ref other => panic!("the key {other:?} is not an INTEGER"),
        })
        .collect()
}

/// Changes the vector of one row of `docs`, or takes it away for `None`.
fn set_vector(on: &impl Run, id: i64, vector: Option<Vec<f32>>) {
    let updated = on.run(
        "UPDATE docs SET v = $v WHERE id = $id",
        &params! { "id" => id, "v" => vector.map_or(Value::Null, Value::Vector) },
    );
    match updated {
        Ok(result) => assert_eq!(result.rows_affected(), 1, "updating the vector of row {id}"),
        Err(error) => panic!("updating the vector of row {id} was refused: {error:?}"),
    }
}

/// The vector of row `n` in the first test: mostly the scattered vector of `n` at one of seven
/// magnitudes, with four rows that hold one vector and a fifth that holds it four times as long
/// (multiplying by four is exact, so its distance is theirs to the bit), two rows of zeros, one
/// row of the largest number an `f32` holds, and one row with no vector.
fn told_apart(n: i64, dimension: usize) -> Option<Vec<f32>> {
    let scaled = |n: i64, by: f32| -> Vec<f32> {
        let scattered = support::vector(n, dimension).into_iter();
        scattered.map(|number| number * by).collect()
    };
    Some(match n {
        5 | 20 | 40 | 60 => scaled(5, 1.0),
        80 => scaled(5, 4.0),
        30 | 31 => vec![0.0; dimension],
        50 => vec![f32::MAX; dimension],
        70 => return None,
        _ => scaled(n, 10.0_f32.powi((n % 7) as i32 - 3)),
    })
}

#[test]
fn exact_search_returns_the_true_nearest_vectors() {
    for (dimension, rows) in [(3, 5000_i64), (16, 5000), (33, 600), (384, 300), (768, 300)] {
        let folder = support::folder(&format!("exact-nearest-{dimension}"));
        let db = support::open(&folder);
        support::declare_docs(&db, dimension, "");
        // Not in key order: the even rows from the last down, then the odd rows from the first.
        let evens = (1..=rows).rev().filter(|n| n % 2 == 0);
        let odds = (1..=rows).filter(|n| n % 2 == 1);
        let tx = db.transaction().expect("begin");
        for n in evens.chain(odds) {
            if let Err(error) = support::insert_doc(&tx, n, "a", told_apart(n, dimension)) {
                panic!("row {n} of dimension {dimension} was refused: {error:?}");
            }
        }
        tx.commit().expect("commit");

        // What was inserted is what is stored, number for number.
        let stored = support::stored(&db, "SELECT id, ctx, tag, v FROM docs ORDER BY id");
        assert_eq!(stored.rows().len() as i64, rows);
        for (n, row) in (1..=rows).zip(stored.rows()) {
            let inserted = told_apart(n, dimension).map_or(Value::Null, Value::Vector);
            assert!(row[3] == inserted, "row {n} of dimension {dimension}");
        }

        let queries = [
            // The vector of rows 5, 20, 40, 60 and 80: the answer begins with a tie of five.
            told_apart(5, dimension).expect("row 5 has a vector"),
            // All zeros: every row is at distance 1, so the answer is in key order.
            vec![0.0; dimension],
            vec![f32::MAX; dimension],
            support::vector(100_001, dimension),
            support::vector(100_002, dimension),
        ];
        // Through the writer or a reader: every search equals the reference from the rows that
        // same door reads.
        let through = |on: &dyn Run, state: &str| {
            let plain = support::stored(&on, support::DOCS);
            // Every row with a vector is found, and the row without one never is.
            let all = plain.nearest(|_| true, &queries[3], usize::MAX);
            assert_eq!(all.len() as i64, rows - 1, "{state}: rows with a vector");
            assert!(!ids(&all).contains(&70), "{state}: the row with no vector");
            // The five rows of one direction come first for that vector, at one distance and
            // in key order.
            let tie = plain.nearest(|_| true, &queries[0], 5);
            assert_eq!(
                ids(&tie),
                [5, 20, 40, 60, 80],
                "{state}: rows of one vector"
            );
            let first = tie[0].distance.to_bits();
            assert!(tie.iter().all(|near| near.distance.to_bits() == first));
            // For a vector of zeros every row is at distance 1 and comes in key order.
            let zeros = plain.nearest(|_| true, &queries[1], 3);
            assert_eq!(ids(&zeros), [1, 2, 3], "{state}: every row at one distance");
            assert!(zeros.iter().all(|near| near.distance == 1.0));

            for (number, query) in queries.iter().enumerate() {
                // One row, ten, and more than the table holds.
                for k in [1, 10, rows as usize + 50] {
                    let wanted = plain.nearest(|_| true, query, k);
                    let sql = support::nearest_sql("docs", "v", None, k);
                    let what = format!("dimension {dimension}, {state}, query {number}, k {k}");
                    support::assert_nearest(&on, &sql, query, &wanted, &what);
                }
            }
        };

        through(&db, "unsealed");
        let first = db.run_maintenance_cycle().expect("the first cycle").vector;
        assert_eq!(
            first.maintenance_vectors_done,
            (rows as u64 - 1).min(4096),
            "dimension {dimension}: vectors the first cycle sealed"
        );
        through(&db, "after one cycle");
        support::seal_all(&db);
        through(&db, "after the last cycle");
        support::close(db);
        let db = support::open(&folder);
        through(&db, "after close and reopen");
        let reader = ReadSession::open(&folder).expect("open the store for reading");
        through(&reader, "through a read session");
    }
}

#[test]
fn exact_search_rescores_from_stored_vectors() {
    const DIMENSION: usize = 16;
    let folder = support::folder("exact-rescores");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 1..=400, DIMENSION, |n| format!("c{}", n % 4));
    tx.commit().expect("commit");
    support::seal_all(&db);

    // What each row that is still there holds: its context, and its vector if it has one.
    let mut held: BTreeMap<i64, (i64, Option<Vec<f32>>)> = (1..=400)
        .map(|n| (n, (n % 4, Some(support::vector(n, DIMENSION)))))
        .collect();
    // Vectors that were a row's and are no row's now: a slot written from one of them is the
    // nearest thing there is to a search for it, and must give nothing.
    let mut left_behind: Vec<Vec<f32>> = Vec::new();
    let mut steps = support::Fixed::seeded(2);
    // Numbers no row was ever given a vector for.
    let mut fresh = 10_000_i64;

    for step in 1..=300 {
        let at = steps.below(held.len() as u64) as usize;
        let id = *held.keys().nth(at).expect("a row that is still there");
        let (ctx, vector) = held.get(&id).expect("the row").clone();
        match steps.below(4) {
            0 => {
                fresh += 1;
                let new = support::vector(fresh, DIMENSION);
                set_vector(&db, id, Some(new.clone()));
                left_behind.extend(vector);
                held.insert(id, (ctx, Some(new)));
            }
            1 => {
                support::run(&db, &format!("DELETE FROM docs WHERE id = {id}"));
                left_behind.extend(vector);
                held.remove(&id);
            }
            2 => {
                set_vector(&db, id, None);
                left_behind.extend(vector);
                held.insert(id, (ctx, None));
            }
            _ => {
                let to = (ctx + 1 + steps.below(3) as i64) % 4;
                support::run(
                    &db,
                    &format!("UPDATE docs SET ctx = 'c{to}' WHERE id = {id}"),
                );
                held.insert(id, (to, vector));
            }
        }
        if step % 40 == 0 {
            db.run_maintenance_cycle().expect("a cycle between changes");
        }
        if step % 5 != 0 {
            continue;
        }

        let stored = support::stored(&db, support::DOCS);
        // The rows a plain SELECT returns are the rows this test has left there.
        let wanted: Vec<(i64, i64, Option<Vec<f32>>)> = held
            .iter()
            .map(|(id, (ctx, vector))| (*id, *ctx, vector.clone()))
            .collect();
        let mut read: Vec<(i64, i64, Option<Vec<f32>>)> = stored
            .rows()
            .iter()
            .map(|row| match row.as_slice() {
                [Value::Int64(id), Value::Text(ctx), _, vector] => {
                    let ctx = ctx[1..].parse().expect("a context is c and a number");
                    let vector = match vector {
                        Value::Vector(vector) => Some(vector.clone()),
                        _ => None,
                    };
                    (*id, ctx, vector)
                }
                other => panic!("a row of docs came back as {other:?}"),
            })
            .collect();
        read.sort_by_key(|row| row.0);
        assert!(read == wanted, "after step {step}: the rows of docs");

        let mut queries: Vec<Vec<f32>> = left_behind.iter().rev().take(2).cloned().collect();
        queries.push(support::vector(20_000 + step, DIMENSION));
        for (number, query) in queries.iter().enumerate() {
            for k in [1, 10, 1000] {
                let what = format!("after step {step}, query {number}");
                let everywhere = stored.nearest(|_| true, query, k);
                // No row answers from a vector it no longer has: every distance is the one of
                // the vector the row holds now.
                for near in &everywhere {
                    let Value::Int64(id) = near.key else {
                        panic!("the key {:?}", near.key)
                    };
                    assert!(held.get(&id).is_some_and(|(_, vector)| vector.is_some()));
                }
                let sql = support::nearest_sql("docs", "v", None, k);
                support::assert_nearest(&db, &sql, query, &everywhere, &what);
                let in_one = stored.nearest(|row| row[1] == support::text("c1"), query, k);
                let sql = support::nearest_sql("docs", "v", Some("ctx = 'c1'"), k);
                support::assert_nearest(&db, &sql, query, &in_one, &what);
            }
        }
    }

    // The same after everything pending is sealed, and after the store is opened again.
    support::seal_all(&db);
    support::close(db);
    let db = support::open(&folder);
    let stored = support::stored(&db, support::DOCS);
    for query in left_behind.iter().rev().take(20) {
        for k in [1, 10, 1000] {
            let wanted = stored.nearest(|_| true, query, k);
            let sql = support::nearest_sql("docs", "v", None, k);
            support::assert_nearest(&db, &sql, query, &wanted, "after sealing and reopening");
        }
    }
}

#[test]
fn exact_search_reads_its_own_snapshot() {
    const DIMENSION: usize = 16;
    let folder = support::folder("exact-own-snapshot");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    let ctx = |n: i64| format!("c{}", n % 2);
    // Rows 1 to 100 are in blocks and rows 101 to 200 on pending lists.
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 1..=100, DIMENSION, ctx);
    tx.commit().expect("commit");
    support::seal_all(&db);
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 101..=200, DIMENSION, ctx);
    tx.commit().expect("commit");

    // The search is for the vector of row 7, which is sealed. Row 160, which is pending, is
    // given the same direction, so the committed answer begins with rows 7 and 160.
    let query = support::vector(7, DIMENSION);
    let doubled: Vec<f32> = query.iter().map(|number| number * 2.0).collect();
    set_vector(&db, 160, Some(doubled));
    let reader = ReadSession::open(&folder).expect("open the store for reading");

    // Holds a search over every partition, and over each one, to the rows `on` itself reads.
    fn agrees(on: &impl Run, query: &[f32], what: &str) -> Vec<Near> {
        let stored = support::stored(on, support::DOCS);
        for k in [1, 10, 1000] {
            let wanted = stored.nearest(|_| true, query, k);
            let sql = support::nearest_sql("docs", "v", None, k);
            support::assert_nearest(on, &sql, query, &wanted, what);
            for ctx in ["c0", "c1"] {
                let wanted = stored.nearest(|row| row[1] == support::text(ctx), query, k);
                let filter = format!("ctx = '{ctx}'");
                let sql = support::nearest_sql("docs", "v", Some(&filter), k);
                support::assert_nearest(on, &sql, query, &wanted, what);
            }
        }
        stored.nearest(|_| true, query, 1000)
    }
    // What the transaction below does: adds rows, one of them the nearest there can be;
    // changes the vector of a sealed row and of a pending row; deletes one of each.
    let change = |tx: &contextdb::Transaction<'_>| {
        support::insert_docs(tx, 201..=220, DIMENSION, ctx);
        set_vector(tx, 210, Some(query.clone()));
        set_vector(tx, 5, Some(support::vector(30_005, DIMENSION)));
        set_vector(tx, 150, Some(support::vector(30_150, DIMENSION)));
        support::run(tx, "DELETE FROM docs WHERE id = 7");
        support::run(tx, "DELETE FROM docs WHERE id = 160");
    };

    let committed = agrees(&db, &query, "before any transaction");
    assert_eq!(ids(&committed)[..2], [7, 160]);
    assert_eq!(committed.len(), 200);

    // A transaction's searches see what it has done, as its row reads do.
    let tx = db.transaction().expect("begin");
    change(&tx);
    let inside = agrees(&tx, &query, "inside the transaction");
    assert_eq!(inside.len(), 218);
    assert_eq!(ids(&inside)[0], 210, "the row the transaction added");
    for gone in [7, 160] {
        assert!(!ids(&inside).contains(&gone), "row {gone} was deleted");
    }
    // Nobody else does: the writer's own handle and a reader in the folder answer as before.
    assert!(agrees(&db, &query, "beside the open transaction") == committed);
    assert!(agrees(&reader, &query, "a reader beside the open transaction") == committed);
    for on in [&db as &dyn Run, &reader as &dyn Run] {
        let added = on.run("SELECT id FROM docs WHERE id = 210", &params! {});
        assert!(added.expect("look for the added row").rows().is_empty());
    }

    // A rollback leaves the committed answer.
    tx.rollback();
    assert!(agrees(&db, &query, "after the rollback") == committed);
    assert!(agrees(&reader, &query, "a reader after the rollback") == committed);

    // Committed, the same changes are everyone's, the rows and their vectors together: a
    // reader that sees an added row finds it by its vector in the same breath.
    let tx = db.transaction().expect("begin");
    change(&tx);
    let inside = agrees(&tx, &query, "inside the second transaction");
    tx.commit().expect("commit");
    assert!(agrees(&db, &query, "after the commit") == inside);
    assert!(agrees(&reader, &query, "a reader after the commit") == inside);
    assert_eq!(support::int(&reader, "SELECT COUNT(*) FROM docs"), 218);
    assert_eq!(
        support::rows(&reader, "SELECT id FROM docs WHERE id = 210"),
        [[Value::Int64(210)]]
    );
}

#[test]
fn vector_search_filters_before_scoring() {
    const DIMENSION: usize = 16;
    const ROWS: i64 = 5000;
    let folder = support::folder("filters-before-scoring");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "");
    for first in (1..=ROWS).step_by(1000) {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, first..first + 1000, DIMENSION, |_| "a".to_owned());
        tx.commit().expect("commit");
    }
    let stored = support::stored(&db, support::DOCS);
    let tag = |row: &[Value]| match row[2] {
        Value::Int64(tag) => tag,
        ref other => panic!("the tag {other:?}"),
    };
    // A filter that passes every row, half of them, one in fifty, and none. Row `n` has the
    // tag `n % 50`.
    type Passes = fn(i64) -> bool;
    let filters: [(Option<&str>, Passes, usize); 5] = [
        (None, |_| true, 5000),
        (Some("tag >= 0"), |_| true, 5000),
        (Some("tag < 25"), |tag| tag < 25, 2500),
        (Some("tag = 7"), |tag| tag == 7, 100),
        (Some("tag = 50"), |_| false, 0),
    ];
    let queries = [
        support::vector(40_001, DIMENSION),
        support::vector(40_002, DIMENSION),
        support::vector(17, DIMENSION),
    ];
    // Every search equals the reference; returns the rows each one read.
    let check = |state: &str| -> Vec<u64> {
        let mut examined = Vec::new();
        for (filter, passes, passing) in filters {
            for query in &queries {
                let all = stored.nearest(|row| passes(tag(row)), query, usize::MAX);
                assert_eq!(all.len(), passing, "rows that pass {filter:?}");
                let wanted = &all[..all.len().min(10)];
                let sql = support::nearest_sql("docs", "v", filter, 10);
                let what = format!("{state}, filter {filter:?}");
                let result = support::assert_nearest(&db, &sql, query, wanted, &what);
                examined.push(result.rows_examined());
            }
        }
        examined
    };

    check("unsealed");
    assert_eq!(support::seal_all(&db), ROWS as u64);
    let examined = check("everything sealed");
    // With every row passing, the one-byte copies rule nearly every row out before it is read:
    // a search reads under a tenth of the table. The first six searches are the two filters
    // that pass every row, three vectors each.
    for read in &examined[..6] {
        assert!(
            *read < ROWS as u64 / 10,
            "a search of {ROWS} sealed rows, all passing, read {read} rows"
        );
    }
}

#[test]
fn equal_distances_come_in_primary_key_order() {
    const DIMENSION: usize = 8;
    // The first pair of rows with one vector is 3 and 9, the second 4 and 8; the lower key of
    // each is under the context x and the higher under y.
    let pair = |number: i64| support::vector(50_000 + number, DIMENSION);
    let filler = |tx: &contextdb::Transaction<'_>, numbers: std::ops::RangeInclusive<i64>| {
        support::insert_docs(tx, numbers, DIMENSION, |n| {
            let ctx = if n % 2 == 0 { "x" } else { "y" };
            ctx.to_owned()
        });
    };
    let insert = |db: &Database, id: i64, ctx: &str, vector: Vec<f32>| {
        if let Err(error) = support::insert_doc(db, id, ctx, Some(vector)) {
            panic!("row {id} was refused: {error:?}");
        }
    };
    // The lower key comes first and alone for LIMIT 1, and first of the two for LIMIT 2,
    // whichever way both partitions are named.
    let lower_first = |db: &Database, query: &[f32], lower: i64, higher: i64, state: &str| {
        let stored = support::stored(db, support::DOCS);
        for filter in [None, Some("ctx IN ('x', 'y')"), Some("ctx IN ('y', 'x')")] {
            for k in [1, 2, 5, 1000] {
                let wanted = stored.nearest(|_| true, query, k);
                let sql = support::nearest_sql("docs", "v", filter, k);
                let what = format!("{state}, {filter:?}");
                let result = support::assert_nearest(db, &sql, query, &wanted, &what);
                let found = support::found(&result, &sql);
                assert_eq!(ids(&found)[0], lower, "{what}, LIMIT {k}");
                if k >= 2 {
                    assert_eq!(ids(&found)[1], higher, "{what}, LIMIT {k}");
                    assert_eq!(
                        found[0].distance.to_bits(),
                        found[1].distance.to_bits(),
                        "{what}: two rows with one vector are at one distance"
                    );
                }
            }
        }
    };

    for lower_goes_in_first in [true, false] {
        let name = if lower_goes_in_first {
            "lower-first"
        } else {
            "lower-last"
        };
        let folder = support::folder(&format!("equal-distances-{name}"));
        let db = support::open(&folder);
        support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
        // In the first store the lower key of a pair is inserted first, and its context is
        // the first partition; in the second store it is inserted last.
        let in_order = |low: (i64, &'static str), high: (i64, &'static str)| {
            if lower_goes_in_first {
                [low, high]
            } else {
                [high, low]
            }
        };

        // Both rows of the first pair pending.
        let [first, second] = in_order((3, "x"), (9, "y"));
        insert(&db, first.0, first.1, pair(1));
        insert(&db, second.0, second.1, pair(1));
        let tx = db.transaction().expect("begin");
        filler(&tx, 20..=60);
        tx.commit().expect("commit");
        lower_first(&db, &pair(1), 3, 9, &format!("{name}, both pending"));

        // Both sealed.
        support::seal_all(&db);
        lower_first(&db, &pair(1), 3, 9, &format!("{name}, both sealed"));

        // Of the second pair, the row inserted first is sealed and the other is pending.
        let [first, second] = in_order((4, "x"), (8, "y"));
        insert(&db, first.0, first.1, pair(2));
        support::seal_all(&db);
        insert(&db, second.0, second.1, pair(2));
        let state = format!(
            "{name}, row {} sealed and row {} pending",
            first.0, second.0
        );
        lower_first(&db, &pair(2), 4, 8, &state);
        lower_first(&db, &pair(1), 3, 9, &state);

        // And once the second is sealed too, in a later block.
        support::seal_all(&db);
        lower_first(&db, &pair(2), 4, 8, &format!("{name}, all sealed"));
    }
}

#[test]
fn a_row_is_in_an_answer_once_after_its_vector_changes() {
    const DIMENSION: usize = 16;
    let folder = support::folder("in-an-answer-once");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 1..=60, DIMENSION, |n| format!("c{}", n % 3));
    tx.commit().expect("commit");
    support::seal_all(&db);

    // Every vector row 7 has had, and a vector no row has had.
    let mut had = vec![support::vector(7, DIMENSION)];
    let elsewhere = support::vector(60_000, DIMENSION);
    // With LIMIT above the row count every entry that leads to a row is visited. Row 7 is in
    // the answer once, at the distance of the vector it holds now, and no key is there twice.
    let once = |on: &dyn Run, had: &[Vec<f32>], state: &str| {
        let stored = support::stored(&on, support::DOCS);
        let now = had.last().expect("row 7 has a vector");
        for query in had.iter().chain([&elsewhere]) {
            let wanted = stored.nearest(|_| true, query, 1000);
            let sql = support::nearest_sql("docs", "v", None, 1000);
            let result = support::assert_nearest(&on, &sql, query, &wanted, state);
            let found = support::found(&result, &sql);
            let mut keys = ids(&found);
            let of_row_7: Vec<&Near> = found
                .iter()
                .filter(|near| near.key == Value::Int64(7))
                .collect();
            assert_eq!(of_row_7.len(), 1, "{state}: row 7 in one answer");
            assert_eq!(
                of_row_7[0].distance.to_bits(),
                support::distance(now, query).to_bits(),
                "{state}: the distance of row 7"
            );
            keys.sort_unstable();
            keys.dedup();
            assert_eq!(
                keys.len(),
                found.len(),
                "{state}: a key twice in one answer"
            );
            // A search for one row, with a vector the row no longer has: what a slot left
            // behind would answer best.
            let sql = support::nearest_sql("docs", "v", None, 1);
            support::assert_nearest(&on, &sql, query, &wanted[..1], state);
        }
    };

    once(&db, &had, "sealed");
    for change in 1..=2 {
        had.push(support::vector(60_000 + change, DIMENSION));
        set_vector(&db, 7, had.last().cloned());
        once(&db, &had, &format!("after change {change} of the vector"));
    }
    // After the next cycle two slots of row 7 are in blocks: the first and the newest.
    assert_eq!(
        db.run_maintenance_cycle()
            .expect("a cycle")
            .vector
            .maintenance_vectors_done,
        1
    );
    once(&db, &had, "two slots of the row in blocks");

    // Under another partition key, searched over all partitions.
    support::run(&db, "UPDATE docs SET ctx = 'c0' WHERE id = 7");
    once(&db, &had, "after the row moved to another partition");
    let stored = support::stored(&db, support::DOCS);
    for (ctx, there) in [("c0", true), ("c1", false)] {
        let now = had.last().expect("row 7 has a vector");
        let wanted = stored.nearest(|row| row[1] == support::text(ctx), now, 1000);
        let filter = format!("ctx = '{ctx}'");
        let sql = support::nearest_sql("docs", "v", Some(&filter), 1000);
        support::assert_nearest(&db, &sql, now, &wanted, "one partition after the move");
        assert_eq!(ids(&wanted).contains(&7), there, "row 7 under {ctx}");
    }
    support::seal_all(&db);
    once(&db, &had, "three slots of the row in blocks");

    // Inside an open transaction that has changed the vector of a sealed row, row 7, and of a
    // pending row, row 100, searched through the transaction before it commits.
    support::insert_docs(&db, 100..=100, DIMENSION, |_| "c1".to_owned());
    let tx = db.transaction().expect("begin");
    had.push(support::vector(60_003, DIMENSION));
    set_vector(&tx, 7, had.last().cloned());
    set_vector(&tx, 100, Some(support::vector(60_100, DIMENSION)));
    once(&tx, &had, "inside a transaction, changed once");
    had.push(support::vector(60_004, DIMENSION));
    set_vector(&tx, 7, had.last().cloned());
    set_vector(&tx, 100, Some(support::vector(60_101, DIMENSION)));
    once(&tx, &had, "inside a transaction, changed twice");
    tx.commit().expect("commit");
    once(&db, &had, "after the transaction committed");
    support::seal_all(&db);
    once(&db, &had, "after everything was sealed again");
}

#[test]
fn the_distance_is_the_expression_in_the_select_list() {
    const DIMENSION: usize = 16;
    const K: usize = 7;
    let folder = support::folder("distance-in-select-list");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "");
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 1..=200, DIMENSION, |_| "a".to_owned());
    tx.commit().expect("commit");
    let query = support::vector(70_000, DIMENSION);
    // The name this engine gives a select item that was given none.
    let unnamed = support::run(&db, "SELECT id, id + 1 FROM docs LIMIT 1").columns()[1].clone();

    let check = |state: &str| {
        let stored = support::stored(&db, support::DOCS);
        let wanted = stored.nearest(|_| true, &query, K);
        let distances: Vec<Value> = wanted
            .iter()
            .map(|near| Value::Float64(near.distance))
            .collect();
        let similarities: Vec<Value> = wanted
            .iter()
            .map(|near| Value::Float64(1.0 - near.distance))
            .collect();
        // A statement's rows as its columns, each column a list of values.
        let columns_of = |sql: &str, names: &[&str]| -> Vec<Vec<Value>> {
            let result = match support::search(&db, sql, &query) {
                Ok(result) => result,
                Err(error) => panic!("{state}: {sql}\nwas refused: {error:?}"),
            };
            assert_eq!(result.columns(), names, "{state}: the columns of {sql}");
            assert_eq!(result.rows().len(), K, "{state}: the rows of {sql}");
            (0..names.len())
                .map(|at| result.rows().iter().map(|row| row[at].clone()).collect())
                .collect()
        };
        // Two lists of values are the same when every REAL is the same to the bit.
        let same = |found: &[Value], wanted: &[Value], what: &str| {
            let bits = |values: &[Value]| -> Vec<String> {
                let values = values.iter();
                values
                    .map(|value| match value {
                        Value::Float64(real) => format!("{:#018x}", real.to_bits()),
                        other => format!("{other:?}"),
                    })
                    .collect()
            };
            assert_eq!(bits(found), bits(wanted), "{state}: {what}");
        };
        let keys: Vec<Value> = wanted.iter().map(|near| near.key.clone()).collect();

        // The distance under a name, ordered by the name or by the expression, ASC written
        // or not: the same rows in the same order, each with its distance.
        for order in ["distance", "v <=> $q", "distance ASC", "v <=> $q ASC"] {
            let sql =
                format!("SELECT id, v <=> $q AS distance FROM docs ORDER BY {order} LIMIT {K}");
            let columns = columns_of(&sql, &["id", "distance"]);
            same(&columns[0], &keys, &sql);
            same(&columns[1], &distances, &sql);
        }
        // Under no name.
        let sql = format!("SELECT id, v <=> $q FROM docs ORDER BY v <=> $q LIMIT {K}");
        let columns = columns_of(&sql, &["id", unnamed.as_str()]);
        same(&columns[0], &keys, &sql);
        same(&columns[1], &distances, &sql);
        // Twice in one list, before and after the key.
        let sql = format!(
            "SELECT v <=> $q AS near, id, v <=> $q AS again FROM docs ORDER BY near LIMIT {K}"
        );
        let columns = columns_of(&sql, &["near", "id", "again"]);
        same(&columns[0], &distances, &sql);
        same(&columns[1], &keys, &sql);
        same(&columns[2], &distances, &sql);
        // Inside an expression: the similarity is one minus the distance.
        let sql = format!(
            "SELECT id, 1 - (v <=> $q) AS similarity FROM docs ORDER BY v <=> $q LIMIT {K}"
        );
        let columns = columns_of(&sql, &["id", "similarity"]);
        same(&columns[0], &keys, &sql);
        same(&columns[1], &similarities, &sql);
        let sql = format!(
            "SELECT id, v <=> $q AS distance, 1 - (v <=> $q) AS similarity FROM docs \
             ORDER BY distance LIMIT {K}"
        );
        let columns = columns_of(&sql, &["id", "distance", "similarity"]);
        same(&columns[0], &keys, &sql);
        same(&columns[1], &distances, &sql);
        same(&columns[2], &similarities, &sql);

        // A search that selects no distance returns none.
        let sql = format!("SELECT id FROM docs ORDER BY v <=> $q LIMIT {K}");
        same(&columns_of(&sql, &["id"])[0], &keys, &sql);
        let sql = format!("SELECT tag, id FROM docs ORDER BY v <=> $q LIMIT {K}");
        same(&columns_of(&sql, &["tag", "id"])[1], &keys, &sql);
        // SELECT * returns the table's columns and nothing more: each row whole.
        let sql = format!("SELECT * FROM docs ORDER BY v <=> $q LIMIT {K}");
        let columns = columns_of(&sql, &["id", "ctx", "tag", "v"]);
        same(&columns[0], &keys, &sql);
        for (at, key) in keys.iter().enumerate() {
            let row = stored.rows().iter().find(|row| row[0] == *key);
            let row = row.expect("a row of the answer is a row of the table");
            let found: Vec<Value> = columns.iter().map(|column| column[at].clone()).collect();
            assert!(found == *row, "{state}: row {key:?} of {sql}");
        }
    };

    check("unsealed");
    support::seal_all(&db);
    check("sealed");
}

// ---- answers where the search has to look twice, and where two entries lead to one row ----

/// The vector the two tests below search for: one along the first of eight axes.
const AHEAD: [f32; 8] = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

/// A vector whose similarity to [`AHEAD`] is `similarity`, as nearly as 32 bits hold it: that
/// much along the first axis and the rest along one of the other seven. The slot a block keeps
/// for such a vector proves its similarity to within one hundredth, so two vectors a tenth
/// apart are never mistaken for one another by a search that only looked at slots.
fn toward(similarity: f32, axis: i64) -> Vec<f32> {
    let mut vector = vec![0.0_f32; AHEAD.len()];
    vector[0] = similarity;
    vector[1 + (axis % 7) as usize] = (1.0 - similarity * similarity).sqrt();
    vector
}

/// Inserts rows of `docs` under one context: row `first + n` holds the vector at the
/// similarity `nearest - step * n`.
fn insert_toward(on: &impl Run, ctx: &str, first: i64, count: i64, nearest: f32, step: f32) {
    for n in 0..count {
        let vector = toward(nearest - step * n as f32, first + n);
        if let Err(error) = support::insert_doc(on, first + n, ctx, Some(vector)) {
            panic!("row {} was refused: {error:?}", first + n);
        }
    }
}

#[test]
fn a_search_that_has_to_look_twice_returns_the_true_nearest() {
    let folder = support::folder("search-looks-twice");
    let db = support::open(&folder);
    support::declare_docs(&db, AHEAD.len(), "");
    // Ten rows very near the vector searched for, ten at about half its similarity and thirty
    // far from it, all in one block.
    let tx = db.transaction().expect("begin");
    insert_toward(&tx, "near", 1, 10, 0.99, 0.005);
    insert_toward(&tx, "mid", 11, 10, 0.50, 0.01);
    insert_toward(&tx, "far", 21, 30, 0.10, 0.02);
    tx.commit().expect("commit");
    assert_eq!(support::seal_all(&db), 50);

    // The ten nearest rows of the table are the near ones, and the ten nearest of the rest are
    // the ones at about half.
    let stored = support::stored(&db, support::DOCS);
    assert_eq!(
        ids(&stored.nearest(|_| true, &AHEAD, 10)),
        (1..=10).collect::<Vec<i64>>()
    );
    let not_near = |row: &[Value]| row[1] != support::text("near");
    assert_eq!(
        ids(&stored.nearest(not_near, &AHEAD, 10)),
        (11..=20).collect::<Vec<i64>>()
    );

    // A filter lets none of the near rows through. Their slots still say that ten vectors
    // lie very near, which rules the slots of every other row out: the first look visits the
    // near rows, is told each does not pass, and ends with no row at all. Only a second look,
    // at the slots the first one left out, finds the answer. For three rows, ten, and fifteen,
    // where the first look ends with some rows and not enough.
    let filter = Some("ctx <> 'near'");
    let through = |on: &dyn Run, stored: &support::Stored, state: &str| {
        for k in [3, 10, 15] {
            let wanted = stored.nearest(not_near, &AHEAD, k);
            assert_eq!(wanted.len(), k, "{state}: rows that pass the filter");
            let sql = support::nearest_sql("docs", "v", filter, k);
            support::assert_nearest(&on, &sql, &AHEAD, &wanted, state);
        }
    };
    through(&db, &stored, "near rows filtered out, everything sealed");

    // Ten rows more, between the far ones and the ones at half, wait on the pending list. They
    // are read first and fill an answer of ten, so the first look ends with a full answer of
    // the wrong rows: the rows at half are nearer, and their slots were ruled out by the near
    // rows' slots.
    let tx = db.transaction().expect("begin");
    insert_toward(&tx, "late", 51, 10, 0.30, 0.01);
    tx.commit().expect("commit");
    let stored = support::stored(&db, support::DOCS);
    let wanted = stored.nearest(not_near, &AHEAD, 10);
    assert_eq!(ids(&wanted), (11..=20).collect::<Vec<i64>>());
    through(&db, &stored, "near rows filtered out, ten rows pending");
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    through(&reader, &stored, "the same through a read session");

    // The same without a filter once the near rows are deleted: their slots stay in the block,
    // and no row answers for them.
    assert_eq!(
        support::run(&db, "DELETE FROM docs WHERE ctx = 'near'").rows_affected(),
        10
    );
    let stored = support::stored(&db, support::DOCS);
    assert_eq!(stored.rows().len(), 50);
    for k in [3, 10, 15] {
        let wanted = stored.nearest(|_| true, &AHEAD, k);
        assert_eq!(ids(&wanted)[..3], [11, 12, 13]);
        let sql = support::nearest_sql("docs", "v", None, k);
        for on in [&db as &dyn Run, &reader as &dyn Run] {
            support::assert_nearest(&on, &sql, &AHEAD, &wanted, "near rows deleted");
        }
    }
}

#[test]
fn a_row_two_entries_lead_to_is_in_the_answer_once_and_pushes_no_row_out() {
    // Every search here is for ten rows, and each store is built so that a row of the answer
    // is reached a second time when the answer already holds ten rows. Reached twice, the row
    // must stay in the answer once, and the tenth row must stay too.
    const K: usize = 10;
    let nearest_ten = |on: &dyn Run, state: &str| -> (Vec<i64>, u64) {
        let stored = support::stored(&on, support::DOCS);
        let wanted = stored.nearest(|_| true, &AHEAD, K);
        assert_eq!(wanted.len(), K, "{state}: the rows of the reference");
        let sql = support::nearest_sql("docs", "v", None, K);
        let result = support::assert_nearest(&on, &sql, &AHEAD, &wanted, state);
        (ids(&wanted), result.rows_examined())
    };

    // A row a transaction has staged, whose pending record is still there. Twenty rows wait on
    // the pending list; row 20 is the nearest and has the greatest key. A transaction changes
    // another column of it, so its search reads row 20 as a staged row, then the twenty
    // pending rows in key order, row 20 last of all.
    let folder = support::folder("reached-twice-staged-and-pending");
    let db = support::open(&folder);
    support::declare_docs(&db, AHEAD.len(), "");
    let tx = db.transaction().expect("begin");
    insert_toward(&tx, "a", 1, 19, 0.88, 0.02);
    insert_toward(&tx, "a", 20, 1, 0.95, 0.0);
    tx.commit().expect("commit");
    let (committed, examined) = nearest_ten(&db, "twenty pending rows");
    assert_eq!(committed[0], 20);
    assert_eq!(
        examined,
        20 + K as u64,
        "twenty pending rows and ten returned"
    );
    let tx = db.transaction().expect("begin");
    support::run(&tx, "UPDATE docs SET tag = 99 WHERE id = 20");
    let (inside, examined) = nearest_ten(&tx, "row 20 staged over its pending record");
    assert_eq!(inside, committed);
    // One staged row, twenty pending rows, ten returned: row 20 was reached twice.
    assert_eq!(examined, 1 + 20 + K as u64);
    tx.rollback();

    // A row on the pending list whose earlier vector has a slot. Twenty far rows and row 30
    // are in a block. Then nine rows arrive on the pending list, and row 30 is given a vector
    // that makes it the third nearest: the ten pending rows are the answer, and they fill it
    // before any slot is looked at. The slot row 30 left behind is of a vector nearer than
    // the tenth row, so it is visited, and leads to row 30 again.
    let folder = support::folder("reached-twice-pending-and-slot");
    let db = support::open(&folder);
    support::declare_docs(&db, AHEAD.len(), "");
    let tx = db.transaction().expect("begin");
    insert_toward(&tx, "a", 1, 20, 0.40, 0.02);
    insert_toward(&tx, "a", 30, 1, 0.81, 0.0);
    tx.commit().expect("commit");
    assert_eq!(support::seal_all(&db), 21);
    let tx = db.transaction().expect("begin");
    insert_toward(&tx, "a", 41, 2, 0.90, 0.02);
    insert_toward(&tx, "a", 43, 7, 0.84, 0.02);
    tx.commit().expect("commit");
    set_vector(&db, 30, Some(toward(0.86, 3)));
    let (answer, _) = nearest_ten(&db, "row 30 pending with a slot of its earlier vector");
    assert_eq!(answer, [41, 42, 30, 43, 44, 45, 46, 47, 48, 49]);
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    nearest_ten(&reader, "the same through a read session");

    // Two slots of one vector, one of them left behind. Rows 1 to 8, row 20, row 50 and
    // twenty far rows are in a block; rows 20 and 50 hold the same vector, so their slots are
    // the same bytes. Then row 50 is given a vector that makes it the nearest. The answer is
    // row 50, rows 1 to 8 and row 20: row 20 is its tenth row, and the slot row 50 left
    // behind is held back beside the slot of row 20, proving the same range.
    let folder = support::folder("reached-twice-slot-of-an-earlier-vector");
    let db = support::open(&folder);
    support::declare_docs(&db, AHEAD.len(), "");
    let tx = db.transaction().expect("begin");
    insert_toward(&tx, "a", 1, 8, 0.90, 0.02);
    for id in [20, 50] {
        if let Err(error) = support::insert_doc(&tx, id, "a", Some(toward(0.60, 3))) {
            panic!("row {id} was refused: {error:?}");
        }
    }
    insert_toward(&tx, "a", 60, 20, 0.40, 0.02);
    tx.commit().expect("commit");
    assert_eq!(support::seal_all(&db), 30);
    set_vector(&db, 50, Some(toward(0.95, 5)));
    let (answer, _) = nearest_ten(
        &db,
        "row 50 pending, its earlier slot beside the slot of row 20",
    );
    assert_eq!(answer, [50, 1, 2, 3, 4, 5, 6, 7, 8, 20]);
    // And once row 50's new vector is in the block too.
    assert_eq!(support::seal_all(&db), 1);
    let (answer, _) = nearest_ten(&db, "two slots of row 50 in the block");
    assert_eq!(answer, [50, 1, 2, 3, 4, 5, 6, 7, 8, 20]);
}
