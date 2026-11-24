//! The index of a VECTOR column: a partition of a column that is large enough is cut into
//! groups, and a search under INDEXED, or under AUTO from `INDEXING_THRESHOLD` vectors on, looks at
//! the nearest groups first. Every row it returns has its true distance; a column below its
//! size is searched whole and pays nothing; and INDEXED is refused by name where the index
//! cannot serve the statement.

mod support;
use support::{Run, count};

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::{DatabaseOpenOptions, Error, QueryResult, ReadSession, Value, params};

const DIMENSION: usize = 16;

/// The clause under which a partition of 2,000 rows is cut into groups.
const GROUPED: &str = "INDEXING_THRESHOLD 1000";

/// Question `j`: a vector near one of the eight directions the rows are scattered around.
fn question(j: i64) -> Vec<f32> {
    support::gathered(50_000 + j, DIMENSION)
}

fn all(_: &[Value]) -> bool {
    true
}

fn gathered(n: i64) -> Vec<f32> {
    support::gathered(n, DIMENSION)
}

fn lists(partition: &support::Shown) -> Value {
    support::field(partition, "lists").clone()
}

#[test]
fn an_indexed_search_does_fewer_distance_evaluations_than_exact() {
    let (_, db) = support::built("indexed-fewer-evaluations", DIMENSION, GROUPED, 2000);
    assert_eq!(lists(&support::the_partition(&db)), Value::Int64(45));
    let stored = support::stored(&db, support::DOCS);
    let (mut work, mut found) = (0, 0);
    for j in 0..20 {
        let query = question(j);
        let wanted = stored.nearest(all, &query, 10);
        let sql = support::by_mode(None, "EXACT", 10);
        let exact = support::assert_nearest(&db, &sql, &query, &wanted, "EXACT");
        let indexed = support::answer(&db, &support::by_mode(None, "INDEXED", 10), &query);
        let what = format!("INDEXED, question {j}");
        let answer = support::assert_sound(&indexed, &stored, all, &query, 10, &what);
        work += indexed.distance_evaluations();
        found += answer.iter().filter(|near| wanted.contains(near)).count();
        assert!(
            indexed.distance_evaluations() < exact.distance_evaluations(),
            "{what}: {} distance evaluations, EXACT {}",
            indexed.distance_evaluations(),
            exact.distance_evaluations()
        );
    }
    // A filter that lets half the rows through: every question still finds EXACT's ten rows.
    let half = |row: &[Value]| matches!(row[2], Value::Int64(tag) if tag < 25);
    for j in 0..20 {
        let query = question(j);
        let wanted = stored.nearest(half, &query, 10);
        let exact = support::answer(
            &db,
            &support::by_mode(Some("tag < 25"), "EXACT", 10),
            &query,
        );
        for mode in ["INDEXED", "AUTO"] {
            let sql = support::by_mode(Some("tag < 25"), mode, 10);
            let found = support::assert_nearest(&db, &sql, &query, &wanted, mode);
            let less = found.distance_evaluations() < exact.distance_evaluations();
            assert!(mode == "AUTO" || less, "INDEXED does less work than EXACT");
        }
    }
    // A plain search follows the column's mode; a cursor opened before the ALTER keeps its own,
    // and a session's next statement takes the new one.
    let (plain, query) = (support::by_mode(None, "", 10), question(0));
    let session = db.read_session(Default::default()).unwrap();
    let evals =
        |on: &dyn support::Run, sql: &str| support::answer(&on, sql, &query).distance_evaluations();
    let exact = evals(&db, &support::by_mode(None, "EXACT", 10));
    let before = evals(&session, &plain);
    let mut cursor = session
        .open_cursor(&plain, &params! { "q" => query.clone() })
        .unwrap();
    support::run(&db, "ALTER TABLE docs ALTER COLUMN v SET SEARCH_MODE EXACT");
    assert_eq!(evals(&db, &plain), exact, "a plain search is EXACT's work");
    assert_eq!(
        evals(&session, &plain),
        exact,
        "the session's next statement"
    );
    let opened = cursor.fetch(None).unwrap().distance_evaluations();
    assert_eq!(
        opened, before,
        "the cursor opened before keeps the older mode"
    );
    assert!(before < exact, "the older mode does less");
    // Recorded from the first run: their work, and how many of EXACT's 200 rows they found.
    let recorded = (15_573, 200);
    assert_eq!((work, found), recorded, "INDEXED's work, rows found");
}

#[test]
fn a_column_below_its_size_pays_nothing_and_is_searched_whole() {
    let (_, db) = support::built("below-its-size", DIMENSION, "", 2000);
    // The three modes return the reference answer, and INDEXED does EXACT's work.
    let agree = |k: usize, rows: usize, state: &str| {
        let stored = support::stored(&db, support::DOCS);
        for j in 0..3 {
            let query = question(j);
            let wanted = stored.nearest(all, &query, k);
            assert_eq!(wanted.len(), rows, "{state}");
            let work = ["EXACT", "AUTO", "INDEXED"].map(|mode| {
                let sql = support::by_mode(None, mode, k);
                support::assert_nearest(&db, &sql, &query, &wanted, state).distance_evaluations()
            });
            assert_eq!(
                work[2], work[0],
                "{state}: INDEXED's evaluations and EXACT's"
            );
        }
    };
    let index_bytes = || {
        let index = support::shown(&db, "SHOW VECTOR_INDEXES").remove(0);
        support::whole(&index, "durable_index_bytes")
    };
    assert_eq!(lists(&support::the_partition(&db)), Value::Null);
    assert_eq!(index_bytes(), 0);
    agree(10, 10, "2,000 rows sealed");

    // A fifth of the rows leave their slots: the one build such a column gets drops them.
    assert_eq!(
        support::run(&db, "DELETE FROM docs WHERE tag < 10").rows_affected(),
        400
    );
    support::maintenance(&db);
    let partition = support::the_partition(&db);
    assert_eq!(support::whole(&partition, "tombstones"), 0);
    assert_eq!(lists(&partition), Value::Int64(1));
    assert_eq!(index_bytes(), 28, "the record of an IVF of one group");
    agree(10, 10, "a fifth deleted");
    agree(5000, 1600, "a LIMIT above the rows that are left");
}

#[test]
fn indexed_is_served_at_every_size() {
    let folder = support::folder("indexed-every-size");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, GROUPED);
    let query = question(0);
    let sql = support::by_mode(None, "INDEXED", 10);
    let mut next = 1;
    for upto in [1, 10, 1999] {
        support::load_docs(&db, next..=upto, "a", gathered);
        next = upto + 1;
        // With no IVF the answer is the exact one.
        let wanted = support::stored(&db, support::DOCS).nearest(all, &query, 10);
        let state = format!("{upto} rows and no cycle");
        support::assert_nearest(&db, &sql, &query, &wanted, &state);
    }
    support::maintenance(&db);
    assert_eq!(lists(&support::the_partition(&db)), Value::Int64(45));
    let stored = support::stored(&db, support::DOCS);
    let result = support::answer(&db, &sql, &query);
    support::assert_sound(&result, &stored, all, &query, 10, "1,999 rows grouped");
}

/// One partition built and one given 200 rows since the last cycle, which is twice the
/// 100 a column of these clauses may have outside an IVF. `indexed` is how the search
/// asks for the index: `INDEXED`, or nothing on a column declared so.
fn refused_whole_while_one_partition_is_behind(name: &str, clauses: &str, indexed: &str) {
    let (_, db) = support::built(name, DIMENSION, clauses, 200);
    support::load_docs(&db, 201..=400, "b", gathered);
    let query = question(1);
    let both = support::by_mode(None, indexed, 10);
    let before = support::watermark(&db);
    support::assert_indexed_refused(support::search(&db, &both, &query), name);
    assert_eq!(support::watermark(&db), before, "{name}: a refused search");

    let stored = support::stored(&db, support::DOCS);
    let in_a = |row: &[Value]| row[1] == support::text("a");
    let alone = support::by_mode(Some("ctx = 'a'"), indexed, 10);
    let result = support::answer(&db, &alone, &query);
    support::assert_sound(
        &result,
        &stored,
        in_a,
        &query,
        10,
        "the built partition alone",
    );
    let wanted = stored.nearest(all, &query, 10);
    let sql = support::by_mode(None, "EXACT", 10);
    support::assert_nearest(&db, &sql, &query, &wanted, "EXACT");
    // AUTO searches the built partition as INDEXED does and the other exactly: ten rows with
    // their true distances, nearest first, and of the partition with no IVF every row
    // nearer than the last of them.
    let result = support::answer(&db, &support::by_mode(None, "AUTO", 10), &query);
    let answer = support::assert_sound(&result, &stored, all, &query, 10, "AUTO over both");
    let in_b = |row: &[Value]| row[1] == support::text("b");
    for near in stored.nearest(in_b, &query, 10) {
        assert!(
            near.distance >= answer[9].distance || answer.contains(&near),
            "{name}: AUTO left out {near:?} of the partition it searches exactly; it answered \
             {answer:?}"
        );
    }

    support::maintenance(&db);
    let result = support::answer(&db, &both, &query);
    support::assert_sound(&result, &stored, all, &query, 10, "after the cycles");
}

#[test]
fn indexed_is_refused_whole_when_one_partition_is_behind() {
    refused_whole_while_one_partition_is_behind(
        "behind-use-vector",
        "PARTITION_KEY (ctx) INDEXING_THRESHOLD 100",
        "INDEXED",
    );
    refused_whole_while_one_partition_is_behind(
        "behind-declared",
        "PARTITION_KEY (ctx) SEARCH_MODE INDEXED INDEXING_THRESHOLD 100",
        "",
    );
}

/// The answers and the counted work of one statement under AUTO and under EXACT.
fn auto_and_exact(
    db: &contextdb::Database,
    filter: Option<&str>,
    query: &[f32],
) -> [QueryResult; 2] {
    ["AUTO", "EXACT"].map(|mode| support::answer(db, &support::by_mode(filter, mode, 10), query))
}

#[test]
fn auto_uses_ivfs_from_indexing_threshold_on() {
    // Two built partitions of 1,200 under INDEXING_THRESHOLD 2000: the count is over the selected
    // partitions together.
    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 2000";
    let (_, db) = support::built("auto-threshold", DIMENSION, clauses, 1200);
    support::load_docs(&db, 1201..=2400, "b", gathered);
    support::maintenance(&db);
    let grouped: Vec<Value> = support::shown(&db, "SHOW VECTOR_PARTITIONS")
        .iter()
        .map(lists)
        .collect();
    assert_eq!(grouped, [Value::Int64(35), Value::Int64(35)]);
    let stored = support::stored(&db, support::DOCS);
    let query = question(2);
    let [auto, exact] = auto_and_exact(&db, Some("ctx = 'a'"), &query);
    assert_eq!(
        support::found(&auto, "AUTO"),
        support::found(&exact, "EXACT")
    );
    assert_eq!(auto.distance_evaluations(), exact.distance_evaluations());
    let [auto, exact] = auto_and_exact(&db, None, &query);
    support::assert_sound(&auto, &stored, all, &query, 10, "AUTO over 2,400");
    assert!(
        auto.distance_evaluations() < exact.distance_evaluations(),
        "AUTO over 2,400 vectors did {} distance evaluations, EXACT {}",
        auto.distance_evaluations(),
        exact.distance_evaluations()
    );
    let show = "SHOW VECTOR_PARTITIONS";
    let before = support::shown(&db, show);
    let sql = "ALTER TABLE docs ALTER COLUMN v SET INDEXING_THRESHOLD 3000";
    support::run(&db, sql);
    assert_eq!(support::shown(&db, show), before, "no cycle: rebuilt");

    // A column below its declared size stays ungrouped however many cycles run: 1,000 rows
    // under INDEXING_THRESHOLD 50000 are not 50,000, though one partition holds a hundredth of it.
    let index_bytes = |db: &contextdb::Database| {
        let index = support::shown(db, "SHOW VECTOR_INDEXES").remove(0);
        support::whole(&index, "durable_index_bytes")
    };
    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 50000";
    let (_, db) = support::built("auto-below-declared", DIMENSION, clauses, 400);
    support::load_docs(&db, 401..=1000, "b", gathered);
    for _ in 0..3 {
        support::maintenance(&db);
    }
    let grouped: Vec<Value> = support::shown(&db, "SHOW VECTOR_PARTITIONS")
        .iter()
        .map(lists)
        .collect();
    assert_eq!(grouped, [Value::Null, Value::Null]);
    assert_eq!(index_bytes(&db), 0);

    // With no INDEXING_THRESHOLD the number in force is 100,000: a partition of 1,200 stays
    // ungrouped, and AUTO over it is EXACT.
    let (_, db) = support::built("auto-undeclared", DIMENSION, "", 1200);
    let index = support::shown(&db, "SHOW VECTOR_INDEXES").remove(0);
    assert_eq!(
        support::field(&index, "declared_indexing_threshold"),
        &Value::Null
    );
    assert_eq!(
        support::whole(&index, "effective_indexing_threshold"),
        100_000
    );
    assert_eq!(lists(&support::the_partition(&db)), Value::Null);
    assert_eq!(index_bytes(&db), 0);
    let [auto, exact] = auto_and_exact(&db, None, &query);
    assert_eq!(
        support::found(&auto, "AUTO"),
        support::found(&exact, "EXACT")
    );
    assert_eq!(auto.distance_evaluations(), exact.distance_evaluations());
}

#[test]
fn auto_follows_the_threshold_in_force_and_indexed_does_not() {
    let (_, db) = support::built("auto-threshold-set", DIMENSION, GROUPED, 2000);
    let query = question(2);
    let ask = |mode: &str| {
        let sql = support::by_mode(None, mode, 10);
        let result = support::answer(&db, &sql, &query);
        (support::found(&result, mode), result.distance_evaluations())
    };
    let set = |to: &str| {
        let sql = format!("ALTER TABLE docs ALTER COLUMN v SET INDEXING_THRESHOLD {to}");
        support::run(&db, &sql);
    };
    let (indexed, exact) = (ask("INDEXED"), ask("EXACT"));
    assert!(indexed.1 < exact.1 && ask("AUTO").1 < exact.1);
    set("50000");
    assert_eq!(ask("AUTO"), exact, "AUTO is EXACT above the size");
    assert_eq!(ask("INDEXED"), indexed, "INDEXED ignores it");
    set("1000");
    assert!(ask("AUTO").1 < exact.1, "AUTO is indexed again");
    set("DEFAULT");
    assert_eq!(ask("AUTO"), exact, "the default puts it back");
}

#[test]
fn auto_searches_each_partition_by_what_its_ivf_saves() {
    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000";
    let (_, db) = support::built("auto-each-partition", DIMENSION, clauses, 2000);
    // Five pending rows under another context, nearer the question than any row of the first.
    let query = question(3);
    support::load_docs(&db, 9001..=9005, "b", |n| {
        let moved = support::vector(n, DIMENSION);
        query
            .iter()
            .zip(moved)
            .map(|(q, by)| q + 0.001 * by)
            .collect()
    });
    let stored = support::stored(&db, support::DOCS);
    let [auto, exact] = auto_and_exact(&db, None, &query);
    let answer = support::assert_sound(&auto, &stored, all, &query, 10, "AUTO over both");
    let pending = |near: &&support::Near| matches!(near.key, Value::Int64(id) if id > 9000);
    assert_eq!(answer.iter().filter(pending).count(), 5, "{answer:?}");
    // Recorded from the first run, AUTO's below EXACT's: another count is another search.
    let work = [&auto, &exact].map(|found| found.distance_evaluations());
    let wanted = stored.nearest(all, &query, 10);
    let found = answer.iter().filter(|near| wanted.contains(near)).count();
    let recorded = ([790, 2_244], 10);
    assert_eq!((work, found), recorded, "work of AUTO, EXACT; rows found");
    support::run(&db, "DROP TABLE docs");
    support::declare_docs(&db, 4, "PARTITION_KEY (ctx) INDEXING_THRESHOLD 4");
    support::load_docs(&db, 1..=20, "a", |n| support::vector(n, 4));
    support::maintenance(&db);
    support::load_docs(&db, 21..=25, "b", |n| support::vector(n, 4));
    support::run(&db, "UPDATE docs SET tag = 99 WHERE id >= 24");
    support::cycle(&db);
    let direct = ["AUTO", "EXACT"].map(|mode| {
        support::answer(
            &db,
            &support::by_mode(Some("tag = 99"), mode, 5),
            &support::vector(3, 4),
        )
    });
    assert_eq!(
        support::found(&direct[0], "AUTO"),
        support::found(&direct[1], "EXACT")
    );
    assert_eq!(direct.each_ref().map(|r| r.rows_examined()), [27, 27]);
    let b = &support::shown(&db, "SHOW VECTOR_PARTITIONS")[1];
    assert_eq!(support::word(b, "availability_reason"), "none");
    let column = &support::shown(&db, "SHOW VECTOR_INDEXES")[0];
    assert_eq!(support::whole(column, "unavailable_partitions"), 0);
}

#[test]
fn auto_answers_exactly_where_a_ivf_is_behind_or_damaged() {
    let (_, db) = support::built("auto-behind", DIMENSION, GROUPED, 2000);
    // 800 vectors outside the IVF: twice the 400 at which it is grouped again.
    support::load_docs(&db, 2001..=2800, "a", gathered);
    let query = question(4);
    let wanted = support::stored(&db, support::DOCS).nearest(all, &query, 10);
    let sql = support::by_mode(None, "INDEXED", 10);
    support::assert_indexed_refused(support::search(&db, &sql, &query), "a ivf behind");
    let partition = support::the_partition(&db);
    assert_eq!(
        support::word(&partition, "availability_reason"),
        "behind_writes"
    );
    let work = ["EXACT", "AUTO"].map(|mode| {
        let sql = support::by_mode(None, mode, 10);
        support::assert_nearest(&db, &sql, &query, &wanted, mode).distance_evaluations()
    });
    assert_eq!(work[1], work[0], "AUTO's evaluations and EXACT's");
}

#[test]
fn auto_over_a_partition_behind_its_writes_is_held_to_the_exact_work_and_refused_below_it() {
    let (_, db) = support::built("auto-budget", DIMENSION, GROUPED, 2000);
    support::load_docs(&db, 2001..=2800, "a", gathered);
    let query = question(4);
    for filter in [None, Some("tag < 3")] {
        let exact = support::answer(&db, &support::by_mode(filter, "EXACT", 10), &query);
        let cost = exact.rows_examined() + exact.distance_evaluations();
        let auto = support::by_mode(filter, "AUTO", 10);
        let within = |work| {
            db.read_session(contextdb::ReadLimits {
                work,
                ..Default::default()
            })
        };
        let answered = support::answer(&within(cost).unwrap(), &auto, &query);
        assert_eq!(
            support::found(&answered, "AUTO"),
            support::found(&exact, "EXACT")
        );
        let refused = support::search(&within(cost - 1).unwrap(), &auto, &query);
        assert!(
            matches!(refused, Err(Error::ReadLimitExceeded { .. })),
            "{filter:?}: {refused:?}"
        );
    }
}

#[test]
fn a_column_with_no_mode_declared_searches_as_auto_at_ten_thousand_vectors() {
    let (_, db) = support::built("auto-10k", DIMENSION, "INDEXING_THRESHOLD 1000", 10_000);
    let stored = support::stored(&db, support::DOCS);
    for j in 0..20 {
        let (query, wanted) = (question(j), stored.nearest(all, &question(j), 10));
        let [plain, auto, exact] = ["", "AUTO", "EXACT"].map(|mode| {
            let sql = support::by_mode(None, mode, 10);
            support::assert_nearest(&db, &sql, &query, &wanted, mode).distance_evaluations()
        });
        assert_eq!(plain, auto, "no clause is AUTO");
        assert!(auto < exact, "AUTO does less work than EXACT");
    }
}

#[test]
fn writes_after_a_ivf_are_found_at_once() {
    let (folder, db) = support::built("writes-after-ivf", DIMENSION, GROUPED, 2000);
    let gone = gathered(7);
    let inserted = support::vector(70_001, DIMENSION);
    let changed = support::vector(70_002, DIMENSION);
    support::insert_doc(&db, 3001, "a", Some(inserted.clone())).expect("insert a row");
    db.execute(
        "UPDATE docs SET v = $v WHERE id = 5",
        &params! { "v" => changed.clone() },
    )
    .expect("change a vector");
    support::run(&db, "DELETE FROM docs WHERE id = 7");
    let check = |db: &contextdb::Database, state: &str| {
        let stored = support::stored(db, support::DOCS);
        for (query, id) in [(&inserted, 3001), (&changed, 5)] {
            let result = support::answer(db, &support::by_mode(None, "INDEXED", 1), query);
            let answer = support::assert_sound(&result, &stored, all, query, 1, state);
            assert_eq!(answer[0].key, Value::Int64(id), "{state}");
        }
        let result = support::answer(db, &support::by_mode(None, "INDEXED", 10), &gone);
        let answer = support::assert_sound(&result, &stored, all, &gone, 10, state);
        assert!(
            answer.iter().all(|near| near.key != Value::Int64(7)),
            "{state}: the deleted row is in {answer:?}"
        );
    };
    check(&db, "before any cycle");
    assert_eq!(count(support::cycle(&db).maintenance_vectors_done), 2);
    check(&db, "after sealing");
    let later = support::vector(70_003, DIMENSION);
    support::insert_doc(&db, 3002, "a", Some(later.clone())).unwrap();
    let db = support::reopen(db, &folder);
    assert!(support::whole(&support::the_partition(&db), "pending_tuples") == 1);
    check(&db, "after the restart");
    let result = support::answer(&db, &support::by_mode(None, "INDEXED", 1), &later);
    assert!(support::found(&result, "INDEXED")[0].key == Value::Int64(3002));
}

#[test]
fn a_search_with_a_filter_on_rows_widens_until_it_is_whole() {
    let (_, db) = support::built("filter-widens", DIMENSION, GROUPED, 2000);
    let stored = support::stored(&db, support::DOCS);
    let query = question(5);
    let search = |db: &contextdb::Database, filter: Option<&str>, mode: &str, k: usize| {
        support::search(db, &support::by_mode(filter, mode, k), &query)
    };

    // A filter that passes every row costs what no filter costs.
    let plain = search(&db, None, "INDEXED", 10).expect("no filter");
    let passing = search(&db, Some("tag >= 0"), "INDEXED", 10).expect("every row passes");
    assert_eq!(
        support::found(&passing, "every row passes"),
        support::found(&plain, "no filter")
    );
    assert_eq!(passing.distance_evaluations(), plain.distance_evaluations());

    // Nine rows in ten pass. INDEXED answers ten rows the filter allows, nearest first, each
    // with the distance the exact route is held to for that row, and does less than the exact
    // route's work. Which ten is the index's to say: it may leave out a row the exact ten hold.
    let most = |row: &[Value]| matches!(row[2], Value::Int64(tag) if tag < 45);
    let wanted = stored.nearest(most, &query, 10);
    let sql = support::by_mode(Some("tag < 45"), "EXACT", 10);
    let exact = support::assert_nearest(&db, &sql, &query, &wanted, "EXACT");
    let indexed = search(&db, Some("tag < 45"), "INDEXED", 10).expect("nine in ten pass");
    let what = "INDEXED, nine rows in ten pass";
    support::assert_sound(&indexed, &stored, most, &query, 10, what);
    assert!(
        indexed.distance_evaluations() < exact.distance_evaluations(),
        "{what}: {} distance evaluations, EXACT {}",
        indexed.distance_evaluations(),
        exact.distance_evaluations()
    );

    // Twelve rows pass, spread over the groups, and ten are asked for. AUTO widens until it
    // has done the exact work and returns the ten nearest of them; INDEXED holds ten allowed
    // rows before the step that would take every group, and answers them.
    let tagged = |upto: i64| move |row: &[Value]| matches!((&row[0], &row[2]), (Value::Int64(id), Value::Int64(7)) if *id <= upto);
    let twelve = Some("tag = 7 AND id <= 600");
    assert_eq!(stored.nearest(tagged(600), &query, 100).len(), 12);
    let wanted = stored.nearest(tagged(600), &query, 10);
    let sql = support::by_mode(twelve, "AUTO", 10);
    support::assert_nearest(&db, &sql, &query, &wanted, "AUTO, twelve rows pass");
    let indexed = search(&db, twelve, "INDEXED", 10).expect("twelve rows pass");
    let what = "INDEXED, twelve rows pass";
    support::assert_sound(&indexed, &stored, tagged(600), &query, 10, what);

    // Six rows pass and ten are asked for: AUTO returns the six, and INDEXED, short of its
    // limit at the step that would take every group, is refused by name.
    let six = Some("tag = 7 AND id <= 300");
    let wanted = stored.nearest(tagged(300), &query, 10);
    assert_eq!(wanted.len(), 6);
    let sql = support::by_mode(six, "AUTO", 10);
    support::assert_nearest(&db, &sql, &query, &wanted, "AUTO, six rows pass");
    support::assert_indexed_refused(search(&db, six, "INDEXED", 10), "six rows pass");

    // Three rows pass: AUTO returns the three, and INDEXED is refused by name.
    let few = Some("tag = 7 AND id <= 150");
    let wanted = stored.nearest(tagged(150), &query, 10);
    assert_eq!(wanted.len(), 3);
    let sql = support::by_mode(few, "AUTO", 10);
    support::assert_nearest(&db, &sql, &query, &wanted, "AUTO, three rows pass");
    support::assert_indexed_refused(search(&db, few, "INDEXED", 10), "USE VECTOR INDEXED");

    // With no filter and a LIMIT above the rows, INDEXED returns every row.
    let every = search(&db, None, "INDEXED", 5000).expect("every row");
    support::assert_sound(&every, &stored, all, &query, 2000, "a LIMIT above the rows");

    // The indexed filter's AUTO work is bounded even when it must find every passing row.
    support::run(&db, "CREATE INDEX docs_tag ON docs (tag)");
    for upto in [1000, 1500, 2000] {
        let filter = format!("tag = 7 AND id <= {upto}");
        let wanted = stored.nearest(tagged(upto), &query, 100);
        let sql = support::by_mode(Some(&filter), "AUTO", wanted.len() + 1);
        let answer = support::assert_nearest(&db, &sql, &query, &wanted, &filter);
        assert!(
            answer.distance_evaluations() <= 17 * wanted.len() as u64,
            "{filter}: {answer:?}"
        );
    }

    let clauses = "SEARCH_MODE INDEXED INDEXING_THRESHOLD 1000";
    let (_, declared) = support::built("filter-widens-declared", DIMENSION, clauses, 2000);
    support::assert_indexed_refused(search(&declared, few, "", 10), "SEARCH_MODE INDEXED");
}

#[test]
fn an_indexed_search_that_takes_every_group_does_the_exact_evaluations() {
    let (_, db) = support::built("every-group-evaluations", DIMENSION, GROUPED, 2000);
    let stored = support::stored(&db, support::DOCS);
    let query = question(6);
    let evaluations = |mode: &str, k: usize, rows: usize| {
        let result = support::answer(&db, &support::by_mode(None, mode, k), &query);
        let what = format!("{mode} LIMIT {k}");
        support::assert_sound(&result, &stored, all, &query, rows, &what);
        result.distance_evaluations()
    };
    // Ten rows: the nearest groups, and fewer evaluations than the whole partition takes.
    let (indexed, exact) = (evaluations("INDEXED", 10, 10), evaluations("EXACT", 10, 10));
    let off = "INDEXING_THRESHOLD 18446744073709551615";
    let (_, flat) = support::built("every-group-flat", DIMENSION, off, 2000);
    let flat = support::answer(&flat, &support::by_mode(None, "EXACT", 10), &query);
    assert_eq!(flat.distance_evaluations(), exact, "no centres used");
    assert!(
        indexed < exact,
        "INDEXED did {indexed} distance evaluations for ten rows, EXACT {exact}"
    );
    // Every row: every group is taken. An evaluation is a centroid compared, a binary code
    // estimated, a slot screened or a row asked for its distance.
    let (indexed, exact) = (
        evaluations("INDEXED", 5000, 2000),
        evaluations("EXACT", 5000, 2000),
    );
    assert_eq!(
        exact,
        2 * 2000,
        "EXACT's distance evaluations for every row: each of the 2,000 slots is screened, \
         which is one, and each of the 2,000 rows is asked for its distance, which is one more"
    );
    assert!(
        indexed >= exact + 2000 + 45,
        "INDEXED did {indexed} distance evaluations for every row: it screens every slot and \
         asks every row as EXACT does ({exact}), after comparing its 45 centroids and estimating \
         the code of each of the 2,000 slots at least once"
    );
}

#[test]
fn an_indexed_search_short_of_its_limit_is_refused_before_the_whole_table_is_read() {
    // 128 rows spread evenly round a circle, in vectors wide enough for four blocks of 42
    // slots. The size is fixed by the blocks: a refusal reads less than the whole table only
    // when there are blocks for it to stop short of, and the filter needs its three rows
    // among the first 150; few rows in wide vectors make those with a short build.
    const WIDE: usize = 6144;
    const ROWS: i64 = 128;
    let folder = support::folder("indexed-refused-before-the-table");
    let db = support::open(&folder);
    support::declare_docs(&db, WIDE, "INDEXING_THRESHOLD 128");
    support::load_docs(&db, 1..=ROWS, "a", |n| support::ringed(n, ROWS, WIDE));
    support::maintenance(&db);
    assert_eq!(lists(&support::the_partition(&db)), Value::Int64(11));
    let stored = support::stored(&db, support::DOCS);
    support::close(db);

    // Three rows pass the filter and the statement asks for ten.
    let passes = |row: &[Value]| matches!((&row[0], &row[2]), (Value::Int64(id), Value::Int64(7)) if *id <= 150);
    let query = support::ringed(157, 2 * ROWS, WIDE);
    // With no filter both answer ten rows. EXACT reads every block; INDEXED reads what EXACT
    // does not, the centroids and the binary codes, and still reads less: it left blocks unread.
    let read_by = |mode: &str| -> u64 {
        let (bytes, result) = read_alone(&folder, &support::by_mode(None, mode, 10), &query);
        assert_eq!(result.expect(mode).rows().len(), 10, "rows under {mode}");
        bytes
    };
    let (exact, indexed) = (read_by("EXACT"), read_by("INDEXED"));
    assert!(
        indexed < exact,
        "INDEXED read {indexed} bytes of the data file and EXACT {exact}"
    );
    let wanted = stored.nearest(passes, &query, 10);
    assert_eq!(wanted.len(), 3);
    // Once by the key's range, once by the same rows through an expression that has none.
    for few in ["tag = 7 AND id <= 150", "tag = 7 AND id + 0 <= 150"].map(Some) {
        let read_by = |mode: &str| read_alone(&folder, &support::by_mode(few, mode, 10), &query);
        // AUTO asks the filter of every row, which is the whole table's work, and answers.
        let (whole, auto) = read_by("AUTO");
        let auto = auto.expect("AUTO, three rows pass");
        support::assert_same_rows(
            &support::found(&auto, "AUTO"),
            &wanted,
            "AUTO, three rows pass",
        );
        // INDEXED is refused at the step that would take every group: before that work.
        let (refused, indexed) = read_by("INDEXED");
        support::assert_indexed_refused(indexed, "INDEXED, three rows pass");
        assert!(
            refused < whole,
            "INDEXED was refused after reading {refused} bytes of the data file; AUTO, which asks \
             the filter of all {ROWS} rows, read {whole}"
        );
    }
}

/// Runs a search in a read session of its own, so every page it needs is read once, and gives
/// the bytes of the data file it read and its answer.
fn read_alone(folder: &Path, sql: &str, query: &[f32]) -> (u64, contextdb::Result<QueryResult>) {
    let (door, bytes) = support::counting_bytes_read();
    let mut options = support::reader_options(None);
    options.storage = Some(door);
    let reader = ReadSession::open_with_options(folder, options).expect("open for reading");
    let opened = bytes.load(Ordering::SeqCst);
    let outcome = support::search(&reader, sql, query);
    (bytes.load(Ordering::SeqCst) - opened, outcome)
}

/// One store of [`a_nearer_allowed_row_in_a_group_taken_later_is_in_the_answer`], with row
/// 507 at these two numbers.
fn the_nearer_allowed_row_is_first(name: &str, nearer: (f32, f32)) {
    // The question is the first direction. Rows 7, 57, .. 457 (ten rows with tag 7) lie
    // half way from it, each towards a direction of its own, and row 507 (tag 7 too) lies
    // nearer the question but nearer still to the second direction, among whose 500 rows
    // it is grouped. Every other row is gathered round one of four directions; those round
    // the first are the nearest rows of all, and none of them passes the filter.
    let fixed = |n: i64| -> Vec<f32> {
        let mut at = [0.0_f32; DIMENSION];
        match n {
            507 => (at[0], at[1]) = nearer,
            n if n % 50 == 7 && n < 507 => {
                at[0] = 0.5;
                at[2 + (n / 50) as usize] = 0.866;
            }
            n => at[[0, 1, 12, 13][n.rem_euclid(4) as usize]] = 1.0,
        }
        let moved = support::vector(n, DIMENSION);
        (0..DIMENSION).map(|i| at[i] + 0.02 * moved[i]).collect()
    };
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, GROUPED);
    support::load_docs(&db, 1..=2000, "a", fixed);
    support::maintenance(&db);
    let groups = lists(&support::the_partition(&db));
    assert!(
        matches!(groups, Value::Int64(groups) if groups > 1),
        "the partition has {groups:?} groups"
    );
    let stored = support::stored(&db, support::DOCS);
    let mut query = vec![0.0_f32; DIMENSION];
    query[0] = 1.0;

    let filter = Some("tag = 7 AND id <= 550");
    let passes = |row: &[Value]| matches!((&row[0], &row[2]), (Value::Int64(id), Value::Int64(7)) if *id <= 550);
    let wanted = stored.nearest(passes, &query, 10);
    assert_eq!(stored.nearest(passes, &query, 100).len(), 11);
    assert_eq!(wanted[0].key, Value::Int64(507), "the nearest allowed row");
    // AUTO answers ten of the eleven allowed rows with their true distances, and the
    // nearest of them first: it is nearer than every row the answer could hold in its place.
    let result = support::answer(&db, &support::by_mode(filter, "AUTO", 10), &query);
    let what = format!("{name}: AUTO, eleven rows pass");
    let answer = support::assert_sound(&result, &stored, passes, &query, 10, &what);
    assert_eq!(answer, wanted, "{what}");
}

#[test]
fn a_nearer_allowed_row_in_a_group_taken_later_is_in_the_answer() {
    the_nearer_allowed_row_is_first("nearer-row-in-a-later-group", (0.7, 0.714));
    // Nearer the second direction still, on the line half way between the two: the group it
    // is in lies four times as far behind the first as the row itself does.
    the_nearer_allowed_row_is_first("nearer-row-half-way-to-a-later-group", (0.55, 0.835));
}

#[test]
fn a_ivf_whose_codes_cannot_tell_its_vectors_apart_answers_as_exact() {
    // Every row has one of eight vectors, so each vector of a group is the group's centroid:
    // the binary code of every one is the same and the width measured for it is nothing.
    let same = |n: i64| -> Vec<f32> {
        let direction = n.rem_euclid(8) as usize;
        (0..DIMENSION)
            .map(|at| f32::from(u8::from(at / 2 == direction)) + 0.25)
            .collect()
    };
    let folder = support::folder("codes-all-the-same");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, GROUPED);
    support::load_docs(&db, 1..=2000, "a", same);
    support::maintenance(&db);
    let groups = lists(&support::the_partition(&db));
    assert!(
        matches!(groups, Value::Int64(groups) if groups > 1),
        "the partition has {groups:?} groups"
    );
    let stored = support::stored(&db, support::DOCS);
    let half = |row: &[Value]| matches!(row[2], Value::Int64(tag) if tag < 25);
    for direction in [0, 3, 7] {
        let query = same(direction);
        // Rows at one distance come in key order, so the ten are the same under every mode.
        let wanted = stored.nearest(all, &query, 10);
        let allowed = stored.nearest(half, &query, 10);
        for mode in ["EXACT", "AUTO", "INDEXED"] {
            let what = format!("{mode}, direction {direction}");
            let sql = support::by_mode(None, mode, 10);
            support::assert_nearest(&db, &sql, &query, &wanted, &what);
            let sql = support::by_mode(Some("tag < 25"), mode, 10);
            support::assert_nearest(&db, &sql, &query, &allowed, &format!("{what}, filtered"));
        }
    }
}

#[test]
fn a_search_with_no_room_for_its_codes_is_refused_by_the_memory_limit() {
    const MIB: usize = 1 << 20;
    let (folder, db) = support::built("no-room-for-codes", DIMENSION, GROUPED, 2000);
    assert_eq!(lists(&support::the_partition(&db)), Value::Int64(45));
    let stored = support::stored(&db, support::DOCS);
    support::close(db);

    // The smallest limit a store opens under leaves a search nothing to keep
    // the binary codes of its groups in.
    let query = question(7);
    let sql = support::by_mode(None, "INDEXED", 10);
    // Pay for SQL/planning first; the code-range workspace still cannot fit.
    let codes = 32 * DIMENSION.div_ceil(64) + 8 * (262_080 / (8 + DIMENSION));
    let limit = support::opening::opening_charge(&folder, None)
        + support::statement_text_bytes(&sql)
        + codes / 2;
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(limit);
    let db = support::open_with(&folder, options).expect("open under the smallest limit");
    let before = (support::used(&db), support::watermark(&db));
    match support::search(&db, &sql, &query) {
        Err(Error::MemoryBudgetExceeded {
            budget_limit_bytes,
            requested_bytes,
            ..
        }) => {
            assert_eq!(budget_limit_bytes, limit as u64);
            assert_eq!(
                requested_bytes, codes as u64,
                "the binary-code range workspace"
            );
        }
        other => panic!("{sql}\nunder the smallest limit got {other:?}"),
    }
    assert_eq!(
        (support::used(&db), support::watermark(&db)),
        (
            before.0 + support::form_charge(&sql.replace("LIMIT 10", "LIMIT ?")),
            before.1
        ),
        "after the refused search: the record's line for its form, and no more"
    );

    support::close(db);
    let db = support::open_limited(&folder, 64 * MIB);
    let result = support::answer(&db, &sql, &query);
    support::assert_sound(&result, &stored, all, &query, 10, "under 64 MiB");
}

#[test]
fn a_filter_of_partition_keys_only_is_no_filter_on_rows() {
    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000";
    let (_, db) = support::built("keys-only-filter", DIMENSION, clauses, 2000);
    support::load_docs(&db, 2001..=2005, "b", gathered);
    support::maintenance(&db);
    let stored = support::stored(&db, support::DOCS);
    let query = question(8);
    let in_b = |row: &[Value]| row[1] == support::text("b");

    // Five rows are selected and ten asked for: every one of them, and no refusal.
    let wanted = stored.nearest(in_b, &query, 10);
    assert_eq!(wanted.len(), 5);
    let sql = support::by_mode(Some("ctx IN ('b')"), "INDEXED", 10);
    support::assert_nearest(&db, &sql, &query, &wanted, "one small partition selected");
    // A tuple no row is under adds nothing and refuses nothing.
    let sql = support::by_mode(Some("ctx IN ('b', 'zz')"), "INDEXED", 10);
    support::assert_nearest(&db, &sql, &query, &wanted, "a tuple with no row beside it");
    // Both partitions and a LIMIT above their 2,005 rows: every row.
    let sql = support::by_mode(Some("ctx IN ('a', 'b')"), "INDEXED", 5000);
    let result = support::answer(&db, &sql, &query);
    support::assert_sound(
        &result,
        &stored,
        all,
        &query,
        2005,
        "both partitions selected",
    );
}

#[test]
fn a_read_that_fails_under_auto_is_returned_as_that_failure() {
    let (folder, db) = support::built("auto-read-fails", DIMENSION, GROUPED, 2000);
    support::close(db);
    let query = question(9);
    let sql = support::by_mode(None, "AUTO", 10);
    // A search in a session of its own, whose storage door fails read number `fail` of the
    // data file; 0 fails none. Returns the reads made before the search and by its end.
    let searched = |fail: u64| -> (u64, u64, contextdb::Result<QueryResult>) {
        let reads = Arc::new(AtomicU64::new(0));
        let mut options = support::reader_options(None);
        options.storage = Some(support::failing_storage({
            let reads = reads.clone();
            move |call| match call {
                support::Call::Read(..) if reads.fetch_add(1, Ordering::SeqCst) + 1 == fail => {
                    Err(io::Error::other("a read this test makes fail"))
                }
                _ => Ok(()),
            }
        }));
        let reader = ReadSession::open_with_options(&folder, options).expect("open for reading");
        let opened = reads.load(Ordering::SeqCst);
        let outcome = support::search(&reader, &sql, &query);
        (opened, reads.load(Ordering::SeqCst), outcome)
    };
    let (opened, ended, whole) = searched(0);
    assert_eq!(whole.expect("AUTO, no read failing").rows().len(), 10);
    assert!(ended > opened, "the search read the data file");
    // Whichever read of the search fails, of an IVF's record as of any other, the search
    // returns the failure: it never answers as though the record were only unreadable.
    for fail in opened + 1..=ended {
        match searched(fail).2 {
            Err(Error::StoreIo { .. }) => {}
            other => panic!(
                "read {} of the {} of an AUTO search failed, and the search gave {other:?}",
                fail - opened,
                ended - opened
            ),
        }
    }
}

#[test]
fn an_indexed_search_whose_first_pass_takes_every_slot_is_refused_before_a_row_is_asked() {
    // One row, and no IVF. Its vector is wide enough that asking the row for its
    // distance shows in the bytes read: 65,536 bytes of numbers.
    const WIDE: usize = 16384;
    let folder = support::folder("indexed-refused-before-a-row");
    let db = support::open(&folder);
    support::declare_docs(&db, WIDE, "");
    support::insert_doc(&db, 1, "a", Some(support::vector(1, WIDE))).expect("insert the row");
    support::close(db);

    let query = support::vector(2, WIDE);
    let read_by =
        |mode: &str| read_alone(&folder, &support::by_mode(Some("tag = 1"), mode, 2), &query);
    let vector_bytes = 4 * WIDE as u64;
    let (asked, exact) = read_by("EXACT");
    assert_eq!(exact.expect("EXACT, one row passes").rows().len(), 1);
    assert!(
        asked >= vector_bytes,
        "EXACT asked the row for its distance and read {asked} bytes of the data file"
    );
    // The filter passes one row and two are asked for: the pass that would keep every slot is
    // the first, and INDEXED is refused there, with no row asked for its distance.
    let (refused, indexed) = read_by("INDEXED");
    support::assert_indexed_refused(indexed, "one row, a filter and LIMIT 2");
    assert!(
        refused < vector_bytes,
        "INDEXED was refused after reading {refused} bytes of the data file; the row's vector \
         is {vector_bytes}"
    );
}

/// A filter no row can pass is answered with no rows and no work, by every mode and by a search,
/// a table read, a count and a write alike; a partition that is behind its writes is never
/// asked, so INDEXED is not refused on its account.
#[test]
fn a_filter_no_row_can_pass_is_no_work_and_never_refused() {
    let (_, db) = support::built(
        "no-row-search",
        DIMENSION,
        "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000",
        2000,
    );
    support::load_docs(&db, 2001..=2800, "b", gathered);
    let query = question(4);
    for filter in [
        "ctx = 'a' AND ctx = 'b'",
        "tag = 1 AND tag > 5",
        "ctx = 'a' AND ctx IN ('b', 'c')",
    ] {
        for mode in ["INDEXED", "AUTO", "EXACT", ""] {
            let answer = support::search(&db, &support::by_mode(Some(filter), mode, 10), &query)
                .unwrap_or_else(|e| {
                    panic!("{filter} under {mode}: no row can pass, so no refusal: {e:?}")
                });
            assert!(
                answer.rows().is_empty(),
                "{filter} under {mode}: no row can pass"
            );
            assert_eq!(
                (answer.rows_examined(), answer.distance_evaluations()),
                (0, 0),
                "{filter} under {mode}: nothing is read or compared"
            );
        }
        let table = support::run(&db, &format!("SELECT id FROM docs WHERE {filter}"));
        assert_eq!(
            (table.rows().len(), table.rows_examined()),
            (0, 0),
            "{filter}: a table read"
        );
        let count = support::run(&db, &format!("SELECT COUNT(*) FROM docs WHERE {filter}"));
        assert_eq!(
            count.rows_examined(),
            0,
            "{filter}: a count examines no row"
        );
        assert_eq!(
            count.rows()[0][0],
            Value::Int64(0),
            "{filter}: a count of none"
        );
        let write = support::run(&db, &format!("UPDATE docs SET tag = tag WHERE {filter}"));
        assert_eq!(
            write.rows_examined(),
            0,
            "{filter}: a write examines no row"
        );
    }
}

/// A NaN bound sorts above every number, as in Postgres, so `distance < NaN` is true of every
/// row: the bound excludes nothing and the search answers as it does without the bound, in every
/// mode.
#[test]
fn a_nan_distance_bound_changes_no_row() {
    let (_, db) = support::built("nan-bound", DIMENSION, GROUPED, 2000);
    let query = question(4);
    for mode in ["INDEXED", "AUTO", "EXACT"] {
        let with = format!(
            "SELECT id, v <=> $q AS distance FROM docs WHERE v <=> $q < $nan ORDER BY distance USE VECTOR {mode} LIMIT 10"
        );
        let without = support::by_mode(None, mode, 10);
        let bounded = db
            .execute(&with, &params! { "q" => query.clone(), "nan" => f64::NAN })
            .unwrap_or_else(|e| panic!("{mode}: a NaN bound is not an error: {e:?}"));
        let plain = support::answer(&db, &without, &query);
        assert_eq!(
            bounded.rows(),
            plain.rows(),
            "{mode}: a NaN bound excludes nothing"
        );
        assert_eq!(
            plain.rows().len(),
            10,
            "{mode}: the search has ten rows to give"
        );
    }
}

/// A search INDEXED refuses for want of rows (three pass, ten are asked for) is refused by a
/// bound that cut nothing in the pass, as without the bound; a bound that cuts a row from those
/// three answers the rows within it, which are the exact scan's.
#[test]
fn a_bound_that_cuts_nothing_is_refused_as_without_it_and_one_that_cuts_answers() {
    let (_, db) = support::built("bound-refusal", DIMENSION, GROUPED, 2000);
    let stored = support::stored(&db, support::DOCS);
    let query = question(5);
    let few = "tag = 7 AND id <= 150";
    let passes = |row: &[Value]| matches!((&row[0], &row[2]), (Value::Int64(id), Value::Int64(7)) if *id <= 150);
    let wanted = stored.nearest(passes, &query, 10);
    assert_eq!(wanted.len(), 3, "three rows pass the row conditions");
    let bounded = |bound: f64| {
        let filter = format!("v <=> $q < {bound:?} AND {few}");
        support::search(&db, &support::by_mode(Some(&filter), "INDEXED", 10), &query)
    };
    support::assert_indexed_refused(
        support::search(&db, &support::by_mode(Some(few), "INDEXED", 10), &query),
        "the premise: without a bound these three rows are refused",
    );
    support::assert_indexed_refused(bounded(3.0), "a bound above every distance cuts nothing");
    let (near, far) = (wanted[1].distance, wanted[2].distance);
    assert!(
        near < far,
        "the third row is farther than the second, so a bound can lie between"
    );
    let cut = bounded((near + far) / 2.0).expect("a bound that cuts a row answers");
    support::assert_same_rows(
        &support::found(&cut, "cut"),
        &wanted[..2],
        "the rows within the bound, as the exact scan gives them",
    );
}

/// Past 4,096 partition tuples the filter is not whole partitions, so INDEXED is refused as
/// it is today and the default mode answers, with the exact scan's rows. The filter names the
/// partitions by 3,000 values of `a` in one case and 3,000 values of `b` under another `a` in
/// the other, which together are more tuples than the cap.
#[test]
fn past_the_most_partition_tuples_indexed_is_refused_and_the_default_answers() {
    let folder = support::folder("tuples-cap");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE d2 (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, v VECTOR(2) PARTITION_KEY (a, b))",
    );
    let tx = db.transaction().expect("begin");
    for n in 1..=200_i64 {
        let v = vec![1.0_f32, n as f32 / 100.0];
        tx.run(
            "INSERT INTO d2 (id, a, b, v) VALUES ($id, $a, 1, $v)",
            &params! { "id" => n, "a" => n, "v" => v },
        )
        .expect("a row");
    }
    tx.commit().expect("commit");
    support::maintenance(&db);
    let list = |from: i64| {
        (from..from + 3000)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let filter = format!(
        "a IN (1, 2, {}) OR (a = 9999 AND b IN (1, {}))",
        list(10_001),
        list(2)
    );
    let sql = |mode: &str| {
        format!("SELECT id FROM d2 WHERE {filter} ORDER BY v <=> [1.0, 0.0] {mode}LIMIT 10")
    };
    let refusal = db.execute(&sql("USE VECTOR INDEXED "), &params! {});
    assert!(
        matches!(refusal, Err(Error::VectorIndexedRouteUnavailable { .. })),
        "past the most tuples the filter is asked of rows, so INDEXED is refused: {refusal:?}"
    );
    let default = support::run(&db, &sql(""));
    let exact = support::run(&db, &sql("USE VECTOR EXACT "));
    assert_eq!(
        default.rows(),
        exact.rows(),
        "the default mode answers as the exact scan does"
    );
    assert_eq!(default.rows().len(), 2, "the rows of a = 1 and a = 2");
}

/// A partition that holds fewer than `k` live rows, searched under INDEXED with a row condition
/// and a bound: a bound that cuts a row answers the rows within it, which are the exact scan's,
/// and a bound that cuts nothing is refused as the same search without it.
#[test]
fn a_short_partition_under_a_bound_answers_when_it_cut_and_is_refused_when_it_did_not() {
    let (_, db) = support::built(
        "short-partition",
        DIMENSION,
        "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000",
        2000,
    );
    support::load_docs(&db, 2001..=2005, "b", gathered);
    support::maintenance(&db);
    let stored = support::stored(&db, support::DOCS);
    let query = question(5);
    let in_b = |row: &[Value]| row[1] == support::text("b");
    let wanted = stored.nearest(in_b, &query, 8);
    assert_eq!(
        wanted.len(),
        5,
        "five rows lie in the partition, and eight are asked for"
    );
    let run = |bound: f64| {
        let filter = format!("v <=> $q < {bound:?} AND ctx = 'b' AND tag < 50");
        support::search(&db, &support::by_mode(Some(&filter), "INDEXED", 8), &query)
    };
    let plain = "ctx = 'b' AND tag < 50";
    support::assert_indexed_refused(
        support::search(&db, &support::by_mode(Some(plain), "INDEXED", 8), &query),
        "the premise: without a bound the five rows are refused",
    );
    support::assert_indexed_refused(run(3.0), "a bound above every distance cuts nothing");
    let (near, far) = (wanted[3].distance, wanted[4].distance);
    assert!(near < far, "the fifth row is farther than the fourth");
    let cut = run((near + far) / 2.0).expect("a bound that cuts a row answers");
    support::assert_same_rows(
        &support::found(&cut, "cut"),
        &wanted[..4],
        "the rows within the bound, as the exact scan gives them",
    );
}

/// The bound written the other way round is the same bound: `0.5 > v <=> $q` answers as
/// `v <=> $q < 0.5` and `0.5 >= v <=> $q` as `<= 0.5`.
#[test]
fn a_bound_written_with_the_distance_on_the_right_is_the_same_bound() {
    let (_, db) = support::built("bound-reversed", DIMENSION, GROUPED, 2000);
    let query = gathered(7);
    for (reversed, plain) in [
        ("0.5 > v <=> $q", "v <=> $q < 0.5"),
        ("0.5 >= v <=> $q", "v <=> $q <= 0.5"),
    ] {
        let search = |filter: &str| {
            support::search(&db, &support::by_mode(Some(filter), "INDEXED", 8), &query)
                .expect(filter)
        };
        let (a, b) = (search(reversed), search(plain));
        assert_eq!(a.rows(), b.rows(), "{reversed} and {plain}");
        assert_eq!(
            a.distance_evaluations(),
            b.distance_evaluations(),
            "{reversed}: the same cut-off"
        );
        assert!(
            !a.rows().is_empty(),
            "{reversed}: rows lie within the bound"
        );
    }
}

/// A search short of `k` under a bound, in either mode, answers as the exact scan does and
/// compares at most twice the table and fewer than without the bound: the groups beyond the bound are never looked at.
#[test]
fn a_search_short_under_a_bound_compares_less_than_without_it() {
    let (_, db) = support::built("bound-short", DIMENSION, GROUPED, 2000);
    let query = gathered(7);
    let exact = |filter: &str| {
        let sql = format!(
            "SELECT id, v <=> $q AS distance FROM docs WHERE {filter} ORDER BY (v <=> $q) + 0 LIMIT 8"
        );
        support::search(&db, &sql, &query).expect("the exact scan")
    };
    let filter = "v <=> $q < 0.3 AND tag = 7 AND id < 700";
    let wanted = exact(filter);
    assert!(
        !wanted.rows().is_empty() && wanted.rows().len() < 8,
        "a short answer"
    );
    for mode in ["", "INDEXED"] {
        let sql = support::by_mode(Some(filter), mode, 8);
        let got = support::search(&db, &sql, &query)
            .unwrap_or_else(|e| panic!("{mode:?}: a bound that cut rows answers short: {e:?}"));
        assert_eq!(got.rows(), wanted.rows(), "{mode:?}: the exact scan's rows");
        let without = support::search(
            &db,
            &support::by_mode(Some("tag = 7 AND id < 700"), mode, 8),
            &query,
        );
        // Sealed vectors are screened by their one-byte copies first: at most two evaluations a row.
        assert!(
            got.distance_evaluations() <= 2 * 2000,
            "{mode:?}: {} compared of at most two for each of the 2,000 vectors",
            got.distance_evaluations()
        );
        if let Ok(without) = without {
            assert!(
                got.distance_evaluations() < without.distance_evaluations(),
                "{mode:?}: {} compared under the bound, {} without it: the bound stops the search",
                got.distance_evaluations(),
                without.distance_evaluations()
            );
        }
    }
}

/// A row whose bucketed distance passes the bound but whose exact distance does not is a cut
/// like any other: the search answers the rows within the bound, short of `k`, as the exact scan
/// does, and does not refuse. The bound lies a hair below the fifth row's exact distance.
#[test]
fn a_row_cut_by_its_exact_distance_is_a_cut_and_the_short_answer_stands() {
    let (_, db) = support::built(
        "exact-offer-cut",
        DIMENSION,
        "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000",
        2000,
    );
    support::load_docs(&db, 2001..=2005, "b", gathered);
    support::maintenance(&db);
    let stored = support::stored(&db, support::DOCS);
    let query = question(5);
    let in_b = |row: &[Value]| row[1] == support::text("b");
    let wanted = stored.nearest(in_b, &query, 8);
    assert_eq!(wanted.len(), 5, "five rows lie in the partition");
    let (fourth, fifth) = (wanted[3].distance, wanted[4].distance);
    let bound = fifth - (fifth - fourth).min(1e-6) / 2.0;
    assert!(
        bound > fourth && bound < fifth,
        "the bound lies between the fourth and the fifth"
    );
    let filter = format!("v <=> $q < {bound:?} AND ctx = 'b' AND tag < 50");
    let cut = support::search(&db, &support::by_mode(Some(&filter), "INDEXED", 8), &query)
        .expect("a row cut by its exact distance is a cut, so the short answer stands");
    support::assert_same_rows(
        &support::found(&cut, "cut"),
        &wanted[..4],
        "the rows within the bound",
    );
}
