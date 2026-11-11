//! The index of a VECTOR column: a partition of a column that is large enough is cut into
//! groups, and a search under INDEXED, or under AUTO from `AUTO_INDEX_AT` vectors on, looks at
//! the nearest groups first. Every row it returns has its true distance; a column below its
//! size is searched whole and pays nothing; and INDEXED is refused by name where the index
//! cannot serve the statement.

mod support;

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::{
    DatabaseOpenOptions, Error, QueryResult, ReadSession, ReadSessionOptions, Value, params,
};

const DIMENSION: usize = 16;

/// The clause under which a partition of 2,000 rows is cut into groups.
const GROUPED: &str = "AUTO_INDEX_AT 1000";

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
    for j in 0..20 {
        let query = question(j);
        let wanted = stored.nearest(all, &query, 10);
        let sql = support::by_mode(None, "EXACT", 10);
        let exact = support::assert_nearest(&db, &sql, &query, &wanted, "EXACT");
        let indexed = support::answer(&db, &support::by_mode(None, "INDEXED", 10), &query);
        let what = format!("INDEXED, question {j}");
        support::assert_sound(&indexed, &stored, all, &query, 10, &what);
        assert!(
            indexed.distance_evaluations() < exact.distance_evaluations(),
            "{what}: {} distance evaluations, EXACT {}",
            indexed.distance_evaluations(),
            exact.distance_evaluations()
        );
    }
}

#[test]
fn an_indexed_search_reads_sketches_before_blocks() {
    // 256 rows spread evenly round a circle, in vectors wide enough for five blocks of 63
    // slots. The size is fixed by the blocks: INDEXED reads every centre and the sketches
    // first, so it reads less than EXACT only when the blocks it then leaves unread outweigh
    // them, which takes several blocks; few rows in wide vectors make those with a short build.
    const WIDE: usize = 4096;
    const ROWS: i64 = 256;
    let folder = support::folder("indexed-reads-sketches");
    let db = support::open(&folder);
    support::declare_docs(&db, WIDE, "AUTO_INDEX_AT 256");
    support::load_docs(&db, 1..=ROWS, "a", |n| support::ringed(n, ROWS, WIDE));
    support::upkeep(&db);
    assert_eq!(lists(&support::the_partition(&db)), Value::Int64(16));
    support::close(db);

    // Each search runs first in a session of its own, so every page it needs is read once.
    let query = support::ringed(157, 2 * ROWS, WIDE);
    let read_by = |mode: &str| -> u64 {
        let (door, bytes) = support::counting_bytes_read();
        let mut options = ReadSessionOptions::default();
        options.storage = Some(door);
        let reader = ReadSession::open_with_options(&folder, options).expect("open for reading");
        let opened = bytes.load(Ordering::SeqCst);
        let result = support::answer(&reader, &support::by_mode(None, mode, 10), &query);
        assert_eq!(result.rows().len(), 10, "rows under {mode}");
        bytes.load(Ordering::SeqCst) - opened
    };
    // EXACT reads every block. INDEXED reads what EXACT does not, the centres and the
    // sketches, and still reads less: it left blocks unread.
    let (exact, indexed) = (read_by("EXACT"), read_by("INDEXED"));
    assert!(
        indexed < exact,
        "INDEXED read {indexed} bytes of the data file and EXACT {exact}"
    );
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
    support::upkeep(&db);
    let partition = support::the_partition(&db);
    assert_eq!(support::whole(&partition, "tombstones"), 0);
    assert_eq!(lists(&partition), Value::Int64(1));
    assert_eq!(index_bytes(), 28, "the record of a grouping of one group");
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
        // With no grouping the answer is the exact one.
        let wanted = support::stored(&db, support::DOCS).nearest(all, &query, 10);
        let state = format!("{upto} rows and no cycle");
        support::assert_nearest(&db, &sql, &query, &wanted, &state);
    }
    support::upkeep(&db);
    assert_eq!(lists(&support::the_partition(&db)), Value::Int64(45));
    let stored = support::stored(&db, support::DOCS);
    let result = support::answer(&db, &sql, &query);
    support::assert_sound(&result, &stored, all, &query, 10, "1,999 rows grouped");
}

/// One partition built and one given 2,000 rows since the last cycle, which is twice the
/// 1,000 a column of these clauses may have outside a grouping. `indexed` is how the search
/// asks for the index: `INDEXED`, or nothing on a column declared so.
fn refused_whole_while_one_partition_is_behind(name: &str, clauses: &str, indexed: &str) {
    let (_, db) = support::built(name, DIMENSION, clauses, 2000);
    support::load_docs(&db, 2001..=4000, "b", gathered);
    let query = question(1);
    let both = support::by_mode(None, indexed, 10);
    let before = db.committed_watermark();
    support::assert_indexed_refused(support::search(&db, &both, &query), name);
    assert_eq!(db.committed_watermark(), before, "{name}: a refused search");

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
    // their true distances, nearest first, and of the partition with no grouping every row
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

    support::upkeep(&db);
    let result = support::answer(&db, &both, &query);
    support::assert_sound(&result, &stored, all, &query, 10, "after the cycles");
}

#[test]
fn indexed_is_refused_whole_when_one_partition_is_behind() {
    refused_whole_while_one_partition_is_behind(
        "behind-use-vector",
        "PARTITION_KEY (ctx) AUTO_INDEX_AT 1000",
        "INDEXED",
    );
    refused_whole_while_one_partition_is_behind(
        "behind-declared",
        "PARTITION_KEY (ctx) SEARCH_MODE INDEXED AUTO_INDEX_AT 1000",
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
fn auto_uses_groupings_from_auto_index_at_on() {
    // Two built partitions of 1,200 under AUTO_INDEX_AT 2000: the count is over the selected
    // partitions together.
    let clauses = "PARTITION_KEY (ctx) AUTO_INDEX_AT 2000";
    let (_, db) = support::built("auto-threshold", DIMENSION, clauses, 1200);
    support::load_docs(&db, 1201..=2400, "b", gathered);
    support::upkeep(&db);
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

    // A column below its declared size stays ungrouped however many cycles run: 1,000 rows
    // under AUTO_INDEX_AT 50000 are not 50,000, though one partition holds a hundredth of it.
    let index_bytes = |db: &contextdb::Database| {
        let index = support::shown(db, "SHOW VECTOR_INDEXES").remove(0);
        support::whole(&index, "durable_index_bytes")
    };
    let clauses = "PARTITION_KEY (ctx) AUTO_INDEX_AT 50000";
    let (_, db) = support::built("auto-below-declared", DIMENSION, clauses, 400);
    support::load_docs(&db, 401..=1000, "b", gathered);
    for _ in 0..3 {
        support::upkeep(&db);
    }
    let grouped: Vec<Value> = support::shown(&db, "SHOW VECTOR_PARTITIONS")
        .iter()
        .map(lists)
        .collect();
    assert_eq!(grouped, [Value::Null, Value::Null]);
    assert_eq!(index_bytes(&db), 0);

    // With no AUTO_INDEX_AT the number in force is 100,000: a partition of 1,200 stays
    // ungrouped, and AUTO over it is EXACT.
    let (_, db) = support::built("auto-undeclared", DIMENSION, "", 1200);
    let index = support::shown(&db, "SHOW VECTOR_INDEXES").remove(0);
    assert_eq!(
        support::field(&index, "declared_auto_index_at"),
        &Value::Null
    );
    assert_eq!(support::whole(&index, "effective_auto_index_at"), 100_000);
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
fn auto_searches_each_partition_by_what_its_grouping_saves() {
    let clauses = "PARTITION_KEY (ctx) AUTO_INDEX_AT 1000";
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
    assert!(
        auto.distance_evaluations() < exact.distance_evaluations(),
        "AUTO did {} distance evaluations, EXACT {}",
        auto.distance_evaluations(),
        exact.distance_evaluations()
    );
}

#[test]
fn auto_answers_exactly_where_a_grouping_is_behind_or_damaged() {
    let (_, db) = support::built("auto-behind", DIMENSION, GROUPED, 2000);
    // 800 vectors outside the grouping: twice the 400 at which it is grouped again.
    support::load_docs(&db, 2001..=2800, "a", gathered);
    let query = question(4);
    let wanted = support::stored(&db, support::DOCS).nearest(all, &query, 10);
    let sql = support::by_mode(None, "INDEXED", 10);
    support::assert_indexed_refused(support::search(&db, &sql, &query), "a grouping behind");
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
fn writes_after_a_grouping_are_found_at_once() {
    let (_, db) = support::built("writes-after-grouping", DIMENSION, GROUPED, 2000);
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
    let check = |state: &str| {
        let stored = support::stored(&db, support::DOCS);
        for (query, id) in [(&inserted, 3001), (&changed, 5)] {
            let result = support::answer(&db, &support::by_mode(None, "INDEXED", 1), query);
            let answer = support::assert_sound(&result, &stored, all, query, 1, state);
            assert_eq!(answer[0].key, Value::Int64(id), "{state}");
        }
        let result = support::answer(&db, &support::by_mode(None, "INDEXED", 10), &gone);
        let answer = support::assert_sound(&result, &stored, all, &gone, 10, state);
        assert!(
            answer.iter().all(|near| near.key != Value::Int64(7)),
            "{state}: the deleted row is in {answer:?}"
        );
    };
    check("before any cycle");
    assert_eq!(support::cycle(&db).maintenance_vectors_done, 2);
    check("after sealing");
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

    let clauses = "SEARCH_MODE INDEXED AUTO_INDEX_AT 1000";
    let (_, declared) = support::built("filter-widens-declared", DIMENSION, clauses, 2000);
    support::assert_indexed_refused(search(&declared, few, "", 10), "SEARCH_MODE INDEXED");
}

/// The keys of an answer, which must be `count` different rows.
fn different_keys(result: &QueryResult, count: usize, what: &str) {
    let keys: BTreeSet<i64> = result
        .rows()
        .iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref other => panic!("{what}: the key {other:?} is no INTEGER"),
        })
        .collect();
    assert_eq!(result.rows().len(), count, "{what}: rows in the answer");
    assert_eq!(keys.len(), count, "{what}: different rows in the answer");
}

#[test]
fn a_search_that_widens_returns_no_row_twice() {
    let (_, db) = support::built("widens-no-row-twice", DIMENSION, GROUPED, 2000);
    let stored = support::stored(&db, support::DOCS);
    // Filters that pass a half, a tenth and a fiftieth of the rows: the fewer pass, the more
    // often the search goes back to groups and sketches it has already been through.
    type Passing = (&'static str, fn(&[Value]) -> bool);
    let filters: [Passing; 3] = [
        (
            "tag < 25",
            |row| matches!(row[2], Value::Int64(tag) if tag < 25),
        ),
        (
            "tag < 5",
            |row| matches!(row[2], Value::Int64(tag) if tag < 5),
        ),
        ("tag = 7", |row| row[2] == Value::Int64(7)),
    ];
    for j in 0..8 {
        let query = question(j);
        for (filter, passes) in filters {
            for k in [10, 30] {
                let what = format!("AUTO WHERE {filter} LIMIT {k}, question {j}");
                let sql = support::by_mode(Some(filter), "AUTO", k);
                let result = support::answer(&db, &sql, &query);
                different_keys(&result, k, &what);
                support::assert_sound(&result, &stored, passes, &query, k, &what);
            }
        }
    }
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
    assert!(
        indexed < exact,
        "INDEXED did {indexed} distance evaluations for ten rows, EXACT {exact}"
    );
    // Every row: every group is taken. An evaluation is a centre compared, a sketch
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
         asks every row as EXACT does ({exact}), after comparing its 45 centres and estimating \
         the sketch of each of the 2,000 slots at least once"
    );
}

#[test]
fn an_indexed_search_short_of_its_limit_is_refused_before_the_whole_table_is_read() {
    // 256 rows spread evenly round a circle, in vectors wide enough for five blocks of 63
    // slots. The size is fixed by the blocks: a refusal reads less than the whole table only
    // when there are blocks for it to stop short of, and the filter needs its three rows
    // among the first 150; few rows in wide vectors make those with a short build.
    const WIDE: usize = 4096;
    const ROWS: i64 = 256;
    let folder = support::folder("indexed-refused-before-the-table");
    let db = support::open(&folder);
    support::declare_docs(&db, WIDE, "AUTO_INDEX_AT 256");
    support::load_docs(&db, 1..=ROWS, "a", |n| support::ringed(n, ROWS, WIDE));
    support::upkeep(&db);
    assert_eq!(lists(&support::the_partition(&db)), Value::Int64(16));
    let stored = support::stored(&db, support::DOCS);
    support::close(db);

    // Three rows pass the filter and the statement asks for ten.
    let few = Some("tag = 7 AND id <= 150");
    let passes = |row: &[Value]| matches!((&row[0], &row[2]), (Value::Int64(id), Value::Int64(7)) if *id <= 150);
    let query = support::ringed(157, 2 * ROWS, WIDE);
    let wanted = stored.nearest(passes, &query, 10);
    assert_eq!(wanted.len(), 3);
    // Each search runs first in a session of its own, so every page it needs is read once.
    let read_by = |mode: &str| -> (u64, contextdb::Result<QueryResult>) {
        let (door, bytes) = support::counting_bytes_read();
        let mut options = ReadSessionOptions::default();
        options.storage = Some(door);
        let reader = ReadSession::open_with_options(&folder, options).expect("open for reading");
        let opened = bytes.load(Ordering::SeqCst);
        let outcome = support::search(&reader, &support::by_mode(few, mode, 10), &query);
        (bytes.load(Ordering::SeqCst) - opened, outcome)
    };
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
    support::upkeep(&db);
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
fn a_grouping_whose_sketches_cannot_tell_its_vectors_apart_answers_as_exact() {
    // Every row has one of eight vectors, so each vector of a group is the group's centre:
    // the sketch of every one is the same and the width measured for it is nothing.
    let same = |n: i64| -> Vec<f32> {
        let direction = n.rem_euclid(8) as usize;
        (0..DIMENSION)
            .map(|at| f32::from(u8::from(at / 2 == direction)) + 0.25)
            .collect()
    };
    let folder = support::folder("sketches-all-the-same");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, GROUPED);
    support::load_docs(&db, 1..=2000, "a", same);
    support::upkeep(&db);
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
fn a_search_with_no_room_for_its_sketches_is_refused_by_the_memory_limit() {
    const MIB: usize = 1 << 20;
    let (folder, db) = support::built("no-room-for-sketches", DIMENSION, GROUPED, 2000);
    assert_eq!(lists(&support::the_partition(&db)), Value::Int64(45));
    let stored = support::stored(&db, support::DOCS);
    support::close(db);

    // 9 MiB is the smallest limit a store opens under, and leaves a search nothing to keep
    // the sketches of its groups in.
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(9 * MIB);
    let db = support::open_with(&folder, options).expect("open under 9 MiB");
    let query = question(7);
    let sql = support::by_mode(None, "INDEXED", 10);
    let before = (db.memory_used(), db.committed_watermark());
    match support::search(&db, &sql, &query) {
        Err(Error::MemoryBudgetExceeded {
            budget_limit_bytes, ..
        }) => assert_eq!(budget_limit_bytes, 9 * MIB as u64),
        other => panic!("{sql}\nunder 9 MiB got {other:?}"),
    }
    assert_eq!(
        (db.memory_used(), db.committed_watermark()),
        before,
        "after the refused search"
    );

    db.set_memory_limit(Some(64 * MIB)).expect("a larger limit");
    let result = support::answer(&db, &sql, &query);
    support::assert_sound(&result, &stored, all, &query, 10, "under 64 MiB");
}

#[test]
fn a_filter_of_partition_keys_only_is_no_filter_on_rows() {
    let clauses = "PARTITION_KEY (ctx) AUTO_INDEX_AT 1000";
    let (_, db) = support::built("keys-only-filter", DIMENSION, clauses, 2000);
    support::load_docs(&db, 2001..=2005, "b", gathered);
    support::upkeep(&db);
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
    different_keys(&result, 2005, "both partitions selected");
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
        let mut options = ReadSessionOptions::default();
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
    // Whichever read of the search fails, of a grouping's record as of any other, the search
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
    // One row, and no grouping. Its vector is wide enough that asking the row for its
    // distance shows in the bytes read: 65,536 bytes of numbers.
    const WIDE: usize = 16384;
    let folder = support::folder("indexed-refused-before-a-row");
    let db = support::open(&folder);
    support::declare_docs(&db, WIDE, "");
    support::insert_doc(&db, 1, "a", Some(support::vector(1, WIDE))).expect("insert the row");
    support::close(db);

    let query = support::vector(2, WIDE);
    // Each search runs first in a session of its own, so every page it needs is read once.
    let read_by = |mode: &str| -> (u64, contextdb::Result<QueryResult>) {
        let (door, bytes) = support::counting_bytes_read();
        let mut options = ReadSessionOptions::default();
        options.storage = Some(door);
        let reader = ReadSession::open_with_options(&folder, options).expect("open for reading");
        let opened = bytes.load(Ordering::SeqCst);
        let sql = support::by_mode(Some("tag = 1"), mode, 2);
        let outcome = support::search(&reader, &sql, &query);
        (bytes.load(Ordering::SeqCst) - opened, outcome)
    };
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
