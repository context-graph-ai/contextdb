//! The index of a VECTOR column: a partition of a column that is large enough is cut into
//! groups, and a search under INDEXED, or under AUTO from `INDEXING_THRESHOLD` vectors on, looks at
//! the nearest groups first. Every row it returns has its true distance; a column below its
//! size is searched whole and pays nothing; and INDEXED is refused by name where the index
//! cannot serve the statement.

mod support;
#[path = "support/vecsearch.rs"]
mod vecsearch;
use support::{Run, count};

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::{DatabaseOpenOptions, Error, QueryResult, ReadSession, Uuid, Value, params};

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

/// The bytes the first vector index holds on disk.
fn index_bytes(db: &contextdb::Database) -> i64 {
    let index = support::shown(db, "SHOW VECTOR_INDEXES").remove(0);
    support::whole(&index, "durable_index_bytes")
}

/// The test of a stored row that `tag = 7 AND id <= upto` makes.
fn tagged(upto: i64) -> impl Fn(&[Value]) -> bool + Copy {
    move |row: &[Value]| matches!((&row[0], &row[2]), (Value::Int64(id), Value::Int64(7)) if *id <= upto)
}

/// The groups of each partition, in the order SHOW VECTOR_PARTITIONS lists them.
fn grouped(db: &contextdb::Database) -> Vec<Value> {
    support::shown(db, "SHOW VECTOR_PARTITIONS")
        .iter()
        .map(lists)
        .collect()
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
    // A key range that lets half the rows through: every question still finds EXACT's ten rows.
    let half = |row: &[Value]| matches!(row[0], Value::Int64(id) if id <= 1000);
    for j in 0..20 {
        let query = question(j);
        let wanted = stored.nearest(half, &query, 10);
        let exact = support::answer(
            &db,
            &support::by_mode(Some("id <= 1000"), "EXACT", 10),
            &query,
        );
        for mode in ["INDEXED", "AUTO"] {
            let sql = support::by_mode(Some("id <= 1000"), mode, 10);
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
    // The root's recall: at least 95 in 100 of EXACT's 200 rows, in less than EXACT's work.
    assert!(
        found * 100 >= 200 * 95,
        "INDEXED found {found} of EXACT's 200 rows in {work} evaluations"
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
    assert_eq!(lists(&support::the_partition(&db)), Value::Null);
    assert_eq!(index_bytes(&db), 0);
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
    assert_eq!(
        index_bytes(&db),
        28 + 4 * 16,
        "the record of an IVF of one group, with its reach"
    );
    agree(10, 10, "a fifth deleted");
    agree(5000, 1600, "a LIMIT above the rows that are left");
}

#[test]
fn indexed_is_served_at_every_size() {
    let (_, db) = vecsearch::docs("indexed-every-size", DIMENSION, GROUPED);
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

/// AUTO finds what EXACT finds, by the same distance evaluations.
fn auto_is_exact(db: &contextdb::Database, filter: Option<&str>, query: &[f32]) {
    let [auto, exact] = auto_and_exact(db, filter, query);
    assert_eq!(
        support::found(&auto, "AUTO"),
        support::found(&exact, "EXACT")
    );
    assert_eq!(auto.distance_evaluations(), exact.distance_evaluations());
}

#[test]
fn auto_uses_ivfs_from_indexing_threshold_on() {
    // Two built partitions of 1,200 under INDEXING_THRESHOLD 2000: the count is over the selected
    // partitions together.
    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 2000";
    let (_, db) = support::built("auto-threshold", DIMENSION, clauses, 1200);
    support::load_docs(&db, 1201..=2400, "b", gathered);
    support::maintenance(&db);
    assert_eq!(grouped(&db), [Value::Int64(35), Value::Int64(35)]);
    let stored = support::stored(&db, support::DOCS);
    let query = question(2);
    auto_is_exact(&db, Some("ctx = 'a'"), &query);
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
    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 50000";
    let (_, db) = support::built("auto-below-declared", DIMENSION, clauses, 400);
    support::load_docs(&db, 401..=1000, "b", gathered);
    for _ in 0..3 {
        support::maintenance(&db);
    }
    assert_eq!(grouped(&db), [Value::Null, Value::Null]);
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
    auto_is_exact(&db, None, &query);
}

/// The mandatory cell of 10,000 vectors in one partition: AUTO takes its IVF and compares
/// fewer vectors than EXACT, and answers ten rows with their true distances.
/// Left to the release run by `v2/.config/nextest.toml`.
#[test]
fn auto_over_ten_thousand_vectors_in_one_partition_does_fewer_evaluations_than_exact() {
    let (_, db) = support::built("auto-ten-thousand", DIMENSION, GROUPED, 10_000);
    let plan = db.explain(&support::by_mode(None, "AUTO", 10)).unwrap();
    let plan = plan.to_string();
    assert!(plan.lines().any(|l| l.trim() == "route: indexed"), "{plan}");
    let (stored, query) = (support::stored(&db, support::DOCS), question(3));
    let [auto, exact] = auto_and_exact(&db, None, &query);
    support::assert_sound(&auto, &stored, all, &query, 10, "AUTO over 10,000");
    let (auto, exact) = (auto.distance_evaluations(), exact.distance_evaluations());
    assert!(
        auto < exact,
        "AUTO did {auto} distance evaluations, EXACT {exact}"
    );
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
    support::load_docs(&db, 9001..=9005, "b", |n| nearer(&query, n));
    let stored = support::stored(&db, support::DOCS);
    let [auto, _] = auto_and_exact(&db, None, &query);
    let answer = support::assert_sound(&auto, &stored, all, &query, 10, "AUTO over both");
    let pending = |near: &&support::Near| matches!(near.key, Value::Int64(id) if id > 9000);
    assert_eq!(answer.iter().filter(pending).count(), 5, "{answer:?}");
    let wanted = stored.nearest(all, &query, 10);
    let found = answer.iter().filter(|near| wanted.contains(near)).count();
    assert_eq!(found, 10, "every one of the ten nearest");
    support::run(&db, "DROP TABLE docs");
    support::declare_docs(&db, 4, "PARTITION_KEY (ctx) INDEXING_THRESHOLD 4");
    support::load_docs(&db, 1..=20, "a", |n| support::vector(n, 4));
    support::maintenance(&db);
    support::load_docs(&db, 21..=25, "b", |n| support::vector(n, 4));
    support::run(&db, "UPDATE docs SET tag = 99 WHERE id >= 24");
    support::cycle(&db);
    let [auto, exact] = auto_and_exact(&db, Some("tag = 99"), &support::vector(3, 4));
    let same = support::found(&auto, "AUTO") == support::found(&exact, "EXACT");
    assert!(same, "AUTO answers as EXACT");
    let b = &support::shown(&db, "SHOW VECTOR_PARTITIONS")[1];
    assert_eq!(support::word(b, "availability_reason"), "none");
    let column = &support::shown(&db, "SHOW VECTOR_INDEXES")[0];
    assert_eq!(support::whole(column, "unavailable_partitions"), 0);
}

/// A vector a thousandth of the way from the question `q` to row `n`'s: nearer it than any row.
fn nearer(q: &[f32], n: i64) -> Vec<f32> {
    let moved = support::vector(n, DIMENSION);
    q.iter().zip(moved).map(|(q, m)| q + 0.001 * m).collect()
}

/// A find's units: rows read and distances evaluated (section 6, F4, as today).
fn work(found: &QueryResult) -> u64 {
    found.rows_examined() + found.distance_evaluations()
}

/// Five rows nearer the question than any of 2,000 under a second partition: named in either
/// order the two give the same rows for the same work, and a find of the first counts the same
/// whatever the second holds (section 6, F2).
#[test]
fn two_partitions_give_the_same_rows_and_work_in_either_order() {
    let db = support::open(&support::folder("either-order"));
    support::run(&db, "CREATE TABLE bonus (id INTEGER PRIMARY KEY, w REAL)");
    let rank = "JOIN bonus ON id, FORMULA 'coalesce({w}, 0.0) + {vector_score}', SORT_KEY s";
    let clauses = format!("PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000 RANK_POLICY ({rank})");
    support::declare_docs(&db, DIMENSION, &clauses);
    support::run(&db, "CREATE INDEX docs_tag ON docs (tag)");
    support::load_docs(&db, 1..=2000, "a", gathered);
    support::maintenance(&db);
    let query = question(3);
    let ask =
        |mode: &str, f: &str| support::answer(&db, &support::by_mode(Some(f), mode, 10), &query);
    let before = MODES.map(|mode| work(&ask(mode, "ctx = 'a'")));
    support::load_docs(&db, 9001..=9005, "b", |n| nearer(&query, n));
    let stored = support::stored(&db, support::DOCS);
    for (mode, before) in MODES.into_iter().zip(before) {
        assert_eq!(
            work(&ask(mode, "ctx = 'a'")),
            before,
            "{mode}: 'a' whatever 'b' holds"
        );
        let [ab, ba] =
            ["'a', 'b'", "'b', 'a'"].map(|named| ask(mode, &format!("ctx IN ({named})")));
        support::assert_sound(&ab, &stored, all, &query, 10, mode);
        let told = |r: &QueryResult| (r.rows().to_vec(), work(r));
        assert_eq!(
            told(&ab),
            told(&ba),
            "{mode}: the order named changes no row and no count"
        );
    }
    // Under EXACT the find over both is the merge of each partition's own answer by rank, equal
    // ranks by key (R8): 9006 ties a's fifth at the limit, 9007, the farthest, ranks first. a's
    // keys are below b's, so a stable sort of a's answer then b's keeps equal ranks in key order.
    let tie = ask("EXACT", "ctx IN ('a', 'b')").rows()[9][0].clone();
    let tie = (1..=2000).find(|n| tie == Value::Int64(*n)).unwrap();
    let far: Vec<f32> = query.iter().map(|q| -q).collect();
    support::load_docs(&db, 9006..=9006, "b", |_| gathered(tie));
    support::load_docs(&db, 9007..=9007, "b", |_| far.clone());
    support::run(&db, "INSERT INTO bonus (id, w) VALUES (9007, 3.0)");
    for (rows, rank) in [("", ""), (" AND tag < 25", ""), ("", "USE RANK s ")] {
        let find = |mode: &str, named: &str| {
            let sql = support::by_mode(Some(&format!("ctx IN ({named}){rows}")), mode, 10);
            let sql = sql.replace("LIMIT", &format!("{rank}LIMIT"));
            support::found(&support::answer(&db, &sql, &query), &sql)
        };
        let w = |n: &support::Near| 3.0 * f64::from(u8::from(n.key == Value::Int64(9007)));
        let score = |n: &support::Near| [n.distance, -(w(n) + (1.0 - n.distance))];
        let mut merged = [find("EXACT", "'a'"), find("EXACT", "'b'")].concat();
        let by = usize::from(!rank.is_empty());
        merged.sort_by(|x, y| score(x)[by].total_cmp(&score(y)[by]));
        // INDEXED by rank is held to b's six rows, unindexed and so always compared, above a's.
        for (mode, upto) in [("EXACT", 10), ("INDEXED", 10 - 4 * by)] {
            for named in ["'a', 'b'", "'b', 'a'"] {
                let found = find(mode, named);
                let same = found[..upto] == merged[..upto];
                assert!(same, "{mode} ({named}){rows} {rank}: {found:?}");
            }
        }
    }
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
fn a_column_with_no_mode_declared_searches_as_auto() {
    // 2,000 rows over INDEXING_THRESHOLD 1000: the partition is grouped, so AUTO uses its IVF.
    let (_, db) = support::built("auto-undeclared-mode", DIMENSION, GROUPED, 2000);
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

/// An index damaged after a find has begun costs that find only the work already spent: AUTO
/// then answers what EXACT answers within EXACT's work plus what was spent, a read session
/// limited to that recovers, INDEXED over a damaged partition is refused by name while the sound
/// one answers, and maintenance rebuilds it (section 6, the guard's damaged-index cases).
#[test]
fn a_damaged_ivf_costs_a_find_only_the_work_already_spent() {
    {
        // 128 rows spread evenly round a circle, in vectors wide enough for four blocks of 42
        // slots. The size is fixed by the blocks: INDEXED reads every centroid and the binary codes
        // first, so it reads less than EXACT only when the blocks it then leaves unread outweigh
        // them, which takes several blocks; few rows in wide vectors make those with a short build.
        const WIDE: usize = 6144;
        const ROWS: i64 = 128;
        let folder = support::folder("indexed-reads-codes");
        let db = support::open(&folder);
        support::declare_docs(&db, WIDE, "INDEXING_THRESHOLD 128");
        support::load_docs(&db, 1..=ROWS, "a", |n| support::ringed(n, ROWS, WIDE));
        support::maintenance(&db);
        assert_eq!(lists(&support::the_partition(&db)), Value::Int64(11));
        support::close(db);

        let query = support::ringed(157, 2 * ROWS, WIDE);
        let read_by = |mode: &str| -> u64 {
            let (bytes, result) = read_alone(&folder, &support::by_mode(None, mode, 10), &query);
            assert_eq!(result.expect(mode).rows().len(), 10, "rows under {mode}");
            bytes
        };
        // EXACT reads every block. INDEXED reads what EXACT does not, the centroids and the
        // binary codes, and still reads less: it left blocks unread.
        let (exact, indexed) = (read_by("EXACT"), read_by("INDEXED"));
        assert!(
            indexed < exact,
            "INDEXED read {indexed} bytes of the data file and EXACT {exact}"
        );
        let db = support::open(&folder);
        support::run(&db, "CREATE INDEX docs_tag ON docs(tag)");
        support::run(&db, "UPDATE docs SET tag = 0");
        support::run(&db, "UPDATE docs SET tag = 1 WHERE id <= 20");
        support::close(db);
        let damage = |folder: &Path, block: u32| {
            use redb::{ReadableTable, TableDefinition};
            let file = redb::Database::open(folder.join("data")).unwrap();
            let write = file.begin_write().unwrap();
            {
                let mut blocks = write
                    .open_table(TableDefinition::<&[u8], &[u8]>::new("vector_blocks"))
                    .unwrap();
                let (key, mut bytes) = blocks
                    .iter()
                    .unwrap()
                    .map(Result::unwrap)
                    .find_map(|(k, v)| {
                        let key = k.value();
                        (key[0] == 7 && key[9..] == block.to_be_bytes())
                            .then(|| (key.to_vec(), v.value().to_vec()))
                    })
                    .expect("the target binary block exists");
                bytes.pop();
                blocks.insert(key.as_slice(), bytes.as_slice()).unwrap();
            }
            write.commit().unwrap();
        };
        damage(&folder, 1);
        let db = support::open(&folder);
        let filter = Some("tag = 1");
        support::load_docs(&db, ROWS + 1..=ROWS + 5, "a", |_| {
            support::ringed(64, ROWS, WIDE)
        });
        support::run(&db, "UPDATE docs SET tag = 1 WHERE id > 128");
        // The rows read before the second binary block fails: the five pending at distance 0,
        // then at most k = 5 of the first group's kept slots, best first, the walk asking for no
        // more after a group (e9815ada5).
        let (query, read_before_damage, work_before_damage) = (32..=96)
            .find_map(|n| {
                let q = support::ringed(n, ROWS, WIDE);
                db.execute(
                    "UPDATE docs SET v = $q WHERE id > 128",
                    &params! {"q"=>q.clone()},
                )
                .unwrap();
                let before = db.work_done().unwrap();
                let result = support::search(&db, &support::by_mode(filter, "INDEXED", 5), &q);
                let after = db.work_done().unwrap();
                let (reads, compared) = (
                    after.rows_read - before.rows_read,
                    after.vectors_compared - before.vectors_compared,
                );
                let asked = (5..=5 + 5).contains(&reads);
                (matches!(result, Err(Error::StoreCorrupted { .. })) && asked).then_some((
                    q,
                    reads,
                    reads + compared,
                ))
            })
            .expect(
                "five pending rows and at most five of the first group's are read before the second binary block fails",
            );
        let repair_due = support::watermark(&db);
        let [auto, exact] = ["AUTO", "EXACT"]
            .map(|mode| support::answer(&db, &support::by_mode(filter, mode, 5), &query));
        assert_eq!(support::watermark(&db), repair_due);
        assert_eq!(
            support::found(&auto, "AUTO after corruption"),
            support::found(&exact, "EXACT")
        );
        assert!(
            auto.rows_examined() <= exact.rows_examined() + read_before_damage,
            "the rows read before the damage plus EXACT's: AUTO {auto:?}, EXACT {exact:?}"
        );
        let limits = contextdb::ReadLimits {
            work: work_before_damage + exact.rows_examined() + exact.distance_evaluations(),
            ..Default::default()
        };
        let bounded = db.read_session(limits).unwrap();
        let result = support::answer(&bounded, &support::by_mode(filter, "AUTO", 5), &query);
        assert_eq!(
            support::found(&result, "bounded AUTO recovery"),
            support::found(&exact, "EXACT")
        );
        support::maintenance(&db);
        assert!(support::word(&support::the_partition(&db), "maintenance_state") == "idle");
        support::answer(&db, &support::by_mode(filter, "INDEXED", 5), &query);
        support::close(db);
        // Two partitions need not carry 6,144-wide vectors: 64 dimensions test the same
        // per-partition isolation and repair in milliseconds instead of tens of seconds.
        const SMALL: usize = 64;
        let c = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 128";
        let (f, db) = support::built("beside", SMALL, c, ROWS);
        support::load_docs(&db, 201..=200 + ROWS, "b", |n| support::gathered(n, SMALL));
        support::maintenance(&db);
        support::close(db);
        damage(&f, 0);
        let q = support::ringed(64, ROWS, SMALL);
        let (db, q) = (support::open(&f), &q);
        assert!(matches!(db.verify(), Err(Error::StoreCorrupted { .. })));
        let up = |p: &support::Shown| support::field(p, "indexed_available") == &Value::Bool(true);
        let parts = support::shown(&db, "SHOW VECTOR_PARTITIONS");
        let down: Vec<_> = parts.iter().filter(|p| !up(p)).collect();
        assert_eq!(down.len(), 1);
        assert!(support::word(down[0], "recovery_action") == "run a maintenance cycle");
        let ask = |c| support::search(&db, &support::by_mode(Some(c), "INDEXED", 5), q).is_ok();
        assert!(ask("ctx = 'a'") ^ ask("ctx = 'b'"));
        support::maintenance(&db);
        assert!(support::shown(&db, "SHOW VECTOR_PARTITIONS").iter().all(up));
        assert!(ask("ctx = 'a'") && ask("ctx = 'b'"));
    }
}

/// The filter cases of the route that gathered passing keys, as one table against section 6's
/// F3. `tag` has an index, so each filter of the table is countable: AUTO and EXACT take the
/// direct route or the walk by counted cost, and INDEXED the walk at every size. Each is
/// answered, and whole where few rows pass; under AUTO and EXACT the rows read to test a filter
/// whose path reaches `m` entries are fewer than 3m; and one find ranks each centre once and
/// estimates each code and screens each slot at most once (F2). INDEXED with a filter no index or
/// key gives is refused when planned, having read nothing; a declared SEARCH_MODE INDEXED is the
/// written one.
#[test]
fn a_filtered_find_is_answered_by_its_cheaper_route_in_every_mode() {
    let (_, db) = support::built("filter-routes", DIMENSION, GROUPED, 2000);
    support::run(&db, "CREATE INDEX docs_tag ON docs (tag)");
    let (stored, query) = (support::stored(&db, support::DOCS), question(5));
    let Value::Int64(groups) = lists(&support::the_partition(&db)) else {
        panic!("grouped")
    };
    // Even tags lie round directions 0, 2, 4 and 6, all far from the question's 5: 400 entries.
    let far = "tag IN (0, 2, 4, 6, 8, 10, 12, 14, 16, 18)";
    let one = format!("{far} AND id + 0 = 102");
    type Case<'a> = (&'a str, fn(i64, i64) -> bool, usize, bool, u64);
    // The filter, the rows it passes by id and tag, the rows asked for, whether every mode
    // answers it whole, and the entries its path reaches where the rows read are bounded by them.
    let cases: [Case; 9] = [
        ("tag < 45", |_, tag| tag < 45, 10, false, 0),
        (
            "tag = 7 AND id <= 600",
            |id, tag| tag == 7 && id <= 600,
            10,
            false,
            0,
        ),
        (
            "tag = 7 AND id <= 300",
            |id, tag| tag == 7 && id <= 300,
            10,
            true,
            0,
        ),
        (
            "tag = 7 AND id <= 150",
            |id, tag| tag == 7 && id <= 150,
            10,
            true,
            0,
        ),
        ("", |_, _| true, 5000, true, 0),
        ("tag = 99", |_, _| false, 10, true, 0),
        ("id = 102", |id, _| id == 102, 10, true, 0),
        (far, |_, tag| tag < 20 && tag % 2 == 0, 10, false, 400),
        (&one, |id, _| id == 102, 10, true, 400),
    ];
    for (filter, passes, k, whole, entries) in cases {
        let passes = |row: &[Value]| matches!((&row[0], &row[2]), (Value::Int64(id), Value::Int64(tag)) if passes(*id, *tag));
        let (wanted, filter) = (
            stored.nearest(passes, &query, k),
            Some(filter).filter(|f| !f.is_empty()),
        );
        for mode in ["AUTO", "EXACT", "INDEXED"] {
            let (sql, what) = (
                support::by_mode(filter, mode, k),
                format!("{mode}, {filter:?}"),
            );
            let found = if whole || mode == "EXACT" {
                support::assert_nearest(&db, &sql, &query, &wanted, &what)
            } else {
                let found = support::answer(&db, &sql, &query);
                support::assert_sound(&found, &stored, passes, &query, wanted.len(), &what);
                found
            };
            let (evaluations, read) = (found.distance_evaluations(), found.rows_examined());
            // Nothing done twice (F2): each centre, code and slot once, the mean once, and an
            // exact distance only for a row read.
            let most = groups as u64 + 1 + 2 * 2000 + read;
            assert!(
                evaluations <= most,
                "{what}: {evaluations} evaluations, most {most}"
            );
            // Tag 7 below id 301 is six rows (7, 57, ..., 257), short of ten, so INDEXED takes
            // every group: each centre, every code and slot, the mean, and six exact distances.
            let six = mode == "INDEXED" && filter == Some("tag = 7 AND id <= 300");
            let pinned = groups as u64 + 1 + 2 * 2000 + 6;
            assert!(
                !six || evaluations == pinned,
                "{what}: {evaluations} of {pinned}"
            );
            let bounded = entries > 0 && mode != "INDEXED";
            assert!(
                !bounded || read < 3 * entries,
                "{what}: {read} rows read for {entries} entries"
            );
            if mode == "INDEXED" && filter.is_some() {
                let plan = db.explain(&sql).expect(&what).to_string();
                assert!(
                    plan.contains("filter_route: walk"),
                    "{what}: INDEXED keeps to the walk:\n{plan}"
                );
                let ranked = filter != Some("id = 102") || evaluations > groups as u64;
                assert!(
                    ranked,
                    "{what}: one passing row is found through the ranked centres"
                );
            }
        }
    }
    // The far case again under a bound that cuts some of its rows: the first answer's rows inside
    // the bound, read below the same three times its entries (review 13, R6).
    let first = stored.nearest(
        |row| matches!(row[2], Value::Int64(t) if t < 20 && t % 2 == 0),
        &query,
        10,
    );
    let bound = (first[4].distance + first[5].distance) / 2.0;
    let inside: Vec<_> = first
        .into_iter()
        .filter(|near| near.distance < bound)
        .collect();
    for mode in ["AUTO", "EXACT"] {
        let sql = support::by_mode(Some(&format!("v <=> $q < {bound:?} AND {far}")), mode, 10);
        let read = support::assert_nearest(&db, &sql, &query, &inside, mode).rows_examined();
        assert!(
            read < 3 * 400,
            "{mode}: {read} rows read under the bound for 400 entries"
        );
    }
    // Rows written since the last cycle, passing the filter beside grouped ones: AUTO and EXACT
    // give one answer. Below its INDEXING_THRESHOLD AUTO is EXACT (README:1019-1021), and AUTO
    // never does more work than EXACT (root intent:879-880).
    support::run(&db, "UPDATE docs SET tag = 8 WHERE tag = 7 AND id > 1000");
    support::load_docs(&db, 2001..=2002, "a", gathered);
    support::run(&db, "UPDATE docs SET tag = 7 WHERE id > 2000");
    let pending = ["AUTO", "EXACT"]
        .map(|mode| support::answer(&db, &support::by_mode(Some("tag = 7"), mode, 21), &query));
    assert_eq!(
        support::found(&pending[0], "AUTO"),
        support::found(&pending[1], "EXACT")
    );
    let [auto, exact] = pending.each_ref().map(|r| r.rows_examined());
    assert!(auto <= exact, "AUTO read {auto} rows, EXACT {exact}");
    // Rows tied on distance all reach the statement's own ORDER BY, whatever route is taken.
    support::load_docs(&db, 3001..=3003, "a", |_| gathered(1));
    support::run(&db, "UPDATE docs SET tag = 99 WHERE id > 3000");
    let tie = "SELECT id FROM docs WHERE tag = 99 ORDER BY v <=> $q, id DESC LIMIT 1";
    let found = support::answer(&db, tie, &query);
    assert_eq!(found.rows(), [[Value::Int64(3003)]]);
    // A filter the index on `tag` serves in part, beside a condition no index gives, is served:
    // every mode answers its three rows whole, INDEXED through the walk (F3, "served").
    let partly = "tag = 7 AND id + 0 <= 150";
    let three = tagged(150);
    for mode in MODES {
        let sql = support::by_mode(Some(partly), mode, 10);
        support::assert_nearest(&db, &sql, &query, &stored.nearest(three, &query, 10), mode);
    }
    let refused = |filter: &str, mode: &str| {
        let before = db.work_done().unwrap();
        let outcome = support::search(&db, &support::by_mode(Some(filter), mode, 10), &query);
        assert_eq!(
            db.work_done().unwrap(),
            before,
            "{filter} {mode:?}: nothing read"
        );
        assert_filter_refused(outcome, &format!("{filter} under {mode:?}"));
    };
    support::run(
        &db,
        "ALTER TABLE docs ALTER COLUMN v SET SEARCH_MODE INDEXED",
    );
    let [written, declared] = ["INDEXED", ""]
        .map(|mode| support::answer(&db, &support::by_mode(Some("tag = 7"), mode, 10), &query));
    let told = |r: &QueryResult| {
        (
            r.rows().to_vec(),
            r.rows_examined(),
            r.distance_evaluations(),
        )
    };
    assert_eq!(
        told(&declared),
        told(&written),
        "a declared INDEXED is the written one"
    );
    // Once the index is dropped no index or key gives the rows: INDEXED, written or declared, is
    // refused when the statement is planned, with no row read and no vector compared.
    support::run(&db, "DROP INDEX docs_tag ON docs");
    refused("tag = 7", "INDEXED");
    refused("tag = 7", "");
    refused(partly, "");
    // Rows tied on distance reach the ORDER BY too when AUTO's walk turns to direct reads at a
    // doubling: 700 asked of 1,100 rows of one vector, far from the question.
    let tied = params! { "v" => gathered(1) };
    db.execute("UPDATE docs SET v = $v WHERE id <= 1100", &tied)
        .expect("tie");
    support::maintenance(&db);
    let tie = |mode| {
        let order = "v <=> $q, tag DESC, id DESC";
        let sql = format!(
            "SELECT id FROM docs WHERE id <= 700 ORDER BY {order} USE VECTOR {mode} LIMIT 8"
        );
        support::answer(&db, &sql, &query).rows().to_vec()
    };
    assert_eq!(tie("AUTO"), tie("EXACT"));
}

/// Growing only one Context leaves another's find unchanged (section 6): A's find of its rows of
/// tag 7 under `(tag, ctx)`, countable, and `(tag)`, not, as rows of tag 7 go to B, and by keys.
#[test]
fn growing_one_context_leaves_anothers_filtered_find_unchanged() {
    // `c` gives a row of B a Context the narrowed caller below does not hold.
    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000, c UUID CONTEXT_ID";
    let (query, filter) = (question(5), "ctx = 'a' AND tag = 7");
    let recovery = "recovery: CREATE INDEX docs_ctx_tag_idx ON docs (ctx, tag)";
    for (index, route) in [("tag, ctx", "direct"), ("tag", "walk")] {
        let (_, db) = support::built(&format!("growth-{route}"), DIMENSION, clauses, 2000);
        support::run(&db, &format!("CREATE INDEX docs_by ON docs ({index})"));
        let passes = |row: &[Value]| row[1] == support::text("a") && row[2] == Value::Int64(7);
        let wanted = support::stored(&db, support::DOCS).nearest(passes, &query, 50);
        let route_is = format!("filter_route: {route}");
        let words = [route_is.as_str(), recovery, "_cost:"];
        let asked = || {
            let plan = db.explain(&support::by_mode(Some(filter), "", 50)).unwrap();
            let told = words.each_ref().map(|word| plan.to_string().contains(word));
            let found = MODES.map(|mode| {
                let sql = support::by_mode(Some(filter), mode, 50);
                let found = support::assert_nearest(&db, &sql, &query, &wanted, mode);
                (found.rows().to_vec(), work(&found))
            });
            (found, told)
        };
        let (before, walked) = (asked(), route == "walk");
        assert!(before.1 == [true, walked, !walked], "{index}: {words:?}");
        support::load_docs(&db, 3001..=3200, "b", gathered);
        support::run(&db, "UPDATE docs SET tag = 7 WHERE ctx = 'b'");
        let same = asked() == before;
        assert!(same, "{index}: A's rows, work and route whatever B holds");
        // Keys named are read directly: B's row one more, no row or a row hidden from us nothing.
        let keys = |on: &contextdb::Database, more: &str| {
            let sql = support::by_mode(Some(&format!("{filter} AND id IN (7{more})")), "", 50);
            let found = support::answer(on, &sql, &query);
            (found.rows().to_vec(), work(&found))
        };
        let alone = keys(&db, "");
        assert!(keys(&db, ", 3500") == alone, "{index}: a key naming no row");
        support::load_docs(&db, 3500..=3500, "b", gathered);
        let one_more = keys(&db, ", 3500") == (alone.0, alone.1 + 1);
        assert!(one_more, "{index}: B's row named is read and counted");
        let unseen = contextdb::ContextId::new(Uuid::from_u128(1));
        let narrowed = db.scoped_with_contexts(std::collections::BTreeSet::from([unseen]));
        let hidden = keys(&narrowed, ", 3501");
        let sql = "INSERT INTO docs (id, ctx, tag, c, v) VALUES (3501, 'b', 7, $c, $v)";
        let row = params! { "c" => Uuid::from_u128(2), "v" => gathered(3501) };
        db.execute(sql, &row).unwrap();
        let same = keys(&narrowed, ", 3501") == hidden;
        assert!(same, "{index}: a row the caller may not see adds nothing");
    }
    let (_, db) = support::built("growth-refused", DIMENSION, clauses, 2000);
    support::run(&db, "CREATE INDEX docs_ctx ON docs (ctx)");
    let sql = support::by_mode(Some(filter), "INDEXED", 50);
    let before = db.work_done().unwrap();
    let refusal = support::search(&db, &sql, &query);
    assert_filter_refused(refusal, "the partition's column serves no filter");
    let plan = db.explain(&sql).unwrap().to_string();
    let told = ["refusal: none", recovery, "filter_route", "_cost:"].map(|w| plan.contains(w));
    let ok = db.work_done().unwrap() == before && told == [false, true, false, false];
    assert!(ok, "refused when planned, nothing read:\n{plan}");
}

/// Asserts the refusal of an INDEXED search whose filter no index or key gives (section 6, F3):
/// its own kind, which no maintenance cycle cures, naming the schema's cure and the other modes.
fn assert_filter_refused(outcome: contextdb::Result<QueryResult>, what: &str) {
    match outcome {
        Err(error @ Error::VectorIndexedFilterUnindexed { .. }) => {
            let words = error.to_string();
            let cures = words.contains("declare an index") && words.contains("EXACT or AUTO");
            assert!(cures, "{what}: {words}");
        }
        other => panic!("{what}: INDEXED with a filter no index gives, and got {other:?}"),
    }
}

/// A pending vector carries its one-byte form (section 6, F1): a find over 5,000 vectors written
/// and not yet sealed reads no more rows than the same find after sealing, plus the rows asked
/// for; and a committed row is the first answer for its own vector as soon as its commit
/// returns, through the writer and through a read session beside it.
#[test]
fn a_find_over_unsealed_vectors_reads_no_more_rows_than_after_sealing() {
    let db = support::open(&support::folder("unsealed-reads"));
    support::declare_docs(&db, DIMENSION, "");
    support::load_docs(&db, 1..=5000, "a", gathered);
    let (sql, query) = (support::by_mode(None, "", 10), question(6));
    let unsealed = support::answer(&db, &sql, &query);
    support::maintenance(&db);
    let sealed = support::answer(&db, &sql, &query);
    assert_eq!(unsealed.rows(), sealed.rows(), "sealing changes no answer");
    let (before, after) = (unsealed.rows_examined(), sealed.rows_examined());
    assert!(
        before <= after + 10,
        "{before} rows read unsealed, {after} sealed"
    );
    for n in 5001..=5003 {
        let vector = support::vector(n, DIMENSION);
        support::insert_doc(&db, n, "a", Some(vector.clone())).expect("a row");
        let session = db.read_session(Default::default()).unwrap();
        for on in [&db as &dyn Run, &session] {
            let found = support::answer(&on, &support::by_mode(None, "", 1), &vector);
            let first = &found.rows()[0][0];
            assert_eq!(
                first,
                &Value::Int64(n),
                "row {n} is the first answer for its vector"
            );
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
    let off = "INDEXING_THRESHOLD 18446744073709551615";
    let (_, flat) = support::built("every-group-flat", DIMENSION, off, 2000);
    // EXACT uses no centre and no code, grouped or flat: it screens the 2,000 slots and asks
    // the distance of each row it reads, less the ten returned, which are read once more.
    for on in [&db, &flat] {
        let found = support::answer(on, &support::by_mode(None, "EXACT", 10), &query);
        let (counted, read) = (found.distance_evaluations(), found.rows_examined());
        assert_eq!(counted + 10, 2000 + read, "no centres used");
    }
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
    let (_, db) = vecsearch::docs(name, DIMENSION, GROUPED);
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
    let passes = tagged(550);
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
    let (_, db) = vecsearch::docs("codes-all-the-same", DIMENSION, GROUPED);
    support::load_docs(&db, 1..=2000, "a", same);
    support::maintenance(&db);
    let groups = lists(&support::the_partition(&db));
    assert!(
        matches!(groups, Value::Int64(groups) if groups > 1),
        "the partition has {groups:?} groups"
    );
    let stored = support::stored(&db, support::DOCS);
    let half = |row: &[Value]| matches!(row[0], Value::Int64(id) if id <= 1000);
    for direction in [0, 3, 7] {
        let query = same(direction);
        // Rows at one distance come in key order, so the ten are the same under every mode.
        let wanted = stored.nearest(all, &query, 10);
        let allowed = stored.nearest(half, &query, 10);
        for mode in ["EXACT", "AUTO", "INDEXED"] {
            let what = format!("{mode}, direction {direction}");
            let sql = support::by_mode(None, mode, 10);
            support::assert_nearest(&db, &sql, &query, &wanted, &what);
            let sql = support::by_mode(Some("id <= 1000"), mode, 10);
            support::assert_nearest(&db, &sql, &query, &allowed, &format!("{what}, filtered"));
        }
    }
}

/// Every row the walk reads at the distance where it stops reaches the statement's ORDER BY,
/// as pgvector's ivfflat gives every row it reads to the outer sort: of 600 rows of one vector
/// in a grouped partition, the later sort keys pick the eight under AUTO and INDEXED as under
/// EXACT, with no filter and with one on rows.
#[test]
fn rows_tied_at_the_walks_last_distance_all_reach_the_order_by() {
    let (_, db) = vecsearch::docs("tied-at-the-cutoff", DIMENSION, "INDEXING_THRESHOLD 100");
    support::load_docs(&db, 1..=600, "a", |_| gathered(1));
    support::run(&db, "CREATE INDEX docs_tag ON docs (tag)");
    support::maintenance(&db);
    let query = question(0);
    let sql = |filter: &str, mode: &str| {
        let order = "ORDER BY v <=> $q, tag DESC, id DESC";
        format!("SELECT id FROM docs {filter} {order} USE VECTOR {mode} LIMIT 8")
    };
    let plan = db.explain(&sql("", "AUTO")).unwrap().to_string();
    assert!(plan.lines().any(|l| l.trim() == "route: indexed"), "{plan}");
    let ids = |filter, mode| {
        support::answer(&db, &sql(filter, mode), &query)
            .rows()
            .concat()
    };
    let top: Vec<_> = (0..8).map(|n| Value::Int64(599 - 50 * n)).collect();
    assert_eq!(ids("", "EXACT"), top, "EXACT: tag 49, highest key first");
    for filter in ["", "WHERE tag < 25", "WHERE id > 300"] {
        let exact = ids(filter, "EXACT");
        for mode in ["AUTO", "INDEXED"] {
            assert_eq!(ids(filter, mode), exact, "{mode} {filter:?}");
        }
    }
}

/// A tie past the LIMIT costs an exact search nothing: with one row at the query and 999 tied
/// behind it, a later ORDER BY key reads each row and then only the one answered; and the
/// rows tied at the cutoff are read again streamed, not held, so its memory does not grow with
/// the number tied. AUTO reading a narrow filter's rows directly holds no ties either.
#[test]
fn an_exact_search_with_ties_at_its_cutoff_stays_within_its_bounds() {
    let (_, db) = vecsearch::docs("tied-exact-bounds", DIMENSION, "INDEXING_THRESHOLD 100");
    support::load_docs(&db, 1..=1, "a", |_| question(0));
    support::load_docs(&db, 2..=1000, "a", |_| gathered(1));
    support::maintenance(&db);
    let sql =
        |order| format!("SELECT id FROM docs ORDER BY v <=> $q{order} USE VECTOR EXACT LIMIT 1");
    let near = support::answer(&db, &sql(""), &question(0));
    let ordered = support::answer(&db, &sql(", id DESC"), &question(0));
    assert_eq!(ordered.rows(), [vec![Value::Int64(1)]]);
    assert_eq!(
        ordered.rows_examined(),
        1001,
        "each row, then the one answered: {near:?}"
    );
    let peak = |top| {
        let tied = support::answer(&db, &sql(", id DESC"), &gathered(1));
        assert_eq!(tied.rows().concat(), [Value::Int64(top)]);
        tied.peak_bytes().unwrap()
    };
    let at_999 = peak(1000);
    let narrowed = |most| {
        let order = "ORDER BY v <=> $q, id DESC USE VECTOR AUTO LIMIT 1";
        let sql = format!("SELECT id FROM docs WHERE id <= {most} {order}");
        let plan = db.explain(&sql).unwrap().to_string();
        assert!(plan.contains("filter_route: direct"), "{plan}");
        let tied = support::answer(&db, &sql, &gathered(1));
        assert_eq!(tied.rows().concat(), [Value::Int64(most)]);
        tied.peak_bytes().unwrap()
    };
    assert_eq!(
        narrowed(99),
        narrowed(60),
        "AUTO read directly: 98 tied rows against 59"
    );
    support::load_docs(&db, 1001..=3000, "a", |_| gathered(1));
    assert_eq!(peak(3000), at_999, "2,999 tied rows against 999");
}

/// A search that walks the index holding rows tied at its cutoff and then reads its filter's
/// rows directly gives back the room it held them in before it reads a row again: with every
/// row tied and a 1 MiB answer its filter reads, it peaks within a kibibyte of one that read
/// directly at once.
#[test]
fn a_walk_that_turns_direct_gives_back_the_room_its_ties_took() {
    let (_, db) = support::built("tied-walk-turns-direct", DIMENSION, GROUPED, 2000);
    let big = "b".repeat(1 << 20);
    support::run(&db, &format!("UPDATE docs SET ctx = '{big}' WHERE id = 1"));
    let peak = |most, route| {
        let order = "ORDER BY v <=> $q, ctx DESC, id DESC USE VECTOR AUTO LIMIT 1";
        let sql = format!("SELECT id FROM docs WHERE id <= {most} AND ctx >= 'a' {order}");
        let plan = db.explain(&sql).unwrap().to_string();
        assert!(plan.contains(route), "{plan}");
        let tied = support::answer(&db, &sql, &[0.0; DIMENSION]);
        assert_eq!(tied.rows().concat(), [Value::Int64(1)]);
        tied.peak_bytes().unwrap()
    };
    let (walked, direct) = (
        peak(800, "filter_route: walk"),
        peak(500, "filter_route: direct"),
    );
    assert!(walked <= direct + 1024, "{walked} bytes against {direct}");
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

/// A store of 2,000 built rows under `a` grouped by INDEXING_THRESHOLD 1000, and five more under
/// `b` that its partition has after a maintenance pass; and what the store holds.
fn with_five_under_b(name: &str) -> (contextdb::Database, support::Stored) {
    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000";
    let (_, db) = support::built(name, DIMENSION, clauses, 2000);
    support::load_docs(&db, 2001..=2005, "b", gathered);
    support::maintenance(&db);
    let stored = support::stored(&db, support::DOCS);
    (db, stored)
}

/// Whether a stored row is under the context `b`.
fn in_b(row: &[Value]) -> bool {
    row[1] == support::text("b")
}

#[test]
fn a_filter_of_partition_keys_only_is_no_filter_on_rows() {
    let (db, stored) = with_five_under_b("keys-only-filter");
    let query = question(8);

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
    // Selected partitions that are missing or hold no row answer empty in every mode.
    support::run(&db, "DELETE FROM docs WHERE ctx = 'b'");
    for (filter, mode) in ["ctx = 'b'", "ctx = 'c'"]
        .iter()
        .flat_map(|f| MODES.map(|m| (*f, m)))
    {
        let answer = support::search(&db, &support::by_mode(Some(filter), mode, 10), &query);
        let empty = answer.as_ref().is_ok_and(|answer| answer.rows().is_empty());
        assert!(
            empty,
            "{filter} under {mode}: nothing to search is empty: {answer:?}"
        );
    }
}

const MODES: [&str; 3] = ["INDEXED", "AUTO", "EXACT"];

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

/// A search INDEXED over three rows of a key range, ten asked for, is answered whole however
/// few, and under a bound that cuts nothing in the same order for the same work (section 6, F2:
/// a bound that has dropped nothing changes nothing); a bound that cuts a row from
/// those three answers the rows within it, which are the exact scan's.
#[test]
fn a_bound_that_cuts_nothing_answers_as_without_it_and_one_that_cuts_answers_within_it() {
    let (_, db) = support::built("bound-refusal", DIMENSION, GROUPED, 2000);
    let stored = support::stored(&db, support::DOCS);
    let query = question(5);
    let few = "id <= 3";
    let passes = |row: &[Value]| matches!(row[0], Value::Int64(id) if id <= 3);
    let wanted = stored.nearest(passes, &query, 10);
    assert_eq!(wanted.len(), 3, "three rows pass the row conditions");
    let bounded = |bound: f64| {
        let filter = format!("v <=> $q < {bound:?} AND {few}");
        support::search(&db, &support::by_mode(Some(&filter), "INDEXED", 10), &query)
    };
    let sql = support::by_mode(Some(few), "INDEXED", 10);
    let plain = support::assert_nearest(&db, &sql, &query, &wanted, "three rows, however few");
    let none_cut = bounded(3.0).expect("a bound above every distance cuts nothing");
    let told = |r: &QueryResult| {
        (
            r.rows().to_vec(),
            r.rows_examined(),
            r.distance_evaluations(),
        )
    };
    assert_eq!(
        told(&none_cut),
        told(&plain),
        "a bound that drops nothing changes nothing"
    );
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

/// Past 4,096 partition tuples the filter still names its partitions: INDEXED, the default mode
/// and EXACT answer ids 1 and 2 in distance order; EXPLAIN plans the two partitions with no
/// refusal; and INDEXED counts the same work as the short form `a IN (1, 2)`, so it searched those
/// two and not all 200. The filter names 3,000 values of `a` in one case and 3,000 values of `b`
/// under another `a` in the other: more tuples than the cap.
#[test]
fn past_the_most_partition_tuples_every_mode_answers_from_the_partitions_named() {
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
        "(a IN (1, 2, {}) OR (a = 9999 AND b IN (1, {})))",
        list(10_001),
        list(2)
    );
    let sql = |filter: &str, mode: &str| {
        format!("SELECT id FROM d2 WHERE {filter} ORDER BY v <=> [1.0, 0.0] {mode}LIMIT 10")
    };
    for mode in ["USE VECTOR INDEXED ", "", "USE VECTOR EXACT "] {
        let found = support::run(&db, &sql(&filter, mode)).rows().concat();
        let wanted = [Value::Int64(1), Value::Int64(2)];
        assert_eq!(found, wanted, "{mode:?}: ids 1 and 2 in distance order");
    }
    let indexed = sql(&filter, "USE VECTOR INDEXED ");
    let plan = db.explain(&indexed).unwrap().to_string();
    let said = |line: &str| plan.lines().any(|l| l.trim() == line);
    assert!(said("partitions: 2") && said("refusal: none"), "{plan}");
    let short = sql("a IN (1, 2)", "USE VECTOR INDEXED ");
    let work = |r: QueryResult| (r.rows_examined(), r.distance_evaluations());
    let same = work(support::run(&db, &indexed)) == work(support::run(&db, &short));
    assert!(
        same,
        "the long form searches the partitions the short one does"
    );
}

/// A partition that holds fewer than `k` live rows, searched under INDEXED with a key range and
/// a bound: a bound that cuts a row answers the rows within it, which are the exact scan's, and
/// one that cuts nothing answers the five whole, as the same search without it.
#[test]
fn a_short_partition_under_a_bound_answers_within_it_and_whole_when_it_did_not_cut() {
    let (db, stored) = with_five_under_b("short-partition");
    let query = question(5);
    let wanted = stored.nearest(in_b, &query, 8);
    assert_eq!(
        wanted.len(),
        5,
        "five rows lie in the partition, and eight are asked for"
    );
    let run = |bound: f64| {
        let filter = format!("v <=> $q < {bound:?} AND ctx = 'b' AND id > 2000");
        support::search(&db, &support::by_mode(Some(&filter), "INDEXED", 8), &query)
    };
    let plain = support::by_mode(Some("ctx = 'b' AND id > 2000"), "INDEXED", 8);
    support::assert_nearest(
        &db,
        &plain,
        &query,
        &wanted,
        "the five, however few, are answered",
    );
    let none_cut = run(3.0).expect("a bound above every distance cuts nothing");
    support::assert_same_rows(
        &support::found(&none_cut, "uncut"),
        &wanted,
        "as without it",
    );
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
/// compares each vector at most twice; once the bound has cut, it stands for the last row while
/// the answer is short, so INDEXED ends short of the last group, in fewer units than without the
/// bound. The default mode's route may change during the find, so only its answer is asserted
/// (section 6, F2).
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
        // Sealed vectors are screened by their one-byte copies first: at most two evaluations a row.
        assert!(
            got.distance_evaluations() <= 2 * 2000,
            "{mode:?}: {} compared of at most two for each of the 2,000 vectors",
            got.distance_evaluations()
        );
        if mode == "INDEXED" {
            let without = support::by_mode(Some("tag = 7 AND id < 700"), mode, 8);
            let without = support::answer(&db, &without, &query);
            let (under, over) = (work(&got), work(&without));
            assert!(
                under < over,
                "{under} units under the bound, {over} without it"
            );
        }
    }
}

/// A row whose bucketed distance passes the bound but whose exact distance does not is a cut
/// like any other: the search answers the rows within the bound, short of `k`, as the exact scan
/// does, and does not refuse. The bound lies a hair below the fifth row's exact distance.
#[test]
fn a_row_cut_by_its_exact_distance_is_a_cut_and_the_short_answer_stands() {
    let (db, stored) = with_five_under_b("exact-offer-cut");
    let query = question(5);
    let wanted = stored.nearest(in_b, &query, 8);
    assert_eq!(wanted.len(), 5, "five rows lie in the partition");
    let (fourth, fifth) = (wanted[3].distance, wanted[4].distance);
    let bound = fifth - (fifth - fourth).min(1e-6) / 2.0;
    assert!(
        bound > fourth && bound < fifth,
        "the bound lies between the fourth and the fifth"
    );
    let filter = format!("v <=> $q < {bound:?} AND ctx = 'b' AND id > 2000");
    let cut = support::search(&db, &support::by_mode(Some(&filter), "INDEXED", 8), &query)
        .expect("a row cut by its exact distance is a cut, so the short answer stands");
    support::assert_same_rows(
        &support::found(&cut, "cut"),
        &wanted[..4],
        "the rows within the bound",
    );
}
